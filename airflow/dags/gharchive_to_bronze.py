"""
GHArchive -> S3 Bronze archive

Downloads the previous hour's GitHub Archive (.json.gz) and uploads it to
s3://<bucket>/bronze/archive/year=YYYY/month=MM/day=DD/hour=HH/gharchive.json.gz

Schedule: hourly (cron 0 * * * *), each run handles `data_interval_start` (UTC).
Idempotent: skips upload if the target S3 object already exists with non-zero size.

Manual backfill example (CLI inside scheduler container):
  airflow dags backfill gharchive_to_bronze \\
    --start-date 2026-04-29T00:00:00+00:00 \\
    --end-date   2026-04-29T05:00:00+00:00
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone

import boto3
import requests
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------

S3_BUCKET = os.environ["AWS_S3_BUCKET"]
S3_ARCHIVE_PREFIX = os.environ.get("S3_BRONZE_ARCHIVE_PREFIX", "bronze/archive")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

GHARCHIVE_URL_TEMPLATE = "https://data.gharchive.org/{date}-{hour}.json.gz"
DOWNLOAD_TIMEOUT_SEC = 300  # 5 min — large hours can be 100+ MB
HTTP_CHUNK_SIZE = 1024 * 1024  # 1 MiB streaming chunks


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _gharchive_url(dt: datetime) -> str:
    """Build the GHArchive URL for the given UTC hour."""
    # GHArchive uses non-zero-padded hour: 2026-04-30-0.json.gz, ..., 2026-04-30-23.json.gz
    return GHARCHIVE_URL_TEMPLATE.format(
        date=dt.strftime("%Y-%m-%d"),
        hour=str(dt.hour),
    )


def _s3_key(dt: datetime) -> str:
    """Build the S3 destination key for the given UTC hour."""
    return (
        f"{S3_ARCHIVE_PREFIX}/"
        f"year={dt.year:04d}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"hour={dt.hour:02d}/"
        f"gharchive.json.gz"
    )


def _object_exists(s3_client, bucket: str, key: str) -> bool:
    """Return True if S3 object exists with non-zero size."""
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        return resp.get("ContentLength", 0) > 0
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# ----------------------------------------------------------------------
# Task
# ----------------------------------------------------------------------


def fetch_and_upload(**context) -> str:
    """
    Download GHArchive for the run's logical hour and upload to S3.
    Logical hour = data_interval_start (UTC).
    """
    # data_interval_start is timezone-aware UTC for hourly DAGs
    target_dt: datetime = context["data_interval_start"]
    if target_dt.tzinfo is None:
        target_dt = target_dt.replace(tzinfo=timezone.utc)
    else:
        target_dt = target_dt.astimezone(timezone.utc)

    url = _gharchive_url(target_dt)
    key = _s3_key(target_dt)

    log.info("Target hour:  %s", target_dt.isoformat())
    log.info("Source URL:   %s", url)
    log.info("Target S3:    s3://%s/%s", S3_BUCKET, key)

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Idempotency: skip if already uploaded with non-zero size
    if _object_exists(s3, S3_BUCKET, key):
        log.info("Object already exists in S3, skipping.")
        raise AirflowSkipException(f"s3://{S3_BUCKET}/{key} already present")

    # Stream-download to a temp file (don't load whole file into memory)
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=True) as tmp:
        log.info("Downloading...")
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SEC) as resp:
            resp.raise_for_status()
            content_length = resp.headers.get("Content-Length")
            if content_length:
                log.info("Content-Length: %s bytes", content_length)

            bytes_downloaded = 0
            for chunk in resp.iter_content(chunk_size=HTTP_CHUNK_SIZE):
                if chunk:
                    tmp.write(chunk)
                    bytes_downloaded += len(chunk)
            tmp.flush()

        log.info("Downloaded %d bytes.", bytes_downloaded)
        if bytes_downloaded == 0:
            raise RuntimeError(f"Empty download from {url}")

        log.info("Uploading to S3...")
        s3.upload_file(
            Filename=tmp.name,
            Bucket=S3_BUCKET,
            Key=key,
            ExtraArgs={
                "ContentType": "application/x-ndjson",
                "ContentEncoding": "gzip",
                "Metadata": {
                    "source": "gharchive",
                    "source-url": url,
                    "logical-hour": target_dt.isoformat(),
                },
            },
        )

    log.info("Upload complete: s3://%s/%s", S3_BUCKET, key)
    return key


# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------

default_args = {
    "owner": "jin",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="gharchive_to_bronze",
    description="Hourly GHArchive .json.gz -> S3 Bronze archive",
    default_args=default_args,
    # Start at a safe historical hour so manual triggers work without weird semantics.
    start_date=datetime(2026, 4, 29, 0, 0, tzinfo=timezone.utc),
    schedule="0 * * * *",      # top of every hour
    catchup=False,             # don't auto-fill history; use manual backfill if needed
    max_active_runs=2,          # allow some concurrency for backfills
    tags=["bronze", "gharchive"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload,
        # GHArchive publishes ~1-2h after the hour ends, so wait before fetching.
        # data_interval_end is 1h after start; we want at least another 30 min buffer.
        execution_timeout=timedelta(minutes=15),
    )
