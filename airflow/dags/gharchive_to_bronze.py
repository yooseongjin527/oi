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

# GHArchive 는 한 시간 끝난 후 평균 60~120 분 lag 후에야 publish.
# data_interval_end 이후 이만큼 지나기 전엔 fetch 안 시도 (404 발생 방지).
PUBLISH_LAG_MIN = 90


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
    """Return True if S3 object exists with non-zero size.

    S3 HeadObject 응답 코드 매핑:
    - 200: 객체 존재
    - 404: 객체 없음 + s3:ListBucket 권한 있음
    - 403: 객체 없음 + s3:ListBucket 권한 없음, 또는 객체 자체 권한 부재
    - 5xx: AWS 일시 장애

    EC2 IAM Role 에 ListBucket 이 빠져있으면 새 hour 의 객체 존재 여부 검사가
    404 가 아닌 403 으로 돌아옵니다 (S3 보안 설계). 멱등성 체크 용도이므로
    403 도 "없음" 으로 간주해 다운로드/업로드를 진행 — 같은 hour 의 GHArchive
    파일은 동일한 내용이라 덮어쓰기는 안전 (멱등).

    근본 해결을 원하면 EC2 IAM Role 에 다음 정책 추가:
        Effect: Allow
        Action: s3:ListBucket
        Resource: arn:aws:s3:::<BUCKET>
    그러면 404 응답으로 돌아와서 이 fallback 로직이 안 타게 됩니다.
    """
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        return resp.get("ContentLength", 0) > 0
    except ClientError as e:
        err = e.response.get("Error", {})
        code = err.get("Code")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("404", "NoSuchKey", "NotFound") or status == 404:
            return False
        if code in ("403", "Forbidden", "AccessDenied") or status == 403:
            log.warning(
                "HeadObject 403 for s3://%s/%s — s3:ListBucket 권한 부재로 추정. "
                "객체 없음으로 간주하고 다운로드 진행 (덮어쓰기는 멱등이라 안전). "
                "근본 해결: EC2 IAM Role 에 s3:ListBucket on 버킷 추가.",
                bucket, key,
            )
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
    # data_interval_start 는 pendulum.DateTime — stdlib datetime 과 산술 시
    # 'offset-naive vs offset-aware' 충돌이 나는 케이스가 있어 강제 정규화.
    raw_dt = context["data_interval_start"]
    target_dt = datetime.fromtimestamp(raw_dt.timestamp(), tz=timezone.utc)

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

    # GHArchive publish lag 가드 — data_interval_end + PUBLISH_LAG_MIN 미만이면 retry
    now = datetime.now(timezone.utc)
    data_interval_end = target_dt + timedelta(hours=1)
    elapsed_min = (now - data_interval_end).total_seconds() / 60.0
    if elapsed_min < PUBLISH_LAG_MIN:
        wait_min = PUBLISH_LAG_MIN - elapsed_min
        raise RuntimeError(
            f"GHArchive not yet published: only {elapsed_min:.0f}min after "
            f"data_interval_end, need {PUBLISH_LAG_MIN}min (~{wait_min:.0f}min more). "
            f"Will retry."
        )

    # 추가 가드: HEAD 로 빠른 존재 확인 → 없으면 retry (다운로드 시간 절약)
    try:
        head_resp = requests.head(url, timeout=15, allow_redirects=True)
    except requests.RequestException as e:
        raise RuntimeError(f"HEAD request failed for {url}: {e}")
    if head_resp.status_code == 404:
        raise RuntimeError(f"GHArchive object not yet available (404): {url}. Will retry.")
    if head_resp.status_code >= 400:
        raise RuntimeError(f"HEAD {url} -> {head_resp.status_code}")

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
    # GHArchive 발행 lag 가 변동적이라 충분한 retry 시간 확보
    # 5 → 10 → 20 → 40 → 60 → 60min cap. 누적 약 3시간 안에 성공해야 함.
    "retries": 6,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
}

with DAG(
    dag_id="gharchive_to_bronze",
    description="Hourly GHArchive .json.gz -> S3 Bronze archive",
    default_args=default_args,
    # Start at a safe historical hour so manual triggers work without weird semantics.
    start_date=datetime(2026, 4, 27, 0, 0, tzinfo=timezone.utc),
    schedule="0 * * * *",      # top of every hour
    catchup=True,             # don't auto-fill history; use manual backfill if needed
    max_active_runs=4,          # allow some concurrency for backfills
    tags=["bronze", "gharchive"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload,
        # GHArchive publishes ~1-2h after the hour ends, so wait before fetching.
        # data_interval_end is 1h after start; we want at least another 30 min buffer.
        execution_timeout=timedelta(minutes=15),
    )
