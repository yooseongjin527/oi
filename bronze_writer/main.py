"""
bronze_writer
-------------
Consumes events from Redpanda (gh.events.live) and writes them to S3 Bronze
in batches as gzipped JSONL files, partitioned by event creation time.

Batch trigger: 500 messages OR 60 seconds, whichever comes first.
Offset commit: after successful S3 upload (at-least-once).
"""

import gzip
import io
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_LIVE", "gh.events.live")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP_ID", "bronze_writer")

S3_BUCKET = os.environ["AWS_S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_BRONZE_LIVE_PREFIX", "bronze/live")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")

BATCH_SIZE = int(os.environ.get("BRONZE_BATCH_SIZE", "500"))
BATCH_INTERVAL_SEC = int(os.environ.get("BRONZE_BATCH_INTERVAL_SEC", "60"))

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_writer")

# ----------------------------------------------------------------------
# Graceful shutdown
# ----------------------------------------------------------------------

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info(f"Received signal {signum}, will shut down after current batch.")
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# ----------------------------------------------------------------------
# S3 partitioning
# ----------------------------------------------------------------------


def partition_path_for(event: dict) -> str:
    """
    Build S3 key prefix from event's created_at (UTC).
    Falls back to ingestion time if created_at missing/unparseable.
    Format: bronze/live/year=YYYY/month=MM/day=DD/hour=HH/
    """
    created = event.get("created_at")
    dt: Optional[datetime] = None

    if isinstance(created, str):
        try:
            # GitHub timestamps look like "2026-04-30T12:34:56Z"
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            dt = None

    if dt is None:
        dt = datetime.now(timezone.utc)

    return (
        f"{S3_PREFIX}/"
        f"year={dt.year:04d}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"hour={dt.hour:02d}"
    )


def group_by_partition(events: List[dict]) -> dict:
    """Group events by their target S3 partition path."""
    groups: dict = {}
    for ev in events:
        path = partition_path_for(ev)
        groups.setdefault(path, []).append(ev)
    return groups


# ----------------------------------------------------------------------
# S3 upload
# ----------------------------------------------------------------------


def encode_jsonl_gz(events: List[dict]) -> bytes:
    """Encode events to gzipped JSON Lines (one JSON object per line)."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for ev in events:
            gz.write(json.dumps(ev, ensure_ascii=False).encode("utf-8"))
            gz.write(b"\n")
    return buf.getvalue()


def upload_batch(s3_client, partition_prefix: str, events: List[dict]) -> str:
    """Upload one partition's events as a single gzipped JSONL object."""
    body = encode_jsonl_gz(events)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{partition_prefix}/batch_{ts}_{uuid.uuid4().hex[:8]}.jsonl.gz"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return key


# ----------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------


def run() -> None:
    log.info(f"Starting bronze_writer")
    log.info(f"  Kafka:  {KAFKA_BOOTSTRAP}  topic={KAFKA_TOPIC}  group={CONSUMER_GROUP}")
    log.info(f"  S3:     s3://{S3_BUCKET}/{S3_PREFIX}/  region={AWS_REGION}")
    log.info(f"  Batch:  size={BATCH_SIZE}  interval={BATCH_INTERVAL_SEC}s")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit manually after S3 upload
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # poll a reasonable chunk; 1s timeout keeps the time-flush responsive
        consumer_timeout_ms=1000,
        max_poll_records=BATCH_SIZE,
    )

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    buffer: List[dict] = []
    last_flush = time.time()

    log.info("Consumer started, waiting for messages...")

    while not _shutdown:
        try:
            # poll() returns dict[TopicPartition, list[ConsumerRecord]]
            polled = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
            for _tp, records in polled.items():
                for rec in records:
                    buffer.append(rec.value)

            now = time.time()
            time_due = (now - last_flush) >= BATCH_INTERVAL_SEC
            size_due = len(buffer) >= BATCH_SIZE

            if buffer and (size_due or time_due):
                groups = group_by_partition(buffer)
                total_uploaded = 0
                for prefix, events in groups.items():
                    try:
                        key = upload_batch(s3_client, prefix, events)
                        total_uploaded += len(events)
                        log.info(
                            f"Uploaded {len(events)} events -> s3://{S3_BUCKET}/{key}"
                        )
                    except ClientError as e:
                        log.error(f"S3 upload failed for {prefix}: {e}")
                        # do NOT commit; will retry next loop with same buffer
                        raise

                # All partitions uploaded successfully -> commit Kafka offsets
                consumer.commit()
                log.info(
                    f"Committed offsets after uploading {total_uploaded} events "
                    f"in {len(groups)} partition(s)."
                )
                buffer.clear()
                last_flush = now

        except KafkaError as e:
            log.error(f"Kafka error: {e}; sleeping 5s before retry.")
            time.sleep(5)
        except ClientError:
            # Already logged above; back off and retry
            time.sleep(5)
        except Exception as e:
            log.exception(f"Unexpected error: {e}; sleeping 5s.")
            time.sleep(5)

    # Shutdown: flush whatever is left
    log.info("Shutting down. Flushing remaining buffer...")
    if buffer:
        try:
            groups = group_by_partition(buffer)
            for prefix, events in groups.items():
                key = upload_batch(s3_client, prefix, events)
                log.info(f"Final flush: {len(events)} events -> s3://{S3_BUCKET}/{key}")
            consumer.commit()
        except Exception as e:
            log.error(f"Final flush failed: {e}. Some events may be reprocessed.")

    consumer.close()
    log.info("bronze_writer stopped.")


if __name__ == "__main__":
    try:
        run()
    except KeyError as e:
        log.error(f"Missing required environment variable: {e}")
        sys.exit(1)
