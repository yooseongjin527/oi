"""bronze-to-silver DAG
Bronze (oi.bronze_archive, gzipped JSON) -> Silver (oi.silver_events, Parquet).
Daily schedule; idempotent on re-run.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import boto3
from airflow.decorators import dag, task
from airflow.datasets import Dataset

SILVER_DAILY = Dataset("s3://oi-data-lake/silver/events/")

logger = logging.getLogger(__name__)

REGION = "ap-northeast-2"
BUCKET = "oi-data-lake"
SILVER_PREFIX = "silver/events"
WORKGROUP = "oi-workgroup"
DATABASE = "oi"


def _athena_run(sql: str, poll_seconds: float = 2.0):
    athena = boto3.client("athena", region_name=REGION)
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )
    qid = resp["QueryExecutionId"]
    logger.info("Athena query started: %s", qid)
    while True:
        exec_resp = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = exec_resp["Status"]["State"]
        if state == "SUCCEEDED":
            stats = exec_resp.get("Statistics", {})
            logger.info(
                "Athena %s SUCCEEDED (scanned=%s B, time=%s ms)",
                qid, stats.get("DataScannedInBytes"), stats.get("EngineExecutionTimeInMillis"),
            )
            return exec_resp
        if state in ("FAILED", "CANCELLED"):
            reason = exec_resp["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {qid} {state}: {reason}")
        time.sleep(poll_seconds)


def _parts(logical_date):
    d = logical_date.astimezone(timezone.utc)
    return f"{d.year:04d}", f"{d.month:02d}", f"{d.day:02d}"


@dag(
    dag_id="bronze_to_silver",
    start_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
    schedule="0 2 * * *",
    catchup=True,
    max_active_runs=2,
    default_args={"owner": "jin", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["silver", "athena"],
)
def bronze_to_silver_dag():

    @task
    def cleanup(logical_date=None):
        y, m, d = _parts(logical_date)
        prefix = f"{SILVER_PREFIX}/year={y}/month={m}/day={d}/"
        s3 = boto3.resource("s3", region_name=REGION)
        bucket = s3.Bucket(BUCKET)
        n = 0
        for obj in bucket.objects.filter(Prefix=prefix):
            obj.delete()
            n += 1
        logger.info("Cleanup: deleted %d objects from %s", n, prefix)
        return {"prefix": prefix, "deleted": n}

    @task
    def insert(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = f"""
        INSERT INTO oi.silver_events
        SELECT
            id,
            type,
            actor.login AS actor_login,
            actor.id    AS actor_id,
            repo.name   AS repo_name,
            repo.id     AS repo_id,
            payload    AS payload,
            json_extract_scalar(payload, '$.action') AS payload_action,
            public AS public,
            CAST(replace(replace(created_at, 'T', ' '), 'Z', '') AS timestamp) AS created_at,
            org.login AS org_login,
            year, month, day, hour
        FROM oi.bronze_archive
        WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        """
        resp = _athena_run(sql)
        return {
            "qid": resp["QueryExecutionId"],
            "scanned": resp.get("Statistics", {}).get("DataScannedInBytes"),
        }

    @task(outlets=[SILVER_DAILY])
    def verify(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = f"""
        SELECT COUNT(*) AS cnt, COUNT(DISTINCT hour) AS hrs
        FROM oi.silver_events
        WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        """
        resp = _athena_run(sql)
        qid = resp["QueryExecutionId"]
        athena = boto3.client("athena", region_name=REGION)
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        vals = [c.get("VarCharValue") for c in rows[1]["Data"]]
        cnt, hrs = int(vals[0]), int(vals[1])
        logger.info("Silver %s-%s-%s: rows=%s hours=%s", y, m, d, cnt, hrs)
        if cnt == 0:
            raise RuntimeError(f"Silver has 0 rows for {y}-{m}-{d}")
        return {"rows": cnt, "hours": hrs}

    cleanup() >> insert() >> verify()


dag_instance = bronze_to_silver_dag()
