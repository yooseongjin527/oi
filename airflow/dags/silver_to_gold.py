"""silver-to-gold DAG

Silver (oi.silver_events) -> Gold (oi.gold_repo_daily, oi.gold_actor_daily).
Triggered by Silver Dataset update.
Idempotent: deletes target-day Gold prefixes before INSERT.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import boto3
from airflow.decorators import dag, task
from airflow.datasets import Dataset

logger = logging.getLogger(__name__)

REGION = "ap-northeast-2"
BUCKET = "oi-data-lake"
GOLD_REPO_PREFIX = "gold/repo_daily"
GOLD_ACTOR_PREFIX = "gold/actor_daily"
WORKGROUP = "oi-workgroup"
DATABASE = "oi"

SILVER_DAILY = Dataset("s3://oi-data-lake/silver/events/")


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


def _s3_cleanup(prefix: str):
    s3 = boto3.resource("s3", region_name=REGION)
    bucket = s3.Bucket(BUCKET)
    n = 0
    for obj in bucket.objects.filter(Prefix=prefix):
        obj.delete()
        n += 1
    logger.info("Cleaned %s: deleted %d objects", prefix, n)
    return n


@dag(
    dag_id="silver_to_gold",
    start_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
    schedule=[SILVER_DAILY],
    catchup=True,
    max_active_runs=2,
    default_args={"owner": "jin", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "athena"],
)
def silver_to_gold_dag():

    @task
    def cleanup(logical_date=None):
        y, m, d = _parts(logical_date)
        r1 = f"{GOLD_REPO_PREFIX}/year={y}/month={m}/day={d}/"
        r2 = f"{GOLD_ACTOR_PREFIX}/year={y}/month={m}/day={d}/"
        return {"repo": _s3_cleanup(r1), "actor": _s3_cleanup(r2)}

    @task
    def insert_repo_daily(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = f"""
        INSERT INTO oi.gold_repo_daily
        SELECT
            DATE '{y}-{m}-{d}'                  AS event_date,
            repo_id,
            MAX(repo_name)                      AS repo_name,
            COUNT(*)                          AS event_count,
            COUNT(DISTINCT actor_id)           AS unique_actors,
            MAX_BY(type, cnt)                    AS dominant_event_type,
            SUM(IF(type = 'PushEvent', 1, 0))   AS push_count,
            SUM(IF(type = 'PullRequestEvent', 1, 0)) AS pr_count,
            SUM(IF(type = 'IssuesEvent', 1, 0)) AS issue_count,
            SUM(IF(type = 'WatchEvent', 1, 0))  AS watch_count,
            SUM(IF(type = 'ForkEvent', 1, 0))   AS fork_count,
            MIN(created_at)                      AS first_event_at,
            MAX(created_at)                      AS last_event_at,
            '{y}' AS year, '{m}' AS month, '{d}' AS day
        FROM (
            SELECT repo_id, repo_name, actor_id, type, created_at,
                   COUNT(*) OVER (PARTITION BY repo_id, type) AS cnt
            FROM oi.silver_events
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        )
        GROUP BY repo_id
        """
        resp = _athena_run(sql)
        return {"qid": resp["QueryExecutionId"]}

    @task
    def insert_actor_daily(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = f"""
        INSERT INTO oi.gold_actor_daily
        SELECT
            DATE '{y}-{m}-{d}'                  AS event_date,
            actor_id,
            MAX(actor_login)                     AS actor_login,
            COUNT(*)                          AS event_count,
            COUNT(DISTINCT repo_id)            AS unique_repos,
            MAX_BY(type, cnt)                    AS dominant_event_type,
            SUM(IF(type = 'PushEvent', 1, 0))   AS push_count,
            SUM(IF(type = 'PullRequestEvent', 1, 0)) AS pr_count,
            SUM(IF(type = 'IssuesEvent', 1, 0)) AS issue_count,
            SUM(IF(type = 'WatchEvent', 1, 0))  AS watch_count,
            SUM(IF(type = 'ForkEvent', 1, 0))   AS fork_count,
            '{y}' AS year, '{m}' AS month, '{d}' AS day
        FROM (
            SELECT actor_id, actor_login, repo_id, type,
                   COUNT(*) OVER (PARTITION BY actor_id, type) AS cnt
            FROM oi.silver_events
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        )
        GROUP BY actor_id
        """
        resp = _athena_run(sql)
        return {"qid": resp["QueryExecutionId"]}

    @task
    def verify(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = f"""
        SELECT
          (SELECT COUNT(*) FROM oi.gold_repo_daily
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS repos,
          (SELECT COUNT(*) FROM oi.gold_actor_daily
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS actors
        """
        resp = _athena_run(sql)
        qid = resp["QueryExecutionId"]
        athena = boto3.client("athena", region_name=REGION)
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        vals = [c.get("VarCharValue") for c in rows[1]["Data"]]
        r_, a_ = int(vals[0]), int(vals[1])
        logger.info("Gold %s-%s-%s: repos=%s actors=%s", y, m, d, r_, a_)
        if r_ == 0 or a_ == 0:
            raise RuntimeError(f"Gold empty for {y}-{m}-{d}: repos={r_} actors={a_}")
        return {"repos": r_, "actors": a_}

    c = cleanup()
    r = insert_repo_daily()
    a = insert_actor_daily()
    v = verify()
    c >> [r, a] >> v


dag_instance = silver_to_gold_dag()
