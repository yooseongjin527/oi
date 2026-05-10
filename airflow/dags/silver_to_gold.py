"""silver-to-gold DAG (Day 4 확장)

Silver (oi.silver_events) -> Gold (5개 마트):
  - oi.gold_repo_daily       (Day 3)
  - oi.gold_actor_daily      (Day 3)
  - oi.gold_repo_acceleration (Day 4 F2)
  - oi.gold_repo_anomaly      (Day 4 F3)
  - oi.gold_repo_hourly       (Day 4 F4)

Triggered by Silver Dataset update.
Idempotent: deletes target-day Gold prefixes before INSERT.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import boto3
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

import os

REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
BUCKET = os.environ.get("AWS_S3_BUCKET", "oi-data-lake")
GOLD_REPO_PREFIX = "gold/repo_daily"
GOLD_ACTOR_PREFIX = "gold/actor_daily"
GOLD_ACCEL_PREFIX = "gold/repo_acceleration"
GOLD_ANOMALY_PREFIX = "gold/repo_anomaly"
GOLD_HOURLY_PREFIX = "gold/repo_hourly"
GOLD_LANG_PREFIX = "gold/language_activity"
WORKGROUP = os.environ.get("OI_ATHENA_WORKGROUP", "oi-workgroup")
DATABASE = os.environ.get("OI_GLUE_DATABASE", "oi")

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


def _prev_parts(logical_date):
    """logical_date의 하루 전 (UTC) y/m/d 반환. F2 가속도 계산용."""
    d = logical_date.astimezone(timezone.utc) - timedelta(days=1)
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
    # 매일 01:00 UTC = KST 10:00 — bronze_to_silver(00:30 UTC) 후 30분 lag.
    # KST 사용자가 어제 분석을 오전 10시에 확인 가능.
    schedule="0 1 * * *",
    catchup=True,
    max_active_runs=2,
    default_args={"owner": "jin", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "athena"],
)
def silver_to_gold_dag():

    @task
    def cleanup(logical_date=None):
        y, m, d = _parts(logical_date)
        prefixes = {
            "repo": f"{GOLD_REPO_PREFIX}/year={y}/month={m}/day={d}/",
            "actor": f"{GOLD_ACTOR_PREFIX}/year={y}/month={m}/day={d}/",
            "accel": f"{GOLD_ACCEL_PREFIX}/year={y}/month={m}/day={d}/",
            "anomaly": f"{GOLD_ANOMALY_PREFIX}/year={y}/month={m}/day={d}/",
            "hourly": f"{GOLD_HOURLY_PREFIX}/year={y}/month={m}/day={d}/",
            "lang":   f"{GOLD_LANG_PREFIX}/year={y}/month={m}/day={d}/",
        }
        return {k: _s3_cleanup(p) for k, p in prefixes.items()}

    @task(retries=6, retry_delay=timedelta(minutes=15))
    def gate_silver_ready(logical_date=None):
        """Silver 파티션이 채워졌는지 사전 검증.

        bronze_to_silver 가 늦게 끝났을 때 silver 가 비어있는 채로 INSERT 가
        0건을 적재하는 사고를 막기 위한 gate. silver 가 비면 즉시 RuntimeError 를
        던져 후속 INSERT 들이 아예 돌지 않게 함.

        retry 정책을 task 단위로 강하게 (6회 × 15분) 잡아서 bronze_to_silver 가
        뒤늦게 silver 를 채워주는 시나리오를 자연스럽게 흡수. 최대 1.5h 대기.
        """
        y, m, d = _parts(logical_date)
        sql = f"""
        SELECT COUNT(*) AS cnt
        FROM oi.silver_events
        WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        """
        info = _athena_run(sql)
        qid = info["QueryExecutionId"]
        athena = boto3.client("athena", region_name=REGION)
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        cnt_str = rows[1]["Data"][0].get("VarCharValue", "0") if len(rows) > 1 else "0"
        cnt = int(cnt_str or 0)
        logger.info("Silver row count for %s-%s-%s: %d", y, m, d, cnt)
        if cnt == 0:
            raise RuntimeError(
                f"Silver empty for {y}-{m}-{d}. bronze_to_silver 의 logical_date "
                f"{y}-{m}-{d} run 이 성공했는지 먼저 확인하세요."
            )
        return cnt

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

    # ---------------------- Day 4 추가 task 3개 -----------------------

    @task
    def insert_repo_acceleration(logical_date=None):
        """F2: 어제 대비 오늘 event_count 가속도.

        - prev_event_count가 없는 경우 (첫날): 0으로 채우고 acceleration_ratio=NULL
        - acceleration_ratio = today / NULLIF(yesterday, 0)
        - dominant_event_type은 오늘 기준
        """
        y, m, d = _parts(logical_date)
        py, pm, pd = _prev_parts(logical_date)
        sql = f"""
        INSERT INTO oi.gold_repo_acceleration
        WITH today AS (
            SELECT
                repo_id,
                MAX(repo_name) AS repo_name,
                COUNT(*) AS event_count,
                MAX_BY(type, cnt) AS dominant_event_type
            FROM (
                SELECT repo_id, repo_name, type,
                       COUNT(*) OVER (PARTITION BY repo_id, type) AS cnt
                FROM oi.silver_events
                WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
            )
            GROUP BY repo_id
        ),
        yesterday AS (
            SELECT repo_id, COUNT(*) AS prev_event_count
            FROM oi.silver_events
            WHERE year = '{py}' AND month = '{pm}' AND day = '{pd}'
            GROUP BY repo_id
        )
        SELECT
            DATE '{y}-{m}-{d}'                              AS event_date,
            t.repo_id,
            t.repo_name,
            t.event_count,
            COALESCE(y.prev_event_count, 0)                  AS prev_event_count,
            t.event_count - COALESCE(y.prev_event_count, 0)  AS event_delta,
            CAST(t.event_count AS double)
                / NULLIF(y.prev_event_count, 0)              AS acceleration_ratio,
            t.dominant_event_type,
            '{y}' AS year, '{m}' AS month, '{d}' AS day
        FROM today t
        LEFT JOIN yesterday y ON t.repo_id = y.repo_id
        """
        resp = _athena_run(sql)
        return {"qid": resp["QueryExecutionId"]}

    @task
    def insert_repo_anomaly(logical_date=None):
        """F3: 이상 탐지 — activity z-score + watch/fork 비율 z-score.

        - z-score: (x - mean) / NULLIF(stddev_pop, 0)  (분모 0 가드)
        - anomaly_score: 세 z-score의 sqrt(sum of squares) (NaN 방지 위해 NULL은 0 처리)
        """
        y, m, d = _parts(logical_date)
        sql = f"""
        INSERT INTO oi.gold_repo_anomaly
        WITH per_repo AS (
            SELECT
                repo_id,
                MAX(repo_name) AS repo_name,
                COUNT(*) AS event_count,
                SUM(IF(type = 'WatchEvent', 1, 0)) AS watch_count,
                SUM(IF(type = 'ForkEvent', 1, 0))  AS fork_count
            FROM oi.silver_events
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
            GROUP BY repo_id
        ),
        with_ratios AS (
            SELECT
                repo_id, repo_name, event_count, watch_count,
                CAST(watch_count AS double) / NULLIF(event_count, 0) AS watch_ratio,
                CAST(fork_count  AS double) / NULLIF(event_count, 0) AS fork_ratio
            FROM per_repo
        ),
        with_stats AS (
            SELECT
                *,
                AVG(CAST(event_count AS double))         OVER () AS activity_mean,
                NULLIF(STDDEV_POP(CAST(event_count AS double)) OVER (), 0) AS activity_std,
                AVG(watch_ratio)                          OVER () AS watch_mean,
                NULLIF(STDDEV_POP(watch_ratio)            OVER (), 0) AS watch_std,
                AVG(fork_ratio)                           OVER () AS fork_mean,
                NULLIF(STDDEV_POP(fork_ratio)             OVER (), 0) AS fork_std
            FROM with_ratios
        )
        SELECT
            DATE '{y}-{m}-{d}'                              AS event_date,
            repo_id,
            repo_name,
            event_count,
            COALESCE((CAST(event_count AS double) - activity_mean) / activity_std, 0) AS activity_zscore,
            watch_count,
            watch_ratio,
            COALESCE((watch_ratio - watch_mean) / watch_std, 0)           AS watch_zscore,
            fork_ratio,
            COALESCE((fork_ratio - fork_mean)  / fork_std, 0)             AS fork_zscore,
            SQRT(
                POW(COALESCE((CAST(event_count AS double) - activity_mean) / activity_std, 0), 2) +
                POW(COALESCE((watch_ratio - watch_mean) / watch_std, 0), 2) +
                POW(COALESCE((fork_ratio  - fork_mean)  / fork_std,  0), 2)
            )                                                AS anomaly_score,
            '{y}' AS year, '{m}' AS month, '{d}' AS day
        FROM with_stats
        """
        resp = _athena_run(sql)
        return {"qid": resp["QueryExecutionId"]}

    @task
    def insert_repo_hourly(logical_date=None):
        """F4: repo × hour 입자도. silver의 hour partition 활용."""
        y, m, d = _parts(logical_date)
        sql = f"""
        INSERT INTO oi.gold_repo_hourly
        SELECT
            DATE '{y}-{m}-{d}'                              AS event_date,
            CAST(hour AS integer)                            AS hour,
            repo_id,
            MAX(repo_name)                                   AS repo_name,
            COUNT(*)                                       AS event_count,
            MAX_BY(type, cnt)                                 AS dominant_event_type,
            '{y}' AS year, '{m}' AS month, '{d}' AS day
        FROM (
            SELECT hour, repo_id, repo_name, type,
                   COUNT(*) OVER (PARTITION BY hour, repo_id, type) AS cnt
            FROM oi.silver_events
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        )
        GROUP BY hour, repo_id
        """
        resp = _athena_run(sql)
        return {"qid": resp["QueryExecutionId"]}

    @task
    def insert_language_activity(logical_date=None):
        """F4: hour × language 히트맵 마트.

        - silver.repo_language 정규화 결과 그대로 사용 (Unknown 포함)
        - 'Unknown' 도 한 카테고리로 유지 — F4 화면에서 별도 처리 가능
        """
        y, m, d = _parts(logical_date)
        sql = f"""
        INSERT INTO oi.gold_language_activity
        SELECT
            DATE '{y}-{m}-{d}'                              AS event_date,
            CAST(hour AS integer)                            AS hour,
            repo_language                                    AS language,
            COUNT(*)                                       AS event_count,
            COUNT(DISTINCT repo_id)                          AS unique_repos,
            COUNT(DISTINCT actor_id)                         AS unique_actors,
            '{y}' AS year, '{m}' AS month, '{d}' AS day
        FROM oi.silver_events
        WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        GROUP BY CAST(hour AS integer), repo_language
        """
        resp = _athena_run(sql)
        return {"qid": resp["QueryExecutionId"]}

    # ---------------------- verify 확장 -----------------------

    @task
    def verify(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = f"""
        SELECT
          (SELECT COUNT(*) FROM oi.gold_repo_daily
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS repos,
          (SELECT COUNT(*) FROM oi.gold_actor_daily
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS actors,
          (SELECT COUNT(*) FROM oi.gold_repo_acceleration
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS accel,
          (SELECT COUNT(*) FROM oi.gold_repo_anomaly
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS anomaly,
          (SELECT COUNT(*) FROM oi.gold_repo_hourly
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS hourly,
          (SELECT COUNT(*) FROM oi.gold_language_activity
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}') AS lang
        """
        resp = _athena_run(sql)
        qid = resp["QueryExecutionId"]
        athena = boto3.client("athena", region_name=REGION)
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        vals = [c.get("VarCharValue") for c in rows[1]["Data"]]
        repos, actors, accel, anomaly, hourly, lang = (int(v) for v in vals)
        logger.info(
            "Gold %s-%s-%s: repos=%s actors=%s accel=%s anomaly=%s hourly=%s lang=%s",
            y, m, d, repos, actors, accel, anomaly, hourly, lang,
        )
        if min(repos, actors, accel, anomaly, hourly, lang) == 0:
            raise RuntimeError(
                f"Gold empty for {y}-{m}-{d}: "
                f"repos={repos} actors={actors} accel={accel} "
                f"anomaly={anomaly} hourly={hourly} lang={lang}"
            )
        return {
            "repos": repos, "actors": actors,
            "accel": accel, "anomaly": anomaly,
            "hourly": hourly, "lang": lang,
        }

    c = cleanup()
    g = gate_silver_ready()
    r = insert_repo_daily()
    a = insert_actor_daily()
    f2 = insert_repo_acceleration()
    f3 = insert_repo_anomaly()
    f4_hourly = insert_repo_hourly()
    f4_lang = insert_language_activity()
    v = verify()
    c >> g >> [r, a, f2, f3, f4_hourly, f4_lang] >> v


dag_instance = silver_to_gold_dag()