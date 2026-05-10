"""silver_to_gold_hourly DAG — 매시간 hourly 마트 적재 (풀 커버리지)

목적: "오늘 시간대별 진행" 을 GHArchive 풀 커버리지로 노출.

데이터 소스:
- bronze_live (collector → Redpanda → S3, 매분 적재) — 즉시성 ↑, sample (~5~30%)
- bronze_archive (gharchive_to_bronze hourly DAG, GHArchive 다운로드) — 풀 커버리지(~100%), 90~180min lag

silver 우회:
- silver_to_gold (daily) 가 silver 만들기 전엔 silver 의 진행 hour 가 비어있어 사용 불가
- 대신 bronze 두 source 를 직접 UNION → dedup (archive 우선)

타이밍:
- schedule: 매시 05분 (`5 * * * *`), catchup=False
- gate 가 bronze_archive 도착까지 retry (6 × 30min = 3h 윈도우)
- 평균 가시 lag: ~2시간 (gharchive publish 90min + retry 1회 평균)

흐름:
  cleanup → gate_sources_ready → insert_hourly → verify_hourly
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
BUCKET = os.environ.get("AWS_S3_BUCKET", "oi-data-lake")
GOLD_HOURLY_PREFIX = "gold/hourly_recent"
WORKGROUP = os.environ.get("OI_ATHENA_WORKGROUP", "oi-workgroup")
DATABASE = os.environ.get("OI_GLUE_DATABASE", "oi")

# 처리 대상 hour 보정 (logical_date 보다 N 시간 전 hour 를 처리).
# GHArchive 가 hour 끝난 후 평균 60~90min 후 publish → gharchive_to_bronze 가
# PUBLISH_LAG_MIN=90 후 다운로드. 따라서 *2시간 전* hour 면 archive 가 거의 항상
# 도착해 있어 gate 가 첫 시도에 통과. scheduled / manual trigger 둘 다 일관 처리.
# 평균 가시 lag = 2시간 + 처리 ~5분.
PROCESS_HOURS_BACK = int(os.environ.get("HOURLY_PROCESS_HOURS_BACK", "2"))


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
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state == "SUCCEEDED":
            stats = info.get("Statistics", {})
            logger.info(
                "Athena %s SUCCEEDED (scanned=%s B, time=%s ms)",
                qid, stats.get("DataScannedInBytes"), stats.get("EngineExecutionTimeInMillis"),
            )
            return info
        if state in ("FAILED", "CANCELLED"):
            reason = info["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena {qid} {state}: {reason}")
        time.sleep(poll_seconds)


def _hour_parts(logical_date):
    """처리 대상 hour 의 year/month/day/hour 문자열.

    logical_date 의 *PROCESS_HOURS_BACK 시간 전* hour 를 처리한다.
    예: scheduled "5 * * * *" 에서 5/10 03:05 trigger → logical_date=02:00 →
        2h 전 = hour=00 처리 (이 시점엔 archive 가 안전하게 도착해 있음).
    Manual trigger 도 동일 로직 적용 → archive 부재로 영구 fail 방지.
    """
    d = logical_date.astimezone(timezone.utc) - timedelta(hours=PROCESS_HOURS_BACK)
    return (
        f"{d.year:04d}",
        f"{d.month:02d}",
        f"{d.day:02d}",
        f"{d.hour:02d}",
    )


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
    dag_id="silver_to_gold_hourly",
    start_date=datetime(2026, 5, 10, tzinfo=timezone.utc),
    schedule="5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "jin", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "athena", "hourly"],
)
def silver_to_gold_hourly_dag():

    @task
    def cleanup(logical_date=None):
        y, m, d, h = _hour_parts(logical_date)
        prefix = f"{GOLD_HOURLY_PREFIX}/year={y}/month={m}/day={d}/hour={h}/"
        return {"deleted": _s3_cleanup(prefix), "prefix": prefix}

    @task(retries=6, retry_delay=timedelta(minutes=30))
    def gate_sources_ready(logical_date=None):
        """두 source 가 해당 hour 에 다 차있는지 검증.

        - bronze_live: collector + bronze_writer 가 매분 적재 → 거의 항상 OK
        - bronze_archive: gharchive_to_bronze 가 publish lag 90min 이상 후에 다운로드 →
          retry 정책으로 도착 기다림 (6회 × 30분 = 3h 윈도우)

        archive 가 끝까지 도착 안 하면 fail. live 만으로 sample 만이라도 적재하고 싶으면
        max_attempts 늘리거나 별도 fallback 구현 필요 — 지금은 "풀 커버리지 보장" 정책.
        """
        y, m, d, h = _hour_parts(logical_date)
        sql = f"""
        SELECT
          (SELECT COUNT(*) FROM oi.bronze_live
            WHERE year='{y}' AND month='{m}' AND day='{d}' AND hour='{h}')   AS live_cnt,
          (SELECT COUNT(*) FROM oi.bronze_archive
            WHERE year='{y}' AND month='{m}' AND day='{d}' AND hour='{h}')   AS archive_cnt
        """
        info = _athena_run(sql)
        athena = boto3.client("athena", region_name=REGION)
        rows = athena.get_query_results(QueryExecutionId=info["QueryExecutionId"])["ResultSet"]["Rows"]
        live = int(rows[1]["Data"][0].get("VarCharValue", "0") or 0)
        archive = int(rows[1]["Data"][1].get("VarCharValue", "0") or 0)
        logger.info("Sources for %s-%s-%s hour=%s: live=%d, archive=%d",
                    y, m, d, h, live, archive)

        if live == 0 and archive == 0:
            raise RuntimeError(
                f"두 source 모두 비어있음 ({y}-{m}-{d} hour={h}). "
                f"collector / gharchive_to_bronze 동작 확인."
            )
        if archive == 0:
            raise RuntimeError(
                f"bronze_archive 미도착 ({y}-{m}-{d} hour={h}). "
                f"gharchive_to_bronze 의 해당 hour run 이 끝나길 기다림 (retry)."
            )
        return {"live": live, "archive": archive}

    @task
    def insert_hourly(logical_date=None):
        """bronze_live + bronze_archive UNION + dedup → hourly mart 적재.

        dedup 정책: 같은 event_id 면 source='archive' 우선
        (archive 가 풀 데이터라 더 정확. silver 단계는 'live' 우선이지만 hourly mart 는
        풀 커버리지 우선이라 archive 우선이 자연스러움)
        """
        y, m, d, h = _hour_parts(logical_date)
        sql = f"""
        INSERT INTO oi.gold_hourly_recent
        WITH unioned AS (
            SELECT id, type, actor, repo, 'live' AS src
            FROM oi.bronze_live
            WHERE year='{y}' AND month='{m}' AND day='{d}' AND hour='{h}'
            UNION ALL
            SELECT id, type, actor, repo, 'archive' AS src
            FROM oi.bronze_archive
            WHERE year='{y}' AND month='{m}' AND day='{d}' AND hour='{h}'
        ),
        dedup AS (
            SELECT id, type, actor, repo,
                   ROW_NUMBER() OVER (
                     PARTITION BY id
                     ORDER BY CASE src WHEN 'archive' THEN 0 ELSE 1 END
                   ) AS rn
            FROM unioned
            WHERE id IS NOT NULL
        ),
        per_repo_type AS (
            SELECT repo, actor, type,
                   COUNT(*) OVER (PARTITION BY repo.id, type) AS cnt
            FROM dedup
            WHERE rn = 1
              AND repo.id IS NOT NULL
        )
        SELECT
            DATE '{y}-{m}-{d}'                                          AS event_date,
            CAST({int(h)} AS integer)                                    AS hour,
            CAST('{y}-{m}-{d} {h}:00:00' AS timestamp)                   AS hour_ts,
            repo.id                                                      AS repo_id,
            MAX(repo.name)                                                AS repo_name,
            COUNT(*)                                                   AS event_count,
            COUNT(DISTINCT actor.id)                                      AS unique_actors,
            SUM(IF(type = 'PushEvent', 1, 0))                             AS push_count,
            SUM(IF(type = 'PullRequestEvent', 1, 0))                      AS pr_count,
            SUM(IF(type = 'IssuesEvent', 1, 0))                           AS issue_count,
            SUM(IF(type = 'WatchEvent', 1, 0))                            AS watch_count,
            SUM(IF(type = 'ForkEvent', 1, 0))                             AS fork_count,
            MAX_BY(type, cnt)                                              AS dominant_event_type,
            '{y}' AS year, '{m}' AS month, '{d}' AS day, '{h}' AS hour_p
        FROM per_repo_type
        GROUP BY repo.id
        """
        info = _athena_run(sql)
        return {"qid": info["QueryExecutionId"]}

    @task
    def verify_hourly(logical_date=None):
        y, m, d, h = _hour_parts(logical_date)
        sql = f"""
        SELECT COUNT(*) AS repos
        FROM oi.gold_hourly_recent
        WHERE year='{y}' AND month='{m}' AND day='{d}' AND hour_p='{h}'
        """
        info = _athena_run(sql)
        athena = boto3.client("athena", region_name=REGION)
        rows = athena.get_query_results(QueryExecutionId=info["QueryExecutionId"])["ResultSet"]["Rows"]
        repos = int(rows[1]["Data"][0].get("VarCharValue", "0") or 0)
        logger.info("Hourly Gold %s-%s-%s hour=%s: repos=%d", y, m, d, h, repos)
        if repos == 0:
            raise RuntimeError(f"Hourly Gold empty for {y}-{m}-{d} hour={h}")
        return {"repos": repos}

    c = cleanup()
    g = gate_sources_ready()
    i = insert_hourly()
    v = verify_hourly()
    c >> g >> i >> v


dag_instance = silver_to_gold_hourly_dag()
