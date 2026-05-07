"""bronze_to_silver — Live + Archive 통합 silver CTAS

Bronze (oi.bronze_archive: GHArchive .json.gz, oi.bronze_live: Redpanda .jsonl.gz)
  -> Silver (oi.silver_events, Parquet, dedup, 정규화)

핵심 정책 (HANDOFF §3.2 / §10.5):
  - UNION ALL of bronze_archive + bronze_live (source 컬럼으로 출처 표시)
  - dedup: 같은 event_id 면 source='live' 우선 (실시간 데이터의 신선도가 더 가치 있음)
  - language 정규화: payload 에서 추출 + JavaScript/TypeScript/Python/... 매핑
  - created_at: STRING ('2026-04-29T12:34:56Z') → TIMESTAMP

스케줄: 매일 02:00 UTC. 어제(logical date) 의 이벤트만 처리.
멱등: silver/year=Y/month=M/day=D 의 모든 hour 파티션 사전 삭제 후 INSERT.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# 환경에서 읽기 — bootstrap_athena.py 와 동일 변수
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
BUCKET = os.environ.get("AWS_S3_BUCKET", "oi-data-lake")
DATABASE = os.environ.get("OI_GLUE_DATABASE", "oi")
WORKGROUP = os.environ.get("OI_ATHENA_WORKGROUP", "oi-workgroup")
SILVER_PREFIX = "silver/events"


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


def _parts(logical_date):
    d = logical_date.astimezone(timezone.utc)
    return f"{d.year:04d}", f"{d.month:02d}", f"{d.day:02d}"


# ─── Silver INSERT SQL ─────────────────────────────────────
# - bronze_archive 와 bronze_live 의 스키마가 같아서 UNION ALL 단순함
# - source 컬럼으로 출처 표기 → silver 단계에서 row_number() OVER 로 dedup
# - 언어 정규화: payload JSON 에서 추출 + LOWER 매핑
#
# SILVER_INSERT 는 .format(year=..., month=..., day=..., bucket=...) 으로 채움.
SILVER_INSERT = """
INSERT INTO oi.silver_events
WITH unioned AS (
    SELECT
        id,
        type,
        actor.id            AS actor_id,
        actor.login          AS actor_login,
        repo.id              AS repo_id,
        repo.name           AS repo_name,
        payload,
        public,
        created_at,
        org.login            AS org_login,
        'archive'             AS source,
        year, month, day, hour
    FROM oi.bronze_archive
    WHERE year = '{year}' AND month = '{month}' AND day = '{day}'

    UNION ALL

    SELECT
        id,
        type,
        actor.id            AS actor_id,
        actor.login          AS actor_login,
        repo.id              AS repo_id,
        repo.name           AS repo_name,
        payload,
        public,
        created_at,
        org.login            AS org_login,
        'live'                AS source,
        year, month, day, hour
    FROM oi.bronze_live
    WHERE year = '{year}' AND month = '{month}' AND day = '{day}'
),
dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY id
               ORDER BY CASE source WHEN 'live' THEN 0 ELSE 1 END
           ) AS rn
    FROM unioned
),
normalized AS (
    SELECT
        id                                                        AS event_id,
        type,
        actor_login,
        actor_id,
        repo_name,
        repo_id,
        payload,
        json_extract_scalar(payload, '$.action')                  AS payload_action,
        public,
        CAST(replace(replace(created_at, 'T', ' '), 'Z', '') AS timestamp) AS created_at,
        org_login,
        -- 언어 후보: PR head/base, repository.language 우선순위
        COALESCE(
            json_extract_scalar(payload, '$.pull_request.head.repo.language'),
            json_extract_scalar(payload, '$.pull_request.base.repo.language'),
            json_extract_scalar(payload, '$.repository.language')
        )                                                          AS lang_raw,
        source,
        year, month, day, hour
    FROM dedup
    WHERE rn = 1
)
SELECT
    event_id,
    type,
    actor_login,
    actor_id,
    repo_name,
    repo_id,
    payload,
    payload_action,
    public,
    created_at,
    org_login,
    -- HANDOFF §10.5 정규화 매핑
    CASE LOWER(COALESCE(lang_raw, ''))
        WHEN 'javascript'  THEN 'JavaScript'
        WHEN 'js'          THEN 'JavaScript'
        WHEN 'typescript'  THEN 'TypeScript'
        WHEN 'ts'          THEN 'TypeScript'
        WHEN 'python'       THEN 'Python'
        WHEN 'py'          THEN 'Python'
        WHEN 'rust'         THEN 'Rust'
        WHEN 'go'          THEN 'Go'
        WHEN 'golang'       THEN 'Go'
        WHEN 'c++'         THEN 'C++'
        WHEN 'cpp'         THEN 'C++'
        WHEN 'c#'          THEN 'C#'
        WHEN 'csharp'       THEN 'C#'
        WHEN ''            THEN 'Unknown'
        WHEN NULL           THEN 'Unknown'
        ELSE COALESCE(lang_raw, 'Unknown')
    END                                                            AS repo_language,
    source,
    year, month, day, hour
FROM normalized
"""


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
        logger.info("Cleanup: deleted %d objects from s3://%s/%s", n, BUCKET, prefix)
        return {"prefix": prefix, "deleted": n}

    @task
    def insert(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = SILVER_INSERT.format(year=y, month=m, day=d)
        info = _athena_run(sql)
        return {
            "qid": info["QueryExecutionId"],
            "scanned": info.get("Statistics", {}).get("DataScannedInBytes"),
        }

    @task
    def verify(logical_date=None):
        y, m, d = _parts(logical_date)
        sql = f"""
        SELECT
          COUNT(*)                            AS cnt,
          COUNT(DISTINCT hour)                  AS hrs,
          SUM(CASE WHEN source = 'live'    THEN 1 ELSE 0 END) AS live_rows,
          SUM(CASE WHEN source = 'archive' THEN 1 ELSE 0 END) AS archive_rows,
          SUM(CASE WHEN repo_language <> 'Unknown' THEN 1 ELSE 0 END) AS lang_known
        FROM oi.silver_events
        WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
        """
        info = _athena_run(sql)
        athena = boto3.client("athena", region_name=REGION)
        rows = athena.get_query_results(QueryExecutionId=info["QueryExecutionId"])["ResultSet"]["Rows"]
        vals = [c.get("VarCharValue", "0") for c in rows[1]["Data"]]
        cnt, hrs, live, archive, lang = (int(v) for v in vals)
        logger.info(
            "Silver %s-%s-%s: rows=%d hours=%d live=%d archive=%d lang_known=%d",
            y, m, d, cnt, hrs, live, archive, lang,
        )
        if cnt == 0:
            raise RuntimeError(f"Silver has 0 rows for {y}-{m}-{d}")
        return {"rows": cnt, "hours": hrs, "live": live, "archive": archive, "lang_known": lang}

    cleanup() >> insert() >> verify()


dag_instance = bronze_to_silver_dag()
