#!/usr/bin/env python3
"""Athena/Glue 부트스트랩 — DB·Workgroup·결과위치 생성 + 모든 DDL 실행.

이 스크립트는 1회성 셋업이지만 멱등이라 여러 번 실행해도 안전.

사용:
    AWS_REGION=ap-northeast-2 \\
    AWS_S3_BUCKET=oi-data-lake \\
    GLUE_DATABASE=oi \\
    ATHENA_WORKGROUP=oi-workgroup \\
    python3 scripts/bootstrap_athena.py

동작:
    1. S3 버킷 존재 확인 (없으면 안내 출력 + 종료 — 인프라는 사람이 만든다)
    2. Glue Database 생성 (이미 있으면 skip)
    3. Athena Workgroup 생성 (결과 위치 = s3://<bucket>/athena-results/)
    4. sql/ddl/*.sql 의 ${BUCKET} 치환 후 순차 실행 (CREATE TABLE IF NOT EXISTS)

전제:
    - AWS 자격증명이 환경 변수 또는 ~/.aws/credentials 에 있어야 함
    - IAM 정책: GlueFullAccess, AthenaFullAccess, S3 ListBucket on target bucket
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
BUCKET = os.environ.get("AWS_S3_BUCKET", "oi-data-lake")
DATABASE = os.environ.get("GLUE_DATABASE", os.environ.get("OI_GLUE_DATABASE", "oi"))
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", os.environ.get("OI_ATHENA_WORKGROUP", "oi-workgroup"))
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"

DDL_DIR = Path(__file__).parent.parent / "sql" / "ddl"

# DDL 실행 순서 — 의존 없음(모두 EXTERNAL TABLE)이지만 출력 가독성 위해 그룹핑
DDL_FILES = [
    "bronze_archive.sql",
    "bronze_live.sql",
    "silver_events.sql",
    "gold_tables.sql",          # gold_repo_daily
    "gold_actor_daily.sql",
    "gold_repo_acceleration.sql",
    "gold_repo_anomaly.sql",
    "gold_repo_hourly.sql",
    "gold_language_activity.sql",
    "gold_repo_enriched.sql",
]


def log(msg: str) -> None:
    print(f"[bootstrap] {msg}", flush=True)


# ─── S3 ────────────────────────────────────────────────
def check_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=BUCKET)
        log(f"S3 OK: s3://{BUCKET}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            log(f"❌ S3 bucket '{BUCKET}' 가 존재하지 않습니다.")
            log("   콘솔 또는 CLI 로 먼저 만드세요:")
            log(f"     aws s3 mb s3://{BUCKET} --region {REGION}")
            sys.exit(1)
        if code in ("403", "AccessDenied"):
            log(f"⚠️  bucket head 가 403 입니다 — 권한은 부족하지만 IAM Role 운영 환경에선 OK 일 수 있어 진행.")
            return
        raise


# ─── Glue Database ─────────────────────────────────────
def ensure_glue_db(glue) -> None:
    try:
        glue.create_database(DatabaseInput={"Name": DATABASE, "Description": "Opensource Insights data lake"})
        log(f"Glue DB created: {DATABASE}")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "AlreadyExistsException":
            log(f"Glue DB already exists: {DATABASE}")
        else:
            raise


# ─── Athena Workgroup ──────────────────────────────────
def ensure_workgroup(athena) -> None:
    try:
        athena.create_work_group(
            Name=WORKGROUP,
            Description="OI primary workgroup",
            Configuration={
                "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": False,
                "BytesScannedCutoffPerQuery": 10 * 1024 * 1024 * 1024,  # 10GB cap (cost guard)
            },
        )
        log(f"Athena workgroup created: {WORKGROUP}")
    except ClientError as e:
        msg = str(e)
        if "already exists" in msg.lower() or e.response.get("Error", {}).get("Code") == "InvalidRequestException":
            log(f"Athena workgroup already exists: {WORKGROUP}")
        else:
            raise


# ─── Athena DDL 실행 ───────────────────────────────────
def run_athena_query(athena, sql: str) -> None:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = info["Status"].get("StateChangeReason", "")
            # IF NOT EXISTS 인데 이미 존재한다는 류의 에러는 무시
            if "AlreadyExistsException" in reason or "already exists" in reason.lower():
                return
            raise RuntimeError(f"Athena {state}: {reason}\nSQL preview: {sql[:200]}")
        time.sleep(0.5)


def apply_ddl(athena, fname: str) -> None:
    path = DDL_DIR / fname
    if not path.exists():
        log(f"  skip {fname} (not found)")
        return
    sql = path.read_text(encoding="utf-8")
    sql = sql.replace("${BUCKET}", BUCKET)
    # Athena 는 한 번에 한 statement 만 받음. 우리 DDL 은 모두 단일 statement.
    sql_clean = sql.strip().rstrip(";").strip()
    log(f"  applying {fname} ...")
    run_athena_query(athena, sql_clean)


# ─── Main ──────────────────────────────────────────────
def main() -> None:
    log(f"region={REGION} bucket={BUCKET} db={DATABASE} workgroup={WORKGROUP}")

    s3 = boto3.client("s3", region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)
    athena = boto3.client("athena", region_name=REGION)

    check_bucket(s3)
    ensure_glue_db(glue)
    ensure_workgroup(athena)

    log("Applying DDLs ...")
    for fname in DDL_FILES:
        apply_ddl(athena, fname)

    log("✓ bootstrap complete.")


if __name__ == "__main__":
    main()
