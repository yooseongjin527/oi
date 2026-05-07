"""Athena 조회 헬퍼 — backend/services/athena_client.py 의 운영 콘솔용 미니 버전.
캐싱 강화 + pandas DataFrame 반환 (Streamlit 친화적).
"""
import os
import time
import logging
from typing import Any

import boto3
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


def _get_athena_client():
    """boto3 Athena 클라이언트 — 환경변수에서 자격증명 자동 로드"""
    return boto3.client(
        "athena",
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"),
    )


def _wait_for_query(client, query_id: str, timeout_sec: int = 90) -> None:
    """쿼리 완료 대기 — polling 방식 (1초 간격)"""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = client.get_query_execution(QueryExecutionId=query_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(1)
    raise TimeoutError(f"Athena query timeout after {timeout_sec}s")


def _fetch_results(client, query_id: str) -> pd.DataFrame:
    """get_query_results 페이지네이션 → DataFrame 변환"""
    paginator = client.get_paginator("get_query_results")
    rows: list[list[str]] = []
    columns: list[str] = []
    for page in paginator.paginate(QueryExecutionId=query_id):
        result_set = page["ResultSet"]
        if not columns:
            # 첫 페이지 첫 행은 헤더
            columns = [c["Name"] for c in result_set["ResultSetMetadata"]["ColumnInfo"]]
            data_rows = result_set["Rows"][1:]
        else:
            data_rows = result_set["Rows"]
        for r in data_rows:
            rows.append([col.get("VarCharValue") for col in r["Data"]])
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql: str, timeout_sec: int = 90) -> pd.DataFrame:
    """
    Athena SQL 실행 → DataFrame.
    Streamlit cache_data 로 1시간 캐싱 (발표 데이터는 정적이라 충분).
    """
    client = _get_athena_client()
    workgroup = os.getenv("OI_ATHENA_WORKGROUP", "oi-workgroup")
    output = os.getenv("OI_ATHENA_OUTPUT", "s3://oi-data-lake/athena-results/")
    database = os.getenv("OI_GLUE_DATABASE", "oi")

    start = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output},
    )
    query_id = start["QueryExecutionId"]
    logger.info("Athena query started id=%s", query_id)

    _wait_for_query(client, query_id, timeout_sec)
    return _fetch_results(client, query_id)


# ─── 미리 정의된 자주 쓰는 쿼리 ──────────────────────────

TOP10_SQL = """
WITH ranked AS (
  SELECT
    d.repo_id, d.repo_name, d.event_count, d.dominant_event_type,
    a.acceleration_ratio, a.prev_event_count,
    n.anomaly_score, n.watch_ratio, n.watch_zscore,
    0.4 * LEAST(COALESCE(a.acceleration_ratio, 0), 10.0)
    + 0.4 * COALESCE(n.anomaly_score, 0)
    + 0.2 * LN(d.event_count + 1) AS rank_score
  FROM oi.gold_repo_daily d
  LEFT JOIN oi.gold_repo_acceleration a
    ON d.repo_id = a.repo_id AND d.year=a.year AND d.month=a.month AND d.day=a.day
  LEFT JOIN oi.gold_repo_anomaly n
    ON d.repo_id = n.repo_id AND d.year=n.year AND d.month=n.month AND d.day=n.day
  WHERE d.year='{year}' AND d.month='{month}' AND d.day='{day}'
    AND d.event_count >= 50
    AND COALESCE(a.prev_event_count, 0) >= 10
    AND COALESCE(n.watch_zscore, -999) > 0
)
SELECT * FROM ranked
ORDER BY rank_score DESC
LIMIT {limit}
"""


def get_top_repos(date: str, limit: int = 10) -> pd.DataFrame:
    """편의 함수 — 특정 날짜의 top-N repo 조회"""
    year, month, day = date.split("-")
    sql = TOP10_SQL.format(year=year, month=month, day=day, limit=limit)
    df = run_query(sql)
    # 숫자 컬럼 변환 (Athena 결과는 모두 string)
    numeric_cols = [
        "event_count", "acceleration_ratio", "prev_event_count",
        "anomaly_score", "watch_ratio", "watch_zscore", "rank_score",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


AVAILABLE_DATES_SQL = """
SELECT DISTINCT
  CONCAT(year, '-', month, '-', day) AS date_str
FROM oi.gold_repo_daily
ORDER BY date_str DESC
LIMIT 30
"""


@st.cache_data(ttl=3600, show_spinner=False)
def get_available_dates() -> list[str]:
    """Gold 마트에 데이터가 있는 날짜 목록 — 사이드바 selectbox 용"""
    df = run_query(AVAILABLE_DATES_SQL)
    return df["date_str"].tolist() if not df.empty else []