"""
services/athena_client.py
Athena 쿼리 실행 헬퍼. boto3 기반.

scripts/run_validation_queries.sh 의 run_query / wait_query / get_results 를
Python 으로 1:1 포팅. 일반 사용은 query() 동기 함수 1개로 충분.

설계 노트:
- 모든 결과값은 string 으로 옴 (Athena 특성). 숫자 변환은 호출자 책임.
- 컬럼명은 ResultSetMetadata 에서 직접 추출 (첫 row 헤더 의존 X).
"""
import os
import time
import logging
from typing import Optional

import boto3

logger = logging.getLogger(__name__)

# 환경변수 기본값 — .envrc 에서 export 되어 있어야 함
_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
_DATABASE = os.environ.get("OI_GLUE_DATABASE", "oi")
_WORKGROUP = os.environ.get("OI_ATHENA_WORKGROUP", "oi-workgroup")
_OUTPUT = os.environ.get("OI_ATHENA_OUTPUT", "s3://oi-data-lake/athena-results/")

# boto3 클라이언트는 모듈 로드 시 1회만 생성 (재사용)
_client = boto3.client("athena", region_name=_REGION)


class AthenaQueryError(RuntimeError):
    """Athena 쿼리 실패 시 raise. state 와 reason 보관."""
    def __init__(self, qid: str, state: str, reason: str):
        self.qid = qid
        self.state = state
        self.reason = reason
        super().__init__(f"Athena query {qid} {state}: {reason}")


def query_async(sql: str) -> str:
    """
    SQL 비동기 실행. QueryExecutionId(qid) 만 반환.
    polling 은 호출자가 wait() 로.
    """
    resp = _client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": _DATABASE},
        WorkGroup=_WORKGROUP,
        ResultConfiguration={"OutputLocation": _OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    logger.info("athena.start qid=%s", qid)
    return qid


def wait(qid: str, timeout_sec: int = 60, poll_interval: float = 0.5) -> dict:
    """
    qid 가 SUCCEEDED 될 때까지 polling. 실패/타임아웃 시 AthenaQueryError raise.
    return: 마지막 get_query_execution 응답의 QueryExecution dict
    """
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = _client.get_query_execution(QueryExecutionId=qid)
        qexec = resp["QueryExecution"]
        state = qexec["Status"]["State"]
        if state == "SUCCEEDED":
            return qexec
        if state in ("FAILED", "CANCELLED"):
            reason = qexec["Status"].get("StateChangeReason", "(no reason)")
            raise AthenaQueryError(qid, state, reason)
        time.sleep(poll_interval)
    raise AthenaQueryError(qid, "TIMEOUT", f"exceeded {timeout_sec}s")


def fetch_results(qid: str, max_rows: int = 1000) -> list:
    """
    SUCCEEDED 된 qid 의 결과를 list[dict] 로 반환.
    컬럼명은 ResultSetMetadata 에서 추출 (첫 row 헤더 의존 X).
    모든 값은 string. 숫자 변환은 호출자 책임.
    """
    paginator = _client.get_paginator("get_query_results")
    columns: Optional[list] = None
    rows: list = []
    is_first_page = True

    for page in paginator.paginate(QueryExecutionId=qid):
        # 첫 페이지에서 컬럼명 추출
        if columns is None:
            meta = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            columns = [c["Name"] for c in meta]

        for r in page["ResultSet"]["Rows"]:
            data = [c.get("VarCharValue", None) for c in r["Data"]]
            # 첫 페이지의 첫 row 는 컬럼 헤더 (Athena 특성) — skip
            if is_first_page:
                is_first_page = False
                continue
            rows.append(data)
            if len(rows) >= max_rows:
                break
        if len(rows) >= max_rows:
            break

    if columns is None:
        return []
    return [dict(zip(columns, row)) for row in rows]


def query(sql: str, timeout_sec: int = 60, max_rows: int = 1000) -> list:
    """
    동기 실행 = start + wait + fetch 합친 헬퍼.
    인사이트 서비스에서 일반적으로 이 함수 1개만 쓰면 됨.
    """
    qid = query_async(sql)
    wait(qid, timeout_sec=timeout_sec)
    return fetch_results(qid, max_rows=max_rows)


def get_stats(qid: str) -> dict:
    """
    실행된 쿼리의 메타 (스캔량/실행시간) 반환. 발표 슬라이드/로그용.
    """
    resp = _client.get_query_execution(QueryExecutionId=qid)
    qexec = resp["QueryExecution"]
    stats = qexec.get("Statistics", {})
    return {
        "scan_bytes": stats.get("DataScannedInBytes", 0),
        "exec_ms": stats.get("EngineExecutionTimeInMillis", 0),
        "total_ms": stats.get("TotalExecutionTimeInMillis", 0),
        "state": qexec["Status"]["State"],
    }
