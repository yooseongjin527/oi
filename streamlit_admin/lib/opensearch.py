"""OpenSearch 조회 헬퍼 — backend/services/opensearch_client.py 의 read-only 미러.
운영 콘솔은 검색/조회만 하고 인덱싱은 하지 않음 (인덱싱은 fastapi 책임).
"""
import os
import logging
from typing import Any

import streamlit as st
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import NotFoundError

logger = logging.getLogger(__name__)

INDEX_NAME = "oi-repo-daily"


def _get_client() -> OpenSearch:
    host_url = os.getenv("OI_OPENSEARCH_HOST", "http://opensearch:9200")
    use_ssl = host_url.startswith("https://")
    host_part = host_url.replace("https://", "").replace("http://", "")
    if ":" in host_part:
        host, port_str = host_part.rsplit(":", 1)
        port = int(port_str)
    else:
        host, port = host_part, 9200
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_compress=True,
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
        timeout=30,
    )


@st.cache_data(ttl=300, show_spinner=False)
def search(query: str, size: int = 10) -> dict[str, Any]:
    """Full-text 검색. 5분 캐싱 (반복 검색 비용 절감)"""
    client = _get_client()
    body = {
        "size": size,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["repo_name_text^2", "insight_markdown"],
                "type": "best_fields",
                "operator": "and",
            }
        },
        "sort": ["_score", {"date": {"order": "desc"}}],
    }
    try:
        resp = client.search(index=INDEX_NAME, body=body)
    except NotFoundError:
        return {"total": 0, "hits": []}
    return {
        "total": resp["hits"]["total"]["value"],
        "hits": [{"_score": h["_score"], **h["_source"]} for h in resp["hits"]["hits"]],
    }


@st.cache_data(ttl=300, show_spinner=False)
def get_by_date(date: str, size: int = 50) -> dict[str, Any]:
    """특정 날짜의 인덱싱된 모든 repo — 히스토리 페이지용"""
    client = _get_client()
    body = {
        "size": size,
        "query": {"term": {"date": date}},
        "sort": [{"rank_score": {"order": "desc"}}],
    }
    try:
        resp = client.search(index=INDEX_NAME, body=body)
    except NotFoundError:
        return {"total": 0, "hits": []}
    return {
        "total": resp["hits"]["total"]["value"],
        "hits": [{"_score": h["_score"], **h["_source"]} for h in resp["hits"]["hits"]],
    }


@st.cache_data(ttl=60, show_spinner=False)
def get_indexed_dates() -> list[str]:
    """인덱싱된 날짜 목록 — date 필드 distinct aggregation"""
    client = _get_client()
    body = {
        "size": 0,
        "aggs": {
            "dates": {
                "terms": {"field": "date", "size": 100, "order": {"_key": "desc"}}
            }
        },
    }
    try:
        resp = client.search(index=INDEX_NAME, body=body)
    except NotFoundError:
        return []
    buckets = resp.get("aggregations", {}).get("dates", {}).get("buckets", [])
    return [b["key"] for b in buckets]


@st.cache_data(ttl=60, show_spinner=False)
def get_index_stats() -> dict[str, Any]:
    """인덱스 통계 — Streamlit에서 헤더 메트릭으로 표시"""
    client = _get_client()
    try:
        stats = client.indices.stats(index=INDEX_NAME)
        primaries = stats["indices"][INDEX_NAME]["primaries"]
        return {
            "doc_count": primaries["docs"]["count"],
            "size_bytes": primaries["store"]["size_in_bytes"],
        }
    except (NotFoundError, KeyError):
        return {"doc_count": 0, "size_bytes": 0}