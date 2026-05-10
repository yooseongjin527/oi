# backend/services/opensearch_client.py
"""
OpenSearch 클라이언트 — 일별 repo 인사이트 인덱싱 및 검색
- 인덱스: oi-repo-daily
- doc_id 패턴: {date}_{repo_name}  (upsert, 동일 일자 재실행 시 덮어쓰기)
- 환경변수: OI_OPENSEARCH_HOST (기본값: http://opensearch:9200)
"""
import os
import logging
from typing import Iterable, Any
from datetime import datetime

from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk
from opensearchpy.exceptions import NotFoundError, RequestError

logger = logging.getLogger(__name__)

# ─── 상수 정의 ─────────────────────────────────────────────
INDEX_NAME = "oi-repo-daily"

# 인덱스 매핑 정의 (Day 6 핸드오프 스펙 기준)
# - keyword: 정확 매치 / aggregation 용 (repo_name, dominant_event_type 등)
# - text: full-text 검색용 (repo_name_text, insight_markdown)
# - 숫자 타입은 dashboards에서 시각화 용이하도록 명시
INDEX_MAPPING: dict[str, Any] = {
    "settings": {
        "number_of_shards": 1,        # 단일 노드라 1샤드면 충분
        "number_of_replicas": 0,      # 단일 노드 환경에서 replica=1이면 yellow 됨
        "analysis": {
            "analyzer": {
                # repo_name용 — 표준 토크나이저로 / - _ 분리 + 소문자화
                "repo_name_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "date":                {"type": "keyword"},
            "repo_id":             {"type": "keyword"},  # Athena가 string으로 주므로 keyword
            "repo_name":           {"type": "keyword"},
            "repo_name_text":      {"type": "text", "analyzer": "repo_name_analyzer"},
            "event_count":         {"type": "integer"},
            "acceleration_ratio":  {"type": "float"},
            "anomaly_score":       {"type": "float"},
            "watch_zscore":        {"type": "float"},
            "watch_ratio":         {"type": "float"},
            "rank_score":          {"type": "float"},
            "dominant_event_type": {"type": "keyword"},
            "insight_markdown":    {"type": "text"},
            "indexed_at":          {"type": "date"},
            # ─── F5 카테고리 분류 (Day 6) ─────────────────
            # category: 메인 페이지 그룹핑 / 필터링용 (keyword 정확 매치)
            # confidence: 0.0~1.0 float, 운영자 콘솔에서 낮은 값 검토용
            # reasoning: 디버깅용 한 줄 설명 (text)
            # categorized_at: 분류 실행 시각, 재분류 여부 판단용
            "category":            {"type": "keyword"},
            "category_confidence": {"type": "float"},
            "category_reasoning":  {"type": "text"},
            "categorized_at":      {"type": "date"}
        }
    }
}


# ─── 타입 변환 헬퍼 ────────────────────────────────────────
# Athena 결과는 모든 컬럼이 string 으로 반환됨 (boto3 get_query_results 특성).
# OpenSearch 매핑에 integer/float 로 박혀 있으므로 인덱싱 전 변환 필요.
# None / 빈 문자열 / 변환 실패는 모두 None 반환 → OpenSearch 가 missing 으로 처리.

def _safe_float(v: Any) -> float | None:
    """str/None/빈 문자열 → float 안전 변환"""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_int(v: Any) -> int | None:
    """str/None/빈 문자열 → int 안전 변환. '50.0' 같은 케이스도 허용."""
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


# ─── 클라이언트 ────────────────────────────────────────────

def _get_client() -> OpenSearch:
    """OpenSearch 클라이언트 — 환경변수 기반 host 파싱"""
    host_url = os.getenv("OI_OPENSEARCH_HOST", "http://opensearch:9200")

    # http://host:port 또는 host:port 둘 다 허용
    # opensearch-py는 dict 형태의 hosts 받음
    if host_url.startswith("http://") or host_url.startswith("https://"):
        use_ssl = host_url.startswith("https://")
        host_part = host_url.replace("https://", "").replace("http://", "")
    else:
        use_ssl = False
        host_part = host_url

    if ":" in host_part:
        host, port_str = host_part.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_part
        port = 9200

    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_compress=True,
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
        timeout=30,
    )


def ensure_index() -> None:
    """인덱스 멱등 생성 — 이미 있으면 스킵"""
    client = _get_client()
    if client.indices.exists(index=INDEX_NAME):
        logger.debug("Index %s already exists, skipping creation", INDEX_NAME)
        return
    try:
        client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
        logger.info("Created index %s", INDEX_NAME)
    except RequestError as e:
        # 동시 생성 race condition — 이미 존재하면 무시
        if "resource_already_exists_exception" in str(e):
            logger.info("Index %s created concurrently, skipping", INDEX_NAME)
        else:
            raise


def _build_doc_id(date: str, repo_name: str) -> str:
    """doc_id 패턴: {date}_{repo_name} — repo_name 의 / 는 _ 로 치환"""
    safe_repo = repo_name.replace("/", "_")
    return f"{date}_{safe_repo}"


# ─── 인덱싱 ────────────────────────────────────────────────

def index_daily(
    date: str,
    rows: list[dict[str, Any]],
    insight_markdown: str | None = None,
) -> dict[str, int]:
    """
    Gold 마트 top-N + 인사이트 마크다운을 OpenSearch에 bulk index.

    Args:
        date: YYYY-MM-DD 형식
        rows: athena_client.query() 결과 (dict 리스트, 모든 값 string).
              각 row는 repo_name, event_count, rank_score 등 포함
        insight_markdown: 그날의 통합 인사이트 (선택). 모든 row에 동일값 저장.

    Returns:
        {"indexed": N, "errors": M}
    """
    ensure_index()
    client = _get_client()

    now_iso = datetime.utcnow().isoformat() + "Z"

    def _gen_actions() -> Iterable[dict[str, Any]]:
        for row in rows:
            repo_name = row.get("repo_name", "")
            # Athena → OpenSearch 타입 변환 (string → int/float)
            doc = {
                "date": date,
                "repo_id": str(row["repo_id"]) if row.get("repo_id") else None,
                "repo_name": repo_name,
                "repo_name_text": repo_name,                              # text 검색용 복제 필드
                "event_count":        _safe_int(row.get("event_count")),
                "acceleration_ratio": _safe_float(row.get("acceleration_ratio")),
                "anomaly_score":      _safe_float(row.get("anomaly_score")),
                "watch_zscore":       _safe_float(row.get("watch_zscore")),
                "watch_ratio":        _safe_float(row.get("watch_ratio")),
                "rank_score":         _safe_float(row.get("rank_score")),
                "dominant_event_type": row.get("dominant_event_type"),
                "insight_markdown":   insight_markdown,
                "indexed_at":         now_iso,
            }
            # 카테고리 필드 — insights_service 가 inline batch 분류한 결과를 row 에
            # 채워주면 같이 색인. 없으면 기존 OpenSearch 의 카테고리 그대로 유지.
            if row.get("category"):
                doc["category"] = row["category"]
                doc["category_confidence"] = _safe_float(row.get("category_confidence"))
                if row.get("category_reasoning"):
                    doc["category_reasoning"] = row["category_reasoning"]
                doc["categorized_at"] = now_iso
            yield {
                "_op_type": "update",  # upsert 의미 (동일 _id면 덮어쓰기)
                "_index": INDEX_NAME,
                "_id": _build_doc_id(date, repo_name),
                "doc": doc,                                 # ← _source → doc
                "doc_as_upsert": True,                      # ← 새 doc 이면 insert
            }

    success, errors = bulk(client, _gen_actions(), raise_on_error=False)
    error_count = len(errors) if isinstance(errors, list) else 0
    if error_count > 0:
        logger.warning("Bulk index errors (first 3): %s", errors[:3])
    logger.info("Indexed %d docs into %s for date=%s", success, INDEX_NAME, date)
    return {"indexed": success, "errors": error_count}


# ─── 단일 repo 인사이트 캐시 (Repo 상세 페이지) ────────────

def get_repo_insight(date: str, repo_name: str) -> str | None:
    """특정 날짜·repo 의 캐시된 단일 인사이트 마크다운 조회.

    Returns: markdown string 또는 None (doc 자체가 없거나 필드가 없을 때)
    """
    client = _get_client()
    doc_id = _build_doc_id(date, repo_name)
    try:
        resp = client.get(index=INDEX_NAME, id=doc_id, _source_includes=["repo_insight_markdown"])
        return resp.get("_source", {}).get("repo_insight_markdown")
    except NotFoundError:
        return None
    except Exception as e:
        logger.warning("get_repo_insight failed (%s): %s", doc_id, e)
        return None


def cache_repo_insight(date: str, repo_name: str, markdown: str) -> bool:
    """특정 날짜·repo 의 단일 인사이트 마크다운을 OpenSearch 에 캐시.

    - doc_as_upsert=True 라 doc 이 없어도 새로 만들어서 저장
    - 기존 다른 필드 (event_count, category 등) 는 건드리지 않음 (partial update)
    """
    client = _get_client()
    doc_id = _build_doc_id(date, repo_name)
    now_iso = datetime.utcnow().isoformat() + "Z"
    try:
        client.update(
            index=INDEX_NAME,
            id=doc_id,
            body={
                "doc": {
                    "date": date,
                    "repo_name": repo_name,
                    "repo_insight_markdown": markdown,
                    "repo_insight_generated_at": now_iso,
                },
                "doc_as_upsert": True,
            },
        )
        return True
    except Exception as e:
        logger.warning("cache_repo_insight failed (%s): %s", doc_id, e)
        return False


# ─── 검색 ──────────────────────────────────────────────────

def search(query: str, size: int = 10) -> dict[str, Any]:
    """
    Full-text 검색 — repo_name_text + insight_markdown multi-match.

    Args:
        query: 검색어 (예: "AI agent", "rust terminal")
        size: 결과 개수

    Returns:
        {"total": N, "hits": [{...}, ...]}
    """
    ensure_index()
    client = _get_client()

    # multi_match — repo_name_text 는 가중치 2배 (이름 매칭이 본문보다 더 중요)
    # operator=and 라 검색어 모든 토큰이 매칭돼야 함 (정밀도 우선)
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
        "sort": [
            "_score",
            {"date": {"order": "desc"}}
        ],
    }

    try:
        resp = client.search(index=INDEX_NAME, body=body)
    except NotFoundError:
        return {"total": 0, "hits": []}

    total = resp["hits"]["total"]["value"]
    hits = [
        {
            "_score": h["_score"],
            **h["_source"],
        }
        for h in resp["hits"]["hits"]
    ]
    return {"total": total, "hits": hits}


def list_dates(size: int = 30) -> list[str]:
    """색인된 모든 date 값 (YYYY-MM-DD) 의 distinct 목록을 최신순으로 반환.

    대시보드 / repo 페이지에서 사용 가능한 분석 날짜 picker 채울 때 사용.
    인덱스가 없거나 비었으면 빈 리스트.
    """
    ensure_index()
    client = _get_client()
    body = {
        "size": 0,
        "aggs": {
            "dates": {
                "terms": {
                    "field": "date",
                    "size": size,
                    "order": {"_key": "desc"},
                }
            }
        },
    }
    try:
        resp = client.search(index=INDEX_NAME, body=body)
    except NotFoundError:
        return []
    buckets = resp.get("aggregations", {}).get("dates", {}).get("buckets", [])
    return [b["key"] for b in buckets if b.get("key")]


def get_by_date(date: str, size: int = 10) -> dict[str, Any]:
    """특정 날짜의 모든 repo 조회 — Streamlit 히스토리 페이지용"""
    ensure_index()
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