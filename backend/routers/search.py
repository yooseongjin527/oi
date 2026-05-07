# backend/routers/search.py
"""검색 API — OpenSearch full-text 기반 repo 검색.

승인된 사용자만 접근 가능 (get_current_user 의존성).
"""
from fastapi import APIRouter, Depends, Query, HTTPException

from auth import get_current_user
from models import User
from services import opensearch_client

router = APIRouter(prefix="/api/search", tags=["search"])


@router.get("")
def search_repos(
    q: str = Query(..., min_length=1, description="검색어"),
    size: int = Query(10, ge=1, le=50, description="결과 개수"),
    user: User = Depends(get_current_user),
):
    """
    Full-text 검색 — repo 이름 또는 인사이트 본문 매칭.
    예: /api/search?q=AI+agent&size=10
    """
    try:
        result = opensearch_client.search(query=q, size=size)
    except Exception as e:
        # OpenSearch 다운 시 500 — 인사이트 카드(Athena+Bedrock)는 영향 없음
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")
    return result


@router.get("/by-date")
def search_by_date(
    date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
    size: int = Query(10, ge=1, le=50),
    user: User = Depends(get_current_user),
):
    """특정 날짜의 인덱싱된 repo 목록 — 히스토리 조회용"""
    try:
        result = opensearch_client.get_by_date(date=date, size=size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    return result
