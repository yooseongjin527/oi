"""
routers/insights.py
GET /api/insights/daily?date=YYYY-MM-DD
GET /api/insights/dates                — 색인된 사용 가능 날짜 목록

동기 함수(get_daily_insights)를 asyncio.to_thread 로 감싸서
FastAPI 이벤트 루프 블로킹 방지.

승인된 사용자만 접근 가능 (get_current_user 의존성).
"""
import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query, HTTPException

from auth import get_current_user
from models import User
from services import opensearch_client
from services.insights_service import get_daily_insights

logger = logging.getLogger(__name__)
router = APIRouter()

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _default_date() -> str:
    """기본 분석 날짜 = 어제 UTC."""
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


@router.get("/api/insights/daily")
async def daily_insights(
    date: str | None = Query(None, description="조회 날짜 (YYYY-MM-DD, 기본: 어제 UTC)"),
    user: User = Depends(get_current_user),
):
    """
    Gold 마트 top 10 repo Bedrock 인사이트 카드 반환.
    Athena + Bedrock 합산 약 5~10초 소요.

    인증: 승인된 사용자만. 비로그인/미승인은 401/403.
    """
    if date is None or date == "":
        date = _default_date()
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD 이어야 합니다.")

    try:
        # 동기 함수를 스레드풀에서 실행 — 이벤트 루프 블로킹 방지
        result = await asyncio.to_thread(get_daily_insights, date)
        return result
    except Exception as e:
        logger.exception("insights endpoint error date=%s user=%s", date, user.username)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/insights/dates")
async def available_dates(
    size: int = Query(30, ge=1, le=180, description="최대 반환 날짜 개수"),
    user: User = Depends(get_current_user),
):
    """OpenSearch 에 색인된 분석 날짜 목록 (최신순). 대시보드 picker 채움용."""
    try:
        dates = await asyncio.to_thread(opensearch_client.list_dates, size)
    except Exception as e:
        logger.warning("insights/dates failed (returning empty): %s", e)
        dates = []
    return {"dates": dates, "default": _default_date()}
