"""
routers/insights.py
GET /api/insights/daily?date=YYYY-MM-DD

동기 함수(get_daily_insights)를 asyncio.to_thread 로 감싸서
FastAPI 이벤트 루프 블로킹 방지.

승인된 사용자만 접근 가능 (get_current_user 의존성).
"""
import asyncio
import logging
from fastapi import APIRouter, Depends, Query, HTTPException

from auth import get_current_user
from models import User
from services.insights_service import get_daily_insights

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/insights/daily")
async def daily_insights(
    date: str = Query(..., description="조회 날짜 (YYYY-MM-DD)", example="2026-04-29"),
    user: User = Depends(get_current_user),
):
    """
    Gold 마트 top 10 repo Bedrock 인사이트 카드 반환.
    Athena + Bedrock 합산 약 5~10초 소요.

    인증: 승인된 사용자만. 비로그인/미승인은 401/403.
    """
    # 날짜 형식 기본 검증
    try:
        parts = date.split("-")
        assert len(parts) == 3 and len(parts[0]) == 4
    except Exception:
        raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD 이어야 합니다.")

    try:
        # 동기 함수를 스레드풀에서 실행 — 이벤트 루프 블로킹 방지
        result = await asyncio.to_thread(get_daily_insights, date)
        return result
    except Exception as e:
        logger.exception("insights endpoint error date=%s user=%s", date, user.username)
        raise HTTPException(status_code=500, detail=str(e))
