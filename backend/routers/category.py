# backend/routers/category.py
"""F5 카테고리 분류 트리거 API.

수동 실행용 — Streamlit 운영자 콘솔에서 버튼으로 호출하거나,
Airflow DAG 에서 HTTP 호출로 트리거 가능.

관리자 (role=admin) 만 트리거 가능. Airflow 에서 호출할 경우 admin 계정으로
JWT 발급받아 쿠키에 실어 호출하거나, 별도 service token 도입 (Day 7+).
"""
import logging
import asyncio
import re

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import require_admin
from models import User
from services import category_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/category", tags=["category"])

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@router.post("/run")
async def run_categorization(
    date: str = Query(..., description="YYYY-MM-DD"),
    force: bool = Query(False, description="True 면 이미 분류된 repo 재분류"),
    admin: User = Depends(require_admin),
):
    """특정 날짜의 카테고리 분류 배치 실행 (admin 전용).

    - top-10 repo 직렬 처리 → ~10~20초 소요
    - 멱등성: force=False (기본) 면 이미 분류된 repo 스킵
    """
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}")

    try:
        # Bedrock 호출 직렬이라 blocking → to_thread
        result = await asyncio.to_thread(
            category_service.categorize_daily, date, force,
        )
    except Exception as e:
        logger.exception("categorize_daily failed admin=%s", admin.username)
        raise HTTPException(status_code=500, detail=f"Batch failed: {e}")

    return result
