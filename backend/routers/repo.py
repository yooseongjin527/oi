"""
routers/repo.py
Repo 프로필 페이지 + JSON API.

다른 라우터(pages, insights)와 동일하게 router 단일 export 패턴 사용.
- GET /repo/{owner}/{name}              → Jinja2 HTML 페이지
- GET /api/repo/{owner}/{name}/profile  → JSON (페이지에서 fetch)
"""
import re
import logging
import asyncio

from fastapi import APIRouter, Depends, Request, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import get_optional_user
from services import repo_service

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


# ─── 입력 검증 ─────────────────────────────────────────
# repo 이름은 owner/name 형식만 허용 — SQL injection 방지
_REPO_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate(owner: str, name: str, date: str) -> tuple[str, str]:
    """입력 검증 — 실패 시 HTTPException(400)"""
    repo_name = f"{owner}/{name}"
    if not _REPO_NAME_RE.match(repo_name):
        raise HTTPException(status_code=400, detail=f"Invalid repo name: {repo_name}")
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}")
    return repo_name, date


# ─── JSON API ─────────────────────────────────────────

@router.get("/api/repo/{owner}/{name}/profile")
async def get_profile(
    owner: str,
    name: str,
    date: str = Query("2026-04-29", description="YYYY-MM-DD"),
):
    """Repo 프로필 JSON — 24h 시계열 + 메트릭 + 그날의 인사이트 섹션"""
    repo_name, date = _validate(owner, name, date)
    try:
        # Athena/OpenSearch 호출은 blocking → to_thread 로 감쌈
        result = await asyncio.to_thread(
            repo_service.get_repo_profile, repo_name, date
        )
    except Exception as e:
        logger.exception("repo_profile failed")
        raise HTTPException(status_code=500, detail=f"Profile query failed: {e}")
    return result


# ─── 페이지 (HTML) ────────────────────────────────────

@router.get("/repo/{owner}/{name}", response_class=HTMLResponse)
def repo_page(
    request: Request,
    owner: str,
    name: str,
    date: str = Query("2026-04-29"),
    user=Depends(get_optional_user),
):
    """Repo 프로필 HTML — JS 가 /api/repo/.../profile 호출해서 데이터 채움"""
    repo_name, date = _validate(owner, name, date)
    return templates.TemplateResponse(
        "repo_profile.html",
        {
            "request": request,
            "user": user,
            "title": f"{repo_name} · OI",
            "repo_name": repo_name,
            "owner": owner,
            "name": name,
            "date": date,
        },
    )