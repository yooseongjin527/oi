"""
routers/repo.py
Repo 프로필 페이지 + JSON API.

다른 라우터(pages, insights)와 동일하게 router 단일 export 패턴 사용.
- GET /repo/{owner}/{name}              → Jinja2 HTML 페이지 (승인된 사용자만, 미승인 시 redirect)
- GET /api/repo/{owner}/{name}/profile  → JSON (승인된 사용자만, 미승인 시 401/403)
"""
import re
import logging
import asyncio
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, require_approved_user_page
from models import User
from services import repo_service

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


# ─── 입력 검증 ─────────────────────────────────────────
# repo 이름은 owner/name 형식만 허용 — SQL injection 방지
_REPO_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _default_date() -> str:
    """기본 분석 날짜 = 어제 UTC.

    매일 02:00 UTC 에 silver_to_gold 가 어제 분 빌드를 마치므로,
    오늘이 아닌 어제를 default 로 잡으면 항상 데이터가 있을 가능성이 높다.
    """
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def _resolve_date(date: str | None) -> str:
    return date or _default_date()


def _validate(owner: str, name: str, date: str) -> tuple[str, str]:
    """입력 검증 — 실패 시 HTTPException(400)"""
    repo_name = f"{owner}/{name}"
    if not _REPO_NAME_RE.match(repo_name):
        raise HTTPException(status_code=400, detail=f"Invalid repo name: {repo_name}")
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}")
    return repo_name, date


# ─── JSON API (인증 필수) ─────────────────────────────

@router.get("/api/repo/{owner}/{name}/profile")
async def get_profile(
    owner: str,
    name: str,
    date: str | None = Query(None, description="YYYY-MM-DD (기본: 어제 UTC)"),
    user: User = Depends(get_current_user),
):
    """Repo 프로필 JSON — 24h 시계열 + 메트릭 + 그날의 인사이트 섹션"""
    repo_name, date = _validate(owner, name, _resolve_date(date))
    try:
        # Athena/OpenSearch 호출은 blocking → to_thread 로 감쌈
        result = await asyncio.to_thread(
            repo_service.get_repo_profile, repo_name, date
        )
    except Exception as e:
        logger.exception("repo_profile failed user=%s repo=%s", user.username, repo_name)
        raise HTTPException(status_code=500, detail=f"Profile query failed: {e}")
    return result


# ─── 페이지 (HTML, 인증 필수) ─────────────────────────

@router.get("/repo/{owner}/{name}", response_class=HTMLResponse)
def repo_page(
    request: Request,
    owner: str,
    name: str,
    date: str | None = Query(None, description="YYYY-MM-DD (기본: 어제 UTC)"),
    user: User = Depends(require_approved_user_page),
):
    """Repo 프로필 HTML — JS 가 /api/repo/.../profile 호출해서 데이터 채움.

    require_approved_user_page: 비로그인→/login, pending→/pending 으로 redirect.
    """
    repo_name, date = _validate(owner, name, _resolve_date(date))
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
