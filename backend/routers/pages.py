"""페이지 렌더링 라우터 (GET).

POST 액션은 routers/auth_router.py 참조.
"""
from fastapi import APIRouter, Depends, Query, Request
from fastapi.templating import Jinja2Templates

from auth import get_optional_user, require_approved_user_page
from models import User

router = APIRouter()
templates = Jinja2Templates(directory="templates")


# ─── 홈 (비로그인/로그인 모두 허용) ─────────────────────
@router.get("/")
def home(request: Request, user=Depends(get_optional_user)):
    return templates.TemplateResponse(
        "home.html",
        {"request": request, "user": user, "title": "Opensource Insights"},
    )


# ─── 회원가입 / 로그인 / pending ─────────────────────────
@router.get("/signup")
def signup_page(request: Request, user=Depends(get_optional_user)):
    return templates.TemplateResponse(
        "signup.html",
        {"request": request, "user": user, "title": "회원가입"},
    )


@router.get("/login")
def login_page(request: Request, user=Depends(get_optional_user)):
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "user": user, "title": "로그인"},
    )


# pending 페이지 — 비로그인이어도 접근 가능 (가입 직후 정보 조회용)
@router.get("/pending")
def pending_page(request: Request, user=Depends(get_optional_user)):
    return templates.TemplateResponse(
        "pending.html",
        {"request": request, "user": user, "title": "승인 대기 중"},
    )


# ─── 비밀번호 찾기 / 재설정 ─────────────────────────────
@router.get("/forgot-password")
def forgot_password_page(request: Request, user=Depends(get_optional_user)):
    return templates.TemplateResponse(
        "forgot_password.html",
        {"request": request, "user": user, "title": "비밀번호 찾기"},
    )


@router.get("/reset-password")
def reset_password_page(
    request: Request,
    token: str = Query("", description="발급받은 재설정 토큰"),
    user=Depends(get_optional_user),
):
    return templates.TemplateResponse(
        "reset_password.html",
        {"request": request, "user": user, "title": "비밀번호 재설정", "token": token},
    )


# ─── 대시보드 (approved 사용자만, 미승인 시 redirect) ──
@router.get("/dashboard")
def dashboard(
    request: Request,
    user: User = Depends(require_approved_user_page),
):
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "title": "대시보드"},
    )


# ─── 계정 설정 (로그인된 사용자 — pending 포함) ─────────
@router.get("/settings")
def settings_page(
    request: Request,
    updated: int = Query(0),
    user=Depends(get_optional_user),
):
    if not user:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "user": user,
            "title": "계정 설정",
            "updated": bool(updated),
        },
    )
