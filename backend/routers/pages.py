"""페이지 렌더링 라우터 (GET).

POST 액션은 routers/auth_router.py 참조.
"""
from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates

from auth import get_optional_user, require_approved_user_page
from models import User

router = APIRouter()
templates = Jinja2Templates(directory="templates")


# ─── 홈 (비로그인/로그인 모두 허용) ─────────────────────
# home.html 은 user 변수를 봐서 hero CTA 와 안내 배너를 분기 처리.
@router.get("/")
def home(request: Request, user=Depends(get_optional_user)):
    return templates.TemplateResponse(
        "home.html",
        {"request": request, "user": user, "title": "Opensource Insights"},
    )


# ─── 회원가입 / 로그인 / pending ─────────────────────────
@router.get("/signup")
def signup_page(request: Request):
    return templates.TemplateResponse(
        "signup.html",
        {"request": request, "user": None, "title": "회원가입"},
    )


@router.get("/login")
def login_page(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "user": None, "title": "로그인"},
    )


@router.get("/pending")
def pending_page(request: Request):
    return templates.TemplateResponse(
        "pending.html",
        {"request": request, "user": None, "title": "승인 대기 중"},
    )


# ─── 대시보드 (approved 사용자만, 미승인 시 redirect) ──
# require_approved_user_page 는 비로그인 → /login, pending → /pending, rejected → /login
# 으로 자동 redirect 시킴 (RedirectException 을 main.py 핸들러가 받음).
@router.get("/dashboard")
def dashboard(
    request: Request,
    user: User = Depends(require_approved_user_page),
):
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "title": "대시보드"},
    )
