"""페이지 렌더링 라우터 (GET).

POST 액션은 routers/auth_router.py 참조.
"""
from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates

from auth import get_current_user, get_optional_user
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


# ─── 대시보드 (approved 사용자만) ───────────────────────
@router.get("/dashboard")
def dashboard(request: Request, user: User = Depends(get_current_user)):
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "title": "대시보드"},
    )
