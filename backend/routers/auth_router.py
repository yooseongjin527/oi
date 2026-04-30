"""인증 액션 라우터 — POST /signup, /login, /logout.

GET 페이지 라우트는 routers/pages.py 참조.
"""
import re

from fastapi import APIRouter, Depends, Form, HTTPException, Response, status
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from auth import (
    COOKIE_NAME,
    JWT_EXPIRE_MINUTES,
    create_access_token,
    hash_password,
    verify_password,
)
from database import get_db
from models import User, UserStatus

router = APIRouter()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
USERNAME_RE = re.compile(r"^[a-zA-Z0-9_-]{3,32}$")


# ─── POST /signup ────────────────────────────────────────
@router.post("/signup")
def signup(
    email: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    # 입력 검증
    email = email.strip().lower()
    username = username.strip()
    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="이메일 형식이 올바르지 않습니다.")
    if not USERNAME_RE.match(username):
        raise HTTPException(status_code=400, detail="사용자명은 영문/숫자/_-만 3~32자.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="비밀번호는 최소 8자.")

    # 중복 체크
    if db.query(User).filter(User.email == email).first():
        raise HTTPException(status_code=409, detail="이미 등록된 이메일입니다.")
    if db.query(User).filter(User.username == username).first():
        raise HTTPException(status_code=409, detail="이미 사용 중인 사용자명입니다.")

    # 생성 (status=pending 디폴트)
    user = User(
        email=email,
        username=username,
        password_hash=hash_password(password),
        status=UserStatus.pending,
    )
    db.add(user)
    db.commit()

    return RedirectResponse(url="/pending", status_code=status.HTTP_303_SEE_OTHER)


# ─── POST /login ─────────────────────────────────────────
@router.post("/login")
def login(
    response: Response,
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    email = email.strip().lower()
    user = db.query(User).filter(User.email == email).first()

    # 사용자 없거나 비밀번호 불일치 → 동일 메시지 (열거 공격 방지)
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 일치하지 않습니다.")

    # 사용자 없거나 비밀번호 불일치 → 동일 메시지 (열거 공격 방지)
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="사용자명 또는 비밀번호가 일치하지 않습니다.")

    # 상태 체크
    if user.status == UserStatus.pending:
        raise HTTPException(status_code=403, detail="승인 대기 중입니다. 관리자 승인 후 로그인하실 수 있습니다.")
    if user.status == UserStatus.rejected:
        raise HTTPException(status_code=403, detail="가입이 거부된 계정입니다.")

    # JWT 발급 → 쿠키
    token = create_access_token(sub=user.username, role=user.role.value)

    # admin이면 /admin, 일반은 /dashboard
    redirect_to = "/admin" if user.role.value == "admin" else "/dashboard"
    resp = RedirectResponse(url=redirect_to, status_code=status.HTTP_303_SEE_OTHER)
    resp.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=JWT_EXPIRE_MINUTES * 60,
        httponly=True,
        samesite="lax",
        secure=False,   # 운영에서 HTTPS면 True (Day 8에 변경)
        path="/",
    )
    return resp


# ─── POST /logout ────────────────────────────────────────
@router.post("/logout")
def logout():
    resp = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    resp.delete_cookie(key=COOKIE_NAME, path="/")
    return resp
