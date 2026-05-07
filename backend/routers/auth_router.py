"""인증/계정 액션 라우터.

- POST /signup                  → 가입 + 자동 로그인 → /pending 리다이렉트
- POST /login                   → 로그인
- POST /logout                  → 로그아웃
- POST /forgot-password         → 재설정 토큰 발급
- POST /reset-password          → 토큰으로 비밀번호 변경
- POST /settings/profile        → 사용자명/이메일 수정
- POST /settings/password       → 비밀번호 변경
- POST /account/delete          → 본인 탈퇴

GET 페이지 라우트는 routers/pages.py 참조.
"""
import logging
import re
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from auth import (
    COOKIE_NAME,
    JWT_EXPIRE_MINUTES,
    create_access_token,
    get_current_user,
    get_optional_user,
    hash_password,
    verify_password,
)
from database import get_db
from models import PasswordResetToken, User, UserStatus

logger = logging.getLogger(__name__)
router = APIRouter()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# 사용자명 — 한글(가-힣), 영문, 숫자, _ - 3~32자
USERNAME_RE = re.compile(r"^[가-힣a-zA-Z0-9_-]{3,32}$")

RESET_TOKEN_TTL_MIN = 60   # 비밀번호 재설정 토큰 유효 시간 (분)


def _set_auth_cookie(resp: Response, user: User) -> None:
    """JWT 발급 + 쿠키 세팅 (signup 자동 로그인 / login 공용 헬퍼)."""
    token = create_access_token(sub=user.username, role=user.role.value)
    resp.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=JWT_EXPIRE_MINUTES * 60,
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )


# ─── POST /signup ────────────────────────────────────────
@router.post("/signup")
def signup(
    email: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    email = email.strip().lower()
    username = username.strip()
    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="이메일 형식이 올바르지 않습니다.")
    if not USERNAME_RE.match(username):
        raise HTTPException(status_code=400, detail="사용자명은 한글/영문/숫자/_- 3~32자.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="비밀번호는 최소 8자.")

    if db.query(User).filter(User.email == email).first():
        raise HTTPException(status_code=409, detail="이미 등록된 이메일입니다.")
    if db.query(User).filter(User.username == username).first():
        raise HTTPException(status_code=409, detail="이미 사용 중인 사용자명입니다.")

    user = User(
        email=email,
        username=username,
        password_hash=hash_password(password),
        status=UserStatus.pending,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    # 가입 직후 자동 로그인 — 쿠키 발급 후 /pending 으로
    resp = RedirectResponse(url="/pending", status_code=status.HTTP_303_SEE_OTHER)
    _set_auth_cookie(resp, user)
    return resp


# ─── POST /login ─────────────────────────────────────────
@router.post("/login")
def login(
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    email = email.strip().lower()
    user = db.query(User).filter(User.email == email).first()

    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 일치하지 않습니다.")

    # pending / rejected 모두 로그인 허용 — 안내 페이지(/pending)로 보내고
    # 거기서 계정 설정·탈퇴 등으로 이동할 수 있게 한다.
    if user.status != UserStatus.approved:
        resp = RedirectResponse(url="/pending", status_code=status.HTTP_303_SEE_OTHER)
        _set_auth_cookie(resp, user)
        return resp

    redirect_to = "/admin" if user.role.value == "admin" else "/dashboard"
    resp = RedirectResponse(url=redirect_to, status_code=status.HTTP_303_SEE_OTHER)
    _set_auth_cookie(resp, user)
    return resp


# ─── POST /logout ────────────────────────────────────────
@router.post("/logout")
def logout():
    resp = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    resp.delete_cookie(key=COOKIE_NAME, path="/")
    return resp


# ─── POST /forgot-password ────────────────────────────────
# SMTP 미구현 — 토큰을 응답 JSON 으로 직접 반환 (개발/발표용).
# 운영 시에는 reset URL 을 메일로 보내고 응답에는 일반 안내만.
@router.post("/forgot-password")
def forgot_password(
    request: Request,
    email: str = Form(...),
    db: Session = Depends(get_db),
):
    email = email.strip().lower()
    user = db.query(User).filter(User.email == email).first()

    # 응답은 항상 동일 (이메일 존재 여부 노출 방지) — 단, 존재하면 토큰 생성
    generic_msg = "해당 이메일이 등록되어 있다면 재설정 링크를 발급했습니다."

    if not user:
        return {"ok": True, "message": generic_msg, "reset_url": None}

    token = secrets.token_urlsafe(32)
    expires = datetime.now(timezone.utc) + timedelta(minutes=RESET_TOKEN_TTL_MIN)
    rec = PasswordResetToken(user_id=user.id, token=token, expires_at=expires)
    db.add(rec)
    db.commit()

    base = str(request.base_url).rstrip("/")
    reset_url = f"{base}/reset-password?token={token}"
    logger.info("password_reset.issued user=%s url=%s", user.username, reset_url)

    return {
        "ok": True,
        "message": generic_msg,
        "reset_url": reset_url,                # 개발/발표용 — 메일 인프라 미구축
        "expires_in_min": RESET_TOKEN_TTL_MIN,
    }


# ─── POST /reset-password ────────────────────────────────
@router.post("/reset-password")
def reset_password(
    token: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="비밀번호는 최소 8자.")

    rec = (
        db.query(PasswordResetToken)
        .filter(PasswordResetToken.token == token)
        .first()
    )
    if not rec or rec.used:
        raise HTTPException(status_code=400, detail="유효하지 않은 토큰입니다.")
    if rec.expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="만료된 토큰입니다.")

    user = db.query(User).filter(User.id == rec.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")

    user.password_hash = hash_password(password)
    rec.used = True
    db.commit()

    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)


# ─── POST /settings/profile ─────────────────────────────
# 사용자명 / 이메일 수정. 본인만 가능.
@router.post("/settings/profile")
def update_profile(
    email: str = Form(...),
    username: str = Form(...),
    user: User = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    if not user:
        raise HTTPException(status_code=401, detail="인증이 필요합니다.")

    email = email.strip().lower()
    username = username.strip()
    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="이메일 형식이 올바르지 않습니다.")
    if not USERNAME_RE.match(username):
        raise HTTPException(status_code=400, detail="사용자명은 한글/영문/숫자/_- 3~32자.")

    # 다른 사용자가 쓰는지 검사
    if email != user.email and db.query(User).filter(User.email == email).first():
        raise HTTPException(status_code=409, detail="이미 등록된 이메일입니다.")
    if username != user.username and db.query(User).filter(User.username == username).first():
        raise HTTPException(status_code=409, detail="이미 사용 중인 사용자명입니다.")

    username_changed = username != user.username
    user.email = email
    user.username = username
    db.commit()

    # username 변경 시 JWT sub 가 바뀌므로 토큰 재발급 필요
    resp = RedirectResponse(url="/settings?updated=1", status_code=status.HTTP_303_SEE_OTHER)
    if username_changed:
        _set_auth_cookie(resp, user)
    return resp


# ─── POST /settings/password ────────────────────────────
@router.post("/settings/password")
def update_password(
    current_password: str = Form(...),
    new_password: str = Form(...),
    user: User = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    if not user:
        raise HTTPException(status_code=401, detail="인증이 필요합니다.")
    if not verify_password(current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="현재 비밀번호가 일치하지 않습니다.")
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="새 비밀번호는 최소 8자.")

    user.password_hash = hash_password(new_password)
    db.commit()
    return RedirectResponse(url="/settings?updated=1", status_code=status.HTTP_303_SEE_OTHER)


# ─── POST /account/delete ──────────────────────────────
# 본인 탈퇴 — 비밀번호 확인 필수. admin 자기 자신 삭제도 차단.
@router.post("/account/delete")
def delete_my_account(
    password: str = Form(...),
    user: User = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    if not user:
        raise HTTPException(status_code=401, detail="인증이 필요합니다.")
    if not verify_password(password, user.password_hash):
        raise HTTPException(status_code=400, detail="비밀번호가 일치하지 않습니다.")
    if user.role.value == "admin":
        raise HTTPException(status_code=400, detail="관리자는 본인 계정을 직접 탈퇴할 수 없습니다.")

    db.query(PasswordResetToken).filter(PasswordResetToken.user_id == user.id).delete()
    db.delete(user)
    db.commit()

    resp = RedirectResponse(url="/?deleted=1", status_code=status.HTTP_303_SEE_OTHER)
    resp.delete_cookie(key=COOKIE_NAME, path="/")
    return resp
