"""인증 유틸 — JWT, bcrypt, FastAPI Depends.

- 비밀번호: bcrypt (passlib)
- 토큰: JWT (python-jose), httpOnly + SameSite=Lax 쿠키로 발급
- /dashboard/* 가드: get_current_user
- /admin/* 가드: require_admin
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from database import get_db
from models import User, UserRole, UserStatus

# ─── 설정 ────────────────────────────────────────────────
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret-change-me")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))
COOKIE_NAME = "oi_token"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ─── 비밀번호 ────────────────────────────────────────────
def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


# ─── JWT ─────────────────────────────────────────────────
def create_access_token(*, sub: str, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": sub,                                 # username
        "role": role,                               # "user" | "admin"
        "iat": now,
        "exp": now + timedelta(minutes=JWT_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    """JWT 디코딩. 실패 시 JWTError 발생."""
    return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])


# ─── FastAPI Depends ─────────────────────────────────────
def _get_token_from_cookie(request: Request) -> Optional[str]:
    return request.cookies.get(COOKIE_NAME)


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
) -> User:
    """쿠키에서 JWT 읽고, approved 상태인 User 반환.

    실패 시:
    - 토큰 없음/만료/위조 → 401
    - 토큰은 OK인데 DB에 없음 → 401
    - status가 approved가 아니면 → 403
    """
    token = _get_token_from_cookie(request)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="인증이 필요합니다.",
        )
    try:
        payload = decode_token(token)
        username: str = payload.get("sub")
        if not username:
            raise JWTError("missing sub")
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="유효하지 않은 토큰입니다.",
        )

    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="사용자를 찾을 수 없습니다.",
        )
    if user.status != UserStatus.approved:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="승인되지 않은 계정입니다.",
        )
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    """admin 전용 라우트 가드."""
    if user.role != UserRole.admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="관리자 권한이 필요합니다.",
        )
    return user


# ─── 페이지 라우트 용 (선택적 인증) ─────────────────────
def get_optional_user(
    request: Request,
    db: Session = Depends(get_db),
) -> Optional[User]:
    """홈/로그인 페이지처럼 비회원도 보는 곳에서 사용.

    토큰 없으면 None, 있으면 User. 401/403 던지지 않음.
    """
    token = _get_token_from_cookie(request)
    if not token:
        return None
    try:
        payload = decode_token(token)
        username = payload.get("sub")
        if not username:
            return None
    except JWTError:
        return None
    return db.query(User).filter(User.username == username).first()


# ─── 페이지 라우트 용 (가드 + redirect) ─────────────────
# get_current_user 는 401/403 raw 에러 던지므로 API 용.
# 페이지에서는 비로그인이면 /login, 미승인이면 /pending 으로 깔끔하게 보냄.
class RedirectException(Exception):
    """페이지 가드에서 redirect 가 필요할 때 raise. main.py 에서 핸들러 등록."""
    def __init__(self, url: str):
        self.url = url


def require_approved_user_page(
    request: Request,
    db: Session = Depends(get_db),
) -> User:
    """페이지 라우트용 가드 — 인증 + 승인 상태 검사, 실패 시 적절한 페이지로 redirect.

    - 토큰 없음/잘못됨 → /login
    - 사용자 없음 → /login
    - status == pending → /pending
    - status == rejected → /login (rejected 사용자는 그냥 비회원처럼 처리)
    - status == approved → User 반환
    """
    token = _get_token_from_cookie(request)
    if not token:
        raise RedirectException("/login")
    try:
        payload = decode_token(token)
        username = payload.get("sub")
        if not username:
            raise RedirectException("/login")
    except JWTError:
        raise RedirectException("/login")

    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise RedirectException("/login")

    # pending / rejected 모두 안내 페이지로 — 거기서 설정·탈퇴 가능
    if user.status != UserStatus.approved:
        raise RedirectException("/pending")

    return user
