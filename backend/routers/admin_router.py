"""Admin 콘솔 라우터.

- GET  /admin                       → 승인/거부 대기 + 전체 사용자 목록
- POST /admin/users/{id}/approve    → 승인
- POST /admin/users/{id}/reject     → 거부

require_admin 가드로 role=admin만 접근 가능.
"""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import require_admin
from database import get_db
from models import User, UserStatus

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/admin")
def admin_page(
    request: Request,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    pending_users = (
        db.query(User)
        .filter(User.status == UserStatus.pending)
        .order_by(User.created_at.desc())
        .all()
    )
    other_users = (
        db.query(User)
        .filter(User.status != UserStatus.pending)
        .order_by(User.created_at.desc())
        .all()
    )
    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "user": admin,                    # base.html nav가 user 변수 참조
            "title": "관리자 콘솔",
            "pending_users": pending_users,
            "other_users": other_users,
        },
    )


@router.post("/admin/users/{user_id}/approve")
def approve_user(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    target.status = UserStatus.approved
    target.approved_at = datetime.now(timezone.utc)
    db.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/admin/users/{user_id}/reject")
def reject_user(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    if target.id == admin.id:
        raise HTTPException(status_code=400, detail="자기 자신은 거부할 수 없습니다.")
    target.status = UserStatus.rejected
    db.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
