"""최초 admin 계정 부트스트랩 스크립트.

Usage:
    docker compose exec fastapi python init_admin.py

.env의 ADMIN_EMAIL / ADMIN_USERNAME / ADMIN_PASSWORD를 읽어 admin 계정 생성.

멱등(idempotent):
- 동일 email 또는 username으로 이미 존재하면 → role=admin, status=approved로 승격만.
- 새로 만드는 경우 → status=approved, role=admin.
"""
import os
import sys
from datetime import datetime, timezone

from sqlalchemy.exc import SQLAlchemyError

from auth import hash_password
from database import Base, SessionLocal, engine
from models import User, UserRole, UserStatus


def main() -> int:
    email = os.getenv("ADMIN_EMAIL", "").strip().lower()
    username = os.getenv("ADMIN_USERNAME", "").strip()
    password = os.getenv("ADMIN_PASSWORD", "")

    if not email or not username or not password:
        print(
            "[init_admin] ❌ .env에 ADMIN_EMAIL / ADMIN_USERNAME / ADMIN_PASSWORD 가 모두 설정되어야 합니다.",
            file=sys.stderr,
        )
        return 1

    # 테이블이 없을 가능성 대비 (보통은 main.py가 이미 만들었음)
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        existing = (
            db.query(User)
            .filter((User.email == email) | (User.username == username))
            .first()
        )
        now = datetime.now(timezone.utc)

        if existing:
            # 멱등 업그레이드: 비번은 그대로, 권한/상태만 보장
            changed = []
            if existing.role != UserRole.admin:
                existing.role = UserRole.admin
                changed.append("role=admin")
            if existing.status != UserStatus.approved:
                existing.status = UserStatus.approved
                existing.approved_at = now
                changed.append("status=approved")
            db.commit()
            if changed:
                print(f"[init_admin] ✅ 기존 사용자 '{existing.username}' 권한 승격: {', '.join(changed)}")
            else:
                print(f"[init_admin] ℹ️  '{existing.username}'는 이미 admin/approved 상태입니다.")
            return 0

        # 신규 생성
        admin = User(
            email=email,
            username=username,
            password_hash=hash_password(password),
            status=UserStatus.approved,
            role=UserRole.admin,
            approved_at=now,
        )
        db.add(admin)
        db.commit()
        print(f"[init_admin] ✅ admin 계정 생성: {username} ({email})")
        return 0

    except SQLAlchemyError as e:
        db.rollback()
        print(f"[init_admin] ❌ DB 오류: {e}", file=sys.stderr)
        return 2
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
