"""SQLAlchemy 엔진 / 세션 / Base."""
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# ─── 환경변수에서 DB 접속 정보 ────────────────────────────
POSTGRES_USER = os.getenv("POSTGRES_USER", "oi_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "oi")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ─── 엔진 / 세션 ──────────────────────────────────────────
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # 끊긴 커넥션 자동 감지
    pool_size=5,
    max_overflow=10,
    echo=False,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


# ─── FastAPI Depends 용 ──────────────────────────────────
def get_db():
    """라우터에서 Depends(get_db) 형태로 주입."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
