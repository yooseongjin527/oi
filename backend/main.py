"""FastAPI 엔트리포인트.

Block 1.4.B: pages + auth + admin 라우터 모두 등록.
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import models  # noqa: F401  (Base.metadata에 모델 등록)
from database import Base, engine
from routers import admin_router, auth_router, pages, insights

# DB 테이블 자동 생성
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Opensource Insights",
    description="GitHub 트렌딩 repo의 부상 원인을 LLM으로 자연어 요약",
    version="0.1.0",
)

app.mount("/static", StaticFiles(directory="static"), name="static")

# 라우터 등록
app.include_router(pages.router)
app.include_router(auth_router.router)
app.include_router(admin_router.router)
app.include_router(insights.router)


@app.get("/health")
def health():
    return {"status": "ok"}
