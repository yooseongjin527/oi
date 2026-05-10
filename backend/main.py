"""FastAPI 엔트리포인트.

Block 1.4.B: pages + auth + admin 라우터 모두 등록.
+ Live aggregator 백그라운드 컨슈머 (lifespan 으로 시작/정지)
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

import models  # noqa: F401  (Base.metadata에 모델 등록)
from auth import RedirectException
from database import Base, engine
from routers import admin_router, auth_router, pages, insights, search, repo, category, language, live, hourly
from services.live_aggregator import aggregator

logger = logging.getLogger(__name__)

# DB 테이블 자동 생성
Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup: Redpanda 컨슈머 백그라운드 task 띄우기
    try:
        await aggregator.start()
    except Exception as e:
        # 컨슈머 실패가 FastAPI 자체를 막지 않게.
        # /api/live/* 는 connected=false 로 응답.
        logger.exception("LiveAggregator start failed: %s", e)
    yield
    # shutdown
    try:
        await aggregator.stop()
    except Exception as e:
        logger.warning("LiveAggregator stop failed: %s", e)


app = FastAPI(
    title="Opensource Insights",
    description="GitHub 트렌딩 repo의 부상 원인을 LLM으로 자연어 요약",
    version="0.1.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory="static"), name="static")


# 페이지 가드 (auth.require_approved_user_page) 가 던지는 redirect 예외 핸들러.
# 비로그인/미승인 사용자가 보호된 페이지에 접근하면 적절한 안내 페이지로 보냄.
@app.exception_handler(RedirectException)
async def redirect_exception_handler(request: Request, exc: RedirectException):
    return RedirectResponse(url=exc.url, status_code=303)


# 라우터 등록
app.include_router(pages.router)
app.include_router(auth_router.router)
app.include_router(admin_router.router)
app.include_router(insights.router)
app.include_router(search.router)
app.include_router(repo.router)
app.include_router(category.router)
app.include_router(language.router)
app.include_router(live.router)
app.include_router(hourly.router)


@app.get("/health")
def health():
    return {"status": "ok"}
