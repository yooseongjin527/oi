"""FastAPI 엔트리포인트.

Block 1.4.B: pages + auth + admin 라우터 모두 등록.
"""
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

import models  # noqa: F401  (Base.metadata에 모델 등록)
from auth import RedirectException
from database import Base, engine
from routers import admin_router, auth_router, pages, insights, search, repo, category

# DB 테이블 자동 생성
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Opensource Insights",
    description="GitHub 트렌딩 repo의 부상 원인을 LLM으로 자연어 요약",
    version="0.1.0",
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


@app.get("/health")
def health():
    return {"status": "ok"}
