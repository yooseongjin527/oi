"""실시간 라이브 집계 API.

- GET /api/live/pulse  : 60분 sliding window 시계열 (메인페이지 ticker 용)
- GET /api/live/top    : 최근 5분 동안 활동량 Top 10 (진짜 5분 단위 가속도)
- GET /api/live/health : 컨슈머 상태 (운영자 콘솔 / 디버그 용)

인증 정책: **비로그인도 조회 가능**.
메인페이지 hero 의 라이브 ticker 가 누구에게나 보여야 하기 때문.
GitHub Events API 자체가 public 이므로 노출에 따른 위험은 없음.
"""

from fastapi import APIRouter

from services.live_aggregator import aggregator

router = APIRouter(prefix="/api/live", tags=["live"])


@router.get("/pulse")
async def live_pulse():
    """60분 sliding window 시계열 + 현재 분 카운터 + 60분 누적."""
    snap = await aggregator.snapshot()
    return {
        "now": snap["now"],
        "connected": snap["connected"],
        "lag_seconds": snap["lag_seconds"],
        "window_minutes": snap["window_minutes"],
        "current_minute": snap["current_minute"],
        "buckets": snap["buckets"],
        "totals_window": snap["totals_window"],
        "by_type_window": snap["by_type_window"],
    }


@router.get("/top")
async def live_top():
    """최근 5분 동안 가장 활발한 repo Top N — 5분 단위 가속도 시그널."""
    snap = await aggregator.snapshot()
    return {
        "now": snap["now"],
        "connected": snap["connected"],
        "window_minutes": snap["top_window_minutes"],
        "items": snap["top_repos"],
    }


@router.get("/health")
async def live_health():
    """컨슈머 헬스 — Redpanda 연결 / 처리한 메시지 수 / lag."""
    snap = await aggregator.snapshot()
    return {
        "connected": snap["connected"],
        "started_at": snap["started_at"],
        "messages_consumed": snap["messages_consumed"],
        "lag_seconds": snap["lag_seconds"],
        "current_minute_events": snap["current_minute"]["events"],
    }
