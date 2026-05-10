"""F4 시간대별 활동 패턴 히트맵 API.

원래 "언어 활동 히트맵" 이었지만, GitHub Events payload 에서
language 필드가 추출되는 비율이 너무 낮아 (~30% 미만, 대부분 Unknown)
유의미한 시각화가 어려움. 차원을 **이벤트 타입** 으로 전환.

- 모든 GitHub Events 는 type 필드를 100% 가짐 (PushEvent, WatchEvent, ...)
- KST 기준 시간대 표시 (UTC+9 변환은 프론트에서)

GET /api/language/heatmap?date=YYYY-MM-DD
  -> { date, hours: [0..23], languages: [type, ...], matrix, totals, coverage }

응답 키 이름은 호환을 위해 유지 (`languages` = 표시할 행, 여기선 이벤트 타입).
승인된 사용자만 조회 가능.
"""
import asyncio
import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import get_current_user
from models import User
from services import athena_client

logger = logging.getLogger(__name__)
router = APIRouter()

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# 사용자가 직관적으로 알아볼 수 있게 type 별 표시 이름 매핑
# 라벨은 짧게 — 좁은 rowhead 칸에 깔끔하게 들어가도록.
_TYPE_DISPLAY = {
    "PushEvent": "Push",
    "PullRequestEvent": "PR",
    "PullRequestReviewEvent": "PR 리뷰",
    "PullRequestReviewCommentEvent": "PR 코멘트",
    "IssuesEvent": "Issue",
    "IssueCommentEvent": "Issue 코멘트",
    "WatchEvent": "Star",
    "ForkEvent": "Fork",
    "CreateEvent": "생성",
    "DeleteEvent": "삭제",
    "ReleaseEvent": "Release",
    "PublicEvent": "공개",
    "GollumEvent": "Wiki",
    "MemberEvent": "멤버",
    "CommitCommentEvent": "Commit 코멘트",
}

_HEATMAP_SQL = """
SELECT
  CAST(hour AS integer)              AS hour,
  type                                AS type_raw,
  CAST(COUNT(*) AS bigint)            AS event_count,
  CAST(COUNT(DISTINCT repo_id) AS bigint) AS unique_repos
FROM oi.silver_events
WHERE year='{year}' AND month='{month}' AND day='{day}'
GROUP BY CAST(hour AS integer), type
ORDER BY hour, event_count DESC
"""


def _build_heatmap(rows: list[dict]) -> dict:
    """Athena 결과(긴 형식) → 프런트 wide 매트릭스.

    - type 별 합계 산정 후 활동량 많은 순서대로 표시
    - 표시명은 _TYPE_DISPLAY 매핑 적용 (없으면 원본)
    """
    totals_by_type: dict[str, int] = {}
    for r in rows:
        t = r.get("type_raw") or "Unknown"
        totals_by_type[t] = totals_by_type.get(t, 0) + int(r.get("event_count") or 0)

    # 활동량 많은 순으로 정렬
    sorted_types = sorted(totals_by_type.items(), key=lambda kv: kv[1], reverse=True)
    raw_types = [t for t, _ in sorted_types]
    display_types = [_TYPE_DISPLAY.get(t, t) for t in raw_types]

    hours = list(range(24))
    matrix = [[0 for _ in raw_types] for _ in hours]
    type_to_idx = {t: i for i, t in enumerate(raw_types)}

    for r in rows:
        try:
            h = int(r["hour"])
            count = int(r["event_count"] or 0)
        except (KeyError, TypeError, ValueError):
            continue
        t = r.get("type_raw") or "Unknown"
        idx = type_to_idx.get(t)
        if idx is None:
            continue
        if 0 <= h <= 23:
            matrix[h][idx] += count

    display_totals = {
        _TYPE_DISPLAY.get(t, t): cnt for t, cnt in sorted_types
    }

    # 응답 키는 호환을 위해 유지 ("languages" = 행 라벨)
    return {
        "hours": hours,
        "languages": display_types,
        "matrix": matrix,
        "totals": display_totals,
        "row_count": sum(sum(r) for r in matrix),
    }


@router.get("/api/language/heatmap")
async def language_heatmap(
    date: str = Query(..., example="2026-04-29"),
    user: User = Depends(get_current_user),
):
    """일별 활동 패턴 히트맵 — silver_events 의 hour × event_type 분포."""
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD 이어야 합니다.")

    year, month, day = date.split("-")
    sql = _HEATMAP_SQL.format(year=year, month=month, day=day)
    try:
        rows = await asyncio.to_thread(athena_client.query, sql, 60)
    except Exception as e:
        logger.exception("activity heatmap query failed user=%s", user.username)
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    payload = _build_heatmap(rows)
    payload["date"] = date
    # coverage: 이벤트 타입은 모든 이벤트가 100% 가짐
    total_events = sum(payload["totals"].values())
    payload["coverage"] = {
        "total_events": total_events,
        "known_events": total_events,
        "known_pct": 100.0,
    }
    return payload
