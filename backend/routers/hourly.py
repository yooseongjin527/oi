"""오늘 시간대별 진행 API.

Daily batch 마트가 어제 데이터까지 보여주는 동안, 오늘의 시간대별 진행을
hourly 마트(`gold_hourly_recent`)에서 조회해서 노출.

- GET /api/hourly/today?date=YYYY-MM-DD  → 해당 UTC 날짜의 시간대별 누적
  date 미지정 시 today UTC.

응답:
{
  "date": "2026-05-10",
  "hours": [
    {"hour": 0, "hour_kst": 9, "events": 12345, "unique_repos": 234, "by_type": {...}},
    ...
  ],
  "totals": {"events": ..., "unique_repos": ...},
  "top_repos": [{"repo_name": "...", "events": ...}, ...]   # 오늘까지 누적 Top
}

승인된 사용자만 조회 가능.
"""
import asyncio
import logging
import re
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import get_current_user
from models import User
from services import athena_client

logger = logging.getLogger(__name__)
router = APIRouter()

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TOP_N = 10


@router.get("/api/hourly/today")
async def hourly_today(
    date: str | None = Query(None, example="2026-05-10"),
    user: User = Depends(get_current_user),
):
    """오늘(또는 지정 UTC date)의 시간대별 누적 활동.

    데이터 소스: gold_hourly_recent (silver_to_gold_hourly DAG 가 매시 05분 적재).
    """
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD 이어야 합니다.")

    year, month, day = date.split("-")

    # 1) 시간대별 합계
    sql_hours = f"""
    SELECT
      hour                                  AS hour,
      CAST(SUM(event_count) AS bigint)       AS events,
      CAST(COUNT(DISTINCT repo_id) AS bigint) AS unique_repos,
      CAST(SUM(unique_actors) AS bigint)     AS unique_actors,
      CAST(SUM(push_count)  AS bigint)        AS push_count,
      CAST(SUM(pr_count)    AS bigint)        AS pr_count,
      CAST(SUM(issue_count) AS bigint)        AS issue_count,
      CAST(SUM(watch_count) AS bigint)        AS watch_count,
      CAST(SUM(fork_count)  AS bigint)        AS fork_count
    FROM oi.gold_hourly_recent
    WHERE year='{year}' AND month='{month}' AND day='{day}'
    GROUP BY hour
    ORDER BY hour
    """

    # 2) Top repos (오늘까지 누적)
    sql_top = f"""
    SELECT
      repo_name,
      CAST(SUM(event_count) AS bigint) AS events,
      MAX_BY(dominant_event_type, event_count) AS primary_type
    FROM oi.gold_hourly_recent
    WHERE year='{year}' AND month='{month}' AND day='{day}'
    GROUP BY repo_name
    ORDER BY events DESC
    LIMIT {_TOP_N}
    """

    try:
        rows_hours, rows_top = await asyncio.gather(
            asyncio.to_thread(athena_client.query, sql_hours, 60),
            asyncio.to_thread(athena_client.query, sql_top, 60),
        )
    except Exception as e:
        logger.exception("hourly today query failed user=%s", user.username)
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    # 시간대별 정리 (UTC hour → KST 변환은 프론트에서)
    hours_view = []
    totals_events = 0
    totals_actors = 0
    for r in rows_hours:
        try:
            h = int(r["hour"])
        except (KeyError, TypeError, ValueError):
            continue
        events = int(r.get("events") or 0)
        actors = int(r.get("unique_actors") or 0)
        totals_events += events
        totals_actors += actors
        hours_view.append({
            "hour": h,
            "hour_kst": (h + 9) % 24,
            "events": events,
            "unique_repos": int(r.get("unique_repos") or 0),
            "unique_actors": actors,
            "by_type": {
                "PushEvent": int(r.get("push_count") or 0),
                "PullRequestEvent": int(r.get("pr_count") or 0),
                "IssuesEvent": int(r.get("issue_count") or 0),
                "WatchEvent": int(r.get("watch_count") or 0),
                "ForkEvent": int(r.get("fork_count") or 0),
            },
        })

    top_repos = [
        {
            "repo_name": r.get("repo_name"),
            "events": int(r.get("events") or 0),
            "primary_type": r.get("primary_type") or "Unknown",
        }
        for r in rows_top
    ]

    return {
        "date": date,
        "hours": hours_view,
        "totals": {
            "events": totals_events,
            "unique_actors": totals_actors,
        },
        "top_repos": top_repos,
        "source": "GHArchive + bronze_live (풀 커버리지) — 매시 갱신, 약 2~3시간 lag",
    }
