"""F4 언어 활동 히트맵 API.

- GET /api/language/heatmap?date=YYYY-MM-DD
  -> { date, hours: [0..23], languages: [...], matrix: [[..]], totals: {...} }

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

# 표시할 상위 언어 개수 (이외는 'Other' 로 합산)
_TOP_N_LANGUAGES = 10

_HEATMAP_SQL = """
SELECT
  CAST(hour AS integer)              AS hour,
  language,
  CAST(SUM(event_count) AS bigint)    AS event_count,
  CAST(SUM(unique_repos) AS bigint)    AS unique_repos
FROM oi.gold_language_activity
WHERE year='{year}' AND month='{month}' AND day='{day}'
GROUP BY CAST(hour AS integer), language
ORDER BY hour, event_count DESC
"""


def _build_heatmap(rows: list[dict]) -> dict:
    """Athena 결과(긴 형식) → 프런트가 그리기 쉬운 wide 매트릭스로 정리.

    - language 별 합계 산정 후 상위 N 만 유지 (그 외는 'Other' 로 합산)
    - 0 시간대는 0 으로 채움
    """
    # 언어별 총합
    totals_by_lang: dict[str, int] = {}
    for r in rows:
        lang = r.get("language") or "Unknown"
        totals_by_lang[lang] = totals_by_lang.get(lang, 0) + int(r.get("event_count") or 0)

    # 상위 N 언어 + Other
    top_langs = [
        l for l, _ in sorted(totals_by_lang.items(), key=lambda kv: kv[1], reverse=True)[:_TOP_N_LANGUAGES]
    ]
    if "Unknown" in top_langs:
        # Unknown 은 상위에서 빼고 항상 마지막에 두기 (가독성)
        top_langs = [l for l in top_langs if l != "Unknown"] + ["Unknown"]
    other_langs = set(totals_by_lang.keys()) - set(top_langs)
    has_other = bool(other_langs)
    languages = top_langs + (["Other"] if has_other else [])

    hours = list(range(24))
    matrix = [[0 for _ in languages] for _ in hours]

    lang_to_idx = {l: i for i, l in enumerate(languages)}
    for r in rows:
        try:
            h = int(r["hour"])
            count = int(r["event_count"] or 0)
        except (KeyError, TypeError, ValueError):
            continue
        lang = r.get("language") or "Unknown"
        idx = lang_to_idx.get(lang)
        if idx is None and has_other:
            idx = lang_to_idx["Other"]
        if idx is None:
            continue
        if 0 <= h <= 23:
            matrix[h][idx] += count

    # 표시용 totals — 상위 N 언어만 (Other 별도 표시)
    display_totals = {l: totals_by_lang.get(l, 0) for l in top_langs}
    if has_other:
        display_totals["Other"] = sum(totals_by_lang[l] for l in other_langs)

    return {
        "hours": hours,
        "languages": languages,
        "matrix": matrix,
        "totals": display_totals,
        "row_count": sum(sum(r) for r in matrix),
    }


@router.get("/api/language/heatmap")
async def language_heatmap(
    date: str = Query(..., example="2026-04-29"),
    user: User = Depends(get_current_user),
):
    """일별 언어 활동 히트맵 — gold_language_activity 마트에서 직접 조회."""
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD 이어야 합니다.")

    year, month, day = date.split("-")
    sql = _HEATMAP_SQL.format(year=year, month=month, day=day)
    try:
        rows = await asyncio.to_thread(athena_client.query, sql, 60)
    except Exception as e:
        logger.exception("language heatmap query failed user=%s", user.username)
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    payload = _build_heatmap(rows)
    payload["date"] = date
    return payload
