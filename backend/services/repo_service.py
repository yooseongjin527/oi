"""
services/repo_service.py
Repo 프로필 페이지 — Athena hourly + daily 메트릭 + OpenSearch 인사이트 통합 조회.

데이터 소스:
- oi.gold_repo_hourly      → 24시간 시계열
- oi.gold_repo_daily       → 일별 메트릭 (event_count, dominant_event_type, ...)
- oi.gold_repo_acceleration → 가속도 (전일 대비 비율)
- oi.gold_repo_anomaly     → 이상 탐지 (z-score)
- OpenSearch oi-repo-daily → 그날의 인사이트 마크다운
"""
import logging
from typing import Any

from services import athena_client

logger = logging.getLogger(__name__)


# ─── SQL 정의 ───────────────────────────────────────────

# 24시간 시계열 — 빈 시간대는 클라이언트에서 0으로 채움
_HOURLY_SQL = """
SELECT
  CAST(hour AS INTEGER) AS hour,
  event_count,
  dominant_event_type
FROM oi.gold_repo_hourly
WHERE repo_name = '{repo_name}'
  AND year='{year}' AND month='{month}' AND day='{day}'
ORDER BY CAST(hour AS INTEGER)
"""

# 일별 메트릭 + 가속도 + 이상 탐지 (1행)
_METRICS_SQL = """
SELECT
  d.repo_id,
  d.repo_name,
  d.event_count,
  d.unique_actors,
  d.dominant_event_type,
  d.push_count,
  d.pr_count,
  d.issue_count,
  d.watch_count,
  d.fork_count,
  d.first_event_at,
  d.last_event_at,
  a.acceleration_ratio,
  a.prev_event_count,
  n.anomaly_score,
  n.watch_ratio,
  n.watch_zscore
FROM oi.gold_repo_daily d
LEFT JOIN oi.gold_repo_acceleration a
  ON d.repo_id = a.repo_id AND d.year=a.year AND d.month=a.month AND d.day=a.day
LEFT JOIN oi.gold_repo_anomaly n
  ON d.repo_id = n.repo_id AND d.year=n.year AND d.month=n.month AND d.day=n.day
WHERE d.repo_name = '{repo_name}'
  AND d.year='{year}' AND d.month='{month}' AND d.day='{day}'
LIMIT 1
"""


# ─── 헬퍼 ──────────────────────────────────────────────

def _fill_24h(rows: list[dict]) -> list[dict]:
    """
    Athena 결과의 hour/event_count 를 24시간 배열로 정규화.
    누락된 시간대는 event_count=0 으로 채움 (차트 끊김 방지).

    Returns: [{"hour": 0, "event_count": N, "dominant_event_type": "..."}] x 24
    """
    by_hour = {int(r["hour"]): r for r in rows}
    result = []
    for h in range(24):
        if h in by_hour:
            r = by_hour[h]
            result.append({
                "hour": h,
                "event_count": int(r["event_count"]) if r["event_count"] else 0,
                "dominant_event_type": r.get("dominant_event_type"),
            })
        else:
            result.append({
                "hour": h,
                "event_count": 0,
                "dominant_event_type": None,
            })
    return result


def _extract_repo_section(insight_md: str | None, repo_name: str) -> str | None:
    """
    인사이트 마크다운 전체에서 해당 repo 섹션만 추출.

    프롬프트 템플릿이 #### N. [owner/name](url) 형식으로 섹션을 만드는 걸 활용.
    매칭 안 되면 None 반환.
    """
    if not insight_md:
        return None

    if repo_name not in insight_md:
        return None

    # #### N. 으로 시작하는 섹션 헤더 기준으로 split
    # repo_name 이 있는 섹션부터 다음 #### 까지 추출
    parts = insight_md.split("\n#### ")
    for part in parts:
        if repo_name in part:
            # 첫 part 가 아니면 #### 헤더 prefix 복원
            if not part.startswith("#"):
                part = "#### " + part
            return part.strip()

    return None


# ─── 메인 함수 ─────────────────────────────────────────

def get_repo_profile(repo_name: str, date: str) -> dict[str, Any]:
    """
    Repo 프로필 페이지용 통합 조회.

    Args:
        repo_name: "owner/name" 형식
        date: "YYYY-MM-DD"

    Returns:
        {
          "repo_name": str,
          "date": str,
          "metrics": dict,         # daily + acceleration + anomaly
          "timeline": list[dict],  # 24시간 (0~23)
          "insight_section": str | None,  # 그날의 인사이트 중 해당 repo 섹션
        }
    """
    year, month, day = date.split("-")

    # 단순 quote escape — repo_name 은 라우터에서 정규식 검증된 owner/name 형식
    safe_repo = repo_name.replace("'", "''")

    logger.info("repo_profile.start repo=%s date=%s", repo_name, date)

    # 1. 24시간 시계열
    hourly_sql = _HOURLY_SQL.format(
        repo_name=safe_repo, year=year, month=month, day=day,
    )
    hourly_rows = athena_client.query(hourly_sql, timeout_sec=60)
    timeline = _fill_24h(hourly_rows)
    logger.info("repo_profile.hourly rows=%d", len(hourly_rows))

    # 2. 일별 메트릭 (acceleration + anomaly join)
    metrics_sql = _METRICS_SQL.format(
        repo_name=safe_repo, year=year, month=month, day=day,
    )
    metrics_rows = athena_client.query(metrics_sql, timeout_sec=60)
    metrics = metrics_rows[0] if metrics_rows else {}
    logger.info("repo_profile.metrics found=%s", bool(metrics))

    # 3. 그날의 인사이트 — OpenSearch 에서 조회 (best-effort)
    # import 를 함수 안에 둔 이유: OpenSearch 가 fastapi startup 시점에
    # 안 떠있어도 fastapi 자체는 뜨도록 안전장치
    insight_section = None
    try:
        from services import opensearch_client
        result = opensearch_client.get_by_date(date=date, size=20)
        for hit in result.get("hits", []):
            if hit.get("repo_name") == repo_name:
                insight_md = hit.get("insight_markdown")
                insight_section = _extract_repo_section(insight_md, repo_name)
                break
    except Exception as e:
        logger.warning("repo_profile.opensearch failed (non-fatal): %s", e)

    return {
        "repo_name": repo_name,
        "date": date,
        "metrics": metrics,
        "timeline": timeline,
        "insight_section": insight_section,
    }