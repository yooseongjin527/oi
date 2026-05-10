"""
services/repo_service.py
Repo 프로필 페이지 — Athena hourly + daily 메트릭 + 단일 repo 인사이트 생성/캐시.

데이터 소스:
- oi.gold_repo_hourly      → 24시간 시계열
- oi.gold_repo_daily       → 일별 메트릭 (event_count, dominant_event_type, ...)
- oi.gold_repo_acceleration → 가속도 (전일 대비 비율)
- oi.gold_repo_anomaly     → 이상 탐지 (z-score)
- OpenSearch oi-repo-daily → 그 repo·그 날짜에 캐시된 단일 인사이트 마크다운
                              (없으면 Bedrock 으로 즉석 생성 후 캐시)

설계 원칙:
- 대시보드의 통합 인사이트(Top 3)와 별도로, repo 상세 페이지는 항상 그 repo
  단독에 대한 분석을 보여줌. Top 3 든 아니든 일관된 사용자 경험.
- 첫 진입 시 Bedrock 호출 1회 (~2-3초, ~$0.0001), 이후엔 OpenSearch 캐시 hit.
"""
import logging
from pathlib import Path
from typing import Any

from services import athena_client

logger = logging.getLogger(__name__)

_PROMPT_DIR = Path(__file__).parent.parent / "prompts"


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


def _load_prompt(name: str) -> str:
    return (_PROMPT_DIR / f"{name}.md").read_text(encoding="utf-8")


def _build_single_prompt(template_md: str, **kwargs) -> tuple[str, str]:
    """## system / ## user_template 분리 + {{key}} 치환."""
    parts = template_md.split("## user_template")
    system_raw = parts[0].replace("## system", "").strip()
    user_raw = parts[1].strip() if len(parts) > 1 else ""
    user_text = user_raw
    for key, val in kwargs.items():
        user_text = user_text.replace(f"{{{{{key}}}}}", str(val))
    return system_raw, user_text


def _fmt(v: Any, decimals: int = 2, default: str = "—") -> str:
    """메트릭 값을 prompt 에 넣기 좋은 문자열로 포맷."""
    if v is None or v == "":
        return default
    try:
        f = float(v)
        if f != f:  # NaN
            return default
        return f"{f:.{decimals}f}"
    except (TypeError, ValueError):
        return str(v)


def _generate_repo_insight(repo_name: str, date: str, metrics: dict) -> str | None:
    """단일 repo Bedrock 호출로 그날 인사이트 마크다운 생성.

    실패해도 None 반환 (caller 가 best-effort 처리).
    """
    try:
        from services import bedrock_client
        template = _load_prompt("repo_single_insight_v1")
        system_text, user_text = _build_single_prompt(
            template,
            date=date,
            repo_name=repo_name,
            event_count=metrics.get("event_count") or "?",
            dominant_event_type=metrics.get("dominant_event_type") or "Unknown",
            acceleration_ratio=_fmt(metrics.get("acceleration_ratio")),
            anomaly_score=_fmt(metrics.get("anomaly_score")),
            watch_zscore=_fmt(metrics.get("watch_zscore")),
            watch_count=metrics.get("watch_count") or 0,
            fork_count=metrics.get("fork_count") or 0,
            pr_count=metrics.get("pr_count") or 0,
            push_count=metrics.get("push_count") or 0,
        )
        result = bedrock_client.invoke_with_meta(
            user_text=user_text,
            system=system_text,
            max_tokens=400,        # 짧은 마크다운 3줄
            temperature=0.4,
        )
        text = (result.get("text") or "").strip()
        logger.info(
            "repo_insight.generate repo=%s date=%s tokens=%d/%d latency=%dms",
            repo_name, date,
            result.get("input_tokens", 0), result.get("output_tokens", 0),
            result.get("latency_ms", 0),
        )
        return text or None
    except Exception as e:
        logger.warning("repo_insight.generate failed repo=%s date=%s: %s",
                       repo_name, date, e)
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

    # 3. 그날의 인사이트 — 이 repo 단독에 대한 분석 (캐시 hit → 즉시, miss → Bedrock 호출)
    # OpenSearch 의 같은 doc (date_repo_name) 에 repo_insight_markdown 필드 캐시.
    # 비용: Haiku 4.5 단일 호출 ~$0.0001 / 캐시 hit 후엔 0.
    insight_markdown: str | None = None
    insight_cached: bool = False
    try:
        from services import opensearch_client
        cached = opensearch_client.get_repo_insight(date, repo_name)
        if cached:
            insight_markdown = cached
            insight_cached = True
            logger.info("repo_insight.cache_hit repo=%s date=%s", repo_name, date)
        elif metrics:
            # 메트릭이 있을 때만 Bedrock 호출 (메트릭 자체가 없으면 분석 불가)
            generated = _generate_repo_insight(repo_name, date, metrics)
            if generated:
                insight_markdown = generated
                # best-effort 캐시 — 실패해도 응답은 정상
                opensearch_client.cache_repo_insight(date, repo_name, generated)
    except Exception as e:
        logger.warning("repo_profile.insight failed (non-fatal): %s", e)

    return {
        "repo_name": repo_name,
        "date": date,
        "metrics": metrics,
        "timeline": timeline,
        "insight_markdown": insight_markdown,   # 이 repo 의 그날 단독 인사이트
        "insight_cached": insight_cached,       # 캐시 hit 여부 (디버깅/UI 작은 라벨용)
    }