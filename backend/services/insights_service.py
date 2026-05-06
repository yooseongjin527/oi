"""
services/insights_service.py
Gold 마트 조회 → 프롬프트 렌더링 → Bedrock 호출 E2E 서비스.

athena_client + bedrock_client 를 묶는 오케스트레이터.
FastAPI 라우터에서 asyncio.to_thread() 로 감싸서 호출.
"""
import os
import logging
from pathlib import Path

from services import athena_client, bedrock_client

logger = logging.getLogger(__name__)

# 프롬프트 템플릿 파일 경로 — 이 파일 기준 상위/prompts/
_PROMPT_DIR = Path(__file__).parent.parent / "prompts"

# 확정된 필터 + 가중치 쿼리 (2026-04-29 기준 검증 완료)
_TOP10_SQL = """
WITH ranked AS (
  SELECT
    d.repo_id, d.repo_name, d.event_count, d.dominant_event_type,
    a.acceleration_ratio, a.prev_event_count,
    n.anomaly_score, n.watch_ratio, n.watch_zscore,
    0.4 * LEAST(COALESCE(a.acceleration_ratio, 0), 10.0)
    + 0.4 * COALESCE(n.anomaly_score, 0)
    + 0.2 * LN(d.event_count + 1) AS rank_score
  FROM oi.gold_repo_daily d
  LEFT JOIN oi.gold_repo_acceleration a
    ON d.repo_id = a.repo_id AND d.year=a.year AND d.month=a.month AND d.day=a.day
  LEFT JOIN oi.gold_repo_anomaly n
    ON d.repo_id = n.repo_id AND d.year=n.year AND d.month=n.month AND d.day=n.day
  WHERE d.year='{year}' AND d.month='{month}' AND d.day='{day}'
    AND d.event_count >= 50
    AND COALESCE(a.prev_event_count, 0) >= 10
    AND COALESCE(n.watch_zscore, -999) > 0
)
SELECT * FROM ranked
ORDER BY rank_score DESC
LIMIT 10
"""


def _load_prompt(name: str) -> str:
    """prompts/ 디렉터리에서 마크다운 템플릿 로드."""
    path = _PROMPT_DIR / f"{name}.md"
    return path.read_text(encoding="utf-8")


def _render_repo_table(rows: list) -> str:
    """
    Athena 결과 rows → 프롬프트용 텍스트 테이블 변환.
    Bedrock 에게 던질 구조화된 텍스트. 마크다운 테이블 형식.
    """
    lines = [
        "| # | repo | events | dominant | acc | anomaly | watch_z |",
        "|---|------|--------|----------|-----|---------|---------|",
    ]
    for i, r in enumerate(rows, 1):
        # Athena 결과는 모두 string — 숫자는 여기서 변환
        acc = float(r.get("acceleration_ratio") or 0)
        anomaly = float(r.get("anomaly_score") or 0)
        watch_z = float(r.get("watch_zscore") or 0)
        lines.append(
            f"| {i} | {r['repo_name']} | {r['event_count']} "
            f"| {r.get('dominant_event_type','')} "
            f"| {acc:.2f} | {anomaly:.2f} | {watch_z:.2f} |"
        )
    return "\n".join(lines)


def _build_prompt(template_md: str, date: str, repo_table: str) -> tuple[str, str]:
    """
    템플릿에서 system / user 텍스트 분리 + placeholder 치환.
    Returns: (system_text, user_text)
    """
    # ## system 과 ## user_template 섹션 분리
    parts = template_md.split("## user_template")
    system_raw = parts[0].replace("## system", "").strip()
    user_raw = parts[1].strip() if len(parts) > 1 else ""

    # {{key}} placeholder 치환
    user_text = (
        user_raw
        .replace("{{date}}", date)
        .replace("{{repo_table}}", repo_table)
    )
    system_raw = system_raw.replace("{{date}}", date)

    return system_raw, user_text


def get_daily_insights(date: str) -> dict:
    """
    E2E: Athena Gold 조회 → 프롬프트 렌더링 → Bedrock 호출 → dict 반환.

    Args:
        date: 'YYYY-MM-DD' 형식
    Returns:
        {
          "date": str,
          "insight_markdown": str,   # Bedrock 응답 (마크다운)
          "data_basis": list[dict],  # 근거 데이터 (UI 토글용)
          "bedrock_meta": dict,      # 토큰/latency (운영 로깅용)
        }
    """
    # 날짜 파싱 — YYYY-MM-DD → year/month/day 분리
    year, month, day = date.split("-")

    logger.info("insights.start date=%s", date)

    # Step 1: Athena Gold 마트 조회
    sql = _TOP10_SQL.format(year=year, month=month, day=day)
    rows = athena_client.query(sql, timeout_sec=90)
    logger.info("insights.athena rows=%d", len(rows))

    if not rows:
        return {
            "date": date,
            "insight_markdown": f"### {date} 데이터 없음\n\n해당 날짜의 Gold 마트 데이터가 없습니다.",
            "data_basis": [],
            "bedrock_meta": {},
        }

    # Step 2: 프롬프트 렌더링
    template = _load_prompt("repo_insight_v1")
    repo_table = _render_repo_table(rows)
    system_text, user_text = _build_prompt(template, date, repo_table)

    # Step 3: Bedrock 호출
    logger.info("insights.bedrock model=%s", os.environ.get("OI_BEDROCK_MODEL_ID"))
    result = bedrock_client.invoke_with_meta(
        user_text=user_text,
        system=system_text,
        max_tokens=1500,
        temperature=0.4,
    )
    logger.info(
        "insights.done tokens_in=%d tokens_out=%d latency=%dms",
        result["input_tokens"], result["output_tokens"], result["latency_ms"],
    )

    return {
        "date": date,
        "insight_markdown": result["text"],
        "data_basis": rows,
        "bedrock_meta": {
            "input_tokens": result["input_tokens"],
            "output_tokens": result["output_tokens"],
            "latency_ms": result["latency_ms"],
            "stop_reason": result["stop_reason"],
        },
    }
