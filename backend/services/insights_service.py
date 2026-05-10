"""
services/insights_service.py
Gold 마트 조회 → 프롬프트 렌더링 → Bedrock 호출 → 카테고리 머지 → OpenSearch 인덱싱 E2E.

athena_client + bedrock_client + opensearch_client 를 묶는 오케스트레이터.
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


def _merge_categories(rows: list, date: str) -> None:
    """
    F5: OpenSearch 에서 해당 날짜의 카테고리 정보를 조회해 rows 에 in-place 머지.

    - 카테고리 분류는 별도 배치 (Airflow 또는 수동 트리거) 에서 채워짐
    - 이 함수 호출 시점:
      * 분류 전: 모든 row 의 category=None (UI 에서 "Other" 그룹으로 표시)
      * 분류 후: 실제 카테고리 값
    - OpenSearch 실패 시 모든 row 에 category=None 설정 (best-effort)
    """
    try:
        from services import opensearch_client
        os_result = opensearch_client.get_by_date(date=date, size=20)
        # repo_name → {category, confidence} 매핑 dict
        cat_map = {
            hit["repo_name"]: {
                "category": hit.get("category"),
                "category_confidence": hit.get("category_confidence"),
            }
            for hit in os_result.get("hits", [])
            if hit.get("repo_name")
        }
        # rows 에 머지
        for row in rows:
            cat_info = cat_map.get(row.get("repo_name"), {})
            row["category"] = cat_info.get("category")
            row["category_confidence"] = cat_info.get("category_confidence")
        logger.info(
            "insights.category_merge matched=%d/%d",
            sum(1 for r in rows if r.get("category")),
            len(rows),
        )
    except Exception as e:
        # OpenSearch 다운 등 — 인사이트 응답엔 영향 없음. 카테고리만 비어있게.
        logger.warning("insights.category_merge failed (non-fatal): %s", e)
        for row in rows:
            row.setdefault("category", None)
            row.setdefault("category_confidence", None)


def get_daily_insights(date: str) -> dict:
    """
    E2E: Athena Gold 조회 → 프롬프트 렌더링 → Bedrock 호출 → 카테고리 머지 → OpenSearch 인덱싱 → dict 반환.

    Args:
        date: 'YYYY-MM-DD' 형식
    Returns:
        {
          "date": str,
          "insight_markdown": str,   # Bedrock 응답 (마크다운)
          "data_basis": list[dict],  # 근거 데이터 (UI 토글용 + F5 카테고리 포함)
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
            "insight_markdown": (
                f"### {date} 분석 결과가 아직 준비되지 않았습니다\n\n"
                "이 날짜의 일별 종합 분석은 아직 만들어지지 않았어요.\n\n"
                "- **언제 보이나요?** 일별 분석은 **한국시간 매일 오전 10시**에 어제 자료 기준으로 자동 갱신됩니다. "
                "오늘 날짜(한국시간 기준)를 선택했다면 내일 오전 10시 이후에 확인할 수 있어요.\n"
                "- **어제·그제 등 과거 날짜인데 비어있다면?** 데이터 수집이나 집계 과정에서 일시적으로 누락됐을 수 있습니다. "
                "잠시 후 다시 시도해 주세요.\n"
                "- 지금 진행 중인 활동이 궁금하다면 상단의 **실시간 GitHub 활동** 과 **오늘 시간대별 진행** 카드에서 바로 확인할 수 있어요."
            ),
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

    insight_md = result["text"]

    # Step 4: F5 카테고리 머지 (OpenSearch 에 이미 분류된 결과가 있으면 채움)
    _merge_categories(rows, date)

    # Step 4.5: 카테고리 미분류 row 가 있으면 inline batch 분류 (단일 Bedrock 호출)
    # - 같은 호출에서 만든 인사이트 마크다운을 컨텍스트로 같이 넣음 → 정확도 ↑
    # - 직렬 10× 호출 (~10-20초) 대신 1× 호출 (~3-5초) 로 응답 시간 보호
    # - OpenSearch 에 카테고리 색인이 비어있는 첫 진입에서 사용자가 즉시 카테고리 보게 함
    # - categorize_daily DAG 는 backup 으로 유지 (다른 사용자가 안 본 날짜 채워줌)
    _missing = [r for r in rows if not r.get("category")]
    if _missing:
        try:
            from services import category_service
            results = category_service.classify_repos_batch(_missing, insight_md, date)
            result_map = {res["repo_name"]: res for res in results if res.get("repo_name")}
            for row in rows:
                if row.get("category"):
                    continue
                m = result_map.get(row.get("repo_name"))
                if m:
                    row["category"] = m["category"]
                    row["category_confidence"] = m["confidence"]
                    row["category_reasoning"] = m["reasoning"]
            logger.info(
                "insights.batch_classify filled=%d/%d (missing was %d)",
                sum(1 for r in rows if r.get("category")),
                len(rows), len(_missing),
            )
        except Exception as e:
            logger.warning("insights.batch_classify failed (non-fatal): %s", e)
            # 실패해도 row.category 는 None 으로 남음 → UI 에서 "Other" 로 fallback

    # Step 5: OpenSearch 인덱싱 (best-effort)
    # - 메인 페이지 진입 → 인사이트 생성 → 자동 색인 → 검색 페이지에서 즉시 조회 가능
    # - 색인 실패해도 인사이트 응답은 정상 반환 (검색 기능만 일시 불가)
    # - import 를 함수 안에 둔 이유: OpenSearch 컨테이너가 fastapi startup 시점에
    #   안 떠있어도 fastapi 자체는 뜨도록 안전장치 (모듈-레벨 import 회피)
    # - opensearch_client.index_daily 는 partial update 모드라
    #   카테고리 필드(category, category_confidence, category_reasoning,
    #   categorized_at) 를 건드리지 않음 — 배치가 채워둔 값이 살아남음
    try:
        from services import opensearch_client
        idx_result = opensearch_client.index_daily(
            date=date,
            rows=rows,
            insight_markdown=insight_md,
        )
        logger.info(
            "insights.opensearch indexed=%d errors=%d",
            idx_result["indexed"], idx_result["errors"],
        )
    except Exception as e:
        # 인덱싱 실패는 인사이트 응답에 영향 없음 — 검색 기능만 일시 불가
        logger.warning("insights.opensearch failed (non-fatal): %s", e)

    return {
        "date": date,
        "insight_markdown": insight_md,
        "data_basis": rows,
        "bedrock_meta": {
            "input_tokens": result["input_tokens"],
            "output_tokens": result["output_tokens"],
            "latency_ms": result["latency_ms"],
            "stop_reason": result["stop_reason"],
        },
    }