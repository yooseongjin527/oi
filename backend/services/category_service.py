"""
services/category_service.py
F5 카테고리 분류 — Bedrock 호출 + OpenSearch partial update.

설계:
- 단일 repo 분류 (classify_repo) → 6개 카테고리 + confidence + reasoning
- 일별 batch 분류 (categorize_daily) → top-N repo 한 번에 처리
- 멱등성: categorized_at 이미 있으면 force=True 아닌 한 스킵

호출 비용 (Haiku 4.5 기준):
- 입력: 프롬프트 ~600 + 인사이트 발췌 ~150 = 750 tokens / repo
- 출력: ~50 tokens / repo (JSON 짧음)
- Top-10 분류 시 ~$0.001 (사실상 무료)
- 직렬 처리 latency: 10~20초 (Airflow 배치라 OK)
"""
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from services import bedrock_client, opensearch_client

logger = logging.getLogger(__name__)


# ─── 상수 ──────────────────────────────────────────────

# 허용된 카테고리 — 프롬프트와 일치해야 함
ALLOWED_CATEGORIES = {"AI/ML", "Web", "Infra", "DevTools", "Game", "Other"}

# 프롬프트 템플릿 경로 (insights_service.py 와 동일 패턴)
_PROMPT_DIR = Path(__file__).parent.parent / "prompts"


def _load_prompt(name: str) -> str:
    return (_PROMPT_DIR / f"{name}.md").read_text(encoding="utf-8")


def _build_prompt(template_md: str, **kwargs) -> tuple[str, str]:
    """## system / ## user_template 분리 + placeholder 치환"""
    parts = template_md.split("## user_template")
    system_raw = parts[0].replace("## system", "").strip()
    user_raw = parts[1].strip() if len(parts) > 1 else ""

    # {{key}} 치환
    user_text = user_raw
    for key, val in kwargs.items():
        user_text = user_text.replace(f"{{{{{key}}}}}", str(val))

    return system_raw, user_text


# ─── 인사이트 발췌 (repo_service.py 의 로직 재사용 + 길이 제한) ─

def _extract_repo_excerpt(insight_md: str | None, repo_name: str, max_chars: int = 800) -> str:
    """
    인사이트 마크다운 전체에서 해당 repo 섹션만 발췌.
    프롬프트 토큰 절약 위해 max_chars 로 잘라냄.
    매칭 안 되면 빈 문자열 반환 (Bedrock 이 repo_name 만으로 분류).
    """
    if not insight_md or repo_name not in insight_md:
        return ""

    parts = insight_md.split("\n#### ")
    for part in parts:
        if repo_name in part:
            if not part.startswith("#"):
                part = "#### " + part
            section = part.strip()
            return section[:max_chars]

    return ""


# ─── JSON 응답 파싱 (방어적) ───────────────────────────

def _parse_category_response(text: str) -> dict[str, Any]:
    """
    Bedrock 응답 텍스트에서 JSON 추출.
    Claude 가 가끔 마크다운 ```json 블록으로 감싸거나 앞뒤 설명을 붙임 → 방어적 파싱.

    Returns: {"category": str, "confidence": float, "reasoning": str}
    실패 시: fallback to Other / 0.0
    """
    # 1. ```json ... ``` 블록 우선
    if "```json" in text:
        try:
            chunk = text.split("```json", 1)[1].split("```", 1)[0]
            return _validate_response(json.loads(chunk.strip()))
        except (json.JSONDecodeError, KeyError, IndexError):
            pass

    # 2. ``` ... ``` (언어 지정 없이) 블록
    if "```" in text:
        try:
            chunk = text.split("```", 1)[1].split("```", 1)[0]
            return _validate_response(json.loads(chunk.strip()))
        except (json.JSONDecodeError, KeyError, IndexError):
            pass

    # 3. raw JSON (제일 흔한 케이스)
    try:
        # 첫 { 부터 마지막 } 까지
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            return _validate_response(json.loads(text[start : end + 1]))
    except json.JSONDecodeError:
        pass

    # 4. 모든 시도 실패 → fallback
    logger.warning("Failed to parse category JSON: %r", text[:200])
    return {"category": "Other", "confidence": 0.0, "reasoning": "JSON parse failed"}


def _validate_response(parsed: dict) -> dict[str, Any]:
    """카테고리 값 화이트리스트 검증 + 타입 정규화"""
    category = parsed.get("category", "Other")
    if category not in ALLOWED_CATEGORIES:
        logger.warning("Unknown category from Bedrock: %r → fallback Other", category)
        category = "Other"

    confidence = float(parsed.get("confidence", 0.0))
    confidence = max(0.0, min(1.0, confidence))  # clamp [0, 1]

    reasoning = str(parsed.get("reasoning", ""))[:200]  # 길이 제한

    return {"category": category, "confidence": confidence, "reasoning": reasoning}


# ─── 단일 repo 분류 ─────────────────────────────────────

def classify_repo(
    repo_name: str,
    dominant_event_type: str | None,
    event_count: int | None,
    insight_excerpt: str = "",
) -> dict[str, Any]:
    """
    단일 repo 를 카테고리로 분류.

    Returns:
        {
          "category": "AI/ML" | "Web" | ...,
          "confidence": float,
          "reasoning": str,
          "latency_ms": int,
        }
    """
    template = _load_prompt("repo_category_v1")
    system_text, user_text = _build_prompt(
        template,
        repo_name=repo_name,
        dominant_event_type=dominant_event_type or "Unknown",
        event_count=event_count if event_count is not None else "?",
        insight_excerpt=insight_excerpt or "(인사이트 발췌 없음)",
    )

    logger.info("category.classify repo=%s", repo_name)
    result = bedrock_client.invoke_with_meta(
        user_text=user_text,
        system=system_text,
        max_tokens=200,       # JSON 짧으니 200 이면 충분
        temperature=0.0,      # 결정성 우선
    )

    parsed = _parse_category_response(result["text"])
    parsed["latency_ms"] = result["latency_ms"]
    logger.info(
        "category.classify done repo=%s category=%s conf=%.2f",
        repo_name, parsed["category"], parsed["confidence"],
    )
    return parsed


# ─── 일별 배치 분류 (메인 진입점) ──────────────────────

def categorize_daily(date: str, force: bool = False) -> dict[str, Any]:
    """
    특정 날짜의 모든 색인된 repo 를 분류 + OpenSearch partial update.

    Args:
        date: YYYY-MM-DD
        force: True 면 categorized_at 이 있어도 재분류

    Returns:
        {
          "date": str,
          "total": int,           # 대상 repo 수
          "classified": int,      # 실제 분류 실행 수 (skip 제외)
          "skipped": int,         # 이미 분류됨
          "errors": int,          # Bedrock/파싱 실패
          "results": [{repo_name, category, confidence}, ...]
        }
    """
    logger.info("category.batch.start date=%s force=%s", date, force)

    # 1. 해당 날짜 색인 repo 조회
    indexed = opensearch_client.get_by_date(date=date, size=50)
    hits = indexed.get("hits", [])
    if not hits:
        logger.warning("No indexed docs for date=%s", date)
        return {"date": date, "total": 0, "classified": 0, "skipped": 0, "errors": 0, "results": []}

    # 2. 각 repo 분류 + bulk update payload 생성
    bulk_actions = []
    results = []
    classified = 0
    skipped = 0
    errors = 0
    now_iso = datetime.utcnow().isoformat() + "Z"

    for hit in hits:
        repo_name = hit.get("repo_name")
        if not repo_name:
            continue

        # 멱등성: 이미 분류됐으면 스킵 (force=True 가 아닐 때만)
        if not force and hit.get("categorized_at"):
            skipped += 1
            results.append({
                "repo_name": repo_name,
                "category": hit.get("category"),
                "confidence": hit.get("category_confidence"),
                "skipped": True,
            })
            continue

        # 인사이트 발췌
        excerpt = _extract_repo_excerpt(
            hit.get("insight_markdown"), repo_name, max_chars=800
        )

        # Bedrock 호출
        try:
            classification = classify_repo(
                repo_name=repo_name,
                dominant_event_type=hit.get("dominant_event_type"),
                event_count=hit.get("event_count"),
                insight_excerpt=excerpt,
            )
        except Exception as e:
            logger.exception("category.classify failed repo=%s", repo_name)
            errors += 1
            continue

        classified += 1

        # OpenSearch bulk update payload (partial — 카테고리 필드만 덮어쓰기)
        doc_id = f"{date}_{repo_name.replace('/', '_')}"
        bulk_actions.append({
            "_op_type": "update",
            "_index": opensearch_client.INDEX_NAME,
            "_id": doc_id,
            "doc": {
                "category": classification["category"],
                "category_confidence": classification["confidence"],
                "category_reasoning": classification["reasoning"],
                "categorized_at": now_iso,
            },
        })

        results.append({
            "repo_name": repo_name,
            "category": classification["category"],
            "confidence": classification["confidence"],
            "reasoning": classification["reasoning"],
        })

    # 3. OpenSearch bulk update 실행 (한 번에)
    if bulk_actions:
        from opensearchpy.helpers import bulk as os_bulk
        client = opensearch_client._get_client()
        success, bulk_errors = os_bulk(client, bulk_actions, raise_on_error=False)
        bulk_error_count = len(bulk_errors) if isinstance(bulk_errors, list) else 0
        logger.info(
            "category.batch.bulk_update success=%d errors=%d",
            success, bulk_error_count,
        )
        if bulk_error_count > 0:
            errors += bulk_error_count

    logger.info(
        "category.batch.done date=%s total=%d classified=%d skipped=%d errors=%d",
        date, len(hits), classified, skipped, errors,
    )

    return {
        "date": date,
        "total": len(hits),
        "classified": classified,
        "skipped": skipped,
        "errors": errors,
        "results": results,
    }