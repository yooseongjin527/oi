"""categorize_daily — F5 카테고리 분류 자동 배치 (Day 5)

silver_to_gold 가 끝난 후 Dataset 트리거로 자동 실행.

흐름:
  1. OpenSearch oi-repo-daily 에서 해당 날짜의 Top 색인 repo 조회
  2. 이미 categorized_at 가 있는 repo 는 skip (멱등)
  3. 각 repo 에 대해 Bedrock Converse API 직접 호출 → JSON 파싱
  4. OpenSearch partial update (category, confidence, reasoning, categorized_at)

backend/services/category_service.py 와 동일한 로직을 DAG 안에 인라인.
backend 컨테이너와 분리된 Airflow 워커에서 import 못 하니 어쩔 수 없는 중복.
변경 시 양쪽 다 갱신할 것.

환경 변수 (docker-compose.yml 의 airflow-scheduler 에 추가됨):
  - AWS_REGION, AWS_S3_BUCKET   (이미 있음)
  - OI_BEDROCK_REGION           (없으면 AWS_REGION fallback)
  - OI_BEDROCK_MODEL_ID         (Haiku 4.5 default)
  - OI_OPENSEARCH_HOST          (도커 네트워크: http://opensearch:9200)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# ─── 설정 ──────────────────────────────────────────────
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
BUCKET = os.environ.get("AWS_S3_BUCKET", "oi-data-lake")

BEDROCK_REGION = os.environ.get("OI_BEDROCK_REGION", REGION)
BEDROCK_MODEL_ID = os.environ.get(
    "OI_BEDROCK_MODEL_ID",
    "global.anthropic.claude-haiku-4-5-20251001-v1:0",
)
OPENSEARCH_HOST = os.environ.get("OI_OPENSEARCH_HOST", "http://opensearch:9200")

INDEX_NAME = "oi-repo-daily"
ALLOWED_CATEGORIES = {"AI/ML", "Web", "Infra", "DevTools", "Game", "Other"}



# ─── 프롬프트 (backend/prompts/repo_category_v1.md 와 동일 톤, 간략화) ──
CATEGORY_SYSTEM = (
    "너는 GitHub repository 분류 전문가다. "
    "입력으로 repo 이름, 주요 이벤트 타입, 인사이트 발췌가 주어진다. "
    "이 repo 를 정확히 한 카테고리로 분류해라.\n\n"
    "허용 카테고리: AI/ML | Web | Infra | DevTools | Game | Other\n"
    "응답 형식은 JSON 만. 다른 설명 절대 추가 금지.\n"
    '{"category": "...", "confidence": 0.0~1.0, "reasoning": "한 줄 설명"}'
)


def _build_user_prompt(repo_name: str, dominant: str, event_count: int, excerpt: str) -> str:
    return (
        f"repo: {repo_name}\n"
        f"dominant_event: {dominant or 'Unknown'}\n"
        f"event_count: {event_count}\n"
        f"insight_excerpt:\n{excerpt or '(없음)'}\n"
    )


# ─── Bedrock Converse 호출 ─────────────────────────────
def _bedrock_classify(client, repo_name: str, dominant: str, event_count: int, excerpt: str) -> dict[str, Any]:
    user_text = _build_user_prompt(repo_name, dominant, event_count, excerpt)
    resp = client.converse(
        modelId=BEDROCK_MODEL_ID,
        system=[{"text": CATEGORY_SYSTEM}],
        messages=[{"role": "user", "content": [{"text": user_text}]}],
        inferenceConfig={"maxTokens": 200, "temperature": 0.0},
    )
    text = resp["output"]["message"]["content"][0]["text"]
    return _parse_json(text)


def _parse_json(text: str) -> dict[str, Any]:
    """Claude 응답에서 JSON 추출 — 마크다운 ```json``` 또는 raw."""
    candidate = text
    if "```json" in candidate:
        candidate = candidate.split("```json", 1)[1].split("```", 1)[0]
    elif "```" in candidate:
        candidate = candidate.split("```", 1)[1].split("```", 1)[0]
    else:
        s, e = candidate.find("{"), candidate.rfind("}")
        if s != -1 and e != -1:
            candidate = candidate[s : e + 1]

    try:
        parsed = json.loads(candidate.strip())
    except json.JSONDecodeError:
        return {"category": "Other", "confidence": 0.0, "reasoning": "JSON parse failed"}

    cat = parsed.get("category", "Other")
    if cat not in ALLOWED_CATEGORIES:
        cat = "Other"
    conf = max(0.0, min(1.0, float(parsed.get("confidence", 0.0))))
    reasoning = str(parsed.get("reasoning", ""))[:200]
    return {"category": cat, "confidence": conf, "reasoning": reasoning}


# ─── OpenSearch helpers ────────────────────────────────
def _make_os_client():
    from opensearchpy import OpenSearch, RequestsHttpConnection
    host_url = OPENSEARCH_HOST
    use_ssl = host_url.startswith("https://")
    host_part = host_url.replace("https://", "").replace("http://", "")
    if ":" in host_part:
        host, port_str = host_part.rsplit(":", 1)
        port = int(port_str)
    else:
        host, port = host_part, 9200
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_compress=True,
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
        timeout=30,
    )


def _extract_excerpt(insight_md: str | None, repo_name: str, max_chars: int = 800) -> str:
    if not insight_md or not repo_name or repo_name not in insight_md:
        return ""
    parts = insight_md.split("\n#### ")
    for part in parts:
        if repo_name in part:
            if not part.startswith("#"):
                part = "#### " + part
            return part.strip()[:max_chars]
    return ""


# ─── DAG ───────────────────────────────────────────────
@dag(
    dag_id="categorize_daily",
    start_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
    schedule="0 4 * * *",           # silver_to_gold(03:00) 후 1시간 lag
    catchup=False,                  # 카테고리는 과거분 backfill 가치 낮음 + Bedrock 비용
    max_active_runs=1,
    default_args={"owner": "jin", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["category", "bedrock", "opensearch"],
)
def categorize_daily_dag():

    @task
    def categorize(logical_date=None) -> dict[str, Any]:
        d = logical_date.astimezone(timezone.utc)
        date_str = d.strftime("%Y-%m-%d")

        os_client = _make_os_client()
        bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

        # 해당 날짜의 색인된 repo 조회
        try:
            resp = os_client.search(
                index=INDEX_NAME,
                body={
                    "size": 50,
                    "query": {"term": {"date": date_str}},
                    "sort": [{"rank_score": {"order": "desc"}}],
                },
            )
        except Exception as e:
            logger.warning(
                "OpenSearch query failed (%s) — silver/gold 빌드만 끝났고 인사이트 호출 전이면 정상. skip.",
                e,
            )
            return {"date": date_str, "total": 0, "skipped": True}

        hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
        if not hits:
            logger.info(
                "No indexed docs for %s — 사용자가 /api/insights/daily?date=%s 를 한 번 호출해야 인덱싱됨.",
                date_str, date_str,
            )
            return {"date": date_str, "total": 0, "skipped": True}

        classified = skipped = errors = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for hit in hits:
            repo_name = hit.get("repo_name")
            if not repo_name:
                continue

            # 멱등성
            if hit.get("categorized_at"):
                skipped += 1
                continue

            excerpt = _extract_excerpt(hit.get("insight_markdown"), repo_name)

            try:
                result = _bedrock_classify(
                    bedrock,
                    repo_name=repo_name,
                    dominant=hit.get("dominant_event_type") or "",
                    event_count=int(hit.get("event_count") or 0),
                    excerpt=excerpt,
                )
            except Exception as e:
                logger.exception("Bedrock classify failed: %s", repo_name)
                errors += 1
                continue

            doc_id = f"{date_str}_{repo_name.replace('/', '_')}"
            try:
                os_client.update(
                    index=INDEX_NAME,
                    id=doc_id,
                    body={
                        "doc": {
                            "category": result["category"],
                            "category_confidence": result["confidence"],
                            "category_reasoning": result["reasoning"],
                            "categorized_at": now_iso,
                        }
                    },
                )
                classified += 1
                logger.info("classified %s -> %s (%.2f)",
                            repo_name, result["category"], result["confidence"])
            except Exception as e:
                logger.exception("OpenSearch update failed: %s", doc_id)
                errors += 1

        logger.info(
            "categorize_daily %s: total=%d classified=%d skipped=%d errors=%d",
            date_str, len(hits), classified, skipped, errors,
        )
        return {
            "date": date_str,
            "total": len(hits),
            "classified": classified,
            "skipped": skipped,
            "errors": errors,
        }

    categorize()


dag_instance = categorize_daily_dag()
