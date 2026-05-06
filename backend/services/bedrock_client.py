"""
services/bedrock_client.py
AWS Bedrock Converse API 클라이언트 (Claude Haiku 4.5).

설계 노트:
- Converse API 사용 (Anthropic messages 포맷, system / inferenceConfig 분리).
- 환경변수에서 region / model id 읽음. fallback 은 env 변경으로 처리 (코드는 단순 유지).
- 동기 호출만 제공. FastAPI 라우터에서 asyncio.to_thread() 로 감싸서 사용.
"""
import os
import logging
from typing import Optional

import boto3

logger = logging.getLogger(__name__)

# 환경변수 — .envrc 에서 export 되어 있어야 함
_REGION = os.environ.get("OI_BEDROCK_REGION", "ap-northeast-2")
_MODEL_ID = os.environ.get(
    "OI_BEDROCK_MODEL_ID",
    "global.anthropic.claude-haiku-4-5-20251001-v1:0",
)

# bedrock-runtime 클라이언트는 모듈 로드 시 1회만 생성 (재사용)
_client = boto3.client("bedrock-runtime", region_name=_REGION)


class BedrockInvokeError(RuntimeError):
    """Bedrock 호출 실패 시 raise."""
    pass


def invoke(
    user_text: str,
    system: Optional[str] = None,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> str:
    """
    단일 user 메시지 → assistant 응답 텍스트 반환.
    가장 흔한 사용 패턴 (인사이트 서비스에서 사용).

    Args:
        user_text: user 메시지 본문
        system: system prompt (None 이면 system 블록 생략)
        max_tokens: 응답 최대 토큰
        temperature: 0.0 ~ 1.0
    Returns:
        assistant 응답 텍스트
    """
    result = invoke_with_meta(
        user_text=user_text,
        system=system,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return result["text"]


def invoke_with_meta(
    user_text: str,
    system: Optional[str] = None,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> dict:
    """
    invoke() 와 같지만 토큰 사용량 / latency 메타도 반환.
    운영 로깅 / 비용 추적용.

    Returns:
        {
          "text": str,
          "input_tokens": int,
          "output_tokens": int,
          "latency_ms": int,
          "stop_reason": str,
        }
    """
    # Converse API 페이로드 구성
    kwargs = {
        "modelId": _MODEL_ID,
        "messages": [
            {"role": "user", "content": [{"text": user_text}]},
        ],
        "inferenceConfig": {
            "maxTokens": max_tokens,
            "temperature": temperature,
        },
    }
    # system 은 있을 때만 추가 (빈 list 보내면 일부 region 에러)
    if system:
        kwargs["system"] = [{"text": system}]

    try:
        resp = _client.converse(**kwargs)
    except Exception as e:
        # ClientError, EndpointConnectionError 등 모두 wrap
        logger.exception("bedrock.converse failed")
        raise BedrockInvokeError(str(e)) from e

    # 응답 파싱 — Converse API 표준 구조
    try:
        text = resp["output"]["message"]["content"][0]["text"]
    except (KeyError, IndexError) as e:
        raise BedrockInvokeError(f"unexpected response shape: {resp}") from e

    usage = resp.get("usage", {})
    metrics = resp.get("metrics", {})

    return {
        "text": text,
        "input_tokens": usage.get("inputTokens", 0),
        "output_tokens": usage.get("outputTokens", 0),
        "latency_ms": metrics.get("latencyMs", 0),
        "stop_reason": resp.get("stopReason", ""),
    }
