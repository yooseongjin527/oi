"""GitHub Events API → Redpanda collector.

핸드오프 §3.1 / §10.3 정책:
- 60초 간격 polling
- ETag 캐시로 304 처리 (rate limit 절약)
- 최대 3페이지 = 300건/요청
- repo.id 기반 파티션 키 (같은 repo는 같은 파티션 → 순서 보장)
- 토큰 있으면 5,000/h, 없으면 60/h (개발 OK, EC2는 PAT 권장)
- 종료 시 producer flush + close (메시지 유실 방지)

토픽: gh.events.live (3 파티션, RF 1)
스키마: GitHub Events API 응답 그대로 + ingested_at 추가
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ─── 설정 ────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_LIVE", "gh.events.live")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SEC", "60"))
MAX_PAGES = int(os.getenv("MAX_PAGES_PER_POLL", "3"))
NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))

GITHUB_EVENTS_URL = "https://api.github.com/events"

# ─── 로깅 ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("collector")


# ─── 토픽 보장 ───────────────────────────────────────────
def ensure_topic_exists() -> None:
    """토픽이 없으면 생성. Already exists면 무시."""
    for attempt in range(1, 11):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="oi-collector-admin",
                request_timeout_ms=10_000,
            )
            try:
                admin.create_topics([
                    NewTopic(
                        name=KAFKA_TOPIC,
                        num_partitions=NUM_PARTITIONS,
                        replication_factor=1,
                    )
                ])
                log.info(f"✅ 토픽 생성: {KAFKA_TOPIC} (partitions={NUM_PARTITIONS})")
            except TopicAlreadyExistsError:
                log.info(f"ℹ️  토픽 이미 존재: {KAFKA_TOPIC}")
            finally:
                admin.close()
            return
        except Exception as e:
            log.warning(f"⏳ Redpanda 연결 대기 중 ({attempt}/10): {type(e).__name__}: {e}")
            time.sleep(3)
    log.error("❌ Redpanda에 연결할 수 없음. 종료.")
    sys.exit(1)


# ─── Kafka Producer ──────────────────────────────────────
def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks="all",
        retries=5,
        linger_ms=50,        # 약간 모아서 보내기
        compression_type="gzip",
        client_id="oi-collector",
    )


# ─── GitHub Events Fetcher ──────────────────────────────
class GitHubEventsFetcher:
    """ETag 캐시를 페이지별로 따로 관리."""

    def __init__(self) -> None:
        self.session = requests.Session()
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "oi-collector/0.1",
        }
        if GITHUB_TOKEN:
            headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
            log.info("🔑 GITHUB_TOKEN 사용 (rate limit 5000/h)")
        else:
            log.warning("⚠️  GITHUB_TOKEN 미설정 (rate limit 60/h)")
        self.session.headers.update(headers)
        self.etag_cache: dict[int, str] = {}

    def fetch_page(self, page: int) -> tuple[list, bool]:
        """한 페이지 받아옴.

        Returns:
            (events, was_modified)
            - events: 이벤트 리스트 (304면 빈 리스트)
            - was_modified: 200(True) / 304(False)
        """
        headers = {}
        if etag := self.etag_cache.get(page):
            headers["If-None-Match"] = etag

        resp = self.session.get(
            GITHUB_EVENTS_URL,
            params={"per_page": 100, "page": page},
            headers=headers,
            timeout=15,
        )

        # rate limit 정보 로깅 (남은 횟수가 적을 때만)
        remaining = int(resp.headers.get("X-RateLimit-Remaining", "999"))
        if remaining < 50:
            reset = resp.headers.get("X-RateLimit-Reset", "?")
            log.warning(f"⚠️  rate limit 잔여 {remaining}, reset@{reset}")

        if resp.status_code == 304:
            return [], False

        if resp.status_code != 200:
            log.error(f"❌ GitHub API {resp.status_code}: {resp.text[:200]}")
            return [], False

        # ETag 저장
        if etag := resp.headers.get("ETag"):
            self.etag_cache[page] = etag

        return resp.json(), True


# ─── Graceful Shutdown ──────────────────────────────────
class ShutdownFlag:
    def __init__(self) -> None:
        self.stop = False

    def request_stop(self, signum, frame) -> None:
        log.info(f"🛑 신호 수신 ({signum}). graceful shutdown 시작.")
        self.stop = True


# ─── Main Loop ──────────────────────────────────────────
def main() -> None:
    log.info("=" * 60)
    log.info("OI Collector 시작")
    log.info(f"  bootstrap : {KAFKA_BOOTSTRAP}")
    log.info(f"  topic     : {KAFKA_TOPIC}")
    log.info(f"  interval  : {POLL_INTERVAL}s")
    log.info(f"  max_pages : {MAX_PAGES}")
    log.info("=" * 60)

    ensure_topic_exists()

    producer = build_producer()
    fetcher = GitHubEventsFetcher()
    flag = ShutdownFlag()
    signal.signal(signal.SIGTERM, flag.request_stop)
    signal.signal(signal.SIGINT, flag.request_stop)

    try:
        while not flag.stop:
            cycle_start = time.time()
            total_published = 0
            total_pages_modified = 0

            for page in range(1, MAX_PAGES + 1):
                if flag.stop:
                    break
                events, modified = fetcher.fetch_page(page)
                if not modified:
                    continue
                total_pages_modified += 1

                ingested_at = datetime.now(timezone.utc).isoformat()
                for ev in events:
                    # repo.id를 파티션 키로 → 같은 repo 이벤트는 같은 파티션
                    repo = ev.get("repo") or {}
                    key = repo.get("id")
                    ev["_ingested_at"] = ingested_at
                    producer.send(KAFKA_TOPIC, key=key, value=ev)
                    total_published += 1

            producer.flush(timeout=10)

            elapsed = time.time() - cycle_start
            log.info(
                f"📦 cycle: published={total_published:>3d}  "
                f"modified_pages={total_pages_modified}/{MAX_PAGES}  "
                f"took={elapsed:.1f}s"
            )

            # 다음 사이클까지 대기 (interruptible)
            sleep_remaining = POLL_INTERVAL - elapsed
            while sleep_remaining > 0 and not flag.stop:
                step = min(1.0, sleep_remaining)
                time.sleep(step)
                sleep_remaining -= step

    finally:
        log.info("⏹  producer flush + close")
        try:
            producer.flush(timeout=10)
        except Exception as e:
            log.error(f"flush 중 오류: {e}")
        producer.close(timeout=10)
        log.info("👋 종료 완료")


if __name__ == "__main__":
    main()
