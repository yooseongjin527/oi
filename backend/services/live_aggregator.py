"""실시간 라이브 집계기.

Redpanda `gh.events.live` 토픽을 백그라운드에서 컨슘하고,
1분 단위 sliding window (최근 60분) 카운터를 메모리에 유지.

설계:
- FastAPI lifespan 에서 시작 / 종료 (별도 컨테이너 없음)
- aiokafka 비동기 컨슈머
- consumer group_id = unique per process boot → 모든 메시지 받음
- auto_offset_reset='latest' → 과거 적재 안 함 (메모리 보호)
- asyncio.Lock 으로 read/write 보호 (단일 프로세스 가정)

내보내는 스냅샷 구조 (snapshot()):
{
  "now": iso8601,
  "started_at": iso8601 | null,
  "connected": bool,
  "lag_seconds": float,
  "current_minute": {ts, events, unique_repos, by_type},
  "buckets": [{ts, events, unique_repos, by_type}, ...],     # 최근 60분, 오래된→최신
  "totals_60min": {events, unique_repos, unique_actors},
  "by_type_60min": {PushEvent: N, ...},
  "top_5min": [{repo_name, events, primary_type}, ...],      # Top 10
}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ─── 설정 ────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_LIVE", "gh.events.live")
WINDOW_MINUTES = int(os.getenv("LIVE_WINDOW_MINUTES", "60"))
TOP_WINDOW_MINUTES = int(os.getenv("LIVE_TOP_WINDOW_MINUTES", "5"))
TOP_N = int(os.getenv("LIVE_TOP_N", "10"))
MAX_REPO_SET_PER_BUCKET = int(os.getenv("LIVE_MAX_REPO_SET", "5000"))


def _floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


@dataclass
class MinuteBucket:
    """1분 단위 누적 카운터."""
    ts: datetime
    events: int = 0
    by_type: Counter = field(default_factory=Counter)
    repos: set[str] = field(default_factory=set)
    actors: set[int] = field(default_factory=set)
    repo_events: Counter = field(default_factory=Counter)
    # repo_name -> dominant type 추적용
    repo_types: dict[str, Counter] = field(default_factory=dict)

    def add(self, event_type: str, repo_name: str | None, repo_id: int | None,
            actor_id: int | None) -> None:
        self.events += 1
        self.by_type[event_type] += 1
        if repo_name:
            # set 메모리 폭주 방지 (드물지만 안전장치)
            if len(self.repos) < MAX_REPO_SET_PER_BUCKET:
                self.repos.add(repo_name)
            self.repo_events[repo_name] += 1
            tc = self.repo_types.get(repo_name)
            if tc is None:
                tc = Counter()
                self.repo_types[repo_name] = tc
            tc[event_type] += 1
        if actor_id is not None and len(self.actors) < MAX_REPO_SET_PER_BUCKET:
            self.actors.add(actor_id)


class LiveAggregator:
    """Redpanda 컨슈머 + sliding window 상태."""

    def __init__(self) -> None:
        self._buckets: deque[MinuteBucket] = deque(maxlen=WINDOW_MINUTES)
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._consumer = None
        self._connected = False
        self._started_at: Optional[datetime] = None
        self._last_event_at: Optional[datetime] = None
        self._stop = asyncio.Event()
        self._messages_consumed = 0

    # ── 라이프사이클 ─────────────────────────────────────
    async def start(self) -> None:
        if self._task is not None:
            return
        self._started_at = datetime.now(timezone.utc)
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="live-aggregator")
        logger.info("LiveAggregator started")

    async def stop(self) -> None:
        self._stop.set()
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning("consumer.stop error: %s", e)
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        logger.info("LiveAggregator stopped")

    # ── 컨슈머 루프 ──────────────────────────────────────
    async def _run(self) -> None:
        # aiokafka 는 동적 import — 미설치 시 명확한 에러
        try:
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            logger.error("aiokafka 가 설치되어있지 않음. requirements.txt 확인")
            return

        # 부팅마다 unique group_id → 모든 메시지를 이 인스턴스 메모리에 받기 위해
        group_id = f"oi-live-{socket.gethostname()}-{int(time.time())}"

        backoff = 2.0
        while not self._stop.is_set():
            try:
                self._consumer = AIOKafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id=group_id,
                    auto_offset_reset="latest",  # 과거 안 받음
                    enable_auto_commit=False,
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
                    client_id="oi-live-aggregator",
                )
                await self._consumer.start()
                self._connected = True
                backoff = 2.0
                logger.info("LiveAggregator connected to %s topic=%s group=%s",
                            KAFKA_BOOTSTRAP, KAFKA_TOPIC, group_id)

                async for msg in self._consumer:
                    if self._stop.is_set():
                        break
                    await self._ingest(msg.value)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                logger.warning("LiveAggregator consumer error: %s — reconnecting in %.1fs",
                               type(e).__name__, backoff)
                try:
                    if self._consumer is not None:
                        await self._consumer.stop()
                except Exception:
                    pass
                self._consumer = None
                # graceful backoff (interruptible)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                    break
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 1.5, 30.0)

        self._connected = False

    async def _ingest(self, ev: dict[str, Any] | None) -> None:
        if not isinstance(ev, dict):
            return
        try:
            event_type = ev.get("type") or "Unknown"
            repo = ev.get("repo") or {}
            repo_name = repo.get("name")
            repo_id = repo.get("id")
            actor = ev.get("actor") or {}
            actor_id = actor.get("id")
        except Exception:
            return

        now = datetime.now(timezone.utc)
        bucket_ts = _floor_minute(now)

        async with self._lock:
            self._messages_consumed += 1
            self._last_event_at = now
            # tail 이 현재 분과 같지 않으면 새 bucket
            if not self._buckets or self._buckets[-1].ts != bucket_ts:
                # 갭 보정: 만약 1분 이상 차이나면 빈 bucket 채움 (deque maxlen 이 알아서 잘라냄)
                if self._buckets:
                    last_ts = self._buckets[-1].ts
                    gap = int((bucket_ts - last_ts).total_seconds() // 60) - 1
                    for i in range(1, gap + 1):
                        self._buckets.append(MinuteBucket(ts=last_ts + timedelta(minutes=i)))
                self._buckets.append(MinuteBucket(ts=bucket_ts))
            self._buckets[-1].add(event_type, repo_name, repo_id, actor_id)

    # ── 스냅샷 ───────────────────────────────────────────
    async def snapshot(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        cur_ts = _floor_minute(now)

        async with self._lock:
            # 현재 분 bucket 보장 (없으면 0 으로)
            current = next((b for b in self._buckets if b.ts == cur_ts), None)

            # 최근 60분 buckets (오래된→최신). 빈 분도 채움.
            buckets_view: list[dict[str, Any]] = []
            existing = {b.ts: b for b in self._buckets}
            for i in range(WINDOW_MINUTES - 1, -1, -1):
                ts = cur_ts - timedelta(minutes=i)
                b = existing.get(ts)
                if b is None:
                    buckets_view.append({
                        "ts": ts.isoformat(),
                        "events": 0,
                        "unique_repos": 0,
                        "by_type": {},
                    })
                else:
                    buckets_view.append({
                        "ts": ts.isoformat(),
                        "events": b.events,
                        "unique_repos": len(b.repos),
                        "by_type": dict(b.by_type),
                    })

            # 60분 totals
            window_buckets = [existing[ts] for ts in
                              (cur_ts - timedelta(minutes=i) for i in range(WINDOW_MINUTES))
                              if ts in existing]
            total_events = sum(b.events for b in window_buckets)
            type_counter: Counter = Counter()
            repo_counter: Counter = Counter()
            for b in window_buckets:
                type_counter.update(b.by_type)
            # repos / actors 는 union — 메모리 부담 줄이려 별도 계산
            unique_repos: set[str] = set()
            unique_actors: set[int] = set()
            for b in window_buckets:
                unique_repos.update(b.repos)
                unique_actors.update(b.actors)

            # Top 5min — 최근 N분 bucket 합산 후 sort
            top_window = [
                existing[ts] for ts in
                (cur_ts - timedelta(minutes=i) for i in range(TOP_WINDOW_MINUTES))
                if ts in existing
            ]
            for b in top_window:
                repo_counter.update(b.repo_events)

            # repo 별 dominant type
            repo_dominant: dict[str, str] = {}
            for repo_name, _cnt in repo_counter.most_common(TOP_N):
                tc = Counter()
                for b in top_window:
                    rt = b.repo_types.get(repo_name)
                    if rt:
                        tc.update(rt)
                if tc:
                    repo_dominant[repo_name] = tc.most_common(1)[0][0]

            top_items = [
                {
                    "repo_name": rn,
                    "events": cnt,
                    "primary_type": repo_dominant.get(rn, "Unknown"),
                }
                for rn, cnt in repo_counter.most_common(TOP_N)
            ]

            current_payload = {
                "ts": cur_ts.isoformat(),
                "events": current.events if current else 0,
                "unique_repos": len(current.repos) if current else 0,
                "by_type": dict(current.by_type) if current else {},
            }

            lag = ((now - self._last_event_at).total_seconds()
                   if self._last_event_at else None)

            return {
                "now": now.isoformat(),
                "started_at": self._started_at.isoformat() if self._started_at else None,
                "connected": self._connected,
                "messages_consumed": self._messages_consumed,
                "lag_seconds": lag,
                "window_minutes": WINDOW_MINUTES,
                "top_window_minutes": TOP_WINDOW_MINUTES,
                "current_minute": current_payload,
                "buckets": buckets_view,
                "totals_window": {
                    "events": total_events,
                    "unique_repos": len(unique_repos),
                    "unique_actors": len(unique_actors),
                },
                "by_type_window": dict(type_counter),
                "top_repos": top_items,
            }


# 싱글턴 (FastAPI lifespan 에서 시작/정지)
aggregator = LiveAggregator()
