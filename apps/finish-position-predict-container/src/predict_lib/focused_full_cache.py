"""Bounded in-process store for completed focused-full parquet payloads.

**Why this exists**: the container's R2 credentials are read-only (direct
SigV4 PUT was removed in commit ``41adee27`` after it failed in production
under that token). The only working R2 write path is the container embedding
``parquetBase64``/``parquetKey`` in the last line of a *live* NDJSON HTTP
response, which the Worker's ``container-ndjson-proxy.ts`` reads off the
stream and proxies to R2 via the ``FEATURES_CACHE`` binding.

A focused-full request detaches its pipeline into a background thread and
returns ``accepted`` immediately (see ``serve.build_focused_full_race_key``
and ``serve.iter_predict_chunks``'s focused-full branch) -- by the time that
detached pipeline finishes, the HTTP response that would have carried the
payload bytes has already ended. This store lets the detached thread hand the
computed payload off to a *later*, separate HTTP request: the Worker's queue
consumer polls Neon for completion on redelivery, and once it observes a
race is done, it makes one follow-up call to fetch (and consume) this race's
cached payload so it can still be proxied to R2 through the normal channel.

Population must never fail the underlying prediction run (log-only, per the
same non-blocking convention as :data:`serve.ParquetPayloadFn`), and a missed
pickup (container recycled, Worker never polled, TTL expired) degrades to "no
cache entry for this race today" -- not a prediction failure.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, final

_MAX_ENTRIES: Final[int] = 32
"""Generous relative to the single per-process focused-full slot (at most one
pipeline runs at a time, so entries accumulate only while awaiting pickup)."""

_TTL_SECONDS: Final[float] = 2400.0
"""40 minutes -- matches the queue consumer's focused-full retry budget
(see ``apps/finish-position-cron/src/queue-consumer.ts``), so an entry
survives at least as long as the redelivery window that would want to
consume it."""


@dataclass(frozen=True, slots=True)
class FocusedFullCachePayload:
    """One completed focused-full run's cache payload, ready for R2 proxying."""

    parquet_base64: str | None
    parquet_key: str | None
    per_race_parquets: list[dict[str, str]] | None


@final
class FocusedFullCacheStore:
    """Thread-safe, bounded, TTL-evicting store keyed by focused-full race key.

    ``pop`` consumes the entry (removes it on read) so a Worker pickup never
    re-delivers stale bytes on a later retry -- the next retry simply finds
    nothing cached, which is the same degraded-but-functional outcome as any
    other cache miss in this system.
    """

    def __init__(
        self,
        max_entries: int = _MAX_ENTRIES,
        ttl_seconds: float = _TTL_SECONDS,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._time_fn = time_fn
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[float, FocusedFullCachePayload]] = {}

    def put(self, race_key: str, payload: FocusedFullCachePayload) -> None:
        now = self._time_fn()
        with self._lock:
            self._evict_expired(now)
            self._entries[race_key] = (now, payload)
            self._evict_oldest_while_over_capacity()

    def pop(self, race_key: str) -> FocusedFullCachePayload | None:
        now = self._time_fn()
        with self._lock:
            self._evict_expired(now)
            entry = self._entries.pop(race_key, None)
        return entry[1] if entry is not None else None

    def _evict_expired(self, now: float) -> None:
        expired_keys = [
            key
            for key, (stored_at, _) in self._entries.items()
            if now - stored_at > self._ttl_seconds
        ]
        for key in expired_keys:
            del self._entries[key]

    def _evict_oldest_while_over_capacity(self) -> None:
        while len(self._entries) > self._max_entries:
            oldest_key = min(self._entries, key=lambda key: self._entries[key][0])
            del self._entries[oldest_key]
