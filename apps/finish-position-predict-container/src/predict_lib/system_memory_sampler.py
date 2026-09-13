"""Sample guest-system memory; this is not an exact per-job/cgroup high water."""

from __future__ import annotations

import re
import threading
from _thread import LockType
from dataclasses import dataclass
from pathlib import Path
from typing import Final


@dataclass(frozen=True)
class SystemMemorySnapshot:
    total_bytes: int | None
    used_bytes: int | None
    sampled_peak_bytes: int | None
    sample_count: int
    start_failed: bool


MEMINFO_PATH: Final[Path] = Path("/proc/meminfo")
SAMPLE_INTERVAL_SECONDS: Final[float] = 0.25
JOIN_TIMEOUT_SECONDS: Final[float] = 1.0
KIB_BYTES: Final[int] = 1024
TOTAL_PATTERN: Final[re.Pattern[str]] = re.compile(r"^MemTotal:\s+(\d+)\s+kB$", re.MULTILINE)
AVAILABLE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^MemAvailable:\s+(\d+)\s+kB$", re.MULTILINE
)


def read_system_memory() -> tuple[int, int] | None:
    """Return total/used bytes, including other processes in the guest system."""
    try:
        text = MEMINFO_PATH.read_text(encoding="utf-8")
    except OSError:
        return None
    total_match = TOTAL_PATTERN.search(text)
    available_match = AVAILABLE_PATTERN.search(text)
    if total_match is None or available_match is None:
        return None
    total = int(total_match.group(1)) * KIB_BYTES
    available = int(available_match.group(1)) * KIB_BYTES
    if total == 0 or available > total:
        return None
    return total, total - available


class SystemMemorySampler:
    """Bounded-lifecycle sampling fallback for guests without cgroup counters.

    MemAvailable accounts for reclaimable cache. Samples can miss short peaks;
    neither these values nor process RSS substitute for kernel cgroup peaks.
    """

    def __init__(self) -> None:
        self._stop: threading.Event = threading.Event()
        self._lock: LockType = threading.Lock()
        self._thread: threading.Thread = threading.Thread(target=self._run, daemon=True)
        self._started: bool = False
        self._snapshot: SystemMemorySnapshot = SystemMemorySnapshot(None, None, None, 0, False)

    def _sample(self) -> None:
        memory = read_system_memory()
        if memory is None:
            return
        total, used = memory
        with self._lock:
            previous = self._snapshot
            peak = (
                used
                if previous.sampled_peak_bytes is None
                else max(used, previous.sampled_peak_bytes)
            )
            self._snapshot = SystemMemorySnapshot(
                total, used, peak, previous.sample_count + 1, previous.start_failed
            )

    def _run(self) -> None:
        while not self._stop.wait(SAMPLE_INTERVAL_SECONDS):
            self._sample()

    def start(self) -> None:
        self._sample()
        try:
            self._thread.start()
        except RuntimeError:
            # Resource exhaustion in optional diagnostics must not fail scoring.
            previous = self._snapshot
            self._snapshot = SystemMemorySnapshot(
                previous.total_bytes,
                previous.used_bytes,
                previous.sampled_peak_bytes,
                previous.sample_count,
                True,
            )
            return
        self._started = True

    def finish(self) -> SystemMemorySnapshot:
        self._stop.set()
        if self._started:
            self._thread.join(timeout=JOIN_TIMEOUT_SECONDS)
        self._sample()
        with self._lock:
            return self._snapshot
