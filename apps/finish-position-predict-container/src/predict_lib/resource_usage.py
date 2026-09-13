"""Best-effort cgroup telemetry without changing prediction results or limits."""

from __future__ import annotations

import json
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from .system_memory_sampler import SAMPLE_INTERVAL_SECONDS, SystemMemorySampler


@dataclass(frozen=True)
class ResourceWorkload:
    category: str
    run_date: str
    mode: str
    race: str | None


@dataclass(frozen=True)
class CgroupMemory:
    source: str
    current: int | None
    peak: int | None
    limit: int | None


CGROUP_PATH: Final[Path] = Path("/sys/fs/cgroup")
# Linux v1 uses a page-aligned LONG_MAX for unlimited memory, not usable RAM.
UNLIMITED_MEMORY_THRESHOLD: Final[int] = 1 << 60


def _read_counter(name: str) -> int | None:
    try:
        value = int((CGROUP_PATH / name).read_text(encoding="utf-8").strip())
        return value if value >= 0 else None
    except (OSError, ValueError):
        return None


def read_cgroup_memory() -> CgroupMemory:
    """Keep each snapshot within one controller; never mix v1 and v2 values."""
    candidates = (
        ("cgroup-v2", "memory.current", "memory.peak", "memory.max"),
        (
            "cgroup-v1",
            "memory/memory.usage_in_bytes",
            "memory/memory.max_usage_in_bytes",
            "memory/memory.limit_in_bytes",
        ),
        (
            "cgroup-v1",
            "memory.usage_in_bytes",
            "memory.max_usage_in_bytes",
            "memory.limit_in_bytes",
        ),
    )
    for source, current_path, peak_path, limit_path in candidates:
        current = _read_counter(current_path)
        peak = _read_counter(peak_path)
        limit = _read_counter(limit_path)
        if limit is not None and limit >= UNLIMITED_MEMORY_THRESHOLD:
            limit = None
        if current is not None or peak is not None or limit is not None:
            return CgroupMemory(source, current, peak, limit)
    return CgroupMemory("unavailable", None, None, None)


@contextmanager
def observe_resources(workload: ResourceWorkload) -> Generator[None]:
    """Emit timing and container-lifetime memory high water, even on failure.

    memory.peak includes child processes and prior work on this Container. It is
    not a per-job RSS measurement and must not be labeled as one. Missing cgroup
    files are reported as null, not zero; telemetry must never fail the workload.
    """
    started = time.monotonic()
    sampler = SystemMemorySampler()
    sampler.start()
    succeeded = False
    try:
        yield
        succeeded = True
    finally:
        elapsed = round(time.monotonic() - started, 3)
        system = sampler.finish()
        memory = read_cgroup_memory()
        payload = {
            "event": "container-resource-usage",
            "category": workload.category,
            "run_date": workload.run_date,
            "mode": workload.mode,
            "race": workload.race,
            "elapsed_seconds": elapsed,
            "system_memory_total_bytes": system.total_bytes,
            "system_memory_used_bytes": system.used_bytes,
            "sampled_system_memory_peak_bytes": system.sampled_peak_bytes,
            "system_memory_sample_count": system.sample_count,
            "system_memory_sampler_start_failed": system.start_failed,
            "system_memory_sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
            "memory_measurement_source": memory.source,
            "memory_current_bytes": memory.current,
            "container_lifetime_memory_peak_bytes": memory.peak,
            "memory_limit_bytes": memory.limit,
            "succeeded": succeeded,
        }
        # Broken logging pipes must not replace a successful result or error.
        with suppress(OSError):
            print(json.dumps(payload, sort_keys=True), flush=True)
