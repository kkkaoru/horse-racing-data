#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""DuckDB の threads / memory_limit を自動算出するヘルパ。

メモリ検出の優先順位:
  1. macOS: `sysctl -n hw.memsize`
  2. Linux container: `/sys/fs/cgroup/memory.max` (cgroup v2 — docker --memory= がここを設定)
  3. Linux fallback: `/proc/meminfo` MemTotal (VM 全体なので cgroup 制限は反映されない)

memory_limit: 検出メモリの 50% を DuckDB に割り当てる。OS + Python ランタイム用に
50% 残すことで、CF Container (standard-4 = 12 GiB) では 6 GiB に収まる。

threads: os.cpu_count() を上限とするが、低メモリ環境では per-thread メモリ使用量を
抑えるため GB_PER_THREAD 以上 memory_limit があるか確認して上限キャップする。
DuckDB の並列 hash join / aggregate は thread 数に比例してメモリを消費する。
CF standard-4 (12 GiB / 4 vCPU) では 4 スレッドが適切。

disk spill: `temp_directory` + `max_temp_directory_size` を常に設定する。
メモリが潤沢な環境 (Mac 48 GiB など) では spill は発火しないが、設定しておくことで
メモリ pressure 時に自動的に disk へ中間結果を書き出す。
head-to-head の pairwise 集計 (pair_history + current_pair_aggregates の multi-level
hash join) は CF container でメモリ limit を超えて spill する可能性がある。

CLI 引数 --threads / --memory-limit が与えられればそちらを優先。
"""
from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

# 50% にすることで OS + Python ランタイム (heap / mmap / OS page cache) 用に
# 十分な余裕を残す。CF container (12 GiB) では →6 GiB limit + disk spill。
DEFAULT_MEM_FRACTION = 0.50
FALLBACK_THREADS = 4
FALLBACK_MEMORY_GB = 6

# 1 thread あたり最低必要な memory_limit (GB)。この値を下回るようにスレッド数を
# 抑制することで、並列 hash join の per-thread メモリを制御する。
# 例: memory_limit=6GB の場合 → 6/1.5=4 threads。
GB_PER_THREAD = 1.5

# DuckDB disk spill directory (container では /tmp が書き込み可能)。
# spill が発生するのはメモリ不足時のみ — Mac ではほぼ発火しない。
SPILL_TEMP_DIR = "/tmp/duckdb-spill"
# spill 上限: standard-4 の ephemeral ストレージは ~50 GiB と想定。
# pair_history (NAR/JRA 全期間) の spill は高くても ~10-20 GiB 程度。
SPILL_MAX_SIZE = "30GB"


def _detect_total_memory_bytes() -> int | None:
    if os.name == "posix":
        # macOS: hw.memsize is the most reliable source.
        try:
            out = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
                check=False,
                timeout=2,
            )
            if out.returncode == 0 and out.stdout.strip().isdigit():
                return int(out.stdout.strip())
        except (FileNotFoundError, subprocess.SubprocessError):
            pass
        # Linux: cgroup v2 memory.max reflects the container memory limit set by
        # `docker run --memory=...` (and by CF container instance_type). When
        # present and not "max", use it so DuckDB is bounded by the container
        # budget rather than the host VM's /proc/meminfo (which reports full VM
        # RAM regardless of cgroup limits).
        cgroup_v2 = Path("/sys/fs/cgroup/memory.max")
        if cgroup_v2.exists():
            try:
                raw = cgroup_v2.read_text().strip()
                if raw != "max" and raw.isdigit():
                    return int(raw)
            except (OSError, ValueError):
                pass
        # Fallback: /proc/meminfo (may report VM-level total, not cgroup-limited).
        proc_meminfo = Path("/proc/meminfo")
        if proc_meminfo.exists():
            try:
                for line in proc_meminfo.read_text().splitlines():
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return kb * 1024
            except (OSError, ValueError):
                pass
    return None


def default_memory_limit() -> str:
    total = _detect_total_memory_bytes()
    if total is None:
        return f"{FALLBACK_MEMORY_GB}GB"
    gb = int((total / (1024**3)) * DEFAULT_MEM_FRACTION)
    return f"{max(gb, FALLBACK_MEMORY_GB)}GB"


def default_threads() -> int:
    """Return the DuckDB thread count, capped by memory budget.

    DuckDB's parallel hash join / aggregate operators allocate per-thread
    intermediate buffers. On memory-constrained environments (e.g. CF container
    standard-4 = 12 GiB → 6 GiB memory_limit) using all CPUs (12 in Colima)
    causes the peak to exceed the limit even with disk spill because each
    thread's active partition must fit in memory before it can spill.

    Cap: floor(memory_limit_gb / GB_PER_THREAD), minimum = FALLBACK_THREADS.
    On Mac (48 GiB → 24 GiB limit): 24 / 1.5 = 16 threads ≥ cpu_count → no cap.
    On CF standard-4 (12 GiB → 6 GiB limit): 6 / 1.5 = 4 threads → capped.
    """
    cpu = os.cpu_count() or FALLBACK_THREADS
    mem_gb = int(default_memory_limit().rstrip("GB"))
    mem_cap = max(int(mem_gb / GB_PER_THREAD), FALLBACK_THREADS)
    return min(cpu, mem_cap)


def add_resource_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="DuckDB threads (default: auto-detect = os.cpu_count())",
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        default=None,
        help="DuckDB memory_limit (default: auto-detect ~50%% of physical RAM)",
    )


def apply_to_connection(
    con: duckdb.DuckDBPyConnection,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> None:
    resolved_threads = threads if threads is not None else default_threads()
    resolved_memory = memory_limit if memory_limit is not None else default_memory_limit()
    con.execute(f"SET threads TO {resolved_threads}")
    con.execute(f"SET memory_limit='{resolved_memory}'")
    # disk spill — OOM を防ぐために常に設定する。
    # メモリが潤沢な環境 (Mac など) では spill は発火しない。
    # CF container (standard-4 / 12 GiB) ではメモリ limit =6 GiB を超えた瞬間に
    # /tmp/duckdb-spill へ中間結果を書き出すことで head-to-head pairwise 集計などの
    # OOM を回避する。
    con.execute(f"SET temp_directory='{SPILL_TEMP_DIR}'")
    con.execute(f"SET max_temp_directory_size='{SPILL_MAX_SIZE}'")
