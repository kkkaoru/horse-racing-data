#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""DuckDB の threads / memory_limit を自動算出するヘルパ。

macOS は `sysctl -n hw.memsize`, Linux は `/proc/meminfo` から物理 RAM を取り、66% を
DuckDB に割り当てる。threads は `os.cpu_count()` 全数 (DuckDB は E コアも有効活用)。

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

DEFAULT_MEM_FRACTION = 0.66
FALLBACK_THREADS = 4
FALLBACK_MEMORY_GB = 8


def _detect_total_memory_bytes() -> int | None:
    if os.name == "posix":
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


def default_threads() -> int:
    return os.cpu_count() or FALLBACK_THREADS


def default_memory_limit() -> str:
    total = _detect_total_memory_bytes()
    if total is None:
        return f"{FALLBACK_MEMORY_GB}GB"
    gb = int((total / (1024**3)) * DEFAULT_MEM_FRACTION)
    return f"{max(gb, FALLBACK_MEMORY_GB)}GB"


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
        help="DuckDB memory_limit (default: auto-detect ~66%% of physical RAM)",
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
