"""Tests for _resource_defaults (DuckDB threads/memory_limit auto-detect +
the optional PIPELINE_MAX_MEMORY_GB/PIPELINE_MAX_THREADS env-var ceilings
added 2026-07-12, see that module's own comment for the rationale).

subprocess.run and Path.exists/read_text are mocked throughout -- no real
sysctl/vm_stat process is ever spawned and no real filesystem state (cgroup
v2, /proc/meminfo) is ever read, matching this package's file/network I/O
mocking convention.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import _resource_defaults as subject


def _fake_run(returncode: int, stdout: str) -> MagicMock:
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    return result


# ── _env_positive_int ────────────────────────────────────────────────────────


def test_env_positive_int_unset_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    assert subject._env_positive_int("PIPELINE_MAX_MEMORY_GB") is None


def test_env_positive_int_blank_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "   ")
    assert subject._env_positive_int("PIPELINE_MAX_MEMORY_GB") is None


def test_env_positive_int_non_numeric_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "abc")
    assert subject._env_positive_int("PIPELINE_MAX_MEMORY_GB") is None


def test_env_positive_int_zero_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "0")
    assert subject._env_positive_int("PIPELINE_MAX_MEMORY_GB") is None


def test_env_positive_int_negative_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "-3")
    assert subject._env_positive_int("PIPELINE_MAX_MEMORY_GB") is None


def test_env_positive_int_valid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "6")
    assert subject._env_positive_int("PIPELINE_MAX_MEMORY_GB") == 6


# ── _detect_total_memory_bytes ───────────────────────────────────────────────


def test_detect_total_memory_bytes_macos_sysctl_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: _fake_run(0, "51539607552\n")  # noqa: ARG005
    )
    assert subject._detect_total_memory_bytes() == 51539607552


def test_detect_total_memory_bytes_sysctl_missing_falls_to_cgroup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_a: object, **_k: object) -> MagicMock:
        raise FileNotFoundError("no sysctl")

    monkeypatch.setattr(subprocess, "run", _raise)
    monkeypatch.setattr(Path, "exists", lambda self: str(self).endswith("memory.max"))
    monkeypatch.setattr(Path, "read_text", lambda self: "6442450944")
    assert subject._detect_total_memory_bytes() == 6442450944


def test_detect_total_memory_bytes_cgroup_max_falls_to_proc_meminfo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_a: object, **_k: object) -> MagicMock:
        raise FileNotFoundError("no sysctl")

    monkeypatch.setattr(subprocess, "run", _raise)

    def _fake_exists(self: Path) -> bool:
        return str(self).endswith("memory.max") or str(self).endswith("meminfo")

    def _fake_read_text(self: Path) -> str:
        if str(self).endswith("memory.max"):
            return "max"
        return "MemTotal:       16777216 kB\nMemFree:         100 kB\n"

    monkeypatch.setattr(Path, "exists", _fake_exists)
    monkeypatch.setattr(Path, "read_text", _fake_read_text)
    assert subject._detect_total_memory_bytes() == 16777216 * 1024


def test_detect_total_memory_bytes_nothing_available_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_a: object, **_k: object) -> MagicMock:
        raise FileNotFoundError("no sysctl")

    monkeypatch.setattr(subprocess, "run", _raise)
    monkeypatch.setattr(Path, "exists", lambda self: False)
    assert subject._detect_total_memory_bytes() is None


def test_detect_total_memory_bytes_non_posix_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "name", "nt")
    assert subject._detect_total_memory_bytes() is None


def test_detect_total_memory_bytes_sysctl_nondigit_stdout_falls_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _fake_run(0, "not-a-number"))  # noqa: ARG005
    monkeypatch.setattr(Path, "exists", lambda self: False)
    assert subject._detect_total_memory_bytes() is None


def test_detect_total_memory_bytes_sysctl_nonzero_returncode_falls_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _fake_run(1, ""))  # noqa: ARG005
    monkeypatch.setattr(Path, "exists", lambda self: False)
    assert subject._detect_total_memory_bytes() is None


def test_detect_total_memory_bytes_cgroup_read_error_falls_to_proc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _fake_run(1, ""))  # noqa: ARG005

    def _fake_exists(self: Path) -> bool:
        return str(self).endswith("memory.max") or str(self).endswith("meminfo")

    def _fake_read_text(self: Path) -> str:
        if str(self).endswith("memory.max"):
            raise OSError("boom")
        return "MemTotal:       1048576 kB\n"

    monkeypatch.setattr(Path, "exists", _fake_exists)
    monkeypatch.setattr(Path, "read_text", _fake_read_text)
    assert subject._detect_total_memory_bytes() == 1048576 * 1024


def test_detect_total_memory_bytes_proc_meminfo_malformed_value_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MemTotal line present but its value column is non-numeric -- the
    `int(line.split()[1])` parse raises ValueError, caught by the same
    except clause the cgroup-read-error test above exercises for OSError."""
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _fake_run(1, ""))  # noqa: ARG005

    def _fake_exists(self: Path) -> bool:
        return str(self).endswith("meminfo")

    monkeypatch.setattr(Path, "exists", _fake_exists)
    monkeypatch.setattr(Path, "read_text", lambda self: "MemTotal:       notanumber kB\n")
    assert subject._detect_total_memory_bytes() is None


def test_detect_total_memory_bytes_proc_meminfo_no_memtotal_line_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _fake_run(1, ""))  # noqa: ARG005

    def _fake_exists(self: Path) -> bool:
        return str(self).endswith("meminfo")

    monkeypatch.setattr(Path, "exists", _fake_exists)
    monkeypatch.setattr(Path, "read_text", lambda self: "SomeOtherLine: 1\n")
    assert subject._detect_total_memory_bytes() is None


# ── _run_text / _parse_vm_stat_pages / _detect_macos_pressure_bytes ─────────


def test_run_text_returns_stripped_stdout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _fake_run(0, "  4096  \n"))  # noqa: ARG005
    assert subject._run_text(["sysctl", "-n", "hw.pagesize"]) == "4096"


def test_run_text_nonzero_returncode_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _fake_run(1, ""))  # noqa: ARG005
    assert subject._run_text(["sysctl", "-n", "hw.pagesize"]) is None


def test_run_text_missing_binary_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_a: object, **_k: object) -> MagicMock:
        raise FileNotFoundError("no sysctl")

    monkeypatch.setattr(subprocess, "run", _raise)
    assert subject._run_text(["sysctl", "-n", "hw.pagesize"]) is None


def test_parse_vm_stat_pages_basic() -> None:
    output = "Pages free:                123.\nPages inactive:          4,567.\nGarbage line without colon\n"
    result = subject._parse_vm_stat_pages(output)
    assert result == {"Pages free": 123, "Pages inactive": 4567}


def test_parse_vm_stat_pages_non_digit_value_is_skipped() -> None:
    output = "Translation faults:            abc.\nPages free:                     9.\n"
    result = subject._parse_vm_stat_pages(output)
    assert result == {"Pages free": 9}


def test_detect_macos_pressure_bytes_full_path(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def _fake_run_fn(command: list[str], **_k: object) -> MagicMock:
        calls.append(command)
        if command[-1] == "hw.pagesize":
            return _fake_run(0, "4096")
        return _fake_run(
            0,
            "Pages free:                     100.\n"
            "Pages inactive:                  50.\n"
            "Pages speculative:               10.\n"
            "Pages purgeable:                  5.\n"
            "Pages occupied by compressor:    20.\n",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run_fn)
    result = subject._detect_macos_pressure_bytes()
    assert result == ((100 + 50 + 10 + 5) * 4096, 20 * 4096)


def test_detect_macos_pressure_bytes_missing_pagesize_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_a: object, **_k: object) -> MagicMock:
        raise FileNotFoundError("no sysctl")

    monkeypatch.setattr(subprocess, "run", _raise)
    assert subject._detect_macos_pressure_bytes() is None


def test_detect_macos_pressure_bytes_nonint_pagesize_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_run_fn(command: list[str], **_k: object) -> MagicMock:
        if command[-1] == "hw.pagesize":
            return _fake_run(0, "not-a-number")
        return _fake_run(0, "Pages free: 1.\n")

    monkeypatch.setattr(subprocess, "run", _fake_run_fn)
    assert subject._detect_macos_pressure_bytes() is None


# ── _auto_memory_limit_gb / default_memory_limit (incl. PIPELINE_MAX_MEMORY_GB) ──


def _patch_total_bytes(monkeypatch: pytest.MonkeyPatch, gb: float | None) -> None:
    value = None if gb is None else int(gb * 1024**3)
    monkeypatch.setattr(subject, "_detect_total_memory_bytes", lambda: value)


def _patch_pressure(
    monkeypatch: pytest.MonkeyPatch, available_gb: float | None, compressor_gb: float = 0.0
) -> None:
    if available_gb is None:
        monkeypatch.setattr(subject, "_detect_macos_pressure_bytes", lambda: None)
        return
    pair = (int(available_gb * 1024**3), int(compressor_gb * 1024**3))
    monkeypatch.setattr(subject, "_detect_macos_pressure_bytes", lambda: pair)


def test_default_memory_limit_no_total_uses_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, None)
    assert subject.default_memory_limit() == f"{subject.FALLBACK_MEMORY_GB}GB"


def test_default_memory_limit_no_pressure_uses_capacity_or_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_pressure(monkeypatch, None)
    assert subject.default_memory_limit() == "24GB"


def test_default_memory_limit_high_compressor_ratio_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_pressure(monkeypatch, available_gb=9, compressor_gb=5)
    assert subject.default_memory_limit() == "3GB"


def test_default_memory_limit_mid_compressor_ratio_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_pressure(monkeypatch, available_gb=10, compressor_gb=3)
    assert subject.default_memory_limit() == "5GB"


def test_default_memory_limit_low_pressure_else_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_pressure(monkeypatch, available_gb=20, compressor_gb=0)
    assert subject.default_memory_limit() == "18GB"


def test_default_memory_limit_env_cap_unset_matches_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_pressure(monkeypatch, None)
    assert subject.default_memory_limit() == "24GB"


def test_default_memory_limit_env_cap_below_auto_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "6")
    _patch_total_bytes(monkeypatch, 48)
    _patch_pressure(monkeypatch, None)
    assert subject.default_memory_limit() == "6GB"


def test_default_memory_limit_env_cap_above_auto_has_no_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "999")
    _patch_total_bytes(monkeypatch, 48)
    _patch_pressure(monkeypatch, None)
    assert subject.default_memory_limit() == "24GB"


# ── _auto_threads / default_threads (incl. PIPELINE_MAX_THREADS) ────────────


def _patch_threads_environment(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cpu: int,
    load_1m: float,
    pressure: tuple[float, float] | None,
) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: cpu)
    monkeypatch.setattr(os, "getloadavg", lambda: (load_1m, load_1m, load_1m))
    if pressure is None:
        monkeypatch.setattr(subject, "_detect_macos_pressure_bytes", lambda: None)
    else:
        available_gb, compressor_gb = pressure
        pair = (int(available_gb * 1024**3), int(compressor_gb * 1024**3))
        monkeypatch.setattr(subject, "_detect_macos_pressure_bytes", lambda: pair)


def test_default_threads_no_pressure_no_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_MAX_THREADS", raising=False)
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_threads_environment(monkeypatch, cpu=16, load_1m=0.0, pressure=None)
    assert subject.default_threads() == 16


def test_default_threads_severe_pressure_returns_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_MAX_THREADS", raising=False)
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_threads_environment(monkeypatch, cpu=16, load_1m=0.0, pressure=(5.0, 0.0))
    assert subject.default_threads() == 1


def test_default_threads_moderate_pressure_returns_capped_two(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELINE_MAX_THREADS", raising=False)
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_threads_environment(monkeypatch, cpu=16, load_1m=0.0, pressure=(11.0, 0.0))
    assert subject.default_threads() == 2


def test_default_threads_mild_pressure_falls_through_to_final_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pressure IS present but mild enough (available_ratio>=0.25,
    compressor_ratio<=0.05, load well under 65% of cpu) that neither
    special-case branch fires -- falls through to the plain
    min(cpu, headroom, mem_cap) return at the end of _auto_threads.
    mem_cap=12 here (not cpu=16) because default_memory_limit's own
    available_gb-2 branch resolves to 18GB at this same pressure reading,
    18/1.5=12 -- the plain min() still exercises the target branch."""
    monkeypatch.delenv("PIPELINE_MAX_THREADS", raising=False)
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_threads_environment(monkeypatch, cpu=16, load_1m=0.0, pressure=(20.0, 0.0))
    assert subject.default_threads() == 12


def test_auto_threads_getloadavg_unavailable_defaults_to_zero_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise() -> tuple[float, float, float]:
        raise AttributeError("getloadavg unavailable on this platform")

    monkeypatch.setattr(os, "cpu_count", lambda: 16)
    monkeypatch.setattr(os, "getloadavg", _raise)
    monkeypatch.setattr(subject, "_detect_macos_pressure_bytes", lambda: None)
    assert subject._auto_threads(mem_cap=16) == 16


def test_default_threads_env_cap_below_auto_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_MAX_THREADS", "4")
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_threads_environment(monkeypatch, cpu=16, load_1m=0.0, pressure=None)
    assert subject.default_threads() == 4


def test_default_threads_env_cap_above_auto_has_no_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PIPELINE_MAX_THREADS", "999")
    monkeypatch.delenv("PIPELINE_MAX_MEMORY_GB", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_threads_environment(monkeypatch, cpu=16, load_1m=0.0, pressure=None)
    assert subject.default_threads() == 16


def test_default_threads_memory_cap_indirectly_caps_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PIPELINE_MAX_MEMORY_GB=6 alone (no PIPELINE_MAX_THREADS) reproduces the
    project's 6GB/4-thread pairing via the existing GB_PER_THREAD mechanism."""
    monkeypatch.setenv("PIPELINE_MAX_MEMORY_GB", "6")
    monkeypatch.delenv("PIPELINE_MAX_THREADS", raising=False)
    _patch_total_bytes(monkeypatch, 48)
    _patch_threads_environment(monkeypatch, cpu=16, load_1m=0.0, pressure=None)
    assert subject.default_threads() == 4


# ── add_resource_args / apply_to_connection ─────────────────────────────────


def test_add_resource_args_defaults() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    subject.add_resource_args(parser)
    args = parser.parse_args([])
    assert args.threads is None
    assert args.memory_limit is None


def test_add_resource_args_explicit_values() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    subject.add_resource_args(parser)
    args = parser.parse_args(["--threads", "2", "--memory-limit", "4GB"])
    assert args.threads == 2
    assert args.memory_limit == "4GB"


def test_apply_to_connection_uses_explicit_values() -> None:
    con = MagicMock()
    subject.apply_to_connection(con, threads=2, memory_limit="4GB")
    executed = [call.args[0] for call in con.execute.call_args_list]
    assert executed[0] == "SET threads TO 2"
    assert executed[1] == "SET memory_limit='4GB'"
    assert executed[2] == f"SET temp_directory='{subject.SPILL_TEMP_DIR}'"
    assert executed[3] == f"SET max_temp_directory_size='{subject.SPILL_MAX_SIZE}'"


def test_apply_to_connection_falls_back_to_auto_detect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "default_threads", lambda: 4)
    monkeypatch.setattr(subject, "default_memory_limit", lambda: "6GB")
    con = MagicMock()
    subject.apply_to_connection(con)
    executed = [call.args[0] for call in con.execute.call_args_list]
    assert executed[0] == "SET threads TO 4"
    assert executed[1] == "SET memory_limit='6GB'"
