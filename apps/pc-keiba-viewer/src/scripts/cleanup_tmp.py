"""Bounded cleanup for the ``tmp/`` scratch directory.

The training / evaluation loops drop large per-run artifacts into ``tmp/``
(feature stores, walk-forward scores, eval reports, DuckDB spill dirs ...).
Left unattended this grows without bound and eventually trips the Mac disk /
memory budget. This module identifies disposable run directories and removes
the stale ones while always protecting the production-active stores.

Two safety rails make accidental deletion of live data hard:

* ``KEEP_NAMES`` -- exact directory names that are NEVER deleted regardless of
  age or how many newer siblings exist (production feature stores + models).
* ``--keep-latest N`` -- for each *prefix group* (e.g. every ``feat-jra-v*``
  directory) the N newest directories by mtime are retained even if older than
  ``--max-age-days``.

``--dry-run`` (the default) only reports; ``--execute`` performs the deletions.

Standalone::

    uv run python src/scripts/cleanup_tmp.py --dry-run
    uv run python src/scripts/cleanup_tmp.py --execute --max-age-days 3

Programmatic (e.g. from a training-loop script)::

    from cleanup_tmp import cleanup_tmp
    cleanup_tmp(Path("tmp"), execute=True, max_age_days=3, keep_latest=1)
"""
from __future__ import annotations

import argparse
import fnmatch
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

# Directory-name patterns (fnmatch globs) that mark a tmp dir as disposable.
DISPOSABLE_PATTERNS: tuple[str, ...] = (
    "feat-*",
    "eval-*",
    "score-*",
    "wf-*",
    "*-cache*",
    "*-trial*",
    "duckdb-tmp-*",
    "_*-build",
    "finish-position-eval",
    "bucket-eval",
    "continuous-learn",
    "predictions-jsonl",
    "quarantine-*",
)

# Production-active stores: never deleted regardless of age / newer siblings.
KEEP_NAMES: frozenset[str] = frozenset({
    "feat-jra-v9-weather",
    "feat-nar-v9-weather",
    "models",
    "finish-position-models",
})

DEFAULT_MAX_AGE_DAYS = 7
DEFAULT_KEEP_LATEST = 1
SECONDS_PER_DAY = 86400.0
BYTES_PER_GIB = 1024.0**3

# A hyphen-delimited token is treated as a version/run suffix when it contains a
# digit (v9, 2024, 20260519, a1b2c3d ...). The prefix group strips the first
# such token onward so ``feat-jra-v9-weather`` and ``feat-jra-v8-merged`` group
# under ``feat-jra``.
_VERSION_TOKEN = re.compile(r"\d")


@dataclass(frozen=True)
class DirEntry:
    path: Path
    size_bytes: int
    mtime: float
    group_key: str


@dataclass(frozen=True)
class CleanupPlan:
    delete: tuple[DirEntry, ...]
    kept: tuple[DirEntry, ...]

    @property
    def freed_bytes(self) -> int:
        return sum(entry.size_bytes for entry in self.delete)


@dataclass(frozen=True)
class CleanupResult:
    plan: CleanupPlan
    executed: bool
    total_before_bytes: int
    total_after_bytes: int


def is_disposable(name: str) -> bool:
    """True when ``name`` matches a disposable pattern and is not protected."""
    if name in KEEP_NAMES:
        return False
    return any(fnmatch.fnmatch(name, pattern) for pattern in DISPOSABLE_PATTERNS)


def prefix_group(name: str) -> str:
    """Group key used by ``--keep-latest`` (e.g. ``feat-jra-v9-weather`` -> ``feat-jra``)."""
    tokens = name.split("-")
    kept: list[str] = []
    for token in tokens:
        if _VERSION_TOKEN.search(token):
            break
        kept.append(token)
    if not kept:
        return name
    return "-".join(kept)


def dir_size_bytes(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file() and not child.is_symlink():
            total += child.stat().st_size
    return total


def scan_disposable_dirs(tmp_dir: Path) -> list[DirEntry]:
    if not tmp_dir.is_dir():
        return []
    entries: list[DirEntry] = []
    for child in sorted(tmp_dir.iterdir()):
        if not child.is_dir() or child.is_symlink():
            continue
        if not is_disposable(child.name):
            continue
        entries.append(
            DirEntry(
                path=child,
                size_bytes=dir_size_bytes(child),
                mtime=child.stat().st_mtime,
                group_key=prefix_group(child.name),
            )
        )
    return entries


def select_for_deletion(
    entries: list[DirEntry],
    *,
    max_age_days: float,
    keep_latest: int,
    now: float,
) -> CleanupPlan:
    by_group: dict[str, list[DirEntry]] = {}
    for entry in entries:
        by_group.setdefault(entry.group_key, []).append(entry)
    delete: list[DirEntry] = []
    kept: list[DirEntry] = []
    age_cutoff = max_age_days * SECONDS_PER_DAY
    for group in by_group.values():
        newest_first = sorted(group, key=lambda e: e.mtime, reverse=True)
        protected_newest = set(id(e) for e in newest_first[:keep_latest])
        for entry in newest_first:
            too_young = (now - entry.mtime) < age_cutoff
            if id(entry) in protected_newest or too_young:
                kept.append(entry)
            else:
                delete.append(entry)
    return CleanupPlan(delete=tuple(delete), kept=tuple(kept))


def format_gib(num_bytes: int) -> str:
    return f"{num_bytes / BYTES_PER_GIB:.2f} GiB"


def cleanup_tmp(
    tmp_dir: Path,
    *,
    execute: bool = False,
    max_age_days: float = DEFAULT_MAX_AGE_DAYS,
    keep_latest: int = DEFAULT_KEEP_LATEST,
    now: float | None = None,
) -> CleanupResult:
    """Plan (and optionally perform) cleanup of disposable ``tmp/`` directories."""
    clock = time.time() if now is None else now
    entries = scan_disposable_dirs(tmp_dir)
    plan = select_for_deletion(
        entries, max_age_days=max_age_days, keep_latest=keep_latest, now=clock,
    )
    total_before = sum(entry.size_bytes for entry in entries)
    if execute:
        for entry in plan.delete:
            shutil.rmtree(entry.path)
    total_after = total_before - plan.freed_bytes if execute else total_before
    return CleanupResult(
        plan=plan,
        executed=execute,
        total_before_bytes=total_before,
        total_after_bytes=total_after,
    )


def render_report(result: CleanupResult) -> str:
    verb = "Deleted" if result.executed else "Would delete"
    lines: list[str] = []
    for entry in sorted(result.plan.delete, key=lambda e: e.size_bytes, reverse=True):
        lines.append(f"  {verb}: {entry.path}  ({format_gib(entry.size_bytes)})")
    if not result.plan.delete:
        lines.append("  (nothing to delete)")
    lines.append(f"Total before: {format_gib(result.total_before_bytes)}")
    lines.append(f"Total after:  {format_gib(result.total_after_bytes)}")
    lines.append(f"Freed:        {format_gib(result.plan.freed_bytes)}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="cleanup_tmp")
    parser.add_argument("--tmp-dir", type=Path, default=Path("tmp"))
    parser.add_argument(
        "--max-age-days",
        type=float,
        default=DEFAULT_MAX_AGE_DAYS,
        help="Only delete dirs older than N days (by mtime).",
    )
    parser.add_argument(
        "--keep-latest",
        type=int,
        default=DEFAULT_KEEP_LATEST,
        help="Per prefix group, keep the N newest dirs regardless of age.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="List only (default).")
    mode.add_argument("--execute", action="store_true", help="Actually delete.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = cleanup_tmp(
        args.tmp_dir,
        execute=bool(args.execute),
        max_age_days=float(args.max_age_days),
        keep_latest=int(args.keep_latest),
    )
    print(render_report(result))


if __name__ == "__main__":
    main()
