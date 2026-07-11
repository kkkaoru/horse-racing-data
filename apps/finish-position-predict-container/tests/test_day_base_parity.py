"""Byte/row-parity acceptance test: OLD full-``LAYER_CHAIN`` path vs NEW
day-base + ``RACE_CHAIN`` split path, against REAL historical Neon data.

This is NOT a unit test. It requires live Neon Postgres access (the same
``jvd_se`` / ``nvd_se`` history the container reads in production) and runs
the ACTUAL DuckDB base build + feature-layer subprocesses twice -- once
through the unmodified :func:`pipeline_runner.build_upcoming_feature_rows`
(OLD, full ``LAYER_CHAIN`` per race) and once through
:func:`pipeline_runner.build_day_base` +
:func:`pipeline_runner.build_pipeline_from_day_base` (NEW, day-base once +
``RACE_CHAIN`` per race) -- then asserts the two feature parquets are
row-for-row identical for every race on the target date.

It is intentionally excluded from both the standard ``uv run pytest`` run and
the ``--cov=predict_lib --cov-fail-under=95`` gate (same exemption class as
``predict_upcoming.py`` -- "wires together real I/O", per that module's own
docstring): the whole module is SKIPPED by default via the module-level
``pytestmark`` below unless ``RUN_DAY_BASE_PARITY=1`` is explicitly set, so it
never blocks CI / pre-commit / a normal local test run.

Manual invocation
------------------
Run this BEFORE flipping ``DAY_BASE_SPLIT_ENABLED`` to include a new category
in production. Pick a PAST, already-settled race day (not "today") as the
target date -- see :func:`pipeline_runner.build_day_base`'s docstring for why
a settled date makes the OLD vs NEW comparison a non-issue for the 7
late-binding columns (``tansho_odds`` / ``tansho_ninkijun`` /
``popularity_score`` / ``odds_score`` / ``current_bataiju`` /
``target_zogen_sa`` / ``babajotai_code_shiba`` / ``babajotai_code_dirt``):
for a settled race these are FIXED historical values in ``jvd_se`` /
``nvd_se``, so OLD and NEW read identical values regardless of when each
subprocess runs within the same test.

    RUN_DAY_BASE_PARITY=1 \\
    DATABASE_URL="postgresql://user:pass@host/db" \\
    DAY_BASE_PARITY_CATEGORY=jra \\
    DAY_BASE_PARITY_TARGET_DATE=20250615 \\
    DAY_BASE_PARITY_DAYS_AHEAD=0 \\
    uv run pytest tests/test_day_base_parity.py -v -s

Env vars:
    RUN_DAY_BASE_PARITY        Gate. Must be exactly ``"1"`` to un-skip this module.
    DATABASE_URL                Neon / Postgres connection string (required).
    DAY_BASE_PARITY_CATEGORY    ``"jra"`` / ``"nar"`` / ``"ban-ei"`` (default ``"jra"``).
    DAY_BASE_PARITY_TARGET_DATE ``YYYYMMDD`` target date (required, no default --
                                 a past date must be deliberately chosen).
    DAY_BASE_PARITY_DAYS_AHEAD   Non-negative integer window extension (default ``0``).

What it does
------------
1. OLD path: :func:`pipeline_runner.build_upcoming_feature_rows` for the
   whole ``[target_date, target_date + days_ahead]`` window (every race,
   the existing unmodified full ``LAYER_CHAIN``).
2. Extracts the distinct ``(keibajo_code, race_bango)`` pairs from the OLD
   path's ``race_id`` values (``source:kaisai_nen:kaisai_tsukihi:keibajo_code:
   race_bango``, split rather than reconstructed).
3. NEW path: :func:`pipeline_runner.build_day_base` ONCE for the same
   category+day, then :func:`pipeline_runner.build_pipeline_from_day_base`
   once PER race key from step 2 (mirrors real per-race production serving),
   concatenating every race's output parquet.
4. Diffs the two resulting tables row-for-row on the ``(race_id, umaban)``
   join key: exact equality for non-float columns, ``numpy.isclose`` with
   ``rtol=1e-9`` for float columns. Any mismatch fails the test with the
   offending column names + mismatch counts.
"""

from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline_runner import (
    build_day_base,
    build_pipeline_from_day_base,
    build_upcoming_feature_rows,
)
from predict_lib.model_meta import Category, resolve_category

RUN_DAY_BASE_PARITY_ENV: str = "RUN_DAY_BASE_PARITY"
DATABASE_URL_ENV: str = "DATABASE_URL"
TARGET_DATE_ENV: str = "DAY_BASE_PARITY_TARGET_DATE"
CATEGORY_ENV: str = "DAY_BASE_PARITY_CATEGORY"
DAYS_AHEAD_ENV: str = "DAY_BASE_PARITY_DAYS_AHEAD"
DEFAULT_CATEGORY: str = "jra"
DEFAULT_DAYS_AHEAD: int = 0
FLOAT_RTOL: float = 1e-9
RACE_ID_MIN_PARTS: int = 5
RACE_ID_KEIBAJO_INDEX: int = 3
RACE_ID_BANGO_INDEX: int = 4

pytestmark = pytest.mark.skipif(
    os.environ.get(RUN_DAY_BASE_PARITY_ENV, "").strip() != "1",
    reason=(
        "test_day_base_parity requires live Neon access and is gated behind "
        "RUN_DAY_BASE_PARITY=1 -- see the module docstring for manual invocation"
    ),
)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is required to run test_day_base_parity")
    return value


def _rows_map_to_frame(rows: Mapping[str, list[Mapping[str, object]]]) -> pd.DataFrame:
    """Flatten a ``race_id`` -> entries map (as returned by
    ``build_upcoming_feature_rows``) into a single DataFrame."""
    all_entries = [entry for entries in rows.values() for entry in entries]
    return pd.DataFrame(all_entries)


def _distinct_race_keys(frame: pd.DataFrame) -> list[tuple[str, str]]:
    """Return distinct (keibajo_code, race_bango) pairs from the frame's
    ``race_id`` column, split rather than reconstructed (see module docstring)."""
    pairs: set[tuple[str, str]] = set()
    for race_id in frame["race_id"].unique():
        parts = str(race_id).split(":")
        if len(parts) < RACE_ID_MIN_PARTS:
            continue
        pairs.add((parts[RACE_ID_KEIBAJO_INDEX], parts[RACE_ID_BANGO_INDEX]))
    return sorted(pairs)


def _run_old_path(
    category: Category, target_date: str, days_ahead: int, database_url: str
) -> pd.DataFrame:
    rows = build_upcoming_feature_rows(category, target_date, days_ahead, database_url)
    return _rows_map_to_frame(rows)


def _run_new_path(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    race_keys: list[tuple[str, str]],
) -> pd.DataFrame | None:
    day_base_dir = build_day_base(category, target_date, days_ahead, database_url)
    if day_base_dir is None:
        return None

    frames: list[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as tmp_root:
        for index, (keibajo_code, race_bango) in enumerate(race_keys):
            target_race = f"{keibajo_code}:{race_bango}"
            final_dir = Path(tmp_root) / f"race-{index}"
            built = build_pipeline_from_day_base(
                category,
                target_date,
                days_ahead,
                database_url,
                day_base_dir,
                final_dir,
                target_race,
            )
            if not built:
                continue
            frames.append(pd.read_parquet(final_dir))
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _assert_frames_match(old_frame: pd.DataFrame, new_frame: pd.DataFrame) -> None:
    old_sorted = old_frame.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    new_sorted = new_frame.sort_values(["race_id", "umaban"]).reset_index(drop=True)

    assert list(old_sorted.columns) == list(new_sorted.columns), (
        f"column set/order mismatch: old={list(old_sorted.columns)} "
        f"new={list(new_sorted.columns)}"
    )
    assert len(old_sorted) == len(new_sorted), (
        f"row count mismatch: old={len(old_sorted)} new={len(new_sorted)}"
    )

    mismatches: list[tuple[str, int]] = []
    for col in old_sorted.columns:
        old_col = old_sorted[col]
        new_col = new_sorted[col]
        if pd.api.types.is_float_dtype(old_col) or pd.api.types.is_float_dtype(new_col):
            close = np.isclose(
                old_col.astype(float), new_col.astype(float), rtol=FLOAT_RTOL, equal_nan=True
            )
            if not bool(close.all()):
                mismatches.append((str(col), int((~close).sum())))
        else:
            eq = (old_col == new_col) | (old_col.isna() & new_col.isna())
            if not bool(eq.all()):
                mismatches.append((str(col), int((~eq).sum())))

    assert not mismatches, f"column mismatches (col, mismatched_row_count): {mismatches}"


def test_day_base_parity_matches_full_pipeline() -> None:
    """Manual acceptance test: see module docstring for invocation instructions."""
    database_url = _require_env(DATABASE_URL_ENV)
    category = resolve_category(os.environ.get(CATEGORY_ENV, DEFAULT_CATEGORY))
    target_date = _require_env(TARGET_DATE_ENV)
    days_ahead = int(os.environ.get(DAYS_AHEAD_ENV, str(DEFAULT_DAYS_AHEAD)))

    old_frame = _run_old_path(category, target_date, days_ahead, database_url)
    assert not old_frame.empty, (
        f"OLD path produced no rows for category={category} target_date={target_date} -- "
        "pick a date with at least one race for this category"
    )

    race_keys = _distinct_race_keys(old_frame)
    new_frame = _run_new_path(category, target_date, days_ahead, database_url, race_keys)
    assert new_frame is not None, (
        f"NEW path (day-base + RACE_CHAIN) produced no rows for category={category} "
        f"target_date={target_date} -- the day-base build itself likely failed"
    )

    _assert_frames_match(old_frame, new_frame)
