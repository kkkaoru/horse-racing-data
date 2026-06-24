#!/usr/bin/env python3
"""Shared helpers for finish-position walk-forward train scripts.

Stage 0C of the v8 iterative-loop plan introduces a common helper module so
each `train_finish_position_{arch}_walk_forward.py` (CatBoost / XGBoost /
LightGBM) can share:

  * 2-stage fold skip gate (NDCG x top1/place3 combined)
  * memory pre-check with retry
  * time-decay sample weight builder
  * bucket-aware sample weight composer (Lever 4)
  * atomic metadata.json writer + per-fold completion detector
  * per-bucket NDCG@3 computer
  * stratified KFold index builder for Optuna HPO

Every helper is mockable; the train scripts inject filesystem / system calls
so unit tests can run with zero real I/O.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final

import numpy as np
import pandas as pd
import psutil
from numpy.typing import NDArray
from sklearn.model_selection import StratifiedKFold

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

NDCG_SKIP_RATIO: Final[float] = 0.95
TOP1_SKIP_RATIO: Final[float] = 0.93
PLACE3_SKIP_RATIO: Final[float] = 0.90
MAX_BUCKET_WEIGHT_ALPHA: Final[float] = 0.75
WEIGHT_LOWER_BOUND: Final[float] = 0.5
WEIGHT_UPPER_BOUND: Final[float] = 1.75
TIME_DECAY_MIN_WEIGHT: Final[float] = 0.5
TIME_DECAY_MAX_WEIGHT: Final[float] = 1.0
DEFAULT_MEMORY_MIN_GB: Final[float] = 8.0
DEFAULT_MEMORY_RETRIES: Final[int] = 3
DEFAULT_MEMORY_RETRY_SLEEP_S: Final[int] = 60
BYTES_PER_GIB: Final[float] = 1024.0 ** 3
NDCG_AT_K: Final[int] = 3
NDCG_LOG2_OFFSET: Final[float] = 2.0
COMPLETED_FOLD_STATUS: Final[str] = "completed"
METADATA_FILENAME: Final[str] = "metadata.json"
METADATA_FOLD_KEY: Final[str] = "fold_year"
METADATA_STATUS_KEY: Final[str] = "status"
HPO_MIN_FOLDS: Final[int] = 2
GROUP_SORT_KEYS: Final[tuple[str, str]] = ("race_id", "umaban")


def sort_full_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Sort the whole frame once by ``(race_id, umaban)`` with a fresh index.

    Walk-forward folds each train on a cumulative date window, so every fold's
    train/valid slice is a boolean-masked subset of this frame. A boolean mask
    preserves relative row order, so a slice of an already-sorted frame is itself
    sorted by ``(race_id, umaban)`` and has contiguous race groups -- exactly the
    layout the per-fold rankers re-derive with their own ``sort_values``. Sorting
    once here lets the rankers skip that redundant per-fold sort (see
    ``presorted`` in the CatBoost / XGBoost trainers) without changing results.

    ``umaban`` may be absent in unit fixtures; fall back to ``race_id`` only so
    grouping stays contiguous. ``mergesort`` keeps the sort stable, matching the
    rankers' default stable ``sort_values``.
    """
    keys = [c for c in GROUP_SORT_KEYS if c in df.columns]
    if not keys:
        return df.reset_index(drop=True)
    return df.sort_values(list(keys), kind="mergesort").reset_index(drop=True)


def should_skip_fold(
    val_ndcg: float,
    val_top1: float,
    val_place3: float,
    baseline_ndcg: float,
    baseline_top1: float,
    baseline_place3: float,
) -> tuple[bool, str]:
    """Stability item #5: 2-stage val-regression gate.

    Returns ``(skip, reason)``. The fold is skipped when NDCG@3 falls below
    ``baseline_ndcg * 0.95`` **and** either top1 falls below
    ``baseline_top1 * 0.93`` **or** place3 falls below
    ``baseline_place3 * 0.90``.
    """
    ndcg_threshold = baseline_ndcg * NDCG_SKIP_RATIO
    top1_threshold = baseline_top1 * TOP1_SKIP_RATIO
    place3_threshold = baseline_place3 * PLACE3_SKIP_RATIO
    ndcg_below = val_ndcg < ndcg_threshold
    top1_below = val_top1 < top1_threshold
    place3_below = val_place3 < place3_threshold
    if not ndcg_below:
        return (False, "ndcg-pass")
    if top1_below:
        return (
            True,
            (
                f"ndcg<{ndcg_threshold:.4f} AND top1<{top1_threshold:.4f}"
                f" (val_ndcg={val_ndcg:.4f}, val_top1={val_top1:.4f})"
            ),
        )
    if place3_below:
        return (
            True,
            (
                f"ndcg<{ndcg_threshold:.4f} AND place3<{place3_threshold:.4f}"
                f" (val_ndcg={val_ndcg:.4f}, val_place3={val_place3:.4f})"
            ),
        )
    return (False, "secondary-pass")


def _available_memory_gb(memory_reader: Callable[[], int]) -> float:
    return memory_reader() / BYTES_PER_GIB


def assert_memory_available(
    min_gb: float = DEFAULT_MEMORY_MIN_GB,
    retries: int = DEFAULT_MEMORY_RETRIES,
    retry_sleep_s: int = DEFAULT_MEMORY_RETRY_SLEEP_S,
    memory_reader: Callable[[], int] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> None:
    """Raise if available memory stays below ``min_gb`` after ``retries`` checks.

    Defaults invoke ``psutil.virtual_memory().available`` and ``time.sleep``;
    tests inject fakes via ``memory_reader`` / ``sleeper``.
    """
    if retries < 1:
        raise ValueError(f"retries must be >= 1, got {retries}")
    reader = memory_reader if memory_reader is not None else _default_memory_reader
    rest = sleeper if sleeper is not None else time.sleep
    last_available = 0.0
    attempt = 0
    while attempt < retries:
        last_available = _available_memory_gb(reader)
        if last_available >= min_gb:
            return
        attempt += 1
        if attempt < retries:
            rest(float(retry_sleep_s))
    raise MemoryError(
        f"Available memory {last_available:.2f} GiB below required {min_gb:.2f} GiB"
        f" after {retries} attempts.",
    )


def _default_memory_reader() -> int:
    return int(psutil.virtual_memory().available)


def compute_time_decay_weights(years: NDArray[np.int64]) -> FloatArray:
    """Linear ``0.5 + 0.5 * (year - min) / (max - min)`` weights in ``[0.5, 1.0]``.

    Degenerate single-unique-year input yields a constant
    ``TIME_DECAY_MAX_WEIGHT`` so empty / 1-year folds remain well-defined.
    """
    if years.size == 0:
        return np.zeros(0, dtype=np.float64)
    min_year = int(years.min())
    max_year = int(years.max())
    if min_year == max_year:
        return np.full(years.shape, TIME_DECAY_MAX_WEIGHT, dtype=np.float64)
    span = float(max_year - min_year)
    normalized = (years.astype(np.float64) - float(min_year)) / span
    return TIME_DECAY_MIN_WEIGHT + (TIME_DECAY_MAX_WEIGHT - TIME_DECAY_MIN_WEIGHT) * normalized


def compute_bucket_aware_weights(
    time_weights: FloatArray,
    is_weak_bucket_scores: FloatArray,
    alpha: float,
) -> FloatArray:
    """Compose ``w_time * (1 + alpha * is_weak)`` clipped to ``[0.5, 1.75]``.

    ``alpha`` must be ``<= 0.75`` so the composition stays within the
    documented L4 envelope.
    """
    if alpha < 0:
        raise ValueError(f"alpha must be non-negative, got {alpha}")
    if alpha > MAX_BUCKET_WEIGHT_ALPHA:
        raise ValueError(
            f"alpha must be <= {MAX_BUCKET_WEIGHT_ALPHA}, got {alpha}",
        )
    if time_weights.shape != is_weak_bucket_scores.shape:
        raise ValueError(
            "time_weights and is_weak_bucket_scores must have the same shape;"
            f" got {time_weights.shape} vs {is_weak_bucket_scores.shape}",
        )
    composed = time_weights * (1.0 + alpha * is_weak_bucket_scores)
    return np.clip(composed, WEIGHT_LOWER_BOUND, WEIGHT_UPPER_BOUND)


def atomic_write_metadata(path: Path, data: "Mapping[str, object]") -> None:
    """Write ``data`` as JSON via ``temp + os.replace`` for atomic update."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(dict(data), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temp_path, path)


def detect_completed_fold(model_dir: Path, fold_year: int) -> bool:
    """Return True iff ``model_dir/metadata.json`` exists and is completed.

    The metadata's ``fold_year`` must match the requested fold; otherwise the
    checkpoint belongs to a different fold and we must not skip it.
    """
    metadata_path = model_dir / METADATA_FILENAME
    if not metadata_path.exists():
        return False
    try:
        parsed = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    if not isinstance(parsed, dict):
        return False
    if parsed.get(METADATA_STATUS_KEY) != COMPLETED_FOLD_STATUS:
        return False
    stored_year = parsed.get(METADATA_FOLD_KEY)
    if stored_year is None:
        return False
    try:
        return int(stored_year) == int(fold_year)  # pyright: ignore[reportArgumentType]
    except (TypeError, ValueError):
        return False


def _dcg_at_k(relevances: list[int]) -> float:
    total = 0.0
    for index in range(min(NDCG_AT_K, len(relevances))):
        total += float(relevances[index]) / float(np.log2(index + NDCG_LOG2_OFFSET))
    return total


def _ndcg_at_k(predictions: list[float], labels: list[int]) -> float:
    if not predictions:
        return 0.0
    order = sorted(range(len(predictions)), key=lambda i: predictions[i], reverse=True)
    ranked = [labels[i] for i in order]
    ideal = sorted(labels, reverse=True)
    dcg = _dcg_at_k(ranked)
    idcg = _dcg_at_k(ideal)
    if idcg == 0:
        return 0.0
    return dcg / idcg


def compute_per_bucket_val_ndcg(
    predictions: list[float],
    labels: list[int],
    bucket_dim_values: dict[str, str],
) -> dict[str, float]:
    """Return ``{dim: NDCG@3}`` for one race, keyed by bucket dimension.

    The same flat (predictions, labels) pair is scored once and replicated per
    dimension key so callers can aggregate per-bucket NDCG without re-sorting.
    """
    if len(predictions) != len(labels):
        raise ValueError(
            f"predictions and labels must align; got {len(predictions)} vs {len(labels)}",
        )
    if not bucket_dim_values:
        return {}
    score = _ndcg_at_k(predictions, labels)
    return {dim: score for dim in bucket_dim_values}


def stratified_kfold_indices(
    df: pd.DataFrame,
    strata_cols: list[str],
    n_folds: int,
    seed: int,
) -> list[tuple[IntArray, IntArray]]:
    """Stratified K-fold by composite strata key, no race_id overlap allowed.

    The split is performed at race level (one row per race_id, stratified by the
    first row's strata value) so that every row of a given race is fully in
    train or fully in val. Used by Optuna HPO.
    """
    if n_folds < HPO_MIN_FOLDS:
        raise ValueError(f"n_folds must be >= {HPO_MIN_FOLDS}, got {n_folds}")
    if "race_id" not in df.columns:
        raise ValueError("df must contain a 'race_id' column")
    if not strata_cols:
        raise ValueError("strata_cols must not be empty")
    missing = [col for col in strata_cols if col not in df.columns]
    if missing:
        raise ValueError(f"strata_cols missing from df: {missing}")
    race_lookup = df.drop_duplicates(subset=["race_id"]).reset_index(drop=True)
    strata_series = race_lookup[strata_cols[0]].astype(str)
    for col in strata_cols[1:]:
        next_series = race_lookup[col].astype(str)
        strata_series = strata_series.str.cat(next_series, sep="|")
    splitter = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=seed)
    race_ids = df["race_id"].to_numpy()
    race_to_positions: dict[str, list[int]] = {}
    for position, race_id in enumerate(race_ids.tolist()):
        race_to_positions.setdefault(str(race_id), []).append(position)
    unique_races = race_lookup["race_id"].astype(str).to_numpy()
    indices: list[tuple[IntArray, IntArray]] = []
    base_unique = np.arange(len(unique_races), dtype=np.int64)
    for train_race_pos, val_race_pos in splitter.split(
        base_unique, strata_series.to_numpy(),
    ):
        train_positions: list[int] = []
        val_positions: list[int] = []
        for race_idx in train_race_pos.tolist():
            train_positions.extend(race_to_positions[unique_races[int(race_idx)]])
        for race_idx in val_race_pos.tolist():
            val_positions.extend(race_to_positions[unique_races[int(race_idx)]])
        train_idx = np.array(sorted(train_positions), dtype=np.int64)
        val_idx = np.array(sorted(val_positions), dtype=np.int64)
        train_races = set(race_ids[train_idx].tolist())
        val_races = set(race_ids[val_idx].tolist())
        overlap = train_races & val_races
        if overlap:
            raise AssertionError(
                f"race_id overlap detected between train/val: {sorted(overlap)[:5]}...",
            )
        indices.append((train_idx, val_idx))
    return indices
