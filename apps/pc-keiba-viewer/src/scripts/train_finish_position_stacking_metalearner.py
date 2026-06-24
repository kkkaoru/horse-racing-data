"""Iter 2 of the v8 iterative loop: L8 stacking via Ridge meta-learner.

Inputs (per cat):
* v7-lineage enriched parquet (existing baseline predictions, with
  ``predicted_score``, ``actual_finish_position``, ``race_id``, etc.).
* JRA only: actual running-style label (``jvd_se.kyakushitsu_hantei``,
  values 0-4) supplied as a side parquet via ``--running-style-parquet``.
  NAR has no historical truth labels so it stacks only on race-context.

Output: a per-fold OOS predictions parquet partitioned by ``category`` /
``race_year`` and a per-fold ``metadata.json`` recording the picked
``alpha`` and OOS metrics. Predictions include a new ``predicted_rank``
column (the iter2 rerank) so downstream tooling can read the same schema
as ``v7-lineage`` enriched parquet.

The CLI has two modes:

* ``--mode build-dataset`` -> read enriched parquet, optionally join the
  running-style label parquet (JRA only), and write a stacking dataset
  parquet partitioned by ``category`` / ``race_year``. Race-context (kyori,
  track_code, shusso_tosu, grade_code, kyoso_joken_code) is read from a
  pre-extracted race-context parquet so this script never talks to PG
  directly (PG aggregation stalls hit prior iters).
* ``--mode train`` -> walk-forward train a ridge regressor for each fold
  year on rows where ``race_year < fold_year`` and predict on
  ``race_year == fold_year``. Predictions are reranked within each race
  and written to the iter2 predictions parquet.

All I/O is injected via TypedDict deps so the unit tests can stay fully
mocked (no PG, no real parquet writes).

Run with::

    uv run python src/scripts/train_finish_position_stacking_metalearner.py \\
        --mode build-dataset --cat jra \\
        --baseline-parquet-root tmp/v8/enriched-predictions/v7-lineage-wf-21y \\
        --race-context-parquet tmp/v8/iter2-race-context/category=jra.parquet \\
        --running-style-parquet tmp/v8/iter2-running-style-jra.parquet \\
        --output-root tmp/v8/iter2-stacking-dataset

    uv run python src/scripts/train_finish_position_stacking_metalearner.py \\
        --mode train --cat jra \\
        --dataset-root tmp/v8/iter2-stacking-dataset \\
        --output-predictions-root tmp/bucket-eval/finish-position/iter2-jra-cb+rs-stack-v8/predictions \\
        --model-version iter2-jra-cb+rs-stack-v8
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, TypedDict, cast

import numpy as np
import polars as pl
from sklearn.linear_model import Ridge

MODE_BUILD_DATASET: str = "build-dataset"
MODE_TRAIN: str = "train"
SUPPORTED_MODES: tuple[str, str] = (MODE_BUILD_DATASET, MODE_TRAIN)

CATEGORY_JRA: str = "jra"
CATEGORY_NAR: str = "nar"
SUPPORTED_CATEGORIES: tuple[str, str] = (CATEGORY_JRA, CATEGORY_NAR)

RACE_ID_COLUMN: str = "race_id"
RACE_YEAR_COLUMN: str = "race_year"
CATEGORY_COLUMN: str = "category"
KETTO_TOROKU_BANGO_COLUMN: str = "ketto_toroku_bango"
PREDICTED_SCORE_COLUMN: str = "predicted_score"
PREDICTED_RANK_COLUMN: str = "predicted_rank"
ACTUAL_FINISH_POSITION_COLUMN: str = "actual_finish_position"
KYAKUSHITSU_HANTEI_COLUMN: str = "kyakushitsu_hantei"
KYORI_COLUMN: str = "kyori"
TRACK_CODE_COLUMN: str = "track_code"
SHUSSO_TOSU_COLUMN: str = "shusso_tosu"
GRADE_CODE_COLUMN: str = "grade_code"
KYOSO_JOKEN_CODE_COLUMN: str = "kyoso_joken_code"

KAISAI_NEN_COLUMN: str = "kaisai_nen"
KAISAI_TSUKIHI_COLUMN: str = "kaisai_tsukihi"
KEIBAJO_CODE_COLUMN: str = "keibajo_code"
RACE_BANGO_COLUMN: str = "race_bango"

KYAKUSHITSU_CLASSES: tuple[int, int, int, int, int] = (0, 1, 2, 3, 4)

# JRA / NAR distance bands (meters); 5 buckets per plan spec.
DISTANCE_BAND_EDGES: tuple[int, int, int, int] = (1200, 1600, 2000, 2400)
DISTANCE_BAND_LABELS: tuple[str, str, str, str, str] = (
    "sprint",
    "mile",
    "intermediate",
    "long",
    "extended",
)

# JRA track_code grouping: dirt vs turf. Track-codes 23-29 = dirt
# (per JRA data spec); 10-22 = turf; >=51 = barrier/障害. We collapse
# to a single dirt flag and ignore obstacle races (rare anyway).
TURF_TRACK_CODE_PREFIXES: tuple[str, ...] = (
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22",
)
DIRT_TRACK_CODE_PREFIXES: tuple[str, ...] = (
    "23", "24", "25", "26", "27", "28", "29",
)

DEFAULT_ALPHA_GRID: tuple[float, float, float, float] = (0.1, 1.0, 10.0, 100.0)
DEFAULT_RIDGE_RANDOM_STATE: int = 20260604
DEFAULT_CV_FOLDS: int = 5
MIN_SAMPLES_FOR_TRAINING: int = 1000

# Top-N / place metric thresholds reused by the OOS metric helper.
TOP1_FINISH: int = 1
TOP3_FINISH: int = 3


class BuildDatasetArgs(TypedDict):
    mode: str
    cat: str
    baseline_parquet_root: Path
    race_context_parquet: Path
    running_style_parquet: Path | None
    output_root: Path


class TrainArgs(TypedDict):
    mode: str
    cat: str
    dataset_root: Path
    output_predictions_root: Path
    model_version: str
    alpha_grid: tuple[float, ...]
    cv_folds: int
    random_state: int
    fold_years: tuple[int, ...] | None


class ParquetDirReaderLike(Protocol):
    def __call__(self, path: Path) -> pl.DataFrame: ...


class ParquetFileReaderLike(Protocol):
    def __call__(self, path: Path) -> pl.DataFrame: ...


class PartitionedParquetWriterLike(Protocol):
    def __call__(self, frame: pl.DataFrame, output_dir: Path) -> None: ...


class JsonWriterLike(Protocol):
    def __call__(self, payload: dict[str, object], path: Path) -> None: ...


class RidgeFactoryLike(Protocol):
    def __call__(self, *, alpha: float, random_state: int) -> Ridge: ...


class NowFactoryLike(Protocol):
    def __call__(self) -> datetime: ...


class BuildDeps(TypedDict):
    baseline_reader: ParquetDirReaderLike
    race_context_reader: ParquetFileReaderLike
    running_style_reader: ParquetFileReaderLike
    parquet_writer: PartitionedParquetWriterLike


class TrainDeps(TypedDict):
    dataset_reader: ParquetDirReaderLike
    parquet_writer: PartitionedParquetWriterLike
    json_writer: JsonWriterLike
    ridge_factory: RidgeFactoryLike
    now: NowFactoryLike


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="train_finish_position_stacking_metalearner")
    parser.add_argument("--mode", choices=list(SUPPORTED_MODES), required=True)
    parser.add_argument("--cat", choices=list(SUPPORTED_CATEGORIES), required=True)
    parser.add_argument("--baseline-parquet-root", type=Path, default=None)
    parser.add_argument("--race-context-parquet", type=Path, default=None)
    parser.add_argument("--running-style-parquet", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--dataset-root", type=Path, default=None)
    parser.add_argument("--output-predictions-root", type=Path, default=None)
    parser.add_argument("--model-version", default=None)
    parser.add_argument(
        "--alpha-grid",
        default=",".join(str(x) for x in DEFAULT_ALPHA_GRID),
        help="Comma-separated Ridge alpha candidates for inner CV.",
    )
    parser.add_argument("--cv-folds", type=int, default=DEFAULT_CV_FOLDS)
    parser.add_argument("--random-state", type=int, default=DEFAULT_RIDGE_RANDOM_STATE)
    parser.add_argument(
        "--fold-years",
        default=None,
        help="Optional comma-separated list of OOS fold years (default: all years in dataset).",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def normalize_build_dataset_args(args: argparse.Namespace) -> BuildDatasetArgs:
    if args.baseline_parquet_root is None:
        raise ValueError("--baseline-parquet-root is required for --mode build-dataset")
    if args.race_context_parquet is None:
        raise ValueError("--race-context-parquet is required for --mode build-dataset")
    if args.output_root is None:
        raise ValueError("--output-root is required for --mode build-dataset")
    running_style: Path | None = (
        Path(args.running_style_parquet) if args.running_style_parquet is not None else None
    )
    return {
        "mode": MODE_BUILD_DATASET,
        "cat": cast(str, args.cat),
        "baseline_parquet_root": Path(args.baseline_parquet_root),
        "race_context_parquet": Path(args.race_context_parquet),
        "running_style_parquet": running_style,
        "output_root": Path(args.output_root),
    }


def normalize_train_args(args: argparse.Namespace) -> TrainArgs:
    if args.dataset_root is None:
        raise ValueError("--dataset-root is required for --mode train")
    if args.output_predictions_root is None:
        raise ValueError("--output-predictions-root is required for --mode train")
    if args.model_version is None:
        raise ValueError("--model-version is required for --mode train")
    alpha_grid = tuple(float(x) for x in str(args.alpha_grid).split(",") if x.strip())
    if not alpha_grid:
        raise ValueError("--alpha-grid must contain at least one value")
    fold_years: tuple[int, ...] | None = None
    if args.fold_years is not None:
        fold_years = tuple(int(x) for x in str(args.fold_years).split(",") if x.strip())
        if not fold_years:
            raise ValueError("--fold-years cannot be empty when provided")
    return {
        "mode": MODE_TRAIN,
        "cat": cast(str, args.cat),
        "dataset_root": Path(args.dataset_root),
        "output_predictions_root": Path(args.output_predictions_root),
        "model_version": cast(str, args.model_version),
        "alpha_grid": alpha_grid,
        "cv_folds": int(args.cv_folds),
        "random_state": int(args.random_state),
        "fold_years": fold_years,
    }


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def default_read_parquet_dir(path: Path) -> pl.DataFrame:
    """Read every parquet under ``path`` recursively into one DataFrame.

    Hive-style partition keys (``category=…``, ``race_year=…``) are decoded
    from the directory names so callers that read a partitioned tree still
    see those columns on the returned frame.
    """
    if path.is_file():
        return pl.read_parquet(path.as_posix())
    parts = sorted(path.rglob("*.parquet"))
    frames: list[pl.DataFrame] = []
    for part in parts:
        frame = pl.read_parquet(part.as_posix())
        for segment in part.relative_to(path).parts[:-1]:
            if "=" not in segment:
                continue
            key, raw_value = segment.split("=", 1)
            frame = frame.with_columns(pl.lit(coerce_partition_value(key, raw_value)).alias(key))
        frames.append(frame)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def coerce_partition_value(key: str, raw_value: str) -> int | str:
    """Best-effort cast for hive partition values pulled from path segments.

    ``race_year`` is always integer-valued so we coerce it; everything else
    stays a string, matching DuckDB's hive_partitioning default behaviour.
    """
    if key == RACE_YEAR_COLUMN:
        try:
            return int(raw_value)
        except ValueError:
            return raw_value
    return raw_value


def default_read_parquet_file(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path.as_posix())


def default_write_partitioned_parquet(frame: pl.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(
        output_dir.as_posix(),
        partition_by=[CATEGORY_COLUMN, RACE_YEAR_COLUMN],
    )


def default_write_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def default_ridge_factory(*, alpha: float, random_state: int) -> Ridge:
    return Ridge(alpha=alpha, random_state=random_state)


def assign_distance_band(kyori: object) -> str:
    """Map a race ``kyori`` (meters) to one of the 5 distance bands.

    Non-numeric / missing values fall back to ``"intermediate"`` since the
    median JRA / NAR race sits in that band anyway, so we avoid creating a
    sixth bucket for sparse rows.
    """
    if kyori is None:
        return DISTANCE_BAND_LABELS[2]
    try:
        meters = int(float(str(kyori)))
    except (TypeError, ValueError):
        return DISTANCE_BAND_LABELS[2]
    if meters < DISTANCE_BAND_EDGES[0]:
        return DISTANCE_BAND_LABELS[0]
    if meters < DISTANCE_BAND_EDGES[1]:
        return DISTANCE_BAND_LABELS[1]
    if meters < DISTANCE_BAND_EDGES[2]:
        return DISTANCE_BAND_LABELS[2]
    if meters < DISTANCE_BAND_EDGES[3]:
        return DISTANCE_BAND_LABELS[3]
    return DISTANCE_BAND_LABELS[4]


def assign_surface_dirt_flag(track_code: object) -> int:
    """Return ``1`` for dirt tracks, ``0`` for turf (default) or unknown."""
    if track_code is None:
        return 0
    text = str(track_code).strip()
    if not text:
        return 0
    return 1 if text.startswith(DIRT_TRACK_CODE_PREFIXES) else 0


def encode_kyakushitsu_one_hot(values: pl.Series) -> pl.DataFrame:
    """Return a 5-wide one-hot frame for the JRA kyakushitsu_hantei field.

    Unknown / null values map to the ``0`` (未判定) column, which keeps the
    feature dense and avoids silent NaN propagation downstream.
    """
    numeric = (
        values.cast(pl.Float64, strict=False)
        .fill_null(0.0)
        .cast(pl.Int64)
        .clip(0, 4)
    )
    columns = {
        f"kyakushitsu_{cls}": (numeric == cls).cast(pl.Int64) for cls in KYAKUSHITSU_CLASSES
    }
    return pl.DataFrame(columns)


def encode_distance_band_one_hot(bands: pl.Series) -> pl.DataFrame:
    columns = {
        f"distance_band_{label}": (bands == label).cast(pl.Int64) for label in DISTANCE_BAND_LABELS
    }
    return pl.DataFrame(columns)


def add_race_level_aggregates(frame: pl.DataFrame) -> pl.DataFrame:
    """Append per-race mean / std of ``predicted_score`` plus ``num_horses_in_race``."""
    frame = frame.with_columns(
        pl.col(PREDICTED_SCORE_COLUMN).mean().over(RACE_ID_COLUMN).cast(pl.Float64).alias(
            "mean_field_score",
        ),
        pl.col(PREDICTED_SCORE_COLUMN).std().over(RACE_ID_COLUMN).fill_null(0.0).cast(pl.Float64).alias(
            "score_std_in_race",
        ),
        pl.col(PREDICTED_SCORE_COLUMN).count().over(RACE_ID_COLUMN).cast(pl.Int64).alias(
            "num_horses_in_race",
        ),
    )
    return frame.with_columns(
        (pl.col(PREDICTED_RANK_COLUMN).cast(pl.Float64) / pl.col("num_horses_in_race").cast(pl.Float64)).alias(
            "predicted_rank_norm",
        ),
    )


def assemble_stacking_dataset(
    baseline: pl.DataFrame,
    race_context: pl.DataFrame,
    running_style: pl.DataFrame | None,
    cat: str,
) -> pl.DataFrame:
    """Join enriched predictions with race-context and (JRA only) running style.

    The race-context parquet is keyed by ``race_id`` and supplies ``kyori`` /
    ``track_code`` / ``shusso_tosu`` (which we ignore in favour of the
    per-race count derived from the baseline itself, since the baseline is
    the source of truth for horses we actually score). Running-style parquet
    is keyed by (race_id, ketto_toroku_bango).
    """
    keep_cols = [
        RACE_ID_COLUMN,
        KETTO_TOROKU_BANGO_COLUMN,
        PREDICTED_SCORE_COLUMN,
        PREDICTED_RANK_COLUMN,
        ACTUAL_FINISH_POSITION_COLUMN,
        RACE_YEAR_COLUMN,
        CATEGORY_COLUMN,
        GRADE_CODE_COLUMN,
        KYOSO_JOKEN_CODE_COLUMN,
    ]
    base = baseline.select([c for c in keep_cols if c in baseline.columns])
    base = base.drop_nulls(subset=[ACTUAL_FINISH_POSITION_COLUMN, PREDICTED_RANK_COLUMN])
    ctx_cols = [RACE_ID_COLUMN, KYORI_COLUMN, TRACK_CODE_COLUMN, SHUSSO_TOSU_COLUMN]
    ctx = race_context.select([c for c in ctx_cols if c in race_context.columns]).unique(
        subset=[RACE_ID_COLUMN], maintain_order=True,
    )
    merged = base.join(ctx, how="left", on=RACE_ID_COLUMN)
    merged = merged.with_columns(
        pl.col(KYORI_COLUMN).map_elements(assign_distance_band, return_dtype=pl.String).alias(
            "distance_band",
        ),
        pl.col(TRACK_CODE_COLUMN).map_elements(assign_surface_dirt_flag, return_dtype=pl.Int64).alias(
            "surface_dirt_flag",
        ),
    )
    distance_one_hot = encode_distance_band_one_hot(merged["distance_band"])
    merged = pl.concat([merged, distance_one_hot], how="horizontal")
    if cat == CATEGORY_JRA:
        merged = _attach_running_style(merged, running_style)
    else:
        merged = merged.with_columns(
            [pl.lit(0).alias(f"kyakushitsu_{cls}") for cls in KYAKUSHITSU_CLASSES],
        )
    merged = add_race_level_aggregates(merged)
    return merged


def _attach_running_style(
    merged: pl.DataFrame, running_style: pl.DataFrame | None
) -> pl.DataFrame:
    if running_style is None or running_style.is_empty():
        return merged.with_columns(
            [pl.lit(0).alias(f"kyakushitsu_{cls}") for cls in KYAKUSHITSU_CLASSES],
        )
    rs_cols = [RACE_ID_COLUMN, KETTO_TOROKU_BANGO_COLUMN, KYAKUSHITSU_HANTEI_COLUMN]
    rs = running_style.select([c for c in rs_cols if c in running_style.columns]).unique(
        subset=[RACE_ID_COLUMN, KETTO_TOROKU_BANGO_COLUMN], maintain_order=True,
    )
    merged = merged.join(rs, how="left", on=[RACE_ID_COLUMN, KETTO_TOROKU_BANGO_COLUMN])
    one_hot = encode_kyakushitsu_one_hot(merged[KYAKUSHITSU_HANTEI_COLUMN])
    merged = pl.concat([merged, one_hot], how="horizontal")
    return merged


def stacking_feature_columns(frame: pl.DataFrame) -> list[str]:
    """Return the ordered feature columns used by the Ridge meta-learner."""
    base_cols = [
        PREDICTED_SCORE_COLUMN,
        "predicted_rank_norm",
        "mean_field_score",
        "score_std_in_race",
        "num_horses_in_race",
        "surface_dirt_flag",
        RACE_YEAR_COLUMN,
    ]
    band_cols = [f"distance_band_{label}" for label in DISTANCE_BAND_LABELS]
    kyaku_cols = [f"kyakushitsu_{cls}" for cls in KYAKUSHITSU_CLASSES]
    return [c for c in base_cols + band_cols + kyaku_cols if c in frame.columns]


def pick_alpha_via_cv(
    train_frame: pl.DataFrame,
    *,
    alpha_grid: tuple[float, ...],
    cv_folds: int,
    random_state: int,
    ridge_factory: RidgeFactoryLike,
) -> tuple[float, dict[float, float]]:
    """Pick the alpha that minimises mean per-fold OOS RMSE on a year-block CV.

    We stratify by ``race_year`` so each inner fold sees a contiguous slice
    of seasons. This matches the outer walk-forward expectation.
    """
    feature_cols = stacking_feature_columns(train_frame)
    if not feature_cols:
        raise ValueError("training frame has no usable feature columns")
    years = sorted(train_frame[RACE_YEAR_COLUMN].unique().to_list())
    n_folds = max(2, min(cv_folds, len(years)))
    # Assign years to folds in chronological order (earlier years → lower fold indices).
    # Forward-chain CV: fold k trains on folds 0..k-1 and validates on fold k.
    n_years = len(years)
    fold_assignment = [int(i * n_folds / n_years) for i in range(n_years)]
    year_to_fold = dict(zip(years, fold_assignment, strict=True))
    score_grid: dict[float, float] = {}
    for alpha in alpha_grid:
        fold_rmses: list[float] = []
        for fold_idx in range(n_folds):
            holdout_years = {y for y, f in year_to_fold.items() if f == fold_idx}
            if not holdout_years:
                continue
            # Forward-chain: train only on years assigned to earlier folds
            train_years = {y for y, f in year_to_fold.items() if f < fold_idx}
            train_slice = train_frame.filter(pl.col(RACE_YEAR_COLUMN).is_in(train_years))
            val_slice = train_frame.filter(pl.col(RACE_YEAR_COLUMN).is_in(holdout_years))
            if train_slice.is_empty() or val_slice.is_empty():
                continue
            model = ridge_factory(alpha=alpha, random_state=random_state)
            x_train = train_slice.select(feature_cols).cast(pl.Float64).to_numpy()
            y_train = train_slice[ACTUAL_FINISH_POSITION_COLUMN].cast(pl.Float64).to_numpy()
            x_val = val_slice.select(feature_cols).cast(pl.Float64).to_numpy()
            y_val = val_slice[ACTUAL_FINISH_POSITION_COLUMN].cast(pl.Float64).to_numpy()
            model.fit(x_train, y_train)
            preds = model.predict(x_val)
            fold_rmses.append(float(np.sqrt(np.mean((preds - y_val) ** 2))))
        score_grid[alpha] = float(np.mean(fold_rmses)) if fold_rmses else float("inf")
    best_alpha = min(score_grid.items(), key=lambda item: item[1])[0]
    return (best_alpha, score_grid)


def rerank_within_race(frame: pl.DataFrame, score_column: str) -> pl.DataFrame:
    """Rerank rows by ``score_column`` (ascending = better) within each race."""
    return frame.with_columns(
        pl.col(score_column)
        .rank(method="ordinal", descending=False)
        .over(RACE_ID_COLUMN)
        .cast(pl.Int64)
        .alias(PREDICTED_RANK_COLUMN),
    )


def compute_oos_metrics(frame: pl.DataFrame) -> dict[str, float]:
    """Compute the 4-metric OOS scoreboard (top1 / place2 / place3 / top3_box).

    The metric definitions mirror ``tmp/v8/compute_metrics_duckdb.py``:
    top1 = winner is at predicted_rank==1, place2 = #2 finisher is at
    predicted_rank==2, place3 likewise, top3_box = all 3 of predicted_rank<=3
    finish at actual_finish_position<=3.
    """
    if frame.is_empty():
        return {"races": 0, "top1": 0.0, "place2": 0.0, "place3": 0.0, "top3_box": 0.0}
    rank_col = pl.col(PREDICTED_RANK_COLUMN).cast(pl.Int64)
    actual_col = pl.col(ACTUAL_FINISH_POSITION_COLUMN).cast(pl.Int64)
    per_race = frame.group_by(RACE_ID_COLUMN).agg(
        ((rank_col == TOP1_FINISH) & (actual_col == TOP1_FINISH)).max().cast(pl.Int64).alias("top1"),
        ((rank_col == 2) & (actual_col == 2)).max().cast(pl.Int64).alias("place2"),
        ((rank_col == TOP3_FINISH) & (actual_col == TOP3_FINISH)).max().cast(pl.Int64).alias("place3"),
        (((rank_col <= TOP3_FINISH) & (actual_col <= TOP3_FINISH)).sum() == TOP3_FINISH)
        .cast(pl.Int64)
        .alias("top3_box"),
    )
    return {
        "races": int(per_race.height),
        "top1": float(cast(float, per_race["top1"].mean())),
        "place2": float(cast(float, per_race["place2"].mean())),
        "place3": float(cast(float, per_race["place3"].mean())),
        "top3_box": float(cast(float, per_race["top3_box"].mean())),
    }


def filter_to_fold_year(dataset: pl.DataFrame, fold_year: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    train_df = dataset.filter(pl.col(RACE_YEAR_COLUMN) < fold_year)
    val_df = dataset.filter(pl.col(RACE_YEAR_COLUMN) == fold_year)
    return (train_df, val_df)


def format_iso_now(now: datetime) -> str:
    return now.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def train_one_fold(
    dataset: pl.DataFrame,
    *,
    cat: str,
    fold_year: int,
    model_version: str,
    alpha_grid: tuple[float, ...],
    cv_folds: int,
    random_state: int,
    ridge_factory: RidgeFactoryLike,
    now: datetime,
) -> tuple[pl.DataFrame, dict[str, object]]:
    """Train Ridge on ``year < fold_year``, predict on ``year == fold_year``.

    Returns the reranked OOS predictions plus a metadata dict including the
    picked alpha and per-fold OOS metrics.
    """
    train_frame, val_frame = filter_to_fold_year(dataset, fold_year)
    if len(train_frame) < MIN_SAMPLES_FOR_TRAINING or val_frame.is_empty():
        meta_skip: dict[str, object] = {
            "category": cat,
            "fold_year": fold_year,
            "model_version": model_version,
            "trained_at": format_iso_now(now),
            "alpha_picked": None,
            "train_size": int(len(train_frame)),
            "val_size": int(len(val_frame)),
            "skipped": True,
            "skip_reason": (
                "insufficient training rows"
                if len(train_frame) < MIN_SAMPLES_FOR_TRAINING
                else "no OOS rows for fold year"
            ),
        }
        return (pl.DataFrame(), meta_skip)
    alpha_picked, score_grid = pick_alpha_via_cv(
        train_frame,
        alpha_grid=alpha_grid,
        cv_folds=cv_folds,
        random_state=random_state,
        ridge_factory=ridge_factory,
    )
    feature_cols = stacking_feature_columns(train_frame)
    model = ridge_factory(alpha=alpha_picked, random_state=random_state)
    x_train = train_frame.select(feature_cols).cast(pl.Float64).to_numpy()
    y_train = train_frame[ACTUAL_FINISH_POSITION_COLUMN].cast(pl.Float64).to_numpy()
    model.fit(x_train, y_train)
    x_val = val_frame.select(feature_cols).cast(pl.Float64).to_numpy()
    val_frame = val_frame.with_columns(pl.Series("stacking_score", model.predict(x_val)))
    reranked = rerank_within_race(val_frame, "stacking_score")
    reranked = reranked.with_columns(pl.lit(model_version).alias("model_version"))
    metrics = compute_oos_metrics(reranked)
    meta: dict[str, object] = {
        "category": cat,
        "fold_year": fold_year,
        "model_version": model_version,
        "trained_at": format_iso_now(now),
        "alpha_picked": float(alpha_picked),
        "alpha_grid_scores": {str(k): v for k, v in score_grid.items()},
        "train_size": int(len(train_frame)),
        "val_size": int(len(val_frame)),
        "skipped": False,
        "oos_metrics": metrics,
        "feature_columns": feature_cols,
    }
    return (reranked, meta)


def resolve_fold_years(
    dataset: pl.DataFrame, requested: tuple[int, ...] | None
) -> tuple[int, ...]:
    available = sorted(int(y) for y in dataset[RACE_YEAR_COLUMN].unique().to_list())
    if requested is None:
        return tuple(available[1:]) if len(available) > 1 else tuple(available)
    keep = tuple(year for year in requested if year in set(available))
    if not keep:
        raise ValueError(f"none of the requested fold years {requested!r} exist in the dataset")
    return keep


def run_build_dataset(args: BuildDatasetArgs, deps: BuildDeps) -> int:
    cat = args["cat"]
    baseline = deps["baseline_reader"](args["baseline_parquet_root"] / f"category={cat}")
    if baseline.is_empty():
        raise ValueError(f"baseline parquet for category={cat} is empty: {args['baseline_parquet_root']}")
    race_context = deps["race_context_reader"](args["race_context_parquet"])
    running_style: pl.DataFrame | None = None
    if args["running_style_parquet"] is not None and cat == CATEGORY_JRA:
        running_style = deps["running_style_reader"](args["running_style_parquet"])
    dataset = assemble_stacking_dataset(baseline, race_context, running_style, cat)
    if CATEGORY_COLUMN not in dataset.columns:
        dataset = dataset.with_columns(pl.lit(cat).alias(CATEGORY_COLUMN))
    deps["parquet_writer"](dataset, args["output_root"])
    return 0


def run_train(args: TrainArgs, deps: TrainDeps) -> int:
    cat = args["cat"]
    dataset = deps["dataset_reader"](args["dataset_root"] / f"category={cat}")
    if dataset.is_empty():
        raise ValueError(f"stacking dataset for category={cat} is empty: {args['dataset_root']}")
    if CATEGORY_COLUMN not in dataset.columns:
        dataset = dataset.with_columns(pl.lit(cat).alias(CATEGORY_COLUMN))
    if RACE_YEAR_COLUMN not in dataset.columns:
        raise ValueError(f"stacking dataset is missing {RACE_YEAR_COLUMN!r} column")
    fold_years = resolve_fold_years(dataset, args["fold_years"])
    all_preds: list[pl.DataFrame] = []
    all_meta: list[dict[str, object]] = []
    now = deps["now"]()
    for fold_year in fold_years:
        preds, meta = train_one_fold(
            dataset,
            cat=cat,
            fold_year=fold_year,
            model_version=args["model_version"],
            alpha_grid=args["alpha_grid"],
            cv_folds=args["cv_folds"],
            random_state=args["random_state"],
            ridge_factory=deps["ridge_factory"],
            now=now,
        )
        if not preds.is_empty():
            all_preds.append(preds)
        all_meta.append(meta)
    if all_preds:
        combined = pl.concat(all_preds, how="diagonal_relaxed")
        deps["parquet_writer"](combined, args["output_predictions_root"])
    metadata_path = args["output_predictions_root"].parent / "metadata.json"
    deps["json_writer"](
        {
            "model_version": args["model_version"],
            "category": cat,
            "alpha_grid": list(args["alpha_grid"]),
            "cv_folds": args["cv_folds"],
            "random_state": args["random_state"],
            "fold_years": list(fold_years),
            "fold_results": all_meta,
            "generated_at": format_iso_now(now),
        },
        metadata_path,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == MODE_BUILD_DATASET:
        build_args = normalize_build_dataset_args(args)
        deps: BuildDeps = {
            "baseline_reader": default_read_parquet_dir,
            "race_context_reader": default_read_parquet_file,
            "running_style_reader": default_read_parquet_file,
            "parquet_writer": default_write_partitioned_parquet,
        }
        return run_build_dataset(build_args, deps)
    if args.mode == MODE_TRAIN:
        train_args = normalize_train_args(args)
        train_deps: TrainDeps = {
            "dataset_reader": default_read_parquet_dir,
            "parquet_writer": default_write_partitioned_parquet,
            "json_writer": default_write_json,
            "ridge_factory": default_ridge_factory,
            "now": now_utc,
        }
        return run_train(train_args, train_deps)
    raise ValueError(f"unknown mode: {args.mode}")


if __name__ == "__main__":
    sys.exit(main())
