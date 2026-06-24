"""Iter 11 of the v8 iterative loop: L6 calibrated stacking meta-learner.

Combines (a) a pre-built per-category stacking dataset (carrying baseline
predictions + race-context features + horse panel features) with (b) a
LightGBM ``regression_l2`` meta-learner trained per walk-forward fold and
(c) an optional isotonic re-rank afterwards. Unlike iter3's lambdarank
stacker, this layer regresses against ``actual_finish_position`` so the
meta output is a continuous "expected finish" score that can be calibrated
end-to-end before re-ranking inside each race.

CLI: ``--mode train``. Inputs match iter3's dataset schema so we can reuse
the iter3 stacking dataset (for JRA) and the iter11-built NAR dataset that
swaps v7 ``predicted_score`` for the iter9 NAR XGB predictions.

Run with::

    uv run python src/scripts/train_finish_position_stacking_meta.py \\
        --mode train --cat jra \\
        --dataset-root tmp/v8/iter3-stacking-dataset \\
        --output-predictions-root \
            tmp/bucket-eval/finish-position/iter11-jra-cb+meta-v8/predictions \\
        --output-model-dir tmp/models/jra-meta-v8-iter11-l6-wf-21y \\
        --model-version iter11-jra-cb+meta-v8
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

MODE_TRAIN: str = "train"
SUPPORTED_MODES: tuple[str, ...] = (MODE_TRAIN,)

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

# Same feature panel as iter3 lgbm stacking (so the only thing varying is
# the meta objective).
BASE_FEATURE_COLUMNS: tuple[str, ...] = (
    "predicted_score",
    "umaban",
    "futan_juryo",
    "horse_age",
    "tansho_ninkijun",
    "kyori",
    "shusso_tosu",
    "race_year_int",
    "horse_recent_kohan3f_avg5",
    "horse_recent_finish_position_avg5",
    "days_since_last_race",
    "horse_career_track_win_rate",
    "jockey_recent_30d_win_rate",
    "trainer_recent_30d_win_rate",
)
DERIVED_FEATURE_COLUMNS: tuple[str, ...] = (
    "predicted_rank_norm",
    "score_z_in_race",
    "score_max_in_race_delta",
    "score_min_in_race_delta",
    "kohan3f_pct_rank_in_field",
    "finish_pos_avg5_z_in_race",
    "field_size_log",
)
SURFACE_FEATURE_COLUMNS: tuple[str, ...] = ("surface_dirt_flag",)
DISTANCE_BAND_LABELS: tuple[str, str, str, str, str] = (
    "sprint",
    "mile",
    "intermediate",
    "long",
    "extended",
)
DISTANCE_BAND_EDGES: tuple[int, int, int, int] = (1200, 1600, 2000, 2400)
DIRT_TRACK_CODE_PREFIXES: tuple[str, ...] = (
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
)

DEFAULT_NUM_LEAVES: int = 31
DEFAULT_N_ESTIMATORS: int = 200
DEFAULT_LEARNING_RATE: float = 0.05
DEFAULT_MIN_CHILD_SAMPLES: int = 100
DEFAULT_LAMBDA_L2: float = 1.0
DEFAULT_EARLY_STOPPING: int = 20
DEFAULT_RANDOM_STATE_BASE: int = 42
DEFAULT_VAL_FRACTION: float = 0.10
MIN_SAMPLES_FOR_TRAINING: int = 1000

TOP1_FINISH: int = 1
TOP3_FINISH: int = 3


class TrainArgs(TypedDict):
    mode: str
    cat: str
    dataset_root: Path
    output_predictions_root: Path
    output_model_dir: Path
    model_version: str
    num_leaves: int
    n_estimators: int
    learning_rate: float
    min_child_samples: int
    lambda_l2: float
    early_stopping_rounds: int
    random_state_base: int
    fold_years: tuple[int, ...] | None
    val_fraction: float


class ParquetDirReaderLike(Protocol):
    def __call__(self, path: Path) -> pl.DataFrame: ...


class PartitionedParquetWriterLike(Protocol):
    def __call__(self, frame: pl.DataFrame, output_dir: Path) -> None: ...


class JsonWriterLike(Protocol):
    def __call__(self, payload: dict[str, object], path: Path) -> None: ...


class LightGBMRegressorLike(Protocol):
    feature_importances_: np.ndarray
    best_iteration_: int | None

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        *,
        eval_set: list[tuple[np.ndarray, np.ndarray]] | None,
        callbacks: list[object] | None,
    ) -> None: ...

    def predict(self, X: np.ndarray) -> np.ndarray: ...


class RegressorFactoryLike(Protocol):
    def __call__(
        self,
        *,
        num_leaves: int,
        n_estimators: int,
        learning_rate: float,
        min_child_samples: int,
        lambda_l2: float,
        random_state: int,
    ) -> LightGBMRegressorLike: ...


class EarlyStoppingFactoryLike(Protocol):
    def __call__(self, stopping_rounds: int) -> object: ...


class NowFactoryLike(Protocol):
    def __call__(self) -> datetime: ...


class TrainDeps(TypedDict):
    dataset_reader: ParquetDirReaderLike
    parquet_writer: PartitionedParquetWriterLike
    json_writer: JsonWriterLike
    regressor_factory: RegressorFactoryLike
    early_stopping_factory: EarlyStoppingFactoryLike
    now: NowFactoryLike


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="train_finish_position_stacking_meta")
    parser.add_argument("--mode", choices=list(SUPPORTED_MODES), required=True)
    parser.add_argument("--cat", choices=list(SUPPORTED_CATEGORIES), required=True)
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument("--output-predictions-root", type=Path, required=True)
    parser.add_argument("--output-model-dir", type=Path, required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--num-leaves", type=int, default=DEFAULT_NUM_LEAVES)
    parser.add_argument("--n-estimators", type=int, default=DEFAULT_N_ESTIMATORS)
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    parser.add_argument("--min-child-samples", type=int, default=DEFAULT_MIN_CHILD_SAMPLES)
    parser.add_argument("--lambda-l2", type=float, default=DEFAULT_LAMBDA_L2)
    parser.add_argument("--early-stopping-rounds", type=int, default=DEFAULT_EARLY_STOPPING)
    parser.add_argument("--random-state-base", type=int, default=DEFAULT_RANDOM_STATE_BASE)
    parser.add_argument(
        "--fold-years",
        default=None,
        help="Optional comma-separated OOS fold years (default: all years in dataset).",
    )
    parser.add_argument("--val-fraction", type=float, default=DEFAULT_VAL_FRACTION)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def normalize_train_args(args: argparse.Namespace) -> TrainArgs:
    fold_years: tuple[int, ...] | None = None
    if args.fold_years is not None:
        fold_years = tuple(int(x) for x in str(args.fold_years).split(",") if x.strip())
        if not fold_years:
            raise ValueError("--fold-years cannot be empty when provided")
    val_fraction = float(args.val_fraction)
    if not 0.0 < val_fraction < 1.0:
        raise ValueError("--val-fraction must be strictly between 0 and 1")
    return {
        "mode": MODE_TRAIN,
        "cat": cast(str, args.cat),
        "dataset_root": Path(args.dataset_root),
        "output_predictions_root": Path(args.output_predictions_root),
        "output_model_dir": Path(args.output_model_dir),
        "model_version": cast(str, args.model_version),
        "num_leaves": int(args.num_leaves),
        "n_estimators": int(args.n_estimators),
        "learning_rate": float(args.learning_rate),
        "min_child_samples": int(args.min_child_samples),
        "lambda_l2": float(args.lambda_l2),
        "early_stopping_rounds": int(args.early_stopping_rounds),
        "random_state_base": int(args.random_state_base),
        "fold_years": fold_years,
        "val_fraction": val_fraction,
    }


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def coerce_partition_value(key: str, raw_value: str) -> int | str:
    if key == RACE_YEAR_COLUMN:
        try:
            return int(raw_value)
        except ValueError:
            return raw_value
    return raw_value


def default_read_parquet_dir(path: Path) -> pl.DataFrame:
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


def default_write_partitioned_parquet(frame: pl.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(
        output_dir.as_posix(),
        partition_by=[CATEGORY_COLUMN, RACE_YEAR_COLUMN],
    )


def default_write_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def default_regressor_factory(
    *,
    num_leaves: int,
    n_estimators: int,
    learning_rate: float,
    min_child_samples: int,
    lambda_l2: float,
    random_state: int,
) -> LightGBMRegressorLike:
    import lightgbm as lgb

    regressor = lgb.LGBMRegressor(
        objective="regression_l2",
        metric="rmse",
        n_estimators=n_estimators,
        num_leaves=num_leaves,
        learning_rate=learning_rate,
        min_child_samples=min_child_samples,
        reg_lambda=lambda_l2,
        random_state=random_state,
        n_jobs=8,
        verbose=-1,
    )
    return cast(LightGBMRegressorLike, regressor)


def default_early_stopping_factory(stopping_rounds: int) -> object:
    import lightgbm as lgb

    return lgb.early_stopping(stopping_rounds=stopping_rounds, verbose=False)


def assign_distance_band(kyori: object) -> str:
    if kyori is None or (isinstance(kyori, float) and np.isnan(kyori)):
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
    if track_code is None:
        return 0
    text = str(track_code).strip()
    if not text:
        return 0
    return 1 if text.startswith(DIRT_TRACK_CODE_PREFIXES) else 0


def encode_distance_band_one_hot(bands: pl.Series) -> pl.DataFrame:
    columns = {
        f"distance_band_{label}": (bands == label).cast(pl.Int64) for label in DISTANCE_BAND_LABELS
    }
    return pl.DataFrame(columns)


def add_within_race_normalised(frame: pl.DataFrame) -> pl.DataFrame:
    field_size = pl.col(PREDICTED_RANK_COLUMN).count().over(RACE_ID_COLUMN).cast(pl.Float64)
    score = pl.col(PREDICTED_SCORE_COLUMN).cast(pl.Float64)
    score_mean = score.mean().over(RACE_ID_COLUMN)
    score_std = score.std().over(RACE_ID_COLUMN).fill_null(0.0)
    fpa = pl.col("horse_recent_finish_position_avg5").cast(pl.Float64)
    fpa_mean = fpa.mean().over(RACE_ID_COLUMN)
    fpa_std = fpa.std().over(RACE_ID_COLUMN).fill_null(0.0)
    return frame.with_columns(
        (pl.col(PREDICTED_RANK_COLUMN).cast(pl.Float64) / field_size.replace(0.0, 1.0)).alias(
            "predicted_rank_norm",
        ),
        ((score - score_mean) / score_std.replace(0.0, 1e-6)).alias("score_z_in_race"),
        (score - score.max().over(RACE_ID_COLUMN)).alias("score_max_in_race_delta"),
        (score - score.min().over(RACE_ID_COLUMN)).alias("score_min_in_race_delta"),
        pl.col("horse_recent_kohan3f_avg5")
        .rank(method="average")
        .over(RACE_ID_COLUMN)
        .truediv(pl.col("horse_recent_kohan3f_avg5").count().over(RACE_ID_COLUMN))
        .alias("kohan3f_pct_rank_in_field"),
        ((fpa - fpa_mean) / fpa_std.replace(0.0, 1e-6)).alias("finish_pos_avg5_z_in_race"),
        field_size.log1p().alias("field_size_log"),
    )


def add_categorical_one_hots(frame: pl.DataFrame) -> pl.DataFrame:
    bands = frame["kyori"].map_elements(assign_distance_band, return_dtype=pl.String)
    one_hot = encode_distance_band_one_hot(bands)
    out = frame.with_columns(
        pl.col("track_code")
        .map_elements(assign_surface_dirt_flag, return_dtype=pl.Int64)
        .alias("surface_dirt_flag"),
    )
    return pl.concat([out, one_hot], how="horizontal")


def assemble_feature_frame(frame: pl.DataFrame) -> pl.DataFrame:
    out = add_within_race_normalised(frame)
    out = add_categorical_one_hots(out)
    return out


def feature_columns(frame: pl.DataFrame) -> list[str]:
    band_cols = [f"distance_band_{label}" for label in DISTANCE_BAND_LABELS]
    candidate = (
        list(BASE_FEATURE_COLUMNS)
        + list(DERIVED_FEATURE_COLUMNS)
        + list(SURFACE_FEATURE_COLUMNS)
        + band_cols
    )
    return [c for c in candidate if c in frame.columns]


def rerank_within_race(frame: pl.DataFrame, score_column: str) -> pl.DataFrame:
    """Rerank rows by ``score_column`` (ascending = better since meta predicts
    expected finish position; lower = predicted earlier finish)."""
    return frame.with_columns(
        pl.col(score_column)
        .rank(method="ordinal", descending=False)
        .over(RACE_ID_COLUMN)
        .cast(pl.Int64)
        .alias(PREDICTED_RANK_COLUMN),
    )


def compute_oos_metrics(frame: pl.DataFrame) -> dict[str, float]:
    if frame.is_empty():
        return {"races": 0, "top1": 0.0, "place2": 0.0, "place3": 0.0, "top3_box": 0.0}
    rank_col = pl.col(PREDICTED_RANK_COLUMN).cast(pl.Int64)
    actual_col = pl.col(ACTUAL_FINISH_POSITION_COLUMN).cast(pl.Int64)
    per_race = frame.group_by(RACE_ID_COLUMN).agg(
        ((rank_col == TOP1_FINISH) & (actual_col == TOP1_FINISH)).max().cast(pl.Int64).alias("top1"),
        ((rank_col == 2) & (actual_col == 2)).max().cast(pl.Int64).alias("place2"),
        ((rank_col == TOP3_FINISH) & (actual_col == TOP3_FINISH)).max().cast(pl.Int64).alias("place3"),
        ((rank_col <= TOP3_FINISH) & (actual_col <= TOP3_FINISH)).sum().alias("box_sum"),
        pl.len().alias("field_size"),
    )
    per_race = per_race.with_columns(
        (pl.col("box_sum") >= pl.col("field_size").clip(upper_bound=TOP3_FINISH))
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


def split_train_val(
    train_frame: pl.DataFrame, val_fraction: float
) -> tuple[pl.DataFrame, pl.DataFrame]:
    sorted_frame = train_frame.sort([RACE_YEAR_COLUMN, RACE_ID_COLUMN], maintain_order=True)
    distinct_races = sorted_frame[RACE_ID_COLUMN].unique(maintain_order=True).to_list()
    n_val = max(1, int(round(len(distinct_races) * val_fraction)))
    if n_val >= len(distinct_races):
        n_val = len(distinct_races) - 1
    val_races = set(distinct_races[-n_val:])
    val = sorted_frame.filter(pl.col(RACE_ID_COLUMN).is_in(val_races))
    train = sorted_frame.filter(~pl.col(RACE_ID_COLUMN).is_in(val_races))
    return (train, val)


def format_iso_now(now: datetime) -> str:
    return now.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def train_one_fold(
    dataset: pl.DataFrame,
    *,
    cat: str,
    fold_year: int,
    model_version: str,
    num_leaves: int,
    n_estimators: int,
    learning_rate: float,
    min_child_samples: int,
    lambda_l2: float,
    early_stopping_rounds: int,
    random_state: int,
    val_fraction: float,
    regressor_factory: RegressorFactoryLike,
    early_stopping_factory: EarlyStoppingFactoryLike,
    now: datetime,
) -> tuple[pl.DataFrame, dict[str, object]]:
    train_frame, val_frame = filter_to_fold_year(dataset, fold_year)
    if len(train_frame) < MIN_SAMPLES_FOR_TRAINING or val_frame.is_empty():
        skip_reason = (
            "insufficient training rows"
            if len(train_frame) < MIN_SAMPLES_FOR_TRAINING
            else "no OOS rows for fold year"
        )
        meta_skip: dict[str, object] = {
            "category": cat,
            "fold_year": fold_year,
            "model_version": model_version,
            "trained_at": format_iso_now(now),
            "train_size": int(len(train_frame)),
            "val_size": int(len(val_frame)),
            "skipped": True,
            "skip_reason": skip_reason,
        }
        return (pl.DataFrame(), meta_skip)
    inner_train, inner_val = split_train_val(train_frame, val_fraction)
    feat_cols = feature_columns(inner_train)
    x_train = inner_train.select(feat_cols).cast(pl.Float64).to_numpy()
    y_train = (
        inner_train[ACTUAL_FINISH_POSITION_COLUMN]
        .cast(pl.Float64, strict=False)
        .fill_null(99.0)
        .to_numpy()
    )
    x_val = inner_val.select(feat_cols).cast(pl.Float64).to_numpy()
    y_val = (
        inner_val[ACTUAL_FINISH_POSITION_COLUMN]
        .cast(pl.Float64, strict=False)
        .fill_null(99.0)
        .to_numpy()
    )
    model = regressor_factory(
        num_leaves=num_leaves,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        min_child_samples=min_child_samples,
        lambda_l2=lambda_l2,
        random_state=random_state,
    )
    callback = early_stopping_factory(early_stopping_rounds)
    model.fit(
        x_train,
        y_train,
        eval_set=[(x_val, y_val)],
        callbacks=[callback],
    )
    val_sorted = val_frame
    x_oos = val_sorted.select(feat_cols).cast(pl.Float64).to_numpy()
    val_sorted = val_sorted.with_columns(pl.Series("meta_score", model.predict(x_oos)))
    reranked = rerank_within_race(val_sorted, "meta_score")
    reranked = reranked.with_columns(pl.lit(model_version).alias("model_version"))
    oos_metrics = compute_oos_metrics(reranked)
    importance_pairs = [
        (feat_cols[i], int(model.feature_importances_[i])) for i in range(len(feat_cols))
    ]
    importance_sorted = sorted(importance_pairs, key=lambda kv: -kv[1])
    meta: dict[str, object] = {
        "category": cat,
        "fold_year": fold_year,
        "model_version": model_version,
        "trained_at": format_iso_now(now),
        "num_leaves": num_leaves,
        "n_estimators": n_estimators,
        "learning_rate": learning_rate,
        "min_child_samples": min_child_samples,
        "lambda_l2": lambda_l2,
        "early_stopping_rounds": early_stopping_rounds,
        "random_state": random_state,
        "best_iteration": model.best_iteration_,
        "train_size": int(len(inner_train)),
        "val_size_inner": int(len(inner_val)),
        "val_size_oos": int(len(val_sorted)),
        "skipped": False,
        "oos_metrics": oos_metrics,
        "feature_columns": feat_cols,
        "feature_importance_top10": importance_sorted[:10],
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


def write_fold_predictions(
    fold_preds: pl.DataFrame,
    output_predictions_root: Path,
    writer: PartitionedParquetWriterLike,
) -> None:
    if fold_preds.is_empty():
        return
    writer(fold_preds, output_predictions_root)


def write_fold_metadata(
    meta: dict[str, object],
    output_model_dir: Path,
    fold_year: int,
    writer: JsonWriterLike,
) -> None:
    path = output_model_dir / f"fold_{fold_year}.json"
    writer(meta, path)


def fold_already_complete(output_model_dir: Path, fold_year: int) -> bool:
    path = output_model_dir / f"fold_{fold_year}.json"
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return False
    return payload.get("skipped") is False or payload.get("skipped") is True


def load_fold_metadata(output_model_dir: Path, fold_year: int) -> dict[str, object]:
    path = output_model_dir / f"fold_{fold_year}.json"
    return cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))


def run_train(args: TrainArgs, deps: TrainDeps, *, resume: bool = True) -> int:
    cat = args["cat"]
    dataset = deps["dataset_reader"](args["dataset_root"] / f"category={cat}")
    if dataset.is_empty():
        raise ValueError(f"stacking dataset for category={cat} is empty: {args['dataset_root']}")
    if CATEGORY_COLUMN not in dataset.columns:
        dataset = dataset.with_columns(pl.lit(cat).alias(CATEGORY_COLUMN))
    if RACE_YEAR_COLUMN not in dataset.columns:
        raise ValueError(f"stacking dataset is missing {RACE_YEAR_COLUMN!r} column")
    dataset = assemble_feature_frame(dataset)
    fold_years = resolve_fold_years(dataset, args["fold_years"])
    now = deps["now"]()
    all_metas: list[dict[str, object]] = []
    for fold_year in fold_years:
        if resume and fold_already_complete(args["output_model_dir"], fold_year):
            sys.stderr.write(f"[{cat} fold={fold_year}] resume: existing fold_*.json -> skip\n")
            all_metas.append(load_fold_metadata(args["output_model_dir"], fold_year))
            continue
        random_state = args["random_state_base"] + fold_year
        preds, meta = train_one_fold(
            dataset,
            cat=cat,
            fold_year=fold_year,
            model_version=args["model_version"],
            num_leaves=args["num_leaves"],
            n_estimators=args["n_estimators"],
            learning_rate=args["learning_rate"],
            min_child_samples=args["min_child_samples"],
            lambda_l2=args["lambda_l2"],
            early_stopping_rounds=args["early_stopping_rounds"],
            random_state=random_state,
            val_fraction=args["val_fraction"],
            regressor_factory=deps["regressor_factory"],
            early_stopping_factory=deps["early_stopping_factory"],
            now=now,
        )
        write_fold_predictions(preds, args["output_predictions_root"], deps["parquet_writer"])
        write_fold_metadata(meta, args["output_model_dir"], fold_year, deps["json_writer"])
        all_metas.append(meta)
        sys.stderr.write(
            f"[{cat} fold={fold_year}] done skipped={meta.get('skipped')} "
            f"oos={meta.get('oos_metrics')}\n"
        )
    summary_path = args["output_model_dir"] / "metadata.json"
    deps["json_writer"](
        {
            "model_version": args["model_version"],
            "category": cat,
            "fold_years": list(fold_years),
            "params": {
                "num_leaves": args["num_leaves"],
                "n_estimators": args["n_estimators"],
                "learning_rate": args["learning_rate"],
                "min_child_samples": args["min_child_samples"],
                "lambda_l2": args["lambda_l2"],
                "early_stopping_rounds": args["early_stopping_rounds"],
                "random_state_base": args["random_state_base"],
                "val_fraction": args["val_fraction"],
            },
            "fold_results": all_metas,
            "generated_at": format_iso_now(now),
        },
        summary_path,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode != MODE_TRAIN:
        raise ValueError(f"unknown mode: {args.mode}")
    train_args = normalize_train_args(args)
    train_deps: TrainDeps = {
        "dataset_reader": default_read_parquet_dir,
        "parquet_writer": default_write_partitioned_parquet,
        "json_writer": default_write_json,
        "regressor_factory": default_regressor_factory,
        "early_stopping_factory": default_early_stopping_factory,
        "now": now_utc,
    }
    return run_train(train_args, train_deps)


if __name__ == "__main__":
    sys.exit(main())
