"""Iter 3 of the v8 iterative loop: L8 stacking via LightGBM (lambdarank).

Input: Pre-built per-(cat, race_year) parquet datasets carrying both the
v7-lineage baseline columns (``predicted_score`` / ``predicted_rank`` /
``actual_finish_position`` / ``race_id``) and a horse-level feature panel
extracted from PG. Building the dataset is handled by the one-shot helper
``tmp/v8/iter3_build_lgbm_stacking_dataset.py``; here we only consume the
parquet tree.

Output: Walk-forward OOS predictions partitioned by ``category`` /
``race_year`` with a reranked ``predicted_rank``, plus a per-fold
``metadata.json`` capturing model params, feature importance and OOS
metrics. The LGBM ranker is non-linear so unlike iter2's Ridge it can
re-order horses within a race, satisfying the "rank-changing" requirement
that calibration/linear blending lacked.

CLI modes:

* ``--mode train`` — walk-forward train one ranker per fold year.

All I/O is injected via TypedDict deps so the unit tests can stay fully
mocked (no PG, no real parquet writes, no real LGBM training in unit
tests).

Run with::

    uv run python src/scripts/train_finish_position_lgbm_stacking.py \\
        --mode train --cat jra \\
        --dataset-root tmp/v8/iter3-stacking-dataset \\
        --output-predictions-root tmp/bucket-eval/finish-position/iter3-jra-cb+lgbm-stack-v8/predictions \\
        --output-model-dir apps/pc-keiba-viewer/finish-position/jra/v8-iter3-lgbm-stacking \\
        --model-version iter3-jra-cb+lgbm-stack-v8
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, TypedDict, cast

import numpy as np
import pandas as pd

MODE_TRAIN: str = "train"
SUPPORTED_MODES: tuple[str] = (MODE_TRAIN,)

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

# Feature columns expected to be present in the dataset parquet (built by the
# tmp helper). LightGBM accepts NaN natively so we do not drop or impute.
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
# Within-race normalised features computed on the fly from BASE columns.
DERIVED_FEATURE_COLUMNS: tuple[str, ...] = (
    "predicted_rank_norm",
    "score_z_in_race",
    "kohan3f_pct_rank_in_field",
    "finish_pos_avg5_z_in_race",
    "days_since_last_race_z_in_race",
    "bataiju_filler",  # kept for header parity; always 0 (no bataiju in features)
    "wakuban_norm",
)
# Surface/dirt + sex one-hot derived from categorical columns.
SEX_FEATURE_COLUMNS: tuple[str, ...] = (
    "sex_male",
    "sex_female",
    "sex_gelding",
)
SURFACE_FEATURE_COLUMNS: tuple[str, ...] = (
    "surface_dirt_flag",
)
DISTANCE_BAND_LABELS: tuple[str, str, str, str, str] = (
    "sprint",
    "mile",
    "intermediate",
    "long",
    "extended",
)
DISTANCE_BAND_EDGES: tuple[int, int, int, int] = (1200, 1600, 2000, 2400)

TURF_TRACK_CODE_PREFIXES: tuple[str, ...] = (
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22",
)
DIRT_TRACK_CODE_PREFIXES: tuple[str, ...] = (
    "23", "24", "25", "26", "27", "28", "29",
)

# Lambdarank truncation at 5 emphasises top1/place2/place3/top3_box jointly,
# matching the v7.1 "place2/3 重視" judgement.
DEFAULT_NUM_LEAVES: int = 31
DEFAULT_N_ESTIMATORS: int = 500
DEFAULT_LEARNING_RATE: float = 0.05
DEFAULT_MIN_CHILD_SAMPLES: int = 50
DEFAULT_LAMBDA_L2: float = 1.0
DEFAULT_EARLY_STOPPING: int = 50
DEFAULT_LAMBDARANK_TRUNC: int = 5
DEFAULT_RANDOM_STATE_BASE: int = 42
MIN_SAMPLES_FOR_TRAINING: int = 1000
TOP1_FINISH: int = 1
TOP3_FINISH: int = 3
# Validation holdout: last 10% of training year-block reserved as early-stop signal.
DEFAULT_VAL_FRACTION: float = 0.10


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
    lambdarank_truncation_level: int
    random_state_base: int
    fold_years: tuple[int, ...] | None
    val_fraction: float


class ParquetDirReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class PartitionedParquetWriterLike(Protocol):
    def __call__(self, frame: pd.DataFrame, output_dir: Path) -> None: ...


class JsonWriterLike(Protocol):
    def __call__(self, payload: dict[str, object], path: Path) -> None: ...


class LightGBMRankerLike(Protocol):
    """Protocol matching the LGBMRanker surface we rely on.

    Test fakes implement just ``fit`` / ``predict`` / ``feature_importances_``
    / ``best_iteration_`` so we never need a real LightGBM install during
    unit tests.
    """

    feature_importances_: np.ndarray
    best_iteration_: int | None

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        *,
        group: np.ndarray,
        eval_set: list[tuple[np.ndarray, np.ndarray]] | None,
        eval_group: list[np.ndarray] | None,
        callbacks: list[object] | None,
    ) -> None: ...

    def predict(self, X: np.ndarray) -> np.ndarray: ...


class RankerFactoryLike(Protocol):
    def __call__(
        self,
        *,
        num_leaves: int,
        n_estimators: int,
        learning_rate: float,
        min_child_samples: int,
        lambda_l2: float,
        lambdarank_truncation_level: int,
        random_state: int,
    ) -> LightGBMRankerLike: ...


class EarlyStoppingFactoryLike(Protocol):
    def __call__(self, stopping_rounds: int) -> object: ...


class NowFactoryLike(Protocol):
    def __call__(self) -> datetime: ...


class TrainDeps(TypedDict):
    dataset_reader: ParquetDirReaderLike
    parquet_writer: PartitionedParquetWriterLike
    json_writer: JsonWriterLike
    ranker_factory: RankerFactoryLike
    early_stopping_factory: EarlyStoppingFactoryLike
    now: NowFactoryLike


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="train_finish_position_lgbm_stacking")
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
    parser.add_argument(
        "--early-stopping-rounds", type=int, default=DEFAULT_EARLY_STOPPING
    )
    parser.add_argument(
        "--lambdarank-truncation-level",
        type=int,
        default=DEFAULT_LAMBDARANK_TRUNC,
    )
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
        "lambdarank_truncation_level": int(args.lambdarank_truncation_level),
        "random_state_base": int(args.random_state_base),
        "fold_years": fold_years,
        "val_fraction": val_fraction,
    }


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def default_read_parquet_dir(path: Path) -> pd.DataFrame:
    if path.is_file():
        return pd.read_parquet(path.as_posix())
    parts = sorted(path.rglob("*.parquet"))
    frames: list[pd.DataFrame] = []
    for part in parts:
        frame = pd.read_parquet(part.as_posix())
        for segment in part.relative_to(path).parts[:-1]:
            if "=" not in segment:
                continue
            key, raw_value = segment.split("=", 1)
            frame[key] = coerce_partition_value(key, raw_value)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def coerce_partition_value(key: str, raw_value: str) -> int | str:
    if key == RACE_YEAR_COLUMN:
        try:
            return int(raw_value)
        except ValueError:
            return raw_value
    return raw_value


def default_write_partitioned_parquet(frame: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(
        output_dir.as_posix(),
        partition_cols=[CATEGORY_COLUMN, RACE_YEAR_COLUMN],
        index=False,
    )


def default_write_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def default_ranker_factory(
    *,
    num_leaves: int,
    n_estimators: int,
    learning_rate: float,
    min_child_samples: int,
    lambda_l2: float,
    lambdarank_truncation_level: int,
    random_state: int,
) -> LightGBMRankerLike:
    # Imported lazily so unit tests don't need lightgbm available.
    import lightgbm as lgb

    ranker = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        n_estimators=n_estimators,
        num_leaves=num_leaves,
        learning_rate=learning_rate,
        min_child_samples=min_child_samples,
        reg_lambda=lambda_l2,
        lambdarank_truncation_level=lambdarank_truncation_level,
        random_state=random_state,
        n_jobs=8,
        verbose=-1,
    )
    return cast(LightGBMRankerLike, ranker)


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


def encode_distance_band_one_hot(bands: pd.Series) -> pd.DataFrame:
    columns = {f"distance_band_{label}": (bands == label).astype(int) for label in DISTANCE_BAND_LABELS}
    return pd.DataFrame(columns, index=bands.index)


def encode_sex_one_hot(seibetsu: pd.Series) -> pd.DataFrame:
    """Seibetsu code: 1=male, 2=female, 3=gelding (others -> all-zero)."""
    text = seibetsu.astype(str).str.strip()
    return pd.DataFrame(
        {
            "sex_male": (text == "1").astype(int),
            "sex_female": (text == "2").astype(int),
            "sex_gelding": (text == "3").astype(int),
        },
        index=seibetsu.index,
    )


def add_within_race_normalised(frame: pd.DataFrame) -> pd.DataFrame:
    """Append derived per-race normalised columns: rank_norm / score_z /
    kohan3f pct rank / finish_pos_avg5 z / days_since_last z / wakuban norm.
    """
    out = frame.copy()
    grp_score = out.groupby(RACE_ID_COLUMN)[PREDICTED_SCORE_COLUMN]
    grp_rank = out.groupby(RACE_ID_COLUMN)[PREDICTED_RANK_COLUMN]
    field_size = grp_rank.transform("count").astype(float)
    out["predicted_rank_norm"] = out[PREDICTED_RANK_COLUMN].astype(float) / field_size.replace(0.0, 1.0)
    score_mean = grp_score.transform("mean")
    score_std = grp_score.transform("std").fillna(0.0).replace(0.0, 1e-6)
    out["score_z_in_race"] = (out[PREDICTED_SCORE_COLUMN].astype(float) - score_mean) / score_std
    out["kohan3f_pct_rank_in_field"] = out.groupby(RACE_ID_COLUMN)["horse_recent_kohan3f_avg5"].rank(
        pct=True, ascending=True
    )
    fpa_mean = out.groupby(RACE_ID_COLUMN)["horse_recent_finish_position_avg5"].transform("mean")
    fpa_std = (
        out.groupby(RACE_ID_COLUMN)["horse_recent_finish_position_avg5"].transform("std").fillna(0.0).replace(0.0, 1e-6)
    )
    out["finish_pos_avg5_z_in_race"] = (
        out["horse_recent_finish_position_avg5"].astype(float) - fpa_mean
    ) / fpa_std
    dlr_mean = out.groupby(RACE_ID_COLUMN)["days_since_last_race"].transform("mean")
    dlr_std = (
        out.groupby(RACE_ID_COLUMN)["days_since_last_race"].transform("std").fillna(0.0).replace(0.0, 1e-6)
    )
    out["days_since_last_race_z_in_race"] = (
        out["days_since_last_race"].astype(float) - dlr_mean
    ) / dlr_std
    out["bataiju_filler"] = 0.0
    # wakuban approximation from umaban + shusso_tosu (waku = ceil(umaban*8/shusso))
    umaban = pd.to_numeric(out["umaban"], errors="coerce")
    shusso = pd.to_numeric(out["shusso_tosu"], errors="coerce").replace(0, np.nan)
    waku = np.ceil((umaban * 8.0) / shusso)
    out["wakuban_norm"] = np.clip(waku, 1.0, 8.0) / 8.0
    return out


def add_categorical_one_hots(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    bands = out["kyori"].map(assign_distance_band)
    one_hot = encode_distance_band_one_hot(bands)
    surface = out["track_code"].map(assign_surface_dirt_flag)
    sex = encode_sex_one_hot(out["seibetsu_code"])
    out["surface_dirt_flag"] = surface.astype(int)
    return pd.concat([out, one_hot, sex], axis=1)


def assemble_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply all derived feature transforms and return a wide frame."""
    out = add_within_race_normalised(frame)
    out = add_categorical_one_hots(out)
    return out


def feature_columns(frame: pd.DataFrame) -> list[str]:
    """Return the ordered feature column list used by the ranker.

    Columns that are missing from ``frame`` are silently skipped so a slim
    test fixture can still exercise the training loop.
    """
    band_cols = [f"distance_band_{label}" for label in DISTANCE_BAND_LABELS]
    candidate = (
        list(BASE_FEATURE_COLUMNS)
        + list(DERIVED_FEATURE_COLUMNS)
        + list(SURFACE_FEATURE_COLUMNS)
        + band_cols
        + list(SEX_FEATURE_COLUMNS)
    )
    return [c for c in candidate if c in frame.columns]


def race_group_sizes(frame: pd.DataFrame) -> np.ndarray:
    """Group sizes for LGBM ranker, in dataframe row order.

    The frame **must** be pre-sorted so all rows of the same ``race_id`` are
    contiguous; ``train_one_fold`` ensures this.
    """
    counts = frame.groupby(RACE_ID_COLUMN, sort=False)[RACE_ID_COLUMN].size()
    return counts.to_numpy(dtype=np.int64)


def relevance_from_finish(actuals: pd.Series, truncation_level: int) -> np.ndarray:
    """Convert ``actual_finish_position`` to integer relevance.

    Relevance = truncation_level + 1 - finish for top-(truncation_level)
    finishes, 0 otherwise. Highest relevance goes to the winner so
    lambdarank pushes them to predicted_rank=1.
    """
    actual = pd.to_numeric(actuals, errors="coerce").fillna(99).astype(int).to_numpy()
    rel = truncation_level + 1 - actual
    rel = np.clip(rel, 0, truncation_level)
    return rel.astype(np.int64)


def rerank_within_race(frame: pd.DataFrame, score_column: str) -> pd.DataFrame:
    """Rerank rows by ``score_column`` (descending = better) within each race.

    LGBM ranker scores are "higher = more relevant", so the row with the
    largest stacking score becomes predicted_rank=1.
    """
    out = frame.copy()
    ranks = out.groupby(RACE_ID_COLUMN)[score_column].rank(method="first", ascending=False)
    out[PREDICTED_RANK_COLUMN] = ranks.astype(int)
    return out


def compute_oos_metrics(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {"races": 0, "top1": 0.0, "place2": 0.0, "place3": 0.0, "top3_box": 0.0}
    rank_col = frame[PREDICTED_RANK_COLUMN].astype(int)
    actual_col = frame[ACTUAL_FINISH_POSITION_COLUMN].astype(int)
    top1_hit = ((rank_col == TOP1_FINISH) & (actual_col == TOP1_FINISH)).astype(int)
    place2_hit = ((rank_col == 2) & (actual_col == 2)).astype(int)
    place3_hit = ((rank_col == TOP3_FINISH) & (actual_col == TOP3_FINISH)).astype(int)
    box_match = ((rank_col <= TOP3_FINISH) & (actual_col <= TOP3_FINISH)).astype(int)
    top1_per_race = top1_hit.groupby(frame[RACE_ID_COLUMN]).max()
    place2_per_race = place2_hit.groupby(frame[RACE_ID_COLUMN]).max()
    place3_per_race = place3_hit.groupby(frame[RACE_ID_COLUMN]).max()
    box_per_race = (box_match.groupby(frame[RACE_ID_COLUMN]).sum() == TOP3_FINISH).astype(int)
    races = int(len(top1_per_race))
    return {
        "races": races,
        "top1": float(top1_per_race.mean()),
        "place2": float(place2_per_race.mean()),
        "place3": float(place3_per_race.mean()),
        "top3_box": float(box_per_race.mean()),
    }


def filter_to_fold_year(dataset: pd.DataFrame, fold_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_mask = dataset[RACE_YEAR_COLUMN] < fold_year
    val_mask = dataset[RACE_YEAR_COLUMN] == fold_year
    return (dataset.loc[train_mask].copy(), dataset.loc[val_mask].copy())


def split_train_val(
    train_frame: pd.DataFrame, val_fraction: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split training frame into inner train + early-stop val by trailing race years.

    We hold out the most recent ``val_fraction`` of races (by race_year, then
    race_id ordering) to keep the time-ordering tight against the OOS year.
    """
    sorted_frame = train_frame.sort_values([RACE_YEAR_COLUMN, RACE_ID_COLUMN], kind="stable")
    distinct_races = sorted_frame[RACE_ID_COLUMN].drop_duplicates().tolist()
    n_val = max(1, int(round(len(distinct_races) * val_fraction)))
    if n_val >= len(distinct_races):
        n_val = len(distinct_races) - 1
    val_races = set(distinct_races[-n_val:])
    val = sorted_frame[sorted_frame[RACE_ID_COLUMN].isin(val_races)].copy()
    train = sorted_frame[~sorted_frame[RACE_ID_COLUMN].isin(val_races)].copy()
    return (train, val)


def format_iso_now(now: datetime) -> str:
    return now.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def train_one_fold(
    dataset: pd.DataFrame,
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
    lambdarank_truncation_level: int,
    random_state: int,
    val_fraction: float,
    ranker_factory: RankerFactoryLike,
    early_stopping_factory: EarlyStoppingFactoryLike,
    now: datetime,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Train one walk-forward fold and produce OOS reranked predictions."""
    train_frame, val_frame = filter_to_fold_year(dataset, fold_year)
    if len(train_frame) < MIN_SAMPLES_FOR_TRAINING or val_frame.empty:
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
        return (pd.DataFrame(), meta_skip)
    inner_train, inner_val = split_train_val(train_frame, val_fraction)
    inner_train = inner_train.sort_values([RACE_YEAR_COLUMN, RACE_ID_COLUMN], kind="stable")
    inner_val = inner_val.sort_values([RACE_YEAR_COLUMN, RACE_ID_COLUMN], kind="stable")
    val_frame_sorted = val_frame.sort_values([RACE_YEAR_COLUMN, RACE_ID_COLUMN], kind="stable")
    feat_cols = feature_columns(inner_train)
    x_train = inner_train[feat_cols].to_numpy(dtype=float)
    y_train = relevance_from_finish(
        inner_train[ACTUAL_FINISH_POSITION_COLUMN], lambdarank_truncation_level
    )
    g_train = race_group_sizes(inner_train)
    x_inner_val = inner_val[feat_cols].to_numpy(dtype=float)
    y_inner_val = relevance_from_finish(
        inner_val[ACTUAL_FINISH_POSITION_COLUMN], lambdarank_truncation_level
    )
    g_inner_val = race_group_sizes(inner_val)
    model = ranker_factory(
        num_leaves=num_leaves,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        min_child_samples=min_child_samples,
        lambda_l2=lambda_l2,
        lambdarank_truncation_level=lambdarank_truncation_level,
        random_state=random_state,
    )
    callback = early_stopping_factory(early_stopping_rounds)
    model.fit(
        x_train,
        y_train,
        group=g_train,
        eval_set=[(x_inner_val, y_inner_val)],
        eval_group=[g_inner_val],
        callbacks=[callback],
    )
    x_oos = val_frame_sorted[feat_cols].to_numpy(dtype=float)
    val_frame_sorted = val_frame_sorted.copy()
    val_frame_sorted["stacking_score"] = model.predict(x_oos)
    reranked = rerank_within_race(val_frame_sorted, "stacking_score")
    reranked["model_version"] = model_version
    oos_metrics = compute_oos_metrics(reranked)
    importance_pairs = [
        (feat_cols[i], int(model.feature_importances_[i]))
        for i in range(len(feat_cols))
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
        "lambdarank_truncation_level": lambdarank_truncation_level,
        "random_state": random_state,
        "best_iteration": model.best_iteration_,
        "train_size": int(len(inner_train)),
        "val_size_inner": int(len(inner_val)),
        "val_size_oos": int(len(val_frame_sorted)),
        "skipped": False,
        "oos_metrics": oos_metrics,
        "feature_columns": feat_cols,
        "feature_importance_top10": importance_sorted[:10],
    }
    return (reranked, meta)


def resolve_fold_years(
    dataset: pd.DataFrame, requested: tuple[int, ...] | None
) -> tuple[int, ...]:
    available = sorted(int(y) for y in dataset[RACE_YEAR_COLUMN].unique().tolist())
    if requested is None:
        return tuple(available[1:]) if len(available) > 1 else tuple(available)
    keep = tuple(year for year in requested if year in set(available))
    if not keep:
        raise ValueError(f"none of the requested fold years {requested!r} exist in the dataset")
    return keep


def write_fold_predictions(
    fold_preds: pd.DataFrame, output_predictions_root: Path, writer: PartitionedParquetWriterLike
) -> None:
    """Write a single fold's predictions partitioned by category / race_year."""
    if fold_preds.empty:
        return
    writer(fold_preds, output_predictions_root)


def write_fold_metadata(
    meta: dict[str, object], output_model_dir: Path, fold_year: int, writer: JsonWriterLike
) -> None:
    """Atomic per-fold metadata write so we can resume across crashes."""
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
    if dataset.empty:
        raise ValueError(f"stacking dataset for category={cat} is empty: {args['dataset_root']}")
    if CATEGORY_COLUMN not in dataset.columns:
        dataset[CATEGORY_COLUMN] = cat
    if RACE_YEAR_COLUMN not in dataset.columns:
        raise ValueError(f"stacking dataset is missing {RACE_YEAR_COLUMN!r} column")
    dataset = assemble_feature_frame(dataset)
    fold_years = resolve_fold_years(dataset, args["fold_years"])
    now = deps["now"]()
    all_metas: list[dict[str, object]] = []
    for fold_year in fold_years:
        if resume and fold_already_complete(args["output_model_dir"], fold_year):
            sys.stderr.write(f"[{cat} fold={fold_year}] resume: existing metadata.json -> skip\n")
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
            lambdarank_truncation_level=args["lambdarank_truncation_level"],
            random_state=random_state,
            val_fraction=args["val_fraction"],
            ranker_factory=deps["ranker_factory"],
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
                "lambdarank_truncation_level": args["lambdarank_truncation_level"],
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
        "ranker_factory": default_ranker_factory,
        "early_stopping_factory": default_early_stopping_factory,
        "now": now_utc,
    }
    return run_train(train_args, train_deps)


if __name__ == "__main__":
    sys.exit(main())
