"""Continuous self-improving Walk-Forward learning loop with opt-in auto-deploy."""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Final, TypedDict, cast, get_args

if TYPE_CHECKING:
    import psycopg

import polars as pl

from learning.feature_explorer import (
    CATEGORY_BACKENDS,
    DEFAULT_BACKENDS,
    DEFAULT_PARAMS,
    DEFAULT_TRAIN_START,
    DEFAULT_VALIDATION_YEARS,
    DEFAULT_VALIDATION_YEARS_PER_ROUND,
    VALIDATION_YEAR_POOL,
    ModelBackend,
    evaluate_feature_set,
    predict_fold_with_backend,
    run_exploration,
    select_fold_features,
    select_round_validation_years,
)
from learning.feature_registry import INVERSE_APPROACH_TYPES, FeatureEntry, FeatureRegistry
from learning.feature_selection_policy import (
    compute_feature_set_hash as compute_normalized_feature_set_hash,
)
from learning.subgroup_diagnostics import SubgroupMetrics, compute_subgroup_diagnostics
from finish_position_lightgbm import (
    LABEL_COLUMNS,
    META_COLUMNS,
    FoldSplit,
    resolve_feature_columns,
    split_walk_forward,
)
from walk_forward_common import atomic_write_metadata

_psutil: ModuleType | None
try:
    import psutil

    _psutil = psutil
except ImportError:
    _psutil = None

_logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure the root logger to write INFO-level logs to stdout with ISO timestamps.

    No-ops when handlers are already registered (e.g. pytest captures).
    """
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.setLevel(logging.INFO)
    root.addHandler(handler)

_LABEL_COLS: Final[frozenset[str]] = frozenset(LABEL_COLUMNS)

_TRAINING_SCRIPT: Final[dict[str, str]] = {
    "jra": "train_finish_position_catboost_walk_forward.py",
    "nar": "train_finish_position_xgboost_walk_forward.py",
    "ban-ei": "train_finish_position_catboost_walk_forward.py",
}

_CATEGORY_TRAIN_START: Final[dict[str, str]] = {
    "jra": "20130101",
    "nar": "20060101",
    "ban-ei": "20110101",
}

DEFAULT_DOCKER_TAG: Final[str] = "finish-position-predict-local:split2"
DEFAULT_DEPLOY_THRESHOLD: Final[float] = 0.005
DEFAULT_N_TRIALS: Final[int] = 20
DEFAULT_DOCKER_BUILD_TIMEOUT_S: Final[int] = 3600
DEFAULT_TRAINING_TIMEOUT_S: Final[int] = 7200
STRONG_NEGATIVE_THRESHOLD_PP: Final[float] = -1.0
MAX_INVERSE_PER_ROUND: Final[int] = 3
INVERSE_N_TRIALS: Final[int] = 2
ENRICHMENT_THRESHOLD: Final[float] = 0.3
ENRICHMENT_N_TRIALS: Final[int] = 2
MAX_ENRICHMENT_FEATURES: Final[int] = 5
SATURATION_LOOKBACK: Final[int] = 50
_SATURATED_TRIAL_DIVISOR: Final[int] = 2
_MIN_SATURATED_TRIALS: Final[int] = 5

_MIN_NTHREAD: Final[int] = 2
_MAX_NTHREAD: Final[int] = 6
_MIN_FREE_MEM_GB: Final[float] = 8.0


class CellFilter(TypedDict, total=False):
    keibajo_codes: list[str]
    season_bands: list[str]


class InverseResult(TypedDict):
    delta_pp: dict[str, float]
    decision: str

_CONTAINER_MODELS_ROOT: Final[str] = (
    "apps/finish-position-predict-container/models/finish-position"
)
_MODEL_META_JSON_PATH: Final[str] = (
    "apps/finish-position-predict-container/src/predict_lib/model_meta.json"
)
_CONTAINER_APP_DIR: Final[str] = "apps/finish-position-predict-container"
DEFAULT_CF_DEPLOY_TIMEOUT_S: Final[int] = 300

_LOCAL_PG_URL: Final[str] = (
    "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
)

_CELL_EVAL_TABLE: Final[str] = "cell_training_evaluations"

_CELL_EVAL_DDL = """
CREATE TABLE IF NOT EXISTS cell_training_evaluations (
    prediction_target  TEXT NOT NULL DEFAULT 'finish_position',
    feature_set_hash    TEXT NOT NULL,
    category            TEXT NOT NULL,
    surface             TEXT NOT NULL,
    distance_band       TEXT NOT NULL,
    class_label         TEXT NOT NULL,
    season              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    feature_count       INTEGER NOT NULL,
    race_count          INTEGER NOT NULL,
    ndcg_at_3           DOUBLE PRECISION NOT NULL,
    top1_accuracy       DOUBLE PRECISION NOT NULL,
    place2_accuracy     DOUBLE PRECISION NOT NULL,
    place3_accuracy     DOUBLE PRECISION NOT NULL,
    place4_accuracy     DOUBLE PRECISION NOT NULL,
    place5_accuracy     DOUBLE PRECISION NOT NULL,
    place6_accuracy     DOUBLE PRECISION NOT NULL,
    top3_box_accuracy   DOUBLE PRECISION NOT NULL,
    accuracy_vector     DOUBLE PRECISION[] NOT NULL,
    feature_names_array TEXT[] NOT NULL,
    cell_vector         TEXT[] NOT NULL,
    evaluated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue)
)
"""

_CELL_EVAL_MIGRATION = """
ALTER TABLE cell_training_evaluations
ADD COLUMN IF NOT EXISTS prediction_target TEXT NOT NULL DEFAULT 'finish_position';

DO $$
DECLARE
    pk_cols TEXT[];
BEGIN
    SELECT array_agg(a.attname ORDER BY u.ordinality)
    INTO pk_cols
    FROM pg_constraint c
    JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, ordinality) ON TRUE
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
    WHERE c.conrelid = 'cell_training_evaluations'::regclass
      AND c.contype = 'p';

    IF pk_cols = ARRAY[
        'feature_set_hash', 'category', 'surface', 'distance_band',
        'class_label', 'season', 'venue'
    ] THEN
        ALTER TABLE cell_training_evaluations
        DROP CONSTRAINT cell_training_evaluations_pkey;

        ALTER TABLE cell_training_evaluations
        ADD PRIMARY KEY (
            prediction_target, feature_set_hash, category, surface,
            distance_band, class_label, season, venue
        );
    END IF;
END $$;
"""

_CELL_EVAL_UPSERT = """
INSERT INTO cell_training_evaluations (
    prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue,
    feature_count, race_count, ndcg_at_3,
    top1_accuracy, place2_accuracy, place3_accuracy,
    place4_accuracy, place5_accuracy, place6_accuracy,
    top3_box_accuracy,
    accuracy_vector, feature_names_array, cell_vector
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue)
DO UPDATE SET
    feature_count = EXCLUDED.feature_count,
    race_count = EXCLUDED.race_count,
    ndcg_at_3 = EXCLUDED.ndcg_at_3,
    top1_accuracy = EXCLUDED.top1_accuracy,
    place2_accuracy = EXCLUDED.place2_accuracy,
    place3_accuracy = EXCLUDED.place3_accuracy,
    place4_accuracy = EXCLUDED.place4_accuracy,
    place5_accuracy = EXCLUDED.place5_accuracy,
    place6_accuracy = EXCLUDED.place6_accuracy,
    top3_box_accuracy = EXCLUDED.top3_box_accuracy,
    accuracy_vector = EXCLUDED.accuracy_vector,
    feature_names_array = EXCLUDED.feature_names_array,
    cell_vector = EXCLUDED.cell_vector,
    evaluated_at = NOW()
"""

_CELL_EVAL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_cell_eval_category_season
    ON cell_training_evaluations (prediction_target, category, season);
CREATE INDEX IF NOT EXISTS idx_cell_eval_category_venue
    ON cell_training_evaluations (prediction_target, category, venue);
CREATE INDEX IF NOT EXISTS idx_cell_eval_feature_hash
    ON cell_training_evaluations (prediction_target, feature_set_hash);
CREATE INDEX IF NOT EXISTS idx_cell_eval_category_season_venue
    ON cell_training_evaluations (prediction_target, category, season, venue);
CREATE INDEX IF NOT EXISTS idx_cell_eval_top1
    ON cell_training_evaluations (prediction_target, category, top1_accuracy DESC);
"""

_TRIAL_LOG_TABLE: Final[str] = "trial_exploration_log"

_TRIAL_LOG_DDL = """
CREATE TABLE IF NOT EXISTS trial_exploration_log (
    trial_id            TEXT NOT NULL,
    feature_set_hash    TEXT NOT NULL,
    category            TEXT NOT NULL,
    method              TEXT NOT NULL,
    ndcg_at_3           DOUBLE PRECISION NOT NULL,
    feature_count       INTEGER NOT NULL,
    feature_names_array TEXT[] NOT NULL,
    feature_mask_vector BOOLEAN[] NOT NULL,
    importance_vector   DOUBLE PRECISION[],
    params_json         JSONB,
    explored_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feature_set_hash, category, method)
)
"""

_TRIAL_LOG_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_trial_log_category_method
    ON trial_exploration_log (category, method);
CREATE INDEX IF NOT EXISTS idx_trial_log_ndcg
    ON trial_exploration_log (category, ndcg_at_3 DESC);
CREATE INDEX IF NOT EXISTS idx_trial_log_feature_hash
    ON trial_exploration_log (feature_set_hash);
"""

_TRIAL_LOG_UPSERT = """
INSERT INTO trial_exploration_log (
    trial_id, feature_set_hash, category, method, ndcg_at_3,
    feature_count, feature_names_array, feature_mask_vector,
    importance_vector, params_json
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
)
ON CONFLICT (feature_set_hash, category, method)
DO UPDATE SET
    trial_id = EXCLUDED.trial_id,
    ndcg_at_3 = EXCLUDED.ndcg_at_3,
    feature_count = EXCLUDED.feature_count,
    feature_names_array = EXCLUDED.feature_names_array,
    feature_mask_vector = EXCLUDED.feature_mask_vector,
    importance_vector = EXCLUDED.importance_vector,
    params_json = EXCLUDED.params_json,
    explored_at = NOW()
"""

_SEASON_MONTHS: Final[dict[str, set[int]]] = {
    "spring": {3, 4, 5},
    "summer": {6, 7, 8},
    "autumn": {9, 10, 11},
    "winter": {12, 1, 2},
}


def _load_partitioned_features(glob_pattern: str, min_year: int) -> pl.DataFrame:
    """Load Hive-partitioned parquet, tolerating per-year column dtype drift.

    The fast path is a single lazy ``scan_parquet`` over the glob so the ``race_year``
    predicate is pushed into Hive partition pruning. Year partitions written at
    different times can disagree on a column's dtype (e.g. ``umaban`` is ``Int32`` in
    some years and ``Float64`` in others), which makes that unified scan raise
    ``SchemaError``. On that error each file is scanned independently and concatenated
    with ``diagonal_relaxed`` so mismatched numeric columns are promoted to a common
    supertype; pruning and dtype promotion are both preserved.
    """
    predicate = pl.col("race_year") >= min_year
    try:
        return (
            pl.scan_parquet(glob_pattern, hive_partitioning=True).filter(predicate).collect()
        )
    except pl.exceptions.SchemaError:
        files = sorted(glob.glob(glob_pattern, recursive=True))
        scans = [pl.scan_parquet(f, hive_partitioning=True) for f in files]
        return pl.concat(scans, how="diagonal_relaxed").filter(predicate).collect()


def _load_features_dataframe(parquet_path: Path, train_start: str) -> pl.DataFrame:
    """Read features, pruning Hive-partitioned year dirs older than the train window.

    A directory path is treated as a ``race_year=YYYY/`` partitioned dataset and only
    partitions at or after ``train_start`` (minus one year of warm-up history) are
    read, which avoids loading decades of unused rows. A single file is read whole.
    """
    if parquet_path.is_dir():
        min_year = int(train_start[:4]) - 1
        df = _load_partitioned_features(
            str(parquet_path / "**" / "*.parquet"), min_year
        )
    else:
        df = pl.read_parquet(str(parquet_path))
    # CatBoost is called with presorted=True, which trusts rows to be grouped
    # contiguously by race_id; a single global sort here honours that contract.
    return df.sort(["race_id", "umaban"])


def filter_dataframe_by_cell(df: pl.DataFrame, cell_filter: CellFilter) -> pl.DataFrame:
    filtered = df
    keibajo_codes = cell_filter.get("keibajo_codes")
    if keibajo_codes:
        filtered = filtered.filter(pl.col("keibajo_code").cast(pl.Utf8).is_in(keibajo_codes))
    season_bands = cell_filter.get("season_bands")
    if season_bands:
        allowed_months: set[int] = set()
        for band in season_bands:
            allowed_months |= _SEASON_MONTHS.get(band, set())
        if allowed_months and "kaisai_tsukihi" in filtered.columns:
            filtered = filtered.filter(
                pl.col("kaisai_tsukihi")
                .cast(pl.Utf8)
                .str.slice(0, 2)
                .cast(pl.Int32)
                .is_in(sorted(allowed_months))
            )
    return filtered


def compute_sire_venue_bias_features(
    df: pl.DataFrame, pg_url: str = _LOCAL_PG_URL
) -> pl.DataFrame:
    psycopg = __import__("psycopg")
    horse_ids = df["ketto_toroku_bango"].unique().to_list()

    sire_map: dict[str, str] = {}
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            batch_size = 5000
            for i in range(0, len(horse_ids), batch_size):
                batch = horse_ids[i : i + batch_size]
                placeholders = ",".join(["%s"] * len(batch))
                cur.execute(
                    f"SELECT ketto_toroku_bango, ketto_joho_01a FROM jvd_um "
                    f"WHERE ketto_toroku_bango IN ({placeholders})",
                    batch,
                )
                for row in cur.fetchall():
                    if row[1] and row[1].strip():
                        sire_map[row[0]] = row[1].strip()

    _logger.info("sire venue bias: loaded %d sire mappings from jvd_um", len(sire_map))

    sire_lookup = pl.DataFrame(
        {
            "ketto_toroku_bango": list(sire_map.keys()),
            "_sire_id": list(sire_map.values()),
        },
        schema={"ketto_toroku_bango": pl.Utf8, "_sire_id": pl.Utf8},
    )
    df = df.join(sire_lookup, on="ketto_toroku_bango", how="left")
    df = df.with_columns(pl.col("_sire_id").fill_null(""))

    surface_expr = (
        pl.when(pl.col("track_code").cast(pl.Utf8).str.starts_with("1"))
        .then(pl.lit("turf"))
        .when(pl.col("track_code").cast(pl.Utf8).str.starts_with("2"))
        .then(pl.lit("dirt"))
        .otherwise(pl.lit("other"))
    )
    df = df.with_columns(surface_expr.alias("_surface_type"))

    is_win = (pl.col("finish_position") == 1).cast(pl.Int32)
    is_place = (pl.col("finish_position") <= 3).cast(pl.Int32)

    df = df.with_columns([
        is_win.alias("_is_win"),
        is_place.alias("_is_place"),
        pl.lit(1).alias("_one"),
    ])

    df = df.sort("race_year", "race_id", "finish_position")

    svsd_group = ["_sire_id", "keibajo_code", "_surface_type", "kyori"]
    df = df.with_columns([
        pl.col("_is_win")
        .shift(1)
        .cum_sum()
        .over(svsd_group)
        .truediv(
            pl.col("_one").cum_sum().over(svsd_group) - 1
        )
        .alias("sire_venue_surface_dist_win_rate"),

        pl.col("_is_place")
        .shift(1)
        .cum_sum()
        .over(svsd_group)
        .truediv(
            pl.col("_one").cum_sum().over(svsd_group) - 1
        )
        .alias("sire_venue_surface_dist_place_rate"),

        (pl.col("_one").cum_sum().over(svsd_group) - 1).alias("sire_venue_surface_dist_runs"),
    ])

    svs_group = ["_sire_id", "keibajo_code", "_surface_type"]
    df = df.with_columns([
        pl.col("_is_win")
        .shift(1)
        .cum_sum()
        .over(svs_group)
        .truediv(
            pl.col("_one").cum_sum().over(svs_group) - 1
        )
        .alias("sire_venue_surface_win_rate"),

        pl.col("_is_place")
        .shift(1)
        .cum_sum()
        .over(svs_group)
        .truediv(
            pl.col("_one").cum_sum().over(svs_group) - 1
        )
        .alias("sire_venue_surface_place_rate"),
    ])

    df = df.drop(["_sire_id", "_surface_type", "_is_win", "_is_place", "_one"])

    _logger.info(
        "sire venue bias: added 5 features, non-null rates: dist=%.1f%% surface=%.1f%%",
        (1 - df["sire_venue_surface_dist_win_rate"].null_count() / len(df)) * 100,
        (1 - df["sire_venue_surface_win_rate"].null_count() / len(df)) * 100,
    )

    return df


def compute_feature_set_hash(feature_names: list[str]) -> str:
    return compute_normalized_feature_set_hash(feature_names)


class CellAccuracyStore:
    def __init__(self, pg_url: str = _LOCAL_PG_URL) -> None:
        self._pg_url: str = pg_url
        self._con: psycopg.Connection[tuple[object, ...]] | None = None

    def open(self) -> None:
        _psycopg = __import__("psycopg")
        con = _psycopg.connect(self._pg_url)
        with con.cursor() as cur:
            cur.execute(_CELL_EVAL_DDL)
            cur.execute(_CELL_EVAL_MIGRATION)
            cur.execute(_CELL_EVAL_INDEXES)
        con.commit()
        self._con = con

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> "CellAccuracyStore":
        self.open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def evaluated_cells(
        self,
        feature_set_hash: str,
        prediction_target: str = "finish_position",
    ) -> set[str]:
        assert self._con is not None
        with self._con.cursor() as cur:
            cur.execute(
                "SELECT category, surface, distance_band, class_label, season, venue "
                "FROM cell_training_evaluations "
                "WHERE prediction_target = %s AND feature_set_hash = %s",
                (prediction_target, feature_set_hash),
            )
            return {
                f"{row[0]}_{row[1]}_{row[2]}_{row[3]}_{row[4]}_{row[5]}"
                for row in cur.fetchall()
            }

    def save_cell_metrics(
        self,
        feature_set_hash: str,
        feature_count: int,
        metrics: list[SubgroupMetrics],
        feature_names: list[str] | None = None,
        prediction_target: str = "finish_position",
    ) -> int:
        assert self._con is not None
        sorted_names = sorted(feature_names) if feature_names is not None else []
        saved = 0
        with self._con.cursor() as cur:
            for m in metrics:
                accuracy_vector = [
                    m["top1_accuracy"],
                    m["place2_accuracy"],
                    m["place3_accuracy"],
                    m["place4_accuracy"],
                    m["place5_accuracy"],
                    m["place6_accuracy"],
                ]
                cell_vector = [
                    m["category"],
                    m["surface"],
                    m["distance_band"],
                    m["class_label"],
                    m["season"],
                    m["venue"],
                ]
                cur.execute(
                    _CELL_EVAL_UPSERT,
                    (
                        prediction_target,
                        feature_set_hash,
                        m["category"],
                        m["surface"],
                        m["distance_band"],
                        m["class_label"],
                        m["season"],
                        m["venue"],
                        feature_count,
                        m["race_count"],
                        m["ndcg_at_3"],
                        m["top1_accuracy"],
                        m["place2_accuracy"],
                        m["place3_accuracy"],
                        m["place4_accuracy"],
                        m["place5_accuracy"],
                        m["place6_accuracy"],
                        m["top3_box_accuracy"],
                        accuracy_vector,
                        sorted_names,
                        cell_vector,
                    ),
                )
                saved += 1
        self._con.commit()
        return saved


class TrialExplorationStore:
    """Dedup cache + dense-vector log for feature-set exploration trials.

    Implements the explorer's ``TrialDeduplicator`` protocol (``get_cached_ndcg`` +
    ``record_trial``) so the objective skips retraining a feature set already scored
    under the same method, and persists a row per ``(feature_set_hash, category,
    method)`` carrying mask/importance vectors aligned to ``all_features``.
    """

    def __init__(self, pg_url: str, category: str, all_features: list[str]) -> None:
        self._pg_url: str = pg_url
        self._category: str = category
        # Canonical feature order: every mask/importance vector is built against this
        # so a hash's vector is comparable across trials regardless of selection order.
        self._all_features: list[str] = sorted(all_features)
        self._con: psycopg.Connection[tuple[object, ...]] | None = None

    def open(self) -> None:
        _psycopg = __import__("psycopg")
        con = _psycopg.connect(self._pg_url)
        with con.cursor() as cur:
            cur.execute(_TRIAL_LOG_DDL)
            cur.execute(_TRIAL_LOG_INDEXES)
        con.commit()
        self._con = con

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> "TrialExplorationStore":
        self.open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def get_cached_ndcg(self, feature_set_hash: str, method: str) -> float | None:
        assert self._con is not None
        with self._con.cursor() as cur:
            cur.execute(
                "SELECT ndcg_at_3 FROM trial_exploration_log "
                "WHERE feature_set_hash = %s AND category = %s AND method = %s",
                (feature_set_hash, self._category, method),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return cast("float", row[0])

    def record_trial(
        self,
        feature_set_hash: str,
        method: str,
        ndcg: float,
        feature_names: list[str],
        importance: dict[str, float] | None = None,
        params: dict[str, object] | None = None,
    ) -> None:
        assert self._con is not None
        selected = set(feature_names)
        feature_mask_vector = [f in selected for f in self._all_features]
        importance_vector = (
            [float(importance.get(f, 0.0)) for f in self._all_features]
            if importance is not None
            else None
        )
        params_json = json.dumps(params) if params is not None else None
        trial_id = f"{method}-{feature_set_hash[:16]}"
        with self._con.cursor() as cur:
            cur.execute(
                _TRIAL_LOG_UPSERT,
                (
                    trial_id,
                    feature_set_hash,
                    self._category,
                    method,
                    ndcg,
                    len(feature_names),
                    sorted(feature_names),
                    feature_mask_vector,
                    importance_vector,
                    params_json,
                ),
            )
        self._con.commit()

    def get_explored_hashes(self, method: str | None = None) -> set[str]:
        assert self._con is not None
        with self._con.cursor() as cur:
            if method is None:
                cur.execute(
                    "SELECT DISTINCT feature_set_hash FROM trial_exploration_log "
                    "WHERE category = %s",
                    (self._category,),
                )
            else:
                cur.execute(
                    "SELECT DISTINCT feature_set_hash FROM trial_exploration_log "
                    "WHERE category = %s AND method = %s",
                    (self._category, method),
                )
            return {cast("str", row[0]) for row in cur.fetchall()}

    def get_best_trials(
        self, method: str | None = None, top_k: int = 10
    ) -> list[dict[str, object]]:
        assert self._con is not None
        with self._con.cursor() as cur:
            if method is None:
                cur.execute(
                    "SELECT trial_id, feature_set_hash, method, ndcg_at_3, "
                    "feature_count, feature_names_array FROM trial_exploration_log "
                    "WHERE category = %s ORDER BY ndcg_at_3 DESC LIMIT %s",
                    (self._category, top_k),
                )
            else:
                cur.execute(
                    "SELECT trial_id, feature_set_hash, method, ndcg_at_3, "
                    "feature_count, feature_names_array FROM trial_exploration_log "
                    "WHERE category = %s AND method = %s "
                    "ORDER BY ndcg_at_3 DESC LIMIT %s",
                    (self._category, method, top_k),
                )
            return [
                {
                    "trial_id": row[0],
                    "feature_set_hash": row[1],
                    "method": row[2],
                    "ndcg_at_3": row[3],
                    "feature_count": row[4],
                    "feature_names": row[5],
                }
                for row in cur.fetchall()
            ]

    def trial_count(self, method: str | None = None) -> int:
        assert self._con is not None
        with self._con.cursor() as cur:
            if method is None:
                cur.execute(
                    "SELECT COUNT(*) FROM trial_exploration_log WHERE category = %s",
                    (self._category,),
                )
            else:
                cur.execute(
                    "SELECT COUNT(*) FROM trial_exploration_log "
                    "WHERE category = %s AND method = %s",
                    (self._category, method),
                )
            row = cur.fetchone()
        assert row is not None
        return cast("int", row[0])


def write_filtered_parquet(
    df: pl.DataFrame, feature_names: list[str], output_dir: Path
) -> Path:
    """Write the selected columns as a ``race_year=YYYY/`` Hive-partitioned dataset.

    The production training scripts read features with ``load_parquet_dir``, which globs
    for ``race_year=*/*.parquet``; a flat file is not discoverable that way. ``race_year``
    is always retained even when it is not a selected feature so each partition can be
    keyed by it. The returned path is the dataset directory, which is what the caller
    passes to the training script.
    """
    keep = set(META_COLUMNS) | _LABEL_COLS | set(feature_names) | {"race_year"}
    cols = [c for c in df.columns if c in keep]
    filtered = df.select(cols)
    output_dir.mkdir(parents=True, exist_ok=True)
    for year in sorted(filtered["race_year"].unique().to_list()):
        year_dir = output_dir / f"race_year={year}"
        year_dir.mkdir(parents=True, exist_ok=True)
        filtered.filter(pl.col("race_year") == year).write_parquet(
            year_dir / "part-0.parquet"
        )
    return output_dir


class AdaptiveLoadController:
    def __init__(
        self,
        base_n_trials: int,
        min_n_trials: int = 5,
        max_n_trials: int = 50,
        cpu_high_pct: float = 80.0,
        cpu_low_pct: float = 50.0,
        mem_high_pct: float = 80.0,
        mem_low_pct: float = 60.0,
    ) -> None:
        self._base_n_trials: int = base_n_trials
        self._min_n_trials: int = min_n_trials
        self._max_n_trials: int = max_n_trials
        self._cpu_high_pct: float = cpu_high_pct
        self._cpu_low_pct: float = cpu_low_pct
        self._mem_high_pct: float = mem_high_pct
        self._mem_low_pct: float = mem_low_pct
        # Prime psutil's per-process CPU counter so the first non-blocking
        # _cpu_percent() read has a baseline to diff against. Without this the
        # blocking interval=0.1 form was the only way to get a non-zero first
        # sample, costing a 100ms stall every round.
        if _psutil is not None:
            _psutil.cpu_percent(interval=None)

    def adjusted_n_trials(self) -> int:
        """Return trial count scaled by current system load (delegates to round_params)."""
        return self.round_params()[0]

    def inter_round_sleep_seconds(self) -> float:
        """Return 0.0 normally, 5.0 when load is high (delegates to round_params)."""
        return self.round_params()[1]

    def round_params(self) -> tuple[int, float]:
        """Read CPU/mem once and return (n_trials, sleep_secs) for the upcoming round."""
        cpu = self._cpu_percent()
        mem = self._mem_percent()
        if cpu > self._cpu_high_pct or mem > self._mem_high_pct:
            return max(round(self._base_n_trials * 0.5), self._min_n_trials), 5.0
        if cpu < self._cpu_low_pct and mem < self._mem_low_pct:
            return min(round(self._base_n_trials * 1.25), self._max_n_trials), 0.0
        return self._base_n_trials, 0.0

    def _cpu_percent(self) -> float:
        """Non-blocking psutil.cpu_percent(). Returns 0.0 if psutil not installed.

        ``interval=None`` reports CPU utilisation accumulated since the previous
        call instead of sleeping for a fresh sample, so polling once per round
        costs no wall-clock and reflects the whole inter-round span rather than a
        100ms spot reading. ``__init__`` primes the counter so the first round
        already has a baseline to diff against.
        """
        if _psutil is None:
            return 0.0
        return float(_psutil.cpu_percent(interval=None))

    def _mem_percent(self) -> float:
        """psutil.virtual_memory().percent. Returns 0.0 if psutil not installed."""
        if _psutil is None:
            return 0.0
        return float(_psutil.virtual_memory().percent)


class ContinuousLearner:
    def __init__(
        self,
        registry: FeatureRegistry,
        df: pl.DataFrame,
        category: str,
        repo_root: Path,
        scripts_dir: Path,
        docker_image_tag: str = DEFAULT_DOCKER_TAG,
        n_trials_per_round: int = DEFAULT_N_TRIALS,
        validation_years: list[int] | None = None,
        validation_year_pool: list[int] | None = None,
        blind_holdout_year: int | None = None,
        train_start: str = DEFAULT_TRAIN_START,
        deploy_threshold: float = DEFAULT_DEPLOY_THRESHOLD,
        backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
        docker_build: bool = False,
        cf_deploy: bool = False,
        cf_deploy_dir: Path | None = None,
        auto_deploy: bool = False,
        log_subgroup: bool = False,
        skip_inverse: bool = False,
        skip_enrichment: bool = False,
        load_controller: AdaptiveLoadController | None = None,
        auto_tune: bool = True,
        per_trial_timeout_s: float | None = None,
        cell_filter: CellFilter | None = None,
        cell_accuracy_store: CellAccuracyStore | None = None,
        pg_url: str = _LOCAL_PG_URL,
        exploration_method: str = "block_tpe",
        trial_store: TrialExplorationStore | None = None,
    ) -> None:
        if category not in _TRAINING_SCRIPT:
            raise ValueError(
                f"Unknown category {category!r}. Valid categories: {sorted(_TRAINING_SCRIPT)}"
            )
        self._registry: FeatureRegistry = registry
        self._df: pl.DataFrame = df
        self._category: str = category
        self._repo_root: Path = repo_root
        self._scripts_dir: Path = scripts_dir
        self._docker_image_tag: str = docker_image_tag
        self._n_trials: int = n_trials_per_round
        self._validation_years: list[int] = (
            list(validation_years)
            if validation_years is not None
            else list(DEFAULT_VALIDATION_YEARS)
        )
        if not self._validation_years:
            raise ValueError("validation_years must be a non-empty list of years")
        self._validation_year_pool: list[int] = (
            list(validation_year_pool)
            if validation_year_pool is not None
            else list(VALIDATION_YEAR_POOL)
        )
        self._blind_holdout_year: int = (
            blind_holdout_year
            if blind_holdout_year is not None
            else self._derive_blind_holdout_year(df)
        )
        self._train_start: str = train_start
        self._deploy_threshold: float = deploy_threshold
        self._backends: tuple[ModelBackend, ...] = backends
        self._docker_build: bool = docker_build
        self._cf_deploy: bool = cf_deploy
        self._cf_deploy_dir: Path | None = cf_deploy_dir
        self._auto_deploy: bool = auto_deploy
        self._log_subgroup: bool = log_subgroup
        self._skip_inverse: bool = skip_inverse
        self._skip_enrichment: bool = skip_enrichment
        self._stop: bool = False
        self._load_controller: AdaptiveLoadController | None = load_controller
        self._auto_tune: bool = auto_tune
        self._per_trial_timeout_s: float | None = per_trial_timeout_s
        self._cell_filter: CellFilter | None = cell_filter
        self._cell_accuracy_store: CellAccuracyStore | None = cell_accuracy_store
        self._pg_url: str = pg_url
        self._exploration_method: str = exploration_method
        self._trial_store: TrialExplorationStore | None = trial_store
        if cell_filter:
            original_len = len(self._df)
            self._df = filter_dataframe_by_cell(self._df, cell_filter)
            _logger.info(
                "cell filter applied: %d → %d rows",
                original_len,
                len(self._df),
            )
            if self._df.is_empty():
                raise ValueError("cell filter produced empty DataFrame")
        _sire_required = {"ketto_toroku_bango", "track_code", "kyori", "finish_position", "keibajo_code"}
        if (
            pg_url
            and "sire_venue_surface_dist_win_rate" not in self._df.columns
            and _sire_required.issubset(self._df.columns)
        ):
            _logger.info("computing sire venue bias features...")
            self._df = compute_sire_venue_bias_features(self._df, pg_url)
        self._last_enrichment: list[tuple[str, float]] | None = None
        self._saturated: bool = False
        self._fold_cache: dict[int, FoldSplit] = {}

    @staticmethod
    def _derive_blind_holdout_year(df: pl.DataFrame) -> int:
        """Latest year present in df; falls back to the pool max for an empty/year-less df."""
        if "race_year" in df.columns and not df.is_empty():
            years = df["race_year"].cast(pl.Float64, strict=False).drop_nulls()
            max_year = years.max()
            if isinstance(max_year, (int, float)):
                return int(max_year)
        return max(VALIDATION_YEAR_POOL)

    def request_stop(self) -> None:
        self._stop = True

    def _auto_tune_resources(self) -> int:
        """Return optimal nthread based on current system state.

        Respects hard ceilings (nthread <= _MAX_NTHREAD) and ensures a
        free-memory buffer of at least _MIN_FREE_MEM_GB.
        """
        if _psutil is None:
            return _MAX_NTHREAD
        cpu_count = _psutil.cpu_count(logical=True) or 8
        load_avg_1m = _psutil.getloadavg()[0]
        cpu_idle_fraction = max(0.0, 1.0 - load_avg_1m / cpu_count)
        mem = _psutil.virtual_memory()
        free_gb = mem.available / (1024**3)
        optimal_threads = max(
            _MIN_NTHREAD,
            min(_MAX_NTHREAD, int(cpu_count * cpu_idle_fraction * 0.5)),
        )
        if free_gb < _MIN_FREE_MEM_GB:
            optimal_threads = _MIN_NTHREAD
        _logger.info(
            "resource auto-tune: load=%.1f/%d cores, free_mem=%.1fGB -> nthread=%d",
            load_avg_1m,
            cpu_count,
            free_gb,
            optimal_threads,
        )
        return optimal_threads

    def run(self, max_rounds: int | None = None) -> None:
        round_label = f"max {max_rounds} rounds" if max_rounds is not None else "unlimited"
        _logger.info(
            "━━━ continuous learning loop started ━━━  category: %s | %s | base trials: %d",
            self._category,
            round_label,
            self._n_trials,
        )
        self._warn_auto_deploy_artifacts()
        round_num = 0
        while not self._stop:
            if max_rounds is not None and round_num >= max_rounds:
                _logger.info("reached max rounds (%d) — stopping", max_rounds)
                break

            if self._auto_tune:
                nthread = self._auto_tune_resources()
                _logger.info("round %d auto-tuned nthread: %d", round_num, nthread)

            actual_trials = self._n_trials
            sleep_secs = 0.0
            if self._load_controller is not None:
                actual_trials, sleep_secs = self._load_controller.round_params()
                if actual_trials != self._n_trials:
                    _logger.info(
                        "n_trials adjusted for system load: %d → %d",
                        self._n_trials,
                        actual_trials,
                    )

            if self._saturated:
                actual_trials = max(
                    actual_trials // _SATURATED_TRIAL_DIVISOR, _MIN_SATURATED_TRIALS
                )

            progress = (
                f"{round_num + 1}/{max_rounds}" if max_rounds else f"#{round_num + 1}"
            )
            _logger.info("─── round %s started (trials: %d) ───", progress, actual_trials)
            _round_t0 = time.perf_counter()
            self._explore_round(round_num, n_trials=actual_trials)
            saturated = self._check_deploy_readiness()
            if saturated and not self._saturated:
                _logger.info(
                    "saturation latched — subsequent rounds will use reduced trials"
                )
            self._saturated = self._saturated or saturated
            if self._log_subgroup and (self._cell_filter is not None or round_num % 5 == 0):
                self._log_subgroup_diagnostics()
            if self._saturated:
                _logger.info(
                    "saturated — skipping inverse and enrichment phases this round"
                )
            else:
                if not self._skip_inverse:
                    self._check_and_try_inverses(round_num, actual_trials)
                if not self._skip_enrichment:
                    self._analyze_feature_enrichment(round_num)
            _elapsed = time.perf_counter() - _round_t0
            _logger.info(
                "─── round %s done (elapsed: %.1fs) ───", progress, _elapsed
            )

            if sleep_secs > 0:
                _logger.info(
                    "system load high — sleeping %.1fs before next round",
                    sleep_secs,
                )
                time.sleep(sleep_secs)

            round_num += 1

        _logger.info(
            "━━━ continuous learning loop finished ━━━  completed rounds: %d", round_num
        )

    def _priority_subsets(self) -> list[set[str]]:
        """Feature masks worth force-evaluating first: the active set, then active + top enriched.

        Seeding the study with these guarantees each round spends its first trials on the
        current champion and the most promising enrichment-driven extension before the
        sampler starts its own search, so a short trial budget isn't wasted rediscovering
        the known-good region.
        """
        active = self._registry.get_active_entry()
        if active is None:
            return []
        active_set = set(active["feature_names"])
        subsets = [active_set]
        self._last_enrichment = self._registry.compute_feature_enrichment()
        enriched = [
            name
            for name, score in self._last_enrichment
            if score > 0 and name not in active_set
        ][:MAX_ENRICHMENT_FEATURES]
        if enriched:
            subsets.append(active_set | set(enriched))
        return subsets

    def _explore_round(self, round_num: int, n_trials: int) -> None:
        study_name = f"auto-{self._category}-r{round_num}-{uuid.uuid4().hex[:8]}"
        k = 1 if self._saturated else DEFAULT_VALIDATION_YEARS_PER_ROUND
        round_years = select_round_validation_years(
            round_num, self._validation_year_pool, self._blind_holdout_year, k=k
        )
        _logger.info(
            "round %d validation years: %s (blind holdout: %d)",
            round_num,
            round_years,
            self._blind_holdout_year,
        )
        if self._exploration_method == "combined":
            from learning.feature_explorer import run_combined_exploration

            run_combined_exploration(
                df=self._df,
                registry=self._registry,
                study_name=study_name,
                n_trials=n_trials,
                validation_years=round_years,
                train_start=self._train_start,
                params=DEFAULT_PARAMS,
                backends=self._backends,
                per_trial_timeout_s=self._per_trial_timeout_s,
                screening=True,
                trial_store=self._trial_store,
            )
            return
        run_exploration(
            df=self._df,
            registry=self._registry,
            study_name=study_name,
            n_trials=n_trials,
            validation_years=round_years,
            train_start=self._train_start,
            backends=self._backends,
            per_trial_timeout_s=self._per_trial_timeout_s,
            enqueue_subsets=self._priority_subsets(),
            screening=True,
            trial_dedup=self._trial_store,
        )

    def _check_and_try_inverses(self, round_num: int, n_trials: int) -> None:
        """Try the inverse of each strongly negative trial, capped per round.

        Each inverse trial spawns its own exploration whose new trials can also be
        strongly negative, so an uncapped sweep blocks normal rounds. The cap bounds
        the work to MAX_INVERSE_PER_ROUND inverse explorations before moving on.
        """
        negative_trials = self._registry.list_strongly_negative_trials(
            STRONG_NEGATIVE_THRESHOLD_PP
        )
        attempted = 0
        for trial in negative_trials:
            if attempted >= MAX_INVERSE_PER_ROUND:
                _logger.info(
                    "inverse cap reached (%d) — continuing to next round",
                    MAX_INVERSE_PER_ROUND,
                )
                break
            trial_id = trial["trial_id"]
            for approach in INVERSE_APPROACH_TYPES:
                if attempted >= MAX_INVERSE_PER_ROUND:
                    break
                inverse_name = f"{trial_id}__{approach}"
                if self._registry.has_inverse_been_tried(trial_id, inverse_name):
                    _logger.info(
                        "inverse already tried: %s / %s — skipping", trial_id, approach
                    )
                    continue
                _logger.info("trying inverse: %s / %s", trial_id, approach)
                inverse_result = self._run_inverse_exploration(
                    trial, approach, round_num, n_trials
                )
                self._registry.record_inverse_trial(
                    original_trial_id=trial_id,
                    inverse_name=inverse_name,
                    approach_type=approach,
                    delta_pp=inverse_result["delta_pp"],
                    decision=inverse_result["decision"],
                )
                attempted += 1

    def _run_inverse_exploration(
        self, trial: FeatureEntry, approach: str, round_num: int, n_trials: int
    ) -> InverseResult:
        """Run one inverse approach and return its delta_pp and ADOPT/REJECT decision.

        The delta must reflect *this* inverse run's own gain, so it is measured as the
        best NDCG produced by this study minus the active NDCG captured before the run.
        Using the global best (``get_best_ndcg``) instead would report the same stale
        value for every inverse trial in a round, since unrelated earlier trials and
        any mid-run promotion would dominate.
        """
        inverse_study_name = f"inv-{approach}-{trial['trial_id']}-r{round_num}"
        _logger.info(
            "inverse exploration: %s approach=%s", inverse_study_name, approach
        )
        pre_active = self._registry.get_active_entry()
        pre_active_ndcg = pre_active["ndcg_at_3"] if pre_active is not None else 0.0
        round_years = select_round_validation_years(
            round_num, self._validation_year_pool, self._blind_holdout_year
        )
        # Inverse is a screen for whether negating a feature set helps, so it runs on a
        # single validation fold instead of the full round — if a negated set cannot beat
        # the active model on one year it will not on more, and this halves the per-trial cost.
        screen_years = round_years[:1]
        run_exploration(
            df=self._df,
            registry=self._registry,
            study_name=inverse_study_name,
            n_trials=INVERSE_N_TRIALS,
            validation_years=screen_years,
            train_start=self._train_start,
            backends=self._backends,
            screening=True,
        )
        best_from_study = self._registry.get_best_ndcg_for_study(inverse_study_name)
        if best_from_study is None:
            _logger.info(
                "inverse exploration produced no scored trials: %s", inverse_study_name
            )
            return {"delta_pp": {"ndcg_delta": 0.0}, "decision": "REJECT"}
        delta = best_from_study - pre_active_ndcg
        decision = "ADOPT" if delta > 0 else "REJECT"
        return {"delta_pp": {"ndcg_delta": delta}, "decision": decision}

    def _analyze_feature_enrichment(self, round_num: int) -> None:
        """Log enrichment analysis and run a targeted trial when promising features appear.

        Features that recur in the top trials but not the bottom ones (high enrichment
        score) are candidates the active set is missing. When such features exist that
        are not already active, a follow-up exploration is launched to fold them in.
        """
        if self._last_enrichment is not None:
            enriched = self._last_enrichment
            self._last_enrichment = None
        else:
            enriched = self._registry.compute_feature_enrichment()
        if not enriched:
            _logger.info(
                "no enriched features found (threshold=%.1f)", ENRICHMENT_THRESHOLD
            )
            return
        for name, score in enriched[:10]:
            _logger.info("enriched feature: %s score=%.3f", name, score)
        active = self._registry.get_active_entry()
        if active is None:
            return
        active_features = set(active["feature_names"])
        candidates = [
            (name, score)
            for name, score in enriched
            if name not in active_features and score > 0
        ]
        if not candidates:
            return
        _logger.info(
            "running enrichment trial with %d candidate features",
            min(len(candidates), MAX_ENRICHMENT_FEATURES),
        )
        self._run_enrichment_trial(
            active_features, candidates[:MAX_ENRICHMENT_FEATURES], round_num
        )

    def _run_enrichment_trial(
        self,
        active_features: set[str],
        candidates: list[tuple[str, float]],
        round_num: int,
    ) -> None:
        """Run exploration with active features + enriched candidates as the focus set."""
        _ = active_features
        enriched_names = [name for name, _ in candidates]
        study_name = f"enrichment-r{round_num}-{'+'.join(enriched_names[:3])}"
        round_years = select_round_validation_years(
            round_num, self._validation_year_pool, self._blind_holdout_year
        )
        # Enrichment is a targeted screen for whether folding in the candidate features
        # helps, so it runs on a single validation fold instead of the full round to keep
        # the per-round enrichment cost small.
        screen_years = round_years[:1]
        _logger.info("enrichment trial: adding %s to active set", enriched_names)
        run_exploration(
            df=self._df,
            registry=self._registry,
            study_name=study_name,
            n_trials=ENRICHMENT_N_TRIALS,
            validation_years=screen_years,
            train_start=self._train_start,
            backends=self._backends,
            screening=True,
        )

    def _warn_auto_deploy_artifacts(self) -> None:
        """Log a warning for any ``auto-*`` model dirs left in the working tree.

        These directories are produced only by the opt-in auto-deploy path; their
        presence means a previous auto-deploy staged artifacts and (when enabled)
        rewrote ``model_meta.json`` without a commit, leaving the working tree pointing
        at an ``auto-*`` version instead of the committed production model. They are
        never removed here (data is not deleted) — only surfaced so the operator can
        reconcile the working tree against the committed production models.
        """
        models_root = self._repo_root / _CONTAINER_MODELS_ROOT / self._category
        if not models_root.is_dir():
            return
        for path in sorted(p for p in models_root.glob("auto-*") if p.is_dir()):
            _logger.warning(
                "auto-deploy artifact present in working tree: %s — remove or reconcile "
                "before committing (production model_meta.json must point to the "
                "committed model, not an auto-* version)",
                path,
            )

    def _check_deploy_readiness(self) -> bool:
        """Evaluate whether the active entry is a deploy candidate; return True if saturated.

        This never modifies files or staged artifacts on its own. When the active entry
        clears the deploy threshold and the blind holdout confirms it, the entry is
        logged as a DEPLOY CANDIDATE and recorded in the registry so the same entry is
        not re-flagged every round, but deployment stays manual. Only when the learner
        was constructed with ``auto_deploy=True`` does the (dangerous, NDCG-only) deploy
        path run, overwriting ``model_meta.json`` and staging model artifacts.

        A True return signals the round loop that the search space is exhausted, so the
        per-round inverse and enrichment phases can be skipped — they cannot help once
        the registry is saturated.
        """
        if self._registry.is_saturated(SATURATION_LOOKBACK):
            _logger.info(
                "deploy check skipped: registry saturated (last %d trials showed no improvement)",
                SATURATION_LOOKBACK,
            )
            return True
        active = self._registry.get_active_entry()
        if active is None:
            _logger.debug("no active entry — skipping deploy check")
            return False
        deployed_ndcg = self._registry.get_deployed_ndcg()
        delta = active["ndcg_at_3"] - deployed_ndcg
        if delta < self._deploy_threshold:
            _logger.info(
                "deploy check: improvement below threshold  "
                "current: %.4f | deployed: %.4f | delta: %+.4f | gap to threshold: %.4f",
                active["ndcg_at_3"],
                deployed_ndcg,
                delta,
                self._deploy_threshold - delta,
            )
            return False
        _logger.info(
            "NDCG@3 improved by %+.4f (%.4f → %.4f) — evaluating blind holdout",
            delta,
            deployed_ndcg,
            active["ndcg_at_3"],
        )
        blind_ndcg = self._evaluate_blind_holdout(active)
        blind_delta = blind_ndcg - deployed_ndcg
        if blind_delta < self._deploy_threshold:
            _logger.info(
                "deploy check: blind holdout %d did not confirm  "
                "blind ndcg: %.4f | deployed: %.4f | blind delta: %+.4f",
                self._blind_holdout_year,
                blind_ndcg,
                deployed_ndcg,
                blind_delta,
            )
            return False
        _logger.info(
            "blind holdout %d confirmed (%.4f, delta %+.4f)",
            self._blind_holdout_year,
            blind_ndcg,
            blind_delta,
        )
        if self._auto_deploy:
            _logger.warning(
                "auto_deploy enabled — running NDCG-only deploy path (overwrites "
                "model_meta.json and stages artifacts without the multi-metric gate)"
            )
            self._deploy(active)
            return False
        feature_count = len(active["feature_names"])
        _logger.info(
            "DEPLOY CANDIDATE: %s with NDCG@3=%.4f, features=%d. Manual deployment required.",
            self._make_model_version(),
            active["ndcg_at_3"],
            feature_count,
        )
        self._registry.record_deployment(active["ndcg_at_3"], feature_count)
        return False

    def _evaluate_blind_holdout(self, entry: FeatureEntry) -> float:
        """NDCG@3 of the entry's feature set on the blind holdout year only.

        This year never enters Optuna search, so it gives an unbiased read on
        whether a round's winner truly generalises before we deploy it.
        """
        return evaluate_feature_set(
            self._df,
            entry["feature_names"],
            [self._blind_holdout_year],
            self._train_start,
            DEFAULT_PARAMS,
            self._backends,
        )

    def _log_subgroup_diagnostics(self) -> None:
        active = self._registry.get_active_entry()
        if active is None:
            _logger.info("subgroup diagnostics: no active entry — skipping")
            return
        feature_names = active["feature_names"]
        feature_set_hash = compute_feature_set_hash(feature_names)

        already_evaluated: set[str] = set()
        if self._cell_accuracy_store is not None:
            already_evaluated = self._cell_accuracy_store.evaluated_cells(feature_set_hash)
            if already_evaluated:
                _logger.info(
                    "cell accuracy store: %d cells already evaluated for hash %s",
                    len(already_evaluated),
                    feature_set_hash[:12],
                )

        predictions = self._collect_active_predictions(feature_names)
        if predictions.is_empty():
            _logger.info("subgroup diagnostics: no predictions produced — skipping")
            return
        metrics = compute_subgroup_diagnostics(predictions, self._df)
        if not metrics:
            _logger.info("subgroup diagnostics: no subgroups to report")
            return

        new_metrics: list[SubgroupMetrics] = []
        skipped = 0
        _logger.info(
            "subgroup diagnostics (active set, %d features):", len(feature_names)
        )
        for m in metrics:
            cell_key = f"{m['category']}_{m['surface']}_{m['distance_band']}_{m['class_label']}_{m['season']}_{m['venue']}"
            if cell_key in already_evaluated:
                skipped += 1
                continue
            new_metrics.append(m)
            _logger.info(
                "│  %-8s %-6s %-14s %-8s %-8s %-8s  races=%5d  "
                "ndcg@3=%.4f  top1=%.4f  p2=%.4f  p3=%.4f  p4=%.4f  p5=%.4f  p6=%.4f  top3_box=%.4f",
                m["category"],
                m["surface"],
                m["distance_band"],
                m["class_label"],
                m["season"],
                m["venue"],
                m["race_count"],
                m["ndcg_at_3"],
                m["top1_accuracy"],
                m["place2_accuracy"],
                m["place3_accuracy"],
                m["place4_accuracy"],
                m["place5_accuracy"],
                m["place6_accuracy"],
                m["top3_box_accuracy"],
            )
        if skipped:
            _logger.info("│  (%d cells skipped — already evaluated)", skipped)

        if self._cell_accuracy_store is not None and new_metrics:
            saved = self._cell_accuracy_store.save_cell_metrics(
                feature_set_hash, len(feature_names), new_metrics, feature_names
            )
            _logger.info("cell accuracy store: saved %d cell evaluations", saved)

        self._log_surface_summary(metrics)

    def _log_surface_summary(self, metrics: list[SubgroupMetrics]) -> None:
        """Log aggregated metrics grouped by surface (turf/dirt/other)."""
        surface_groups: dict[str, list[SubgroupMetrics]] = {}
        for m in metrics:
            surface_groups.setdefault(m["surface"], []).append(m)
        _logger.info("surface summary:")
        for surface in sorted(surface_groups):
            group = surface_groups[surface]
            total_races = sum(m["race_count"] for m in group)
            if total_races == 0:
                continue
            weighted_ndcg = (
                sum(m["ndcg_at_3"] * m["race_count"] for m in group) / total_races
            )
            weighted_top1 = (
                sum(m["top1_accuracy"] * m["race_count"] for m in group) / total_races
            )
            weighted_top3 = (
                sum(m["top3_box_accuracy"] * m["race_count"] for m in group)
                / total_races
            )
            _logger.info(
                "│  surface=%-6s  races=%5d  ndcg@3=%.4f  top1=%.4f  top3_box=%.4f",
                surface,
                total_races,
                weighted_ndcg,
                weighted_top1,
                weighted_top3,
            )

    def _get_folds(self, years: list[int]) -> list[FoldSplit]:
        """Return the walk-forward folds for the given years, memoised per year.

        Each fold is produced by ``split_walk_forward(self._df, self._train_start, year)``
        whose result depends only on the year because ``self._df`` and ``self._train_start``
        are fixed for the learner's lifetime, so the O(rows) string-cast + boolean filter is
        paid once per year instead of once per caller per round. Folds are returned in the
        same order as ``years``.
        """
        folds: list[FoldSplit] = []
        for year in years:
            cached = self._fold_cache.get(year)
            if cached is None:
                cached = split_walk_forward(self._df, self._train_start, year)
                self._fold_cache[year] = cached
            folds.append(cached)
        return folds

    def _collect_active_predictions(self, feature_names: list[str]) -> pl.DataFrame:
        """Train the active feature set on each validation fold and stack predictions.

        Returns a frame of (race_id, ketto_toroku_bango, predicted_rank) over all
        validation years; an empty frame when no fold yields predictions.
        """
        feature_set = set(feature_names)
        frames: list[pl.DataFrame] = []
        for fold in self._get_folds(self._validation_years):
            if fold["train_df"].is_empty() or fold["valid_df"].is_empty():
                continue
            fold_filtered = select_fold_features(fold, feature_set)
            for backend in self._backends:
                preds = predict_fold_with_backend(
                    fold_filtered, backend, DEFAULT_PARAMS
                )
                if preds is None:
                    continue
                # select() returns a fresh 3-column frame that owns its data, so the
                # full-width preds block is freed each iteration instead of being
                # pinned alive by the slice until the final concat.
                frames.append(
                    preds.select(["race_id", "ketto_toroku_bango", "predicted_rank"])
                )
                del preds
            del fold_filtered
        if not frames:
            return pl.DataFrame(
                schema={
                    "race_id": pl.Utf8,
                    "ketto_toroku_bango": pl.Utf8,
                    "predicted_rank": pl.Int64,
                }
            )
        return pl.concat(frames)

    def _deploy(self, entry: FeatureEntry) -> None:
        feature_names = entry["feature_names"]
        model_version = self._make_model_version()
        _logger.info("┌── deploy started %s", "─" * 44)
        _logger.info("│  version    : %s", model_version)
        _logger.info("│  ndcg@3     : %.4f", entry["ndcg_at_3"])
        _logger.info("│  features   : %d columns", len(feature_names))
        _logger.info("│")
        _logger.info("│  [1/5] filtering feature parquet ...")
        staged_dest: Path | None = None
        prev_meta_content: str | None = None
        try:
            with tempfile.TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                filtered_parquet = write_filtered_parquet(
                    self._df, feature_names, tmp_path / "parquet"
                )
                _logger.info("│  [2/5] training production model ...")
                model_dir = self._train_production_model(
                    filtered_parquet, tmp_path / "models", model_version
                )
                _logger.info("│  [3/5] staging model artifacts ...")
                staged_dest = self._stage_model(model_dir, feature_names, model_version)
            _logger.info("│  [4/5] updating model_meta.json ...")
            prev_meta_content = self._update_model_meta_json(model_version, len(feature_names))
            if self._docker_build:
                _logger.info("│  [5/5] rebuilding Docker image ...")
                self._rebuild_docker()
            if self._cf_deploy:
                _logger.info("│  [5/5] deploying to Cloudflare Container ...")
                self._deploy_cf_container()
            self._registry.record_deployment(entry["ndcg_at_3"], len(feature_names))
        except Exception:
            _logger.error("│  deploy failed — rolling back staged artifacts")
            self._rollback_deploy(staged_dest, prev_meta_content)
            raise
        _logger.info("└── deploy finished %s", "─" * 44)

    def _make_model_version(self) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"auto-{self._category}-{ts}"

    def _train_production_model(
        self, parquet_path: Path, model_root: Path, model_version: str
    ) -> Path:
        script_name = _TRAINING_SCRIPT[self._category]
        year_to = max(self._validation_years)
        _logger.info("│    script: %s  target year: %d", script_name, year_to)
        cmd = [
            sys.executable,
            str(self._scripts_dir / script_name),
            "--features-parquet",
            str(parquet_path),
            "--category",
            self._category,
            "--walk-forward-namespace",
            model_version,
            "--year-from",
            str(year_to),
            "--year-to",
            str(year_to),
            "--train-start-date",
            self._train_start,
            "--model-root",
            str(model_root),
            "--iteration-id",
            "0",
        ]
        subprocess.run(cmd, check=True, timeout=DEFAULT_TRAINING_TIMEOUT_S)
        return model_root / self._category / "iter0" / f"fold-{year_to}"

    def _stage_model(
        self, model_dir: Path, feature_names: list[str], model_version: str
    ) -> Path:
        model_json = model_dir / "model.json"
        if not model_json.exists():
            raise FileNotFoundError(
                f"model.json not found in fold directory: {model_json}. "
                "Re-train this fold without --resume-from-checkpoint to regenerate."
            )
        dest = self._repo_root / _CONTAINER_MODELS_ROOT / self._category / model_version
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(model_json, dest / "model.json")
        (dest / "metadata.json").write_text(
            json.dumps({"feature_names": feature_names}, ensure_ascii=False),
            encoding="utf-8",
        )
        _logger.info("│    staged to: %s", dest)
        return dest

    def _update_model_meta_json(self, model_version: str, feature_count: int) -> str:
        json_path = self._repo_root / _MODEL_META_JSON_PATH
        if not json_path.exists():
            raise FileNotFoundError(f"model_meta.json not found: {json_path}")
        prev_content = json_path.read_text(encoding="utf-8")
        payload = json.loads(prev_content)
        if not isinstance(payload, dict):
            raise ValueError(f"model_meta.json must be a JSON object: {json_path}")
        raw_mv = payload.get("model_versions")
        model_versions: dict[str, str] = (
            dict(raw_mv) if isinstance(raw_mv, dict) else {}
        )
        raw_fc = payload.get("feature_counts")
        feature_counts: dict[str, int] = (
            dict(raw_fc) if isinstance(raw_fc, dict) else {}
        )
        prev_version = model_versions.get(self._category, "none")
        model_versions[self._category] = model_version
        feature_counts[self._category] = feature_count
        _logger.info("│    model version: %s → %s", prev_version, model_version)
        atomic_write_metadata(
            json_path,
            {"model_versions": model_versions, "feature_counts": feature_counts},
        )
        return prev_content

    def _rebuild_docker(self) -> None:
        dockerfile = (
            self._repo_root
            / "apps"
            / "finish-position-predict-container"
            / "Dockerfile"
        )
        _logger.info("│    building image: %s", self._docker_image_tag)
        subprocess.run(
            [
                "docker",
                "build",
                "-f",
                str(dockerfile),
                "-t",
                self._docker_image_tag,
                str(self._repo_root),
            ],
            check=True,
            timeout=DEFAULT_DOCKER_BUILD_TIMEOUT_S,
        )
        _logger.info("│    Docker build succeeded")

    def _deploy_cf_container(self) -> None:
        container_dir = (
            self._cf_deploy_dir
            if self._cf_deploy_dir is not None
            else self._repo_root / _CONTAINER_APP_DIR
        )
        _logger.info("│    deploying from: %s", container_dir)
        subprocess.run(
            ["bunx", "wrangler", "deploy"],
            cwd=str(container_dir),
            check=True,
            timeout=DEFAULT_CF_DEPLOY_TIMEOUT_S,
        )
        _logger.info("│    CF Container deploy succeeded")

    def _rollback_deploy(self, staged_dest: Path | None, prev_meta_content: str | None) -> None:
        """Remove staged artifacts and restore model_meta.json after a failed deploy."""
        if staged_dest is not None:
            try:
                if staged_dest.exists():
                    shutil.rmtree(staged_dest)
                    _logger.info("│    [rollback] removed staged dir: %s", staged_dest)
            except Exception as exc:
                _logger.error("│    [rollback] failed to remove staged dir: %s", exc)
        if prev_meta_content is None:
            return
        try:
            json_path = self._repo_root / _MODEL_META_JSON_PATH
            temp_path = json_path.with_suffix(json_path.suffix + ".tmp")
            temp_path.write_text(prev_meta_content, encoding="utf-8")
            os.replace(temp_path, json_path)
            _logger.info("│    [rollback] restored model_meta.json")
        except Exception as exc:
            _logger.error("│    [rollback] failed to restore model_meta.json: %s", exc)


def _resolve_backends(
    backends_arg: str | None, category: str
) -> tuple[ModelBackend, ...]:
    """Parse a --backends CSV into validated ModelBackend tokens, or fall back per category."""
    if backends_arg is None:
        return CATEGORY_BACKENDS.get(category, DEFAULT_BACKENDS)
    allowed = get_args(ModelBackend)
    resolved: list[ModelBackend] = []
    for token in backends_arg.split(","):
        name = token.strip()
        if name not in allowed:
            raise ValueError(
                f"Unknown backend {name!r}. Valid backends: {sorted(allowed)}"
            )
        resolved.append(cast("ModelBackend", name))
    return tuple(resolved)


def _setup_signal_handler(learner: ContinuousLearner) -> None:
    def _handler(signum: int, _frame: object) -> None:
        _ = signum
        learner.request_stop()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Continuous Walk-Forward feature exploration loop. Logs deploy readiness; "
            "deployment is manual unless --auto-deploy is set."
        )
    )
    parser.add_argument("--features-parquet", type=Path, required=True)
    parser.add_argument("--category", type=str, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument(
        "--registry-path", type=Path, default=Path("feature_registry.duckdb")
    )
    parser.add_argument("--docker-tag", type=str, default=DEFAULT_DOCKER_TAG)
    parser.add_argument("--n-trials", type=int, default=DEFAULT_N_TRIALS)
    parser.add_argument(
        "--deploy-threshold", type=float, default=DEFAULT_DEPLOY_THRESHOLD
    )
    parser.add_argument("--max-rounds", type=int, default=None)
    parser.add_argument("--min-trials", type=int, default=5)
    parser.add_argument("--max-trials", type=int, default=50)
    parser.add_argument("--backends", type=str, default=None)
    parser.add_argument("--train-start", type=str, default=None)
    parser.add_argument(
        "--auto-deploy",
        action="store_true",
        help=(
            "DANGEROUS: enable the NDCG-only auto-deploy path that overwrites "
            "model_meta.json and stages model artifacts without the multi-metric "
            "(top1/place2/place3/place4-6/top3_box per cell) gate, cell routing, or "
            "sim/E-top2 awareness. Off by default; readiness is logged but deployment "
            "stays manual. --docker-build / --cf-deploy are ignored unless this is set."
        ),
    )
    parser.add_argument("--docker-build", action="store_true")
    parser.add_argument("--cf-deploy", action="store_true")
    parser.add_argument("--cf-deploy-dir", type=Path, default=None)
    parser.add_argument("--log-subgroup", action="store_true")
    parser.add_argument("--skip-inverse", action="store_true")
    parser.add_argument("--skip-enrichment", action="store_true")
    parser.add_argument("--auto-tune", dest="auto_tune", action="store_true", default=True)
    parser.add_argument("--no-auto-tune", dest="auto_tune", action="store_false")
    parser.add_argument("--per-trial-timeout", type=float, default=None)
    parser.add_argument(
        "--keibajo-codes",
        nargs="*",
        default=None,
    )
    parser.add_argument(
        "--season-bands",
        nargs="*",
        default=None,
    )
    parser.add_argument(
        "--pg-url",
        type=str,
        default=_LOCAL_PG_URL,
        help="PostgreSQL URL for cell accuracy persistence",
    )
    parser.add_argument(
        "--exploration-method",
        type=str,
        default="combined",
        choices=["block_tpe", "combined"],
        help="block_tpe (block TPE only) or combined (SHAP stepwise + block TPE)",
    )
    args = parser.parse_args(argv)
    setup_logging()

    category = str(args.category)
    train_start = (
        str(args.train_start)
        if args.train_start is not None
        else _CATEGORY_TRAIN_START.get(category, DEFAULT_TRAIN_START)
    )
    df = _load_features_dataframe(args.features_parquet, train_start)
    cell_filter: CellFilter | None = None
    if args.keibajo_codes or args.season_bands:
        cell_filter = CellFilter()
        if args.keibajo_codes:
            cell_filter["keibajo_codes"] = [str(c) for c in args.keibajo_codes]
        if args.season_bands:
            cell_filter["season_bands"] = [str(s) for s in args.season_bands]
    backends = _resolve_backends(args.backends, category)
    auto_deploy = bool(args.auto_deploy)
    scripts_dir = Path(__file__).parent.parent

    load_controller = AdaptiveLoadController(
        base_n_trials=int(args.n_trials),
        min_n_trials=int(args.min_trials),
        max_n_trials=int(args.max_trials),
    )

    cell_store: CellAccuracyStore | None = None
    trial_store: TrialExplorationStore | None = None
    if args.log_subgroup:
        cell_store = CellAccuracyStore(pg_url=str(args.pg_url))
        cell_store.open()
        trial_store = TrialExplorationStore(
            pg_url=str(args.pg_url),
            category=category,
            all_features=resolve_feature_columns(list(df.columns)),
        )
        trial_store.open()

    with FeatureRegistry(args.registry_path) as registry:
        learner = ContinuousLearner(
            registry=registry,
            df=df,
            category=category,
            repo_root=args.repo_root,
            scripts_dir=scripts_dir,
            docker_image_tag=str(args.docker_tag),
            n_trials_per_round=int(args.n_trials),
            train_start=train_start,
            deploy_threshold=float(args.deploy_threshold),
            backends=backends,
            docker_build=auto_deploy and bool(args.docker_build),
            cf_deploy=auto_deploy and bool(args.cf_deploy),
            cf_deploy_dir=args.cf_deploy_dir,
            auto_deploy=auto_deploy,
            log_subgroup=bool(args.log_subgroup),
            skip_inverse=bool(args.skip_inverse),
            skip_enrichment=bool(args.skip_enrichment),
            load_controller=load_controller,
            auto_tune=bool(args.auto_tune),
            per_trial_timeout_s=(
                float(args.per_trial_timeout)
                if args.per_trial_timeout is not None
                else None
            ),
            cell_filter=cell_filter,
            cell_accuracy_store=cell_store,
            pg_url=str(args.pg_url),
            exploration_method=str(args.exploration_method),
            trial_store=trial_store,
        )
        _setup_signal_handler(learner)
        learner.run(max_rounds=args.max_rounds)
        if cell_store is not None:
            cell_store.close()
        if trial_store is not None:
            trial_store.close()


if __name__ == "__main__":
    main()
