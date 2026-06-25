"""Continuous self-improving Walk-Forward learning loop with auto-deploy."""

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
from typing import Final, TypedDict, cast, get_args

import polars as pl

from learning.feature_explorer import (
    CATEGORY_BACKENDS,
    DEFAULT_BACKENDS,
    DEFAULT_PARAMS,
    DEFAULT_TRAIN_START,
    DEFAULT_VALIDATION_YEARS,
    VALIDATION_YEAR_POOL,
    ModelBackend,
    evaluate_feature_set,
    predict_fold_with_backend,
    run_exploration,
    select_fold_features,
    select_round_validation_years,
)
from learning.feature_registry import INVERSE_APPROACH_TYPES, FeatureEntry, FeatureRegistry
from learning.subgroup_diagnostics import compute_subgroup_diagnostics
from finish_position_lightgbm import LABEL_COLUMNS, META_COLUMNS, split_walk_forward
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

_MIN_NTHREAD: Final[int] = 2
_MAX_NTHREAD: Final[int] = 6
_MIN_FREE_MEM_GB: Final[float] = 8.0


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
        return _load_partitioned_features(
            str(parquet_path / "**" / "*.parquet"), min_year
        )
    return pl.read_parquet(str(parquet_path))


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
        """psutil.cpu_percent(interval=0.1). Returns 0.0 if psutil not installed."""
        if _psutil is None:
            return 0.0
        return float(_psutil.cpu_percent(interval=0.1))

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
        log_subgroup: bool = False,
        skip_inverse: bool = False,
        skip_enrichment: bool = False,
        load_controller: AdaptiveLoadController | None = None,
        auto_tune: bool = True,
        per_trial_timeout_s: float | None = None,
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
        self._log_subgroup: bool = log_subgroup
        self._skip_inverse: bool = skip_inverse
        self._skip_enrichment: bool = skip_enrichment
        self._stop: bool = False
        self._load_controller: AdaptiveLoadController | None = load_controller
        self._auto_tune: bool = auto_tune
        self._per_trial_timeout_s: float | None = per_trial_timeout_s

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

            progress = (
                f"{round_num + 1}/{max_rounds}" if max_rounds else f"#{round_num + 1}"
            )
            _logger.info("─── round %s started (trials: %d) ───", progress, actual_trials)
            _round_t0 = time.perf_counter()
            self._explore_round(round_num, n_trials=actual_trials)
            saturated = self._maybe_deploy()
            if self._log_subgroup:
                self._log_subgroup_diagnostics()
            if not saturated and not self._skip_inverse:
                self._check_and_try_inverses(round_num, actual_trials)
            if not saturated and not self._skip_enrichment:
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
        enriched = [
            name
            for name, score in self._registry.compute_feature_enrichment()
            if score > 0 and name not in active_set
        ][:MAX_ENRICHMENT_FEATURES]
        if enriched:
            subsets.append(active_set | set(enriched))
        return subsets

    def _explore_round(self, round_num: int, n_trials: int) -> None:
        study_name = f"auto-{self._category}-r{round_num}-{uuid.uuid4().hex[:8]}"
        round_years = select_round_validation_years(
            round_num, self._validation_year_pool, self._blind_holdout_year
        )
        _logger.info(
            "round %d validation years: %s (blind holdout: %d)",
            round_num,
            round_years,
            self._blind_holdout_year,
        )
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
        )

    def _maybe_deploy(self) -> bool:
        """Deploy when the active entry beats the deployed one; return True if saturated.

        A True return signals the round loop that the search space is exhausted, so the
        per-round inverse and enrichment phases can be skipped — they cannot help once
        the registry is saturated.
        """
        if self._registry.is_saturated(SATURATION_LOOKBACK):
            _logger.info(
                "deploy skipped: registry saturated (last %d trials showed no improvement)",
                SATURATION_LOOKBACK,
            )
            return True
        active = self._registry.get_active_entry()
        if active is None:
            _logger.debug("no active entry — skipping deploy")
            return False
        deployed_ndcg = self._registry.get_deployed_ndcg()
        delta = active["ndcg_at_3"] - deployed_ndcg
        if delta < self._deploy_threshold:
            _logger.info(
                "deploy skipped: improvement below threshold  "
                "current: %.4f | deployed: %.4f | delta: %+.4f | gap to threshold: %.4f",
                active["ndcg_at_3"],
                deployed_ndcg,
                delta,
                self._deploy_threshold - delta,
            )
            return False
        _logger.info(
            "NDCG@3 improved by %+.4f (%.4f → %.4f) — triggering deploy",
            delta,
            deployed_ndcg,
            active["ndcg_at_3"],
        )
        blind_ndcg = self._evaluate_blind_holdout(active)
        blind_delta = blind_ndcg - deployed_ndcg
        if blind_delta < self._deploy_threshold:
            _logger.info(
                "deploy skipped: blind holdout %d did not confirm  "
                "blind ndcg: %.4f | deployed: %.4f | blind delta: %+.4f",
                self._blind_holdout_year,
                blind_ndcg,
                deployed_ndcg,
                blind_delta,
            )
            return False
        _logger.info(
            "blind holdout %d confirmed (%.4f, delta %+.4f) — proceeding to deploy",
            self._blind_holdout_year,
            blind_ndcg,
            blind_delta,
        )
        self._deploy(active)
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
        """Evaluate the active feature set per validation fold and log per-subgroup metrics.

        Predictions come from re-training the active set on each fold; the ground-truth
        side (incl. ``track_code`` / ``kyori`` used to derive subgroup keys) is taken
        from the full ``self._df`` rather than the feature-filtered fold, so subgrouping
        is unaffected by which features the active set happens to keep.
        """
        active = self._registry.get_active_entry()
        if active is None:
            _logger.info("subgroup diagnostics: no active entry — skipping")
            return
        feature_names = active["feature_names"]
        predictions = self._collect_active_predictions(feature_names)
        if predictions.is_empty():
            _logger.info("subgroup diagnostics: no predictions produced — skipping")
            return
        metrics = compute_subgroup_diagnostics(predictions, self._df)
        if not metrics:
            _logger.info("subgroup diagnostics: no subgroups to report")
            return
        _logger.info(
            "subgroup diagnostics (active set, %d features):", len(feature_names)
        )
        for m in metrics:
            _logger.info(
                "│  %-28s  races=%4d  ndcg@3=%.4f  top1=%.4f  top3_box=%.4f",
                m["subgroup"],
                m["race_count"],
                m["ndcg_at_3"],
                m["top1_accuracy"],
                m["top3_box_accuracy"],
            )

    def _collect_active_predictions(self, feature_names: list[str]) -> pl.DataFrame:
        """Train the active feature set on each validation fold and stack predictions.

        Returns a frame of (race_id, ketto_toroku_bango, predicted_rank) over all
        validation years; an empty frame when no fold yields predictions.
        """
        feature_set = set(feature_names)
        frames: list[pl.DataFrame] = []
        for year in self._validation_years:
            fold = split_walk_forward(self._df, self._train_start, year)
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
            del fold, fold_filtered
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
        description="Continuous Walk-Forward feature exploration and auto-deploy loop."
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
    parser.add_argument("--docker-build", action="store_true")
    parser.add_argument("--cf-deploy", action="store_true")
    parser.add_argument("--cf-deploy-dir", type=Path, default=None)
    parser.add_argument("--log-subgroup", action="store_true")
    parser.add_argument("--skip-inverse", action="store_true")
    parser.add_argument("--skip-enrichment", action="store_true")
    parser.add_argument("--auto-tune", dest="auto_tune", action="store_true", default=True)
    parser.add_argument("--no-auto-tune", dest="auto_tune", action="store_false")
    parser.add_argument("--per-trial-timeout", type=float, default=None)
    args = parser.parse_args(argv)
    setup_logging()

    category = str(args.category)
    train_start = (
        str(args.train_start)
        if args.train_start is not None
        else _CATEGORY_TRAIN_START.get(category, DEFAULT_TRAIN_START)
    )
    df = _load_features_dataframe(args.features_parquet, train_start)
    backends = _resolve_backends(args.backends, category)
    scripts_dir = Path(__file__).parent.parent

    load_controller = AdaptiveLoadController(
        base_n_trials=int(args.n_trials),
        min_n_trials=int(args.min_trials),
        max_n_trials=int(args.max_trials),
    )

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
            docker_build=bool(args.docker_build),
            cf_deploy=bool(args.cf_deploy),
            cf_deploy_dir=args.cf_deploy_dir,
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
        )
        _setup_signal_handler(learner)
        learner.run(max_rounds=args.max_rounds)


if __name__ == "__main__":
    main()
