"""Continuous self-improving Walk-Forward learning loop with auto-deploy."""

from __future__ import annotations

import argparse
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
from typing import Final

import pandas as pd

from feature_explorer import (
    DEFAULT_BACKENDS,
    DEFAULT_TRAIN_START,
    DEFAULT_VALIDATION_YEARS,
    ModelBackend,
    run_exploration,
)
from feature_registry import FeatureEntry, FeatureRegistry
from finish_position_lightgbm import LABEL_COLUMNS, META_COLUMNS
from walk_forward_common import atomic_write_metadata

try:
    import psutil as _psutil

    _PSUTIL_AVAILABLE: bool = True
except ImportError:
    _psutil = None  # type: ignore[assignment]
    _PSUTIL_AVAILABLE = False

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

DEFAULT_DOCKER_TAG: Final[str] = "finish-position-predict-local:split2"
DEFAULT_DEPLOY_THRESHOLD: Final[float] = 0.005
DEFAULT_N_TRIALS: Final[int] = 20
DEFAULT_DOCKER_BUILD_TIMEOUT_S: Final[int] = 3600
DEFAULT_TRAINING_TIMEOUT_S: Final[int] = 7200

_CONTAINER_MODELS_ROOT: Final[str] = (
    "apps/finish-position-predict-container/models/finish-position"
)
_MODEL_META_JSON_PATH: Final[str] = (
    "apps/finish-position-predict-container/src/predict_lib/model_meta.json"
)


def write_filtered_parquet(
    df: pd.DataFrame, feature_names: list[str], output_dir: Path
) -> Path:
    keep = set(META_COLUMNS) | _LABEL_COLS | set(feature_names)
    cols = [c for c in df.columns if c in keep]
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "features.parquet"
    df[cols].to_parquet(out, index=False)
    return out


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
        self._base_n_trials = base_n_trials
        self._min_n_trials = min_n_trials
        self._max_n_trials = max_n_trials
        self._cpu_high_pct = cpu_high_pct
        self._cpu_low_pct = cpu_low_pct
        self._mem_high_pct = mem_high_pct
        self._mem_low_pct = mem_low_pct

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
        """psutil.cpu_percent(interval=0.5). Returns 0.0 if psutil not installed."""
        if not _PSUTIL_AVAILABLE:
            return 0.0
        return float(_psutil.cpu_percent(interval=0.5))

    def _mem_percent(self) -> float:
        """psutil.virtual_memory().percent. Returns 0.0 if psutil not installed."""
        if not _PSUTIL_AVAILABLE:
            return 0.0
        return float(_psutil.virtual_memory().percent)


class ContinuousLearner:
    def __init__(
        self,
        registry: FeatureRegistry,
        df: pd.DataFrame,
        category: str,
        repo_root: Path,
        scripts_dir: Path,
        docker_image_tag: str = DEFAULT_DOCKER_TAG,
        n_trials_per_round: int = DEFAULT_N_TRIALS,
        validation_years: list[int] | None = None,
        train_start: str = DEFAULT_TRAIN_START,
        deploy_threshold: float = DEFAULT_DEPLOY_THRESHOLD,
        backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
        load_controller: AdaptiveLoadController | None = None,
    ) -> None:
        if category not in _TRAINING_SCRIPT:
            raise ValueError(
                f"Unknown category {category!r}. Valid categories: {sorted(_TRAINING_SCRIPT)}"
            )
        self._registry: FeatureRegistry = registry
        self._df: pd.DataFrame = df
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
        self._train_start: str = train_start
        self._deploy_threshold: float = deploy_threshold
        self._backends: tuple[ModelBackend, ...] = backends
        self._stop: bool = False
        self._load_controller: AdaptiveLoadController | None = load_controller

    def request_stop(self) -> None:
        self._stop = True

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
            self._maybe_deploy()
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

    def _explore_round(self, round_num: int, n_trials: int) -> None:
        study_name = f"auto-{self._category}-r{round_num}-{uuid.uuid4().hex[:8]}"
        run_exploration(
            df=self._df,
            registry=self._registry,
            study_name=study_name,
            n_trials=n_trials,
            validation_years=self._validation_years,
            train_start=self._train_start,
            backends=self._backends,
        )

    def _maybe_deploy(self) -> None:
        active = self._registry.get_active_entry()
        if active is None:
            _logger.debug("no active entry — skipping deploy")
            return
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
            return
        _logger.info(
            "NDCG@3 improved by %+.4f (%.4f → %.4f) — triggering deploy",
            delta,
            deployed_ndcg,
            active["ndcg_at_3"],
        )
        self._deploy(active)

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
            _logger.info("│  [5/5] rebuilding Docker image ...")
            self._rebuild_docker()
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
    args = parser.parse_args(argv)
    setup_logging()

    df = pd.read_parquet(str(args.features_parquet))
    scripts_dir = Path(__file__).parent

    load_controller = AdaptiveLoadController(
        base_n_trials=int(args.n_trials),
        min_n_trials=int(args.min_trials),
        max_n_trials=int(args.max_trials),
    )

    with FeatureRegistry(args.registry_path) as registry:
        learner = ContinuousLearner(
            registry=registry,
            df=df,
            category=str(args.category),
            repo_root=args.repo_root,
            scripts_dir=scripts_dir,
            docker_image_tag=str(args.docker_tag),
            n_trials_per_round=int(args.n_trials),
            deploy_threshold=float(args.deploy_threshold),
            load_controller=load_controller,
        )
        _setup_signal_handler(learner)
        learner.run(max_rounds=args.max_rounds)


if __name__ == "__main__":
    main()
