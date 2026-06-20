"""Continuous self-improving Walk-Forward learning loop with auto-deploy."""

from __future__ import annotations

import argparse
import json
import shutil
import signal
import subprocess
import sys
import tempfile
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
from finish_position_lightgbm import META_COLUMNS

_LABEL_COLS: Final[frozenset[str]] = frozenset(
    {
        "finish_position",
        "finish_norm",
        "target_corner_1_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
    }
)

_TRAINING_SCRIPT: Final[dict[str, str]] = {
    "jra": "train_finish_position_catboost_walk_forward.py",
    "nar": "train_finish_position_xgboost_walk_forward.py",
    "ban-ei": "train_finish_position_catboost_walk_forward.py",
}

DEFAULT_DOCKER_TAG: Final[str] = "finish-position-predict-local:split2"
DEFAULT_DEPLOY_THRESHOLD: Final[float] = 0.005
DEFAULT_N_TRIALS: Final[int] = 20

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
    ) -> None:
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
        self._train_start: str = train_start
        self._deploy_threshold: float = deploy_threshold
        self._backends: tuple[ModelBackend, ...] = backends
        self._stop: bool = False

    def request_stop(self) -> None:
        self._stop = True

    def run(self, max_rounds: int | None = None) -> None:
        round_num = 0
        while not self._stop:
            if max_rounds is not None and round_num >= max_rounds:
                break
            self._explore_round(round_num)
            self._maybe_deploy()
            round_num += 1

    def _explore_round(self, round_num: int) -> None:
        study_name = f"auto-{self._category}-r{round_num}-{uuid.uuid4().hex[:8]}"
        run_exploration(
            df=self._df,
            registry=self._registry,
            study_name=study_name,
            n_trials=self._n_trials,
            validation_years=self._validation_years,
            train_start=self._train_start,
            backends=self._backends,
        )

    def _maybe_deploy(self) -> None:
        active = self._registry.get_active_entry()
        if active is None:
            return
        deployed_ndcg = self._registry.get_deployed_ndcg()
        if active["ndcg_at_3"] <= deployed_ndcg + self._deploy_threshold:
            return
        self._deploy(active)

    def _deploy(self, entry: FeatureEntry) -> None:
        feature_names = entry["feature_names"]
        model_version = self._make_model_version()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            filtered_parquet = write_filtered_parquet(
                self._df, feature_names, tmp_path / "parquet"
            )
            model_dir = self._train_production_model(
                filtered_parquet, tmp_path / "models", model_version
            )
            self._stage_model(model_dir, feature_names, model_version)
        self._update_model_meta_json(model_version, len(feature_names))
        self._rebuild_docker()
        self._registry.record_deployment(entry["ndcg_at_3"], len(feature_names))

    def _make_model_version(self) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"auto-{self._category}-{ts}"

    def _train_production_model(
        self, parquet_path: Path, model_root: Path, model_version: str
    ) -> Path:
        script_name = _TRAINING_SCRIPT.get(
            self._category, "train_finish_position_catboost_walk_forward.py"
        )
        year_to = max(self._validation_years)
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
        subprocess.run(cmd, check=True)
        return model_root / self._category / "iter0" / f"fold-{year_to}"

    def _stage_model(
        self, model_dir: Path, feature_names: list[str], model_version: str
    ) -> None:
        dest = self._repo_root / _CONTAINER_MODELS_ROOT / self._category / model_version
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(model_dir / "model.json", dest / "model.json")
        (dest / "metadata.json").write_text(
            json.dumps({"feature_names": feature_names}, ensure_ascii=False),
            encoding="utf-8",
        )

    def _update_model_meta_json(self, model_version: str, feature_count: int) -> None:
        json_path = self._repo_root / _MODEL_META_JSON_PATH
        if not json_path.exists():
            raise FileNotFoundError(f"model_meta.json not found: {json_path}")
        payload = json.loads(json_path.read_text(encoding="utf-8"))
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
        model_versions[self._category] = model_version
        feature_counts[self._category] = feature_count
        json_path.write_text(
            json.dumps(
                {"model_versions": model_versions, "feature_counts": feature_counts},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _rebuild_docker(self) -> None:
        dockerfile = (
            self._repo_root
            / "apps"
            / "finish-position-predict-container"
            / "Dockerfile"
        )
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
        )


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
    args = parser.parse_args(argv)

    df = pd.read_parquet(str(args.features_parquet))
    scripts_dir = Path(__file__).parent

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
        )
        _setup_signal_handler(learner)
        learner.run(max_rounds=args.max_rounds)


if __name__ == "__main__":
    main()
