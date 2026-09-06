from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pytest

import timesfm_finish_position.evaluation as subject
from timesfm_finish_position.data import load_race_dataset, subset
from timesfm_finish_position.domain import (
    ExperimentConfig,
    FloatArray,
    PretrainingMode,
    RaceDataset,
)
from timesfm_finish_position.evaluation import evaluate_actions, run_experiment, write_report
from timesfm_finish_position.forecasting import TemporalForecaster


class FakeForecaster:
    def __init__(self, value: float, backend: str) -> None:
        self.value = value
        self._backend = backend

    @property
    def backend(self) -> str:
        return self._backend

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        return tuple(np.full((21, horizon), self.value, dtype=np.float64) for _ in contexts)


def fake_forecasters(config: ExperimentConfig) -> dict[PretrainingMode, TemporalForecaster]:
    del config
    return {
        PretrainingMode.TIMESFM3: FakeForecaster(0.1, "fake-mps"),
        PretrainingMode.SCRATCH: FakeForecaster(0.2, "fake-mlx"),
    }


def test_evaluate_actions_reports_accuracy_and_baseline(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    evaluation = subset(dataset, dataset.race_years == 2025)
    actions = np.zeros(evaluation.rows, dtype=np.int64)
    metrics = evaluate_actions(evaluation, actions, bootstrap_repetitions=50, seed=7)
    assert metrics.races == 10
    assert metrics.top1 == 0.1
    assert metrics.top5 == 0.5
    assert metrics.activation_rate == 1.0
    assert metrics.mean_selected_weight == 0.0


def test_evaluate_actions_rejects_wrong_shape(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    evaluation = subset(dataset, dataset.race_years == 2025)
    with pytest.raises(ValueError, match="actions must have one row per race"):
        evaluate_actions(evaluation, np.zeros(9, dtype=np.int64), bootstrap_repetitions=10, seed=7)


def test_evaluate_actions_rejects_zero_bootstrap_repetitions(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    evaluation = subset(dataset, dataset.race_years == 2025)
    with pytest.raises(ValueError, match="bootstrap_repetitions must be positive"):
        evaluate_actions(evaluation, np.zeros(10, dtype=np.int64), bootstrap_repetitions=0, seed=7)


def test_run_experiment_emits_all_eighteen_year_arms(
    race_parquet: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(subject, "_forecasters", fake_forecasters)
    config = ExperimentConfig(
        input_path=race_parquet,
        output_path=tmp_path / "report.json",
        context_length=32,
        bootstrap_repetitions=10,
    )
    report = run_experiment(config)
    results = report["results"]
    assert isinstance(results, list)
    assert len(results) == 18
    assert report["schema"] == "timesfm-finish-position-evaluation-v1"
    assert report["research_only"] is True
    assert report["production_integration"] is False
    assert report["runtime_backends"] == {
        "timesfm3-pretrained": "fake-mps",
        "scratch": "fake-mlx",
    }
    input_summary = report["input"]
    assert isinstance(input_summary, dict)
    assert input_summary["years"] == {"2023": 10, "2024": 10, "2025": 10, "2026": 10}


def test_run_experiment_rejects_missing_outer_year(
    race_parquet: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = load_race_dataset(race_parquet)
    truncated = subset(original, original.race_years < 2026)

    def load_truncated(path: Path) -> RaceDataset:
        del path
        return truncated

    monkeypatch.setattr(subject, "load_race_dataset", load_truncated)
    monkeypatch.setattr(subject, "_forecasters", fake_forecasters)
    config = ExperimentConfig(
        input_path=race_parquet,
        output_path=tmp_path / "report.json",
        context_length=32,
        bootstrap_repetitions=10,
    )
    with pytest.raises(ValueError, match="year 2026 lacks train or evaluation races"):
        run_experiment(config)


def test_write_report_creates_deterministic_json(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "report.json"
    write_report({"schema": "test", "value": 1}, path)
    assert json.loads(path.read_text(encoding="utf-8")) == {"schema": "test", "value": 1}
    assert path.read_text(encoding="utf-8").endswith("\n")
