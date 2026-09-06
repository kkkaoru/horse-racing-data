"""Accuracy evaluation for the six requested integration combinations."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import numpy as np

from .data import build_cell_queries, load_race_dataset, map_query_forecasts, subset
from .domain import (
    ACTION_WEIGHTS,
    BASELINE_ACTION_INDEX,
    EVALUATION_YEARS,
    ArmMetrics,
    ArmResult,
    ExperimentConfig,
    IntArray,
    IntegrationPattern,
    PretrainingMode,
    RaceDataset,
)
from .forecasting import (
    ScratchAutoregressiveForecaster,
    TemporalForecaster,
    TimesFm3Forecaster,
    resolve_timesfm_device,
)
from .routing import policy_backend, select_actions

REPORT_SCHEMA = "timesfm-finish-position-evaluation-v1"
MODEL_LICENSE = "timesfm-non-commercial-license-v1.0"


def _selected_winner_ranks(dataset: RaceDataset, actions: IntArray) -> IntArray:
    return dataset.winner_ranks[np.arange(dataset.rows), actions]


def _top123_utility(ranks: IntArray) -> np.ndarray[tuple[int], np.dtype[np.float64]]:
    cutoffs = np.arange(1, 4, dtype=np.int64)
    return (ranks[:, None] <= cutoffs).mean(axis=1)


def _clustered_delta_interval(
    *,
    dates: np.ndarray[tuple[int], np.dtype[np.str_]],
    selected: IntArray,
    baseline: IntArray,
    repetitions: int,
    seed: int,
) -> tuple[float, float]:
    if repetitions < 1:
        raise ValueError("bootstrap_repetitions must be positive")
    unique_dates = np.unique(dates)
    selected_utility = _top123_utility(selected)
    baseline_utility = _top123_utility(baseline)
    differences = selected_utility - baseline_utility
    date_sums = np.asarray(
        [float(np.sum(differences[dates == date])) for date in unique_dates], dtype=np.float64
    )
    date_counts = np.asarray([int(np.sum(dates == date)) for date in unique_dates], dtype=np.int64)
    rng = np.random.default_rng(seed)
    sampled = rng.integers(0, len(unique_dates), size=(repetitions, len(unique_dates)))
    sampled_sums = date_sums[sampled].sum(axis=1)
    sampled_counts = date_counts[sampled].sum(axis=1)
    deltas = sampled_sums / sampled_counts * 100.0
    low, high = np.quantile(deltas, [0.025, 0.975])
    return float(low), float(high)


def evaluate_actions(
    dataset: RaceDataset,
    actions: IntArray,
    *,
    bootstrap_repetitions: int,
    seed: int,
) -> ArmMetrics:
    """Evaluate selected blend actions against the deployed 0.50 baseline."""
    if actions.shape != (dataset.rows,):
        raise ValueError("actions must have one row per race")
    selected = _selected_winner_ranks(dataset, actions)
    baseline = dataset.winner_ranks[:, BASELINE_ACTION_INDEX]
    top_rates = tuple(float(np.mean(selected <= cutoff)) for cutoff in range(1, 6))
    selected_top123 = float(np.mean(_top123_utility(selected)))
    baseline_top123 = float(np.mean(_top123_utility(baseline)))
    low, high = _clustered_delta_interval(
        dates=dataset.race_dates,
        selected=selected,
        baseline=baseline,
        repetitions=bootstrap_repetitions,
        seed=seed,
    )
    return ArmMetrics(
        races=dataset.rows,
        top1=top_rates[0],
        top2=top_rates[1],
        top3=top_rates[2],
        top4=top_rates[3],
        top5=top_rates[4],
        top123_mean=selected_top123,
        mean_reciprocal_rank=float(np.mean(1.0 / selected)),
        mean_winner_rank=float(np.mean(selected)),
        baseline_top123_mean=baseline_top123,
        delta_top123_pp=(selected_top123 - baseline_top123) * 100.0,
        delta_ci95_low_pp=low,
        delta_ci95_high_pp=high,
        activation_rate=float(np.mean(actions != BASELINE_ACTION_INDEX)),
        mean_selected_weight=float(np.mean(ACTION_WEIGHTS[actions])),
    )


def _forecasters(config: ExperimentConfig) -> dict[PretrainingMode, TemporalForecaster]:
    device = resolve_timesfm_device(config.device)
    return {
        PretrainingMode.TIMESFM3: TimesFm3Forecaster(
            checkpoint=config.checkpoint,
            checkpoint_revision=config.checkpoint_revision,
            batch_size=config.batch_size,
            device=device,
        ),
        PretrainingMode.SCRATCH: ScratchAutoregressiveForecaster(lags=config.scratch_lags),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def run_experiment(config: ExperimentConfig) -> dict[str, object]:
    """Run all 3 x 2 arms over chronological 2024/2025/2026 outer folds."""
    dataset = load_race_dataset(config.input_path)
    forecasters = _forecasters(config)
    results: list[ArmResult] = []
    runtime_backends = {mode.value: forecaster.backend for mode, forecaster in forecasters.items()}
    for year in EVALUATION_YEARS:
        train = subset(dataset, dataset.race_years < year)
        evaluation = subset(dataset, dataset.race_years == year)
        if train.rows == 0 or evaluation.rows == 0:
            raise ValueError(f"year {year} lacks train or evaluation races")
        queries = build_cell_queries(train, evaluation, context_length=config.context_length)
        for mode, forecaster in forecasters.items():
            forecasts = forecaster.predict(queries.contexts, horizon=queries.horizon)
            temporal_gains = map_query_forecasts(queries, forecasts)
            for pattern in IntegrationPattern:
                actions = select_actions(
                    pattern,
                    train=train,
                    evaluation=evaluation,
                    temporal_gains=temporal_gains,
                )
                metrics = evaluate_actions(
                    evaluation,
                    actions,
                    bootstrap_repetitions=config.bootstrap_repetitions,
                    seed=config.seed + year,
                )
                results.append(
                    ArmResult(pattern=pattern, pretraining=mode, year=year, metrics=metrics)
                )
    return {
        "schema": REPORT_SCHEMA,
        "research_only": True,
        "production_integration": False,
        "checkpoint": config.checkpoint,
        "checkpoint_revision": config.checkpoint_revision,
        "checkpoint_license": MODEL_LICENSE,
        "pretraining_definition": {
            PretrainingMode.TIMESFM3.value: "official frozen TimesFM 3.0 checkpoint",
            PretrainingMode.SCRATCH.value: (
                "task-only pooled autoregression trained from prior-year race data; "
                "this is the no-foundation-pretraining ablation"
            ),
        },
        "runtime_backends": runtime_backends,
        "policy_optimizer_backend": policy_backend(),
        "input": {
            "path": str(config.input_path),
            "sha256": _sha256(config.input_path),
            "rows": dataset.rows,
            "years": {
                str(year): int(np.sum(dataset.race_years == year)) for year in range(2023, 2027)
            },
        },
        "chronology": "outer year uses only races strictly before that year",
        "results": [
            {
                "pattern": result.pattern.value,
                "pretraining": result.pretraining.value,
                "year": result.year,
                "metrics": asdict(result.metrics),
            }
            for result in results
        ],
    }


def write_report(report: dict[str, object], path: Path) -> None:
    """Write one deterministic JSON accuracy report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
