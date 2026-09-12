"""Fast Rustuna optimization for cached cell-level temporal forecasts."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
import rustuna

from .domain import FloatArray
from .lab_domain import LabStringArray


@dataclass(frozen=True)
class FoldForecastSurface:
    """Cached TimesFM components and market baseline for one cell-year."""

    profile: str
    strategy: str
    year: int
    value_columns: tuple[str, ...]
    timesfm_components: FloatArray
    market_scores: FloatArray
    race_ids: LabStringArray
    horse_ids: LabStringArray
    finish_positions: np.ndarray[tuple[int], np.dtype[np.int64]]
    history_counts: np.ndarray[tuple[int], np.dtype[np.int64]]

    def __post_init__(self) -> None:
        rows = len(self.race_ids)
        if self.timesfm_components.shape != (rows, len(self.value_columns)):
            raise ValueError("TimesFM components must align with value columns")
        if (
            self.market_scores.shape != (rows,)
            or self.horse_ids.shape != (rows,)
            or self.finish_positions.shape != (rows,)
            or self.history_counts.shape != (rows,)
        ):
            raise ValueError("forecast surface columns must align")
        if not bool(np.isfinite(self.timesfm_components).all()) or not bool(
            np.isfinite(self.market_scores).all()
        ):
            raise ValueError("forecast surfaces must be finite")

    @property
    def key(self) -> str:
        """Return the independently tunable profile/scope key."""
        return f"{self.profile}__{self.strategy}"


@dataclass(frozen=True)
class SurfaceMetrics:
    """Top1-Top5 counts against the market baseline."""

    race_count: int
    model_hits: tuple[int, int, int, int, int]
    market_hits: tuple[int, int, int, int, int]

    @property
    def delta_hits(self) -> tuple[int, int, int, int, int]:
        """Return model-minus-market Top1-Top5 counts."""
        return cast(
            tuple[int, int, int, int, int],
            tuple(
                model - market
                for model, market in zip(self.model_hits, self.market_hits, strict=True)
            ),
        )


@dataclass(frozen=True)
class RustunaCellResult:
    """Auditable best parameters and local optimization throughput."""

    study_name: str
    n_trials: int
    elapsed_seconds: float
    trials_per_second: float
    best_value: float
    best_params: dict[str, float | int | str | bool | None]
    feasible_timesfm_trial_count: int
    improving_timesfm_trial_count: int
    best_feasible_timesfm_value: float | None
    best_feasible_timesfm_params: dict[str, float | int | str | bool | None] | None


def _ranks(scores: FloatArray, race_ids: LabStringArray, horse_ids: LabStringArray) -> np.ndarray:
    result = np.zeros(len(scores), dtype=np.int64)
    for race_id in sorted(set(race_ids)):
        indices = np.flatnonzero(race_ids == race_id)
        order = sorted(indices, key=lambda index: (-float(scores[index]), str(horse_ids[index])))
        for rank, index in enumerate(order, start=1):
            result[index] = rank
    return result


def evaluate_surface(
    surface: FoldForecastSurface,
    *,
    component_weights: Mapping[str, float],
    market_weight: float,
    minimum_history_count: int = 1,
) -> SurfaceMetrics:
    """Evaluate one cached forecast readout without rerunning TimesFM."""
    if not 0.0 <= market_weight <= 1.0:
        raise ValueError("market_weight must be between zero and one")
    if minimum_history_count < 1:
        raise ValueError("minimum_history_count must be positive")
    weights = np.asarray(
        [max(0.0, float(component_weights.get(column, 0.0))) for column in surface.value_columns],
        dtype=np.float64,
    )
    if not bool(np.any(weights > 0)):
        weights[:] = 1.0
    weights /= np.sum(weights)
    temporal = surface.timesfm_components @ weights
    scores = surface.market_scores.copy()
    eligible = surface.history_counts >= minimum_history_count
    scores[eligible] = (1.0 - market_weight) * temporal[
        eligible
    ] + market_weight * surface.market_scores[eligible]
    model_ranks = _ranks(scores, surface.race_ids, surface.horse_ids)
    market_ranks = _ranks(surface.market_scores, surface.race_ids, surface.horse_ids)
    winner = surface.finish_positions == 1
    model_hits = cast(
        tuple[int, int, int, int, int],
        tuple(int(np.sum(winner & (model_ranks <= depth))) for depth in range(1, 6)),
    )
    market_hits = cast(
        tuple[int, int, int, int, int],
        tuple(int(np.sum(winner & (market_ranks <= depth))) for depth in range(1, 6)),
    )
    return SurfaceMetrics(len(set(surface.race_ids)), model_hits, market_hits)


def _objective_value(metrics: Sequence[SurfaceMetrics]) -> float:
    deltas = tuple(sum(metric.delta_hits[index] for metric in metrics) for index in range(5))
    races = sum(metric.race_count for metric in metrics)
    if races == 0:
        raise ValueError("optimization has no races")
    return float(
        deltas[0] * 1_000_000_000
        + deltas[1] * 1_000_000
        + deltas[2] * 1_000
        + deltas[3]
        + deltas[4] / (races + 1)
    )


def optimize_cell_surfaces(
    surfaces: Sequence[FoldForecastSurface],
    *,
    n_trials: int,
    seed: int,
    study_name: str,
    storage_path: Path | None = None,
) -> RustunaCellResult:
    """Tune a cell readout with Rust TPE after expensive forecasts are cached."""
    if n_trials < 1:
        raise ValueError("n_trials must be positive")
    by_key: dict[str, list[FoldForecastSurface]] = {}
    for surface in surfaces:
        by_key.setdefault(surface.key, []).append(surface)
    if not by_key:
        raise ValueError("at least one forecast surface is required")
    value_columns = sorted({column for surface in surfaces for column in surface.value_columns})
    storage = (
        rustuna.storages.SQLite3Storage(str(storage_path), create_database=True)
        if storage_path is not None
        else None
    )
    study = rustuna.create_study(
        study_name=study_name,
        storage=storage,
        sampler=rustuna.samplers.TPESampler(seed=seed),
        direction="maximize",
        load_if_exists=storage is not None,
    )

    profile_choices = cast(list[float | int | str | bool | None], sorted(by_key))

    def objective(trial: rustuna.Trial) -> float:
        key = str(trial.suggest_categorical("profile_scope", profile_choices))
        market_weight = cast(
            float,
            trial.suggest_categorical(
                "market_weight",
                [
                    0.0,
                    0.01,
                    0.025,
                    0.05,
                    0.075,
                    0.1,
                    0.15,
                    0.2,
                    0.25,
                    0.3,
                    0.35,
                    0.4,
                    0.45,
                    0.5,
                    0.55,
                    0.6,
                    0.65,
                    0.7,
                    0.75,
                    0.8,
                    0.85,
                    0.9,
                    0.925,
                    0.95,
                    0.975,
                    0.99,
                    1.0,
                ],
            ),
        )
        minimum_history_count = cast(
            int, trial.suggest_categorical("minimum_history_count", [1, 2, 3, 5, 8])
        )
        weights = {
            column: trial.suggest_float(f"weight_{column}", 0.0, 1.0) for column in value_columns
        }
        metrics = [
            evaluate_surface(
                surface,
                component_weights=weights,
                market_weight=market_weight,
                minimum_history_count=minimum_history_count,
            )
            for surface in by_key[key]
        ]
        total_place_violation = 0
        for metric, surface in zip(metrics, by_key[key], strict=True):
            for depth, delta in enumerate(metric.delta_hits[1:], start=2):
                violation = max(0, -delta)
                total_place_violation += violation
                trial.set_constraint(f"{surface.year}-top{depth}", float(violation))
        if total_place_violation:
            return -1_000_000_000_000_000.0 - total_place_violation
        return _objective_value(metrics)

    before = len(study.trials)
    started = time.perf_counter()
    study.optimize(objective, n_trials=n_trials)
    elapsed = time.perf_counter() - started
    completed = len(study.trials) - before
    best = study.best_trial
    if best.value is None:
        raise RuntimeError("Rustuna best trial has no objective value")
    best_value = float(best.value)
    best_params: dict[str, float | int | str | bool | None] = dict(best.params)
    feasible_timesfm_trials = [
        trial
        for trial in study.trials
        if trial.value is not None
        and float(trial.value) > -1_000_000_000_000_000.0
        and float(cast(float, trial.params.get("market_weight", 1.0))) < 1.0
    ]
    improving_timesfm_trials = [
        trial for trial in feasible_timesfm_trials if float(cast(float, trial.value)) > 0.0
    ]
    best_timesfm = (
        max(feasible_timesfm_trials, key=lambda trial: float(cast(float, trial.value)))
        if feasible_timesfm_trials
        else None
    )
    if best_timesfm is not None and float(cast(float, best_timesfm.value)) > best_value:
        best_value = float(cast(float, best_timesfm.value))
        best_params = dict(best_timesfm.params)
    if best_value <= 0.0:
        best_value = 0.0
        best_params = {
            "profile_scope": sorted(by_key)[0],
            "market_only": True,
            "market_weight": 1.0,
        }
    return RustunaCellResult(
        study_name=study_name,
        n_trials=completed,
        elapsed_seconds=elapsed,
        trials_per_second=completed / elapsed if elapsed > 0 else 0.0,
        best_value=best_value,
        best_params=best_params,
        feasible_timesfm_trial_count=len(feasible_timesfm_trials),
        improving_timesfm_trial_count=len(improving_timesfm_trials),
        best_feasible_timesfm_value=(
            float(cast(float, best_timesfm.value)) if best_timesfm is not None else None
        ),
        best_feasible_timesfm_params=(
            dict(best_timesfm.params) if best_timesfm is not None else None
        ),
    )
