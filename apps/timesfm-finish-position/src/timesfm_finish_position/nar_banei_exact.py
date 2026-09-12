"""Rustuna readout search for exact positions, not winner-in-TopK recall."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import cast

import numpy as np
import polars as pl
import rustuna
from numpy.typing import NDArray


@dataclass(frozen=True)
class Readout:
    profile: str
    origin: str = "timesfm"
    temporal_weight: float = 0.0
    speed_weight: float = 0.0
    minimum_history: int = 1
    normalization: str = "rank"
    half_life_days: int = 0

    def __post_init__(self) -> None:
        if self.origin not in ("timesfm", "last", "mean5"):
            raise ValueError("Unsupported forecast origin")
        if not 0 <= self.temporal_weight <= 1 or not 0 <= self.speed_weight <= 1:
            raise ValueError("Readout weights must be in [0, 1]")
        if self.half_life_days < 0:
            raise ValueError("half_life_days must be nonnegative")
        if self.minimum_history < 1:
            raise ValueError("minimum_history must be positive")
        if self.normalization not in ("rank", "centered", "innovation"):
            raise ValueError("Unsupported normalization")


def predicted_ranks(frame: pl.DataFrame, scores: NDArray[np.float64]) -> NDArray[np.int64]:
    if len(scores) != frame.height or not bool(np.isfinite(scores).all()):
        raise ValueError("Invalid scores")
    if frame.is_empty():
        return np.empty(0, dtype=np.int64)
    groups = np.asarray(frame["race_id"].to_numpy(), dtype=str)
    horses = np.asarray(frame["horse_number"].to_numpy(), dtype=np.int64)
    order = np.lexsort((horses, -scores, groups))
    ordered_groups = groups[order]
    starts = np.maximum.accumulate(
        np.where(np.r_[True, ordered_groups[1:] != ordered_groups[:-1]], np.arange(len(order)), 0)
    )
    ranks = np.empty(len(order), dtype=np.int64)
    ranks[order] = np.arange(len(order)) - starts + 1
    return ranks


def exact_hits(frame: pl.DataFrame, scores: NDArray[np.float64]) -> NDArray[np.int64]:
    finish = np.asarray(frame["finish"].to_numpy(), dtype=np.int64)
    if bool(np.any(finish < 1)):
        raise ValueError("Evaluation requires observed positive finish positions")
    ranks = predicted_ranks(frame, scores)
    correct = finish[(ranks == finish) & (finish <= 5)]
    return np.asarray(np.bincount(correct, minlength=6)[1:6], dtype=np.int64)


def normalized_components(
    frame: pl.DataFrame, *, origin: str, minimum_history: int, normalization: str = "rank"
) -> NDArray[np.float64]:
    """Average ties; unknown horses and fields with fewer than two known fall back."""
    columns = [
        f"{origin}_{name}"
        for name in ("performance", "relative_speed", "day_speed", "body_weight")
        if f"{origin}_{name}" in frame.columns
    ]
    if not columns or len(columns) > 2:
        raise ValueError("No forecast components or unsupported component count")
    if normalization not in ("rank", "centered", "innovation"):
        raise ValueError("Unsupported normalization")
    arrays: list[NDArray[np.float64]] = []
    for column in columns:
        count_name = f"history_count_{column.removeprefix(origin + '_')}"
        history_count = pl.col(count_name if count_name in frame.columns else "history_count")
        available = pl.col(column)
        if normalization == "innovation":
            anchor = f"last_{column.removeprefix(origin + '_')}"
            if anchor not in frame.columns:
                raise ValueError("Innovation readout requires last-observation anchors")
            available = available - pl.col(anchor)
        work = frame.with_columns(
            pl.when((history_count >= minimum_history) & available.is_finite())
            .then(available)
            .otherwise(None)
            .alias("available")
        )
        count = pl.col("available").count().over("race_id")
        unit = (
            count - pl.col("available").rank(method="average", descending=True).over("race_id")
        ) / (count - 1).clip(lower_bound=1)
        if normalization == "innovation":
            change = (
                pl.col("available") - pl.col("available").mean().over("race_id")
                if column.endswith("_performance")
                else unit - 0.5
            )
            unit = pl.col("baseline_score") + change
        if normalization == "centered" and column.endswith("_performance"):
            baseline_center = (
                pl.when(pl.col("available").is_not_null())
                .then(pl.col("baseline_score"))
                .otherwise(None)
                .mean()
                .over("race_id")
            )
            unit = (
                pl.col("available") - pl.col("available").mean().over("race_id") + baseline_center
            )
        array = work.select(
            pl.when(count > 1)
            .then(unit)
            .otherwise(None)
            .fill_null(pl.col("baseline_score"))
            .alias("unit")
        )["unit"].to_numpy()
        arrays.append(np.asarray(array, dtype=np.float64))
    return np.column_stack(arrays)


def recency_weights(frame: pl.DataFrame, half_life_days: int) -> NDArray[np.float64]:
    """Decay from the actual query prefix, including frozen-year profiles."""
    if half_life_days <= 0 or "latest_history_date" not in frame.columns:
        raise ValueError("Recency requires a positive half-life and latest_history_date")
    ages = np.asarray(
        frame.select(
            (
                pl.col("race_date").str.strptime(pl.Date, "%Y%m%d")
                - pl.col("latest_history_date").str.strptime(pl.Date, "%Y%m%d", strict=False)
            ).dt.total_days()
        )
        .to_series()
        .to_numpy(),
        dtype=np.float64,
    )
    known = np.asarray(frame["history_count"].to_numpy(), dtype=np.int64) > 0
    if bool(np.any(known & (~np.isfinite(ages) | (ages <= 0)))):
        raise ValueError("Known histories require finite strictly prior dates")
    return np.exp2(-np.where(known, ages, np.inf) / half_life_days)


def readout_scores(
    frame: pl.DataFrame,
    config: Readout,
    components: NDArray[np.float64] | None = None,
    *,
    recency: NDArray[np.float64] | None = None,
) -> NDArray[np.float64]:
    baseline = np.asarray(frame["baseline_score"].to_numpy(), dtype=np.float64)
    if config.temporal_weight == 0:
        return baseline
    matrix = (
        normalized_components(
            frame,
            origin=config.origin,
            minimum_history=config.minimum_history,
            normalization=config.normalization,
        )
        if components is None
        else components
    )
    temporal = matrix[:, 0]
    if matrix.shape[1] > 1:
        temporal = (1 - config.speed_weight) * temporal + config.speed_weight * matrix[:, 1]
    if config.half_life_days:
        decay = recency_weights(frame, config.half_life_days) if recency is None else recency
        return baseline + config.temporal_weight * decay * (temporal - baseline)
    return (1 - config.temporal_weight) * baseline + config.temporal_weight * temporal


def optimize_readout(
    development: Mapping[str, Sequence[pl.DataFrame]],
    *,
    origin: str,
    evaluation_year: int,
    n_trials: int,
    seed: int,
    normalizations: tuple[str, ...] = ("rank",),
    half_lives: tuple[int, ...] = (0,),
) -> tuple[Readout, list[dict[str, object]]]:
    """Use earlier years only; preserve an explicit feasible unchanged control."""
    if not development or n_trials < 1:
        raise ValueError("Development profiles and positive trial budget are required")
    if not normalizations:
        raise ValueError("At least one normalization is required")
    if not half_lives or any(value < 0 for value in half_lives):
        raise ValueError("Nonnegative half-lives are required")
    profiles = sorted(development)
    signatures: list[pl.DataFrame] = []
    prepared: dict[tuple[str, int, str], list[NDArray[np.float64]]] = {}
    baselines: dict[str, NDArray[np.int64]] = {}
    recencies: dict[tuple[str, int], list[NDArray[np.float64] | None]] = {}
    for profile, frames in development.items():
        if not frames or any(frame.is_empty() for frame in frames):
            raise ValueError("Development folds must not be empty")
        joined = pl.concat(frames)
        if joined.filter(
            (pl.col("year") >= evaluation_year)
            | (pl.col("race_date") >= f"{evaluation_year}0101")
            | (pl.col("year").cast(pl.String) != pl.col("race_date").str.slice(0, 4))
        ).height:
            raise ValueError("HPO cannot consume evaluation-year or future outcomes")
        if joined.select("race_id", "horse_id").is_duplicated().any():
            raise ValueError("Duplicate development entrants")
        signatures.append(
            joined.select(
                "race_id",
                "horse_id",
                "race_date",
                "horse_number",
                "year",
                "finish",
                "baseline_score",
            ).sort("race_id", "horse_id")
        )
        baselines[profile] = sum(
            (
                exact_hits(frame, np.asarray(frame["baseline_score"].to_numpy(), dtype=np.float64))
                for frame in frames
            ),
            start=np.zeros(5, dtype=np.int64),
        )
        for half in half_lives:
            recencies[profile, half] = [
                recency_weights(frame, half) if half else None for frame in frames
            ]
        for minimum in (1, 2, 4, 8):
            for normalization in normalizations:
                prepared[profile, minimum, normalization] = [
                    normalized_components(
                        frame, origin=origin, minimum_history=minimum, normalization=normalization
                    )
                    for frame in frames
                ]
    if any(not frame.equals(signatures[0]) for frame in signatures[1:]):
        raise ValueError("Profiles must have identical development cohorts and controls")
    default_normalization = normalizations[0]
    default_half_life = half_lives[0]
    best = Readout(profiles[0], origin, normalization=default_normalization)
    best_value = 0.0
    records: list[dict[str, object]] = []
    study = rustuna.create_study(
        sampler=rustuna.samplers.TPESampler(seed=seed), direction="maximize"
    )

    def objective(trial: rustuna.Trial) -> float:
        nonlocal best, best_value
        profile = str(
            trial.suggest_categorical(
                "profile", cast(list[float | int | str | bool | None], list(profiles))
            )
        )
        minimum = cast(int, trial.suggest_categorical("minimum_history", [1, 2, 4, 8]))
        normalization = (
            str(
                trial.suggest_categorical(
                    "normalization",
                    cast(list[float | int | str | bool | None], list(normalizations)),
                )
            )
            if len(normalizations) > 1
            else default_normalization
        )
        weight = trial.suggest_float("temporal_weight", 0.0, 1.0)
        speed = (
            trial.suggest_float("speed_weight", 0.0, 1.0)
            if prepared[profile, minimum, normalization][0].shape[1] > 1
            else 0.0
        )
        half = (
            int(
                str(
                    trial.suggest_categorical(
                        "half_life_days", [str(value) for value in half_lives]
                    )
                )
            )
            if len(half_lives) > 1
            else default_half_life
        )
        config = Readout(profile, origin, weight, speed, minimum, normalization, half)
        hits = np.zeros(5, dtype=np.int64)
        for frame, components, decay in zip(
            development[profile],
            prepared[profile, minimum, normalization],
            recencies[profile, half],
            strict=True,
        ):
            hits += exact_hits(frame, readout_scores(frame, config, components, recency=decay))
        delta = hits - baselines[profile]
        feasible = bool(np.all(delta >= 0))
        value = float(delta.sum()) if feasible else -1.0 - float(np.maximum(-delta, 0).sum())
        records.append(
            {
                "parameters": asdict(config),
                "hits": hits.tolist(),
                "baseline_hits": baselines[profile].tolist(),
                "delta_hits": delta.tolist(),
                "feasible": feasible,
                "objective": value,
            }
        )
        if feasible and value > best_value:
            best, best_value = config, value
        return value

    study.optimize(objective, n_trials=n_trials)
    return best, records
