"""Nested walk-forward evaluation for tree ranking models."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .domain import FloatArray
from .lab_domain import LabIntArray, LabStringArray, PredictionFrame, ProbabilityMetrics
from .lab_metrics import evaluate_prediction_frame
from .tree_rankers import RankerKind, fit_ranker

IDENTITY_COLUMNS = ("race_id", "race_date", "horse_id")
OUTCOME_COLUMNS = ("finish_position", "decimal_odds")
NON_FEATURE_COLUMNS = frozenset((*IDENTITY_COLUMNS, *OUTCOME_COLUMNS))
TEMPERATURE_GRID = np.geomspace(0.05, 10.0, 81)


class ArrowColumnLike(Protocol):
    """Narrow column conversion boundary."""

    def to_numpy(self, *, zero_copy_only: bool) -> object:
        """Return an array-like value."""
        ...

    def to_pylist(self) -> list[object]:
        """Return scalar values."""
        ...


class ArrowTableLike(Protocol):
    """Narrow table boundary independent of incomplete PyArrow stubs."""

    @property
    def column_names(self) -> list[str]:
        """Return ordered names."""
        ...

    def column(self, name: str) -> ArrowColumnLike:
        """Return one column."""
        ...


@dataclass(frozen=True)
class TabularDataset:
    """In-memory compact PIT frame."""

    race_ids: LabStringArray
    race_dates: LabStringArray
    horse_ids: LabStringArray
    finish_positions: LabIntArray
    decimal_odds: FloatArray
    features: FloatArray
    feature_names: tuple[str, ...]


@dataclass(frozen=True)
class TreeFoldResult:
    """One model/year result with nested calibration."""

    model: str
    year: int
    train_rows: int
    calibration_rows: int
    test_rows: int
    temperature: float
    metrics: ProbabilityMetrics


def _arrow_string(table: ArrowTableLike, name: str) -> LabStringArray:
    return np.asarray(table.column(name).to_pylist(), dtype=np.str_)


def _arrow_numeric(table: ArrowTableLike, name: str, dtype: np.dtype) -> np.ndarray:
    return np.asarray(table.column(name).to_numpy(zero_copy_only=False), dtype=dtype)


def load_tabular_dataset(path: Path) -> TabularDataset:
    """Load only numeric PIT features and aligned outcomes."""
    table = pq.read_table(path)
    feature_names = tuple(name for name in table.column_names if name not in NON_FEATURE_COLUMNS)
    if not feature_names:
        raise ValueError("tabular dataset has no model features")
    features = np.column_stack(
        [_arrow_numeric(table, name, np.dtype(np.float64)) for name in feature_names]
    )
    return TabularDataset(
        race_ids=_arrow_string(table, "race_id"),
        race_dates=_arrow_string(table, "race_date"),
        horse_ids=_arrow_string(table, "horse_id"),
        finish_positions=_arrow_numeric(table, "finish_position", np.dtype(np.int64)).astype(
            np.int64
        ),
        decimal_odds=_arrow_numeric(table, "decimal_odds", np.dtype(np.float64)),
        features=features.astype(np.float64),
        feature_names=feature_names,
    )


def fit_imputation(train: FloatArray) -> FloatArray:
    """Fit train-only medians and use zero for entirely missing columns."""
    medians = [
        float(np.median(column[np.isfinite(column)])) if np.any(np.isfinite(column)) else 0.0
        for column in train.T
    ]
    return np.asarray(medians, dtype=np.float64)


def apply_imputation(features: FloatArray, medians: FloatArray) -> FloatArray:
    """Apply frozen train medians without mutating source data."""
    if features.ndim != 2 or medians.shape != (features.shape[1],):
        raise ValueError("imputation dimensions do not match")
    return np.where(np.isfinite(features), features, medians[None, :]).astype(np.float64)


def scores_to_probabilities(
    scores: FloatArray, race_ids: LabStringArray, *, temperature: float
) -> FloatArray:
    """Convert ranking scores into a separate winner simplex for each race."""
    if scores.shape != race_ids.shape:
        raise ValueError("scores and race_ids must align")
    if temperature <= 0.0:
        raise ValueError("temperature must be positive")
    result = np.zeros_like(scores, dtype=np.float64)
    starts = np.flatnonzero(np.r_[True, race_ids[1:] != race_ids[:-1]])
    ends = np.r_[starts[1:], len(scores)]
    for start, end in zip(starts, ends, strict=True):
        scaled = scores[start:end] / temperature
        exponential = np.exp(scaled - np.max(scaled))
        result[start:end] = exponential / np.sum(exponential)
    return result


def fit_temperature(scores: FloatArray, race_ids: LabStringArray, finish: LabIntArray) -> float:
    """Select temperature only on the nested prior-year calibration partition."""
    labels = (finish == 1).astype(np.float64)
    losses = []
    for temperature in TEMPERATURE_GRID:
        probabilities = scores_to_probabilities(scores, race_ids, temperature=float(temperature))
        losses.append(-float(np.sum(labels * np.log(np.clip(probabilities, 1e-12, 1.0)))))
    return float(TEMPERATURE_GRID[int(np.argmin(losses))])


def nested_masks(
    race_dates: LabStringArray, year: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Use <=Y-2 for model fit, Y-1 for calibration, and Y for evaluation."""
    calibration_year = str(year - 1)
    evaluation_year = str(year)
    years = np.asarray([value[:4] for value in race_dates], dtype=np.str_)
    train = years < calibration_year
    calibration = years == calibration_year
    test = years == evaluation_year
    if not np.any(train) or not np.any(calibration) or not np.any(test):
        raise ValueError(f"year {year} has an empty nested partition")
    return train, calibration, test


def _prediction_frame(
    dataset: TabularDataset, mask: np.ndarray, scores: FloatArray, temperature: float
) -> PredictionFrame:
    return PredictionFrame(
        race_ids=dataset.race_ids[mask],
        race_dates=dataset.race_dates[mask],
        horse_ids=dataset.horse_ids[mask],
        finish_positions=dataset.finish_positions[mask],
        decimal_odds=dataset.decimal_odds[mask],
        win_probabilities=scores_to_probabilities(
            scores, dataset.race_ids[mask], temperature=temperature
        ),
        ranking_scores=scores,
    )


def evaluate_tree_fold(
    dataset: TabularDataset,
    kind: RankerKind,
    year: int,
    *,
    estimators: int = 300,
    threads: int = 4,
) -> tuple[TreeFoldResult, PredictionFrame]:
    """Fit, prior-year calibrate, and evaluate one outer-year ranker."""
    train, calibration, test = nested_masks(dataset.race_dates, year)
    medians = fit_imputation(dataset.features[train])
    ranker = fit_ranker(
        kind,
        apply_imputation(dataset.features[train], medians),
        dataset.finish_positions[train],
        dataset.race_ids[train],
        estimators=estimators,
        threads=threads,
    )
    calibration_scores = np.asarray(
        ranker.predict(apply_imputation(dataset.features[calibration], medians)), dtype=np.float64
    )
    temperature = fit_temperature(
        calibration_scores, dataset.race_ids[calibration], dataset.finish_positions[calibration]
    )
    test_scores = np.asarray(
        ranker.predict(apply_imputation(dataset.features[test], medians)), dtype=np.float64
    )
    frame = _prediction_frame(dataset, test, test_scores, temperature)
    metrics = evaluate_prediction_frame(frame)
    return (
        TreeFoldResult(
            model=kind.value,
            year=year,
            train_rows=int(np.sum(train)),
            calibration_rows=int(np.sum(calibration)),
            test_rows=int(np.sum(test)),
            temperature=temperature,
            metrics=metrics,
        ),
        frame,
    )


def write_prediction_frame(path: Path, frame: PredictionFrame) -> None:
    """Persist an aligned OOF frame for later stacking and ablation."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "race_id": frame.race_ids,
                "race_date": frame.race_dates,
                "horse_id": frame.horse_ids,
                "finish_position": frame.finish_positions,
                "decimal_odds": frame.decimal_odds,
                "win_probability": frame.win_probabilities,
                "ranking_score": frame.ranking_scores,
            }
        ),
        path,
        compression="zstd",
    )


def read_prediction_frame(path: Path) -> PredictionFrame:
    """Load a persisted aligned OOF frame."""
    table = pq.read_table(path)
    return PredictionFrame(
        race_ids=_arrow_string(table, "race_id"),
        race_dates=_arrow_string(table, "race_date"),
        horse_ids=_arrow_string(table, "horse_id"),
        finish_positions=_arrow_numeric(table, "finish_position", np.dtype(np.int64)).astype(
            np.int64
        ),
        decimal_odds=_arrow_numeric(table, "decimal_odds", np.dtype(np.float64)),
        win_probabilities=_arrow_numeric(table, "win_probability", np.dtype(np.float64)),
        ranking_scores=_arrow_numeric(table, "ranking_score", np.dtype(np.float64)),
    )


def write_tree_report(path: Path, results: tuple[TreeFoldResult, ...]) -> None:
    """Write deterministic local research output."""
    payload = {
        "schema": "finish-position-tree-walk-forward-v1",
        "point_in_time": "fit<=Y-2; calibrate=Y-1; evaluate=Y",
        "production_integration": False,
        "results": [asdict(result) for result in results],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
