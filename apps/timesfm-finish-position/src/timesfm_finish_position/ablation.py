"""OOF alignment, regularized stacking, and leave-one-model-out ablation."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

from .lab_domain import PredictionFrame, ProbabilityMetrics
from .lab_metrics import evaluate_prediction_frame
from .stacking import fit_logistic_stacker

STACK_PROBABILITY_EPSILON = 1e-7


@dataclass(frozen=True)
class NamedPredictionFrame:
    """One model's aligned walk-forward predictions."""

    name: str
    frame: PredictionFrame


@dataclass(frozen=True)
class AblationResult:
    """Metrics for the full stack or one leave-one-model-out arm."""

    excluded_model: str | None
    metrics: ProbabilityMetrics
    delta_log_loss_from_full: float
    delta_brier_from_full: float
    delta_ndcg_at_3_from_full: float

    def to_dict(self) -> dict[str, object]:
        """Return JSON-ready values."""
        return {
            "excluded_model": self.excluded_model,
            "metrics": asdict(self.metrics),
            "delta_log_loss_from_full": self.delta_log_loss_from_full,
            "delta_brier_from_full": self.delta_brier_from_full,
            "delta_ndcg_at_3_from_full": self.delta_ndcg_at_3_from_full,
        }


def _identity(frame: PredictionFrame) -> np.ndarray:
    return np.char.add(np.char.add(frame.race_ids, ":"), frame.horse_ids)


def validate_aligned_predictions(predictions: tuple[NamedPredictionFrame, ...]) -> None:
    """Require identical ordered OOF runners, dates, labels, and odds."""
    if len(predictions) < 2:
        raise ValueError("at least two model prediction frames are required")
    names = [prediction.name for prediction in predictions]
    if len(set(names)) != len(names):
        raise ValueError("model prediction names must be unique")
    reference = predictions[0].frame
    reference_identity = _identity(reference)
    for prediction in predictions[1:]:
        frame = prediction.frame
        if not np.array_equal(_identity(frame), reference_identity):
            raise ValueError(f"runner identity mismatch for {prediction.name}")
        if not np.array_equal(frame.race_dates, reference.race_dates):
            raise ValueError(f"race date mismatch for {prediction.name}")
        if not np.array_equal(frame.finish_positions, reference.finish_positions):
            raise ValueError(f"label mismatch for {prediction.name}")
        if not np.array_equal(frame.decimal_odds, reference.decimal_odds):
            raise ValueError(f"odds mismatch for {prediction.name}")


def stacking_features(predictions: tuple[NamedPredictionFrame, ...]) -> np.ndarray:
    """Build probability-logit and within-model rank-score features."""
    validate_aligned_predictions(predictions)
    columns: list[np.ndarray] = []
    for prediction in predictions:
        frame = prediction.frame
        probabilities = np.clip(
            frame.win_probabilities, STACK_PROBABILITY_EPSILON, 1.0 - STACK_PROBABILITY_EPSILON
        )
        columns.append(np.log(probabilities / (1.0 - probabilities)))
        columns.append(frame.ranking_scores)
    return np.column_stack(columns).astype(np.float64)


def _stacked_frame(
    predictions: tuple[NamedPredictionFrame, ...], train_mask: np.ndarray, test_mask: np.ndarray
) -> PredictionFrame:
    reference = predictions[0].frame
    features = stacking_features(predictions)
    labels = (reference.finish_positions == 1).astype(np.float64)
    model = fit_logistic_stacker(features[train_mask], labels[train_mask])
    probabilities, scores = model.predict(features[test_mask], reference.race_ids[test_mask])
    return PredictionFrame(
        race_ids=reference.race_ids[test_mask],
        race_dates=reference.race_dates[test_mask],
        horse_ids=reference.horse_ids[test_mask],
        finish_positions=reference.finish_positions[test_mask],
        decimal_odds=reference.decimal_odds[test_mask],
        win_probabilities=probabilities,
        ranking_scores=scores,
    )


def fit_stacked_prediction_frame(
    predictions: tuple[NamedPredictionFrame, ...], *, train_end: str, test_start: str
) -> PredictionFrame:
    """Fit a stack on earlier OOF rows and return strictly later predictions."""
    validate_aligned_predictions(predictions)
    if train_end >= test_start:
        raise ValueError("stacking train_end must predate test_start")
    dates = predictions[0].frame.race_dates
    train_mask = dates <= train_end
    test_mask = dates >= test_start
    if not np.any(train_mask) or not np.any(test_mask):
        raise ValueError("stacking train or test partition is empty")
    if np.any(train_mask & test_mask):
        raise ValueError("stacking train and test partitions overlap")
    return _stacked_frame(predictions, train_mask, test_mask)


def evaluate_stacking_ablation(
    predictions: tuple[NamedPredictionFrame, ...], *, train_end: str, test_start: str
) -> tuple[AblationResult, ...]:
    """Fit on earlier OOF rows and evaluate full/leave-one-out stacks later in time."""
    if len(predictions) < 3:
        raise ValueError("at least three models are required for leave-one-model-out ablation")
    full_metrics = evaluate_prediction_frame(
        fit_stacked_prediction_frame(predictions, train_end=train_end, test_start=test_start)
    )
    results = [AblationResult(None, full_metrics, 0.0, 0.0, 0.0)]
    for excluded in predictions:
        retained = tuple(item for item in predictions if item.name != excluded.name)
        metrics = evaluate_prediction_frame(
            fit_stacked_prediction_frame(retained, train_end=train_end, test_start=test_start)
        )
        results.append(
            AblationResult(
                excluded.name,
                metrics,
                metrics.log_loss - full_metrics.log_loss,
                metrics.brier_score - full_metrics.brier_score,
                metrics.ndcg_at_3 - full_metrics.ndcg_at_3,
            )
        )
    return tuple(results)
