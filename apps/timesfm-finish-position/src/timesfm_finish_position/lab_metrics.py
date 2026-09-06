"""Common OOF metrics for classifiers, rankers, and stacked predictions."""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from .lab_domain import PredictionFrame, ProbabilityMetrics

PROBABILITY_EPSILON = 1e-7
CALIBRATION_BINS = 10
TOP_K = 3


def normalize_probabilities_by_race(
    race_ids: np.ndarray[tuple[int], np.dtype[np.str_]],
    raw: np.ndarray[tuple[int], np.dtype[np.float64]],
) -> np.ndarray[tuple[int], np.dtype[np.float64]]:
    """Normalize positive runner scores into one probability simplex per race."""
    if race_ids.shape != raw.shape:
        raise ValueError("race_ids and raw probabilities must align")
    if np.any(~np.isfinite(raw)) or np.any(raw < 0.0):
        raise ValueError("raw probabilities must be finite and nonnegative")
    result = np.zeros_like(raw)
    indices: dict[str, list[int]] = defaultdict(list)
    for index, race_id in enumerate(race_ids):
        indices[str(race_id)].append(index)
    for race_indices in indices.values():
        values = raw[race_indices]
        total = float(np.sum(values))
        if total <= 0.0:
            result[race_indices] = 1.0 / len(race_indices)
        else:
            result[race_indices] = values / total
    return result


def _validate_prediction_frame(frame: PredictionFrame) -> None:
    shapes = {
        frame.race_dates.shape,
        frame.horse_ids.shape,
        frame.finish_positions.shape,
        frame.decimal_odds.shape,
        frame.win_probabilities.shape,
        frame.ranking_scores.shape,
    }
    if frame.race_ids.ndim != 1 or shapes != {frame.race_ids.shape}:
        raise ValueError("prediction columns must be aligned one-dimensional arrays")
    if frame.rows == 0:
        raise ValueError("prediction frame must not be empty")
    if np.any(frame.finish_positions < 1):
        raise ValueError("finish positions must be positive")
    if np.any(~np.isfinite(frame.win_probabilities)) or np.any(frame.win_probabilities < 0.0):
        raise ValueError("win probabilities must be finite and nonnegative")


def _race_indices(frame: PredictionFrame) -> dict[str, np.ndarray[tuple[int], np.dtype[np.int64]]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for index, race_id in enumerate(frame.race_ids):
        grouped[str(race_id)].append(index)
    return {race_id: np.asarray(indices, dtype=np.int64) for race_id, indices in grouped.items()}


def _expected_calibration_error(labels: np.ndarray, probabilities: np.ndarray) -> float:
    edges = np.linspace(0.0, 1.0, CALIBRATION_BINS + 1)
    total = len(labels)
    error = 0.0
    for index in range(CALIBRATION_BINS):
        lower = edges[index]
        upper = edges[index + 1]
        mask = (probabilities >= lower) & (
            probabilities <= upper if index == CALIBRATION_BINS - 1 else probabilities < upper
        )
        count = int(np.sum(mask))
        if count == 0:
            continue
        observed = float(np.mean(labels[mask]))
        predicted = float(np.mean(probabilities[mask]))
        error += count / total * abs(observed - predicted)
    return error


def _ndcg_at_3(finish_positions: np.ndarray, ranking_scores: np.ndarray) -> float:
    predicted = np.argsort(-ranking_scores)[:TOP_K]
    relevance = np.maximum(TOP_K + 1 - finish_positions, 0).astype(np.float64)
    discounts = 1.0 / np.log2(np.arange(2, TOP_K + 2, dtype=np.float64))
    predicted_relevance = relevance[predicted]
    dcg = float(np.sum((2.0**predicted_relevance - 1.0) * discounts[: len(predicted)]))
    ideal = np.sort(relevance)[::-1][:TOP_K]
    idcg = float(np.sum((2.0**ideal - 1.0) * discounts[: len(ideal)]))
    return dcg / idcg if idcg > 0.0 else 0.0


def _race_outcomes(
    frame: PredictionFrame, grouped: dict[str, np.ndarray[tuple[int], np.dtype[np.int64]]]
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    ndcg: list[float] = []
    top1: list[float] = []
    winner_in_top3: list[float] = []
    top3_set: list[float] = []
    for indices in grouped.values():
        finish = frame.finish_positions[indices]
        scores = frame.ranking_scores[indices]
        order = np.argsort(-scores)
        ndcg.append(_ndcg_at_3(finish, scores))
        top1.append(float(finish[order[0]] == 1))
        winner_in_top3.append(float(np.any(finish[order[:TOP_K]] == 1)))
        predicted_horses = set(frame.horse_ids[indices][order[:TOP_K]].tolist())
        actual_horses = set(frame.horse_ids[indices][finish <= TOP_K].tolist())
        top3_set.append(float(predicted_horses == actual_horses))
    return (
        np.asarray(ndcg, dtype=np.float64),
        np.asarray(top1, dtype=np.float64),
        np.asarray(winner_in_top3, dtype=np.float64),
        np.asarray(top3_set, dtype=np.float64),
    )


def _betting_returns(
    frame: PredictionFrame, grouped: dict[str, np.ndarray[tuple[int], np.dtype[np.int64]]]
) -> np.ndarray[tuple[int], np.dtype[np.float64]]:
    ordered_groups = sorted(
        grouped.values(),
        key=lambda indices: (str(frame.race_dates[indices[0]]), str(frame.race_ids[indices[0]])),
    )
    returns: list[float] = []
    for indices in ordered_groups:
        picked = indices[int(np.argmax(frame.ranking_scores[indices]))]
        gross = float(frame.decimal_odds[picked]) if frame.finish_positions[picked] == 1 else 0.0
        returns.append(gross)
    return np.asarray(returns, dtype=np.float64)


def evaluate_prediction_frame(frame: PredictionFrame) -> ProbabilityMetrics:
    """Compute common PIT-safe metrics without optimizing directly for ROI."""
    _validate_prediction_frame(frame)
    grouped = _race_indices(frame)
    probabilities = normalize_probabilities_by_race(frame.race_ids, frame.win_probabilities)
    labels = (frame.finish_positions == 1).astype(np.float64)
    clipped = np.clip(probabilities, PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON)
    log_loss = -float(np.mean(labels * np.log(clipped) + (1.0 - labels) * np.log(1.0 - clipped)))
    brier = float(np.mean((probabilities - labels) ** 2))
    ece = _expected_calibration_error(labels, probabilities)
    ndcg, top1, top3, top3_set = _race_outcomes(frame, grouped)
    gross_returns = _betting_returns(frame, grouped)
    profits = gross_returns - 1.0
    equity = np.cumsum(profits)
    peaks = np.maximum.accumulate(np.concatenate((np.zeros(1), equity)))[1:]
    drawdown = peaks - equity
    races = len(grouped)
    return ProbabilityMetrics(
        runners=frame.rows,
        races=races,
        log_loss=log_loss,
        brier_score=brier,
        expected_calibration_error=ece,
        ndcg_at_3=float(np.mean(ndcg)),
        top1_accuracy=float(np.mean(top1)),
        winner_in_top3=float(np.mean(top3)),
        top3_set_accuracy=float(np.mean(top3_set)),
        roi=float(np.sum(gross_returns) / races),
        yield_rate=float(np.sum(profits) / races),
        maximum_drawdown=float(np.max(drawdown)),
    )
