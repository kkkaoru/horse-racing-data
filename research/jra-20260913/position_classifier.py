"""Exact-position probability learning and lexicographic maximum-utility assignment."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import numpy.typing as npt
from catboost import CatBoostClassifier, Pool
from fold_ranker import MAX_RELEVANCE_PLUS_ONE, RankerConfig, relevance_for_top5
from scipy.optimize import linear_sum_assignment

CLASS_COUNT: int = 6
TOP_RANKS: int = 5
PROBABILITY_TOLERANCE: float = 1e-6


def _validate_probabilities(probabilities: npt.NDArray[np.float64]) -> None:
    """Validate exact-position marginals without imputing missing probabilities."""
    if probabilities.ndim != 2 or probabilities.shape[1] != CLASS_COUNT or not len(probabilities):
        raise ValueError("Expected a nonempty runner by six-class probability matrix")
    if (
        not np.isfinite(probabilities).all()
        or np.any(probabilities < 0)
        or np.any(probabilities > 1)
    ):
        raise ValueError("Probabilities must be finite and in [0,1]")
    if not np.allclose(probabilities.sum(axis=1), 1, rtol=0, atol=PROBABILITY_TOLERANCE):
        raise ValueError("Each runner's probability distribution must sum to one")


def assign_positions(probabilities: npt.NDArray[np.float64]) -> npt.NDArray[np.int64]:
    """Maximize winner probability first, then expected exact positions 2..5.

    Rows follow immutable identity order; column zero means outside the top five.
    """
    _validate_probabilities(probabilities)
    winner = int(np.argmax(probabilities[:, 1]))
    available = np.delete(np.arange(len(probabilities), dtype=np.int64), winner)
    depth = min(TOP_RANKS, len(probabilities))
    top = np.empty(depth, dtype=np.int64)
    top[0] = winner
    if depth > 1:
        rows, columns = linear_sum_assignment(-probabilities[available, 2 : depth + 1])
        top[columns + 1] = available[rows]
    return _complete_order(probabilities, top)


def assign_joint_positions(
    probabilities: npt.NDArray[np.float64], *, winner_weight: float
) -> npt.NDArray[np.int64]:
    """Jointly maximize weighted expected exact hits; no horse is locked at rank one."""
    _validate_probabilities(probabilities)
    if not np.isfinite(winner_weight) or winner_weight < 1:
        raise ValueError("Winner utility weight must be finite and at least one")
    depth = min(TOP_RANKS, len(probabilities))
    weights = np.ones(depth, dtype=np.float64)
    weights[0] = winner_weight
    rows, columns = linear_sum_assignment(-probabilities[:, 1 : depth + 1] * weights)
    top = np.empty(depth, dtype=np.int64)
    top[columns] = rows
    return _complete_order(probabilities, top)


def _complete_order(
    probabilities: npt.NDArray[np.float64], top: npt.NDArray[np.int64]
) -> npt.NDArray[np.int64]:
    expected_relevance = probabilities[:, 1:] @ np.arange(TOP_RANKS, 0, -1, dtype=np.float64)
    remaining = np.setdiff1d(np.arange(len(probabilities), dtype=np.int64), top)
    tail = remaining[np.argsort(-expected_relevance[remaining], kind="stable")]
    return np.concatenate((top, tail))


def rank_scores_for_races(
    probabilities: npt.NDArray[np.float64],
    race_ids: npt.NDArray[np.str_],
    *,
    winner_weight: float | None = None,
) -> npt.NDArray[np.float64]:
    """Decode independent complete races; returned scores are ordinal, not confidence."""
    if probabilities.ndim != 2 or race_ids.ndim != 1 or len(probabilities) != len(race_ids):
        raise ValueError("Race identities and probability rows must align")
    if len(race_ids) == 0:
        raise ValueError("No races to decode")
    scores = np.empty(len(race_ids), dtype=np.float64)
    for race_id in np.unique(race_ids):
        indices = np.flatnonzero(race_ids == race_id)
        order = (
            assign_positions(probabilities[indices])
            if winner_weight is None
            else assign_joint_positions(probabilities[indices], winner_weight=winner_weight)
        )
        scores[indices[order]] = np.arange(len(indices), 0, -1, dtype=np.float64)
    return scores


def fit_position_probabilities(
    *,
    x_train: npt.NDArray[np.float32],
    finishes: npt.NDArray[np.float32],
    abnormality: npt.NDArray[np.str_],
    race_ids: npt.NDArray[np.str_],
    x_evaluation: npt.NDArray[np.float32],
    output: Path,
    config: RankerConfig,
) -> npt.NDArray[np.float64]:
    """Fit proper multiclass log loss with equal total weight per training race."""
    if x_train.ndim != 2 or x_evaluation.ndim != 2 or x_train.shape[1] != x_evaluation.shape[1]:
        raise ValueError("Feature matrices must have the same schema")
    if len(x_train) != len(finishes) or race_ids.shape != finishes.shape:
        raise ValueError("Training rows and race groups must align")
    if np.isinf(x_train).any() or np.isinf(x_evaluation).any():
        raise ValueError("Infinite features are invalid")
    relevance = relevance_for_top5(finishes, abnormality)
    classes = np.where(relevance > 0, MAX_RELEVANCE_PLUS_ONE - relevance, 0).astype(np.int64)
    _, group_index, counts = np.unique(race_ids, return_inverse=True, return_counts=True)
    weights = (len(classes) / len(counts)) / counts[group_index]
    output.mkdir(parents=True, exist_ok=False)
    model = CatBoostClassifier(
        loss_function="MultiClass",
        iterations=config.iterations,
        depth=config.depth,
        learning_rate=config.learning_rate,
        random_seed=config.seed,
        l2_leaf_reg=3.0,
        thread_count=config.threads,
        task_type="CPU",
        verbose=False,
        allow_writing_files=False,
    )
    model.fit(Pool(x_train, label=classes, weight=weights))
    model.save_model(str(output / "model.cbm"))
    model.save_model(str(output / "model.json"), format="json")
    raw = np.asarray(model.predict_proba(x_evaluation), dtype=np.float64)
    class_ids = np.asarray(model.classes_, dtype=np.int64)
    if (
        raw.shape != (len(x_evaluation), len(class_ids))
        or len(set(class_ids.tolist())) != len(class_ids)
        or np.any(class_ids < 0)
        or np.any(class_ids >= CLASS_COUNT)
    ):
        raise ValueError("Invalid native classifier class layout")
    probabilities = np.zeros((len(x_evaluation), CLASS_COUNT), dtype=np.float64)
    probabilities[:, class_ids] = raw
    if not np.isfinite(probabilities).all():
        raise ValueError("Nonfinite native classifier forecasts")
    return probabilities
