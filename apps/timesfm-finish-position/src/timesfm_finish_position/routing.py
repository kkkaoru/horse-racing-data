"""Leak-free integration of temporal signals with the existing ensemble router."""

from __future__ import annotations

import platform

import numpy as np

from .data import arm_gains
from .domain import (
    ACTION_WEIGHTS,
    BASELINE_ACTION_INDEX,
    FloatArray,
    IntArray,
    IntegrationPattern,
    RaceDataset,
)

POLICY_RIDGE_ALPHA = 10.0
DYNAMIC_TEMPERATURE = 0.1
MIN_SCALE = 1e-8


def _standardize(train: FloatArray, evaluation: FloatArray) -> tuple[FloatArray, FloatArray]:
    medians = np.nanmedian(train, axis=0)
    train_filled = np.where(np.isfinite(train), train, medians)
    eval_filled = np.where(np.isfinite(evaluation), evaluation, medians)
    means = train_filled.mean(axis=0)
    scales = train_filled.std(axis=0)
    scales[scales < MIN_SCALE] = 1.0
    return (train_filled - means) / scales, (eval_filled - means) / scales


def _ridge_numpy(features: FloatArray, targets: FloatArray) -> FloatArray:
    design = np.column_stack((features, np.ones(len(features), dtype=np.float64)))
    regularizer = np.eye(design.shape[1], dtype=np.float64) * POLICY_RIDGE_ALPHA
    regularizer[-1, -1] = 0.0
    return np.linalg.solve(design.T @ design + regularizer, design.T @ targets)


def _ridge_mlx(features: FloatArray, targets: FloatArray) -> FloatArray:
    import mlx.core as mx

    design = mx.array(
        np.column_stack((features, np.ones(len(features), dtype=np.float64))), dtype=mx.float32
    )
    target = mx.array(targets, dtype=mx.float32)
    regularizer_values = np.eye(design.shape[1], dtype=np.float32) * POLICY_RIDGE_ALPHA
    regularizer_values[-1, -1] = 0.0
    regularizer = mx.array(regularizer_values)
    coefficients = mx.linalg.solve(
        design.T @ design + regularizer, design.T @ target, stream=mx.cpu
    )
    mx.eval(coefficients)
    return np.asarray(coefficients, dtype=np.float64)


def policy_backend() -> str:
    """Return the platform-specific contextual-policy optimization backend."""
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        return "mlx"
    return "numpy"


def contextual_policy_gains(train: RaceDataset, evaluation: RaceDataset) -> FloatArray:
    """Fit the existing full-information contextual policy on prior years only."""
    train_features, eval_features = _standardize(train.features, evaluation.features)
    targets = arm_gains(train.winner_ranks)
    coefficients = (
        _ridge_mlx(train_features, targets)
        if policy_backend() == "mlx"
        else _ridge_numpy(train_features, targets)
    )
    eval_design = np.column_stack((eval_features, np.ones(len(eval_features), dtype=np.float64)))
    return eval_design @ coefficients


def _positive_argmax(gains: FloatArray) -> IntArray:
    best = np.argmax(gains, axis=1).astype(np.int64)
    has_gain = np.max(gains, axis=1) > 0.0
    return np.where(has_gain, best, BASELINE_ACTION_INDEX).astype(np.int64)


def _policy_and_data_actions(
    train: RaceDataset, evaluation: RaceDataset, temporal_gains: FloatArray
) -> IntArray:
    policy_gains = contextual_policy_gains(train, evaluation)
    temporal_confidence = np.ptp(temporal_gains, axis=1)
    policy_confidence = np.ptp(policy_gains, axis=1)
    denominator = temporal_confidence + policy_confidence
    policy_weight = np.divide(
        policy_confidence,
        denominator,
        out=np.full_like(denominator, 0.5),
        where=denominator > MIN_SCALE,
    )
    fused = policy_weight[:, None] * policy_gains + (1.0 - policy_weight[:, None]) * temporal_gains
    return _positive_argmax(fused)


def _historical_cell_actions(train: RaceDataset) -> dict[str, int]:
    gains = arm_gains(train.winner_ranks)
    cells: dict[str, list[FloatArray]] = {}
    for cell, row in zip(train.cell_ids, gains, strict=True):
        cells.setdefault(str(cell), []).append(row)
    actions: dict[str, int] = {}
    for cell, rows in cells.items():
        mean_gain = np.mean(rows, axis=0)
        best = int(np.argmax(mean_gain))
        actions[cell] = best if mean_gain[best] > 0.0 else BASELINE_ACTION_INDEX
    return actions


def _router_only_actions(
    train: RaceDataset, evaluation: RaceDataset, temporal_gains: FloatArray
) -> IntArray:
    historical = _historical_cell_actions(train)
    selected = np.full(evaluation.rows, BASELINE_ACTION_INDEX, dtype=np.int64)
    for index, cell in enumerate(evaluation.cell_ids):
        action = historical.get(str(cell), BASELINE_ACTION_INDEX)
        if temporal_gains[index, action] > 0.0:
            selected[index] = action
    return selected


def _dynamic_ensemble_actions(temporal_gains: FloatArray) -> IntArray:
    shifted = temporal_gains - np.max(temporal_gains, axis=1, keepdims=True)
    probabilities = np.exp(shifted / DYNAMIC_TEMPERATURE)
    probabilities /= probabilities.sum(axis=1, keepdims=True)
    weights = probabilities @ ACTION_WEIGHTS
    selected = np.rint(weights * (len(ACTION_WEIGHTS) - 1)).astype(np.int64)
    has_gain = np.max(temporal_gains, axis=1) > 0.0
    return np.where(has_gain, selected, BASELINE_ACTION_INDEX).astype(np.int64)


def select_actions(
    pattern: IntegrationPattern,
    *,
    train: RaceDataset,
    evaluation: RaceDataset,
    temporal_gains: FloatArray,
) -> IntArray:
    """Select one existing blend weight for every race without target leakage."""
    expected_shape = (evaluation.rows, len(ACTION_WEIGHTS))
    if temporal_gains.shape != expected_shape:
        raise ValueError(f"temporal_gains must have shape {expected_shape}")
    dispatch = {
        IntegrationPattern.POLICY_AND_DATA: lambda: _policy_and_data_actions(
            train, evaluation, temporal_gains
        ),
        IntegrationPattern.ROUTER_ONLY: lambda: _router_only_actions(
            train, evaluation, temporal_gains
        ),
        IntegrationPattern.DYNAMIC_ENSEMBLE: lambda: _dynamic_ensemble_actions(temporal_gains),
    }
    return dispatch[pattern]()
