from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

import timesfm_finish_position.routing as subject
from timesfm_finish_position.data import load_race_dataset, subset
from timesfm_finish_position.domain import BASELINE_ACTION_INDEX, IntegrationPattern
from timesfm_finish_position.routing import contextual_policy_gains, policy_backend, select_actions


def test_contextual_policy_gains_uses_mlx_on_mac(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    gains = contextual_policy_gains(train, evaluation)
    assert policy_backend() == "mlx"
    assert gains.shape == (10, 21)
    assert np.isfinite(gains).all()


def test_contextual_policy_gains_uses_numpy_off_mac(
    race_parquet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(subject.platform, "system", lambda: "Linux")
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    gains = contextual_policy_gains(train, evaluation)
    assert policy_backend() == "numpy"
    assert gains.shape == (10, 21)


def test_policy_and_data_selects_one_action_per_race(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    temporal = np.zeros((10, 21), dtype=np.float64)
    temporal[:, 4] = 0.2
    actions = select_actions(
        IntegrationPattern.POLICY_AND_DATA,
        train=train,
        evaluation=evaluation,
        temporal_gains=temporal,
    )
    assert actions.shape == (10,)
    assert np.all((actions >= 0) & (actions <= 20))


def test_router_only_falls_back_for_unseen_cells(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years == 2023)
    evaluation = subset(dataset, dataset.race_years == 2026)
    temporal = np.ones((10, 21), dtype=np.float64)
    actions = select_actions(
        IntegrationPattern.ROUTER_ONLY,
        train=train,
        evaluation=evaluation,
        temporal_gains=temporal,
    )
    assert actions.tolist() == [BASELINE_ACTION_INDEX] * 10


def test_dynamic_ensemble_moves_toward_high_gain_weight(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    temporal = np.full((10, 21), -1.0, dtype=np.float64)
    temporal[:, 20] = 1.0
    actions = select_actions(
        IntegrationPattern.DYNAMIC_ENSEMBLE,
        train=train,
        evaluation=evaluation,
        temporal_gains=temporal,
    )
    assert actions.tolist() == [20] * 10


def test_dynamic_ensemble_falls_back_without_positive_gain(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    temporal = np.zeros((10, 21), dtype=np.float64)
    actions = select_actions(
        IntegrationPattern.DYNAMIC_ENSEMBLE,
        train=train,
        evaluation=evaluation,
        temporal_gains=temporal,
    )
    assert actions.tolist() == [BASELINE_ACTION_INDEX] * 10


def test_select_actions_rejects_wrong_temporal_shape(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    with pytest.raises(ValueError, match="temporal_gains must have shape"):
        select_actions(
            IntegrationPattern.DYNAMIC_ENSEMBLE,
            train=train,
            evaluation=evaluation,
            temporal_gains=np.zeros((9, 21)),
        )
