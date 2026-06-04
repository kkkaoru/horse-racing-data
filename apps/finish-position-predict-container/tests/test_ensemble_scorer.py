"""Tests for pure-function ensemble scoring (rank-blend within race)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.ensemble_scorer import (
    blend_normalized,
    normalize_within_race,
    score_with_ensemble,
)


def test_normalize_within_race_three_horses_orders_top_to_bottom() -> None:
    race_id = pd.Series(["r1", "r1", "r1"])
    scores = np.array([0.2, 0.9, 0.5], dtype=np.float64)
    tiebreak = pd.Series(["1001", "1002", "1003"])
    normalized = normalize_within_race(race_id, scores, tiebreak)
    assert normalized.tolist() == [0.0, 1.0, 0.5]


def test_normalize_within_race_tiebreak_uses_ketto_toroku() -> None:
    race_id = pd.Series(["r1", "r1", "r1"])
    scores = np.array([0.5, 0.5, 0.5], dtype=np.float64)
    tiebreak = pd.Series(["2003", "2001", "2002"])
    normalized = normalize_within_race(race_id, scores, tiebreak)
    # Ascending tiebreak: 2001 -> 1.0, 2002 -> 0.5, 2003 -> 0.0; positional align:
    assert normalized.tolist() == [0.0, 1.0, 0.5]


def test_normalize_within_race_single_horse_returns_half() -> None:
    race_id = pd.Series(["r1"])
    scores = np.array([0.42], dtype=np.float64)
    tiebreak = pd.Series(["3001"])
    normalized = normalize_within_race(race_id, scores, tiebreak)
    assert normalized.tolist() == [0.5]


def test_normalize_within_race_two_races_independent() -> None:
    race_id = pd.Series(["rA", "rA", "rB", "rB"])
    scores = np.array([0.1, 0.9, 0.3, 0.7], dtype=np.float64)
    tiebreak = pd.Series(["4001", "4002", "4003", "4004"])
    normalized = normalize_within_race(race_id, scores, tiebreak)
    # rA: 0.1 -> 0.0, 0.9 -> 1.0 ; rB: 0.3 -> 0.0, 0.7 -> 1.0
    assert normalized.tolist() == [0.0, 1.0, 0.0, 1.0]


def test_normalize_within_race_empty_inputs_returns_empty() -> None:
    race_id = pd.Series([], dtype="object")
    scores = np.array([], dtype=np.float64)
    tiebreak = pd.Series([], dtype="object")
    normalized = normalize_within_race(race_id, scores, tiebreak)
    assert normalized.shape == (0,)


def test_normalize_within_race_length_mismatch_raises() -> None:
    race_id = pd.Series(["r1", "r1"])
    scores = np.array([0.1, 0.2, 0.3], dtype=np.float64)
    tiebreak = pd.Series(["5001", "5002"])
    with pytest.raises(ValueError, match="length mismatch"):
        normalize_within_race(race_id, scores, tiebreak)


def test_blend_normalized_single_member() -> None:
    arr = np.array([0.0, 0.5, 1.0], dtype=np.float64)
    blended = blend_normalized([arr], [1.0])
    assert blended.tolist() == [0.0, 0.5, 1.0]


def test_blend_normalized_two_members_equal() -> None:
    a = np.array([0.0, 1.0, 0.5], dtype=np.float64)
    b = np.array([1.0, 0.0, 0.5], dtype=np.float64)
    blended = blend_normalized([a, b], [0.5, 0.5])
    assert blended.tolist() == [0.5, 0.5, 0.5]


def test_blend_normalized_weights_must_match_arrays() -> None:
    a = np.array([0.0, 1.0], dtype=np.float64)
    b = np.array([1.0, 0.0], dtype=np.float64)
    with pytest.raises(ValueError, match="length mismatch"):
        blend_normalized([a, b], [0.5, 0.3, 0.2])


def test_blend_normalized_arrays_must_match_length() -> None:
    a = np.array([0.0, 1.0, 0.5], dtype=np.float64)
    b = np.array([1.0, 0.0], dtype=np.float64)
    with pytest.raises(ValueError, match="array length mismatch"):
        blend_normalized([a, b], [0.5, 0.5])


def test_blend_normalized_empty_member_list_raises() -> None:
    with pytest.raises(ValueError, match="at least one member"):
        blend_normalized([], [])


def test_score_with_ensemble_recovers_iter14_alone_when_weight_one() -> None:
    race_id = pd.Series(["r1", "r1", "r1"])
    tiebreak = pd.Series(["6001", "6002", "6003"])
    iter14 = np.array([0.2, 0.9, 0.5], dtype=np.float64)
    iter22 = np.array([0.1, 0.3, 0.7], dtype=np.float64)
    blended = score_with_ensemble(
        {"iter14": iter14, "iter22": iter22},
        {"iter14": 1.0, "iter22": 0.0},
        race_id,
        tiebreak,
    )
    iter14_normalized = normalize_within_race(race_id, iter14, tiebreak)
    assert blended.tolist() == iter14_normalized.tolist()


def test_score_with_ensemble_blends_correctly() -> None:
    race_id = pd.Series(["r1", "r1", "r1"])
    tiebreak = pd.Series(["7001", "7002", "7003"])
    # Member A: scores ascending in position -> normalized [0.0, 0.5, 1.0]
    member_a = np.array([0.1, 0.2, 0.3], dtype=np.float64)
    # Member B: scores descending in position -> normalized [1.0, 0.5, 0.0]
    member_b = np.array([0.9, 0.5, 0.1], dtype=np.float64)
    blended = score_with_ensemble(
        {"a": member_a, "b": member_b},
        {"a": 0.6, "b": 0.4},
        race_id,
        tiebreak,
    )
    # Expected: 0.6 * [0, 0.5, 1] + 0.4 * [1, 0.5, 0] = [0.4, 0.5, 0.6]
    assert blended.tolist() == pytest.approx([0.4, 0.5, 0.6])


def test_score_with_ensemble_empty_dict_raises() -> None:
    race_id = pd.Series([], dtype="object")
    tiebreak = pd.Series([], dtype="object")
    with pytest.raises(ValueError, match="at least one member"):
        score_with_ensemble({}, {}, race_id, tiebreak)


def test_score_with_ensemble_two_races_blend_independently() -> None:
    race_id = pd.Series(["rA", "rA", "rB", "rB"])
    tiebreak = pd.Series(["8001", "8002", "8003", "8004"])
    iter14 = np.array([0.1, 0.9, 0.3, 0.7], dtype=np.float64)
    iter22 = np.array([0.8, 0.2, 0.6, 0.4], dtype=np.float64)
    blended = score_with_ensemble(
        {"iter14": iter14, "iter22": iter22},
        {"iter14": 0.5, "iter22": 0.5},
        race_id,
        tiebreak,
    )
    # rA: iter14_norm=[0,1] iter22_norm=[1,0] -> [0.5, 0.5]
    # rB: iter14_norm=[0,1] iter22_norm=[1,0] -> [0.5, 0.5]
    assert blended.tolist() == [0.5, 0.5, 0.5, 0.5]
