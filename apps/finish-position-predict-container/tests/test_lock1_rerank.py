"""Tests for lock-1 rest-by-score cascade ranking."""

from __future__ import annotations

import pytest

from predict_lib.lock1_rerank import LOCK1_SCORE_GAP, apply_lock1_rerank_rest, blend_lock_scores


def test_apply_lock1_rerank_rest_keeps_rest_scores_when_lock_already_leads() -> None:
    assert apply_lock1_rerank_rest(["H1", "H2", "H3"], [3.0, 2.0, 1.0], [5.0, 4.0, 0.0]) == [
        5.0,
        4.0,
        0.0,
    ]


def test_apply_lock1_rerank_rest_boosts_lock_winner_then_keeps_rest_order() -> None:
    assert apply_lock1_rerank_rest(
        ["H1", "H2", "H3", "H4"],
        [4.0, 3.0, 2.0, 1.0],
        [0.0, 1.0, 5.0, 2.0],
    ) == [5.0 + LOCK1_SCORE_GAP, 1.0, 5.0, 2.0]


def test_apply_lock1_rerank_rest_uses_horse_id_tie_break_and_accepts_empty() -> None:
    assert apply_lock1_rerank_rest(["H2", "H1"], [2.0, 1.0], [3.0, 3.0]) == [
        3.0 + LOCK1_SCORE_GAP,
        3.0,
    ]
    assert apply_lock1_rerank_rest([], [], []) == []
    assert apply_lock1_rerank_rest(["H1"], [1.0], [9.0]) == [9.0]


def test_apply_lock1_rerank_rest_rejects_misaligned_inputs() -> None:
    with pytest.raises(ValueError, match="equal lengths"):
        apply_lock1_rerank_rest(["H1"], [1.0], [])


def test_blend_lock_scores_adds_weighted_race_z_scores() -> None:
    assert blend_lock_scores([1.0, 1.0, 3.0, 3.0], [2.0, 6.0, 2.0, 6.0], 0.25) == [
        -1.25,
        -0.75,
        0.75,
        1.25,
    ]


def test_blend_lock_scores_treats_zero_std_as_one() -> None:
    assert blend_lock_scores([1.0, 3.0], [7.0, 7.0], 0.5) == [-1.0, 1.0]
    assert blend_lock_scores([2.0, 2.0], [0.0, 4.0], 0.5) == [-0.5, 0.5]


def test_blend_lock_scores_accepts_empty_and_rejects_misaligned_lengths() -> None:
    assert blend_lock_scores([], [], 0.25) == []
    with pytest.raises(ValueError, match="equal lengths"):
        blend_lock_scores([1.0, 2.0], [1.0], 0.25)
