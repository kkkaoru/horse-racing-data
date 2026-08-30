"""Tests for lock-1 rest-by-score cascade ranking."""

from __future__ import annotations

import pytest

from predict_lib.lock1_rerank import LOCK1_SCORE_GAP, apply_lock1_rerank_rest


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
