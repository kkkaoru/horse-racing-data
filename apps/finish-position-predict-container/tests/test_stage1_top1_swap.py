"""Tests for the Stage-1 top-1 companion score swap."""

from __future__ import annotations

import pytest

from predict_lib.stage1_top1_swap import apply_top1_score_swap


def test_apply_top1_score_swap_preserves_base_when_top1_agrees() -> None:
    assert apply_top1_score_swap(
        ["H1", "H2", "H3"], [3.0, 2.0, 1.0], [5.0, 4.0, 0.0]
    ) == [3.0, 2.0, 1.0]


def test_apply_top1_score_swap_exchanges_only_base_and_companion_top() -> None:
    assert apply_top1_score_swap(
        ["H1", "H2", "H3", "H4"],
        [4.0, 3.0, 2.0, 1.0],
        [0.0, 1.0, 5.0, 2.0],
    ) == [2.0, 3.0, 4.0, 1.0]


def test_apply_top1_score_swap_uses_horse_id_tie_break_and_accepts_empty() -> None:
    assert apply_top1_score_swap(["H2", "H1"], [2.0, 1.0], [3.0, 3.0]) == [1.0, 2.0]
    assert apply_top1_score_swap([], [], []) == []


def test_apply_top1_score_swap_rejects_misaligned_inputs() -> None:
    with pytest.raises(ValueError, match="equal lengths"):
        apply_top1_score_swap(["H1"], [1.0], [])
