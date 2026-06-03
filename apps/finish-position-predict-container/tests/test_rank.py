"""Tests for within-race ranking."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.rank import ScoredHorse, rank_within_race


def test_rank_orders_by_descending_score() -> None:
    horses = [
        ScoredHorse("aaa", 1, 0.10),
        ScoredHorse("bbb", 2, 0.90),
        ScoredHorse("ccc", 3, 0.50),
    ]
    ranked = rank_within_race(horses)
    assert [horse.ketto_toroku_bango for horse in ranked] == ["bbb", "ccc", "aaa"]


def test_rank_assigns_one_based_ranks() -> None:
    horses = [ScoredHorse("aaa", 1, 0.10), ScoredHorse("bbb", 2, 0.90)]
    ranked = rank_within_race(horses)
    assert [horse.predicted_rank for horse in ranked] == [1, 2]


def test_rank_breaks_ties_on_ketto_ascending() -> None:
    horses = [ScoredHorse("zzz", 1, 0.50), ScoredHorse("aaa", 2, 0.50)]
    ranked = rank_within_race(horses)
    assert [horse.ketto_toroku_bango for horse in ranked] == ["aaa", "zzz"]


def test_rank_single_horse() -> None:
    ranked = rank_within_race([ScoredHorse("only", 4, 0.33)])
    assert ranked[0].predicted_rank == 1


def test_rank_empty() -> None:
    assert rank_within_race([]) == []
