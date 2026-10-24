"""Tests for the race-day going -> RA-shaped baba code mapping."""

from __future__ import annotations

from predict_lib.current_going import baba_condition, baba_refresh_needed, ra_going_pair


def test_ra_going_pair_maps_each_grade_like_training() -> None:
    assert ra_going_pair([{"surface": "turf", "going": "良"}]) == ("1", "0")
    assert ra_going_pair([{"surface": "turf", "going": "稍重"}]) == ("2", "0")
    assert ra_going_pair([{"surface": "dirt", "going": "重"}]) == ("0", "3")
    assert ra_going_pair([{"surface": "dirt", "going": "不良"}]) == ("0", "4")


def test_ra_going_pair_keeps_both_surfaces_for_turf_start_dirt() -> None:
    assert ra_going_pair(
        [{"surface": "turf", "going": "稍重"}, {"surface": "dirt", "going": "重"}]
    ) == ("2", "3")


def test_ra_going_pair_never_invents_a_going() -> None:
    assert ra_going_pair([]) is None
    assert ra_going_pair([{"surface": "dirt", "going": "2.2"}]) is None
    assert ra_going_pair([{"surface": "jump", "going": "良"}]) is None
    assert ra_going_pair([{"surface": "turf"}]) is None


def test_baba_condition_reads_shiba_first_like_baba_condition_sql() -> None:
    assert baba_condition(("2", "0")) == 2
    assert baba_condition(("0", "4")) == 4
    assert baba_condition(("2", "3")) == 2


def test_baba_refresh_needed_when_cached_value_is_missing_or_stale() -> None:
    assert baba_refresh_needed([{"current_baba_condition": None}], 2) is True
    assert baba_refresh_needed([{"current_baba_condition": float("nan")}], 2) is True
    assert baba_refresh_needed([{}], 2) is True
    assert baba_refresh_needed([{"current_baba_condition": True}], 1) is True
    assert baba_refresh_needed([{"current_baba_condition": 1}], 2) is True


def test_baba_refresh_not_needed_when_every_entry_matches() -> None:
    assert (
        baba_refresh_needed([{"current_baba_condition": 2}, {"current_baba_condition": 2.0}], 2)
        is False
    )
    assert baba_refresh_needed([], 2) is False
