"""Tests for the degenerate-feature-matrix guard."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.feature_guard import (
    DEFAULT_MISSING_FEATURE_FRACTION_THRESHOLD,
    is_degenerate_feature_matrix,
    missing_feature_fraction,
    race_missing_feature_fraction,
)


def test_missing_feature_fraction_empty_feature_names_is_zero() -> None:
    assert missing_feature_fraction({"a": 1.0}, []) == 0.0


def test_missing_feature_fraction_all_present_is_zero() -> None:
    assert missing_feature_fraction({"a": 1.0, "b": 2.0}, ["a", "b"]) == 0.0


def test_missing_feature_fraction_all_absent_is_one() -> None:
    assert missing_feature_fraction({}, ["a", "b"]) == 1.0


def test_missing_feature_fraction_all_none_is_one() -> None:
    assert missing_feature_fraction({"a": None, "b": None}, ["a", "b"]) == 1.0


def test_missing_feature_fraction_mixed_absent_and_none() -> None:
    entry = {"a": 1.0, "b": None}
    assert missing_feature_fraction(entry, ["a", "b", "c"]) == 2 / 3


def test_missing_feature_fraction_zero_value_is_not_missing() -> None:
    # A legitimately zero-valued feature (e.g. a binary flag) must NOT count
    # as missing -- only an absent key or an explicit None does.
    assert missing_feature_fraction({"a": 0.0}, ["a"]) == 0.0


def test_missing_feature_fraction_empty_string_is_not_missing() -> None:
    assert missing_feature_fraction({"a": ""}, ["a"]) == 0.0


def test_missing_feature_fraction_false_is_not_missing() -> None:
    assert missing_feature_fraction({"a": False}, ["a"]) == 0.0


def test_race_missing_feature_fraction_empty_entries_is_zero() -> None:
    assert race_missing_feature_fraction([], ["a", "b"]) == 0.0


def test_race_missing_feature_fraction_single_entry_matches_entry_fraction() -> None:
    entries = [{"a": 1.0}]
    assert race_missing_feature_fraction(entries, ["a", "b"]) == 0.5


def test_race_missing_feature_fraction_averages_across_entries() -> None:
    entries = [{"a": 1.0, "b": 2.0}, {"a": None, "b": None}]
    # First entry: 0/2 missing. Second entry: 2/2 missing. Mean: 0.5.
    assert race_missing_feature_fraction(entries, ["a", "b"]) == 0.5


def test_race_missing_feature_fraction_one_sparse_debut_horse_stays_low() -> None:
    # One legitimately-sparse debut horse among otherwise fully-populated
    # entries should not push the race average anywhere near the guard
    # threshold -- this is the "must not false-positive" case the module
    # docstring calls out.
    populated = {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0}
    debut = {"a": 1.0, "b": None, "c": None, "d": None}
    entries = [populated, populated, populated, debut]
    fraction = race_missing_feature_fraction(entries, ["a", "b", "c", "d"])
    assert fraction < DEFAULT_MISSING_FEATURE_FRACTION_THRESHOLD


def test_is_degenerate_feature_matrix_healthy_race_is_false() -> None:
    entries = [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": 4.0}]
    assert is_degenerate_feature_matrix(entries, ["a", "b"]) is False


def test_is_degenerate_feature_matrix_empty_entries_every_column_missing_is_true() -> None:
    entries = [{}, {}]
    assert is_degenerate_feature_matrix(entries, ["a", "b"]) is True


def test_is_degenerate_feature_matrix_exactly_at_threshold_is_true() -> None:
    # 0.5 missing exactly equals the default threshold -- boundary is inclusive.
    entries = [{"a": 1.0, "b": None}]
    assert is_degenerate_feature_matrix(entries, ["a", "b"]) is True


def test_is_degenerate_feature_matrix_just_under_threshold_is_false() -> None:
    entries = [{"a": 1.0, "b": 2.0, "c": None}]
    assert is_degenerate_feature_matrix(entries, ["a", "b", "c"]) is False


def test_is_degenerate_feature_matrix_custom_threshold() -> None:
    entries = [{"a": 1.0, "b": None}]
    assert is_degenerate_feature_matrix(entries, ["a", "b"], threshold=0.9) is False
