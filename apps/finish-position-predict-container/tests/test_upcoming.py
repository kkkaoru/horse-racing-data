"""Tests for the upcoming-prediction transform."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.upcoming import build_prediction_rows, rank_race_entries


def test_rank_race_entries_assigns_ranks() -> None:
    entries = [
        {"ketto_toroku_bango": "111", "umaban": 1},
        {"ketto_toroku_bango": "222", "umaban": 2},
    ]
    ranked = rank_race_entries(entries, [0.2, 0.9])
    assert [horse.ketto_toroku_bango for horse in ranked] == ["222", "111"]
    assert [horse.predicted_rank for horse in ranked] == [1, 2]


def test_rank_race_entries_umaban_none_defaults_zero() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": None}]
    ranked = rank_race_entries(entries, [0.5])
    assert ranked[0].umaban == 0


def test_rank_race_entries_umaban_string_coerced() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": "7"}]
    ranked = rank_race_entries(entries, [0.5])
    assert ranked[0].umaban == 7


def test_rank_race_entries_umaban_bool_zero() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": True}]
    ranked = rank_race_entries(entries, [0.5])
    assert ranked[0].umaban == 0


def test_rank_race_entries_length_mismatch() -> None:
    with pytest.raises(ValueError, match="length mismatch"):
        rank_race_entries([{"ketto_toroku_bango": "111", "umaban": 1}], [0.1, 0.2])


def test_build_prediction_rows_columns_jra_no_entry() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0101:45:08", "jra", ranked)
    assert rows == [
        [
            "jra-cb-v9-sim-2013",
            "jra",
            "2024",
            "0101",
            "45",
            "08",
            "111",
            1,
            0.42,
            1,
            None,
            None,
            None,
            None,
            None,
            "winter",
            None,
            None,
        ]
    ]


def test_build_prediction_rows_jra_with_entry_populates_subgroups() -> None:
    entries = [
        {
            "ketto_toroku_bango": "111",
            "umaban": 1,
            "kyori": 2000,
            "shusso_tosu": 16,
            "track_code": "17",
            "kyoso_joken_code": "010",
        }
    ]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows(
        "jra:2024:0405:05:08", "jra", ranked, None, entries[0]
    )
    assert rows == [
        [
            "jra-cb-v9-sim-2013",
            "jra",
            "2024",
            "0405",
            "05",
            "08",
            "111",
            1,
            0.42,
            1,
            None,
            None,
            None,
            "intermediate",
            "large",
            "spring",
            "010",
            "turf",
        ]
    ]


def test_build_prediction_rows_nar_with_entry_uses_nar_subclass() -> None:
    entries = [
        {
            "ketto_toroku_bango": "222",
            "umaban": 3,
            "kyori": 1200,
            "shusso_tosu": 10,
            "track_code": "24",
            "nar_subclass": "C",
        }
    ]
    ranked = rank_race_entries(entries, [0.7])
    rows = build_prediction_rows(
        "nar:2026:0723:54:11", "nar", ranked, None, entries[0]
    )
    assert rows[0][0] == "iter12-nar-xgb-hpo-v8"
    assert rows[0][13] == "sprint"
    assert rows[0][14] == "medium"
    assert rows[0][15] == "summer"
    assert rows[0][16] == "C"
    assert rows[0][17] == "dirt"


def test_build_prediction_rows_banei_class_code_none() -> None:
    entries = [
        {
            "ketto_toroku_bango": "333",
            "umaban": 5,
            "kyori": 200,
            "shusso_tosu": 8,
            "track_code": "99",
            "kyoso_joken_code": "BAN",
            "nar_subclass": "other",
        }
    ]
    ranked = rank_race_entries(entries, [0.1])
    rows = build_prediction_rows(
        "ban-ei:2026:1201:83:07", "ban-ei", ranked, None, entries[0]
    )
    assert rows[0][0] == "banei-cb-v9-sim-2011"
    assert rows[0][13] == "sprint"
    assert rows[0][14] == "small"
    assert rows[0][15] == "winter"
    assert rows[0][16] is None
    assert rows[0][17] is None


def test_build_prediction_rows_uses_explicit_model_version_override() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows(
        "jra:2024:0101:45:08", "jra", ranked, "iter21-jra-cb-class005-v8"
    )
    assert rows[0][0] == "iter21-jra-cb-class005-v8"


def test_build_prediction_rows_falls_back_to_category_when_none_passed() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0101:45:08", "jra", ranked, None)
    assert rows[0][0] == "jra-cb-v9-sim-2013"


def test_build_prediction_rows_entry_string_kyori_coerced() -> None:
    entries = [
        {
            "ketto_toroku_bango": "111",
            "umaban": 1,
            "kyori": "2000",
            "shusso_tosu": "16",
            "track_code": "17",
        }
    ]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0405:05:08", "jra", ranked, None, entries[0])
    assert rows[0][13] == "intermediate"
    assert rows[0][14] == "large"


def test_build_prediction_rows_entry_blank_track_code_surface_none() -> None:
    entries = [
        {
            "ketto_toroku_bango": "111",
            "umaban": 1,
            "kyori": 1600,
            "shusso_tosu": 12,
            "track_code": "  ",
            "kyoso_joken_code": "  ",
        }
    ]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0405:05:08", "jra", ranked, None, entries[0])
    assert rows[0][16] is None
    assert rows[0][17] is None


def test_build_prediction_rows_entry_missing_metadata_columns() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("nar:2026:0723:54:11", "nar", ranked, None, entries[0])
    assert rows[0][13] is None
    assert rows[0][14] is None
    assert rows[0][15] == "summer"
    assert rows[0][16] is None
    assert rows[0][17] is None


def test_build_prediction_rows_all_horses_share_subgroup_values() -> None:
    entries = [
        {
            "ketto_toroku_bango": "111",
            "umaban": 1,
            "kyori": 1200,
            "shusso_tosu": 18,
            "track_code": "10",
            "kyoso_joken_code": "005",
        },
        {
            "ketto_toroku_bango": "222",
            "umaban": 2,
            "kyori": 1200,
            "shusso_tosu": 18,
            "track_code": "10",
            "kyoso_joken_code": "005",
        },
    ]
    ranked = rank_race_entries(entries, [0.2, 0.9])
    rows = build_prediction_rows("jra:2024:0405:05:08", "jra", ranked, None, entries[0])
    assert rows[0][13:] == ["sprint", "large", "spring", "005", "turf"]
    assert rows[1][13:] == ["sprint", "large", "spring", "005", "turf"]
