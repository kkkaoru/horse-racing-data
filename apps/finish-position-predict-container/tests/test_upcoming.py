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
            "jra-cb-v9-sim-2013-clean",
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
    rows = build_prediction_rows("jra:2024:0405:05:08", "jra", ranked, None, entries[0])
    assert rows == [
        [
            "jra-cb-v9-sim-2013-clean",
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
            None,
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
    rows = build_prediction_rows("nar:2026:0723:54:11", "nar", ranked, None, entries[0])
    assert rows[0][0] == "iter12-nar-xgb-hpo-v8-clean188"
    assert rows[0][17] == "sprint"
    assert rows[0][18] == "medium"
    assert rows[0][19] == "summer"
    assert rows[0][20] == "C"
    assert rows[0][21] == "dirt"


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
    rows = build_prediction_rows("ban-ei:2026:1201:83:07", "ban-ei", ranked, None, entries[0])
    assert rows[0][0] == "banei-cb-v9-sim-2011"
    assert rows[0][17] == "sprint"
    assert rows[0][18] == "small"
    assert rows[0][19] == "winter"
    assert rows[0][20] is None
    assert rows[0][21] is None


def test_build_prediction_rows_uses_explicit_model_version_override() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0101:45:08", "jra", ranked, "iter21-jra-cb-class005-v8")
    assert rows[0][0] == "iter21-jra-cb-class005-v8"


def test_build_prediction_rows_falls_back_to_category_when_none_passed() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0101:45:08", "jra", ranked, None)
    assert rows[0][0] == "jra-cb-v9-sim-2013-clean"


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
    assert rows[0][17] == "intermediate"
    assert rows[0][18] == "large"


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
    assert rows[0][20] is None
    assert rows[0][21] is None


def test_build_prediction_rows_entry_missing_metadata_columns() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("nar:2026:0723:54:11", "nar", ranked, None, entries[0])
    assert rows[0][17] is None
    assert rows[0][18] is None
    assert rows[0][19] == "summer"
    assert rows[0][20] is None
    assert rows[0][21] is None


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
    assert rows[0][17:] == ["sprint", "large", "spring", "005", "turf"]
    assert rows[1][17:] == ["sprint", "large", "spring", "005", "turf"]


# ── MASTER-INVENTORY finding #12: audit columns from the `entries` param ──────


def test_build_prediction_rows_without_entries_leaves_audit_columns_none() -> None:
    """Legacy callers that do not pass `entries` keep all four audit columns
    None -- the additive column set must never break a caller that has not
    been updated yet."""
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0101:45:08", "jra", ranked, None, entries[0])
    assert rows[0][13:17] == [None, None, None, None]


def test_build_prediction_rows_with_entries_populates_audit_columns() -> None:
    entries = [
        {
            "ketto_toroku_bango": "111",
            "umaban": 1,
            "odds_score": 0.72,
            "tansho_odds": 3.5,
            "futan_juryo": 55.0,
            "weight_diff_from_avg": -2.0,
        }
    ]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows(
        "jra:2024:0101:45:08", "jra", ranked, None, entries[0], entries=entries
    )
    assert rows[0][13:17] == [0.72, 3.5, 55.0, -2.0]


def test_build_prediction_rows_audit_columns_looked_up_per_horse_by_ketto() -> None:
    """Each horse's audit values must come from ITS OWN entry, not the first
    entry in the race (a copy-paste bug this test would catch: e.g. reusing
    race_entry's audit values for every horse)."""
    entries = [
        {
            "ketto_toroku_bango": "111",
            "umaban": 1,
            "odds_score": 0.9,
            "tansho_odds": 1.5,
            "futan_juryo": 56.0,
            "weight_diff_from_avg": 1.0,
        },
        {
            "ketto_toroku_bango": "222",
            "umaban": 2,
            "odds_score": 0.1,
            "tansho_odds": 45.0,
            "futan_juryo": 54.0,
            "weight_diff_from_avg": -3.0,
        },
    ]
    ranked = rank_race_entries(entries, [0.9, 0.1])
    rows = build_prediction_rows(
        "jra:2024:0101:45:08", "jra", ranked, None, entries[0], entries=entries
    )
    by_ketto = {row[6]: row[13:17] for row in rows}
    assert by_ketto["111"] == [0.9, 1.5, 56.0, 1.0]
    assert by_ketto["222"] == [0.1, 45.0, 54.0, -3.0]


def test_build_prediction_rows_audit_columns_missing_fields_default_none() -> None:
    """An entry missing one or more of the four audit fields (e.g. odds not
    yet published) leaves just that column None, not the whole tuple."""
    entries = [
        {
            "ketto_toroku_bango": "111",
            "umaban": 1,
            "futan_juryo": 52.0,
        }
    ]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows(
        "jra:2024:0101:45:08", "jra", ranked, None, entries[0], entries=entries
    )
    assert rows[0][13:17] == [None, None, 52.0, None]


def test_build_prediction_rows_audit_columns_unmatched_horse_defaults_none() -> None:
    """A horse in `ranked` with no matching entry in `entries` (should not
    happen in production, but must degrade safely) gets None, not a KeyError."""
    ranked_entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(ranked_entries, [0.42])
    other_entries = [{"ketto_toroku_bango": "999", "umaban": 9, "odds_score": 0.5}]
    rows = build_prediction_rows(
        "jra:2024:0101:45:08", "jra", ranked, None, ranked_entries[0], entries=other_entries
    )
    assert rows[0][13:17] == [None, None, None, None]
