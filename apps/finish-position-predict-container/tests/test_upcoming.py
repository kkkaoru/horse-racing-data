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


def test_build_prediction_rows_columns_jra() -> None:
    entries = [{"ketto_toroku_bango": "111", "umaban": 1}]
    ranked = rank_race_entries(entries, [0.42])
    rows = build_prediction_rows("jra:2024:0101:45:08", "jra", ranked)
    assert rows == [
        [
            "iter14-jra-cb-pacestyle-course-v8",
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
        ]
    ]


def test_build_prediction_rows_nar_model_version() -> None:
    entries = [{"ketto_toroku_bango": "222", "umaban": 3}]
    ranked = rank_race_entries(entries, [0.7])
    rows = build_prediction_rows("nar:2026:0523:54:11", "nar", ranked)
    assert rows[0][0] == "iter12-nar-xgb-hpo-v8"
    assert rows[0][1] == "nar"


def test_build_prediction_rows_banei_model_version() -> None:
    entries = [{"ketto_toroku_bango": "333", "umaban": 5}]
    ranked = rank_race_entries(entries, [0.1])
    rows = build_prediction_rows("ban-ei:2026:0601:83:07", "ban-ei", ranked)
    assert rows[0][0] == "banei-cb-v7-lineage-wf-21y"


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
    assert rows[0][0] == "iter14-jra-cb-pacestyle-course-v8"
