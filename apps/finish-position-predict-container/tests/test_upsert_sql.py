"""Tests for chunked UPSERT SQL building."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.upsert_sql import (
    INSERT_COLUMNS,
    PREDICTION_SUBGROUP_COLUMNS,
    PRIMARY_KEY_COLUMNS,
    UPDATABLE_COLUMNS,
    build_upsert_sql,
    chunk_rows,
    flatten_params,
)


def test_insert_columns_count() -> None:
    assert len(INSERT_COLUMNS) == 18


def test_subgroup_columns_exclude_venue() -> None:
    assert PREDICTION_SUBGROUP_COLUMNS == (
        "distance_band",
        "field_size_band",
        "season_band",
        "class_code",
        "surface",
    )


def test_insert_columns_end_with_subgroup_columns() -> None:
    assert INSERT_COLUMNS[-5:] == (
        "distance_band",
        "field_size_band",
        "season_band",
        "class_code",
        "surface",
    )


def test_updatable_columns_include_subgroup_columns() -> None:
    assert UPDATABLE_COLUMNS == (
        "umaban",
        "predicted_score",
        "predicted_rank",
        "predicted_top1_prob",
        "predicted_top3_prob",
        "predicted_finish_position",
        "distance_band",
        "field_size_band",
        "season_band",
        "class_code",
        "surface",
    )


def test_subgroup_columns_not_in_primary_key() -> None:
    assert all(column not in PRIMARY_KEY_COLUMNS for column in PREDICTION_SUBGROUP_COLUMNS)


def test_primary_key_columns() -> None:
    assert PRIMARY_KEY_COLUMNS == (
        "model_version",
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
    )


def test_build_upsert_sql_single_row_uses_psycopg_placeholders() -> None:
    sql = build_upsert_sql(1)
    # 18 INSERT columns (13 prediction + 5 subgroup), all bound with psycopg3 %s.
    assert sql.count("%s") == 18
    assert "$1" not in sql


def test_build_upsert_sql_multi_row_placeholder_count() -> None:
    sql = build_upsert_sql(3)
    assert sql.count("%s") == 54


def test_build_upsert_sql_sets_subgroup_columns_on_conflict() -> None:
    sql = build_upsert_sql(1)
    assert "distance_band = excluded.distance_band" in sql
    assert "surface = excluded.surface" in sql


def test_build_upsert_sql_has_on_conflict_do_update() -> None:
    sql = build_upsert_sql(2)
    assert "on conflict" in sql
    assert "do update set" in sql
    assert "prediction_generated_at = now()" in sql


def test_build_upsert_sql_targets_predictions_table() -> None:
    sql = build_upsert_sql(1)
    assert "insert into race_finish_position_model_predictions" in sql


def test_build_upsert_sql_rejects_zero() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        build_upsert_sql(0)


def test_build_upsert_sql_rejects_negative() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        build_upsert_sql(-1)


def test_chunk_rows_splits_evenly() -> None:
    rows = [[1], [2], [3], [4]]
    assert chunk_rows(rows, 2) == [[[1], [2]], [[3], [4]]]


def test_chunk_rows_remainder() -> None:
    rows = [[1], [2], [3]]
    assert chunk_rows(rows, 2) == [[[1], [2]], [[3]]]


def test_chunk_rows_empty() -> None:
    assert chunk_rows([], 5) == []


def test_chunk_rows_rejects_zero_chunk() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        chunk_rows([[1]], 0)


def test_flatten_params_concatenates_rows() -> None:
    rows = [["a", 1], ["b", 2]]
    assert flatten_params(rows) == ["a", 1, "b", 2]


def test_flatten_params_empty() -> None:
    assert flatten_params([]) == []
