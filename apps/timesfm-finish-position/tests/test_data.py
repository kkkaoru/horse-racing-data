from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.data import (
    ACTION_COLUMNS,
    arm_gains,
    build_cell_queries,
    load_race_dataset,
    map_query_forecasts,
    subset,
)


def test_load_race_dataset_builds_sorted_cells_and_features(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    assert dataset.rows == 40
    assert dataset.features.shape == (40, 13)
    assert dataset.winner_ranks.shape == (40, 21)
    assert dataset.race_years.tolist() == [2023] * 10 + [2024] * 10 + [2025] * 10 + [2026] * 10
    assert str(dataset.cell_ids[0]) == "30|sprint|2|1|winter|flat|agree"


def test_load_race_dataset_rejects_missing_path(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_race_dataset(tmp_path / "missing.parquet")


def test_load_race_dataset_rejects_missing_column(race_parquet: Path, tmp_path: Path) -> None:
    table = pq.read_table(race_parquet).drop([ACTION_COLUMNS[0]])
    path = tmp_path / "missing-column.parquet"
    pq.write_table(table, path)
    with pytest.raises(ValueError, match="input parquet is missing columns"):
        load_race_dataset(path)


def test_load_race_dataset_rejects_duplicate_identity(race_parquet: Path, tmp_path: Path) -> None:
    table = pq.read_table(race_parquet)
    race_ids = table.column("race_id").to_pylist()
    race_ids[1] = race_ids[0]
    duplicate = table.set_column(
        table.schema.get_field_index("race_id"), "race_id", pa.array(race_ids)
    )
    path = tmp_path / "duplicate.parquet"
    pq.write_table(duplicate, path)
    with pytest.raises(ValueError, match="race_id must be unique"):
        load_race_dataset(path)


def test_load_race_dataset_rejects_nonpositive_rank(race_parquet: Path, tmp_path: Path) -> None:
    table = pq.read_table(race_parquet)
    ranks = table.column(ACTION_COLUMNS[0]).to_pylist()
    ranks[0] = 0
    invalid = table.set_column(
        table.schema.get_field_index(ACTION_COLUMNS[0]), ACTION_COLUMNS[0], pa.array(ranks)
    )
    path = tmp_path / "invalid-rank.parquet"
    pq.write_table(invalid, path)
    with pytest.raises(ValueError, match="winner ranks must contain 21 positive action outcomes"):
        load_race_dataset(path)


def test_arm_gains_uses_top123_utility_against_baseline() -> None:
    ranks = np.full((1, 21), 5, dtype=np.int64)
    ranks[0, 0] = 1
    ranks[0, 10] = 2
    gains = arm_gains(ranks)
    assert gains.shape == (1, 21)
    assert gains[0, 0] == pytest.approx(1.0 / 3.0)
    assert gains[0, 10] == 0.0


def test_build_cell_queries_maps_outer_dates_without_same_year_outcomes(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    queries = build_cell_queries(train, evaluation, context_length=32)
    forecasts = tuple(
        np.full((21, queries.horizon), index, dtype=np.float64)
        for index in range(len(queries.contexts))
    )
    mapped = map_query_forecasts(queries, forecasts)
    assert len(queries.cell_ids) == 1
    assert queries.horizon == 10
    assert mapped.shape == (10, 21)
    assert mapped[0, 0] == 0.0


def test_build_cell_queries_rejects_short_context(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    with pytest.raises(ValueError, match="context_length must be at least"):
        build_cell_queries(train, evaluation, context_length=31)


def test_map_query_forecasts_rejects_wrong_count(race_parquet: Path) -> None:
    dataset = load_race_dataset(race_parquet)
    train = subset(dataset, dataset.race_years < 2025)
    evaluation = subset(dataset, dataset.race_years == 2025)
    queries = build_cell_queries(train, evaluation, context_length=32)
    with pytest.raises(ValueError, match="forecast count does not match"):
        map_query_forecasts(queries, ())
