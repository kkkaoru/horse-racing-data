"""Canonical selection is count-based; sparse/tied forecasts do not invent runners."""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.chronos_cell_diagnostics import (
    diagnose_cells,
    exact_hits,
    exact_support,
    market_order_locked_by_range,
    prediction_order,
    race_hits,
    relative_scores,
)
from timesfm_finish_position.chronos_readout import ReadoutRace


def test_shared_prediction_order_validates_weight_and_retains_fallback() -> None:
    race = ReadoutRace(
        np.array(["b", "a"]), np.array([0.5, 0.5]), np.array([2, 1]), np.array([np.nan, 0.5])
    )
    assert prediction_order(race, 0.5).tolist() == [1, 0]
    with pytest.raises(ValueError, match="market weight"):
        prediction_order(race, float("nan"))


def test_market_lock_certificate_and_actual_swap() -> None:
    race = ReadoutRace(
        np.array(["a", "b", "c"]),
        np.array([1.0, 0.5, 0.0]),
        np.array([1, 2, 3]),
        np.array([0.0, 0.5, 1.0]),
    )
    assert market_order_locked_by_range(race, 0.95) is True
    assert prediction_order(race, 0.95).tolist() == [0, 1, 2]
    assert market_order_locked_by_range(race, 0.0) is False
    assert prediction_order(race, 0.0).tolist() == [2, 1, 0]
    assert market_order_locked_by_range(race, 0.1) is False
    assert prediction_order(race, 0.1).tolist() == [2, 1, 0]
    assert market_order_locked_by_range(race, 0.6) is False
    assert prediction_order(race, 0.6).tolist() == [0, 1, 2]
    assert market_order_locked_by_range(race, 2 / 3 + 1e-16) is False


def test_market_lock_accounts_for_sparse_fallback() -> None:
    race = ReadoutRace(
        np.array(["a", "b", "c"]),
        np.array([1.0, 0.5, 0.0]),
        np.array([1, 2, 3]),
        np.array([np.nan, 0.3, 0.4]),
    )
    assert market_order_locked_by_range(race, 0.9) is True
    assert prediction_order(race, 0.9).tolist() == [0, 1, 2]


def test_tied_market_and_singleton_lock() -> None:
    race = ReadoutRace(
        np.array(["a", "b"]), np.array([0.5, 0.5]), np.array([1, 2]), np.array([0.0, 1.0])
    )
    assert market_order_locked_by_range(race, 0.9) is False
    assert market_order_locked_by_range(race, 1.0) is True
    singleton = ReadoutRace(np.array(["a"]), np.array([1.0]), np.array([1]), np.array([0.0]))
    assert market_order_locked_by_range(singleton, 0.0) is True


def test_market_lock_rejects_invalid_inputs() -> None:
    race = ReadoutRace(np.array(["a"]), np.array([np.nan]), np.array([1]), np.array([0.0]))
    with pytest.raises(ValueError, match="market weight"):
        market_order_locked_by_range(race, -0.1)
    with pytest.raises(ValueError, match="Nonfinite market"):
        market_order_locked_by_range(race, 0.9)


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": ["r1", "r1", "r2", "r2", "r3", "r3"],
                "horse_id": ["a", "b", "c", "d", "e", "f"],
                "race_date": ["20230101"] * 6,
                "venue_code": ["05"] * 6,
                "field_size": [2] * 6,
                "finish_position": [1, 2, 2, 1, 2, 1],
                "decimal_odds": [2.0, 4.0, 2.0, 4.0, 2.0, 4.0],
                "chronos_cpu": [0.8, 0.2, 0.2, 0.8, 0.2, 0.8],
            }
        ),
        path,
    )
    return path


@pytest.fixture
def mapping(tmp_path: Path) -> Path:
    path = tmp_path / "mapping.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": ["r1", "r2", "r3"],
                "cell_id": ["A", "B", "B"],
                "condition_code": ["005"] * 3,
                "race_identity": [None] * 3,
            }
        ),
        path,
    )
    return path


def test_count_selection_and_complete_readouts(source: Path, mapping: Path) -> None:
    result = diagnose_cells(head=source, lora=source, mapping=mapping, minimum_races=1, max_cells=1)
    cells = result["cells"]
    assert isinstance(cells, list)
    assert cells[0]["cell_id"] == "B"
    assert cells[0]["evaluated_races"] == 2
    assert cells[0]["market_hits"] == [0, 2, 2, 2, 2]
    assert cells[0]["readouts"][0]["delta"] == [2, 0, 0, 0, 0]
    assert cells[0]["head_known_runners"] == 4
    assert result["production_eligible"] is False
    table = pq.read_table(source)
    changed = table.set_column(
        table.schema.get_field_index("finish_position"),
        "finish_position",
        pa.array([2, 1, 1, 2, 1, 2]),
    )
    pq.write_table(changed, source)
    revised = diagnose_cells(
        head=source, lora=source, mapping=mapping, minimum_races=1, max_cells=1
    )
    revised_cells = revised["cells"]
    assert isinstance(revised_cells, list)
    assert revised_cells[0]["cell_id"] == "B"


def test_relative_ties_and_singletons() -> None:
    np.testing.assert_allclose(relative_scores(np.array([0.5, 0.5, np.nan])), [0.5, 0.5, np.nan])
    np.testing.assert_allclose(relative_scores(np.array([0.1, 0.3, 0.2])), [0.0, 1.0, 0.5])
    assert np.isnan(relative_scores(np.array([0.5, np.nan]))).all()


def test_dead_heat_is_one_race() -> None:
    race = ReadoutRace(
        np.array(["a", "b"]), np.array([1.0, 0.0]), np.array([1, 1]), np.array([np.nan, np.nan])
    )
    assert race_hits([race], 0.5) == [1, 1, 1, 1, 1]
    assert exact_hits([race], 0.5) == [1, 0, 0, 0, 0]
    assert exact_support([race]) == [1, 0, 0, 0, 0]
    with pytest.raises(ValueError, match="market weight"):
        exact_hits([race], -1.0)
    with pytest.raises(ValueError, match="market weight"):
        race_hits([race], -1.0)


@pytest.mark.parametrize(
    "column,values,message",
    [
        ("horse_id", ["a", "a", "c", "d", "e", "f"], "runner identity"),
        ("race_date", ["20240101"] * 6, "2023 development"),
        ("field_size", [3] * 6, "Incomplete"),
    ],
)
def test_source_guards(
    source: Path, mapping: Path, column: str, values: list[object], message: str
) -> None:
    table = pq.read_table(source)
    pq.write_table(
        table.set_column(table.schema.get_field_index(column), column, pa.array(values)), source
    )
    with pytest.raises(ValueError, match=message):
        diagnose_cells(head=source, lora=source, mapping=mapping)


def test_mapping_and_cohort_guards(source: Path, mapping: Path, tmp_path: Path) -> None:
    short = tmp_path / "short.parquet"
    pq.write_table(pq.read_table(source).slice(0, 2), short)
    with pytest.raises(ValueError, match="match exactly"):
        diagnose_cells(head=source, lora=short, mapping=mapping)
    bad_mapping = tmp_path / "bad-mapping.parquet"
    table = pq.read_table(mapping)
    pq.write_table(pa.concat_tables([table, table]), bad_mapping)
    with pytest.raises(ValueError, match="Duplicate canonical"):
        diagnose_cells(head=source, lora=source, mapping=bad_mapping)
    pq.write_table(table.slice(0, 2), bad_mapping)
    with pytest.raises(ValueError, match="Missing canonical"):
        diagnose_cells(head=source, lora=source, mapping=bad_mapping)
    with pytest.raises(ValueError, match="positive"):
        diagnose_cells(head=source, lora=source, mapping=mapping, max_cells=0)


def test_invalid_market_retains_count_selection(source: Path, mapping: Path) -> None:
    table = pq.read_table(source)
    changed = table.set_column(
        table.schema.get_field_index("decimal_odds"),
        "decimal_odds",
        pa.array([None] * 6, type=pa.float64()),
    )
    pq.write_table(changed, source)
    result = diagnose_cells(head=source, lora=source, mapping=mapping, minimum_races=1, max_cells=1)
    cells = result["cells"]
    assert isinstance(cells, list)
    assert cells[0]["cell_id"] == "B"
    assert cells[0]["complete_source_races"] == 2
    assert cells[0]["evaluated_races"] == 0


def test_unnamed_open_exclusion(source: Path, mapping: Path) -> None:
    table = pq.read_table(mapping)
    pq.write_table(table.set_column(2, "condition_code", pa.array(["005", "999", "999"])), mapping)
    result = diagnose_cells(head=source, lora=source, mapping=mapping, minimum_races=1)
    assert result["exclusions"] == {"unnamed_open": 2}
