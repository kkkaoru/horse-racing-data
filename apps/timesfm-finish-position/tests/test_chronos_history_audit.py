from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.chronos_history_audit import repair_non_domestic_context


@pytest.fixture
def history(tmp_path: Path) -> Path:
    path = tmp_path / "history.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": ["jra:domestic", "jra:foreign", "nar:flat"],
                "venue_code": ["05", "A4", "54"],
                "field_size": [10, 2, 10],
                "finish_position": [2, 10, 3],
                "performance_rating": [0.8, -8.0, 0.7],
            }
        ),
        path,
    )
    return path


def test_official_denominator_preserves_domestic_and_nar(history: Path, tmp_path: Path) -> None:
    output = tmp_path / "repaired.parquet"
    repair_non_domestic_context(history=history, output=output, declared_sizes={"jra:foreign": 10})
    result = pq.read_table(output)
    assert result["performance_rating"].to_pylist() == [0.8, 0.0, 0.7]
    assert result["field_size"].to_pylist() == [10, 10, 10]
    assert pq.read_table(history)["performance_rating"].to_pylist() == [0.8, -8.0, 0.7]


def test_domestic_transfer_context_uses_official_denominator(history: Path, tmp_path: Path) -> None:
    output = tmp_path / "transfers.parquet"
    repair_non_domestic_context(
        history=history,
        output=output,
        declared_sizes={"jra:domestic": 11, "jra:foreign": 10},
        include_domestic=True,
    )
    result = pq.read_table(output)
    assert result["performance_rating"].to_pylist() == [0.9, 0.0, 0.7]
    assert result["field_size"].to_pylist() == [11, 10, 10]


def test_missing_metadata(history: Path, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly cover"):
        repair_non_domestic_context(
            history=history, output=tmp_path / "out.parquet", declared_sizes={}
        )


@pytest.mark.parametrize("size", [0, -1, True])
def test_invalid_official_counts(history: Path, tmp_path: Path, size: int) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        repair_non_domestic_context(
            history=history, output=tmp_path / "out.parquet", declared_sizes={"jra:foreign": size}
        )


def test_inconsistent_official_count(history: Path, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="conflicts"):
        repair_non_domestic_context(
            history=history, output=tmp_path / "out.parquet", declared_sizes={"jra:foreign": 9}
        )
