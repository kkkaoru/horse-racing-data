from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.neural_dataset import (
    load_neural_dataset,
    subset_model_dataset,
)


def _tabular(path: Path) -> None:
    pq.write_table(
        pa.table(
            {
                "race_id": ["r1", "r1"],
                "race_date": ["20240101", "20240101"],
                "horse_id": ["h1", "h2"],
                "finish_position": [1, 2],
                "decimal_odds": [2.0, 3.0],
                "speed": [1.0, 0.5],
            }
        ),
        path,
    )


def _history(path: Path, horses: list[str]) -> None:
    pq.write_table(
        pa.table(
            {
                "race_id": ["r1", "r1"],
                "race_date": ["20240101", "20240101"],
                "horse_id": horses,
                "jockey_code": ["j1", "j2"],
                "owner_id": ["o1", "o2"],
            }
        ),
        path,
    )


def test_load_and_subset_neural_dataset_keep_one_identity_order(tmp_path: Path) -> None:
    tabular = tmp_path / "tabular.parquet"
    history = tmp_path / "history.parquet"
    _tabular(tabular)
    _history(history, ["h1", "h2"])
    dataset = load_neural_dataset(
        tabular,
        history,
        categorical_names=("horse_id", "jockey_id", "owner_id"),
    )
    assert dataset.numeric_names == ("speed",)
    assert dataset.categorical["jockey_id"].tolist() == ["j1", "j2"]
    subset = subset_model_dataset(dataset, np.asarray([True, False]))
    assert subset.horse_ids.tolist() == ["h1"]
    assert subset.numeric.tolist() == [[1.0]]
    assert subset.finish_positions is not None
    assert subset.finish_positions.tolist() == [1]


def test_neural_dataset_fails_closed_on_identity_or_category_drift(tmp_path: Path) -> None:
    tabular = tmp_path / "tabular.parquet"
    history = tmp_path / "history.parquet"
    _tabular(tabular)
    _history(history, ["other", "h2"])
    with pytest.raises(RuntimeError, match="runner identities"):
        load_neural_dataset(tabular, history, categorical_names=("horse_id",))
    with pytest.raises(ValueError, match="unsupported categorical"):
        load_neural_dataset(tabular, history, categorical_names=("unknown",))
