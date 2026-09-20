"""Identity-safe neural dataset loading and chronological subsetting."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from .model_interface import ModelDataset
from .tabular_evaluation import load_tabular_dataset

CATEGORICAL_SOURCE_COLUMNS = {
    "horse_id": "horse_id",
    "jockey_id": "jockey_code",
    "trainer_id": "trainer_code",
    "owner_id": "owner_id",
    "sire_id": "sire_id",
    "dam_id": "dam_id",
    "dam_sire_id": "dam_sire_id",
    "racecourse": "venue_code",
    "race_class": "class_code",
}
DEFAULT_CATEGORICAL_NAMES = tuple(CATEGORICAL_SOURCE_COLUMNS)


def load_neural_dataset(
    tabular_path: Path,
    history_path: Path,
    *,
    categorical_names: tuple[str, ...] = DEFAULT_CATEGORICAL_NAMES,
) -> ModelDataset:
    """Load aligned numeric and categorical data from one snapshot generation."""
    if len(set(categorical_names)) != len(categorical_names):
        raise ValueError("categorical names must be unique")
    unknown = tuple(name for name in categorical_names if name not in CATEGORICAL_SOURCE_COLUMNS)
    if unknown:
        raise ValueError(f"unsupported categorical names: {','.join(unknown)}")
    numeric = load_tabular_dataset(tabular_path)
    source_names = tuple(CATEGORICAL_SOURCE_COLUMNS[name] for name in categorical_names)
    history_columns = tuple(sorted({"race_id", "race_date", "horse_id", *source_names}))
    history = pq.read_table(history_path, columns=list(history_columns))
    history_races = np.asarray(history.column("race_id").to_pylist(), dtype=np.str_)
    history_dates = np.asarray(history.column("race_date").to_pylist(), dtype=np.str_)
    history_horses = np.asarray(history.column("horse_id").to_pylist(), dtype=np.str_)
    if not np.array_equal(history_races, numeric.race_ids) or not np.array_equal(
        history_horses, numeric.horse_ids
    ):
        raise RuntimeError("neural numeric and categorical runner identities do not align")
    if not np.array_equal(history_dates, numeric.race_dates):
        raise RuntimeError("neural numeric and categorical race dates do not align")
    categorical = {
        name: np.asarray(
            history.column(CATEGORICAL_SOURCE_COLUMNS[name]).to_pylist(), dtype=np.str_
        )
        for name in categorical_names
    }
    return ModelDataset(
        race_ids=numeric.race_ids,
        horse_ids=numeric.horse_ids,
        race_dates=numeric.race_dates,
        numeric=numeric.features,
        numeric_names=numeric.feature_names,
        categorical=categorical,
        decimal_odds=numeric.decimal_odds,
        finish_positions=numeric.finish_positions,
    )


def subset_model_dataset(dataset: ModelDataset, mask: np.ndarray) -> ModelDataset:
    """Subset every model input column with one shared chronological mask."""
    if mask.shape != (dataset.rows,) or mask.dtype != np.bool_:
        raise ValueError("neural dataset mask must be an aligned boolean vector")
    return ModelDataset(
        race_ids=dataset.race_ids[mask],
        horse_ids=dataset.horse_ids[mask],
        race_dates=dataset.race_dates[mask],
        numeric=dataset.numeric[mask],
        numeric_names=dataset.numeric_names,
        categorical={name: values[mask] for name, values in dataset.categorical.items()},
        decimal_odds=dataset.decimal_odds[mask],
        finish_positions=(
            dataset.finish_positions[mask] if dataset.finish_positions is not None else None
        ),
    )
