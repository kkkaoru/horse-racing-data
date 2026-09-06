from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.data import ACTION_COLUMNS


@pytest.fixture
def race_parquet(tmp_path: Path) -> Path:
    rows = 40
    years = [2023] * 10 + [2024] * 10 + [2025] * 10 + [2026] * 10
    dates = [f"{year}{month:02d}01" for year in range(2023, 2027) for month in range(1, 11)]
    payload: dict[str, Sequence[object]] = {
        "race_id": [f"nar:{index:04d}" for index in range(rows)],
        "race_date": dates,
        "race_year": years,
        "keibajo_code": ["30"] * 20 + ["44"] * 20,
        "kyori": [1_000] + [1_200 + index * 25 for index in range(1, rows)],
        "track_code": ["2"] * rows,
        "current_baba_condition": ["1"] * rows,
        "field_size": [12] * rows,
        "favorite_market_share": [0.2 + index / 100 for index in range(rows)],
        "market_entropy": [1.5] * rows,
        "model_disagrees": [index % 2 for index in range(rows)],
        "rank_disagreement_mean": [0.5] * rows,
        "top3_overlap": [2] * rows,
        "current_top_margin": [0.1] * rows,
        **{
            column: [1 + ((row + action) % 12) for row in range(rows)]
            for action, column in enumerate(ACTION_COLUMNS)
        },
    }
    path = tmp_path / "races.parquet"
    pq.write_table(pa.table(payload), path)
    return path
