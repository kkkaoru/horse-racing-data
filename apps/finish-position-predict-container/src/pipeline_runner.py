"""Build the v7-lineage feature parquet for TODAY's races and load it.

Deploy-time I/O glue (not unit-tested; verified per DEPLOY.md). It reuses the
unchanged repo feature pipeline that already produced the training parquet:

  1. ``finish_position_features_duckdb.py`` builds the base feature parquet from
     Postgres in ``--target-date`` mode for the requested category over
     [target_date, target_date + days_ahead]. That mode emits feature rows for
     the day's races INCLUDING UPCOMING ones (``finish_position`` still NULL):
     historical aggregates are computed from prior races only
     (``h.race_date < t.race_date``), so the target race's own outcome never
     leaks in and the vector is computable before the race is run.
  2. The v7 layer scripts append the lineage / head-to-head / baba-pedigree /
     trainer (JRA) / ban-ei layers, matching the per-category pipeline in
     FINISH_POSITION_MODEL_V7_LINEAGE.md sections 4 / 8 / 9. Each preserves
     UPCOMING rows via LEFT JOIN (history side filtered to finish_position NOT
     NULL), so today's races survive with NULL/0 lineage features.
  3. The final parquet is read into per-race ordered feature dicts keyed by the
     canonical ``race_id`` so ``predict_upcoming`` can score each race.

The scripts live at ``/app/pipeline`` in the image (see Dockerfile). This module
only wires arguments + reads the result; all feature logic stays in the reused
scripts so there is a single source of truth. The argv vectors themselves are
built by the pure, unit-tested ``predict_lib.pipeline_args`` builders.
"""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

from predict_lib.model_meta import Category
from predict_lib.pipeline_args import (
    build_base_argv,
    build_layer_argv,
    layer_chain_for,
)

PIPELINE_DIR: Final[Path] = Path("/app/pipeline")
DUCKDB_BUILDER: Final[Path] = PIPELINE_DIR / "finish_position_features_duckdb.py"
LAYER_DIR: Final[Path] = PIPELINE_DIR / "finish-position-features"
WORK_DIR: Final[Path] = Path("/tmp/predict-upcoming")
RACE_ID_FIELD: Final[str] = "race_id"


def _run(args: Sequence[str]) -> None:
    subprocess.run(list(args), check=True)


def _final_parquet_dir(category: Category) -> Path:
    return WORK_DIR / f"feat-{category}-v7-final"


def build_upcoming_feature_rows(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
) -> Mapping[str, list[Mapping[str, object]]]:
    """Run the pipeline and return ``race_id`` -> ordered entry feature dicts."""
    import pandas as pd

    final_dir = _final_parquet_dir(category)
    _build_pipeline(category, target_date, days_ahead, database_url, final_dir)
    frame = pd.read_parquet(final_dir)
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for race_id, race_frame in frame.groupby(RACE_ID_FIELD):
        grouped[str(race_id)] = list(race_frame.to_dict(orient="records"))
    return grouped


def _build_pipeline(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    final_dir: Path,
) -> None:
    """Run the DuckDB base build then each v7 layer into ``final_dir``."""
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    base_dir = WORK_DIR / f"feat-{category}-base"
    _run(
        build_base_argv(
            DUCKDB_BUILDER,
            category,
            target_date,
            days_ahead,
            database_url,
            base_dir,
        )
    )
    current = base_dir
    for index, script in enumerate(layer_chain_for(category)):
        nxt = WORK_DIR / f"feat-{category}-layer-{index}"
        _run(
            build_layer_argv(
                script,
                category,
                LAYER_DIR,
                current,
                nxt,
                database_url,
            )
        )
        current = nxt
    current.rename(final_dir)
