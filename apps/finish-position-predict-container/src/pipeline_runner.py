"""Build the v7-lineage feature parquet for the UPCOMING window and load it.

Deploy-time I/O glue (not unit-tested; verified per DEPLOY.md). It reuses the
unchanged repo feature pipeline that already produced the training parquet:

  1. ``finish_position_features_duckdb.py`` builds the base feature parquet from
     Postgres for the requested category over [today, today + days_ahead].
  2. The v7 layer scripts append the lineage / head-to-head / baba-pedigree /
     trainer (JRA) / ban-ei layers, matching the per-category pipeline in
     FINISH_POSITION_MODEL_V7_LINEAGE.md sections 4 / 8 / 9.
  3. The final parquet is read into per-race ordered feature dicts keyed by the
     canonical ``race_id`` so ``predict_upcoming`` can score each race.

The scripts live at ``/app/pipeline`` in the image (see Dockerfile). This module
only wires arguments + reads the result; all feature logic stays in the reused
scripts so there is a single source of truth.
"""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

from predict_lib.model_meta import Category

PIPELINE_DIR: Final[Path] = Path("/app/pipeline")
DUCKDB_BUILDER: Final[Path] = PIPELINE_DIR / "finish_position_features_duckdb.py"
LAYER_DIR: Final[Path] = PIPELINE_DIR / "finish-position-features"
WORK_DIR: Final[Path] = Path("/tmp/predict-upcoming")
RACE_ID_FIELD: Final[str] = "race_id"

# Per-category v7 layer chain (script basename + config). Mirrors the Pipeline
# sections in FINISH_POSITION_MODEL_V7_LINEAGE.md.
LAYER_CHAIN: Final[dict[Category, tuple[str, ...]]] = {
    "jra": (
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        "add-trainer-stable-affinity-features.py",
    ),
    "nar": (
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
    ),
    "ban-ei": (
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        "add-banei-futan-class-features.py",
        "add-banei-grade-career-features.py",
    ),
}


def _run(args: Sequence[str]) -> None:
    subprocess.run(list(args), check=True)


def _final_parquet_dir(category: Category) -> Path:
    return WORK_DIR / f"feat-{category}-v7-final"


def build_upcoming_feature_rows(
    category: Category,
    days_ahead: int,
    database_url: str,
) -> Mapping[str, list[Mapping[str, object]]]:
    """Run the pipeline and return ``race_id`` -> ordered entry feature dicts."""
    import pandas as pd

    final_dir = _final_parquet_dir(category)
    _build_pipeline(category, days_ahead, database_url, final_dir)
    frame = pd.read_parquet(final_dir)
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for race_id, race_frame in frame.groupby(RACE_ID_FIELD):
        grouped[str(race_id)] = list(race_frame.to_dict(orient="records"))
    return grouped


def _build_pipeline(
    category: Category,
    days_ahead: int,
    database_url: str,
    final_dir: Path,
) -> None:
    """Run the DuckDB base build then each v7 layer into ``final_dir``."""
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    base_dir = WORK_DIR / f"feat-{category}-base"
    _run(
        [
            "python",
            str(DUCKDB_BUILDER),
            "--category",
            category,
            "--upcoming-days-ahead",
            str(days_ahead),
            "--pg-url",
            database_url,
            "--output-dir",
            str(base_dir),
        ]
    )
    current = base_dir
    for index, script in enumerate(LAYER_CHAIN[category]):
        nxt = WORK_DIR / f"feat-{category}-layer-{index}"
        _run(
            [
                "python",
                str(LAYER_DIR / script),
                "--input-dir",
                str(current),
                "--output-dir",
                str(nxt),
            ]
        )
        current = nxt
    current.rename(final_dir)
