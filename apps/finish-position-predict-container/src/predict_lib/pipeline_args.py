"""Pure builders for the v7-lineage feature-pipeline subprocess argv vectors.

The container shells out to the reused viewer feature scripts (the DuckDB base
build + the per-category v7 layer chain). Getting the flags right is the whole
point of "predict today's races": the base build must run in ``--target-date``
mode so it emits feature rows for that day's races (including UPCOMING ones whose
``finish_position`` is still NULL), and every layer must receive its required
``--pg-url`` / ``--config`` / ``--category`` / ``--from-date`` flags. That arg
shaping is deterministic, so it lives here and is unit-tested; ``pipeline_runner``
only executes the vectors and reads the parquet.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Final

from .model_meta import Category

PYTHON_BIN: Final[str] = "python"
LINEAGE_SCRIPT: Final[str] = "add-grade-race-lineage-features.py"
TRAINER_SCRIPT: Final[str] = "add-trainer-stable-affinity-features.py"
HISTORY_FROM_DATE: Final[str] = "20100101"

# Per-category v7 layer chain (script basename order). Mirrors the per-category
# Pipeline sections of FINISH_POSITION_MODEL_V7_LINEAGE.md (sections 4 / 8 / 9).
LAYER_CHAIN: Final[dict[Category, tuple[str, ...]]] = {
    "jra": (
        LINEAGE_SCRIPT,
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        TRAINER_SCRIPT,
    ),
    "nar": (
        LINEAGE_SCRIPT,
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
    ),
    "ban-ei": (
        LINEAGE_SCRIPT,
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        "add-banei-futan-class-features.py",
        "add-banei-grade-career-features.py",
    ),
}

# Lineage config file basename per category (lives under lineage-races/).
LINEAGE_CONFIG_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "jra.json",
    "nar": "nar.json",
    "ban-ei": "ban-ei.json",
}

# The trainer layer reads its source rows from jvd_se (jra) or nvd_se (nar).
# Ban-ei never runs the trainer layer, so it has no entry here.
TRAINER_CATEGORY_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "jra",
    "nar": "nar",
}


def build_base_argv(
    builder_path: Path,
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    output_dir: Path,
) -> list[str]:
    """Argv for the DuckDB base build in ``--target-date`` (upcoming) mode."""
    return [
        PYTHON_BIN,
        str(builder_path),
        "--category",
        category,
        "--target-date",
        target_date,
        "--days-ahead",
        str(days_ahead),
        "--pg-url",
        database_url,
        "--output-dir",
        str(output_dir),
    ]


def _extra_layer_args(
    script: str,
    category: Category,
    layer_dir: Path,
) -> list[str]:
    """Per-script extra flags (lineage config / trainer category)."""
    if script == LINEAGE_SCRIPT:
        config_name = LINEAGE_CONFIG_BY_CATEGORY[category]
        return ["--config", str(layer_dir / "lineage-races" / config_name)]
    if script == TRAINER_SCRIPT:
        return ["--category", TRAINER_CATEGORY_BY_CATEGORY[category]]
    return []


def build_layer_argv(
    script: str,
    category: Category,
    layer_dir: Path,
    input_dir: Path,
    output_dir: Path,
    database_url: str,
) -> list[str]:
    """Argv for one v7 layer script (input/output dirs + per-script flags).

    Every layer reads history straight from Postgres, so ``--pg-url`` is always
    threaded through; ``--from-date`` bounds the history scan to the supported
    span. The lineage layer additionally needs ``--config`` and the trainer layer
    needs ``--category``.
    """
    base = [
        PYTHON_BIN,
        str(layer_dir / script),
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
        "--pg-url",
        database_url,
        "--from-date",
        HISTORY_FROM_DATE,
    ]
    return base + _extra_layer_args(script, category, layer_dir)


def layer_chain_for(category: Category) -> Sequence[str]:
    """Return the ordered v7 layer scripts for ``category``."""
    return LAYER_CHAIN[category]
