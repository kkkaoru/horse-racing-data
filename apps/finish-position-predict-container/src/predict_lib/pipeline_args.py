"""Pure builders for the v7-lineage feature-pipeline subprocess argv vectors.

The container shells out to the reused viewer feature scripts (the DuckDB base
build + the per-category FULL layer chain). Getting the flags right is the whole
point of "predict today's races": the base build must run in ``--target-date``
mode so it emits feature rows for that day's races (including UPCOMING ones whose
``finish_position`` is still NULL), and every layer must receive exactly the
flags it declares — no more, no less. Passing an unknown flag (e.g. ``--pg-url``
to the pure-DuckDB ``add-race-internal-features.py``) makes argparse abort, so
the per-script flag surface is encoded here and unit-tested; ``pipeline_runner``
only executes the vectors and reads the parquet.

The chains reproduce the EXACT feature sets the production models were trained
on (FINISH_POSITION_MODEL_V7_LINEAGE.md §4 / §8 / §9 / §10 and
FINISH_POSITION_MODEL_V6_STACKED.md §2):

* JRA (226 features) — full v6 base chain (race-internal → market-signal →
  sectional-and-weight → futan-juryo → workout → near-miss) then the four v7
  layers (lineage → head-to-head → baba-pedigree → trainer).
* NAR (175 features) — lighter v6 base chain (race-internal → near-miss) then
  the v7 layers WITHOUT the trainer layer (trainer is counter-productive on NAR
  per §8); NAR was never built with the market/sectional/futan/workout layers.
* Ban-ei (111 features) — distinct base (no JRA v6 layers) then lineage →
  head-to-head → baba-pedigree → ban-ei futan-class → ban-ei grade-career.

The one-shot "v3 merger" mentioned in the V6 doc only re-prioritised the VALUE
of market-signal columns that ``add-market-signal-features.py`` already computes
straight from Postgres; it adds no new feature NAMES and is not part of the
automated 21y v7 build, so it is intentionally NOT reproduced here.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Final

from .model_meta import Category

PYTHON_BIN: Final[str] = "python"

# v6 base layers (JRA full chain order).
RACE_INTERNAL_SCRIPT: Final[str] = "add-race-internal-features.py"
MARKET_SIGNAL_SCRIPT: Final[str] = "add-market-signal-features.py"
SECTIONAL_WEIGHT_SCRIPT: Final[str] = "add-sectional-and-weight-features.py"
FUTAN_JURYO_SCRIPT: Final[str] = "add-futan-juryo-features.py"
WORKOUT_SCRIPT: Final[str] = "add-workout-features.py"
NEAR_MISS_SCRIPT: Final[str] = "add-near-miss-features.py"

# v7 layers (shared across categories).
LINEAGE_SCRIPT: Final[str] = "add-grade-race-lineage-features.py"
HEAD_TO_HEAD_SCRIPT: Final[str] = "add-head-to-head-features.py"
BABA_PEDIGREE_SCRIPT: Final[str] = "add-baba-pedigree-affinity-features.py"
TRAINER_SCRIPT: Final[str] = "add-trainer-stable-affinity-features.py"

# Ban-ei-specific v7 layers.
BANEI_FUTAN_CLASS_SCRIPT: Final[str] = "add-banei-futan-class-features.py"
BANEI_GRADE_CAREER_SCRIPT: Final[str] = "add-banei-grade-career-features.py"

HISTORY_FROM_DATE: Final[str] = "20100101"

# Per-category full layer chain (script basename order). Mirrors the per-category
# Pipeline sections of FINISH_POSITION_MODEL_V7_LINEAGE.md (§4 / §8 / §9) +
# FINISH_POSITION_MODEL_V6_STACKED.md (§2), validated against each model's
# metadata.json feature_names (226 / 175 / 111).
LAYER_CHAIN: Final[dict[Category, tuple[str, ...]]] = {
    "jra": (
        RACE_INTERNAL_SCRIPT,
        MARKET_SIGNAL_SCRIPT,
        SECTIONAL_WEIGHT_SCRIPT,
        FUTAN_JURYO_SCRIPT,
        WORKOUT_SCRIPT,
        NEAR_MISS_SCRIPT,
        LINEAGE_SCRIPT,
        HEAD_TO_HEAD_SCRIPT,
        BABA_PEDIGREE_SCRIPT,
        TRAINER_SCRIPT,
    ),
    "nar": (
        RACE_INTERNAL_SCRIPT,
        NEAR_MISS_SCRIPT,
        LINEAGE_SCRIPT,
        HEAD_TO_HEAD_SCRIPT,
        BABA_PEDIGREE_SCRIPT,
    ),
    "ban-ei": (
        LINEAGE_SCRIPT,
        HEAD_TO_HEAD_SCRIPT,
        BABA_PEDIGREE_SCRIPT,
        BANEI_FUTAN_CLASS_SCRIPT,
        BANEI_GRADE_CAREER_SCRIPT,
    ),
}

# Scripts that read history straight from Postgres need ``--pg-url``. The pure
# DuckDB ``add-race-internal-features.py`` reads only its input parquet, so it
# must NOT receive ``--pg-url`` (argparse would abort on the unknown flag).
SCRIPTS_WITH_PG_URL: Final[frozenset[str]] = frozenset(
    {
        MARKET_SIGNAL_SCRIPT,
        SECTIONAL_WEIGHT_SCRIPT,
        FUTAN_JURYO_SCRIPT,
        WORKOUT_SCRIPT,
        NEAR_MISS_SCRIPT,
        LINEAGE_SCRIPT,
        HEAD_TO_HEAD_SCRIPT,
        BABA_PEDIGREE_SCRIPT,
        TRAINER_SCRIPT,
        BANEI_FUTAN_CLASS_SCRIPT,
        BANEI_GRADE_CAREER_SCRIPT,
    }
)

# Scripts that accept ``--from-date`` to bound their Postgres history scan. The
# race-internal layer has no Postgres scan and so takes no ``--from-date``.
SCRIPTS_WITH_FROM_DATE: Final[frozenset[str]] = SCRIPTS_WITH_PG_URL

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
    """Argv for the DuckDB base build in ``--target-date`` (upcoming) mode.

    ``--allow-empty-targets`` is always passed in upcoming mode so a category
    with no races on ``target_date`` (e.g. JRA on a NAR-only weekday) writes an
    empty parquet directory and exits 0 instead of aborting the whole pipeline.
    """
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
        "--allow-empty-targets",
    ]


def _pg_args(script: str, database_url: str) -> list[str]:
    """``--pg-url`` for Postgres-reading layers; nothing for the pure layer."""
    if script in SCRIPTS_WITH_PG_URL:
        return ["--pg-url", database_url]
    return []


def _from_date_args(script: str) -> list[str]:
    """``--from-date`` to bound the history scan for Postgres-reading layers."""
    if script in SCRIPTS_WITH_FROM_DATE:
        return ["--from-date", HISTORY_FROM_DATE]
    return []


def _config_args(script: str, category: Category, layer_dir: Path) -> list[str]:
    """``--config`` for the lineage layer (per-category target-race mapping)."""
    if script == LINEAGE_SCRIPT:
        config_name = LINEAGE_CONFIG_BY_CATEGORY[category]
        return ["--config", str(layer_dir / "lineage-races" / config_name)]
    return []


def _trainer_category_args(script: str, category: Category) -> list[str]:
    """``--category`` for the trainer layer (jvd_se vs nvd_se source select)."""
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
    """Argv for one layer script (input/output dirs + only its declared flags).

    Each layer declares which flags it accepts; passing a flag a script does not
    declare makes argparse abort, so the surface is encoded per-script:

    * every layer takes ``--input-dir`` / ``--output-dir``;
    * Postgres-reading layers additionally take ``--pg-url`` and ``--from-date``;
    * the lineage layer additionally takes ``--config``;
    * the trainer layer additionally takes ``--category``.
    """
    base = [
        PYTHON_BIN,
        str(layer_dir / script),
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
    ]
    return (
        base
        + _pg_args(script, database_url)
        + _from_date_args(script)
        + _config_args(script, category, layer_dir)
        + _trainer_category_args(script, category)
    )


def layer_chain_for(category: Category) -> Sequence[str]:
    """Return the ordered full layer chain for ``category``."""
    return LAYER_CHAIN[category]
