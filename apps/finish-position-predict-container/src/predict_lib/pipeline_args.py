"""Pure builders for the v8 production feature-pipeline subprocess argv vectors.

The container shells out to the reused viewer feature scripts (the DuckDB base
build + the per-category FULL layer chain). Getting the flags right is the whole
point of "predict today's races": the base build must run in ``--target-date``
mode so it emits feature rows for that day's races (including UPCOMING ones whose
``finish_position`` is still NULL), and every layer must receive exactly the
flags it declares — no more, no less. Passing an unknown flag (e.g. ``--pg-url``
to the pure-DuckDB ``add-race-internal-features.py``) makes argparse abort, so
the per-script flag surface is encoded here and unit-tested; ``pipeline_runner``
only executes the vectors and reads the parquet.

The chains reproduce the EXACT feature sets the production v8 models were
trained on
(docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md §4 /
§8 / §9 / §10 +
docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V6_STACKED.md §2 +
the v8 iter9 / iter12 / iter14 builds in ``tmp/v8/``):

* JRA iter14-jra-cb-pacestyle-course-v8 (241 features) — full v6 base chain
  (race-internal → market-signal → sectional-and-weight → futan-juryo →
  workout → near-miss) then the four v7 layers (lineage → head-to-head →
  baba-pedigree → trainer) then the two v8 layers (pacestyle → course).
* NAR iter12-nar-xgb-hpo-v8 (192 features) — lighter v6 base chain
  (race-internal → near-miss) then the v7 layers (lineage → head-to-head →
  baba-pedigree → trainer; trainer is INCLUDED for v8 NAR, unlike v7-lineage
  NAR where it was dropped) then the v8 pacestyle layer.
* Ban-ei (111 features, unchanged from v7-lineage) — distinct base (no JRA v6
  layers) then lineage → head-to-head → baba-pedigree → ban-ei futan-class →
  ban-ei grade-career.

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

# v8 layers (iter9 pacestyle + iter14 course numerical).
PACESTYLE_SCRIPT: Final[str] = "add-pacestyle-features.py"
COURSE_NUMERICAL_SCRIPT: Final[str] = "add-course-numerical-features.py"

# iter26 relationship layer (馬体重 x 斤量 x 馬齢 x 距離 x タイム interaction +
# history-normalized speed, 12 cols). Required at inference for the per-class
# ensemble residual / chain members trained on the relationship feature set
# (JRA iter26 = 254 cols, NAR iter30 residual = 174 cols). Reads PG history,
# so it takes ``--pg-url`` + ``--from-date`` + a source-filter ``--category``.
RELATIONSHIP_SCRIPT: Final[str] = "add-relationship-r1-features.py"

HISTORY_FROM_DATE: Final[str] = "20100101"

# Baked course-numerical lookup parquet. Mirrors the
# COPY apps/pc-keiba-viewer/finish-position/lookups/course-numerical-features.parquet
# in the Dockerfile so the path is identical inside the image.
COURSE_LOOKUP_PATH: Final[Path] = Path(
    "/app/lookups/course-numerical-features.parquet"
)

# Per-category full layer chain (script basename order). Mirrors the per-category
# Pipeline sections of
# docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md
# (§4 / §8 / §9) +
# docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V6_STACKED.md (§2)
# plus the v8 iter9 (pacestyle) and iter14 (course numerical) layers introduced
# in the tmp/v8/iter{9,12,14} builds. Validated against each model's
# metadata.json feature_names (241 JRA / 192 NAR / 111 Ban-ei).
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
        PACESTYLE_SCRIPT,
        COURSE_NUMERICAL_SCRIPT,
        RELATIONSHIP_SCRIPT,
    ),
    "nar": (
        RACE_INTERNAL_SCRIPT,
        NEAR_MISS_SCRIPT,
        LINEAGE_SCRIPT,
        HEAD_TO_HEAD_SCRIPT,
        BABA_PEDIGREE_SCRIPT,
        TRAINER_SCRIPT,
        PACESTYLE_SCRIPT,
        RELATIONSHIP_SCRIPT,
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
# must NOT receive ``--pg-url`` (argparse would abort on the unknown flag). The
# v8 ``add-course-numerical-features.py`` reads a baked lookup parquet (no PG)
# so it is also intentionally omitted here.
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
        PACESTYLE_SCRIPT,
        RELATIONSHIP_SCRIPT,
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

# Scripts that accept ``--category {jra,nar}`` (selects the source filter +
# PG table). The trainer layer disambiguates jvd_se vs nvd_se; the pacestyle
# layer disambiguates which ``race_running_style_model_predictions`` rows to
# load. Ban-ei runs neither layer, so it has no entry here.
TRAINER_CATEGORY_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "jra",
    "nar": "nar",
}
PACESTYLE_CATEGORY_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "jra",
    "nar": "nar",
}
# The relationship layer takes ``--category {jra,nar,ban-ei,all}`` to select the
# PG source filter (jvd_se vs nvd_se, and the ban-ei keibajo carve-out). Only
# jra / nar run this layer (it is appended to those two chains), so the map has
# no ban-ei entry — mirrors TRAINER / PACESTYLE.
RELATIONSHIP_CATEGORY_BY_CATEGORY: Final[dict[Category, str]] = {
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
    realtime_odds_path: Path | None = None,
) -> list[str]:
    """Argv for the DuckDB base build in ``--target-date`` (upcoming) mode.

    ``--allow-empty-targets`` is always passed in upcoming mode so a category
    with no races on ``target_date`` (e.g. JRA on a NAR-only weekday) writes an
    empty parquet directory and exits 0 instead of aborting the whole pipeline.

    When ``realtime_odds_path`` is provided the ``--realtime-odds`` flag is
    appended so the DuckDB builder COALESCEs real-time tansho odds (from the
    hot worker) over the nvd_se / jvd_se fallback for UPCOMING races, reviving
    ``odds_score`` / ``popularity_score`` at inference time. Absent → the
    builder falls back to the current NULL-odds path unchanged.
    """
    argv = [
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
    if realtime_odds_path is not None:
        argv += ["--realtime-odds", str(realtime_odds_path)]
    return argv


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


def _pacestyle_category_args(script: str, category: Category) -> list[str]:
    """``--category`` for the pacestyle layer (rs preds source filter)."""
    if script == PACESTYLE_SCRIPT:
        return ["--category", PACESTYLE_CATEGORY_BY_CATEGORY[category]]
    return []


def _relationship_category_args(script: str, category: Category) -> list[str]:
    """``--category`` for the relationship layer (jvd_se vs nvd_se source filter)."""
    if script == RELATIONSHIP_SCRIPT:
        return ["--category", RELATIONSHIP_CATEGORY_BY_CATEGORY[category]]
    return []


def _course_lookup_args(script: str) -> list[str]:
    """``--course-lookup`` for the course-numerical layer (baked parquet path)."""
    if script == COURSE_NUMERICAL_SCRIPT:
        return ["--course-lookup", str(COURSE_LOOKUP_PATH)]
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
    * the trainer + pacestyle + relationship layers additionally take
      ``--category``;
    * the course-numerical layer additionally takes ``--course-lookup``.
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
        + _pacestyle_category_args(script, category)
        + _relationship_category_args(script, category)
        + _course_lookup_args(script)
    )


def layer_chain_for(category: Category) -> Sequence[str]:
    """Return the ordered full layer chain for ``category``."""
    return LAYER_CHAIN[category]
