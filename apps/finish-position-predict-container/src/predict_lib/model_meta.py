"""Container-baked model-version / architecture / R2-key mapping.

Single source of truth for the model the daily-prediction container LOADS and
SCORES TODAY's upcoming races with. As of 2026-06-04 the v8 production deploy
(JRA=iter14-jra-cb-pacestyle-course-v8, NAR=iter12-nar-xgb-hpo-v8) is fully
cut over: the historical PG predictions table + ``finish_position_active_models``
were flipped, the iter12/iter14 BOOSTERS are baked under
``models/finish-position/{nar/jra}/<iter-version>/``, and the v8 feature
layers (pacestyle + course) are wired into ``pipeline_args.LAYER_CHAIN``.

Ban-ei is unchanged from v7-lineage — v8 only retrained the JRA + NAR boosters.
"""

from __future__ import annotations

from typing import Final, Literal, get_args

Category = Literal["jra", "nar", "ban-ei"]
Architecture = Literal["catboost", "xgboost"]

CATEGORIES: Final[tuple[Category, ...]] = get_args(Category)

MODEL_VERSION_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "iter14-jra-cb-pacestyle-course-v8",
    "nar": "iter12-nar-xgb-hpo-v8",
    "ban-ei": "banei-cb-v7-lineage-wf-21y",
}

ARCHITECTURE_BY_CATEGORY: Final[dict[Category, Architecture]] = {
    "jra": "catboost",
    "nar": "xgboost",
    "ban-ei": "catboost",
}

FEATURE_COUNT_BY_CATEGORY: Final[dict[Category, int]] = {
    "jra": 241,
    "nar": 192,
    "ban-ei": 111,
}

R2_KEY_PREFIX: Final[str] = "finish-position"
MODEL_FILE_NAME: Final[str] = "model.json"
METADATA_FILE_NAME: Final[str] = "metadata.json"


def is_category(value: str) -> bool:
    """Return True when ``value`` is one of the supported categories."""
    return value in MODEL_VERSION_BY_CATEGORY


def resolve_category(value: str) -> Category:
    """Narrow a raw string to ``Category`` or raise ``ValueError``."""
    for category in CATEGORIES:
        if category == value:
            return category
    message = f"unsupported category: {value}"
    raise ValueError(message)


def model_version_for(category: Category) -> str:
    """Return the ``model_version`` label written to the predictions table."""
    return MODEL_VERSION_BY_CATEGORY[category]


def architecture_for(category: Category) -> Architecture:
    """Return the booster architecture used to score the category."""
    return ARCHITECTURE_BY_CATEGORY[category]


def feature_count_for(category: Category) -> int:
    """Return the expected feature-vector width (asserted before scoring)."""
    return FEATURE_COUNT_BY_CATEGORY[category]


def build_r2_object_key(category: Category, file_name: str) -> str:
    """Build the R2 object key ``finish-position/{category}/{modelVersion}/{file}``."""
    return f"{R2_KEY_PREFIX}/{category}/{model_version_for(category)}/{file_name}"
