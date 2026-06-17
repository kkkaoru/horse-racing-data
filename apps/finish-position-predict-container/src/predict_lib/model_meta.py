"""Container-baked model-version / architecture / R2-key mapping.

Single source of truth for the model the daily-prediction container LOADS and
SCORES TODAY's upcoming races with. As of 2026-06-17 the v8 production deploy is
JRA=iter20-jra-cb-2013-v8 (244 features, base-only; train start 20130101),
NAR=iter12-nar-xgb-hpo-v8.
The historical PG predictions table + ``finish_position_active_models``
were flipped, the iter12/iter20 BOOSTERS are baked under
``models/finish-position/{nar/jra}/<iter-version>/``, and the v8 feature
layers (pacestyle + course) are wired into ``pipeline_args.LAYER_CHAIN``.

Ban-ei is unchanged from v7-lineage — v8 only retrained the JRA + NAR boosters.
"""

from __future__ import annotations

from typing import Final, Literal, get_args

Category = Literal["jra", "nar", "ban-ei"]
Architecture = Literal["catboost", "xgboost", "lightgbm"]

CATEGORIES: Final[tuple[Category, ...]] = get_args(Category)

# Per-class JRA model registry — see predict_lib/per_class.py for routing logic.
# MODEL_VERSION_BY_CATEGORY['jra'] is the fallback model when no class-specific
# model is registered.
MODEL_VERSION_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "iter20-jra-cb-2013-v8",
    "nar": "iter12-nar-xgb-hpo-v8",
    "ban-ei": "banei-cb-v7-lineage-wf-21y",
}

ARCHITECTURE_BY_CATEGORY: Final[dict[Category, Architecture]] = {
    "jra": "catboost",
    "nar": "xgboost",
    "ban-ei": "catboost",
}

FEATURE_COUNT_BY_CATEGORY: Final[dict[Category, int]] = {
    "jra": 244,
    "nar": 192,
    "ban-ei": 111,
}

R2_KEY_PREFIX: Final[str] = "finish-position"
MODEL_FILE_NAME: Final[str] = "model.json"
# LightGBM boosters are serialised with the native text dump
# (``Booster.save_model`` -> ``model.txt``) rather than the CatBoost / XGBoost
# JSON format, so a per-class LightGBM member's artifact file is named
# ``model.txt`` and discovered through :func:`member_model_file_name`.
LGB_MODEL_FILE_NAME: Final[str] = "model.txt"
METADATA_FILE_NAME: Final[str] = "metadata.json"

# Substrings that mark a per-class ensemble MEMBER ``model_version`` as a
# LightGBM booster (e.g. ``iter36-nar-lgb-lambdarank-residual-C-v8``). Detection
# is by substring so a member trained under either the ``-lgb-`` arch token or
# the ``-lambdarank-`` objective token resolves to the LightGBM architecture +
# the ``model.txt`` artifact file. The ENSEMBLE label
# (``iter36-nar-lgb-ensemble-C-v8``) never flows through architecture / file
# resolution — it is the registry value parsed by ``per_class``, not a member —
# so its embedded ``-lgb-`` token is never mis-read as a member arch.
LGB_MODEL_VERSION_TOKENS: Final[tuple[str, ...]] = ("-lgb-", "-lambdarank-")


def is_lightgbm_model_version(model_version: str) -> bool:
    """Return True when a member ``model_version`` names a LightGBM booster.

    Matches on the ``-lgb-`` / ``-lambdarank-`` tokens (see
    ``LGB_MODEL_VERSION_TOKENS``). Used by both the architecture dispatcher and
    the on-disk artifact-file resolver so the two never disagree on whether a
    member is LightGBM.
    """
    return any(token in model_version for token in LGB_MODEL_VERSION_TOKENS)


def member_model_file_name(model_version: str) -> str:
    """Return the on-disk model artifact file name for a per-class member.

    LightGBM members serialise to ``model.txt`` (native text dump); CatBoost /
    XGBoost members serialise to ``model.json``. Pure string dispatch on the
    member ``model_version`` token so the discoverer can resolve the right
    sibling file before the booster is loaded.
    """
    if is_lightgbm_model_version(model_version):
        return LGB_MODEL_FILE_NAME
    return MODEL_FILE_NAME


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
