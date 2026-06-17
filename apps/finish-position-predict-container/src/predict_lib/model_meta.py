"""Container-baked model-version / architecture / R2-key mapping.

Single source of truth for the model the daily-prediction container LOADS and
SCORES TODAY's upcoming races with. As of 2026-06-18 the v8 production deploy is
JRA=iter20-jra-cb-2013-v8 (244 features, base-only; train start 20130101),
NAR=iter12-nar-xgb-hpo-v8.
The historical PG predictions table + ``finish_position_active_models``
were flipped, the iter12/iter20 BOOSTERS are baked under
``models/finish-position/{nar/jra}/<iter-version>/``, and the v8 feature
layers (pacestyle + course) are wired into ``pipeline_args.LAYER_CHAIN``.

Ban-ei is unchanged from v7-lineage — v8 only retrained the JRA + NAR boosters.

E-top2 (iter22-jra-etop2, STAGED 2026-06-18):
  Applies a place-preserving XGBoost override on top of CB iter20 for JRA.
  XGB model: ``xgb-jra-2013-v8`` (rank:ndcg, 244 features, train 2013-2022).
  Override fires when XGB#1 == CB#2 and race class != 701. Blind 2025 gate:
  top1 LB95 +0.58pp, place2 LB95 +0.06pp, place3 +0.00pp — ADOPT.
  Flip is gated on orchestrator verification of place2 + active_models UPDATE.
  Config flag: JRA_ETOP2_ENABLED = True activates dual-model load at startup.
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

# ---------------------------------------------------------------------------
# E-top2 place-preserving override (iter22-jra-etop2, STAGED 2026-06-18)
# ---------------------------------------------------------------------------
# Set to True to activate dual-model loading at container startup: the predict
# loop loads both CB iter20 AND XGB xgb-jra-2013-v8 for JRA, applies the
# E-top2 score override per race (see predict_lib.etop2_override), and writes
# predictions under ETOP2_JRA_MODEL_VERSION instead of MODEL_VERSION_BY_CATEGORY
# ["jra"]. Set to False (default) to keep the production CB-only path.
# Flip to True by orchestrator after smoke pass + active_models UPDATE.
JRA_ETOP2_ENABLED: Final[bool] = False

# The XGB model version baked at models/finish-position/jra/xgb-jra-2013-v8/.
# Used to build the R2 object key for the XGB model file at startup when
# JRA_ETOP2_ENABLED is True.
JRA_ETOP2_XGB_MODEL_VERSION: Final[str] = "xgb-jra-2013-v8"

# The model_version label written to the predictions table when E-top2 is active.
# Distinct from MODEL_VERSION_BY_CATEGORY["jra"] so E-top2 rows are queryable
# separately in race_finish_position_model_predictions.
JRA_ETOP2_MODEL_VERSION: Final[str] = "iter22-jra-etop2"
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


def build_r2_xgb_etop2_key(file_name: str) -> str:
    """Build the R2 object key for the E-top2 JRA XGBoost artifact.

    Constructs ``finish-position/jra/{JRA_ETOP2_XGB_MODEL_VERSION}/{file}``.
    Separate from :func:`build_r2_object_key` because the XGB model is a
    companion artifact to the CB iter20 primary model — not a standalone
    replacement — so it lives under its own ``model_version`` directory and
    must not be confused with the category-global CB model path.
    """
    return f"{R2_KEY_PREFIX}/jra/{JRA_ETOP2_XGB_MODEL_VERSION}/{file_name}"
