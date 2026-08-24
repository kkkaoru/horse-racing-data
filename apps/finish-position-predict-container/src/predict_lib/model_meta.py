"""Container-baked model-version / architecture / R2-key mapping.

Single source of truth for the model the daily-prediction container LOADS and
SCORES TODAY's upcoming races with. As of 2026-06-18 the v8 production deploy is
JRA=jra-cb-v9-sim-2013 (263 features, base-only; train start 20130101),
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

JRA leak-free clean retrain (jra-cb-v9-sim-2013-clean, 2026-07-04):
  jra-cb-v9-sim-2013 was found to carry 4 WITHIN-RACE LEAK columns
  (target_corner_1_norm, target_corner_3_norm, target_corner_4_norm,
  target_running_style_class — the horse's OWN in-race outcome for the race
  being predicted, NULL at genuine pre-race serve). The label-column fix lives
  in finish_position_catboost.py / xgboost.py (LABEL_COLUMNS). This clean
  retrain (250 features, same 2013-2025 full-train window, 0 leak cols) is
  now the production JRA model. Walk-forward pooled: top1 +0.724
  [LB95 +0.164, all 3 folds positive], place2 +0.135, top3_box +0.309 -> ADOPT.
  Rollback: flip model_versions.jra back to "jra-cb-v9-sim-2013" (263) and
  feature_counts.jra back to 263; re-point finish_position_active_models.
  Current fail-closed serving also rejects target_corner_2_norm when present:
  it is the same current-race label family and may only be used through
  prior-race history features such as past_corner_2_norm_avg_5.
  The JRA cell variant jra-cb-v10-prior-corner274-2013 adds 24 prior-only
  corner2/3/4 history features and is allowed only through cell_routing.json
  for the accepted dirt small-field 005 cell.

NAR leak-free clean retrain (iter12-nar-xgb-hpo-v8-clean188, 2026-07-04):
  iter12-nar-xgb-hpo-v8 (192 features) carried the SAME 4 within-race leak
  columns as the JRA model above (target_corner_1_norm, target_corner_3_norm,
  target_corner_4_norm, target_running_style_class). This clean retrain (188
  features, same full 2006-2025 training window, 0 leak cols, strict subset of
  the deployed 192) is now the production NAR base model. Deploy-grade
  walk-forward (3 seeds x 3 folds, serve-exact 192-name matrix for the "A"
  arm): top1 +5.496pp [LB95 +5.102], place2 +2.360pp [LB95 +1.923], place3
  +1.358pp [LB95 +0.966], place4/5/6 and top3_box all positive with LB95>0
  too -> ADOPT (strict gate). The iter40 transformer companion artifact is
  clean at serve: it was retrained on the same all-history NAR window after
  removing the four leaked inputs from its feature_order. Rollback: set
  NAR_TRANSFORMER_BLEND_ENABLED=0 for pure clean188 base serving; do not re-point
  NAR to historical iter12-nar-xgb-hpo-v8 in production.
  As above, target_corner_2_norm is denied by current artifact guards if a new
  feature store exposes it.

JRA Stage-1 market-free gated fallback (jra-cb-stage1-marketfree235-2013, 2026-07-22):
  Not a category-default swap -- a FALLBACK artifact routed by
  ``predict_lib.stage1_routing`` only when the freshness gate or stddev safety
  net trips (odds-serving incident: the champion's 15 market/odds features
  fall back to a training median and top1 collapses ~33.6%->~9.4%). Same
  training recipe as jra-cb-v9-sim-2013-clean (CatBoost YetiRank, iterations
  300, lr 0.05, depth 8, l2 3.0, relevance 3/2/1, no_cat_features, seed
  20260519, full 2013-01-01..2025-12-31 single fit, 626,798 rows / 44,907
  races -- identical population to the champion) with the 15 market features
  removed (235 feat). Blind WF (triple-anchored: harness + independent
  retest_wf.py reproduction + advisor review): 28.89% top1 in the collapsed
  regime (+19.45pp [LB95 +18.44] recovery over the champion's ~9.44%), -4.75pp
  vs the champion in the healthy regime -- see
  docs/finish-position-accuracy/history/jra-stage1-market-free-fallback-probe-2026-07-22.md.
  Steady-state (odds-healthy) accuracy is unaffected: this ADDS an incident
  floor, it does not replace the primary model.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Final, Literal, get_args

Category = Literal["jra", "nar", "ban-ei"]
Architecture = Literal["catboost", "xgboost", "lightgbm"]

CATEGORIES: Final[tuple[Category, ...]] = get_args(Category)

MODEL_META_JSON_PATH: Final[Path] = Path(__file__).parent / "model_meta.json"

WITHIN_RACE_LEAK_COLUMNS: Final[frozenset[str]] = frozenset({
    "target_corner_1_norm",
    "target_corner_2_norm",
    "target_corner_3_norm",
    "target_corner_4_norm",
    "target_running_style_class",
})

PRODUCTION_MODEL_VERSION_ALLOWLIST: Final[frozenset[str]] = frozenset({
    "jra-cb-v9-sim-2013-clean",
    "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
    "jra-cb-v10-prior-corner274-2013",
    "jra-cb-stage1-marketfree235-2013",
    "jra-cb-stage1-marketfree235-iter500-top1swap-2013",
    "iter12-nar-xgb-hpo-v8-clean188",
    "iter40-nar-settransformer-blend-v1",
    "iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
    "banei-cb-v9-sim-2011",
    "banei-cb-v8-window2011-wf-15y",
})


def leak_columns_in(feature_names: Iterable[str]) -> frozenset[str]:
    """Return in-race outcome columns that must never reach pre-race serving."""
    return WITHIN_RACE_LEAK_COLUMNS.intersection(feature_names)


def assert_no_within_race_leak_columns(
    feature_names: Iterable[str], *, context: str
) -> None:
    """Fail closed when a model artifact still carries within-race leak columns."""
    leaks = leak_columns_in(feature_names)
    if leaks:
        joined = ", ".join(sorted(leaks))
        raise ValueError(f"{context} contains within-race leak columns: {joined}")


def is_production_model_version_allowed(model_version: str) -> bool:
    """Return True when ``model_version`` is approved for production serving."""
    return model_version in PRODUCTION_MODEL_VERSION_ALLOWLIST


def assert_production_model_version_allowed(model_version: str, *, context: str) -> None:
    """Fail closed when a historical/leaky artifact is selected for production."""
    if not is_production_model_version_allowed(model_version):
        raise ValueError(
            f"{context} model_version is not allowed for production serving: {model_version}"
        )


def load_model_meta(
    path: Path = MODEL_META_JSON_PATH,
) -> tuple[dict[Category, str], dict[Category, int]]:
    """Load model_meta.json; raises FileNotFoundError if missing, ValueError if malformed."""
    if not path.exists():
        raise FileNotFoundError(f"model_meta.json not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"model_meta.json must be a JSON object: {path}")
    mv = payload.get("model_versions")
    if not isinstance(mv, dict):
        raise ValueError(f"model_meta.json missing 'model_versions' dict: {path}")
    fc = payload.get("feature_counts")
    if not isinstance(fc, dict):
        raise ValueError(f"model_meta.json missing 'feature_counts' dict: {path}")
    versions: dict[Category, str] = {}
    counts: dict[Category, int] = {}
    for cat in get_args(Category):
        val = mv.get(cat)
        if not isinstance(val, str) or not val.strip():
            raise ValueError(f"model_meta.json missing or empty model_version for '{cat}': {path}")
        versions[cat] = val
        cnt = fc.get(cat)
        if not isinstance(cnt, int) or isinstance(cnt, bool) or cnt <= 0:
            raise ValueError(
                f"model_meta.json missing or invalid feature_count for '{cat}': {path}"
            )
        counts[cat] = cnt
    for cat, version in versions.items():
        assert_production_model_version_allowed(
            version,
            context=f"model_meta.json category={cat}",
        )
    return versions, counts


_resolved_versions, _resolved_counts = load_model_meta()

# Category-default model registry. Per-class routing is historical only; current
# production routing is cell-first and then category-level fallback.
MODEL_VERSION_BY_CATEGORY: Final[dict[Category, str]] = _resolved_versions

ARCHITECTURE_BY_CATEGORY: Final[dict[Category, Architecture]] = {
    "jra": "catboost",
    "nar": "xgboost",
    "ban-ei": "catboost",
}

FEATURE_COUNT_BY_CATEGORY: Final[dict[Category, int]] = _resolved_counts

R2_KEY_PREFIX: Final[str] = "finish-position"
MODEL_FILE_NAME: Final[str] = "model.json"

_ENV_TRUE_TOKENS: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})
_ENV_FALSE_TOKENS: Final[frozenset[str]] = frozenset({"0", "false", "no", "off"})


def _env_flag(name: str, *, default: bool) -> bool:
    """Read a boolean feature flag from the environment (instant-rollback seam).

    Returns ``default`` when the variable is unset or holds an unrecognised
    token; otherwise ``1/true/yes/on`` -> True and ``0/false/no/off`` -> False
    (case-insensitive, whitespace-trimmed). Lets an operator flip a deployed
    flag through the container / Worker env without a redeploy."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    token = raw.strip().lower()
    if token in _ENV_TRUE_TOKENS:
        return True
    if token in _ENV_FALSE_TOKENS:
        return False
    return default


# ---------------------------------------------------------------------------
# E-top2 place-preserving override (iter22-jra-etop2, STAGED 2026-06-18)
# ---------------------------------------------------------------------------
# False since v9-sim deploy: sim_* CB model has 263 features but the XGB
# override model (xgb-jra-2013-v8) expects 244. sim_* CB alone gives +1.34pp
# top1 (comparable to E-top2's +1.36pp). Re-enable after retraining XGB with
# 263 features or adding per-model feature_names support.
JRA_ETOP2_ENABLED: Final[bool] = False

# The XGB model version baked at models/finish-position/jra/xgb-jra-2013-v8/.
# Used to build the R2 object key for the XGB model file at startup when
# JRA_ETOP2_ENABLED is True.
JRA_ETOP2_XGB_MODEL_VERSION: Final[str] = "xgb-jra-2013-v8"

# The model_version label written to the predictions table when E-top2 is active.
# Distinct from MODEL_VERSION_BY_CATEGORY["jra"] so E-top2 rows are queryable
# separately in race_finish_position_model_predictions.
JRA_ETOP2_MODEL_VERSION: Final[str] = "iter22-jra-etop2"

# ---------------------------------------------------------------------------
# NAR E-top2 per-class override (iter23-nar-etop2, 2026-06-19)
# ---------------------------------------------------------------------------
# Mirror image of the JRA override: NAR production scores with XGBoost
# (iter12-nar-xgb-hpo-v8) as the BASE, and the CatBoost CB-2013 model below
# supplies the override signal. The override fires per race when CB#1 == XGB#2
# (CB#3 stays at rank-3, preserving place3), but ONLY for the ADOPT classes.
# See predict_lib.nar_etop2_override for the swap logic.
#
# True = dual-model loading is ACTIVE at container startup for NAR: the predict
# loop loads both XGB iter12 AND CB cb-nar-2013-v8, applies the per-class
# override, and writes predictions under NAR_ETOP2_MODEL_VERSION for ADOPT-class
# races. Held at False until the CB-2013 model is trained + smoke2 PASS.
NAR_ETOP2_ENABLED: Final[bool] = False

# The CatBoost CB-2013 model version baked at
# models/finish-position/nar/cb-nar-2013-v8/. Used to build the R2 object key
# for the CB override model file at startup when NAR_ETOP2_ENABLED is True.
NAR_ETOP2_CB_MODEL_VERSION: Final[str] = "cb-nar-2013-v8"

# The model_version label written to the predictions table for ADOPT-class NAR
# rows when the override is active. Distinct from MODEL_VERSION_BY_CATEGORY
# ["nar"] so iter23-nar-etop2 rows are queryable separately.
NAR_ETOP2_MODEL_VERSION: Final[str] = "iter23-nar-etop2"

# NAR E-top2 is closed. Keep the helper importable for historical tests and
# offline comparison, but no NAR subclass may activate it in production.
NAR_ETOP2_ADOPT_CLASSES: Final[frozenset[str]] = frozenset()
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


# ---------------------------------------------------------------------------
# Category training start year configuration
# ---------------------------------------------------------------------------
# Current production and local routing are per-cell, not per-class. Keep only
# category-wide defaults here so a class/subclass label cannot change the
# training window.
_DEFAULT_TRAIN_START_YEAR: Final[int] = 2006

_CATEGORY_DEFAULT_TRAIN_START_YEAR: Final[dict[str, int]] = {
    "jra": 2013,
    "nar": 2006,
    "ban-ei": 2006,
}

PER_CLASS_TRAIN_START_YEAR: Final[Mapping[tuple[str, str], int]] = {}


def get_train_start_year(category: str, class_code: str) -> int:
    """Return the category-wide training start year.

    ``class_code`` is accepted for backward-compatible callers but ignored:
    per-class training-window overrides are not part of the current production
    or local model-selection authority.
    """
    _ = class_code
    return _CATEGORY_DEFAULT_TRAIN_START_YEAR.get(category, _DEFAULT_TRAIN_START_YEAR)


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


def build_r2_nar_etop2_key(file_name: str) -> str:
    """Build the R2 object key for the E-top2 NAR CatBoost override artifact.

    Constructs ``finish-position/nar/{NAR_ETOP2_CB_MODEL_VERSION}/{file}``.
    The mirror of :func:`build_r2_xgb_etop2_key` for NAR: the CB-2013 model is a
    companion override artifact to the XGB iter12 primary NAR model — not a
    standalone replacement — so it lives under its own ``model_version``
    directory and must not be confused with the category-global XGB model path.
    """
    return f"{R2_KEY_PREFIX}/nar/{NAR_ETOP2_CB_MODEL_VERSION}/{file_name}"


# ---------------------------------------------------------------------------
# NAR Set-Transformer x ensemble rank-fusion blend (iter40, 2026-07-03)
# ---------------------------------------------------------------------------
# A clean 113-feature listwise Set Transformer (3 seeds, all-history 2006-2025)
# whose within-race seed score is fused 0.5/0.5 with the clean NAR production
# base (iter12-nar-xgb-hpo-v8-clean188) score. This is a retrained iter40
# artifact with the four within-race leak columns removed from the training and
# serving feature contract, so the transformer path remains active without
# carrying post-race signals.
#
# ENABLED reads the environment at import. Default True keeps NAR transformer
# serving active with the cleaned artifact; set NAR_TRANSFORMER_BLEND_ENABLED=0
# in the container / Worker env for an immediate rollback to pure clean188 base.
NAR_TRANSFORMER_BLEND_ENABLED: Final[bool] = _env_flag(
    "NAR_TRANSFORMER_BLEND_ENABLED", default=True
)

# The blend artifact version baked at
# models/finish-position/nar/iter40-nar-settransformer-blend-v1/ (norm.json +
# weights_s{1,2,3}.npz). Written as the predictions ``model_version`` for blended
# NAR rows; fallback rows keep MODEL_VERSION_BY_CATEGORY["nar"].
NAR_TRANSFORMER_MODEL_VERSION: Final[str] = "iter40-nar-settransformer-blend-v1"

# Fusion weight on the transformer seed-rank-mean (1 - w on the base rank).
NAR_TRANSFORMER_BLEND_WEIGHT: Final[float] = 0.5


def build_r2_nar_transformer_key(file_name: str) -> str:
    """Build the R2 object key for a NAR transformer-blend artifact file.

    Constructs ``finish-position/nar/{NAR_TRANSFORMER_MODEL_VERSION}/{file}``.
    The artifact (norm.json + weights_s{1,2,3}.npz) lives under its own
    ``model_version`` directory, mirroring the E-top2 companion-artifact key
    builders, and is loaded from the baked ``MODELS_DIR`` at container startup."""
    return f"{R2_KEY_PREFIX}/nar/{NAR_TRANSFORMER_MODEL_VERSION}/{file_name}"
