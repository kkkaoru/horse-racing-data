"""Per-class JRA / NAR model routing (Phase B + Phase F of the per-class architecture pivot).

The v8 production deploy (JRA=iter14-jra-cb-pacestyle-course-v8, NAR=iter12-nar-
xgb-hpo-v8) is a single global model per category. The per-class architecture
adds an optional second axis — a category-specific class code — so per-class
winners can be activated piecemeal without disturbing classes that have no
per-class winner yet. The class-code domain depends on the category:

* JRA — ``kyoso_joken_code`` (race-class code: ``005`` / ``010`` / ``016`` /
  ``701`` / ``703`` / ``other``).
* NAR — ``nar_subclass`` (derived from ``kyoso_joken_meisho`` by the feature
  build: ``NEW`` / ``MUKATSU`` / ``C`` / ``B`` / ``A`` / ``OP`` / ``other``).
* Ban-ei — no per-class registry today.

Routing rules:

* ``PER_CLASS_MODEL_VERSIONS`` maps ``(category, class_code)`` to a registered
  per-class ``model_version`` string. An entry is added ONLY when a per-class
  model has beaten the category-global fallback on its own subset; an
  unmapped class falls back to the category-global model.
* ``PER_CLASS_ENABLED_CATEGORIES`` lists the categories that participate in the
  per-class architecture. Ban-ei is intentionally excluded — it has no
  actionable per-class plan yet — so it always returns the category-global
  model regardless of class code.

Phase B-2A (2026-06-05) adds an ENSEMBLE routing layer on top of the registered
single-model string. When ``PER_CLASS_MODEL_VERSIONS`` resolves to an ensemble
model_version (e.g. ``iter26-jra-cb-ensemble-703-v8`` or
``iter30-nar-cb-ensemble-NEW-v8``), the container can read a sidecar
``manifest.json`` that lists weighted member booster versions and a blend
strategy. ``load_ensemble_manifest`` returns the parsed ``PerClassEnsemble``
dataclass, or ``None`` when no manifest exists (the caller then falls back to
the single-model path). ``resolve_per_class_resolution`` is the unified entry
point that returns either a ``PerClassEnsemble`` (multi-model) or a single
``model_version`` string.

iter 26 v4 (JRA, 2026-06-05): 005 / 016 / 703 / 010 / other ensembles activated.
iter 30 (NAR, 2026-06-05): NEW / MUKATSU / C / A / OP / other ensembles
activated — six NAR sub-classes routed off ``nar_subclass``. ``B`` stays on the
iter12 baseline (no ensemble registered).

The ``other`` entry is a virtual code: real races never carry the literal
``"other"`` as their class code. :func:`normalize_class_code` maps unregistered
codes (and NULL) to ``"other"`` before the registry / manifest lookup so the
offline ``class_filter_mask`` semantics carry over to inference unchanged.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

from .model_meta import R2_KEY_PREFIX, Category, model_version_for


@dataclass(frozen=True)
class EnsembleMember:
    """One weighted member booster inside a per-class ensemble manifest.

    Members come from the iter 23+ ensemble search artefacts. ``is_baseline``
    flags the category-global fallback booster (carried at a small weight as a
    safety net); all other members are per-class iterations whose blend weight
    was optimised on the validation window.
    """

    model_version: str
    weight: float
    is_baseline: bool


@dataclass(frozen=True)
class PerClassEnsemble:
    """A per-class ensemble routing decision parsed from a manifest.json file.

    ``model_version`` is the ensemble label written to the predictions table and
    activated in ``finish_position_active_models``. ``members`` is the immutable
    tuple of weighted booster versions; the rank-blend score function lives in
    ``predict_lib.ensemble_scorer`` (Phase B-2B).
    """

    model_version: str
    category: Category
    kyoso_joken_code: str
    ensemble_type: str
    members: tuple[EnsembleMember, ...]


# Phase B-2A registry. 005 / 016 / 703 ACTIVATED on 2026-06-05 with the iter 26
# v4 ensemble (iter 26 relationship features: 馬体重 x 斤量 x 馬齢 x 距離 x
# タイム interaction columns, 12 cols). iter 26 v4 re-optimised the ensemble
# pool with the new relationship booster added and shipped major per-class
# gains.
#
# Phase F (2026-06-05) adds NAR per-class routing on top: six NAR sub-classes
# (NEW / MUKATSU / C / A / OP / other) activated with iter 30 ensembles on
# iter 12 NAR XGBoost baseline + iter 30 CatBoost residuals. ``B`` stays on
# the iter 12 fallback (no ensemble registered).
#
# iter 36 (NAR class C, 2026-06-10) flips C from the iter 30 CatBoost ensemble
# to ``iter36-nar-lgb-ensemble-C-v8`` — a blend that adds a LightGBM LambdaRank
# residual member (``iter36-nar-lgb-lambdarank-residual-C-v8``) alongside the
# iter 12 XGBoost baseline. The new member's ``model.txt`` text dump is loaded
# through ``lightgbm_adapter`` and scored positionally on its own metadata
# feature order. Other NAR classes are unchanged.
PER_CLASS_MODEL_VERSIONS: Final[dict[tuple[Category, str], str]] = {
    ("jra", "005"): "iter26-jra-cb-ensemble-005-v8",
    ("jra", "010"): "iter25-jra-cb-ensemble-010-v8",
    ("jra", "016"): "iter26-jra-cb-ensemble-016-v8",
    ("jra", "703"): "iter26-jra-cb-ensemble-703-v8",
    ("jra", "other"): "iter25-jra-cb-ensemble-other-v8",
    ("nar", "NEW"): "iter30-nar-cb-ensemble-NEW-v8",
    ("nar", "MUKATSU"): "iter30-nar-cb-ensemble-MUKATSU-v8",
    ("nar", "C"): "iter36-nar-lgb-ensemble-C-v8",
    ("nar", "A"): "iter30-nar-cb-ensemble-A-v8",
    ("nar", "OP"): "iter30-nar-cb-ensemble-OP-v8",
    ("nar", "other"): "iter30-nar-cb-ensemble-other-v8",
}

# Real class-code values that are routed by their literal code rather than the
# catch-all ``"other"`` bucket — split per category because JRA uses numeric
# ``kyoso_joken_code`` while NAR uses the derived ``nar_subclass`` string. The
# JRA set mirrors the offline ``per_class_ensemble_lib.class_filter_mask``
# carve-outs (005 / 010 / 016 / 701 / 703); the NAR set mirrors the offline
# ``nar_subclass`` regex partition (NEW / MUKATSU / C / B / A / OP). Codes
# outside the per-category set (and NULL) collapse to ``"other"`` via
# :func:`normalize_class_code` before the registry lookup. Ban-ei has no
# per-class plan, so its set is empty.
NAMED_PER_CLASS_CODES_BY_CATEGORY: Final[dict[Category, frozenset[str]]] = {
    "jra": frozenset({"005", "010", "016", "701", "703"}),
    "nar": frozenset({"NEW", "MUKATSU", "C", "B", "A", "OP"}),
    "ban-ei": frozenset(),
}
# Back-compat alias for callers / tests that already imported the JRA-only
# constant before Phase F. Equivalent to ``NAMED_PER_CLASS_CODES_BY_CATEGORY
# ["jra"]`` — preserved verbatim so the JRA registry checks stay stable.
NAMED_PER_CLASS_CODES: Final[frozenset[str]] = NAMED_PER_CLASS_CODES_BY_CATEGORY["jra"]
OTHER_CLASS_CODE: Final[str] = "other"

# Categories that participate in per-class routing. Ban-ei is excluded so its
# ``resolve_per_class_model_version`` always returns the category-global
# model — adding it here would silently change routing behaviour, so the
# allowlist is the single switch. Phase F (2026-06-05) adds ``nar`` alongside
# ``jra`` for the iter 30 NAR ensemble rollout.
PER_CLASS_ENABLED_CATEGORIES: Final[frozenset[Category]] = frozenset({"jra", "nar"})

# Sub-directory under ``<models_dir>/{R2_KEY_PREFIX}/{category}/`` where
# per-class artefacts (booster.json + ensemble manifest.json) live. Mirrors the
# host path ``apps/finish-position-predict-container/models/finish-position/
# {category}/per-class/{code}/{model_version}/`` baked into the image at
# ``/models/finish-position/{category}/per-class/...`` by the Dockerfile.
PER_CLASS_SUBDIR: Final[str] = "per-class"
ENSEMBLE_MANIFEST_FILE_NAME: Final[str] = "manifest.json"


def is_per_class_enabled_for(category: Category) -> bool:
    """Return True when ``category`` participates in per-class routing."""
    return category in PER_CLASS_ENABLED_CATEGORIES


def normalize_class_code(category: Category, code: str | None) -> str:
    """Collapse unregistered codes (and ``None``) to the catch-all ``"other"``.

    The named-code allowlist is per-category: JRA uses numeric ``kyoso_joken_
    code`` (``005`` / ``010`` / ``016`` / ``701`` / ``703``), NAR uses the
    derived ``nar_subclass`` string (``NEW`` / ``MUKATSU`` / ``C`` / ``B`` /
    ``A`` / ``OP``), Ban-ei has no named codes. Every code outside the
    per-category set collapses to ``"other"``; NULL also collapses to
    ``"other"``.

    The mapping happens once, immediately before the registry lookup, so
    callers never need to special-case unregistered codes. Returned values are
    guaranteed non-``None`` so downstream code can treat the result as a
    regular string key.
    """
    if code is None:
        return OTHER_CLASS_CODE
    named = NAMED_PER_CLASS_CODES_BY_CATEGORY.get(category, frozenset())
    if code in named:
        return code
    return OTHER_CLASS_CODE


def resolve_per_class_model_version(
    category: Category,
    class_code: str | None,
) -> str:
    """Return per-class model_version if registered, else category fallback.

    Falls back to ``model_version_for(category)`` when:

    * the category is not per-class enabled (Ban-ei), or
    * the normalised class code (see :func:`normalize_class_code`) has no
      registered per-class winner yet — i.e. the named-code-or-``"other"``
      bucket does not appear in ``PER_CLASS_MODEL_VERSIONS``.

    Both branches map to the SAME global model_version so the caller can
    treat the return value as an opaque label and is never accidentally routed
    to a non-existent per-class booster. NULL / empty class-code values from
    the feature parquet are folded into the ``"other"`` bucket by the
    normaliser, so they hit the ``other`` ensemble when it is registered and
    the category fallback otherwise.
    """
    if not is_per_class_enabled_for(category):
        return model_version_for(category)
    normalised = normalize_class_code(category, class_code)
    return PER_CLASS_MODEL_VERSIONS.get(
        (category, normalised),
        model_version_for(category),
    )


def per_class_codes_for(category: Category) -> tuple[str, ...]:
    """Return the registered per-class codes for ``category`` in sorted order.

    Used by callers that need to pre-load per-class boosters at startup. Returns
    an empty tuple for disabled categories AND for enabled categories that have
    no registered per-class winners yet.
    """
    if not is_per_class_enabled_for(category):
        return ()
    codes = {code for cat, code in PER_CLASS_MODEL_VERSIONS if cat == category}
    return tuple(sorted(codes))


def build_per_class_manifest_path(
    models_dir: Path,
    category: Category,
    kyoso_joken_code: str,
    model_version: str,
) -> Path:
    """Return the absolute path to a per-class ensemble manifest.json file.

    The path mirrors the R2 / image layout:
    ``<models_dir>/{R2_KEY_PREFIX}/{category}/per-class/{kyoso_joken_code}/
    {model_version}/manifest.json``. Pure path construction — does not check
    whether the file exists; the caller decides how to handle absent manifests.
    """
    return (
        models_dir
        / R2_KEY_PREFIX
        / category
        / PER_CLASS_SUBDIR
        / kyoso_joken_code
        / model_version
        / ENSEMBLE_MANIFEST_FILE_NAME
    )


def _parse_ensemble_member(raw: object) -> EnsembleMember | None:
    """Parse one ``members`` entry into an ``EnsembleMember`` or ``None``.

    Returns ``None`` when the entry is not a dict, is missing a required key, or
    has a value with the wrong runtime type. The caller treats a single ``None``
    as a corrupt manifest and discards the whole ensemble (falls back to the
    single-model path) so a malformed member never silently disappears.
    """
    if not isinstance(raw, dict):
        return None
    raw_map = cast(dict[str, object], raw)
    model_version = raw_map.get("model_version")
    weight = raw_map.get("weight")
    is_baseline = raw_map.get("is_baseline")
    if not isinstance(model_version, str):
        return None
    if not isinstance(weight, (int, float)) or isinstance(weight, bool):
        return None
    if not isinstance(is_baseline, bool):
        return None
    return EnsembleMember(
        model_version=model_version,
        weight=float(weight),
        is_baseline=is_baseline,
    )


def _parse_ensemble_manifest_payload(
    payload: object,
    category: Category,
    kyoso_joken_code: str,
) -> PerClassEnsemble | None:
    """Validate a parsed manifest JSON payload and build a ``PerClassEnsemble``.

    Returns ``None`` when any required field is missing, has the wrong type, or
    when ``category`` / ``kyoso_joken_code`` recorded in the manifest disagree
    with the caller's expectation (defensive: a misplaced file must not silently
    route to the wrong race-class).
    """
    if not isinstance(payload, dict):
        return None
    payload_map = cast(dict[str, object], payload)
    model_version = payload_map.get("model_version")
    manifest_category = payload_map.get("category")
    manifest_code = payload_map.get("kyoso_joken_code")
    ensemble_type = payload_map.get("ensemble_type")
    raw_members = payload_map.get("members")
    if not isinstance(model_version, str):
        return None
    if manifest_category != category:
        return None
    if manifest_code != kyoso_joken_code:
        return None
    if not isinstance(ensemble_type, str):
        return None
    if not isinstance(raw_members, list):
        return None
    members: list[EnsembleMember] = []
    for raw_member in cast(list[object], raw_members):
        parsed = _parse_ensemble_member(raw_member)
        if parsed is None:
            return None
        members.append(parsed)
    if not members:
        return None
    return PerClassEnsemble(
        model_version=model_version,
        category=category,
        kyoso_joken_code=kyoso_joken_code,
        ensemble_type=ensemble_type,
        members=tuple(members),
    )


def load_ensemble_manifest(
    models_dir: Path,
    category: Category,
    kyoso_joken_code: str,
) -> PerClassEnsemble | None:
    """Load and parse a per-class ensemble manifest, or return ``None``.

    The model_version is derived from ``PER_CLASS_MODEL_VERSIONS`` rather than
    discovered from the filesystem: registry is the single source of truth for
    what runs in production. Returns ``None`` (caller falls back to the
    single-model path) when:

    * the ``(category, kyoso_joken_code)`` pair has no registry entry,
    * the manifest file does not exist on disk,
    * the file is not valid JSON, or
    * the JSON payload fails validation (missing / wrong-typed fields, member
      list empty, category / code mismatch).
    """
    registered = PER_CLASS_MODEL_VERSIONS.get((category, kyoso_joken_code))
    if registered is None:
        return None
    path = build_per_class_manifest_path(
        models_dir, category, kyoso_joken_code, registered
    )
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return _parse_ensemble_manifest_payload(payload, category, kyoso_joken_code)


def resolve_per_class_resolution(
    models_dir: Path,
    category: Category,
    class_code: str | None,
) -> PerClassEnsemble | str:
    """Return either a ``PerClassEnsemble`` (multi-model) or single model_version.

    Routing precedence:

    1. If the category is per-class enabled AND an ensemble manifest exists on
       disk for the normalised class code (named code or ``"other"``), return
       the parsed ``PerClassEnsemble``.
    2. Otherwise return the string from ``resolve_per_class_model_version``
       (registered single model or category-global fallback).

    The raw class code is first normalised through :func:`normalize_class_code`
    so unregistered real codes (e.g. JRA ``"999"`` / NAR ``"X"``) and NULL
    values both route to the ``"other"`` ensemble when it is registered. The
    return type union (``PerClassEnsemble | str``) lets the caller
    pattern-match on ``isinstance(..., PerClassEnsemble)`` to pick the
    rank-blend path vs the single-booster path.
    """
    if is_per_class_enabled_for(category):
        normalised = normalize_class_code(category, class_code)
        ensemble = load_ensemble_manifest(models_dir, category, normalised)
        if ensemble is not None:
            return ensemble
    return resolve_per_class_model_version(category, class_code)
