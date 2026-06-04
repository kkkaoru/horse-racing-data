"""Per-class JRA model routing (Phase B of the per-class architecture pivot).

The v8 production deploy (JRA=iter14-jra-cb-pacestyle-course-v8, NAR=iter12-nar-
xgb-hpo-v8) is a single global model per category. The per-class architecture
adds an optional second axis — ``kyoso_joken_code`` (race class) — so that future
per-class winners can be activated piecemeal without disturbing classes that
have no per-class winner yet.

Routing rules:

* ``PER_CLASS_MODEL_VERSIONS`` maps ``(category, kyoso_joken_code)`` to a
  registered per-class ``model_version`` string. An entry is added ONLY when a
  per-class model has beaten the category-global fallback (iter 14 for JRA) on
  its own subset; an unmapped class falls back to the category-global model.
* ``PER_CLASS_ENABLED_CATEGORIES`` lists the categories that participate in the
  per-class architecture. NAR / Ban-ei are intentionally excluded — neither has
  an actionable per-class plan yet — so they always return the category-global
  model regardless of ``kyoso_joken_code``.

Phase B-2A (2026-06-05) adds an ENSEMBLE routing layer on top of the registered
single-model string. When ``PER_CLASS_MODEL_VERSIONS`` resolves to an ensemble
model_version (e.g. ``iter23-jra-cb-ensemble-703-v8``), the container can read a
sidecar ``manifest.json`` that lists weighted member booster versions and a
blend strategy. ``load_ensemble_manifest`` returns the parsed
``PerClassEnsemble`` dataclass, or ``None`` when no manifest exists (the caller
then falls back to the single-model path). ``resolve_per_class_resolution`` is
the unified entry point that returns either a ``PerClassEnsemble`` (multi-model)
or a single ``model_version`` string.

iter 23 ensemble optimisation (2026-06-05) produced 4 ACCEPT manifests for JRA
classes 005 / 701 / 703 / other. Only 703 beats iter 14 globally with
delta_pp=+0.142pp top1; the other three tie at +0.000pp. Phase B-2A registers
ONLY 703 in ``PER_CLASS_MODEL_VERSIONS`` to keep the booster pool footprint
small. The unregistered tied classes (005 / 701 / other) keep falling back to
iter 14 — see ``docs/finish-position-accuracy/runbook/PER_CLASS_ROUTING.md``.
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

    Members come from the iter 23 ensemble search artefacts. ``is_baseline``
    flags the iter 14 fallback booster (carried at a small weight as a safety
    net); all other members are per-class iterations whose blend weight was
    optimised on the validation window.
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


# Phase B-2A registry. Only 703 is registered — the other 3 ACCEPT manifests
# from iter 23 (005 / 701 / other) tied at +0.000pp and would only inflate the
# booster pool without measurable accuracy gain. 703 ACCEPTED at +0.142pp top1.
PER_CLASS_MODEL_VERSIONS: Final[dict[tuple[Category, str], str]] = {
    ("jra", "703"): "iter23-jra-cb-ensemble-703-v8",
}

# Categories that participate in per-class routing. NAR / Ban-ei are excluded
# so their ``resolve_per_class_model_version`` always returns the category-global
# model — adding them here would silently change routing behaviour, so the
# allowlist is the single switch.
PER_CLASS_ENABLED_CATEGORIES: Final[frozenset[Category]] = frozenset({"jra"})

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


def resolve_per_class_model_version(
    category: Category,
    kyoso_joken_code: str | None,
) -> str:
    """Return per-class model_version if registered, else category fallback.

    Falls back to ``model_version_for(category)`` when:

    * the category is not per-class enabled (NAR / Ban-ei),
    * the race has no ``kyoso_joken_code`` (e.g. the column was NULL in PG and
      the feature build emitted ``None``), or
    * the class code has no registered per-class winner yet.

    All three branches map to the SAME global model_version so the caller can
    treat the return value as an opaque label and is never accidentally routed
    to a non-existent per-class booster.
    """
    if not is_per_class_enabled_for(category):
        return model_version_for(category)
    if kyoso_joken_code is None:
        return model_version_for(category)
    return PER_CLASS_MODEL_VERSIONS.get(
        (category, kyoso_joken_code),
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
    kyoso_joken_code: str | None,
) -> PerClassEnsemble | str:
    """Return either a ``PerClassEnsemble`` (multi-model) or single model_version.

    Routing precedence:

    1. If the category is per-class enabled AND ``kyoso_joken_code`` is provided
       AND an ensemble manifest exists on disk for the registered model_version,
       return the parsed ``PerClassEnsemble``.
    2. Otherwise return the string from ``resolve_per_class_model_version``
       (registered single model or category-global fallback).

    The return type union (``PerClassEnsemble | str``) lets the caller pattern-
    match on ``isinstance(..., PerClassEnsemble)`` to pick the rank-blend path
    vs the single-booster path.
    """
    if is_per_class_enabled_for(category) and kyoso_joken_code is not None:
        ensemble = load_ensemble_manifest(models_dir, category, kyoso_joken_code)
        if ensemble is not None:
            return ensemble
    return resolve_per_class_model_version(category, kyoso_joken_code)
