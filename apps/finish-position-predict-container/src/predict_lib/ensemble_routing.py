"""Per-race ensemble routing for the container's daily predictor (Phase B-2E + Phase F).

Wires together the Wave-1 primitives (per_class manifest loader + booster pool +
ensemble scorer) into two pure helpers that ``predict_upcoming.py`` consumes:

* :func:`init_member_pool` — once per category at startup, walks the per-class
  registry, loads every ensemble manifest's member booster from disk, and
  returns a :class:`predict_lib.booster_pool.BoosterPool` shared across all
  races. Cold-start cost is paid once; per-race scoring is a dict lookup. Phase
  F (2026-06-05) makes the pool architecture-aware so NAR ensembles can carry
  both the iter 12 XGBoost baseline AND iter 30 CatBoost residual members
  without dropping accuracy through a wrong-dtype matrix.
* :func:`score_race_with_resolution` — once per race at scoring time, given a
  :class:`predict_lib.per_class.PerClassEnsemble | str` resolution it either
  (a) scores with the category-global fallback booster (single-model path) or
  (b) scores each ensemble member from the pool, building a feature matrix per
  member architecture, and blends them with the manifest weights. Any failure
  on the ensemble path (missing member booster, scoring exception, shape
  mismatch) falls back to the single-booster path with the category-global
  ``model_version`` label so a per-class corruption never blocks predictions
  for the day.

All side effects (file I/O, native booster loading) are inherited from the
Wave-1 primitives; this module itself is pure — :func:`init_member_pool` is
the only function that touches the filesystem, and it does so through the
existing ``discover_member_models`` + ``build_pool_from_paths`` helpers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .booster_pool import (
    BoosterPool,
    PoolBooster,
    build_pool_from_paths,
    discover_baseline_member_model,
    discover_member_models,
)
from .ensemble_scorer import score_with_ensemble
from .model_meta import (
    R2_KEY_PREFIX,
    Architecture,
    Category,
    architecture_for,
    model_version_for,
)
from .per_class import (
    PER_CLASS_MODEL_VERSIONS,
    EnsembleMember,
    PerClassEnsemble,
    load_ensemble_manifest,
)
from .scorer import BoosterLike, build_feature_matrix, score_matrix
from .upcoming import KETTO_FIELD


@dataclass(frozen=True)
class EnsembleRouteOutcome:
    """Result of scoring one race through the ensemble router.

    ``scores`` is the per-entry score vector consumed by
    :func:`predict_lib.upcoming.rank_race_entries`. ``model_version`` is the
    label written to the predictions UPSERT — either the ensemble's manifest
    label (success) or the category-global fallback label (any failure).
    ``fallback_reason`` is ``None`` on the happy path and a short, human-
    readable string on fallback so the caller can log it once. The reason
    space is small + bounded so logs stay grep-friendly:

    * ``"single-model"`` — resolution was already a string; not a fallback.
    * ``"member-missing:<model_version>"`` — a required member was not in the
      pool (manifest registered but disk artefact missing).
    * ``"score-error:<exception class>"`` — a member booster's ``predict``
      raised, or the blend rejected a shape mismatch.
    """

    scores: list[float]
    model_version: str
    fallback_reason: str | None


def _baseline_member_paths(
    models_dir: Path,
    category: Category,
) -> dict[str, Path]:
    """Resolve the category-global baseline booster path keyed by its model_version.

    The baseline is carried as a member of every per-class ensemble (the
    iter 14 JRA / iter 12 NAR fallback booster acts as a safety net at a small
    weight). Its on-disk location is the category-global single-model path —
    not under ``per-class/`` — so the walker has to look it up directly.
    Returns an empty dict when the baseline file is absent.
    """
    baseline_mv = model_version_for(category)
    baseline_path = discover_baseline_member_model(
        models_dir / R2_KEY_PREFIX, category, baseline_mv
    )
    if baseline_path is None:
        return {}
    return {baseline_mv: baseline_path}


def resolve_member_architecture(
    member_mv: str,
    category: Category,
) -> Architecture:
    """Return the architecture for a per-class ensemble member.

    The category-global baseline (iter 14 JRA = CatBoost, iter 12 NAR =
    XGBoost) is identified by its model_version string; per-class residuals
    follow the category-default arch as encoded in their model_version naming
    convention. NAR ensembles today blend an XGBoost baseline (iter12-nar-xgb-
    hpo-v8) with CatBoost residuals (iter30-nar-cb-*), so the discriminator is
    the ``-xgb-`` / ``-cb-`` substring in the model_version. JRA ensembles are
    homogeneous CatBoost — no per-member dispatch needed there.
    """
    if "-xgb-" in member_mv:
        return "xgboost"
    if "-cb-" in member_mv:
        return "catboost"
    # Fallback: an unrecognised model_version (e.g. legacy banei-cb-v7-lineage-
    # wf-21y) defers to the category default. Today this only hits the JRA /
    # Ban-ei category-global baselines which are both CatBoost — XGBoost is
    # NAR-only and always carries the ``-xgb-`` token.
    return architecture_for(category)


def init_member_pool(models_dir: Path, category: Category) -> BoosterPool:
    """Load every ensemble member booster registered for ``category``.

    Pure-by-design walker: iterates ``PER_CLASS_MODEL_VERSIONS``, asks
    ``load_ensemble_manifest`` for each ``(category, code)`` pair, and for
    every manifest that parses successfully extracts the member
    ``model_version`` tuple and resolves it through
    :func:`predict_lib.booster_pool.discover_member_models` (for per-class
    residual members) plus :func:`_baseline_member_paths` (for the
    category-global baseline carried as a member). Members that are not on
    disk are silently skipped — at scoring time
    :func:`score_race_with_resolution` will detect the gap and fall back to
    the category-global booster.

    Returns an empty pool when ``category`` has no registered ensembles
    (Ban-ei today). Cold-start latency is proportional to the number of unique
    member ``model_version`` strings across all registered ensembles for
    ``category``.
    """
    paths_by_version: dict[str, Path] = {}
    arch_by_version: dict[str, Architecture] = {}
    models_root = models_dir / R2_KEY_PREFIX
    baseline_mv = model_version_for(category)
    baseline_paths = _baseline_member_paths(models_dir, category)
    for (cat, code), _ensemble_mv in PER_CLASS_MODEL_VERSIONS.items():
        if cat != category:
            continue
        manifest = load_ensemble_manifest(models_dir, cat, code)
        if manifest is None:
            continue
        member_mvs = tuple(member.model_version for member in manifest.members)
        discovered = discover_member_models(models_root, cat, code, member_mvs)
        for member_mv, path in discovered.items():
            paths_by_version[member_mv] = path
            arch_by_version[member_mv] = resolve_member_architecture(
                member_mv, category
            )
        for member_mv in member_mvs:
            if member_mv != baseline_mv:
                continue
            if member_mv in paths_by_version:
                continue
            baseline_path = baseline_paths.get(member_mv)
            if baseline_path is None:
                continue
            paths_by_version[member_mv] = baseline_path
            arch_by_version[member_mv] = resolve_member_architecture(
                member_mv, category
            )
    return build_pool_from_paths(paths_by_version, arch_by_version)


def _race_id_series(race_id: str, length: int) -> pd.Series:
    """Return a 1-column Series of ``race_id`` repeated ``length`` times.

    Used inside :func:`score_race_with_resolution` to feed
    :func:`predict_lib.ensemble_scorer.normalize_within_race` which requires a
    Series aligned with the per-entry score vector. Wrapped in a helper so the
    test can assert the exact construction and the dtype stays consistent
    (object), matching the parquet-loaded ``race_id`` Series produced upstream.
    """
    return pd.Series([race_id] * length, dtype="object")


def _tiebreak_series(entries: Sequence[Mapping[str, object]]) -> pd.Series:
    """Return a Series of tiebreak keys aligned with ``entries`` row order.

    The tiebreak is ``ketto_toroku_bango`` (string), mirroring
    :func:`predict_lib.rank.rank_within_race`'s ascending tiebreak. Missing
    values collapse to the empty string so a None upstream never propagates as
    a sort-time TypeError.
    """
    return pd.Series([str(entry.get(KETTO_FIELD, "")) for entry in entries], dtype="object")


def _score_member(
    member: EnsembleMember,
    pool: BoosterPool,
    matrix_by_arch: dict[Architecture, Sequence[Sequence[float]]],
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> tuple[np.ndarray | None, str | None]:
    """Score one ensemble member, returning ``(scores, fallback_reason)``.

    The feature matrix is built lazily per architecture and cached in
    ``matrix_by_arch`` so a mixed-arch NAR ensemble (XGBoost baseline +
    CatBoost residual) pays the matrix-build cost twice at most — once per
    arch — instead of once per member. Returns
    ``(None, "member-missing:<mv>")`` when the booster is absent from the
    pool, ``(None, "score-error:<cls>")`` when ``predict`` raises, or
    ``(array, None)`` on success.
    """
    record = pool.get_record(member.model_version)
    if record is None:
        return None, f"member-missing:{member.model_version}"
    matrix = matrix_by_arch.get(record.architecture)
    if matrix is None:
        matrix = build_feature_matrix(entries, feature_names, record.architecture)
        matrix_by_arch[record.architecture] = matrix
    try:
        scores = score_matrix(record.booster, matrix)
    except BaseException as score_error:
        return None, f"score-error:{type(score_error).__name__}"
    return np.asarray(scores, dtype=np.float64), None


def _score_ensemble(
    ensemble: PerClassEnsemble,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    pool: BoosterPool,
) -> tuple[list[float] | None, str | None]:
    """Score every member, normalise within race, and blend per the manifest.

    Returns ``(scores, None)`` on the happy path or ``(None, reason)`` on the
    first member failure. The caller falls back to the category-global
    booster on any non-``None`` reason; we never partially-blend.
    """
    matrix_by_arch: dict[Architecture, Sequence[Sequence[float]]] = {}
    member_scores: dict[str, np.ndarray] = {}
    weights: dict[str, float] = {}
    for member in ensemble.members:
        scored, reason = _score_member(
            member, pool, matrix_by_arch, entries, feature_names
        )
        if scored is None:
            return None, reason
        member_scores[member.model_version] = scored
        weights[member.model_version] = member.weight
    race_id_series = _race_id_series(race_id, len(entries))
    tiebreak = _tiebreak_series(entries)
    try:
        blended = score_with_ensemble(member_scores, weights, race_id_series, tiebreak)
    except BaseException as blend_error:
        return None, f"score-error:{type(blend_error).__name__}"
    if len(blended) != len(entries):
        return None, f"score-error:shape({len(blended)}!={len(entries)})"
    return [float(value) for value in blended], None


def _score_single(
    fallback_booster: BoosterLike,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    architecture: Architecture,
) -> list[float]:
    """Score with the category-global booster (existing single-model path)."""
    matrix = build_feature_matrix(entries, feature_names, architecture)
    return score_matrix(fallback_booster, matrix)


def score_race_with_resolution(
    *,
    resolution: PerClassEnsemble | str,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    architecture: Architecture,
    pool: BoosterPool,
    fallback_booster: BoosterLike,
    fallback_model_version: str,
) -> EnsembleRouteOutcome:
    """Score one race honouring the per-class ensemble routing decision.

    When ``resolution`` is a string the single-booster path runs and the
    string is written through as the prediction's ``model_version``. When it
    is a :class:`PerClassEnsemble` the rank-blend path runs; on success the
    manifest's ``model_version`` is emitted, on any failure the category-
    global fallback booster scores the race and the fallback
    ``model_version`` is emitted. The dual contract (label + scores stay
    consistent) is the production-safety guarantee — a corrupt ensemble
    never produces ambiguously-labelled predictions.

    ``architecture`` here is the category-global architecture used for the
    single-model fallback path; ensemble members each carry their own arch via
    :meth:`predict_lib.booster_pool.BoosterPool.get_record` so a mixed-arch
    NAR ensemble (iter 12 XGBoost baseline + iter 30 CatBoost residual) is
    scored correctly without the caller needing to know member-level details.

    ``fallback_reason`` is ``None`` for the single-model path and for the
    ensemble happy path. When the ensemble path falls back the reason is a
    short, bounded-vocabulary tag the caller logs once per fallback so an
    operator can grep for it in container logs.
    """
    if isinstance(resolution, str):
        scores = _score_single(fallback_booster, entries, feature_names, architecture)
        return EnsembleRouteOutcome(
            scores=scores, model_version=resolution, fallback_reason=None
        )
    scores_or_none, reason = _score_ensemble(
        resolution, race_id, entries, feature_names, pool
    )
    if scores_or_none is None:
        fallback_scores = _score_single(
            fallback_booster, entries, feature_names, architecture
        )
        return EnsembleRouteOutcome(
            scores=fallback_scores,
            model_version=fallback_model_version,
            fallback_reason=reason,
        )
    return EnsembleRouteOutcome(
        scores=scores_or_none, model_version=resolution.model_version, fallback_reason=None
    )


__all__ = [
    "EnsembleRouteOutcome",
    "PoolBooster",
    "init_member_pool",
    "resolve_member_architecture",
    "score_race_with_resolution",
]
