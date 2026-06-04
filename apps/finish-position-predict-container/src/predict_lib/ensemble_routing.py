"""Per-race ensemble routing for the container's daily predictor (Phase B-2E).

Wires together the Wave-1 primitives (per_class manifest loader + booster pool +
ensemble scorer) into two pure helpers that ``predict_upcoming.py`` consumes:

* :func:`init_member_pool` — once per category at startup, walks the per-class
  registry, loads every ensemble manifest's member booster from disk, and
  returns a :class:`predict_lib.booster_pool.BoosterPool` shared across all
  races. Cold-start cost is paid once; per-race scoring is a dict lookup.
* :func:`score_race_with_resolution` — once per race at scoring time, given a
  :class:`predict_lib.per_class.PerClassEnsemble | str` resolution it either
  (a) scores with the category-global fallback booster (single-model path) or
  (b) scores each ensemble member from the pool and blends them with the
  manifest weights. Any failure on the ensemble path (missing member booster,
  scoring exception, shape mismatch) falls back to the single-booster path
  with the category-global ``model_version`` label so a per-class corruption
  never blocks predictions for the day.

All side effects (file I/O, native booster loading) are inherited from the
Wave-1 primitives; this module itself is pure — :func:`init_member_pool` is
the only function that touches the filesystem, and it does so through the
existing ``discover_member_models`` + ``build_pool_from_paths`` helpers.

Phase B-2A registry currently lists exactly one ensemble — ``("jra", "703")``
-> ``iter23-jra-cb-ensemble-703-v8`` — so the pool stays small in production.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .booster_pool import BoosterPool, build_pool_from_paths, discover_member_models
from .ensemble_scorer import score_with_ensemble
from .model_meta import R2_KEY_PREFIX, Architecture, Category
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


def init_member_pool(models_dir: Path, category: Category) -> BoosterPool:
    """Load every ensemble member booster registered for ``category``.

    Pure-by-design walker: iterates ``PER_CLASS_MODEL_VERSIONS``, asks
    ``load_ensemble_manifest`` for each ``(category, code)`` pair, and for
    every manifest that parses successfully extracts the member
    ``model_version`` tuple and resolves it through
    :func:`predict_lib.booster_pool.discover_member_models`. Members that are
    not on disk are silently skipped — at scoring time
    :func:`score_race_with_resolution` will detect the gap and fall back to
    the category-global booster.

    Returns an empty pool when ``category`` has no registered ensembles
    (Ban-ei / NAR today, JRA classes other than 703). Cold-start latency is
    proportional to the number of unique member ``model_version`` strings
    across all registered ensembles for ``category``.
    """
    paths_by_version: dict[str, Path] = {}
    models_root = models_dir / R2_KEY_PREFIX
    for (cat, code), _ensemble_mv in PER_CLASS_MODEL_VERSIONS.items():
        if cat != category:
            continue
        manifest = load_ensemble_manifest(models_dir, cat, code)
        if manifest is None:
            continue
        member_mvs = tuple(member.model_version for member in manifest.members)
        discovered = discover_member_models(models_root, cat, code, member_mvs)
        paths_by_version.update(discovered)
    return build_pool_from_paths(paths_by_version)


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
    matrix: Sequence[Sequence[float]],
) -> tuple[np.ndarray | None, str | None]:
    """Score one ensemble member, returning ``(scores, fallback_reason)``.

    Returns ``(None, "member-missing:<mv>")`` when the booster is absent from
    the pool, ``(None, "score-error:<cls>")`` when ``predict`` raises, or
    ``(array, None)`` on success. The shape is asserted by the caller during
    blend; per-member length is whatever the booster emits.
    """
    booster = pool.get(member.model_version)
    if booster is None:
        return None, f"member-missing:{member.model_version}"
    try:
        scores = score_matrix(booster, matrix)
    except BaseException as score_error:
        return None, f"score-error:{type(score_error).__name__}"
    return np.asarray(scores, dtype=np.float64), None


def _score_ensemble(
    ensemble: PerClassEnsemble,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    architecture: Architecture,
    pool: BoosterPool,
) -> tuple[list[float] | None, str | None]:
    """Score every member, normalise within race, and blend per the manifest.

    Returns ``(scores, None)`` on the happy path or ``(None, reason)`` on the
    first member failure. The caller falls back to the category-global
    booster on any non-``None`` reason; we never partially-blend.
    """
    matrix = build_feature_matrix(entries, feature_names, architecture)
    member_scores: dict[str, np.ndarray] = {}
    weights: dict[str, float] = {}
    for member in ensemble.members:
        scored, reason = _score_member(member, pool, matrix)
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
        resolution, race_id, entries, feature_names, architecture, pool
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
