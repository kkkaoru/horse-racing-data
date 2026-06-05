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

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

from .booster_pool import (
    BoosterPool,
    PoolBooster,
    build_pool_from_paths,
    discover_baseline_member_model,
    discover_member_models,
    load_member_feature_names,
)
from .ensemble_scorer import score_with_ensemble
from .model_meta import (
    METADATA_FILE_NAME,
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

# Synthetic feature injected at inference per category: the baseline member's
# RAW ``predicted_score`` (verified against ``tmp/v8/iter22_train_predict_
# residual.py`` ``attach_iter14_score`` — a rename of the raw score, NO
# normalization). Residual / chain members were trained with this column, so
# the scorer must score the baseline FIRST and inject its raw scores under this
# name before scoring the non-baseline members.
SCORE_FEATURE_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "iter14_score",
    "nar": "iter12_score",
}


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


def member_feature_order_matches(
    model_feature_names: Sequence[str],
    metadata_feature_names: Sequence[str],
) -> bool:
    """Return True when the loaded booster's feature order matches metadata.

    CatBoost JSON models populate ``feature_names_`` with the exact positional
    order they score against; the member metadata.json carries the same ordered
    list. A mismatch means the metadata sidecar and the booster disagree on
    column order, so the member would be scored on a permuted matrix — the
    caller skips such a member rather than silently emit wrong scores.

    An EMPTY ``model_feature_names`` is treated as a match (returns ``True``):
    XGBoost boosters and any model whose ``feature_names_`` was not populated
    cannot be order-checked, so the assertion is a no-op for them.
    """
    if not model_feature_names:
        return True
    return tuple(model_feature_names) == tuple(metadata_feature_names)


def catboost_model_feature_names(record: PoolBooster) -> tuple[str, ...]:
    """Read the loaded CatBoost booster's ``feature_names_`` defensively.

    Only CatBoost members are order-checked: the native CatBoost model exposes
    ``feature_names_`` (the positional float-feature order). XGBoost boosters
    clear their names at load (see ``xgboost_adapter``) and are skipped, so this
    returns an empty tuple for non-CatBoost records or any booster whose
    ``feature_names_`` attribute is absent / empty.
    """
    if record.architecture != "catboost":
        return ()
    names = getattr(record.booster, "feature_names_", None)
    if not isinstance(names, (list, tuple)):
        return ()
    if not all(isinstance(name, str) for name in names):
        return ()
    return tuple(names)


def _resolve_member_feature_names(
    model_path: Path,
    member_mv: str,
    baseline_mv: str,
) -> tuple[str, ...] | None:
    """Load a non-baseline member's ordered feature_names from its metadata.json.

    Returns the feature_names tuple on success. Failure posture is keyed on
    whether the member IS the category-global baseline:

    * non-baseline member metadata failure -> log ``member-metadata-missing:
      <mv>`` to stderr and return ``None`` so the caller SKIPS the member
      (the surviving members still serve, and the baseline safety-net remains);
    * baseline member metadata failure -> re-raise, because the baseline is the
      fallback safety net for every ensemble and the synthetic-score injection
      reads its feature set; a broken baseline must fail loud, not degrade.

    NOTE: the baseline path in :func:`init_member_pool` calls
    :func:`load_member_feature_names` DIRECTLY so the type-checker sees a
    plain ``tuple[str, ...]`` (this resolver's ``| None`` return type is only
    used on the non-baseline per-class arm).
    """
    metadata_path = model_path.with_name(METADATA_FILE_NAME)
    try:
        return load_member_feature_names(metadata_path)
    except (FileNotFoundError, ValueError):
        if member_mv == baseline_mv:
            raise
        print(f"member-metadata-missing:{member_mv}", file=sys.stderr)
        return None


def drop_order_mismatched_members(pool: BoosterPool) -> BoosterPool:
    """Return a pool with CatBoost members whose feature order disagrees dropped.

    After loading, each CatBoost member's native ``feature_names_`` is compared
    against the metadata-derived order (see :func:`member_feature_order_matches`).
    A mismatch means the matrix the scorer would build is permuted relative to
    what the booster expects, so the member is dropped and a
    ``member-order-mismatch:<mv>`` line is logged. XGBoost members (empty
    ``feature_names_`` after the loader clears them) always pass.
    """
    kept: dict[str, PoolBooster] = {}
    for member_mv, record in pool.boosters.items():
        model_names = catboost_model_feature_names(record)
        if member_feature_order_matches(model_names, record.feature_names):
            kept[member_mv] = record
            continue
        print(f"member-order-mismatch:{member_mv}", file=sys.stderr)
    return BoosterPool(boosters=kept)


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

    Each surviving member's ordered ``feature_names`` is read from its sibling
    metadata.json so the scorer can project entries onto the member's OWN
    column order (a member trained on 254 columns must not be scored with the
    241-wide global list). Non-baseline metadata failures skip the member
    (logged ``member-metadata-missing:<mv>``); a baseline failure re-raises.
    After loading, CatBoost members whose native ``feature_names_`` disagree
    with metadata are dropped (logged ``member-order-mismatch:<mv>``).

    Returns an empty pool when ``category`` has no registered ensembles
    (Ban-ei today). Cold-start latency is proportional to the number of unique
    member ``model_version`` strings across all registered ensembles for
    ``category``.
    """
    paths_by_version: dict[str, Path] = {}
    arch_by_version: dict[str, Architecture] = {}
    names_by_version: dict[str, tuple[str, ...]] = {}
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
            feature_names = _resolve_member_feature_names(path, member_mv, baseline_mv)
            if feature_names is None:
                continue
            paths_by_version[member_mv] = path
            arch_by_version[member_mv] = resolve_member_architecture(member_mv, category)
            names_by_version[member_mv] = feature_names
        for member_mv in member_mvs:
            if member_mv != baseline_mv:
                continue
            if member_mv in paths_by_version:
                continue
            baseline_path = baseline_paths.get(member_mv)
            if baseline_path is None:
                continue
            # Baseline metadata MUST be present — the synthetic-score injection
            # reads the baseline's feature set and the baseline is the fallback
            # safety net for every ensemble, so a missing sidecar fails LOUD
            # rather than degrades. Calling ``load_member_feature_names``
            # directly (instead of the non-baseline-tolerant resolver) gives a
            # plain ``tuple[str, ...]`` return so the type-checker stays happy.
            baseline_metadata_path = baseline_path.with_name(METADATA_FILE_NAME)
            feature_names = load_member_feature_names(baseline_metadata_path)
            paths_by_version[member_mv] = baseline_path
            arch_by_version[member_mv] = resolve_member_architecture(member_mv, category)
            names_by_version[member_mv] = feature_names
    pool = build_pool_from_paths(paths_by_version, arch_by_version, names_by_version)
    return drop_order_mismatched_members(pool)


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


MatrixCacheKey = tuple[Architecture, tuple[str, ...]]


def member_feature_names_for_record(
    record: PoolBooster,
    feature_names: Sequence[str],
) -> tuple[str, ...]:
    """Return the member's OWN feature order, or the global list when empty.

    A pool record built before the metadata wiring (or for the single-model
    fallback path) carries an empty ``feature_names`` tuple; in that case the
    caller-supplied category-global list is used so the legacy single-model
    behaviour is preserved.
    """
    if record.feature_names:
        return record.feature_names
    return tuple(feature_names)


def score_member(
    member: EnsembleMember,
    pool: BoosterPool,
    matrix_by_key: dict[MatrixCacheKey, Sequence[Sequence[float]]],
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> tuple[np.ndarray | None, str | None]:
    """Score one ensemble member, returning ``(scores, fallback_reason)``.

    The feature matrix is built lazily per ``(architecture, member feature
    order)`` and cached in ``matrix_by_key`` so two members sharing the same
    arch AND feature list reuse one matrix, while two members with DIFFERENT
    feature lists each get their own correctly-shaped matrix — the core fix for
    the wrong-width fallback bug. Missing entry keys (e.g. legacy duplicate-
    suffix metadata columns like NAR ``shusso_tosu``+ ``shusso_tosu_1``) get
    0-filled by :func:`build_feature_matrix`, preserving the pre-WIP fallback
    behaviour for every member. Returns ``(None, "member-missing:<mv>")`` when
    the booster is absent from the pool, ``(None, "score-error:<cls>")`` when
    ``predict`` raises, or ``(array, None)`` on success.
    """
    record = pool.get_record(member.model_version)
    if record is None:
        return None, f"member-missing:{member.model_version}"
    member_names = member_feature_names_for_record(record, feature_names)
    cache_key: MatrixCacheKey = (record.architecture, member_names)
    matrix = matrix_by_key.get(cache_key)
    if matrix is None:
        matrix = build_feature_matrix(entries, member_names, record.architecture)
        matrix_by_key[cache_key] = matrix
    try:
        scores = score_matrix(record.booster, matrix)
    except BaseException as score_error:
        return None, f"score-error:{type(score_error).__name__}"
    return np.asarray(scores, dtype=np.float64), None


def find_baseline_member(ensemble: PerClassEnsemble) -> EnsembleMember | None:
    """Return the manifest's baseline member, or ``None`` when none is flagged.

    The baseline (iter 14 JRA / iter 12 NAR) is scored FIRST so its raw scores
    can be injected as the synthetic ``score_col`` feature the residual / chain
    members were trained with.
    """
    for member in ensemble.members:
        if member.is_baseline:
            return member
    return None


def augment_entries_with_score_col(
    entries: Sequence[Mapping[str, object]],
    score_col: str | None,
    raw_scores: np.ndarray,
) -> list[Mapping[str, object]]:
    """Return entries with the baseline's RAW score injected as ``score_col``.

    Mirrors ``tmp/v8/iter22_train_predict_residual.py`` ``attach_iter14_score``:
    the baseline ``predicted_score`` is injected verbatim (NO normalization).
    When ``score_col`` is ``None`` (a category with no synthetic-score feature)
    the entries pass through unchanged.
    """
    if score_col is None:
        return list(entries)
    return [{**entry, score_col: float(raw_scores[index])} for index, entry in enumerate(entries)]


def _score_ensemble(
    ensemble: PerClassEnsemble,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    pool: BoosterPool,
    category: Category,
) -> tuple[list[float] | None, str | None]:
    """Score every member, normalise within race, and blend per the manifest.

    Two-pass to honour the synthetic baseline-score feature
    (``iter14_score`` / ``iter12_score``): pass 1 scores the baseline member on
    the plain entries and keeps its RAW scores; the raw vector is injected under
    the category's ``score_col`` so pass 2 can score each non-baseline member
    against the augmented entries with the member's OWN feature order. Members
    that do not reference ``score_col`` are unaffected — the per-member matrix
    only pulls their declared names. Returns ``(scores, None)`` on the happy
    path or ``(None, reason)`` on the first failure; the caller falls back to
    the category-global booster on any non-``None`` reason (P0 keeps the
    whole-ensemble fallback posture for every member error).
    """
    score_col = SCORE_FEATURE_BY_CATEGORY.get(category)
    baseline = find_baseline_member(ensemble)
    if baseline is None:
        return None, "score-error:no-baseline"
    matrix_by_key: dict[MatrixCacheKey, Sequence[Sequence[float]]] = {}
    baseline_scores, baseline_reason = score_member(
        baseline, pool, matrix_by_key, entries, feature_names
    )
    if baseline_scores is None:
        return None, baseline_reason
    entries_aug = augment_entries_with_score_col(entries, score_col, baseline_scores)
    member_scores: dict[str, np.ndarray] = {baseline.model_version: baseline_scores}
    weights: dict[str, float] = {baseline.model_version: baseline.weight}
    for member in ensemble.members:
        if member.is_baseline:
            continue
        scored, reason = score_member(
            member, pool, matrix_by_key, entries_aug, feature_names
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
        resolution, race_id, entries, feature_names, pool, resolution.category
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
