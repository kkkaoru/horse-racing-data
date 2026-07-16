"""Fail-closed guard against writing predictions from a degenerate feature matrix.

2026-07-17 incident context (see
``apps/pc-keiba-viewer/docs/probes/jra-269-serve-defect-2026-07-17.md``): a
single ~47-second write burst on 2026-07-12 05:51:45-05:52:32 UTC produced
JRA finish-position predictions, under BOTH the category-global champion
model AND the ``jockey-pedigree269`` cell-routing variant, whose within-race
``predicted_score`` standard deviation was collapsed to roughly 1/10th of
every other observed write (healthy clusters: 0.69-1.51; that burst:
0.04-0.16 for every affected race, across both models). This is the
signature of a booster scoring a feature matrix that is mostly zero-filled --
``predict_lib.scorer.build_feature_row`` coerces an absent or ``None`` entry
value to ``0.0``, so a race whose upstream feature build silently returned a
near-empty entry dict still scores "successfully" and produces a smooth,
plausible-looking (but uninformative) prediction. The exact upstream trigger
was not conclusively identified from static analysis (COORDINATOR_ENABLED /
mode=rescore was off at the time; the day-base split fast path was not in
JRA's enabled allowlist), so this module does not attempt to fix the trigger
-- it closes the observable failure mode itself: NEVER silently write a
prediction built from a feature matrix that is mostly missing.

This mirrors the codebase's existing "fail toward the safe path, never
silently degrade" convention (``CacheMissError``,
``pipeline_runner.day_base_covers_entry_list`` returning ``False`` on any
exception, ``ensemble_routing.score_member`` returning a
``member-column-gap`` fallback reason) -- the caller (``predict_upcoming``'s
per-race scoring functions) skips writing that race's rows entirely rather
than upserting a confidently-wrong prediction. A skipped race is
indistinguishable, from Neon's point of view, from a race that has not been
scored yet, so the existing coverage self-healing cron
(``apps/finish-position-cron/src/coverage-self-heal.ts``) naturally retries
it on its own schedule once the transient upstream condition clears.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Final

# Chosen from the 2026-07-17 incident data: every genuinely healthy race
# observed (the 2026-07-11 10:47 JST Mac-batch champion cluster and the two
# pre-2026-07-12 jockey-pedigree269 backfills) had every feature populated for
# every entry (0% missing), while a debut horse legitimately lacking N-year
# history features typically leaves a small minority of columns null. A
# whole-race input failure (this guard's target) leaves the large majority of
# columns null for every entry, so 0.5 sits with wide margin between normal
# sparsity and the failure signature, without needing per-column semantics.
DEFAULT_MISSING_FEATURE_FRACTION_THRESHOLD: Final[float] = 0.5


def missing_feature_fraction(entry: Mapping[str, object], feature_names: Sequence[str]) -> float:
    """Return the fraction of ``feature_names`` absent (or ``None``) from ``entry``.

    Inspects ``entry`` BEFORE ``predict_lib.scorer``'s numeric coercion, which
    collapses both "key absent" and "value is None" to ``0.0`` and therefore
    cannot be used after the fact to distinguish "genuinely missing" from "a
    legitimately zero-valued feature" (e.g. a binary flag that is really 0).
    Returns ``0.0`` for an empty ``feature_names`` (nothing to be missing).
    """
    if not feature_names:
        return 0.0
    missing = sum(1 for name in feature_names if entry.get(name) is None)
    return missing / len(feature_names)


def race_missing_feature_fraction(
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> float:
    """Return the mean per-entry missing-feature fraction across one race.

    Averaged (not maxed) across entries: the failure signature this guards
    against is a whole-race input failure affecting every entry uniformly, so
    averaging stays robust to a single legitimately-sparse entry (e.g. one
    debut horse lacking history features) that should NOT by itself trip the
    guard. Returns ``0.0`` for an empty race (nothing to be missing).
    """
    if not entries:
        return 0.0
    return sum(
        missing_feature_fraction(entry, feature_names) for entry in entries
    ) / len(entries)


def is_degenerate_feature_matrix(
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    threshold: float = DEFAULT_MISSING_FEATURE_FRACTION_THRESHOLD,
) -> bool:
    """Return True when a race's feature inputs are too sparse to score safely.

    ``threshold`` is a fraction in ``[0, 1]``; the default
    (:data:`DEFAULT_MISSING_FEATURE_FRACTION_THRESHOLD`) is calibrated against
    the 2026-07-17 incident data (see module docstring). Callers should skip
    writing predictions for a race where this returns True rather than
    upsert a confidently-wrong row -- see module docstring for the full
    rationale and the self-healing recovery path.
    """
    return race_missing_feature_fraction(entries, feature_names) >= threshold
