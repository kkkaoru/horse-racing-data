"""Stage-1 market-free gated fallback: config + pure routing decision.

Background (see
``docs/finish-position-accuracy/history/jra-stage1-market-free-fallback-probe-2026-07-22.md``):
the JRA champion (``jra-cb-v9-sim-2013-clean``, 250 feat) leans on 15
market/odds-derived features for within-race ranking. When the realtime odds
board is absent for a whole race -- an odds-serving incident (2026-07-12
Cluster B, 2026-07-18 odds-freeze, ``COORDINATOR_ENABLED=0`` gaps) -- those 15
features silently fall back to a training-set median (see ``late_binding.py``)
and the champion's top1 accuracy collapses from ~33.6% to ~9.4% (blind WF,
triple-anchored). A model retrained WITHOUT those 15 features (235 feat,
"Stage-1") only drops to ~28.9% top1 in the same collapse scenario, a
+19.45pp[LB95 +18.44] recovery, at a -4.75pp cost in the healthy regime --
hence a GATED fallback (never a primary replacement): serve the champion
(Stage-2, default) when odds are fresh, Stage-1 only when the gate below
fails. "Stage-1"/"Stage-2" here name the two RANKING MODELS in this gated
architecture -- an unrelated, pre-existing use of "Stage 2" already exists in
this package (``late_binding.py``'s docstring: "Stage 2 of the per-race
rebuild" refers to the late-binding FEATURE REBUILD step, not a model). Do not
conflate the two.

Two independent trip conditions route a race to Stage-1, either is sufficient:

1. **Freshness gate** (primary, cheap, pre-scoring): :func:`race_has_fresh_odds`
   reads ``tansho_ninkijun`` -- the raw odds-board rank late-binding recomputes
   verbatim from the realtime snapshot (never itself median-substituted, unlike
   the derived ``odds_score``/``popularity_score`` -- see
   ``late_binding.apply_late_binding_to_entry``) -- across every entry in the
   race. A race where EVERY entry lacks a real ``tansho_ninkijun`` never
   received odds-board data at all: the whole-race outage this fallback insures
   against. A race where at least one entry has real data is NOT that
   condition, even if a handful of entries individually lack odds (e.g. a very
   late scratch) -- the champion's ranking is not meaningfully impaired by a
   few missing entries.
2. **Stddev safety net** (secondary, post-scoring): :func:`is_score_spread_degraded`
   compares the within-race population stddev of the Stage-2 (champion)
   ``predicted_score`` values already computed against
   ``Stage1CategoryConfig.stddev_threshold``. Reuses
   ``apps/pc-keiba-viewer/src/scripts/serve_health_check.py``'s Cluster-B
   quality signature computation exactly (``statistics.pstdev``, ddof=0), but
   the tracked threshold is 0.4, not that module's 0.3 -- re-validated
   independently against real Neon prediction data for this gate's own,
   different cost asymmetry (a false trip costs -4.75pp for one race; a miss
   costs up to ~-24pp): 2026-07-12 Cluster B (36/36 races, stddev 0.048-0.160,
   a single uniform-collapse incident) and 2026-07-18 odds-freeze (36 races,
   stddev 0.114-0.354, a CONTINUOUS gradient -- consistent with staleness
   rather than total absence, since ``COORDINATOR_ENABLED=0`` lets odds go
   stale rather than making them disappear -- so 0.3 alone would have missed
   7/36, ~19%, of that day's degraded races) against 2026-07-19 healthy (36
   races, stddev 0.749-1.870, matching this module's own documented
   0.72-1.57 Cluster-A range) and 2026-07-11 (a second, independently
   documented mixed-incident day, stddev down to 0.044, corroborating the
   collapse signature recurs). 0.4 covers the full observed incident range
   (max 0.354) with margin, while keeping a >0.3 margin below the observed/
   documented healthy floor (0.72-0.749) -- catches an incident the freshness
   gate cannot see directly (the odds board looked populated -- e.g. stale,
   not absent -- but the resulting ranking still collapsed) by testing the
   OUTCOME instead of the (necessarily incomplete) list of known causes.

``stage1_routing.json`` is a tracked, git-diffable, per-category declaration
(mirrors ``running_style_cell_routing.json`` / ``cell_routing.json``'s
"config file, not a hardcoded constant" convention) so the stddev threshold --
or disabling the gate entirely -- can be tuned without a code change. An
absent category (or ``{}``) means "no Stage-1 fallback configured": the gate
never trips and every race serves Stage-2 unchanged, matching this package's
established "empty/absent config = no live routing" precedent.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, get_args

from .late_binding import TANSHO_NINKIJUN_FIELD, coerce_optional_int
from .model_meta import Architecture
from .upsert_sql import INSERT_COLUMNS

STAGE1_ROUTING_PATH: Final[Path] = Path(__file__).parent / "stage1_routing.json"
STAGE1_ROUTING_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar", "ban-ei"})
_REQUIRED_KEYS: Final[frozenset[str]] = frozenset(
    {"enabled", "model_version", "feature_count", "architecture", "stddev_threshold"}
)
_ARCHITECTURES: Final[frozenset[str]] = frozenset(get_args(Architecture))

PREDICTED_SCORE_COLUMN_INDEX: Final[int] = INSERT_COLUMNS.index("predicted_score")
"""Index of ``predicted_score`` within one flattened ``build_prediction_rows``
value tuple -- derived from the tracked column-name list rather than a magic
number so a future reordering of ``upsert_sql.INSERT_COLUMNS`` cannot silently
desync this module's row-reading from the real layout."""

Stage1GateReason = Literal["disabled", "fresh", "odds-missing", "score-spread-degraded"]


class Stage1RoutingValidationError(ValueError):
    """Raised when the tracked ``stage1_routing.json`` declaration is malformed."""


@dataclass(frozen=True)
class Stage1CategoryConfig:
    enabled: bool
    model_version: str
    feature_count: int
    architecture: str
    stddev_threshold: float


@dataclass(frozen=True)
class Stage1GateDecision:
    """The routing outcome for one race plus why, for observability/logging."""

    use_stage1: bool
    reason: Stage1GateReason
    stddev: float | None


def _as_mapping(value: object, context: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise Stage1RoutingValidationError(f"{context} must be an object")
    return {str(key): item for key, item in value.items()}


def _require_bool(payload: Mapping[str, object], key: str, context: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise Stage1RoutingValidationError(f"{context}.{key} must be a boolean")
    return value


def _require_non_empty_string(payload: Mapping[str, object], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise Stage1RoutingValidationError(f"{context}.{key} must be a non-empty string")
    return value


def _require_positive_int(payload: Mapping[str, object], key: str, context: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise Stage1RoutingValidationError(f"{context}.{key} must be a positive integer")
    return value


def _require_positive_number(payload: Mapping[str, object], key: str, context: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise Stage1RoutingValidationError(f"{context}.{key} must be a positive number")
    return float(value)


def _parse_category_config(value: object, category: str) -> Stage1CategoryConfig:
    context = f"stage1_routing.json[{category}]"
    payload = _as_mapping(value, context)
    actual_keys = frozenset(payload)
    if actual_keys != _REQUIRED_KEYS:
        missing = sorted(_REQUIRED_KEYS - actual_keys)
        unexpected = sorted(actual_keys - _REQUIRED_KEYS)
        raise Stage1RoutingValidationError(
            f"{context} keys differ: missing={missing}, unexpected={unexpected}"
        )
    architecture = _require_non_empty_string(payload, "architecture", context)
    if architecture not in _ARCHITECTURES:
        raise Stage1RoutingValidationError(
            f"{context}.architecture has unsupported value: {architecture}"
        )
    return Stage1CategoryConfig(
        enabled=_require_bool(payload, "enabled", context),
        model_version=_require_non_empty_string(payload, "model_version", context),
        feature_count=_require_positive_int(payload, "feature_count", context),
        architecture=architecture,
        stddev_threshold=_require_positive_number(payload, "stddev_threshold", context),
    )


def load_stage1_routing(
    path: Path = STAGE1_ROUTING_PATH,
) -> dict[str, Stage1CategoryConfig]:
    """Load the tracked Stage-1 routing declaration; ``{}`` means no category
    has a configured fallback (every race serves Stage-2 unchanged)."""
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise Stage1RoutingValidationError(f"stage1_routing.json not found: {path}") from error
    try:
        payload: object = json.loads(raw)
    except json.JSONDecodeError as error:
        raise Stage1RoutingValidationError(f"invalid stage1_routing.json: {error.msg}") from error
    root = _as_mapping(payload, "stage1_routing.json")
    unknown = sorted(frozenset(root) - STAGE1_ROUTING_CATEGORIES)
    if unknown:
        raise Stage1RoutingValidationError(
            f"stage1_routing.json has unsupported categories: {unknown}"
        )
    return {category: _parse_category_config(entry, category) for category, entry in root.items()}


# ---------------------------------------------------------------------------
# Pure gate-decision logic
# ---------------------------------------------------------------------------


def race_has_fresh_odds(entries: Sequence[Mapping[str, object]]) -> bool:
    """Return True when at least one entry carries a real ``tansho_ninkijun``.

    See this module's docstring, condition 1 (freshness gate), for the full
    rationale. Empty ``entries`` returns False (vacuously no fresh data, though
    ``score_races`` never scores an empty race so this is defensive-only).
    """
    return any(
        coerce_optional_int(entry.get(TANSHO_NINKIJUN_FIELD)) is not None for entry in entries
    )


def compute_predicted_score_stddev(scores: Sequence[float]) -> float:
    """Population stddev (ddof=0) of within-race ``predicted_score``.

    Mirrors ``serve_health_check.compute_quality_groups``: fewer than 2 scores
    returns 0.0 instead of raising (``statistics.pstdev`` already does this for
    n=1; this function also treats n=0 the same way defensively, since
    ``statistics.pstdev(())`` raises ``StatisticsError``).
    """
    if len(scores) < 2:
        return 0.0
    return statistics.pstdev(scores)


def is_score_spread_degraded(stddev: float, threshold: float) -> bool:
    """Return True when ``stddev`` falls below the configured degraded threshold."""
    return stddev < threshold


def _coerce_score(value: object) -> float:
    """Coerce one ``predicted_score`` cell to ``float``.

    ``build_prediction_rows`` always writes a genuine ``float`` (see
    ``upcoming._to_scored_horse``), so this narrows the row's ``object``
    element type back to ``float`` without a static type-checker suppression.
    """
    if isinstance(value, bool):
        message = f"predicted_score cell must not be a bool: {value!r}"
        raise TypeError(message)
    if isinstance(value, (int, float)):
        return float(value)
    message = f"predicted_score cell must be numeric, got {type(value).__name__}: {value!r}"
    raise TypeError(message)


def extract_predicted_scores(rows: Sequence[Sequence[object]]) -> list[float]:
    """Read the ``predicted_score`` column back out of flattened prediction rows.

    ``rows`` is the ``list[list[object]]`` shape ``build_prediction_rows``
    returns (one row per horse, column order matching
    ``upsert_sql.INSERT_COLUMNS``). Used to feed the Stage-2 scores already
    computed for a race into :func:`compute_predicted_score_stddev` without
    re-running the booster.
    """
    return [_coerce_score(row[PREDICTED_SCORE_COLUMN_INDEX]) for row in rows]


def resolve_stage1_gate(
    *,
    config: Stage1CategoryConfig | None,
    entries: Sequence[Mapping[str, object]],
    stage2_scores: Sequence[float],
) -> Stage1GateDecision:
    """Decide whether a race should be (re)scored with the Stage-1 fallback.

    ``config`` is ``None`` (or ``config.enabled`` is False) when the category
    has no configured Stage-1 fallback -- always Stage-2, unconditionally.
    Otherwise the freshness gate (cheap, pre-scoring) is checked first; only
    when it passes does the stddev safety net (post-scoring, needs
    ``stage2_scores``) get a chance to trip. Either condition alone is
    sufficient to route to Stage-1.
    """
    if config is None or not config.enabled:
        return Stage1GateDecision(use_stage1=False, reason="disabled", stddev=None)
    if not race_has_fresh_odds(entries):
        return Stage1GateDecision(use_stage1=True, reason="odds-missing", stddev=None)
    stddev = compute_predicted_score_stddev(stage2_scores)
    if is_score_spread_degraded(stddev, config.stddev_threshold):
        return Stage1GateDecision(use_stage1=True, reason="score-spread-degraded", stddev=stddev)
    return Stage1GateDecision(use_stage1=False, reason="fresh", stddev=stddev)
