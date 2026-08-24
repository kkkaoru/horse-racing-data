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
   race. A real rank must be strictly positive (1-based): raw JVD odds columns
   use a non-NULL ``'00'`` placeholder for "unconfirmed" that not every
   upstream SQL path strips before casting to int, so a coerced ``0`` is
   treated the same as missing, never as a real rank (confirmed against
   2,730,085 real NAR feature-store rows: ``tansho_ninkijun`` is never 0 or
   negative). With the default-off
   ``STAGE1_PRESERVED_ODDS_GATE_ENABLED=1`` rollout flag, the gate instead
   requires a valid full board: every active entry has positive finite
   ``tansho_odds``, ranks form ``1..N``, and odds are nondecreasing with rank.
   The canonical odds survive near-miss in both JRA and NAR. When canonical
   rank does not, the gate recovers it from the champion's retained normalized
   ``popularity_score``. This rejects transitional partial boards; flag OFF
   preserves the historical any-positive-canonical-rank behavior exactly.
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

   **Not every Stage-2 scoring path can use this safety net.**
   ``Stage1CategoryConfig.enable_stddev_safety_net`` lets a category disable
   it explicitly (see :func:`resolve_stage1_gate`'s docstring): a within-race
   z-normalized fused score (e.g. NAR's XGBoost-base x Set-Transformer
   score-level z-fusion) structurally cannot collapse toward this signature
   the way JRA's raw CatBoost score does, so no threshold value would ever
   correctly separate incident from healthy for such a category -- confirmed
   against real NAR-served Neon data (548 races incl. the 2026-07-15..18
   odds-freeze window: within-race stddev never dropped below ~0.36, no
   distinct low-stddev incident signature). A category with the net disabled
   still gets the freshness gate at full strength; ``stddev_threshold`` is
   still a required config field for schema simplicity but is never consulted.

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
import math
import os
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Final, Literal, get_args

from .late_binding import (
    POPULARITY_SCORE_FIELD,
    TANSHO_NINKIJUN_FIELD,
    TANSHO_ODDS_FIELD,
    coerce_optional_float,
    coerce_optional_int,
)
from .model_meta import Architecture
from .upsert_sql import INSERT_COLUMNS

STAGE1_ROUTING_PATH: Final[Path] = Path(__file__).parent / "stage1_routing.json"
STAGE1_ROUTING_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar", "ban-ei"})
STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV: Final[str] = "STAGE1_PRESERVED_ODDS_GATE_ENABLED"
_REQUIRED_KEYS: Final[frozenset[str]] = frozenset(
    {
        "enabled",
        "model_version",
        "feature_count",
        "architecture",
        "stddev_threshold",
        "enable_stddev_safety_net",
    }
)
_OPTIONAL_KEYS: Final[frozenset[str]] = frozenset(
    {"top1_swap_base_model_version", "top1_swap_prior3_temperature_lt"}
)
_ARCHITECTURES: Final[frozenset[str]] = frozenset(get_args(Architecture))
PRIOR3_TEMPERATURE_FIELD: Final[str] = "venue_temperature_prior3"

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
    enable_stddev_safety_net: bool
    top1_swap_base_model_version: str | None = None
    top1_swap_prior3_temperature_lt: float | None = None


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


def _optional_non_empty_string(payload: Mapping[str, object], key: str, context: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise Stage1RoutingValidationError(f"{context}.{key} must be null or a non-empty string")
    return value


def _optional_positive_number(
    payload: Mapping[str, object], key: str, context: str
) -> float | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise Stage1RoutingValidationError(f"{context}.{key} must be null or a positive number")
    return float(value)


def _parse_category_config(value: object, category: str) -> Stage1CategoryConfig:
    context = f"stage1_routing.json[{category}]"
    payload = _as_mapping(value, context)
    actual_keys = frozenset(payload)
    allowed_keys = _REQUIRED_KEYS | _OPTIONAL_KEYS
    if not _REQUIRED_KEYS.issubset(actual_keys) or not actual_keys.issubset(allowed_keys):
        missing = sorted(_REQUIRED_KEYS - actual_keys)
        unexpected = sorted(actual_keys - allowed_keys)
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
        enable_stddev_safety_net=_require_bool(payload, "enable_stddev_safety_net", context),
        top1_swap_base_model_version=_optional_non_empty_string(
            payload, "top1_swap_base_model_version", context
        ),
        top1_swap_prior3_temperature_lt=_optional_positive_number(
            payload, "top1_swap_prior3_temperature_lt", context
        ),
    )


def race_passes_top1_swap_weather_gate(
    config: Stage1CategoryConfig,
    entries: Sequence[Mapping[str, object]],
) -> bool:
    """Require one complete, consistent pre-race temperature per race.

    A missing or inconsistent Cloudflare window fails closed to the established
    300-tree Stage-1 base. The threshold is exclusive, matching the validated
    research rule (prior-three-hour mean temperature < 28°C).
    """
    threshold = config.top1_swap_prior3_temperature_lt
    if threshold is None:
        return True
    temperatures = [coerce_optional_float(entry.get(PRIOR3_TEMPERATURE_FIELD)) for entry in entries]
    if not temperatures or any(value is None for value in temperatures):
        return False
    values = [float(value) for value in temperatures if value is not None]
    first = values[0]
    if not all(math.isclose(value, first, rel_tol=0.0, abs_tol=1e-9) for value in values[1:]):
        return False
    return first < threshold


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


def preserved_odds_gate_enabled() -> bool:
    """Return whether the rollback-safe preserved-odds fallback is enabled.

    Default-off keeps the production routing behavior unchanged. Setting the
    runtime flag to exactly ``1`` lets :func:`race_has_fresh_odds` use the
    canonical ``tansho_odds`` column that survives the near-miss layer when the
    canonical rank column was projected out. Any other value fails closed to
    the established rank-only gate.
    """
    return os.environ.get(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "").strip() == "1"


def _rank_from_popularity_score(value: object, runner_count: int) -> int | None:
    """Recover a 1-based rank from the builder's normalized popularity score.

    This is coupled to ``finish_position_features_duckdb.py``'s
    ``popularity_score = (rank - 1) / (runner_count - 1)`` formula, where
    ``runner_count`` is the active target race's ``shusso_tosu``. If that
    builder formula changes, this inverse and its full-board tests must change
    in the same commit.
    """
    score = coerce_optional_float(value)
    if score is None or not math.isfinite(score) or score < 0 or score > 1:
        return None
    rank_value = score * (runner_count - 1) + 1
    rank = round(rank_value)
    if not math.isclose(rank_value, rank, abs_tol=1e-9):
        return None
    return rank


def _has_valid_full_board(entries: Sequence[Mapping[str, object]]) -> bool:
    """Validate complete coverage, a rank permutation, and odds/rank ordering."""
    runner_count = len(entries)
    if runner_count < 2:
        return False

    canonical_ranks = [coerce_optional_int(entry.get(TANSHO_NINKIJUN_FIELD)) for entry in entries]
    if all(rank is not None and rank > 0 for rank in canonical_ranks):
        ranks = [int(rank) for rank in canonical_ranks if rank is not None]
    else:
        derived_ranks = [
            _rank_from_popularity_score(entry.get(POPULARITY_SCORE_FIELD), runner_count)
            for entry in entries
        ]
        if any(rank is None for rank in derived_ranks):
            return False
        ranks = [int(rank) for rank in derived_ranks if rank is not None]

    if set(ranks) != set(range(1, runner_count + 1)):
        return False

    odds = [coerce_optional_float(entry.get(TANSHO_ODDS_FIELD)) for entry in entries]
    if any(value is None or not math.isfinite(value) or value <= 0 for value in odds):
        return False
    ranked_odds = sorted(
        zip(ranks, (float(value) for value in odds if value is not None), strict=True)
    )
    return all(current[1] <= following[1] for current, following in pairwise(ranked_odds))


def race_has_fresh_odds(entries: Sequence[Mapping[str, object]]) -> bool:
    """Return whether the race carries a trustworthy market board.

    Flag OFF preserves the original any-positive-rank predicate exactly. Flag
    ON requires a valid full board: every active entry has positive finite odds,
    ranks form ``1..N``, and odds are nondecreasing with rank. When near-miss
    removed the canonical rank, the rank permutation is recovered from the
    retained normalized ``popularity_score`` that the champion model consumes.
    """
    if preserved_odds_gate_enabled():
        return _has_valid_full_board(entries)
    return any(
        (rank := coerce_optional_int(entry.get(TANSHO_NINKIJUN_FIELD))) is not None and rank > 0
        for entry in entries
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

    ``config.enable_stddev_safety_net`` lets a category opt out of the
    second condition entirely (freshness-gate-only): some Stage-2 scoring
    paths within-race normalize their score (e.g. NAR's z-normalized
    score-level fusion of the XGBoost base and Set Transformer blend,
    ``transformer_scorer.fuse_ensemble_transformer``) so the raw stddev
    signature this safety net relies on cannot collapse the way JRA's champion
    CatBoost score does even during a genuine incident -- no threshold value
    would ever trip correctly for such a category (confirmed against real
    NAR-served Neon data: within-race stddev never dropped below ~0.36 across
    548 races including the 2026-07-15..18 odds-freeze window, with no
    distinct low-stddev signature separating incident from healthy days).
    ``compute_predicted_score_stddev`` is skipped entirely (not merely
    unused) when the net is disabled, and the reported ``stddev`` is ``None``
    -- there is nothing meaningful to report for a metric that structurally
    cannot signal anything for that category.
    """
    if config is None or not config.enabled:
        return Stage1GateDecision(use_stage1=False, reason="disabled", stddev=None)
    if not race_has_fresh_odds(entries):
        return Stage1GateDecision(use_stage1=True, reason="odds-missing", stddev=None)
    if not config.enable_stddev_safety_net:
        return Stage1GateDecision(use_stage1=False, reason="fresh", stddev=None)
    stddev = compute_predicted_score_stddev(stage2_scores)
    if is_score_spread_degraded(stddev, config.stddev_threshold):
        return Stage1GateDecision(use_stage1=True, reason="score-spread-degraded", stddev=stddev)
    return Stage1GateDecision(use_stage1=False, reason="fresh", stddev=stddev)
