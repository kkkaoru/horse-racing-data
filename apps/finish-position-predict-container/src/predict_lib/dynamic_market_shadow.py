"""Pure runtime contract for loop-43 dynamic-market shadow evaluation."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Protocol

from . import dynamic_market_versions as _versions
from .dynamic_market_router import (
    DynamicMarketRouterConfig,
    route_dynamic_market_scores,
)
from .jra_joint_alternate_scorer import (
    MODEL_VERSION as JOINT_ALTERNATE_MODEL_VERSION,
)
from .jra_joint_alternate_scorer import (
    JointAlternateCandidate,
)
from .rank import ScoredHorse, rank_within_race
from .subgroup import classify_distance_band, classify_surface

SHADOW_ROUTER_VERSION: Final[str] = _versions.SHADOW_ROUTER_VERSION
SHADOW_TABLE: Final[str] = "finish_position_dynamic_market_shadow_predictions"
COMPARISON_CONTRACT: Final[str] = "served-same-snapshot/v1"
SHADOW_BATCH_SIZE: Final[int] = 500
JOINT_ALTERNATE_ROUTER_VERSION: Final[str] = JOINT_ALTERNATE_MODEL_VERSION
UPSET_FEATURE_NAMES: Final[tuple[str, ...]] = (
    "field_size",
    "favorite_odds",
    "second_odds",
    "third_odds",
    "market_entropy",
    "normalized_entropy",
    "effective_runners",
    "favorite_gap_ratio",
    "favorite_stage1_rank",
    "stage1_top_market_rank",
    "champion_top_market_rank",
    "models_disagree",
    "stage1_margin",
    "champion_margin",
)
ACTIVE_SHADOW_CONFIG: Final[DynamicMarketRouterConfig] = DynamicMarketRouterConfig(
    enabled=True,
    minimum_market_weight=0.95,
    activation_threshold=0.40,
    gamma=0.50,
    high_upset_context_threshold=0.55,
)


@dataclass(frozen=True, slots=True)
class DynamicMarketShadowHypothesis:
    loop: int
    config: DynamicMarketRouterConfig

    @property
    def router_version(self) -> str:
        return f"jra-dynamic-market-shadow-loop{self.loop}-2026"


def shadow_hypotheses() -> tuple[DynamicMarketShadowHypothesis, ...]:
    """Return the fixed 50-hypothesis forward sweep used by the research contract."""
    hypotheses: list[DynamicMarketShadowHypothesis] = []
    for minimum_market_weight in (0.60, 0.70, 0.80, 0.90, 0.95):
        for activation_threshold in (0.35, 0.40, 0.45, 0.50, 0.55):
            for gamma in (0.5, 2.0):
                hypotheses.append(
                    DynamicMarketShadowHypothesis(
                        loop=len(hypotheses) + 1,
                        config=DynamicMarketRouterConfig(
                            enabled=True,
                            minimum_market_weight=minimum_market_weight,
                            activation_threshold=activation_threshold,
                            gamma=gamma,
                            high_upset_context_threshold=0.55,
                        ),
                    )
                )
    if len(hypotheses) != 50:
        raise AssertionError(f"expected 50 shadow hypotheses, got {len(hypotheses)}")
    return tuple(hypotheses)


SHADOW_HYPOTHESES: Final[tuple[DynamicMarketShadowHypothesis, ...]] = shadow_hypotheses()
ACTIVE_SHADOW_HYPOTHESIS: Final[DynamicMarketShadowHypothesis] = SHADOW_HYPOTHESES[42]
if (
    ACTIVE_SHADOW_HYPOTHESIS.router_version != SHADOW_ROUTER_VERSION
    or ACTIVE_SHADOW_HYPOTHESIS.config != ACTIVE_SHADOW_CONFIG
):
    raise AssertionError("loop 43 runtime configuration differs from the forward sweep")


def surface_expert_version(surface: str, *, market_free: bool) -> str:
    """Return the immutable artifact version for one shadow surface."""
    return _versions.surface_expert_version(surface, market_free=market_free)


def classifier_version() -> str:
    """Return the immutable upset-classifier artifact version."""
    return _versions.classifier_version()


def selected_artifact_versions() -> tuple[str, ...]:
    """Return every model version required by the enabled shadow runtime."""
    return _versions.selected_artifact_versions()


def additional_top5_candidates(
    baseline_top5: Sequence[str], shadow_top5: Sequence[str]
) -> tuple[str, ...]:
    """Return shadow candidates outside the served Top5 without changing served ranks."""
    baseline = frozenset(baseline_top5)
    return tuple(horse_id for horse_id in shadow_top5 if horse_id not in baseline)


class ProbabilityModel(Protocol):
    """Minimal classifier interface needed by the pure shadow scorer."""

    def predict_proba(self, matrix: Sequence[Sequence[float]]) -> Sequence[Sequence[float]]: ...


@dataclass(frozen=True, slots=True)
class ShadowExpert:
    booster: object
    feature_names: tuple[str, ...]
    model_version: str


@dataclass(frozen=True, slots=True)
class DynamicMarketShadowRecord:
    race_id: str
    router_version: str
    upset_probability: float
    market_weight: float
    active: bool
    reason: str
    surface: str | None
    distance_band: str | None
    baseline_top5: tuple[str, ...]
    shadow_top5: tuple[str, ...]
    additional_top5_candidates: tuple[str, ...]
    classifier_version: str
    market_expert_version: str
    market_free_expert_version: str
    comparison_contract: str
    served_model_version: str
    feature_snapshot_sha256: str


@dataclass(frozen=True, slots=True)
class ServedBaseline:
    model_version: str
    top5: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DynamicMarketShadowOutcome:
    """One finalized race joined to its baseline and shadow Top1 horses."""

    race_date: str
    baseline_top1_finish: int
    shadow_top1_finish: int
    winner_market_rank: int

    def __post_init__(self) -> None:
        if len(self.race_date) != 8 or not self.race_date.isascii() or not self.race_date.isdigit():
            raise ValueError("race_date must be YYYYMMDD")
        if (
            min(
                self.baseline_top1_finish,
                self.shadow_top1_finish,
                self.winner_market_rank,
            )
            < 1
        ):
            raise ValueError("finish positions and market rank must be positive")

    @property
    def segment(self) -> str:
        return "upset" if self.winner_market_rank >= 4 else "favorite_driven"


@dataclass(frozen=True, slots=True)
class CumulativeTop5Metrics:
    top1: float | None
    top2: float | None
    top3: float | None
    top4: float | None
    top5: float | None


@dataclass(frozen=True, slots=True)
class ShadowSegmentEvaluation:
    races: int
    baseline: CumulativeTop5Metrics
    shadow: CumulativeTop5Metrics
    delta: CumulativeTop5Metrics


@dataclass(frozen=True, slots=True)
class DynamicMarketShadowEvaluation:
    all: ShadowSegmentEvaluation
    favorite_driven: ShadowSegmentEvaluation
    upset: ShadowSegmentEvaluation


def _cumulative_top5(finishes: Sequence[int]) -> CumulativeTop5Metrics:
    if not finishes:
        return CumulativeTop5Metrics(None, None, None, None, None)
    count = len(finishes)
    values = tuple(sum(finish <= cutoff for finish in finishes) / count for cutoff in range(1, 6))
    return CumulativeTop5Metrics(*values)


def _metric_delta(
    shadow: CumulativeTop5Metrics, baseline: CumulativeTop5Metrics
) -> CumulativeTop5Metrics:
    values: list[float | None] = []
    for shadow_value, baseline_value in zip(
        (shadow.top1, shadow.top2, shadow.top3, shadow.top4, shadow.top5),
        (baseline.top1, baseline.top2, baseline.top3, baseline.top4, baseline.top5),
        strict=True,
    ):
        values.append(
            None
            if shadow_value is None or baseline_value is None
            else shadow_value - baseline_value
        )
    return CumulativeTop5Metrics(*values)


def _evaluate_segment(
    outcomes: Sequence[DynamicMarketShadowOutcome], segment: str | None
) -> ShadowSegmentEvaluation:
    selected = [outcome for outcome in outcomes if segment is None or outcome.segment == segment]
    baseline = _cumulative_top5([outcome.baseline_top1_finish for outcome in selected])
    shadow = _cumulative_top5([outcome.shadow_top1_finish for outcome in selected])
    return ShadowSegmentEvaluation(
        races=len(selected),
        baseline=baseline,
        shadow=shadow,
        delta=_metric_delta(shadow, baseline),
    )


def evaluate_shadow_outcomes(
    outcomes: Sequence[DynamicMarketShadowOutcome],
) -> DynamicMarketShadowEvaluation:
    """Always report cumulative Top1..Top5 overall and for both outcome segments."""
    return DynamicMarketShadowEvaluation(
        all=_evaluate_segment(outcomes, None),
        favorite_driven=_evaluate_segment(outcomes, "favorite_driven"),
        upset=_evaluate_segment(outcomes, "upset"),
    )


def _finite_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_odds(entry: Mapping[str, object]) -> float | None:
    serving_odds = entry.get("tansho_odds")
    odds = _finite_float(serving_odds if serving_odds is not None else entry.get("tansho_odds_raw"))
    return odds if odds is not None and odds > 0.0 else None


def _sample_zscores(scores: Sequence[float]) -> tuple[float, ...]:
    count = len(scores)
    if count < 2:
        return tuple(0.0 for _ in scores)
    mean = sum(scores) / count
    variance = sum((score - mean) ** 2 for score in scores) / (count - 1)
    stddev = math.sqrt(variance)
    if stddev == 0.0:
        return tuple(0.0 for _ in scores)
    return tuple((score - mean) / stddev for score in scores)


def _ordinal_ranks(values: Sequence[float], *, descending: bool) -> tuple[int, ...]:
    order = sorted(
        range(len(values)),
        key=lambda index: ((-values[index] if descending else values[index]), index),
    )
    ranks = [0] * len(values)
    for rank, index in enumerate(order, start=1):
        ranks[index] = rank
    return tuple(ranks)


def build_upset_feature_vector(
    entries: Sequence[Mapping[str, object]],
    champion_scores: Sequence[float],
    market_free_scores: Sequence[float],
) -> tuple[float, ...] | None:
    """Reproduce the 14-feature forward classifier contract for one race."""
    count = len(entries)
    if count < 3 or len(champion_scores) != count or len(market_free_scores) != count:
        return None
    odds_values = [_positive_odds(entry) for entry in entries]
    if any(value is None for value in odds_values):
        return None
    odds = tuple(float(value) for value in odds_values if value is not None)
    market_ranks = _ordinal_ranks(odds, descending=False)
    champion_ranks = _ordinal_ranks(champion_scores, descending=True)
    market_free_ranks = _ordinal_ranks(market_free_scores, descending=True)
    champion_z = _sample_zscores(champion_scores)
    market_free_z = _sample_zscores(market_free_scores)
    inverse = tuple(1.0 / value for value in odds)
    inverse_total = sum(inverse)
    market_probabilities = tuple(value / inverse_total for value in inverse)
    entropy = -sum(probability * math.log(probability) for probability in market_probabilities)
    ordered_odds = sorted(odds)
    favorite_index = market_ranks.index(1)
    market_free_top_index = market_free_ranks.index(1)
    champion_top_index = champion_ranks.index(1)
    market_free_sorted = sorted(market_free_z, reverse=True)
    champion_sorted = sorted(champion_z, reverse=True)
    return (
        float(count),
        ordered_odds[0],
        ordered_odds[1],
        ordered_odds[2],
        entropy,
        entropy / math.log(count),
        1.0 / sum(probability**2 for probability in market_probabilities),
        ordered_odds[1] / ordered_odds[0],
        float(market_free_ranks[favorite_index]),
        float(market_ranks[market_free_top_index]),
        float(market_ranks[champion_top_index]),
        float(market_free_ranks[champion_top_index] != 1),
        market_free_sorted[0] - market_free_sorted[1],
        champion_sorted[0] - champion_sorted[1],
    )


def predict_upset_probability(model: ProbabilityModel, feature_vector: Sequence[float]) -> float:
    probabilities = model.predict_proba([feature_vector])
    if len(probabilities) != 1 or len(probabilities[0]) != 2:
        raise ValueError("upset classifier must return one binary probability row")
    probability = float(probabilities[0][1])
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        raise ValueError("upset classifier returned an invalid probability")
    return probability


def _top5_ids(entries: Sequence[Mapping[str, object]], scores: Sequence[float]) -> tuple[str, ...]:
    def horse_number(entry: Mapping[str, object]) -> int:
        value = entry["umaban"]
        if not isinstance(value, (int, str)) or isinstance(value, bool):
            raise ValueError("umaban must be an integer")
        return int(value)

    horses = [
        ScoredHorse(
            ketto_toroku_bango=str(entry["ketto_toroku_bango"]),
            umaban=horse_number(entry),
            predicted_score=float(score),
        )
        for entry, score in zip(entries, scores, strict=True)
    ]
    return tuple(horse.ketto_toroku_bango for horse in rank_within_race(horses)[:5])


def feature_snapshot_sha256(entries: Sequence[Mapping[str, object]]) -> str:
    """Fingerprint the exact ordered feature rows shared by served and shadow scoring."""
    payload = json.dumps(
        entries,
        default=str,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def served_baseline_from_rows(rows: Sequence[Sequence[object]]) -> ServedBaseline | None:
    """Extract the actual persisted model and Top5 from prediction UPSERT rows."""
    if not rows:
        return None
    if any(len(row) < 10 for row in rows):
        raise ValueError("served prediction row is shorter than the rank contract")
    model_versions = {str(row[0]) for row in rows}
    if len(model_versions) != 1:
        raise ValueError("served prediction rows contain multiple model versions")
    ranked = sorted(rows, key=lambda row: int(str(row[9])))
    ranks = [int(str(row[9])) for row in ranked]
    if ranks != list(range(1, len(ranked) + 1)):
        raise ValueError("served prediction ranks must be contiguous from one")
    return ServedBaseline(
        model_version=model_versions.pop(),
        top5=tuple(str(row[6]) for row in ranked[:5]),
    )


def build_joint_additional_candidate_record(
    *,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    candidate: JointAlternateCandidate,
    specialist_model_version: str,
    champion_model_version: str,
    served_baseline: ServedBaseline,
) -> DynamicMarketShadowRecord:
    """Build one auditable record that leaves served Top5 structurally unchanged."""
    representative = entries[0] if entries else {}
    raw_track_code = representative.get("track_code")
    surface = classify_surface(None if raw_track_code is None else str(raw_track_code))
    raw_distance = _finite_float(representative.get("kyori"))
    distance_band = classify_distance_band(None if raw_distance is None else int(raw_distance))
    margin = candidate.margin
    stored_margin = 0.0 if margin is None else margin if math.isfinite(margin) else 1e9
    return DynamicMarketShadowRecord(
        race_id=race_id,
        router_version=JOINT_ALTERNATE_ROUTER_VERSION,
        upset_probability=candidate.defer_probability,
        market_weight=stored_margin,
        active=candidate.emitted,
        reason=candidate.reason,
        surface=surface,
        distance_band=distance_band,
        baseline_top5=served_baseline.top5,
        shadow_top5=served_baseline.top5,
        additional_top5_candidates=(candidate.horse_id,) if candidate.horse_id else (),
        classifier_version=JOINT_ALTERNATE_MODEL_VERSION,
        market_expert_version=specialist_model_version,
        market_free_expert_version=champion_model_version,
        comparison_contract=COMPARISON_CONTRACT,
        served_model_version=served_baseline.model_version,
        feature_snapshot_sha256=feature_snapshot_sha256(entries),
    )


def build_shadow_record(
    *,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    champion_scores: Sequence[float],
    market_free_scores: Sequence[float],
    surface_market_scores: Sequence[float],
    surface_market_free_scores: Sequence[float],
    classifier: ProbabilityModel,
    classifier_model_version: str,
    market_expert_version: str,
    market_free_expert_version: str,
    served_baseline: ServedBaseline,
) -> DynamicMarketShadowRecord | None:
    """Build the selected loop-43 record; incomplete market boards skip it."""
    records = _build_shadow_records(
        race_id=race_id,
        entries=entries,
        champion_scores=champion_scores,
        market_free_scores=market_free_scores,
        surface_market_scores=surface_market_scores,
        surface_market_free_scores=surface_market_free_scores,
        classifier=classifier,
        classifier_model_version=classifier_model_version,
        market_expert_version=market_expert_version,
        market_free_expert_version=market_free_expert_version,
        served_baseline=served_baseline,
        hypotheses=(ACTIVE_SHADOW_HYPOTHESIS,),
    )
    return records[0] if records else None


def build_shadow_records(
    *,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    champion_scores: Sequence[float],
    market_free_scores: Sequence[float],
    surface_market_scores: Sequence[float],
    surface_market_free_scores: Sequence[float],
    classifier: ProbabilityModel,
    classifier_model_version: str,
    market_expert_version: str,
    market_free_expert_version: str,
    served_baseline: ServedBaseline,
) -> tuple[DynamicMarketShadowRecord, ...]:
    """Build all 50 forward hypotheses from one classifier and expert inference."""
    return _build_shadow_records(
        race_id=race_id,
        entries=entries,
        champion_scores=champion_scores,
        market_free_scores=market_free_scores,
        surface_market_scores=surface_market_scores,
        surface_market_free_scores=surface_market_free_scores,
        classifier=classifier,
        classifier_model_version=classifier_model_version,
        market_expert_version=market_expert_version,
        market_free_expert_version=market_free_expert_version,
        served_baseline=served_baseline,
        hypotheses=SHADOW_HYPOTHESES,
    )


def _build_shadow_records(
    *,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    champion_scores: Sequence[float],
    market_free_scores: Sequence[float],
    surface_market_scores: Sequence[float],
    surface_market_free_scores: Sequence[float],
    classifier: ProbabilityModel,
    classifier_model_version: str,
    market_expert_version: str,
    market_free_expert_version: str,
    served_baseline: ServedBaseline,
    hypotheses: Sequence[DynamicMarketShadowHypothesis],
) -> tuple[DynamicMarketShadowRecord, ...]:
    """Build records while sharing the expensive upset-classifier inference."""
    features = build_upset_feature_vector(entries, champion_scores, market_free_scores)
    if features is None or not entries:
        return ()
    representative = entries[0]
    surface = classify_surface(
        None if representative.get("track_code") is None else str(representative["track_code"])
    )
    raw_distance = _finite_float(representative.get("kyori"))
    distance_band = classify_distance_band(None if raw_distance is None else int(raw_distance))
    upset_probability = predict_upset_probability(classifier, features)
    snapshot_sha256 = feature_snapshot_sha256(entries)
    records: list[DynamicMarketShadowRecord] = []
    for hypothesis in hypotheses:
        decision = route_dynamic_market_scores(
            champion_scores=champion_scores,
            surface_market_scores=surface_market_scores,
            surface_market_free_scores=surface_market_free_scores,
            upset_probability=upset_probability,
            surface=surface,
            distance_band=distance_band,
            config=hypothesis.config,
        )
        shadow_top5 = _top5_ids(entries, decision.routed_scores)
        records.append(
            DynamicMarketShadowRecord(
                race_id=race_id,
                router_version=hypothesis.router_version,
                upset_probability=upset_probability,
                market_weight=decision.market_weight,
                active=decision.active,
                reason=decision.reason,
                surface=surface,
                distance_band=distance_band,
                baseline_top5=served_baseline.top5,
                shadow_top5=shadow_top5,
                additional_top5_candidates=additional_top5_candidates(
                    served_baseline.top5, shadow_top5
                ),
                classifier_version=classifier_model_version,
                market_expert_version=market_expert_version,
                market_free_expert_version=market_free_expert_version,
                comparison_contract=COMPARISON_CONTRACT,
                served_model_version=served_baseline.model_version,
                feature_snapshot_sha256=snapshot_sha256,
            )
        )
    return tuple(records)


SHADOW_COLUMNS: Final[tuple[str, ...]] = (
    "race_id",
    "router_version",
    "upset_probability",
    "market_weight",
    "active",
    "reason",
    "surface",
    "distance_band",
    "baseline_top5",
    "shadow_top5",
    "additional_top5_candidates",
    "classifier_version",
    "market_expert_version",
    "market_free_expert_version",
    "comparison_contract",
    "served_model_version",
    "feature_snapshot_sha256",
)


def build_shadow_table_ddl() -> str:
    return f"""
create table if not exists {SHADOW_TABLE} (
  race_id text not null,
  router_version text not null,
  upset_probability double precision not null check (upset_probability between 0 and 1),
  market_weight double precision not null check (market_weight between 0 and 1),
  active boolean not null,
  reason text not null,
  surface text,
  distance_band text,
  baseline_top5 text[] not null,
  shadow_top5 text[] not null,
  additional_top5_candidates text[] not null default '{{}}',
  classifier_version text not null,
  market_expert_version text not null,
  market_free_expert_version text not null,
  comparison_contract text not null,
  served_model_version text not null,
  feature_snapshot_sha256 text not null check (length(feature_snapshot_sha256) = 64),
  generated_at timestamptz not null default now(),
  primary key (race_id, router_version)
)
""".strip()


def build_shadow_migration_sql() -> tuple[str, ...]:
    """Add comparison-audit columns without fabricating values for legacy rows."""
    return (
        f"alter table {SHADOW_TABLE} add column if not exists comparison_contract text",
        f"alter table {SHADOW_TABLE} add column if not exists served_model_version text",
        f"alter table {SHADOW_TABLE} add column if not exists feature_snapshot_sha256 text",
        f"alter table {SHADOW_TABLE} add column if not exists "
        "additional_top5_candidates text[] not null default '{}'",
    )


def build_shadow_upsert_sql() -> str:
    return build_shadow_batch_upsert_sql(1)


def build_shadow_batch_upsert_sql(record_count: int) -> str:
    if record_count < 1:
        raise ValueError("record_count must be positive")
    columns = ", ".join(SHADOW_COLUMNS)
    row_placeholders = "(" + ", ".join("%s" for _ in SHADOW_COLUMNS) + ")"
    placeholders = ", ".join(row_placeholders for _ in range(record_count))
    updates = ", ".join(
        f"{column} = excluded.{column}" for column in SHADOW_COLUMNS if column != "race_id"
    )
    return (
        f"insert into {SHADOW_TABLE} ({columns}) values {placeholders} "
        f"on conflict (race_id, router_version) do update set {updates}, generated_at = now()"
    )


def shadow_params(record: DynamicMarketShadowRecord) -> list[object]:
    return [
        record.race_id,
        record.router_version,
        record.upset_probability,
        record.market_weight,
        record.active,
        record.reason,
        record.surface,
        record.distance_band,
        list(record.baseline_top5),
        list(record.shadow_top5),
        list(record.additional_top5_candidates),
        record.classifier_version,
        record.market_expert_version,
        record.market_free_expert_version,
        record.comparison_contract,
        record.served_model_version,
        record.feature_snapshot_sha256,
    ]


def shadow_batch_params(records: Sequence[DynamicMarketShadowRecord]) -> list[object]:
    return [value for record in records for value in shadow_params(record)]
