"""Tests for dynamic-market shadow feature parity, records, and SQL."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import final

import pytest

from predict_lib.dynamic_market_shadow import (
    COMPARISON_CONTRACT,
    SHADOW_HYPOTHESES,
    SHADOW_ROUTER_VERSION,
    SHADOW_TABLE,
    UPSET_FEATURE_NAMES,
    DynamicMarketShadowOutcome,
    ServedBaseline,
    additional_top5_candidates,
    build_joint_additional_candidate_record,
    build_shadow_batch_upsert_sql,
    build_shadow_migration_sql,
    build_shadow_record,
    build_shadow_records,
    build_shadow_table_ddl,
    build_shadow_upsert_sql,
    build_upset_feature_vector,
    classifier_version,
    evaluate_shadow_outcomes,
    feature_snapshot_sha256,
    predict_upset_probability,
    selected_artifact_versions,
    served_baseline_from_rows,
    shadow_batch_params,
    shadow_params,
    surface_expert_version,
)
from predict_lib.jra_joint_alternate_scorer import JointAlternateCandidate

_SERVED_BASELINE = ServedBaseline("served-model", ("horse-c", "horse-a", "horse-b"))


def test_additional_top5_candidates_preserves_shadow_order() -> None:
    assert additional_top5_candidates(
        ("horse-a", "horse-b", "horse-c", "horse-d", "horse-e"),
        ("horse-b", "horse-f", "horse-a", "horse-g", "horse-c"),
    ) == ("horse-f", "horse-g")


@final
class _Classifier:
    probability: float

    def __init__(self, probability: float = 0.8) -> None:
        self.probability = probability
        self.seen: list[list[float]] = []
        self.calls = 0

    def predict_proba(self, matrix: Sequence[Sequence[float]]) -> Sequence[Sequence[float]]:
        self.calls += 1
        self.seen = [list(row) for row in matrix]
        return [[1.0 - self.probability, self.probability]]


def _entries() -> list[dict[str, object]]:
    return [
        {
            "ketto_toroku_bango": "horse-a",
            "umaban": 1,
            "tansho_odds_raw": 2.0,
            "track_code": "10",
            "kyori": 1600,
        },
        {
            "ketto_toroku_bango": "horse-b",
            "umaban": 2,
            "tansho_odds_raw": 4.0,
            "track_code": "10",
            "kyori": 1600,
        },
        {
            "ketto_toroku_bango": "horse-c",
            "umaban": 3,
            "tansho_odds_raw": 8.0,
            "track_code": "10",
            "kyori": 1600,
        },
    ]


def test_artifact_versions_are_fixed_complete_and_reject_unknown_surface() -> None:
    assert classifier_version() == f"{SHADOW_ROUTER_VERSION}-upset-classifier"
    assert selected_artifact_versions() == (
        f"{SHADOW_ROUTER_VERSION}-turf-champion",
        f"{SHADOW_ROUTER_VERSION}-turf-stage1",
        f"{SHADOW_ROUTER_VERSION}-dirt-champion",
        f"{SHADOW_ROUTER_VERSION}-dirt-stage1",
        f"{SHADOW_ROUTER_VERSION}-obstacle-champion",
        f"{SHADOW_ROUTER_VERSION}-obstacle-stage1",
        f"{SHADOW_ROUTER_VERSION}-upset-classifier",
        "jra-joint-group-dro-alternate-top5-v1",
        "jra-cb-high-payout-specialist235-2026-v1",
    )
    assert surface_expert_version("turf", market_free=False).endswith("turf-champion")
    with pytest.raises(ValueError, match="unsupported shadow surface"):
        surface_expert_version("synthetic", market_free=False)


def test_joint_additional_candidate_record_preserves_served_top5() -> None:
    record = build_joint_additional_candidate_record(
        race_id="jra:2026:0825:01:01",
        entries=_entries(),
        candidate=JointAlternateCandidate("horse-a", 1.2, 0.3, 0.7, "joint-margin-pass"),
        specialist_model_version="specialist",
        champion_model_version="champion",
        served_baseline=_SERVED_BASELINE,
    )

    assert record.router_version == "jra-joint-group-dro-alternate-top5-v1"
    assert record.baseline_top5 == record.shadow_top5 == _SERVED_BASELINE.top5
    assert record.additional_top5_candidates == ("horse-a",)
    assert record.market_weight == pytest.approx(0.3)
    assert record.upset_probability == pytest.approx(0.7)
    assert record.active is True


def test_forward_sweep_has_exactly_50_fixed_hypotheses_and_loop43_contract() -> None:
    assert len(SHADOW_HYPOTHESES) == 50
    assert [hypothesis.loop for hypothesis in SHADOW_HYPOTHESES] == list(range(1, 51))
    assert len({hypothesis.router_version for hypothesis in SHADOW_HYPOTHESES}) == 50
    selected = SHADOW_HYPOTHESES[42]
    assert selected.router_version == SHADOW_ROUTER_VERSION
    assert selected.config.minimum_market_weight == 0.95
    assert selected.config.activation_threshold == 0.40
    assert selected.config.gamma == 0.5


def test_evaluation_always_reports_top1_through_top5_and_both_segments() -> None:
    evaluation = evaluate_shadow_outcomes(
        [
            DynamicMarketShadowOutcome("20260823", 1, 2, 1),
            DynamicMarketShadowOutcome("20260823", 4, 3, 5),
            DynamicMarketShadowOutcome("20260824", 6, 5, 4),
        ]
    )

    assert evaluation.all.races == 3
    assert evaluation.all.baseline.top1 == pytest.approx(1 / 3)
    assert evaluation.all.baseline.top5 == pytest.approx(2 / 3)
    assert evaluation.all.shadow.top1 == 0.0
    assert evaluation.all.shadow.top2 == pytest.approx(1 / 3)
    assert evaluation.all.shadow.top3 == pytest.approx(2 / 3)
    assert evaluation.all.shadow.top4 == pytest.approx(2 / 3)
    assert evaluation.all.shadow.top5 == 1.0
    assert evaluation.all.delta.top3 == pytest.approx(1 / 3)
    assert evaluation.favorite_driven.races == 1
    assert evaluation.upset.races == 2
    assert evaluation.upset.delta.top5 == pytest.approx(0.5)


def test_evaluation_empty_segment_is_explicit_none_not_fabricated_zero() -> None:
    evaluation = evaluate_shadow_outcomes([DynamicMarketShadowOutcome("20260823", 1, 1, 1)])
    assert evaluation.upset.races == 0
    assert evaluation.upset.baseline.top1 is None
    assert evaluation.upset.shadow.top5 is None
    assert evaluation.upset.delta.top3 is None


def test_outcome_rejects_invalid_date_and_nonpositive_positions() -> None:
    with pytest.raises(ValueError, match="race_date"):
        DynamicMarketShadowOutcome("bad", 1, 1, 1)
    with pytest.raises(ValueError, match="positive"):
        DynamicMarketShadowOutcome("20260823", 0, 1, 1)


def test_feature_vector_matches_hand_computed_market_and_rank_features() -> None:
    vector = build_upset_feature_vector(
        _entries(), champion_scores=[3.0, 2.0, 1.0], market_free_scores=[1.0, 3.0, 2.0]
    )
    assert vector is not None
    assert len(vector) == len(UPSET_FEATURE_NAMES) == 14
    probabilities = [4 / 7, 2 / 7, 1 / 7]
    entropy = -sum(probability * math.log(probability) for probability in probabilities)
    assert vector[:4] == (3.0, 2.0, 4.0, 8.0)
    assert vector[4] == pytest.approx(entropy)
    assert vector[5] == pytest.approx(entropy / math.log(3))
    assert vector[6] == pytest.approx(1 / sum(value**2 for value in probabilities))
    assert vector[7:12] == (2.0, 3.0, 2.0, 1.0, 1.0)
    assert vector[12:] == pytest.approx((1.0, 1.0))


@pytest.mark.parametrize(
    "entries,champion,market_free",
    [
        (_entries()[:2], [2.0, 1.0], [1.0, 2.0]),
        (_entries(), [2.0], [1.0, 2.0, 3.0]),
        (_entries(), [3.0, 2.0, 1.0], [1.0]),
    ],
)
def test_feature_vector_skips_short_or_mismatched_races(
    entries: list[dict[str, object]], champion: list[float], market_free: list[float]
) -> None:
    assert build_upset_feature_vector(entries, champion, market_free) is None


@pytest.mark.parametrize("value", [None, True, 0.0, -1.0, math.inf, "bad", object()])
def test_feature_vector_skips_incomplete_or_invalid_odds(value: object) -> None:
    entries = _entries()
    entries[1]["tansho_odds_raw"] = value
    assert build_upset_feature_vector(entries, [3.0, 2.0, 1.0], [1.0, 3.0, 2.0]) is None


def test_feature_vector_falls_back_to_raw_odds_and_handles_flat_scores() -> None:
    entries = _entries()
    for entry in entries:
        entry["tansho_odds"] = entry.pop("tansho_odds_raw")
    vector = build_upset_feature_vector(entries, [1.0] * 3, [1.0] * 3)
    assert vector is not None
    assert vector[-2:] == (0.0, 0.0)


def test_feature_vector_prefers_latest_serving_odds_over_stale_raw_odds() -> None:
    entries = _entries()
    for index, entry in enumerate(entries, start=1):
        entry["tansho_odds"] = float(index)
        entry["tansho_odds_raw"] = float(index + 10)
    vector = build_upset_feature_vector(entries, [3.0, 2.0, 1.0], [1.0, 3.0, 2.0])
    assert vector is not None
    assert vector[1:4] == (1.0, 2.0, 3.0)


def test_probability_validation_accepts_binary_row_and_rejects_bad_shapes() -> None:
    assert predict_upset_probability(_Classifier(0.7), [1.0]) == pytest.approx(0.7)

    class BadShape:
        def predict_proba(self, matrix: Sequence[Sequence[float]]) -> Sequence[Sequence[float]]:
            del matrix
            return [[1.0]]

    with pytest.raises(ValueError, match="binary probability"):
        predict_upset_probability(BadShape(), [1.0])
    with pytest.raises(ValueError, match="invalid probability"):
        predict_upset_probability(_Classifier(math.nan), [1.0])


def test_shadow_record_routes_high_upset_turf_and_keeps_top5_auditable() -> None:
    classifier = _Classifier(1.0)
    record = build_shadow_record(
        race_id="jra:2026:0824:01:01",
        entries=_entries(),
        champion_scores=[3.0, 2.0, 1.0],
        market_free_scores=[1.0, 3.0, 2.0],
        surface_market_scores=[1.0, 3.0, 2.0],
        surface_market_free_scores=[3.0, 1.0, 2.0],
        classifier=classifier,
        classifier_model_version=classifier_version(),
        market_expert_version=surface_expert_version("turf", market_free=False),
        market_free_expert_version=surface_expert_version("turf", market_free=True),
        served_baseline=_SERVED_BASELINE,
    )
    assert record is not None
    assert len(classifier.seen[0]) == 14
    assert record.active is True
    assert record.reason == "surface-dynamic-blend"
    assert record.market_weight == pytest.approx(0.95)
    assert record.surface == "turf"
    assert record.distance_band == "mile"
    assert record.baseline_top5 == ("horse-c", "horse-a", "horse-b")
    assert record.shadow_top5 == ("horse-b", "horse-c", "horse-a")
    assert record.additional_top5_candidates == ()


def test_shadow_sweep_reuses_one_classifier_inference_for_all_50_records() -> None:
    classifier = _Classifier(1.0)
    records = build_shadow_records(
        race_id="jra:2026:0824:01:01",
        entries=_entries(),
        champion_scores=[3.0, 2.0, 1.0],
        market_free_scores=[1.0, 3.0, 2.0],
        surface_market_scores=[1.0, 3.0, 2.0],
        surface_market_free_scores=[3.0, 1.0, 2.0],
        classifier=classifier,
        classifier_model_version=classifier_version(),
        market_expert_version=surface_expert_version("turf", market_free=False),
        market_free_expert_version=surface_expert_version("turf", market_free=True),
        served_baseline=_SERVED_BASELINE,
    )

    assert len(records) == 50
    assert classifier.calls == 1
    assert {record.router_version for record in records} == {
        hypothesis.router_version for hypothesis in SHADOW_HYPOTHESES
    }
    assert all(record.baseline_top5 == _SERVED_BASELINE.top5 for record in records)
    assert len({record.feature_snapshot_sha256 for record in records}) == 1


def test_shadow_record_returns_none_when_feature_contract_is_unavailable() -> None:
    assert (
        build_shadow_record(
            race_id="race",
            entries=[],
            champion_scores=[],
            market_free_scores=[],
            surface_market_scores=[],
            surface_market_free_scores=[],
            classifier=_Classifier(),
            classifier_model_version="classifier",
            market_expert_version="market",
            market_free_expert_version="free",
            served_baseline=_SERVED_BASELINE,
        )
        is None
    )


def test_shadow_record_rejects_non_integer_horse_number() -> None:
    entries = _entries()
    entries[0]["umaban"] = True
    with pytest.raises(ValueError, match="umaban must be an integer"):
        build_shadow_record(
            race_id="race",
            entries=entries,
            champion_scores=[3.0, 2.0, 1.0],
            market_free_scores=[1.0, 3.0, 2.0],
            surface_market_scores=[1.0, 3.0, 2.0],
            surface_market_free_scores=[3.0, 1.0, 2.0],
            classifier=_Classifier(),
            classifier_model_version="classifier",
            market_expert_version="market",
            market_free_expert_version="free",
            served_baseline=_SERVED_BASELINE,
        )


def test_shadow_record_handles_unknown_surface_and_distance_outside_context() -> None:
    entries = _entries()
    entries[0]["track_code"] = None
    entries[0]["kyori"] = "bad"
    record = build_shadow_record(
        race_id="race",
        entries=entries,
        champion_scores=[3.0, 2.0, 1.0],
        market_free_scores=[1.0, 3.0, 2.0],
        surface_market_scores=[1.0, 3.0, 2.0],
        surface_market_free_scores=[3.0, 1.0, 2.0],
        classifier=_Classifier(),
        classifier_model_version="classifier",
        market_expert_version="market",
        market_free_expert_version="free",
        served_baseline=_SERVED_BASELINE,
    )
    assert record is not None
    assert record.active is False
    assert record.reason == "outside-context"
    assert record.surface is None
    assert record.distance_band is None


def test_shadow_sql_and_params_cover_fixed_top5_contract() -> None:
    record = build_shadow_record(
        race_id="race",
        entries=_entries(),
        champion_scores=[3.0, 2.0, 1.0],
        market_free_scores=[1.0, 3.0, 2.0],
        surface_market_scores=[1.0, 3.0, 2.0],
        surface_market_free_scores=[3.0, 1.0, 2.0],
        classifier=_Classifier(),
        classifier_model_version="classifier",
        market_expert_version="market",
        market_free_expert_version="free",
        served_baseline=_SERVED_BASELINE,
    )
    assert record is not None
    ddl = build_shadow_table_ddl()
    upsert = build_shadow_upsert_sql()
    params = shadow_params(record)
    assert f"create table if not exists {SHADOW_TABLE}" in ddl
    assert "primary key (race_id, router_version)" in ddl
    assert f"insert into {SHADOW_TABLE}" in upsert
    assert "on conflict (race_id, router_version) do update" in upsert
    assert upsert.count("%s") == len(params) == 17
    assert params[8] == ["horse-c", "horse-a", "horse-b"]
    assert params[9] == ["horse-b", "horse-c", "horse-a"]
    assert params[10] == []
    assert params[14] == COMPARISON_CONTRACT
    assert params[15] == "served-model"
    assert params[16] == feature_snapshot_sha256(_entries())
    assert "comparison_contract text not null" in ddl
    assert "additional_top5_candidates text[] not null default '{}'" in ddl
    assert len(build_shadow_migration_sql()) == 4
    batch_sql = build_shadow_batch_upsert_sql(2)
    assert batch_sql.count("%s") == 34
    assert len(shadow_batch_params((record, record))) == 34
    with pytest.raises(ValueError, match="record_count"):
        build_shadow_batch_upsert_sql(0)


def _served_row(model: str, horse: str, rank: int) -> tuple[object, ...]:
    return (model, "jra", "2026", "0824", "01", "01", horse, 0.0, 0.0, rank)


def test_served_baseline_uses_persisted_rank_and_model_contract() -> None:
    baseline = served_baseline_from_rows(
        [_served_row("cell-model", "horse-a", 2), _served_row("cell-model", "horse-c", 1)]
    )

    assert baseline == ServedBaseline("cell-model", ("horse-c", "horse-a"))


def test_served_baseline_rejects_malformed_persisted_rows() -> None:
    assert served_baseline_from_rows([]) is None
    with pytest.raises(ValueError, match="shorter"):
        served_baseline_from_rows([("model",)])
    with pytest.raises(ValueError, match="multiple model"):
        served_baseline_from_rows([_served_row("a", "horse-a", 1), _served_row("b", "horse-b", 2)])
    with pytest.raises(ValueError, match="contiguous"):
        served_baseline_from_rows([_served_row("a", "horse-a", 2)])


def test_feature_snapshot_hash_is_deterministic_and_content_sensitive() -> None:
    first = feature_snapshot_sha256(_entries())
    copied = [dict(entry) for entry in _entries()]
    assert feature_snapshot_sha256(copied) == first
    copied[0]["kyori"] = 1800
    assert feature_snapshot_sha256(copied) != first
