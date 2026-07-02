"""Tests for the build_cell_models cell-routing CLI."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

import learning.build_cell_models as subject
from learning.build_cell_models import (
    AdoptionResult,
    CellKey,
    CellMetrics,
    all_gated_metrics_improved,
    bootstrap_lb95,
    check_multi_metric_gate,
    check_no_regression,
    compute_deltas,
    evaluate_category,
    evaluate_cell,
    generate_routing_json,
    generate_running_style_feature_selection_json,
    group_variants,
    load_cell_metrics,
    main,
    parse_row,
    select_best_adoptions_by_cell,
    synthesize_hit_vector,
    variant_name_for_hash,
)

_NOW = datetime(2026, 6, 28, 12, 0, 0, tzinfo=timezone.utc)
_FRESH = datetime(2026, 6, 27, 12, 0, 0, tzinfo=timezone.utc)
_STALE = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)


def _cell(
    category: str = "jra",
    class_label: str = "A",
    subgroup: str = "mile",
    racetrack: str = "05",
    season: str = "summer",
    surface: str = "turf",
    cell_subgroup: str = "",
) -> CellKey:
    return CellKey(
        category=category,
        class_label=class_label,
        subgroup=subgroup,
        racetrack=racetrack,
        season=season,
        surface=surface,
        cell_subgroup=cell_subgroup,
    )


def _metrics(
    feature_set_hash: str,
    *,
    race_count: int = 1000,
    top1: float = 0.40,
    place2: float = 0.30,
    place3: float = 0.25,
    place4: float = 0.20,
    place5: float = 0.18,
    place6: float = 0.15,
    top3_box: float = 0.50,
    evaluated_at: datetime = _FRESH,
    feature_names: list[str] | None = None,
    model_version: str | None = None,
    architecture: str | None = None,
    method: str | None = None,
) -> CellMetrics:
    return CellMetrics(
        race_count=race_count,
        top1=top1,
        place2=place2,
        place3=place3,
        place4=place4,
        place5=place5,
        place6=place6,
        top3_box=top3_box,
        evaluated_at=evaluated_at,
        feature_set_hash=feature_set_hash,
        feature_names=feature_names if feature_names is not None else ["f1", "f2"],
        model_version=model_version,
        architecture=architecture,
        method=method,
    )


# ---------------------------------------------------------------------------
# _db_category


def test_db_category_translates_ban_ei_to_banei() -> None:
    assert subject._db_category("ban-ei") == "banei"


def test_db_category_is_identity_for_jra() -> None:
    assert subject._db_category("jra") == "jra"


def test_db_category_is_identity_for_nar() -> None:
    assert subject._db_category("nar") == "nar"


# ---------------------------------------------------------------------------
# compute_deltas


def test_compute_deltas_returns_candidate_minus_baseline() -> None:
    baseline = _metrics("BASE", top1=0.40, place2=0.30, place3=0.25, top3_box=0.50)
    candidate = _metrics("CAND", top1=0.50, place2=0.34, place3=0.28, top3_box=0.55)
    deltas = compute_deltas(baseline, candidate)
    assert deltas["top1"] == pytest.approx(0.10)
    assert deltas["place2"] == pytest.approx(0.04)
    assert deltas["place3"] == pytest.approx(0.03)
    assert deltas["place4"] == pytest.approx(0.0)
    assert deltas["top3_box"] == pytest.approx(0.05)


# ---------------------------------------------------------------------------
# check_multi_metric_gate


def test_multi_metric_gate_passes_when_place_metrics_improve() -> None:
    deltas = {"top1": 0.0, "place2": 0.01, "place3": 0.01}
    passed, reasons = check_multi_metric_gate(deltas)
    assert passed is True
    assert reasons == []


def test_multi_metric_gate_passes_with_top1_and_one_place() -> None:
    deltas = {"top1": 0.02, "place2": 0.02, "place3": 0.0}
    passed, reasons = check_multi_metric_gate(deltas)
    assert passed is True
    assert reasons == []


def test_multi_metric_gate_fails_when_only_top1_improves() -> None:
    deltas = {"top1": 0.05, "place2": 0.0, "place3": 0.0}
    passed, reasons = check_multi_metric_gate(deltas)
    assert passed is False
    assert reasons == [
        "only 1 primary metric(s) improved by >= 0.0008; need >= 2",
        "no place2/place3 among improved primary metrics",
    ]


def test_multi_metric_gate_fails_when_no_metric_improves() -> None:
    deltas = {"top1": 0.0, "place2": 0.0, "place3": 0.0}
    passed, reasons = check_multi_metric_gate(deltas)
    assert passed is False
    assert reasons == [
        "only 0 primary metric(s) improved by >= 0.0008; need >= 2",
        "no place2/place3 among improved primary metrics",
    ]


# ---------------------------------------------------------------------------
# check_no_regression


def test_no_regression_passes_when_all_above_threshold() -> None:
    deltas = {name: 0.0 for name in subject._NO_REGRESSION_METRICS}
    passed, reasons = check_no_regression(deltas)
    assert passed is True
    assert reasons == []


def test_no_regression_fails_when_a_metric_drops_beyond_threshold() -> None:
    deltas = {name: 0.0 for name in subject._NO_REGRESSION_METRICS}
    deltas["place4"] = -0.01
    passed, reasons = check_no_regression(deltas)
    assert passed is False
    assert reasons == ["place4 regressed by -0.01000 (<= -0.0005)"]


def test_all_gated_metrics_improved_requires_min_delta_for_finish_position() -> None:
    deltas = {name: 0.001 for name in subject._NO_REGRESSION_METRICS}
    assert all_gated_metrics_improved(deltas) is True
    deltas["place4"] = 0.0001
    assert all_gated_metrics_improved(deltas) is False


def test_all_gated_metrics_improved_uses_running_style_metric_profile() -> None:
    deltas = {"top1": 0.001, "place2": 0.001, "place3": 0.001}
    assert all_gated_metrics_improved(deltas, prediction_target="running_style") is True
    deltas["place3"] = 0.0001
    assert all_gated_metrics_improved(deltas, prediction_target="running_style") is False


# ---------------------------------------------------------------------------
# synthesize_hit_vector


def test_synthesize_hit_vector_builds_bernoulli_vector() -> None:
    vector = synthesize_hit_vector(0.3, 10)
    assert vector == [1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]


def test_synthesize_hit_vector_clamps_above_race_count() -> None:
    vector = synthesize_hit_vector(1.5, 4)
    assert vector == [1.0, 1.0, 1.0, 1.0]


# ---------------------------------------------------------------------------
# bootstrap_lb95


def test_bootstrap_lb95_positive_for_large_separation() -> None:
    baseline = synthesize_hit_vector(0.40, 1000)
    candidate = synthesize_hit_vector(0.60, 1000)
    lb95 = bootstrap_lb95(baseline, candidate, n_boot=300)
    assert lb95 > 0.0


def test_bootstrap_lb95_returns_zero_on_empty_input() -> None:
    lb95 = bootstrap_lb95([], [1.0, 0.0], n_boot=10)
    assert lb95 == 0.0


# ---------------------------------------------------------------------------
# evaluate_cell


def test_evaluate_cell_adopts_strong_candidate() -> None:
    baseline = _metrics("BASE", top1=0.40, place2=0.30, place3=0.25)
    candidate = _metrics(
        "CAND", top1=0.50, place2=0.40, place3=0.35, feature_names=["f1", "f2", "f3"]
    )
    result = evaluate_cell(
        _cell(), baseline, candidate, n_boot=300, now=_NOW
    )
    assert result.adopted is True
    assert result.rejection_reasons == []


def test_evaluate_cell_rejects_small_race_count() -> None:
    baseline = _metrics("BASE", race_count=100, top1=0.40, place2=0.30, place3=0.25)
    candidate = _metrics("CAND", race_count=100, top1=0.50, place2=0.40, place3=0.35)
    result = evaluate_cell(_cell(), baseline, candidate, n_boot=300, now=_NOW)
    assert result.adopted is False
    assert "race_count 100 < 200" in result.rejection_reasons


def test_evaluate_cell_rejects_stale_evaluation() -> None:
    baseline = _metrics("BASE", top1=0.40, place2=0.30, place3=0.25)
    candidate = _metrics(
        "CAND", top1=0.50, place2=0.40, place3=0.35, evaluated_at=_STALE
    )
    result = evaluate_cell(_cell(), baseline, candidate, n_boot=300, now=_NOW)
    assert result.adopted is False
    assert any("older than 14 days" in reason for reason in result.rejection_reasons)


def test_evaluate_cell_rejects_when_only_top1_improves() -> None:
    baseline = _metrics("BASE", top1=0.40, place2=0.30, place3=0.25)
    candidate = _metrics("CAND", top1=0.50, place2=0.30, place3=0.25)
    result = evaluate_cell(_cell(), baseline, candidate, n_boot=300, now=_NOW)
    assert result.adopted is False
    assert "only 1 primary metric(s) improved by >= 0.0008; need >= 2" in (
        result.rejection_reasons
    )


def test_evaluate_cell_rejects_on_regression() -> None:
    baseline = _metrics(
        "BASE", top1=0.40, place2=0.30, place3=0.25, place4=0.30
    )
    candidate = _metrics(
        "CAND", top1=0.50, place2=0.40, place3=0.35, place4=0.10
    )
    result = evaluate_cell(_cell(), baseline, candidate, n_boot=300, now=_NOW)
    assert result.adopted is False
    assert "place4 regressed by -0.20000 (<= -0.0005)" in result.rejection_reasons


def test_evaluate_cell_rejects_on_weak_lb95() -> None:
    baseline = _metrics("BASE", race_count=200, top1=0.40, place2=0.30, place3=0.25)
    candidate = _metrics(
        "CAND", race_count=200, top1=0.405, place2=0.305, place3=0.255
    )
    result = evaluate_cell(_cell(), baseline, candidate, n_boot=300, now=_NOW)
    assert result.adopted is False
    assert any("bootstrap LB95" in reason for reason in result.rejection_reasons)


def test_evaluate_cell_adopts_all_metric_improvement_with_weak_lb95() -> None:
    baseline = _metrics(
        "BASE",
        race_count=200,
        top1=0.400,
        place2=0.300,
        place3=0.250,
        place4=0.200,
        place5=0.180,
        place6=0.150,
        top3_box=0.500,
    )
    candidate = _metrics(
        "CAND",
        race_count=200,
        top1=0.401,
        place2=0.301,
        place3=0.251,
        place4=0.201,
        place5=0.181,
        place6=0.151,
        top3_box=0.501,
    )
    result = evaluate_cell(_cell(), baseline, candidate, n_boot=300, now=_NOW)
    assert result.adopted is True
    assert not any("bootstrap LB95" in reason for reason in result.rejection_reasons)


def test_evaluate_cell_does_not_rescue_when_non_primary_metric_improves_too_little() -> None:
    baseline = _metrics(
        "BASE",
        race_count=200,
        top1=0.400,
        place2=0.300,
        place3=0.250,
        place4=0.200,
        place5=0.180,
        place6=0.150,
        top3_box=0.500,
    )
    candidate = _metrics(
        "CAND",
        race_count=200,
        top1=0.401,
        place2=0.301,
        place3=0.251,
        place4=0.2001,
        place5=0.181,
        place6=0.151,
        top3_box=0.501,
    )
    result = evaluate_cell(_cell(), baseline, candidate, n_boot=300, now=_NOW)
    assert result.adopted is False
    assert any("bootstrap LB95" in reason for reason in result.rejection_reasons)


def test_evaluate_cell_running_style_uses_accuracy_required_profile() -> None:
    baseline = _metrics("BASE", top1=0.40, place2=0.65, place3=0.30)
    candidate = _metrics("CAND", top1=0.50, place2=0.75, place3=0.30)
    result = evaluate_cell(
        _cell(),
        baseline,
        candidate,
        n_boot=300,
        now=_NOW,
        prediction_target="running_style",
    )
    assert result.adopted is True
    assert result.rejection_reasons == []


def test_evaluate_cell_running_style_rejects_without_accuracy_improvement() -> None:
    baseline = _metrics("BASE", top1=0.40, place2=0.65, place3=0.30)
    candidate = _metrics("CAND", top1=0.40, place2=0.75, place3=0.40)
    result = evaluate_cell(
        _cell(),
        baseline,
        candidate,
        n_boot=300,
        now=_NOW,
        prediction_target="running_style",
    )
    assert result.adopted is False
    assert "no top1 among improved primary metrics" in result.rejection_reasons


def test_evaluate_cell_running_style_adopts_all_metric_improvement_with_weak_lb95() -> None:
    baseline = _metrics("BASE", race_count=200, top1=0.400, place2=0.650, place3=0.300)
    candidate = _metrics("CAND", race_count=200, top1=0.401, place2=0.651, place3=0.301)
    result = evaluate_cell(
        _cell(),
        baseline,
        candidate,
        n_boot=300,
        now=_NOW,
        prediction_target="running_style",
    )
    assert result.adopted is True
    assert not any("bootstrap LB95" in reason for reason in result.rejection_reasons)


# ---------------------------------------------------------------------------
# evaluate_category


def test_evaluate_category_skips_cells_without_baseline() -> None:
    cell = _cell()
    grouped = {cell: [_metrics("OTHER"), _metrics("ANOTHER")]}
    results = evaluate_category(grouped, "BASE", now=_NOW)
    assert results == []


def test_evaluate_category_evaluates_candidates_against_baseline() -> None:
    cell = _cell()
    baseline = _metrics("BASE", top1=0.40, place2=0.30, place3=0.25)
    candidate = _metrics("CAND", top1=0.50, place2=0.40, place3=0.35)
    grouped = {cell: [baseline, candidate]}
    results = evaluate_category(grouped, "BASE", now=_NOW)
    assert len(results) == 1
    assert results[0].candidate.feature_set_hash == "CAND"


# ---------------------------------------------------------------------------
# variant_name_for_hash + group_variants


def test_variant_name_for_hash_uses_short_prefix() -> None:
    assert variant_name_for_hash("abcdef1234567890") == "cell-abcdef12"


def test_group_variants_groups_by_feature_set_hash() -> None:
    deltas: dict[str, float] = {}
    result_a = AdoptionResult(
        cell=_cell(class_label="A"),
        candidate=_metrics("hash1aaaa"),
        baseline=_metrics("BASE"),
        deltas=deltas,
        adopted=True,
        rejection_reasons=[],
    )
    result_b = AdoptionResult(
        cell=_cell(class_label="B"),
        candidate=_metrics("hash1aaaa"),
        baseline=_metrics("BASE"),
        deltas=deltas,
        adopted=True,
        rejection_reasons=[],
    )
    groups = group_variants([result_a, result_b])
    assert list(groups) == ["cell-hash1aaa"]
    assert len(groups["cell-hash1aaa"]) == 2


def test_group_variants_splits_same_hash_by_runtime_model_identity() -> None:
    baseline = _metrics("BASE")
    result_a = AdoptionResult(
        cell=_cell(class_label="A"),
        candidate=_metrics(
            "hash1aaaa",
            model_version="model-a",
            architecture="xgboost",
        ),
        baseline=baseline,
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )
    result_b = AdoptionResult(
        cell=_cell(class_label="B"),
        candidate=_metrics(
            "hash1aaaa",
            model_version="model-b",
            architecture="lightgbm",
        ),
        baseline=baseline,
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )
    groups = group_variants([result_a, result_b])
    assert len(groups) == 2
    assert sorted(len(results) for results in groups.values()) == [1, 1]


def test_select_best_adoptions_by_cell_keeps_strongest_candidate() -> None:
    cell = _cell()
    baseline = _metrics("BASE")
    weak = AdoptionResult(
        cell=cell,
        candidate=_metrics("weakhash", race_count=400),
        baseline=baseline,
        deltas={
            "top1": 0.002,
            "place2": 0.002,
            "place3": 0.002,
            "place4": 0.001,
            "place5": 0.001,
            "place6": 0.001,
            "top3_box": 0.001,
        },
        adopted=True,
        rejection_reasons=[],
    )
    strong = AdoptionResult(
        cell=cell,
        candidate=_metrics("stronghash", race_count=400),
        baseline=baseline,
        deltas={
            "top1": 0.005,
            "place2": 0.004,
            "place3": 0.004,
            "place4": 0.001,
            "place5": 0.001,
            "place6": 0.001,
            "top3_box": 0.001,
        },
        adopted=True,
        rejection_reasons=[],
    )
    selected = select_best_adoptions_by_cell([weak, strong])
    assert selected == [strong]
    assert select_best_adoptions_by_cell([strong, weak]) == [strong]


def test_select_best_adoptions_by_cell_retains_distinct_cells() -> None:
    baseline = _metrics("BASE")
    result_a = AdoptionResult(
        cell=_cell(class_label="A"),
        candidate=_metrics("hash-a"),
        baseline=baseline,
        deltas={"top1": 0.002, "place2": 0.002, "place3": 0.002},
        adopted=True,
        rejection_reasons=[],
    )
    result_b = AdoptionResult(
        cell=_cell(class_label="B"),
        candidate=_metrics("hash-b"),
        baseline=baseline,
        deltas={"top1": 0.002, "place2": 0.002, "place3": 0.002},
        adopted=True,
        rejection_reasons=[],
    )
    selected = select_best_adoptions_by_cell([result_b, result_a])
    assert selected == [result_a, result_b]


def test_select_best_adoptions_by_cell_ignores_finish_position_cell_subgroup() -> None:
    baseline = _metrics("BASE")
    legacy_subgroup = AdoptionResult(
        cell=_cell(cell_subgroup="legacy-subgroup"),
        candidate=_metrics("legacyhash"),
        baseline=baseline,
        deltas={
            "top1": 0.002,
            "place2": 0.002,
            "place3": 0.002,
            "place4": 0.001,
            "place5": 0.001,
            "place6": 0.001,
            "top3_box": 0.001,
        },
        adopted=True,
        rejection_reasons=[],
    )
    stronger_blank_subgroup = AdoptionResult(
        cell=_cell(cell_subgroup=""),
        candidate=_metrics("stronghash"),
        baseline=baseline,
        deltas={
            "top1": 0.004,
            "place2": 0.004,
            "place3": 0.004,
            "place4": 0.001,
            "place5": 0.001,
            "place6": 0.001,
            "top3_box": 0.001,
        },
        adopted=True,
        rejection_reasons=[],
    )
    selected = select_best_adoptions_by_cell(
        [legacy_subgroup, stronger_blank_subgroup],
        "finish_position",
    )
    assert selected == [stronger_blank_subgroup]


def test_select_best_adoptions_by_cell_keeps_running_style_cell_subgroup() -> None:
    baseline = _metrics("BASE")
    subgroup_a = AdoptionResult(
        cell=_cell(cell_subgroup="703"),
        candidate=_metrics("hash-a"),
        baseline=baseline,
        deltas={"top1": 0.002, "place2": 0.002, "place3": 0.002},
        adopted=True,
        rejection_reasons=[],
    )
    subgroup_b = AdoptionResult(
        cell=_cell(cell_subgroup="OP"),
        candidate=_metrics("hash-b"),
        baseline=baseline,
        deltas={"top1": 0.002, "place2": 0.002, "place3": 0.002},
        adopted=True,
        rejection_reasons=[],
    )
    selected = select_best_adoptions_by_cell(
        [subgroup_b, subgroup_a],
        "running_style",
    )
    assert selected == [subgroup_a, subgroup_b]


# ---------------------------------------------------------------------------
# generate_routing_json


def test_generate_routing_json_with_variant_and_rule() -> None:
    result = AdoptionResult(
        cell=_cell(
            class_label="A",
            subgroup="mile",
            racetrack="05",
            season="summer",
            surface="turf",
        ),
        candidate=_metrics("hashAAAA1", feature_names=["f1", "f2", "f3"]),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )
    variants = {"cell-hashAAAA": [result]}
    config = generate_routing_json(
        "jra",
        "jra-prod",
        200,
        "catboost",
        variants,
        allow_synthetic_model_version=True,
    )
    assert config == {
        "jra": {
            "default_variant": "sim",
            "variants": {
                "sim": {
                    "model_version": "jra-prod",
                    "feature_count": 200,
                    "architecture": "catboost",
                },
                "cell-hashAAAA": {
                    "model_version": "cell-hashAAAA",
                    "feature_count": 3,
                    "feature_set_hash": "hashAAAA1",
                    "feature_names": ["f1", "f2", "f3"],
                    "architecture": "catboost",
                },
            },
            "rules": [
                {
                    "conditions": [
                        {"dimension": "class", "values": ["A"]},
                        {"dimension": "distance_band", "values": ["mile"]},
                        {"dimension": "season", "values": ["summer"]},
                        {"dimension": "surface", "values": ["turf"]},
                        {"dimension": "venue", "values": ["05"]},
                    ],
                    "variant": "cell-hashAAAA",
                }
            ],
        }
    }


def test_generate_routing_json_uses_candidate_model_provenance_when_present() -> None:
    result = AdoptionResult(
        cell=_cell(class_label="E", subgroup="mile", racetrack="54", surface="dirt"),
        candidate=_metrics(
            "hashNAR54",
            feature_names=["f1", "f2", "f3"],
            model_version="nar-mile-e-54-lgbm-v1",
            architecture="lightgbm",
            method="cell-train-lgbm",
        ),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )
    variants = group_variants([result])
    config = generate_routing_json("nar", "nar-prod", 200, "xgboost", variants)
    nar = cast("dict[str, object]", config["nar"])
    variant_specs = cast("dict[str, dict[str, object]]", nar["variants"])
    variant_name = next(name for name in variant_specs if name != "sim")
    assert variant_specs[variant_name] == {
        "model_version": "nar-mile-e-54-lgbm-v1",
        "feature_count": 3,
        "feature_set_hash": "hashNAR54",
        "feature_names": ["f1", "f2", "f3"],
        "architecture": "lightgbm",
        "method": "cell-train-lgbm",
    }


def test_generate_routing_json_requires_model_provenance_by_default() -> None:
    result = AdoptionResult(
        cell=_cell(class_label="E", subgroup="mile", racetrack="54", surface="dirt"),
        candidate=_metrics("hashNAR54", feature_names=["f1", "f2", "f3"]),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )

    with pytest.raises(
        subject.RoutingModelProvenanceError,
        match="requires real model provenance",
    ):
        generate_routing_json(
            "nar",
            "nar-prod",
            200,
            "xgboost",
            {"cell-hashNAR5": [result]},
        )


def _write_model_artifact(
    root: Path,
    category: str,
    model_version: str,
    feature_names: list[str],
) -> None:
    artifact_dir = root / category / model_version
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "model.json").write_text("{}", encoding="utf-8")
    (artifact_dir / "metadata.json").write_text(
        json.dumps({"feature_names": feature_names}),
        encoding="utf-8",
    )


def test_generate_routing_json_validates_model_artifact_root(tmp_path: Path) -> None:
    _write_model_artifact(tmp_path, "nar", "nar-prod", ["d1", "d2"])
    _write_model_artifact(tmp_path, "nar", "nar-mile-e-54-lgbm-v1", ["f1", "f2", "f3"])
    result = AdoptionResult(
        cell=_cell(class_label="E", subgroup="mile", racetrack="54", surface="dirt"),
        candidate=_metrics(
            "hashNAR54",
            feature_names=["f1", "f2", "f3"],
            model_version="nar-mile-e-54-lgbm-v1",
            architecture="lightgbm",
        ),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )

    config = generate_routing_json(
        "nar",
        "nar-prod",
        2,
        "xgboost",
        group_variants([result]),
        model_artifacts_root=tmp_path,
    )

    nar = cast("dict[str, object]", config["nar"])
    variants = cast("dict[str, dict[str, object]]", nar["variants"])
    variant_name = next(name for name in variants if name != "sim")
    assert variants[variant_name]["model_version"] == "nar-mile-e-54-lgbm-v1"


def test_generate_routing_json_rejects_missing_model_artifact(tmp_path: Path) -> None:
    _write_model_artifact(tmp_path, "nar", "nar-prod", ["d1", "d2"])
    result = AdoptionResult(
        cell=_cell(class_label="E", subgroup="mile", racetrack="54", surface="dirt"),
        candidate=_metrics(
            "hashNAR54",
            feature_names=["f1", "f2", "f3"],
            model_version="nar-mile-e-54-lgbm-v1",
            architecture="lightgbm",
        ),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )

    with pytest.raises(subject.RoutingModelArtifactError, match="missing"):
        generate_routing_json(
            "nar",
            "nar-prod",
            2,
            "xgboost",
            group_variants([result]),
            model_artifacts_root=tmp_path,
        )


def test_generate_routing_json_rejects_artifact_without_feature_names(
    tmp_path: Path,
) -> None:
    _write_model_artifact(tmp_path, "nar", "nar-prod", ["d1", "d2"])
    _write_model_artifact(tmp_path, "nar", "nar-mile-e-54-lgbm-v1", ["f1", "f2", "f3"])
    metadata_path = (
        tmp_path
        / "nar"
        / "nar-mile-e-54-lgbm-v1"
        / "metadata.json"
    )
    metadata_path.write_text("{}", encoding="utf-8")
    result = AdoptionResult(
        cell=_cell(class_label="E", subgroup="mile", racetrack="54", surface="dirt"),
        candidate=_metrics(
            "hashNAR54",
            feature_names=["f1", "f2", "f3"],
            model_version="nar-mile-e-54-lgbm-v1",
            architecture="lightgbm",
        ),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )

    with pytest.raises(subject.RoutingModelArtifactError, match="must be a list"):
        generate_routing_json(
            "nar",
            "nar-prod",
            2,
            "xgboost",
            group_variants([result]),
            model_artifacts_root=tmp_path,
        )


def test_generate_routing_json_rejects_artifact_feature_count_mismatch(
    tmp_path: Path,
) -> None:
    _write_model_artifact(tmp_path, "nar", "nar-prod", ["d1", "d2"])
    _write_model_artifact(tmp_path, "nar", "nar-mile-e-54-lgbm-v1", ["f1", "f2"])
    result = AdoptionResult(
        cell=_cell(class_label="E", subgroup="mile", racetrack="54", surface="dirt"),
        candidate=_metrics(
            "hashNAR54",
            feature_names=["f1", "f2", "f3"],
            model_version="nar-mile-e-54-lgbm-v1",
            architecture="lightgbm",
        ),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )

    with pytest.raises(subject.RoutingModelArtifactError, match="length mismatch"):
        generate_routing_json(
            "nar",
            "nar-prod",
            2,
            "xgboost",
            group_variants([result]),
            model_artifacts_root=tmp_path,
        )


def test_generate_routing_json_skips_empty_dimension_values() -> None:
    result = AdoptionResult(
        cell=_cell(
            class_label="A",
            subgroup="mile",
            racetrack="05",
            season="summer",
            surface="",
        ),
        candidate=_metrics("hashBBBB1"),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )
    config = generate_routing_json(
        "nar",
        "nar-prod",
        140,
        "xgboost",
        {"cell-hashBBBB": [result]},
        allow_synthetic_model_version=True,
    )
    category = cast("dict[str, object]", config["nar"])
    rules = cast("list[dict[str, object]]", category["rules"])
    conditions = cast("list[dict[str, object]]", rules[0]["conditions"])
    dimensions = [condition["dimension"] for condition in conditions]
    assert dimensions == ["class", "distance_band", "season", "venue"]


def test_generate_routing_json_with_no_variants() -> None:
    config = generate_routing_json("ban-ei", "banei-prod", 130, "catboost", {})
    assert config == {
        "ban-ei": {
            "default_variant": "sim",
            "variants": {
                "sim": {
                    "model_version": "banei-prod",
                    "feature_count": 130,
                    "architecture": "catboost",
                }
            },
            "rules": [],
        }
    }


def test_generate_running_style_feature_selection_json_is_not_worker_routing() -> None:
    result = AdoptionResult(
        cell=_cell(
            class_label="G1",
            subgroup="sprint",
            racetrack="05",
            season="spring",
            surface="turf",
            cell_subgroup="703",
        ),
        candidate=_metrics("hashRS001", feature_names=["feature_b", "feature_a"]),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )
    config = generate_running_style_feature_selection_json(
        "jra", 180, {"cell-hashRS00": [result]}
    )

    assert config["schema_version"] == 1
    assert config["type"] == "running_style_cell_feature_selection_routing"
    assert config["prediction_target"] == "running_style"
    assert config["consumer"] == "running_style_lightgbm --cell-feature-selection-json"
    assert config["worker_production_routing"] is False
    assert config["default_feature_selection"] == {
        "mode": "training_default",
        "feature_count": 180,
    }

    jra = cast("dict[str, object]", config["jra"])
    assert "default_variant" not in jra
    variants = cast("dict[str, dict[str, object]]", jra["variants"])
    assert variants == {
        "cell-hashRS00": {
            "feature_count": 2,
            "feature_set_hash": "hashRS001",
            "feature_names": ["feature_a", "feature_b"],
        }
    }
    assert "model_version" not in variants["cell-hashRS00"]
    assert "architecture" not in variants["cell-hashRS00"]
    assert jra["rules"] == [
        {
            "conditions": [
                {"dimension": "class", "values": ["G1"]},
                {"dimension": "distance_band", "values": ["sprint"]},
                {"dimension": "season", "values": ["spring"]},
                {"dimension": "surface", "values": ["turf"]},
                {"dimension": "venue", "values": ["05"]},
                {"dimension": "subgroup", "values": ["703"]},
            ],
            "variant": "cell-hashRS00",
        }
    ]


def test_generate_routing_json_omits_db_subgroup_for_finish_position_routing() -> None:
    result = AdoptionResult(
        cell=_cell(cell_subgroup="703"),
        candidate=_metrics("CANDIDATEHASH", feature_names=["f1", "f2", "f3"]),
        baseline=_metrics("BASE"),
        deltas={},
        adopted=True,
        rejection_reasons=[],
    )
    config = generate_routing_json(
        "jra",
        "jra-production",
        130,
        "catboost",
        {"cell-CANDIDAT": [result]},
        allow_synthetic_model_version=True,
    )
    jra = cast("dict[str, object]", config["jra"])
    rules = cast("list[dict[str, object]]", jra["rules"])
    conditions = cast("list[dict[str, object]]", rules[0]["conditions"])
    assert {"dimension": "distance_band", "values": ["mile"]} in conditions
    assert {"dimension": "subgroup", "values": ["703"]} not in conditions


def test_generate_running_style_feature_selection_json_with_no_variants() -> None:
    config = generate_running_style_feature_selection_json("jra", 180, {})
    assert config == {
        "schema_version": 1,
        "type": "running_style_cell_feature_selection_routing",
        "prediction_target": "running_style",
        "consumer": "running_style_lightgbm --cell-feature-selection-json",
        "worker_production_routing": False,
        "default_feature_selection": {
            "mode": "training_default",
            "feature_count": 180,
        },
        "jra": {
            "variants": {},
            "rules": [],
        },
    }


# ---------------------------------------------------------------------------
# parse_row + load_cell_metrics


def _db_row(
    feature_set_hash: str,
    *,
    class_label: str = "A",
    top1: float = 0.40,
    subgroup: str = "subgroup-703",
    metric_payload: dict[str, object] | None = None,
    model_version: str = "",
    architecture: str = "",
    method: str = "",
    cell_model_key: str = "",
    cell_variant_id: str = "",
) -> tuple[object, ...]:
    return (
        "jra",
        class_label,
        "mile",
        "05",
        "summer",
        "turf",
        subgroup,
        feature_set_hash,
        1000,
        top1,
        0.30,
        0.25,
        0.20,
        0.18,
        0.15,
        0.50,
        _FRESH,
        ["f1", "f2"],
        metric_payload if metric_payload is not None else {},
        model_version,
        architecture,
        method,
        cell_model_key,
        cell_variant_id,
    )


def test_parse_row_maps_distance_band_to_router_cell() -> None:
    cell, metrics = parse_row(_db_row("BASE"))
    assert cell == CellKey(
        category="jra",
        class_label="A",
        subgroup="mile",
        racetrack="05",
        season="summer",
        surface="turf",
        cell_subgroup="",
    )
    assert metrics.race_count == 1000
    assert metrics.top1 == pytest.approx(0.40)
    assert metrics.feature_set_hash == "BASE"
    assert metrics.feature_names == ["f1", "f2"]
    assert metrics.evaluated_at == _FRESH


def test_parse_row_reads_model_provenance_from_metric_payload() -> None:
    _, metrics = parse_row(
        _db_row(
            "CAND",
            metric_payload={
                "model_version": "nar-mile-e-54-lgbm-v1",
                "architecture": "lightgbm",
                "method": "cell-train-lgbm",
            },
        )
    )
    assert metrics.model_version == "nar-mile-e-54-lgbm-v1"
    assert metrics.architecture == "lightgbm"
    assert metrics.method == "cell-train-lgbm"


def test_parse_row_reads_model_provenance_from_extra_payload() -> None:
    _, metrics = parse_row(
        _db_row(
            "CAND",
            metric_payload={
                "extra": {
                    "model_version": "auto-jra-1",
                    "architecture": "catboost",
                    "exploration_method": "block_tpe",
                }
            },
        )
    )
    assert metrics.model_version == "auto-jra-1"
    assert metrics.architecture == "catboost"
    assert metrics.method == "block_tpe"


def test_parse_row_reads_runtime_identity_from_columns() -> None:
    _, metrics = parse_row(
        _db_row(
            "CAND",
            model_version="rs-cell-v1",
            architecture="lightgbm",
            method="train-cells",
            cell_model_key="running-style/models/jra/cells/cell-a.flatbin",
            cell_variant_id="cell-a",
        ),
        prediction_target="running_style",
    )
    assert metrics.model_version == "rs-cell-v1"
    assert metrics.architecture == "lightgbm"
    assert metrics.method == "train-cells"
    assert metrics.cell_model_key == "running-style/models/jra/cells/cell-a.flatbin"
    assert metrics.cell_variant_id == "cell-a"


def test_parse_row_normalizes_canonical_finish_position_subgroup() -> None:
    cell, _ = parse_row(
        _db_row("BASE", subgroup="jra_turf_mile_A_summer_05"),
        prediction_target="finish_position",
    )
    assert cell == CellKey(
        category="jra",
        class_label="A",
        subgroup="mile",
        racetrack="05",
        season="summer",
        surface="turf",
        cell_subgroup="",
    )


def test_parse_row_keeps_canonical_running_style_subgroup() -> None:
    cell, _ = parse_row(
        _db_row("BASE", subgroup="jra_turf_mile_A_summer_05"),
        prediction_target="running_style",
    )
    assert cell == CellKey(
        category="jra",
        class_label="A",
        subgroup="mile",
        racetrack="05",
        season="summer",
        surface="turf",
        cell_subgroup="jra_turf_mile_A_summer_05",
    )


def test_load_cell_metrics_groups_rows_by_cell() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        _db_row("BASE"),
        _db_row("CAND"),
        _db_row("BASE", class_label="B"),
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    grouped = load_cell_metrics(conn, "jra")
    cursor.execute.assert_called_once_with(
        subject._SELECT_CELLS, ("finish_position", "jra")
    )
    assert len(grouped) == 2
    cell_a = CellKey("jra", "A", "mile", "05", "summer", "turf", "")
    assert {m.feature_set_hash for m in grouped[cell_a]} == {"BASE", "CAND"}


def test_load_cell_metrics_groups_finish_position_subgroups_into_cell() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        _db_row("BASE", subgroup=""),
        _db_row("CAND", subgroup="jra_turf_mile_A_summer_05"),
        _db_row("ALT", subgroup="legacy-per-class-703"),
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    grouped = load_cell_metrics(conn, "jra", "finish_position")
    assert len(grouped) == 1
    cell = CellKey("jra", "A", "mile", "05", "summer", "turf", "")
    assert {m.feature_set_hash for m in grouped[cell]} == {"BASE", "CAND", "ALT"}


def test_load_cell_metrics_keeps_distinct_db_subgroups_as_distinct_cells() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        _db_row("BASE", subgroup="703"),
        _db_row("CAND", subgroup="703"),
        _db_row("BASE", subgroup="OP"),
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    grouped = load_cell_metrics(conn, "jra", "running_style")
    assert len(grouped) == 2
    assert CellKey("jra", "A", "mile", "05", "summer", "turf", "703") in grouped
    assert CellKey("jra", "A", "mile", "05", "summer", "turf", "OP") in grouped


def test_load_cell_metrics_can_filter_running_style_target() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [_db_row("RUN")]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    load_cell_metrics(conn, "jra", "running_style")
    cursor.execute.assert_called_once_with(
        subject._SELECT_CELLS, ("running_style", "jra")
    )


# ---------------------------------------------------------------------------
# _connect


def test_connect_opens_psycopg_connection() -> None:
    psycopg_mod = __import__("psycopg")
    sentinel = MagicMock()
    with patch.object(psycopg_mod, "connect", return_value=sentinel) as connect:
        result = subject._connect("postgresql://example")
    assert result is sentinel
    connect.assert_called_once_with("postgresql://example")


# ---------------------------------------------------------------------------
# _infer_default_feature_count


def test_infer_default_feature_count_returns_baseline_length() -> None:
    grouped = {
        _cell(): [_metrics("BASE", feature_names=["f1", "f2", "f3", "f4"])],
    }
    assert subject._infer_default_feature_count(grouped, "BASE") == 4


def test_infer_default_feature_count_zero_when_no_baseline() -> None:
    grouped = {_cell(): [_metrics("OTHER")]}
    assert subject._infer_default_feature_count(grouped, "BASE") == 0


# ---------------------------------------------------------------------------
# main


def _patched_grouped() -> dict[CellKey, list[CellMetrics]]:
    cell = _cell()
    baseline = _metrics(
        "BASE", top1=0.40, place2=0.30, place3=0.25, evaluated_at=datetime.now(timezone.utc)
    )
    candidate = _metrics(
        "CANDIDATEHASH",
        top1=0.50,
        place2=0.40,
        place3=0.35,
        evaluated_at=datetime.now(timezone.utc),
        feature_names=["f1", "f2", "f3"],
    )
    return {cell: [baseline, candidate]}


def test_main_writes_output_file(tmp_path: Path) -> None:
    output_path = tmp_path / "cell_routing.json"
    conn = MagicMock()
    with patch.object(subject, "_connect", return_value=conn), patch.object(
        subject, "load_cell_metrics", return_value=_patched_grouped()
    ):
        main(
            [
                "--pg-url",
                "postgresql://example",
                "--category",
                "jra",
                "--baseline-hash",
                "BASE",
                "--output-path",
                str(output_path),
                "--allow-synthetic-model-version",
            ]
        )
    conn.close.assert_called_once_with()
    config = json.loads(output_path.read_text(encoding="utf-8"))
    jra = config["jra"]
    assert jra["default_variant"] == "sim"
    assert jra["variants"]["sim"]["feature_count"] == 2
    assert jra["variants"]["cell-CANDIDAT"]["feature_count"] == 3
    assert len(jra["rules"]) == 1


def test_main_requires_model_provenance_for_finish_position_production(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "cell_routing.json"
    conn = MagicMock()
    with patch.object(subject, "_connect", return_value=conn), patch.object(
        subject, "load_cell_metrics", return_value=_patched_grouped()
    ), pytest.raises(subject.RoutingModelProvenanceError):
        main(
            [
                "--pg-url",
                "postgresql://example",
                "--category",
                "jra",
                "--baseline-hash",
                "BASE",
                "--output-path",
                str(output_path),
            ]
        )

    conn.close.assert_called_once_with()
    assert output_path.exists() is False


def test_main_writes_only_best_candidate_for_each_cell(tmp_path: Path) -> None:
    output_path = tmp_path / "cell_routing.json"
    cell = _cell()
    baseline = _metrics(
        "BASE",
        top1=0.40,
        place2=0.30,
        place3=0.25,
        evaluated_at=datetime.now(timezone.utc),
    )
    weaker = _metrics(
        "WEAKERHASH",
        top1=0.50,
        place2=0.40,
        place3=0.35,
        evaluated_at=datetime.now(timezone.utc),
        feature_names=["f1", "f2", "weak"],
    )
    stronger = _metrics(
        "STRONGERHASH",
        top1=0.55,
        place2=0.45,
        place3=0.40,
        evaluated_at=datetime.now(timezone.utc),
        feature_names=["f1", "f2", "strong"],
    )
    conn = MagicMock()
    with patch.object(subject, "_connect", return_value=conn), patch.object(
        subject,
        "load_cell_metrics",
        return_value={cell: [baseline, weaker, stronger]},
    ):
        main(
            [
                "--pg-url",
                "postgresql://example",
                "--category",
                "jra",
                "--baseline-hash",
                "BASE",
                "--output-path",
                str(output_path),
                "--allow-synthetic-model-version",
            ]
        )
    config = json.loads(output_path.read_text(encoding="utf-8"))
    jra = config["jra"]
    assert "cell-STRONGER" in jra["variants"]
    assert "cell-WEAKERHA" not in jra["variants"]
    assert jra["rules"] == [
        {
            "conditions": [
                {"dimension": "class", "values": ["A"]},
                {"dimension": "distance_band", "values": ["mile"]},
                {"dimension": "season", "values": ["summer"]},
                {"dimension": "surface", "values": ["turf"]},
                {"dimension": "venue", "values": ["05"]},
            ],
            "variant": "cell-STRONGER",
        }
    ]


def _patched_banei_grouped() -> dict[CellKey, list[CellMetrics]]:
    cell = _cell(category="ban-ei")
    baseline = _metrics(
        "BASE", top1=0.40, place2=0.30, place3=0.25, evaluated_at=datetime.now(timezone.utc)
    )
    candidate = _metrics(
        "CANDIDATEHASH",
        top1=0.50,
        place2=0.40,
        place3=0.35,
        evaluated_at=datetime.now(timezone.utc),
        feature_names=["f1", "f2", "f3"],
    )
    return {cell: [baseline, candidate]}


def test_main_ban_ei_category_queries_db_as_banei_but_emits_ban_ei_key(
    tmp_path: Path,
) -> None:
    """Regression test for the CLI/DB category spelling mismatch.

    ``--category ban-ei`` must query ``cell_training_evaluations`` with the DB
    row spelling ``banei`` (no hyphen), while the emitted routing config keeps
    the hyphenated ``ban-ei`` key that ``predict_lib.cell_router`` expects.
    Before the ``_db_category`` translation this queried with the literal
    ``ban-ei`` and silently returned zero rows.
    """
    output_path = tmp_path / "cell_routing.json"
    conn = MagicMock()
    with patch.object(subject, "_connect", return_value=conn), patch.object(
        subject, "load_cell_metrics", return_value=_patched_banei_grouped()
    ) as load_cell_metrics:
        main(
            [
                "--pg-url",
                "postgresql://example",
                "--category",
                "ban-ei",
                "--baseline-hash",
                "BASE",
                "--output-path",
                str(output_path),
                "--allow-synthetic-model-version",
            ]
        )
    load_cell_metrics.assert_called_once_with(conn, "banei", "finish_position")
    config = json.loads(output_path.read_text(encoding="utf-8"))
    assert "ban-ei" in config
    ban_ei = config["ban-ei"]
    assert ban_ei["default_variant"] == "sim"
    assert ban_ei["variants"]["sim"]["model_version"] == "ban-ei-production"
    assert ban_ei["variants"]["cell-CANDIDAT"]["feature_count"] == 3


def test_main_running_style_writes_feature_selection_config(tmp_path: Path) -> None:
    output_path = tmp_path / "running_style_cell_feature_selection.json"
    conn = MagicMock()
    with patch.object(subject, "_connect", return_value=conn), patch.object(
        subject, "load_cell_metrics", return_value=_patched_grouped()
    ) as load_cell_metrics:
        main(
            [
                "--pg-url",
                "postgresql://example",
                "--category",
                "jra",
                "--prediction-target",
                "running_style",
                "--baseline-hash",
                "BASE",
                "--output-path",
                str(output_path),
            ]
        )

    load_cell_metrics.assert_called_once_with(conn, "jra", "running_style")
    config = json.loads(output_path.read_text(encoding="utf-8"))
    assert config["type"] == "running_style_cell_feature_selection_routing"
    assert config["worker_production_routing"] is False
    jra = config["jra"]
    assert "default_variant" not in jra
    assert "sim" not in jra["variants"]
    assert jra["variants"]["cell-CANDIDAT"]["feature_names"] == ["f1", "f2", "f3"]


def test_main_dry_run_prints_without_writing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output_path = tmp_path / "should_not_exist.json"
    conn = MagicMock()
    with patch.object(subject, "_connect", return_value=conn), patch.object(
        subject, "load_cell_metrics", return_value=_patched_grouped()
    ):
        main(
            [
                "--pg-url",
                "postgresql://example",
                "--category",
                "jra",
                "--baseline-hash",
                "BASE",
                "--output-path",
                str(output_path),
                "--dry-run",
                "--allow-synthetic-model-version",
            ]
        )
    captured = capsys.readouterr()
    assert output_path.exists() is False
    assert '"default_variant": "sim"' in captured.out


def test_main_prints_to_stdout_when_no_output_path(
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn = MagicMock()
    with patch.object(subject, "_connect", return_value=conn), patch.object(
        subject, "load_cell_metrics", return_value=_patched_grouped()
    ):
        main(
            [
                "--pg-url",
                "postgresql://example",
                "--category",
                "jra",
                "--baseline-hash",
                "BASE",
                "--default-model-version",
                "jra-cb-v9-sim-2013",
                "--default-feature-count",
                "263",
                "--default-architecture",
                "catboost",
                "--allow-synthetic-model-version",
            ]
        )
    captured = capsys.readouterr()
    config = json.loads(captured.out)
    assert config["jra"]["variants"]["sim"]["model_version"] == "jra-cb-v9-sim-2013"
    assert config["jra"]["variants"]["sim"]["feature_count"] == 263
