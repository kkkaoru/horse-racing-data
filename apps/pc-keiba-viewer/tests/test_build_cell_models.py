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
    bootstrap_lb95,
    check_multi_metric_gate,
    check_no_regression,
    compute_deltas,
    evaluate_category,
    evaluate_cell,
    generate_routing_json,
    group_variants,
    load_cell_metrics,
    main,
    parse_row,
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
) -> CellKey:
    return CellKey(
        category=category,
        class_label=class_label,
        subgroup=subgroup,
        racetrack=racetrack,
        season=season,
        surface=surface,
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
    )


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
    config = generate_routing_json("jra", "jra-prod", 200, "catboost", variants)
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
        "nar", "nar-prod", 140, "xgboost", {"cell-hashBBBB": [result]}
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


# ---------------------------------------------------------------------------
# parse_row + load_cell_metrics


def _db_row(
    feature_set_hash: str,
    *,
    class_label: str = "A",
    top1: float = 0.40,
) -> tuple[object, ...]:
    return (
        "jra",
        class_label,
        "mile",
        "05",
        "summer",
        "turf",
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
    )


def test_parse_row_maps_columns_to_cell_and_metrics() -> None:
    cell, metrics = parse_row(_db_row("BASE"))
    assert cell == CellKey(
        category="jra",
        class_label="A",
        subgroup="mile",
        racetrack="05",
        season="summer",
        surface="turf",
    )
    assert metrics.race_count == 1000
    assert metrics.top1 == pytest.approx(0.40)
    assert metrics.feature_set_hash == "BASE"
    assert metrics.feature_names == ["f1", "f2"]
    assert metrics.evaluated_at == _FRESH


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
    cell_a = CellKey("jra", "A", "mile", "05", "summer", "turf")
    assert {m.feature_set_hash for m in grouped[cell_a]} == {"BASE", "CAND"}


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
            ]
        )
    conn.close.assert_called_once_with()
    config = json.loads(output_path.read_text(encoding="utf-8"))
    jra = config["jra"]
    assert jra["default_variant"] == "sim"
    assert jra["variants"]["sim"]["feature_count"] == 2
    assert jra["variants"]["cell-CANDIDAT"]["feature_count"] == 3
    assert len(jra["rules"]) == 1


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
            ]
        )
    captured = capsys.readouterr()
    config = json.loads(captured.out)
    assert config["jra"]["variants"]["sim"]["model_version"] == "jra-cb-v9-sim-2013"
    assert config["jra"]["variants"]["sim"]["feature_count"] == 263
