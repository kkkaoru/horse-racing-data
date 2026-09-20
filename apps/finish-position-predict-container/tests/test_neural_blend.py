from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from predict_lib.neural_blend import (
    ARTIFACT_VERSION,
    NEURAL_BLEND_ARTIFACT_ENV,
    NEURAL_BLEND_ENABLED_ENV,
    NEURAL_BLEND_WEIGHT_ENV,
    adjust_prediction_rows_with_neural,
    configured_neural_weight,
    load_neural_blend_artifact,
    parse_neural_blend_artifact,
)
from predict_lib.prophet_cell_policy import parse_prophet_cell_policy

IDENTITY_GELU = [
    {"weight": [[1.0, 0.0], [0.0, 1.0]], "bias": [0.0, 0.0]},
    {"weight": [[1.0, -1.0]], "bias": [0.0]},
]


def artifact_document(**overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "version": ARTIFACT_VERSION,
        "category": "jra",
        "feature_order": ["a", "b"],
        "mean": [0.0, 0.0],
        "scale": [1.0, 1.0],
        "layers": IDENTITY_GELU,
    }
    document.update(overrides)
    return document


def policy():
    return parse_prophet_cell_policy(
        {
            "version": "neural-test-v1",
            "default_enabled": False,
            "default_weight": 0.05,
            "categories": {
                "jra": {"cells": {"winner": {"enabled": True, "weight": 0.03}}},
                "nar": {"cells": {"winner": {"enabled": False, "weight": 0.03}}},
            },
        }
    )


def rows() -> list[list[object]]:
    return [
        ["model", "jra", "2026", "0903", "06", "01", "h1", 1, 1.0, 1],
        ["model", "jra", "2026", "0903", "06", "01", "h2", 2, 0.8, 2],
        ["model", "jra", "2026", "0903", "06", "01", "h3", 3, 0.0, 3],
    ]


def test_artifact_scores_forward_pass() -> None:
    artifact = parse_neural_blend_artifact(artifact_document())
    assert artifact.category == "jra"
    scores = artifact.score([{"a": 2.0, "b": 1.0}, {"a": "bad", "b": None}])
    expected = 0.5 * 2.0 * (1.0 + math.erf(2.0 / math.sqrt(2.0))) - 0.5 * 1.0 * (
        1.0 + math.erf(1.0 / math.sqrt(2.0))
    )
    assert scores[0] == pytest.approx(expected)
    assert scores[1] == pytest.approx(0.0)


def test_artifact_rejects_malformed_documents() -> None:
    with pytest.raises(ValueError):
        parse_neural_blend_artifact([])
    for overrides in (
        {"version": "other"},
        {"category": "usa"},
        {"feature_order": []},
        {"feature_order": ["a", ""]},
        {"mean": [0.0]},
        {"scale": [1.0]},
        {"scale": [0.0, 1.0]},
        {"mean": ["x", 0.0]},
        {"layers": []},
        {"layers": [{"weight": [], "bias": []}]},
        {"layers": [{"weight": [[1.0, 0.0]], "bias": [0.0, 0.0]}]},
        {"layers": [{"weight": [[1.0, 0.0], [0.0, 1.0]], "bias": [0.0]}]},
        {"layers": [{"weight": [[1.0, 0.0], [0.0, 1.0]], "bias": [0.0, 0.0]}]},
        {"layers": [{"weight": [[1.0, 0.0], [0.0, 1.0]], "bias": ["x", 0.0]}]},
    ):
        with pytest.raises(ValueError):
            parse_neural_blend_artifact(artifact_document(**overrides))


def test_load_artifact_is_fail_safe(tmp_path: Path) -> None:
    assert load_neural_blend_artifact(tmp_path / "missing.json") is None
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert load_neural_blend_artifact(broken) is None
    invalid = tmp_path / "invalid.json"
    invalid.write_text(json.dumps(artifact_document(category="usa")), encoding="utf-8")
    assert load_neural_blend_artifact(invalid) is None
    valid = tmp_path / "valid.json"
    valid.write_text(json.dumps(artifact_document()), encoding="utf-8")
    assert load_neural_blend_artifact(valid) is not None


def test_configured_neural_weight_resolution() -> None:
    cell_policy = policy()
    assert configured_neural_weight("jra", {}, cell_variant="winner", policy=cell_policy) == 0.03
    assert configured_neural_weight("jra", {}, cell_variant="other", policy=cell_policy) is None
    assert configured_neural_weight("nar", {}, cell_variant="winner", policy=cell_policy) is None
    assert (
        configured_neural_weight(
            "jra",
            {NEURAL_BLEND_ENABLED_ENV: "off"},
            cell_variant="winner",
            policy=cell_policy,
        )
        is None
    )
    assert (
        configured_neural_weight(
            "jra",
            {NEURAL_BLEND_ENABLED_ENV: "on", NEURAL_BLEND_WEIGHT_ENV: "0.25"},
            cell_variant="winner",
            policy=cell_policy,
        )
        == 0.25
    )
    for invalid in ("bad", "0", "1.5", "-0.1"):
        assert (
            configured_neural_weight(
                "jra",
                {NEURAL_BLEND_ENABLED_ENV: "on", NEURAL_BLEND_WEIGHT_ENV: invalid},
                cell_variant="winner",
                policy=cell_policy,
            )
            is None
        )


def test_adjustment_applies_enabled_cell(tmp_path: Path) -> None:
    path = tmp_path / "artifact.json"
    path.write_text(json.dumps(artifact_document()), encoding="utf-8")
    entries = [
        {"ketto_toroku_bango": "h1", "a": 0.0, "b": 0.0},
        {"ketto_toroku_bango": "h2", "a": 3.0, "b": 0.0},
        {"ketto_toroku_bango": "h3", "a": 0.0, "b": 3.0},
    ]
    result = adjust_prediction_rows_with_neural(
        rows(),
        entries,
        "jra",
        {NEURAL_BLEND_ARTIFACT_ENV: str(path), NEURAL_BLEND_WEIGHT_ENV: "1"},
        cell_variant="winner",
        policy=policy(),
    )
    assert result.applied is True
    assert result.reason == "applied"
    assert [row[6] for row in result.rows] == ["h2", "h1", "h3"]
    assert [row[9] for row in result.rows] == [1, 2, 3]


def test_adjustment_fail_safe_reasons(tmp_path: Path) -> None:
    path = tmp_path / "artifact.json"
    path.write_text(json.dumps(artifact_document()), encoding="utf-8")
    entries = [
        {"ketto_toroku_bango": "h1", "a": 0.0, "b": 0.0},
        {"ketto_toroku_bango": "h2", "a": 3.0, "b": 0.0},
        {"ketto_toroku_bango": "h3", "a": 0.0, "b": 3.0},
    ]
    disabled = adjust_prediction_rows_with_neural(
        rows(), entries, "jra", {}, cell_variant="other", policy=policy()
    )
    assert disabled.reason == "disabled"
    mismatch = adjust_prediction_rows_with_neural(
        rows(), [], "jra", {NEURAL_BLEND_WEIGHT_ENV: "0.1"}, cell_variant="winner", policy=policy()
    )
    assert mismatch.reason == "row-entry-mismatch"
    unavailable = adjust_prediction_rows_with_neural(
        rows(),
        entries,
        "jra",
        {NEURAL_BLEND_ARTIFACT_ENV: str(tmp_path / "missing.json"), NEURAL_BLEND_WEIGHT_ENV: "0.1"},
        cell_variant="winner",
        policy=policy(),
    )
    assert unavailable.reason == "artifact-unavailable"
    wrong_category = adjust_prediction_rows_with_neural(
        rows(),
        entries,
        "nar",
        {NEURAL_BLEND_ARTIFACT_ENV: str(path), NEURAL_BLEND_WEIGHT_ENV: "0.1"},
        cell_variant="winner",
        policy=parse_prophet_cell_policy(
            {
                "version": "neural-test-v2",
                "default_enabled": True,
                "default_weight": 0.05,
                "categories": {},
            }
        ),
    )
    assert wrong_category.reason == "artifact-unavailable"
    sparse_entries = [
        {"ketto_toroku_bango": "h1", "a": 0.0, "b": 0.0},
        {"a": 1.0, "b": 1.0},
        {"ketto_toroku_bango": "", "a": 2.0, "b": 2.0},
    ]
    insufficient = adjust_prediction_rows_with_neural(
        rows(),
        sparse_entries,
        "jra",
        {NEURAL_BLEND_ARTIFACT_ENV: str(path), NEURAL_BLEND_WEIGHT_ENV: "0.1"},
        cell_variant="winner",
        policy=policy(),
    )
    assert insufficient.reason == "insufficient-coverage"
    degenerate = adjust_prediction_rows_with_neural(
        rows(),
        [dict(entry, a=1.0, b=1.0) for entry in entries],
        "jra",
        {NEURAL_BLEND_ARTIFACT_ENV: str(path), NEURAL_BLEND_WEIGHT_ENV: "0.1"},
        cell_variant="winner",
        policy=policy(),
    )
    assert degenerate.reason == "degenerate-spread"


def test_default_policy_file_is_baked_and_fail_safe() -> None:
    assert configured_neural_weight("jra", {}, cell_variant="sim") is None
    assert configured_neural_weight("nar", {}, cell_variant="sim") is None
    assert configured_neural_weight("ban-ei", {}, cell_variant="sim") is None
