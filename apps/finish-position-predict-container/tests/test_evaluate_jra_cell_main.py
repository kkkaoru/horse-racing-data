from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

import evaluate_jra_cell_models as evaluation
from predict_lib.jra_cell_scope import JraRace
from train_jra_cell_models import TableLike


@pytest.fixture
def offline_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
    races = (
        JraRace("eval", date(2024, 9, 1), "06", 1200, "11", "703", "", "", "", "", ("H1", "H2")),
    )

    class OfflineDataset:
        schema: pa.Schema = pa.schema([("feature", pa.float64())])

        def to_table(self, *, columns: list[str], filter: object) -> TableLike:
            raise AssertionError("Fold evaluation is mocked")

    def load_races(*_args: object) -> tuple[JraRace, ...]:
        return races

    def load_dataset(*_args: object, **_kwargs: object) -> OfflineDataset:
        return OfflineDataset()

    def evaluate_fold(*_args: object) -> dict[str, object]:
        return {"evaluation_year": 2024, "status": "no-evaluation-races", "metrics": None}

    declarations = tmp_path / "declarations.json"
    outcomes = tmp_path / "outcomes.json"
    declarations.write_text(
        '{"version":"independent-declarations-v1","races":[]}',
        encoding="utf-8",
    )
    outcomes.write_text('{"version":"runner-outcomes-v1","races":[]}', encoding="utf-8")
    monkeypatch.setattr(evaluation, "load_races", load_races)
    monkeypatch.setattr(evaluation.ds, "dataset", load_dataset)
    monkeypatch.setattr(evaluation, "evaluate_fold", evaluate_fold)
    return [
        "--pg-url",
        "postgresql://unused",
        "--features-root",
        str(tmp_path),
        "--output-dir",
        str(tmp_path),
        "--teacher-declarations",
        str(declarations),
        "--teacher-outcomes",
        str(outcomes),
        "--teacher-declarations-sha256",
        hashlib.sha256(declarations.read_bytes()).hexdigest(),
        "--teacher-outcomes-sha256",
        hashlib.sha256(outcomes.read_bytes()).hexdigest(),
    ]


def test_main_writes_versioned_checkpoint_and_resumes_without_fitting(
    offline_main: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert evaluation.main(offline_main) == 0
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["version"] == "jra-cell-walk-forward-v8"
    assert report["cells"][0]["version"] == "jra-cell-walk-forward-v8"
    assert report["legacy_metrics_definition"] == "winner-in-predicted-top-K"
    assert report["exact_position_metrics_location"] == "cells[].folds[].exact_position_metrics"
    assert report["exact_position_aggregate_location"] == "aggregate.exact_positions"
    assert report["aggregate"]["status_counts"] == {"no-evaluation-races": 1}

    def forbidden_evaluation(*_args: object) -> None:
        raise AssertionError("Compatible checkpoint should be reused")

    monkeypatch.setattr(evaluation, "evaluate_cell", forbidden_evaluation)
    assert evaluation.main([*offline_main, "--resume"]) == 0


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"folds": []},
        {"version": "jra-cell-walk-forward-v2", "folds": []},
        {"version": "jra-cell-walk-forward-v3", "folds": []},
        {"version": "jra-cell-walk-forward-v4", "folds": []},
        {"version": "jra-cell-walk-forward-v5", "folds": []},
        {"version": "jra-cell-walk-forward-v6", "folds": []},
        {"version": "jra-cell-walk-forward-v7", "folds": []},
        {"version": "jra-cell-walk-forward-v8", "folds": []},
    ],
)
def test_main_rejects_old_checkpoint_without_overwriting(
    offline_main: list[str], tmp_path: Path, payload: object
) -> None:
    assert evaluation.main(offline_main) == 0
    checkpoint = next((tmp_path / "cells").glob("*.json"))
    body = json.dumps(payload)
    checkpoint.write_text(body, encoding="utf-8")
    with pytest.raises(ValueError, match="checkpoint evaluation contract is incompatible"):
        evaluation.main([*offline_main, "--resume"])
    assert checkpoint.read_text(encoding="utf-8") == body


def test_source_changed_checkpoint_is_not_reused_or_overwritten(
    offline_main: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert evaluation.main(offline_main) == 0
    checkpoint = next((tmp_path / "cells").glob("*.json"))
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    payload["teacher_evidence"]["declared_counts_sha256"] = "0" * 64
    body = json.dumps(payload)
    checkpoint.write_text(body, encoding="utf-8")

    def forbid_evaluation(*_args: object) -> None:
        raise AssertionError("An incompatible cache must not silently trigger a fit")

    monkeypatch.setattr(evaluation, "evaluate_cell", forbid_evaluation)
    with pytest.raises(ValueError, match="checkpoint evaluation contract is incompatible"):
        evaluation.main([*offline_main, "--resume"])
    assert checkpoint.read_text(encoding="utf-8") == body


@pytest.mark.parametrize("version", ["jra-cell-walk-forward-v6", "jra-cell-walk-forward-v7"])
def test_matching_source_old_cache_still_lacks_target_status_contract(
    offline_main: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    version: str,
) -> None:
    assert evaluation.main(offline_main) == 0
    checkpoint = next((tmp_path / "cells").glob("*.json"))
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    payload["version"] = version
    body = json.dumps(payload)
    checkpoint.write_text(body, encoding="utf-8")

    def forbidden(*_args: object) -> None:
        raise AssertionError("Old target contract must not refit")

    monkeypatch.setattr(evaluation, "evaluate_cell", forbidden)
    with pytest.raises(ValueError, match="checkpoint evaluation contract is incompatible"):
        evaluation.main([*offline_main, "--resume"])
    assert checkpoint.read_text(encoding="utf-8") == body


def test_main_target_plan_and_max_cells(offline_main: list[str], tmp_path: Path) -> None:
    assert evaluation.main(offline_main) == 0
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    plan = tmp_path / "plan.json"
    plan.write_text(
        json.dumps({"cells": [{"canonical": report["cells"][0]["canonical"]}]}), encoding="utf-8"
    )
    assert evaluation.main([*offline_main, "--target-plan", str(plan), "--max-cells", "1"]) == 0


def test_main_rejects_unobserved_target(offline_main: list[str], tmp_path: Path) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text('{"cells":[{"canonical":"absent"}]}', encoding="utf-8")
    with pytest.raises(ValueError, match="target plan cells are absent"):
        evaluation.main([*offline_main, "--target-plan", str(plan)])


def test_main_empty_shard(offline_main: list[str], tmp_path: Path) -> None:
    assert evaluation.main([*offline_main, "--shard-count", "2", "--shard-index", "1"]) == 0
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["cell_count"] == 0
    assert report["aggregate"]["overall"]["top1_accuracy"] == 0.0
    assert report["aggregate"]["exact_positions"] == {
        "overall": {
            "model": {
                "race_count": 0,
                "support": [0, 0, 0, 0, 0],
                "hits": [0, 0, 0, 0, 0],
                "accuracy": [None, None, None, None, None],
            },
            "market": {
                "race_count": 0,
                "support": [0, 0, 0, 0, 0],
                "hits": [0, 0, 0, 0, 0],
                "accuracy": [None, None, None, None, None],
            },
            "delta_hits": [0, 0, 0, 0, 0],
        },
        "by_year": {},
    }


def test_main_rejects_invalid_schema(
    offline_main: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def invalid_dataset(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(schema=None)

    monkeypatch.setattr(evaluation.ds, "dataset", invalid_dataset)
    with pytest.raises(TypeError, match="feature dataset schema is unavailable"):
        evaluation.main(offline_main)


def test_main_rejects_empty_numeric_schema(
    offline_main: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def empty_dataset(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(schema=pa.schema([]))

    monkeypatch.setattr(evaluation.ds, "dataset", empty_dataset)
    with pytest.raises(ValueError, match="no numeric model features"):
        evaluation.main(offline_main)


def test_parse_requires_database_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL_LOCAL", raising=False)
    with pytest.raises(SystemExit):
        evaluation.parse_args(
            [
                "--features-root",
                str(tmp_path),
                "--output-dir",
                str(tmp_path),
                "--teacher-declarations",
                "declarations.json",
                "--teacher-outcomes",
                "outcomes.json",
                "--teacher-declarations-sha256",
                "a" * 64,
                "--teacher-outcomes-sha256",
                "b" * 64,
            ]
        )
