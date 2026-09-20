"""Optimizer orchestration and source-bound caches; no native model work."""

import json
from argparse import Namespace
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

import optimize_priority_jra_cells as optimization
from predict_lib.jra_cell_scope import (
    JraCellKey,
    JraFoldScope,
    JraRace,
    JraRaceIndex,
    cell_for_race,
)
from predict_lib.teacher_catalog import TeacherCatalog, require_teacher_catalog
from predict_lib.training_roster_match import EvidenceReferences
from train_jra_cell_models import DatasetLike, TableLike


class UnusedDataset:
    schema: pa.Schema = pa.schema([("feature", pa.float64())])

    def to_table(self, *, columns: list[str], filter: object) -> TableLike:
        raise AssertionError("Optimizer evidence tests must not read features or fit models")


@dataclass(frozen=True)
class CandidateCase:
    index: JraRaceIndex
    cell: JraCellKey
    races: tuple[JraRace, ...]
    target: JraRace
    args: Namespace


@pytest.fixture
def case(tmp_path: Path) -> CandidateCase:
    target = JraRace(
        "eval",
        date(2025, 9, 5),
        "06",
        1600,
        "18",
        "999",
        "C",
        "priority",
        "18",
        "3",
        ("H1", "H2"),
    )
    history = tuple(
        replace(target, race_id=f"t{index}", race_date=date(2023, 1, 1), venue="09")
        for index in range(100)
    )
    races = (*history, target)
    return CandidateCase(
        JraRaceIndex(races),
        cell_for_race(target),
        races,
        target,
        Namespace(
            target_date=date(2026, 9, 6),
            iterations=2,
            relevance_mode="top3",
            thread_count=1,
            input_revision="test-only",
            features_root=tmp_path,
            checkpoint_dir=tmp_path / "cache",
            resume=False,
            teacher_catalog=TeacherCatalog(EvidenceReferences("a" * 64, "b" * 64), {}),
        ),
    )


@pytest.fixture
def scored_calls(monkeypatch: pytest.MonkeyPatch) -> list[Namespace]:
    calls: list[Namespace] = []

    def evaluate(
        _dataset: DatasetLike,
        _features: Sequence[str],
        scope: JraFoldScope,
        args: Namespace,
        *,
        include_predictions: bool = False,
    ) -> dict[str, object]:
        catalog = require_teacher_catalog(args)
        calls.append(args)
        result: dict[str, object] = {
            "version": "jra-cell-walk-forward-v8",
            "teacher_evidence": asdict(catalog.references),
            "status": "evaluated",
            "evaluation_year": scope.evaluation_year,
            "evaluation_race_count": 1,
            "metrics": {
                "race_count": 1,
                "top1_hits": 1,
                "top2_hits": 1,
                "top3_hits": 1,
                "top4_hits": 1,
                "top5_hits": 1,
            },
            "market_baseline_metrics": {
                "race_count": 1,
                "top1_hits": 1,
                "top2_hits": 1,
                "top3_hits": 1,
                "top4_hits": 1,
                "top5_hits": 1,
            },
            "exact_position_metrics": {
                "race_count": 1,
                "support": [1, 1, 0, 0, 0],
                "hits": [1, 1, 0, 0, 0],
            },
            "market_exact_position_metrics": {
                "race_count": 1,
                "support": [1, 1, 0, 0, 0],
                "hits": [1, 1, 0, 0, 0],
            },
        }
        if include_predictions:
            result["predictions"] = []
        return result

    monkeypatch.setattr(optimization, "evaluate_fold", evaluate)
    return calls


def test_catalog_propagates_and_compatible_cache_reuses_without_refit(
    case: CandidateCase,
    scored_calls: list[Namespace],
) -> None:
    result = optimization.evaluate_candidate(
        UnusedDataset(),
        ["feature"],
        case.index,
        case.cell,
        case.races,
        [case.target],
        [2024, 2025],
        "entrant-history",
        6,
        0.05,
        case.args,
    )
    assert result["all_requested_years_evaluated"] is False
    assert result["annual_top2_top5_guard_passed"] is True
    assert len(scored_calls) == 1
    assert require_teacher_catalog(scored_calls[0]) is require_teacher_catalog(case.args)
    case.args.resume = True
    resumed = optimization.evaluate_candidate(
        UnusedDataset(),
        ["feature"],
        case.index,
        case.cell,
        case.races,
        [case.target],
        [2025],
        "entrant-history",
        6,
        0.05,
        case.args,
    )
    assert resumed["all_requested_years_evaluated"] is True
    assert len(scored_calls) == 1


def test_compatible_prediction_request_fills_missing_predictions_then_reuses(
    case: CandidateCase,
    scored_calls: list[Namespace],
) -> None:
    optimization.evaluate_candidate(
        UnusedDataset(),
        ["feature"],
        case.index,
        case.cell,
        case.races,
        [],
        [2025],
        "entrant-history",
        6,
        0.05,
        case.args,
    )
    case.args.resume = True
    optimization.evaluate_candidate(
        UnusedDataset(),
        ["feature"],
        case.index,
        case.cell,
        case.races,
        [],
        [2025],
        "entrant-history",
        6,
        0.05,
        case.args,
        include_predictions=True,
    )
    optimization.evaluate_candidate(
        UnusedDataset(),
        ["feature"],
        case.index,
        case.cell,
        case.races,
        [],
        [2025],
        "entrant-history",
        6,
        0.05,
        case.args,
        include_predictions=True,
    )
    assert len(scored_calls) == 2
    path = optimization.build_checkpoint_path(
        case.args.checkpoint_dir,
        case.cell,
        "entrant-history__depth-6__lr-0.05",
        2025,
        args=case.args,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["evaluation_scope_mode"] == "related-course-evaluation-proxy"
    assert payload["evaluation_proxy_race_count"] == 1
    assert payload["predictions"] == []


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"version": "jra-cell-walk-forward-v5"},
        {"version": "jra-cell-walk-forward-v6", "teacher_evidence": {}},
        {"version": "jra-cell-walk-forward-v7", "teacher_evidence": {}},
        {"version": "jra-cell-walk-forward-v8", "teacher_evidence": {}},
        {
            "version": "jra-cell-walk-forward-v8",
            "teacher_evidence": {
                "declared_counts_sha256": "c" * 64,
                "runner_outcomes_sha256": "b" * 64,
            },
        },
        {
            "version": "jra-cell-walk-forward-v8",
            "teacher_evidence": {
                "declared_counts_sha256": "a" * 64,
                "runner_outcomes_sha256": "c" * 64,
            },
        },
    ],
)
def test_incompatible_cached_fold_is_not_overwritten_or_refitted(
    case: CandidateCase,
    scored_calls: list[Namespace],
    payload: object,
) -> None:
    path = optimization.build_checkpoint_path(
        case.args.checkpoint_dir,
        case.cell,
        "entrant-history__depth-6__lr-0.05",
        2025,
        args=case.args,
    )
    path.parent.mkdir(parents=True)
    body = json.dumps(payload)
    path.write_text(body, encoding="utf-8")
    case.args.resume = True
    with pytest.raises(ValueError, match="checkpoint evaluation contract is incompatible"):
        optimization.evaluate_candidate(
            UnusedDataset(),
            ["feature"],
            case.index,
            case.cell,
            case.races,
            [case.target],
            [2025],
            "entrant-history",
            6,
            0.05,
            case.args,
            include_predictions=True,
        )
    assert scored_calls == []
    assert path.read_text(encoding="utf-8") == body


def test_catalog_required_even_for_an_empty_candidate_year_set(case: CandidateCase) -> None:
    with pytest.raises(ValueError, match="explicit teacher catalog argument"):
        optimization.evaluate_candidate(
            UnusedDataset(),
            ["feature"],
            case.index,
            case.cell,
            case.races,
            [],
            [],
            "entrant-history",
            6,
            0.05,
            Namespace(),
        )
