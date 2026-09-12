from __future__ import annotations

import json
from argparse import Namespace
from dataclasses import replace
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

import optimize_priority_jra_cells as optimization
from optimize_priority_jra_cells import (
    PRIORITY_RACES,
    build_checkpoint_path,
    evaluation_races_for_year,
    parse_args,
    parse_priority_races,
    priority_races_from_plan,
    resolve_priority_cells,
    scope_for_strategy,
    select_development_candidate,
)
from predict_lib.jra_cell_scope import JraRace, JraRaceIndex, cell_for_race


def race(race_id: str, venue: str = "06", distance: int = 1600) -> JraRace:
    return JraRace(
        race_id=race_id,
        race_date=date(2026, 9, 5),
        venue=venue,
        distance=distance,
        track_code="18",
        condition_code="999",
        grade_code="C",
        race_name="priority",
        race_type_code="18",
        weight_type_code="3",
        horse_ids=("H1", "H2"),
    )


def candidate(top1: int, *, guard: bool) -> dict[str, object]:
    return {
        "candidate_key": f"c-{top1}",
        "annual_top2_top5_guard_passed": guard,
        "aggregate": {
            "market_delta_hits": {
                "top1": top1,
                "top2": 0,
                "top3": 0,
                "top4": 0,
                "top5": 0,
            }
        },
    }


def test_resolve_priority_cells_requires_all_four_races() -> None:
    races = [
        race(PRIORITY_RACES["nakayama-1r-obstacle-open"], distance=3200),
        race(PRIORITY_RACES["nakayama-11r-keisei-hai-ah"]),
        race(PRIORITY_RACES["hanshin-11r-enif-stakes"], venue="09", distance=1400),
        race(PRIORITY_RACES["sapporo-11r-sapporo-2yo-stakes"], venue="01", distance=1800),
    ]
    assert set(resolve_priority_cells(races)) == set(PRIORITY_RACES)
    with pytest.raises(ValueError, match="priority JRA races are missing"):
        resolve_priority_cells(races[:-1])


def test_evaluation_uses_exact_history_then_related_course_proxy() -> None:
    target = race("target")
    cell = cell_for_race(target)
    exact = replace(target, race_id="exact", race_date=date(2023, 9, 5))
    proxy = replace(
        target,
        race_id="proxy",
        race_date=date(2024, 6, 1),
        distance=1800,
        condition_code="005",
        race_name="proxy",
    )
    exact_rows, exact_mode = evaluation_races_for_year(
        [exact, proxy, target], [exact, target], cell, 2023
    )
    assert [item.race_id for item in exact_rows] == ["exact"]
    assert exact_mode == "exact-cell"
    proxy_rows, proxy_mode = evaluation_races_for_year(
        [exact, proxy, target], [exact, target], cell, 2024
    )
    assert [item.race_id for item in proxy_rows] == ["proxy"]
    assert proxy_mode == "related-course-evaluation-proxy"


def test_scope_strategy_rejects_unknown_strategy() -> None:
    target = race("target")
    index = JraRaceIndex([target])
    with pytest.raises(ValueError, match="unknown JRA scope strategy"):
        scope_for_strategy(
            index,
            cell_for_race(target),
            date(2026, 9, 5),
            [target],
            "same-cell-forbidden",
        )


def test_development_selection_requires_positive_top1_and_annual_guard() -> None:
    selected, eligible = select_development_candidate(
        [candidate(5, guard=False), candidate(2, guard=True), candidate(-1, guard=True)]
    )
    assert selected["candidate_key"] == "c-2"
    assert eligible is True
    fallback, fallback_eligible = select_development_candidate(
        [candidate(0, guard=True), candidate(-1, guard=True)]
    )
    assert fallback["candidate_key"] == "c-0"
    assert fallback_eligible is False
    with pytest.raises(ValueError, match="no evaluated candidates"):
        select_development_candidate([])
    incomplete = candidate(10, guard=True)
    incomplete["all_requested_years_evaluated"] = False
    with pytest.raises(ValueError, match="no complete development candidates"):
        select_development_candidate([incomplete])


def test_candidate_checkpoint_records_independent_cell_training_scope(tmp_path: Path) -> None:
    target = replace(race("target"), race_date=date(2026, 9, 5), horse_ids=("NEW",))
    historical = replace(
        target, race_id="historical", race_date=date(2025, 9, 5), horse_ids=("PAST",)
    )
    cross = replace(historical, race_id="cross", race_date=date(2024, 5, 1), venue="09")
    races = [historical, cross, target]

    class UnusedDataset:
        schema: pa.Schema = pa.schema([("feature", pa.float64())])

        def to_table(self, *, columns: list[str], filter: object) -> pa.Table:
            raise AssertionError("insufficient scope must not load training data")

    args = Namespace(
        target_date=date(2026, 9, 6),
        iterations=100,
        relevance_mode="top3",
        thread_count=1,
        input_revision="snapshot",
        features_root=tmp_path,
        checkpoint_dir=tmp_path / "checkpoints",
        resume=False,
    )
    optimization.evaluate_candidate(
        UnusedDataset(),
        ["feature"],
        JraRaceIndex(races),
        cell_for_race(target),
        races,
        [target],
        [2026],
        "entrant-history",
        6,
        0.05,
        args,
    )
    checkpoint = build_checkpoint_path(
        args.checkpoint_dir,
        cell_for_race(target),
        "entrant-history__depth-6__lr-0.05",
        2026,
        args=args,
    )
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert payload["status"] == "insufficient-diverse-training-scope"
    assert payload["training_scope"]["cutoff"] == "2026-01-01"
    assert payload["training_scope"]["history_start"] == "2006-01-01"
    assert payload["training_scope"]["cell_history_seed_race_ids"] == ["historical"]
    assert payload["training_scope"]["seed_horse_ids"] == ["NEW", "PAST"]
    assert payload["training_scope"]["training_race_ids"] == ["cross", "historical"]
    assert payload["training_scope"]["evaluation_race_ids"] == ["target"]


def test_new_date_requires_explicit_priority_races() -> None:
    with pytest.raises(ValueError, match="explicit --priority-race"):
        parse_priority_races(None, target_date=date(2026, 9, 6))
    assert parse_priority_races(
        ["shion=jra:2026:0906:06:11", "centaur=jra:2026:0906:09:11"],
        target_date=date(2026, 9, 6),
    ) == {"shion": "jra:2026:0906:06:11", "centaur": "jra:2026:0906:09:11"}


@pytest.mark.parametrize(
    "values",
    [
        [],
        ["bad"],
        ["=jra:2026:0906:06:11"],
        ["shion=jra:2026:0905:06:11"],
        ["shion=jra:2026:0906:11:11"],
        ["shion=jra:2026:0906:06:13"],
        ["a=jra:2026:0906:06:11", "a=jra:2026:0906:09:11"],
        ["a=jra:2026:0906:06:11", "b=jra:2026:0906:06:11"],
    ],
)
def test_priority_race_validation(values: list[str]) -> None:
    with pytest.raises(ValueError, match="priority race"):
        parse_priority_races(values, target_date=date(2026, 9, 6))


def test_explicit_cell_resolution() -> None:
    target = replace(race("jra:2026:0906:06:11"), race_date=date(2026, 9, 6))
    cells = resolve_priority_cells([target], priority_races={"shion": "jra:2026:0906:06:11"})
    assert list(cells) == ["shion"]


def test_target_day_is_excluded_from_evaluation() -> None:
    target = replace(race("target"), race_date=date(2026, 9, 6))
    past = replace(target, race_id="past", race_date=date(2026, 9, 5))
    rows, mode = evaluation_races_for_year(
        [past, target],
        [past, target],
        cell_for_race(target),
        2026,
        target_date=date(2026, 9, 6),
    )
    assert [item.race_id for item in rows] == ["past"]
    assert mode == "exact-cell"


@pytest.mark.parametrize(
    "change",
    [
        {"iterations": 250},
        {"target_date": date(2026, 9, 6)},
        {"input_revision": "snapshot-b"},
        {"relevance_mode": "reciprocal-rank"},
        {"thread_count": 2},
        {"features_root": Path("other-features")},
    ],
)
def test_checkpoint_isolated_by_training_and_input_contract(
    tmp_path: Path,
    change: dict[str, object],
) -> None:
    settings = {
        "iterations": 100,
        "target_date": date(2026, 9, 5),
        "input_revision": "snapshot-a",
        "thread_count": 6,
        "features_root": tmp_path / "features",
    }
    cell = cell_for_race(race("target"))
    before = build_checkpoint_path(tmp_path, cell, "candidate", 2025, args=Namespace(**settings))
    after = build_checkpoint_path(
        tmp_path, cell, "candidate", 2025, args=Namespace(**(settings | change))
    )
    assert before != after


def test_priority_races_from_plan_selects_one_representative_per_cell(
    tmp_path: Path,
) -> None:
    plan = tmp_path / "production.json"
    plan.write_text(
        json.dumps(
            {
                "target_date": "2026-09-12",
                "cells": [
                    {
                        "cell_id": "jra-cell-a",
                        "target_race_ids": [
                            "jra:2026:0912:06:01",
                            "jra:2026:0912:06:07",
                        ],
                    },
                    {
                        "cell_id": "jra-cell-b",
                        "target_race_ids": ["jra:2026:0912:09:11"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    assert priority_races_from_plan(plan, target_date=date(2026, 9, 12)) == {
        "jra-cell-a": "jra:2026:0912:06:01",
        "jra-cell-b": "jra:2026:0912:09:11",
    }


def test_cli_rejects_production_plan_with_explicit_priority_race(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--pg-url",
                "postgresql://localhost/test",
                "--features-root",
                "features",
                "--output",
                "report.json",
                "--checkpoint-dir",
                "checkpoints",
                "--input-revision",
                "snapshot-a",
                "--production-plan",
                str(tmp_path / "production.json"),
                "--priority-race",
                "race=jra:2026:0905:06:11",
            ]
        )


def test_cli_accepts_dated_target_and_provenance() -> None:
    args = parse_args(
        [
            "--pg-url",
            "postgresql://localhost/test",
            "--features-root",
            "features",
            "--output",
            "report.json",
            "--checkpoint-dir",
            "checkpoints",
            "--input-revision",
            "snapshot-a",
            "--target-date",
            "2026-09-06",
            "--priority-race",
            "shion=jra:2026:0906:06:11",
        ]
    )
    assert args.target_date == date(2026, 9, 6)
    assert args.priority_races == {"shion": "jra:2026:0906:06:11"}
    assert args.relevance_mode == "top3"
    assert args.development_only is False
    assert args.development_years == (2020, 2021, 2022, 2023)


@pytest.mark.parametrize("bounds", [(2024, 2025), (2023, 2020), (1999, 2023)])
def test_cli_keeps_expanded_development_strictly_before_holdout(
    bounds: tuple[int, int],
) -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--pg-url",
                "postgresql://localhost/test",
                "--features-root",
                "features",
                "--output",
                "report.json",
                "--checkpoint-dir",
                "checkpoints",
                "--input-revision",
                "snapshot-a",
                "--development-year-from",
                str(bounds[0]),
                "--development-year-to",
                str(bounds[1]),
            ]
        )


def test_cli_accepts_longer_past_only_development_window() -> None:
    args = parse_args(
        [
            "--pg-url",
            "postgresql://localhost/test",
            "--features-root",
            "features",
            "--output",
            "report.json",
            "--checkpoint-dir",
            "checkpoints",
            "--input-revision",
            "snapshot-a",
            "--development-year-from",
            "2006",
            "--development-year-to",
            "2023",
        ]
    )
    assert args.development_years == (
        2006,
        2007,
        2008,
        2009,
        2010,
        2011,
        2012,
        2013,
        2014,
        2015,
        2016,
        2017,
        2018,
        2019,
        2020,
        2021,
        2022,
        2023,
    )


@pytest.mark.parametrize("complete", [True, False])
@pytest.mark.parametrize("development_year_from", [2006, 2020])
def test_development_only_does_not_evaluate_holdout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    complete: bool,
    development_year_from: int,
) -> None:
    target = replace(race("jra:2026:0906:06:11"), race_date=date(2026, 9, 6))
    historical = replace(target, race_id="jra:2008:0906:06:11", race_date=date(2008, 9, 6))
    requested_years: list[object] = []
    exact_cohort_sizes: list[int] = []

    class EmptyDataset:
        schema: pa.Schema = pa.schema([("feature", pa.float64())])

    def evaluate(*positional: object, **_keywords: object) -> dict[str, object]:
        requested_years.append(positional[6])
        exact_races = positional[5]
        assert isinstance(exact_races, tuple)
        exact_cohort_sizes.append(len(exact_races))
        result = candidate(1, guard=True)
        result["all_requested_years_evaluated"] = complete
        return result

    monkeypatch.setattr(optimization, "load_races", lambda *_args: [historical, target])
    monkeypatch.setattr(optimization.ds, "dataset", lambda *_args, **_kwargs: EmptyDataset())
    monkeypatch.setattr(optimization, "evaluate_candidate", evaluate)
    output = tmp_path / "report.json"
    assert (
        optimization.main(
            [
                "--pg-url",
                "postgresql://localhost/test",
                "--features-root",
                str(tmp_path),
                "--output",
                str(output),
                "--checkpoint-dir",
                str(tmp_path / "checkpoints"),
                "--input-revision",
                "snapshot-a",
                "--target-date",
                "2026-09-06",
                "--priority-race",
                "shion=jra:2026:0906:06:11",
                "--development-only",
                "--development-year-from",
                str(development_year_from),
                "--relevance-mode",
                "reciprocal-rank",
            ]
        )
        == 0
    )
    report = json.loads(output.read_text(encoding="utf-8"))
    assert len(requested_years) == 6
    if development_year_from == 2006:
        assert set(requested_years) == {
            (
                2006,
                2007,
                2008,
                2009,
                2010,
                2011,
                2012,
                2013,
                2014,
                2015,
                2016,
                2017,
                2018,
                2019,
                2020,
                2021,
                2022,
                2023,
            )
        }
        assert exact_cohort_sizes == [2, 2, 2, 2, 2, 2]
    else:
        assert set(requested_years) == {(2020, 2021, 2022, 2023)}
        assert exact_cohort_sizes == [1, 1, 1, 1, 1, 1]
    assert report["holdout_years"] == []
    assert report["relevance_mode"] == "reciprocal-rank"
    assert report["all_priority_cells_production_eligible"] is False
    if complete:
        assert (
            report["cells"][0]["production_eligibility_reason"]
            == "development-only-no-holdout-evaluated"
        )
    else:
        assert (
            report["cells"][0]["production_eligibility_reason"]
            == "no-complete-development-candidates"
        )
        assert report["cells"][0]["selected_candidate_key"] is None
        assert report["cells"][0]["development_eligible"] is False
