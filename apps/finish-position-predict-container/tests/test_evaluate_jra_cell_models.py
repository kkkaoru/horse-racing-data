from __future__ import annotations

import json
from argparse import Namespace
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Protocol, cast, override

import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from catboost import Pool

import evaluate_jra_cell_models as evaluation
from predict_lib.jra_cell_scope import JraCellKey, JraFoldScope
from predict_lib.teacher_catalog import TeacherCatalog
from predict_lib.training_admission import RunnerOutcome
from predict_lib.training_roster_match import EvidenceReferences, TeacherRaceEvidence
from train_jra_cell_models import DatasetLike, TableLike


def scope() -> JraFoldScope:
    return JraFoldScope(
        cell=JraCellKey("06", 1200, "autumn", "turf", "703", None),
        evaluation_year=2024,
        cutoff=date(2024, 1, 1),
        history_start=date(2004, 1, 1),
        seed_race_ids=("eval",),
        seed_horse_ids=("H1", "H2"),
        training_race_ids=tuple(f"t{index}" for index in range(100)),
        training_horse_rows=200,
        evaluation_race_ids=("eval",),
        evaluation_horse_rows=2,
        training_scope_mode="target-entrant-history",
        related_seed_race_count=0,
        training_target_cell_race_count=1,
        training_cross_cell_race_count=99,
    )


def frame(race_ids: tuple[str, ...], race_date: str, *, complete: bool = True) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for race_id in race_ids:
        rows.extend(
            [
                {
                    "race_id": race_id,
                    "race_date": race_date,
                    "ketto_toroku_bango": f"{race_id}-H1",
                    "umaban": 1,
                    "finish_position": 1 if complete else None,
                    "feature": 1.0,
                    "odds_score": 0.1,
                },
                {
                    "race_id": race_id,
                    "race_date": race_date,
                    "ketto_toroku_bango": f"{race_id}-H2",
                    "umaban": 2,
                    "finish_position": 2 if complete else None,
                    "feature": 0.0,
                    "odds_score": 0.9,
                },
            ]
        )
    return pd.DataFrame(rows)


class PoolLike(Protocol):
    def num_row(self) -> int: ...


class FakeRanker:
    def __init__(self, **_kwargs: object) -> None:
        pass

    def fit(self, _pool: object) -> None:
        pass

    def predict(self, pool: object) -> np.ndarray:
        row_count = cast(PoolLike, pool).num_row()
        return np.resize(np.array([1.0, 0.0]), row_count)


def args() -> Namespace:
    catalog = TeacherCatalog(
        EvidenceReferences("a" * 64, "b" * 64),
        {
            f"t{index}": TeacherRaceEvidence(
                f"t{index}",
                2,
                (
                    RunnerOutcome(f"t{index}-H1", 1, "classified", 1),
                    RunnerOutcome(f"t{index}-H2", 2, "classified", 2),
                ),
            )
            for index in range(100)
        },
    )
    catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                2,
                (
                    RunnerOutcome("eval-H1", 1, "classified", 1),
                    RunnerOutcome("eval-H2", 2, "classified", 2),
                ),
            ),
        },
    )
    return Namespace(
        iterations=2,
        learning_rate=0.05,
        depth=2,
        thread_count=1,
        teacher_catalog=catalog,
    )


def test_fold_requires_teacher_catalog_before_reading_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_load(*_args: object) -> pd.DataFrame:
        raise AssertionError("Missing teacher evidence must stop before feature loading")

    monkeypatch.setattr(evaluation, "load_feature_rows", forbidden_load)
    with pytest.raises(ValueError, match="explicit teacher catalog argument"):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})),
            ["feature"],
            scope(),
            Namespace(),
        )


def test_fold_retains_explicit_nonfinish_teacher_in_the_actual_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical = frame(scope().training_race_ids, "20230101")
    historical["finish_position"] = historical["finish_position"].astype(object)
    historical.loc[1, "finish_position"] = None
    target = frame(("eval",), "20240101")
    parameters = args()
    original = parameters.teacher_catalog
    assert isinstance(original, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        original.references,
        {
            **original.races,
            "t0": TeacherRaceEvidence(
                "t0",
                2,
                (
                    RunnerOutcome("t0-H1", 1, "classified", 1),
                    RunnerOutcome("t0-H2", 2, "dnf", None),
                ),
            ),
        },
    )
    labels: list[float] = []

    class RecordingRanker(FakeRanker):
        @override
        def fit(self, _pool: object) -> None:
            if not isinstance(_pool, Pool):
                raise TypeError("expected CatBoost Pool")
            labels.extend(_pool.get_label().tolist()[:2])

    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return target if "eval" in race_ids else historical

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", RecordingRanker)
    result = evaluation.evaluate_fold(
        ds.dataset(pa.table({"unused": [1]})),
        ["feature", "odds_score"],
        scope(),
        parameters,
    )
    assert labels == [3.0, 0.0]
    assert result["training_runner_count"] == 200
    assert result["training_roster_admitted"] is True
    assert historical.loc[1, "finish_position"] is None


@pytest.mark.parametrize("finish", [None, 0, True])
def test_fold_refuses_unresolved_teacher_before_model_construction(
    monkeypatch: pytest.MonkeyPatch,
    finish: object,
) -> None:
    historical = frame(scope().training_race_ids, "20230101")
    historical["finish_position"] = historical["finish_position"].astype(object)
    historical.loc[0, "finish_position"] = finish
    target = frame(("eval",), "20240101")

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target if "eval" in race_ids else historical

    def forbidden_model(**_kwargs: object) -> None:
        raise AssertionError("Model construction must follow teacher label preflight")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden_model)
    with pytest.raises(ValueError, match="Feature finish conflicts"):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), args()
        )
    assert len(historical) == 200


@pytest.mark.parametrize("string_finishes", [False, True])
def test_fold_evaluation_is_pit_and_reports_model_vs_market(
    monkeypatch: pytest.MonkeyPatch,
    string_finishes: bool,
) -> None:
    historical = frame(scope().training_race_ids, "20230101")
    target = frame(("eval",), "20240101")
    if string_finishes:
        target["finish_position"] = pd.Series(["1.0", "2"], dtype=object)

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target.copy() if "eval" in race_ids else historical.copy()

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", FakeRanker)
    result = evaluation.evaluate_fold(
        cast(DatasetLike, object()),
        ["feature", "odds_score"],
        scope(),
        args(),
        include_predictions=True,
    )
    assert result["status"] == "evaluated"
    assert result["training_finish_contract"] == "supplied-roster-evidence-explicit-status-gains-v1"
    assert result["independent_training_roster_attested"] is False
    predictions = cast(list[dict[str, object]], result["predictions"])
    assert len(predictions) == 2
    assert predictions[0]["race_id"] == "eval"
    assert result["metrics"] == {
        "race_count": 1,
        "top1_hits": 1,
        "top1_accuracy": 1.0,
        "top2_hits": 1,
        "top2_accuracy": 1.0,
        "top3_hits": 1,
        "top3_accuracy": 1.0,
        "top4_hits": 1,
        "top4_accuracy": 1.0,
        "top5_hits": 1,
        "top5_accuracy": 1.0,
    }
    assert result["legacy_metrics_definition"] == "winner-in-predicted-top-K"
    assert result["exact_position_metrics"] == {
        "race_count": 1,
        "support": (1, 1, 0, 0, 0),
        "hits": (1, 1, 0, 0, 0),
        "accuracy": (1.0, 1.0, None, None, None),
    }
    assert result["market_exact_position_metrics"] == {
        "race_count": 1,
        "support": (1, 1, 0, 0, 0),
        "hits": (1, 1, 0, 0, 0),
        "accuracy": (1.0, 1.0, None, None, None),
    }
    assert result["market_delta_hits"] == {
        "top1": 0,
        "top2": 0,
        "top3": 0,
        "top4": 0,
        "top5": 0,
    }


def test_reciprocal_relevance_reaches_evaluation_training_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    labels: list[float] = []

    class RecordingRanker(FakeRanker):
        @override
        def fit(self, _pool: object) -> None:
            if not isinstance(_pool, Pool):
                raise TypeError("expected CatBoost Pool")
            labels.extend(_pool.get_label().tolist()[:2])

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return frame(race_ids, "20240101" if "eval" in race_ids else "20230101")

    monkeypatch.setattr(evaluation, "CatBoostRanker", RecordingRanker)
    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)

    class UnusedDataset:
        schema: pa.Schema = pa.schema([])

        def to_table(self, *, columns: list[str], filter: object) -> TableLike:
            raise AssertionError("feature loading is mocked")

    parameters = args()
    parameters.relevance_mode = "reciprocal-rank"
    result = evaluation.evaluate_fold(
        UnusedDataset(),
        ["feature", "odds_score"],
        scope(),
        parameters,
    )
    assert labels == [1, 0.5]
    assert result["relevance_mode"] == "reciprocal-rank"


def test_fold_evaluation_reports_insufficient_and_incomplete_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return frame(race_ids, "20230101" if "eval" not in race_ids else "20240101")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    too_small = replace(scope(), training_race_ids=("t1",))
    result = evaluation.evaluate_fold(
        cast(DatasetLike, object()), ["feature", "odds_score"], too_small, args()
    )
    assert result["status"] == "insufficient-training-races"

    target_cell_only = replace(
        scope(),
        training_target_cell_race_count=100,
        training_cross_cell_race_count=0,
    )
    result = evaluation.evaluate_fold(
        cast(DatasetLike, object()), ["feature", "odds_score"], target_cell_only, args()
    )
    assert result["status"] == "target-cell-only-training-forbidden"

    def load_unresolved(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return (
            frame(race_ids, "20240101", complete=False)
            if "eval" in race_ids
            else frame(race_ids, "20230101")
        )

    monkeypatch.setattr(evaluation, "load_feature_rows", load_unresolved)
    with pytest.raises(ValueError, match="Feature finish conflicts"):
        evaluation.evaluate_fold(
            cast(DatasetLike, object()), ["feature", "odds_score"], scope(), args()
        )


@pytest.mark.parametrize(
    "finish", [None, "bad", float("nan"), float("inf"), -float("inf"), 0, -1, 1.5]
)
def test_mixed_unresolved_evaluation_never_fits_or_drops_runners(
    monkeypatch: pytest.MonkeyPatch, finish: object
) -> None:
    historical = frame(scope().training_race_ids, "20230101")
    target = frame(("eval",), "20240101")
    target["finish_position"] = pd.Series([1, finish], dtype=object)
    original = target.copy(deep=True)

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target if "eval" in race_ids else historical

    def forbidden_ranker(**_kwargs: object) -> None:
        raise AssertionError("Unresolved evaluation must stop before model construction")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden_ranker)
    with pytest.raises(ValueError, match="Feature finish conflicts"):
        evaluation.evaluate_fold(
            cast(DatasetLike, object()),
            ["feature", "odds_score"],
            scope(),
            args(),
            include_predictions=True,
        )
    pd.testing.assert_frame_equal(target, original)


def test_empty_evaluation_is_distinct_from_unresolved_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical = frame(scope().training_race_ids, "20230101")
    target = pd.DataFrame(frame(("eval",), "20240101").iloc[:0])

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target if "eval" in race_ids else historical

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    result = evaluation.evaluate_fold(
        cast(DatasetLike, object()), ["feature", "odds_score"], scope(), args()
    )
    assert result["status"] == "no-evaluation-races"
    assert result["evaluation_runner_count"] == 0
    assert result["evaluation_unresolved_runner_count"] == 0
    assert result["metrics"] is None


@pytest.mark.parametrize(
    ("history_date", "target_date", "overlap", "message"),
    [
        ("20230101", "20240101", True, "training and evaluation races overlap"),
        ("20240101", "20240101", False, "training row is not before cutoff"),
        ("20230101", "20231231", False, "evaluation row is before cutoff"),
    ],
)
def test_fold_guards_stop_before_model_construction(
    monkeypatch: pytest.MonkeyPatch,
    history_date: str,
    target_date: str,
    overlap: bool,
    message: str,
) -> None:
    historical = frame(scope().training_race_ids, history_date)
    if overlap:
        historical.loc[historical.race_id.eq("t0"), "race_id"] = "eval"
    target = frame(("eval",), target_date)

    def forbidden_ranker(**_kwargs: object) -> None:
        raise AssertionError("Invalid fold must stop before model construction")

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target if race_ids == ("eval",) else historical

    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden_ranker)
    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    with pytest.raises(ValueError, match=message):
        evaluation.evaluate_fold(
            cast(DatasetLike, object()), ["feature", "odds_score"], scope(), args()
        )


def test_fold_rejects_nonfinite_model_scores(monkeypatch: pytest.MonkeyPatch) -> None:
    class NonfiniteRanker(FakeRanker):
        @override
        def predict(self, pool: object) -> npt.NDArray[np.float64]:
            return np.asarray(super().predict(pool), dtype=np.float64) * np.nan

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return frame(race_ids, "20240101" if race_ids == ("eval",) else "20230101")

    monkeypatch.setattr(evaluation, "CatBoostRanker", NonfiniteRanker)
    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    with pytest.raises(ValueError, match="non-finite score"):
        evaluation.evaluate_fold(
            cast(DatasetLike, object()), ["feature", "odds_score"], scope(), args()
        )


def test_aggregate_preserves_dead_heat_excluded_by_legacy_winner_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical = frame(scope().training_race_ids, "20230101")
    target = frame(("eval",), "20240101")
    target["finish_position"] = [1, 1]

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target.copy() if "eval" in race_ids else historical.copy()

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", FakeRanker)
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                2,
                (
                    RunnerOutcome("eval-H1", 1, "classified", 1),
                    RunnerOutcome("eval-H2", 2, "classified", 1),
                ),
            ),
        },
    )
    fold = evaluation.evaluate_fold(
        ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
    )
    assert fold["evaluation_race_count"] == 1
    legacy = fold["metrics"]
    assert isinstance(legacy, dict)
    assert legacy["race_count"] == 0
    report = evaluation.aggregate_evaluations([{"folds": [fold]}])
    assert report["legacy_population_definition"] == "races-with-exactly-one-recorded-winner"
    assert (
        report["exact_population_definition"]
        == "scored-race-fold-occurrences-with-position-specific-support"
    )
    assert report["exact_positions"] == {
        "overall": {
            "model": {
                "race_count": 1,
                "support": (1, 0, 0, 0, 0),
                "hits": (1, 0, 0, 0, 0),
                "accuracy": (1.0, None, None, None, None),
            },
            "market": {
                "race_count": 1,
                "support": (1, 0, 0, 0, 0),
                "hits": (1, 0, 0, 0, 0),
                "accuracy": (1.0, None, None, None, None),
            },
            "delta_hits": (0, 0, 0, 0, 0),
        },
        "by_year": {
            "2024": {
                "model": {
                    "race_count": 1,
                    "support": (1, 0, 0, 0, 0),
                    "hits": (1, 0, 0, 0, 0),
                    "accuracy": (1.0, None, None, None, None),
                },
                "market": {
                    "race_count": 1,
                    "support": (1, 0, 0, 0, 0),
                    "hits": (1, 0, 0, 0, 0),
                    "accuracy": (1.0, None, None, None, None),
                },
                "delta_hits": (0, 0, 0, 0, 0),
            }
        },
    }


def test_aggregate_reports_annual_and_overall_market_deltas() -> None:
    metrics = {
        "race_count": 2,
        "top1_hits": 1,
        "top2_hits": 2,
        "top3_hits": 2,
        "top4_hits": 2,
        "top5_hits": 2,
    }
    baseline = {**metrics, "top1_hits": 0, "top2_hits": 1}
    report = evaluation.aggregate_evaluations(
        [
            {
                "folds": [
                    {
                        "evaluation_year": 2024,
                        "status": "evaluated",
                        "metrics": metrics,
                        "evaluation_race_count": 2,
                        "market_baseline_metrics": baseline,
                        "exact_position_metrics": {
                            "race_count": 2,
                            "support": [2, 2, 1, 0, 0],
                            "hits": [1, 0, 1, 0, 0],
                        },
                        "market_exact_position_metrics": {
                            "race_count": 2,
                            "support": [2, 2, 1, 0, 0],
                            "hits": [0, 1, 0, 0, 0],
                        },
                    },
                    {
                        "evaluation_year": 2024,
                        "status": "evaluated",
                        "metrics": {**baseline, "top2_hits": 2},
                        "evaluation_race_count": 2,
                        "market_baseline_metrics": {**baseline, "top2_hits": 2},
                        "exact_position_metrics": {
                            "race_count": 2,
                            "support": [2, 0, 0, 0, 0],
                            "hits": [0, 0, 0, 0, 0],
                        },
                        "market_exact_position_metrics": {
                            "race_count": 2,
                            "support": [2, 0, 0, 0, 0],
                            "hits": [0, 0, 0, 0, 0],
                        },
                    },
                    {
                        "evaluation_year": 2025,
                        "status": "no-complete-evaluation-races",
                        "metrics": None,
                    },
                ]
            }
        ]
    )
    assert report["market_delta_hits"] == {
        "top1": 1,
        "top2": 1,
        "top3": 0,
        "top4": 0,
        "top5": 0,
    }
    by_year = cast(dict[str, dict[str, object]], report["by_year"])
    delta_hits = cast(dict[str, int], by_year["2024"]["delta_hits"])
    assert delta_hits["top1"] == 1
    assert report["status_counts"] == {"evaluated": 2, "no-complete-evaluation-races": 1}
    assert report["exact_positions"] == {
        "overall": {
            "model": {
                "race_count": 4,
                "support": (4, 2, 1, 0, 0),
                "hits": (1, 0, 1, 0, 0),
                "accuracy": (0.25, 0.0, 1.0, None, None),
            },
            "market": {
                "race_count": 4,
                "support": (4, 2, 1, 0, 0),
                "hits": (0, 1, 0, 0, 0),
                "accuracy": (0.0, 0.5, 0.0, None, None),
            },
            "delta_hits": (1, -1, 1, 0, 0),
        },
        "by_year": {
            "2024": {
                "model": {
                    "race_count": 4,
                    "support": (4, 2, 1, 0, 0),
                    "hits": (1, 0, 1, 0, 0),
                    "accuracy": (0.25, 0.0, 1.0, None, None),
                },
                "market": {
                    "race_count": 4,
                    "support": (4, 2, 1, 0, 0),
                    "hits": (0, 1, 0, 0, 0),
                    "accuracy": (0.0, 0.5, 0.0, None, None),
                },
                "delta_hits": (1, -1, 1, 0, 0),
            }
        },
    }


@pytest.mark.parametrize(
    ("model", "market", "evaluation_count"),
    [
        (None, None, 1),
        ({"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]}, None, 1),
        (
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            {"race_count": 2, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            2,
        ),
        (
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            {"race_count": 1, "support": [1, 1, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            1,
        ),
        (
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            2,
        ),
        (
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            None,
        ),
        (
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
            True,
        ),
    ],
)
def test_aggregate_rejects_missing_or_conflicting_exact_counts(
    model: object,
    market: object,
    evaluation_count: object,
) -> None:
    legacy = {
        "race_count": 0,
        "top1_hits": 0,
        "top2_hits": 0,
        "top3_hits": 0,
        "top4_hits": 0,
        "top5_hits": 0,
    }
    with pytest.raises(ValueError):
        evaluation.aggregate_evaluations(
            [
                {
                    "folds": [
                        {
                            "evaluation_year": 2024,
                            "status": "evaluated",
                            "metrics": legacy,
                            "evaluation_race_count": evaluation_count,
                            "market_baseline_metrics": legacy,
                            "exact_position_metrics": model,
                            "market_exact_position_metrics": market,
                        }
                    ]
                }
            ]
        )


@pytest.mark.parametrize(
    ("column", "values"),
    [
        ("ketto_toroku_bango", ["eval-H1", "intruder"]),
        ("ketto_toroku_bango", ["eval-H1", "eval-H1"]),
        ("umaban", [1, 3]),
        ("umaban", [1, 1]),
        ("finish_position", [1, 3]),
        ("finish_position", [True, 2]),
    ],
)
def test_equal_sized_wrong_target_roster_never_constructs_model(
    monkeypatch: pytest.MonkeyPatch,
    column: str,
    values: list[object],
) -> None:
    target = frame(("eval",), "20240101")
    target[column] = pd.Series(values, dtype=object)

    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return target.copy() if "eval" in race_ids else frame(race_ids, "20230101")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Invalid target must stop before model construction")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), args()
        )


@pytest.mark.parametrize(
    ("disposition", "finish"), [("classified", 2), ("dnf", None), ("dq", None)]
)
def test_missing_target_runner_never_scores_surviving_subset(
    monkeypatch: pytest.MonkeyPatch,
    disposition: str,
    finish: int | None,
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                2,
                (
                    RunnerOutcome("eval-H1", 1, "classified", 1),
                    RunnerOutcome("eval-H2", 2, disposition, finish),
                ),
            ),
        },
    )
    target = frame(("eval",), "20240101").iloc[:1].copy()

    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return target.copy() if "eval" in race_ids else frame(race_ids, "20230101")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Partial targets must stop before model construction")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
        )


def test_missing_whole_target_race_never_scores_surviving_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval2": TeacherRaceEvidence(
                "eval2",
                2,
                (
                    RunnerOutcome("eval2-H1", 1, "classified", 1),
                    RunnerOutcome("eval2-H2", 2, "classified", 2),
                ),
            ),
        },
    )
    requested = replace(scope(), evaluation_race_ids=("eval", "eval2"), evaluation_horse_rows=4)

    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return frame(("eval",), "20240101") if "eval" in race_ids else frame(race_ids, "20230101")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Missing requested race must stop before model construction")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], requested, parameters
        )


def test_missing_target_evidence_never_constructs_model(monkeypatch: pytest.MonkeyPatch) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    races = dict(catalog.races)
    races.pop("eval")
    parameters.teacher_catalog = TeacherCatalog(catalog.references, races)

    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return frame(race_ids, "20240101" if "eval" in race_ids else "20230101")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Missing target evidence must stop before model construction")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError, match="entire nonempty requested scope"):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
        )


@pytest.mark.parametrize("declared", [None, 3])
def test_target_counts_are_not_inferred_from_feature_rows(
    monkeypatch: pytest.MonkeyPatch,
    declared: int | None,
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": replace(catalog.races["eval"], declared_starters=declared),
        },
    )

    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return frame(race_ids, "20240101" if "eval" in race_ids else "20230101")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Invalid declared count must stop before model construction")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
        )


def test_target_pool_error_precedes_model_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return frame(race_ids, "20240101" if "eval" in race_ids else "20230101")

    def invalid_pool(*_args: object, **_kwargs: object) -> Pool:
        raise ValueError("Target Pool rejected")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Target Pool failure must not spend a fit")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "build_rank_pool", invalid_pool)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError, match="Target Pool rejected"):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), args()
        )


def test_classified_target_admission_retains_source_withdrawal_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                2,
                (
                    RunnerOutcome("eval-H1", 1, "classified", 1),
                    RunnerOutcome("eval-H2", 2, "classified", 2),
                    RunnerOutcome("eval-H3", 3, "withdrawn", None),
                ),
            ),
        },
    )

    def load_rows(
        _dataset: DatasetLike,
        race_ids: tuple[str, ...],
        _features: list[str],
    ) -> pd.DataFrame:
        return frame(race_ids, "20240101" if "eval" in race_ids else "20230101")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", FakeRanker)
    result = evaluation.evaluate_fold(
        ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
    )
    assert result["evaluation_roster_admitted"] is True
    assert result["independent_evaluation_roster_attested"] is False
    assert result["evaluation_runner_count"] == 2
    assert result["evaluation_source_withdrawn_runner_count"] == 1
    assert result["evaluation_feature_withdrawn_runner_count"] == 0
    assert result["exact_position_metrics"] == {
        "race_count": 1,
        "support": (1, 1, 0, 0, 0),
        "hits": (1, 1, 0, 0, 0),
        "accuracy": (1.0, 1.0, None, None, None),
    }


def test_explicit_dnf_dq_and_withdrawal_preserve_active_roster_and_json_nulls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                3,
                (
                    RunnerOutcome("eval-H1", 1, "classified", 1),
                    RunnerOutcome("eval-H2", 2, "dnf", None),
                    RunnerOutcome("eval-H3", 3, "dq", None),
                    RunnerOutcome("eval-H4", 4, "withdrawn", None),
                ),
            ),
        },
    )
    target = frame(("eval",), "20240101")
    target["finish_position"] = pd.Series([1, None], dtype=object)
    target.loc[1, "odds_score"] = float("nan")
    target = (
        pd.concat(
            [
                target,
                pd.DataFrame(
                    [
                        {
                            "race_id": "eval",
                            "race_date": "20240101",
                            "ketto_toroku_bango": "eval-H3",
                            "umaban": 3,
                            "finish_position": None,
                            "feature": 0.0,
                            "odds_score": 0.9,
                        },
                        {
                            "race_id": "eval",
                            "race_date": "20240101",
                            "ketto_toroku_bango": "eval-H4",
                            "umaban": 4,
                            "finish_position": None,
                            "feature": 0.0,
                            "odds_score": 0.9,
                        },
                    ]
                ),
            ],
            ignore_index=True,
        )
        .iloc[[1, 3, 0, 2]]
        .copy()
    )
    original = target.copy(deep=True)

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target if "eval" in race_ids else frame(race_ids, "20230101")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", FakeRanker)
    result = evaluation.evaluate_fold(
        ds.dataset(pa.table({"unused": [1]})),
        ["feature", "odds_score"],
        scope(),
        parameters,
        include_predictions=True,
    )
    assert result["evaluation_runner_count"] == 3
    assert result["evaluation_unresolved_runner_count"] == 0
    assert result["evaluation_dnf_runner_count"] == 1
    assert result["evaluation_dq_runner_count"] == 1
    assert result["evaluation_source_withdrawn_runner_count"] == 1
    assert result["evaluation_feature_withdrawn_runner_count"] == 1
    assert result["exact_position_metrics"] == {
        "race_count": 1,
        "support": (1, 0, 0, 0, 0),
        "hits": (1, 0, 0, 0, 0),
        "accuracy": (1.0, None, None, None, None),
    }
    decoded = json.loads(json.dumps(result, allow_nan=False))
    assert decoded["predictions"] == [
        {
            "race_date": "20240101",
            "race_id": "eval",
            "ketto_toroku_bango": "eval-H1",
            "umaban": 1,
            "finish_position": 1,
            "teacher_disposition": "classified",
            "odds_score": 0.1,
            "predicted_score": 1.0,
            "predicted_rank": 1,
        },
        {
            "race_date": "20240101",
            "race_id": "eval",
            "ketto_toroku_bango": "eval-H2",
            "umaban": 2,
            "finish_position": None,
            "teacher_disposition": "dnf",
            "odds_score": None,
            "predicted_score": 0.0,
            "predicted_rank": 3,
        },
        {
            "race_date": "20240101",
            "race_id": "eval",
            "ketto_toroku_bango": "eval-H3",
            "umaban": 3,
            "finish_position": None,
            "teacher_disposition": "dq",
            "odds_score": 0.9,
            "predicted_score": 1.0,
            "predicted_rank": 2,
        },
    ]
    pd.testing.assert_frame_equal(target, original)


def test_all_unranked_target_race_has_zero_support_not_fabricated_losses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                2,
                (
                    RunnerOutcome("eval-H1", 1, "dnf", None),
                    RunnerOutcome("eval-H2", 2, "dq", None),
                ),
            ),
        },
    )

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return (
            frame(race_ids, "20240101", complete=False)
            if "eval" in race_ids
            else frame(race_ids, "20230101")
        )

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", FakeRanker)
    result = evaluation.evaluate_fold(
        ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
    )
    assert result["evaluation_runner_count"] == 2
    assert result["evaluation_dnf_runner_count"] == 1
    assert result["evaluation_dq_runner_count"] == 1
    assert result["evaluation_unresolved_runner_count"] == 0
    assert result["exact_position_metrics"] == {
        "race_count": 1,
        "support": (0, 0, 0, 0, 0),
        "hits": (0, 0, 0, 0, 0),
        "accuracy": (None, None, None, None, None),
    }
    aggregate = evaluation.aggregate_evaluations([{"folds": [result]}])
    exact = aggregate["exact_positions"]
    assert isinstance(exact, dict)
    overall = exact["overall"]
    assert isinstance(overall, dict)
    assert overall["model"] == {
        "race_count": 1,
        "support": (0, 0, 0, 0, 0),
        "hits": (0, 0, 0, 0, 0),
        "accuracy": (None, None, None, None, None),
    }


@pytest.mark.parametrize("disposition", ["unknown_status", "unresolved_finish"])
def test_unknown_target_source_status_never_becomes_dnf(
    monkeypatch: pytest.MonkeyPatch,
    disposition: str,
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                2,
                (
                    RunnerOutcome("eval-H1", 1, "classified", 1),
                    RunnerOutcome("eval-H2", 2, disposition, None),
                ),
            ),
        },
    )

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return frame(race_ids, "20240101" if "eval" in race_ids else "20230101")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Unknown target status must stop before any model")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError, match="roster defects"):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
        )


@pytest.mark.parametrize("finish", [0, 2])
def test_dnf_target_must_not_carry_numeric_feature_finish(
    monkeypatch: pytest.MonkeyPatch, finish: int
) -> None:
    parameters = args()
    catalog = parameters.teacher_catalog
    assert isinstance(catalog, TeacherCatalog)
    parameters.teacher_catalog = TeacherCatalog(
        catalog.references,
        {
            **catalog.races,
            "eval": TeacherRaceEvidence(
                "eval",
                2,
                (
                    RunnerOutcome("eval-H1", 1, "classified", 1),
                    RunnerOutcome("eval-H2", 2, "dnf", None),
                ),
            ),
        },
    )
    target = frame(("eval",), "20240101")
    target["finish_position"] = [1, finish]

    def load_rows(
        _dataset: DatasetLike, race_ids: tuple[str, ...], _features: list[str]
    ) -> pd.DataFrame:
        return target if "eval" in race_ids else frame(race_ids, "20230101")

    def forbidden(**_kwargs: object) -> None:
        raise AssertionError("Contradictory target must stop before any model")

    monkeypatch.setattr(evaluation, "load_feature_rows", load_rows)
    monkeypatch.setattr(evaluation, "CatBoostRanker", forbidden)
    with pytest.raises(ValueError, match="Feature finish conflicts"):
        evaluation.evaluate_fold(
            ds.dataset(pa.table({"unused": [1]})), ["feature", "odds_score"], scope(), parameters
        )


def test_parse_args_accepts_target_plan_and_relevance_mode(tmp_path: Path) -> None:
    target_plan = tmp_path / "target-plan.json"
    args = evaluation.parse_args(
        [
            "--pg-url",
            "postgresql://local",
            "--features-root",
            str(tmp_path),
            "--output-dir",
            str(tmp_path),
            "--target-plan",
            str(target_plan),
            "--relevance-mode",
            "reciprocal-rank",
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
    assert args.target_plan == target_plan
    assert args.relevance_mode == "reciprocal-rank"


def test_parse_args_rejects_invalid_shard(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        evaluation.parse_args(
            [
                "--pg-url",
                "postgresql://local",
                "--features-root",
                str(tmp_path),
                "--output-dir",
                str(tmp_path),
                "--shard-count",
                "2",
                "--shard-index",
                "2",
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
