from __future__ import annotations

from argparse import Namespace
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Protocol, cast, override

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from catboost import Pool

import evaluate_jra_cell_models as evaluation
from predict_lib.jra_cell_scope import JraCellKey, JraFoldScope
from train_jra_cell_models import DatasetLike


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
    return Namespace(iterations=2, learning_rate=0.05, depth=2, thread_count=1)


def test_fold_evaluation_is_pit_and_reports_model_vs_market(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical = frame(scope().training_race_ids, "20230101")
    target = frame(("eval",), "20240101")

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

    monkeypatch.setattr(evaluation, "CatBoostRanker", RecordingRanker)
    monkeypatch.setattr(
        evaluation,
        "load_feature_rows",
        lambda _dataset, race_ids, _features: frame(
            tuple(race_ids), "20240101" if "eval" in race_ids else "20230101"
        ),
    )

    class UnusedDataset:
        schema: pa.Schema = pa.schema([])

        def to_table(self, *, columns: list[str], filter: object) -> pa.Table:
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
    monkeypatch.setattr(
        evaluation,
        "load_feature_rows",
        lambda _dataset, race_ids, _features: frame(
            tuple(race_ids), "20230101" if "eval" not in race_ids else "20240101"
        ),
    )
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

    monkeypatch.setattr(
        evaluation,
        "load_feature_rows",
        lambda _dataset, race_ids, _features: (
            frame(tuple(race_ids), "20240101", complete=False)
            if "eval" in race_ids
            else frame(tuple(race_ids), "20230101")
        ),
    )
    result = evaluation.evaluate_fold(
        cast(DatasetLike, object()), ["feature", "odds_score"], scope(), args()
    )
    assert result["status"] == "no-complete-evaluation-races"


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
                        "market_baseline_metrics": baseline,
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
            ]
        )
