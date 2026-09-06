from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timesfm_finish_position.tabular_evaluation as subject
from timesfm_finish_position.lab_domain import ProbabilityMetrics
from timesfm_finish_position.tabular_evaluation import (
    TabularDataset,
    TreeFoldResult,
    apply_imputation,
    evaluate_tree_fold,
    fit_imputation,
    fit_temperature,
    load_tabular_dataset,
    nested_masks,
    read_prediction_frame,
    scores_to_probabilities,
    write_prediction_frame,
    write_tree_report,
)
from timesfm_finish_position.tree_rankers import RankerKind


class FakeRanker:
    def predict(self, features: np.ndarray) -> np.ndarray:
        return features[:, 0].astype(np.float64)


def _dataset() -> TabularDataset:
    dates = ["20220101"] * 3 + ["20230101"] * 3 + ["20240101"] * 3
    races = ["r22"] * 3 + ["r23"] * 3 + ["r24"] * 3
    return TabularDataset(
        race_ids=np.asarray(races, dtype=np.str_),
        race_dates=np.asarray(dates, dtype=np.str_),
        horse_ids=np.asarray([f"h{index}" for index in range(9)], dtype=np.str_),
        finish_positions=np.asarray([1, 2, 3] * 3, dtype=np.int64),
        decimal_odds=np.asarray([2.0, 3.0, 4.0] * 3, dtype=np.float64),
        features=np.asarray([[3.0], [2.0], [1.0]] * 3, dtype=np.float64),
        feature_names=("feature",),
    )


def test_load_tabular_dataset_keeps_only_model_features(tmp_path: Path) -> None:
    path = tmp_path / "features.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": ["r1", "r1"],
                "race_date": ["20240101", "20240101"],
                "horse_id": ["h1", "h2"],
                "finish_position": [1, 2],
                "decimal_odds": [2.0, 3.0],
                "distance": [1200, 1200],
            }
        ),
        path,
    )
    dataset = load_tabular_dataset(path)
    assert dataset.feature_names == ("distance",)
    assert dataset.features.tolist() == [[1200.0], [1200.0]]


def test_imputation_is_fit_on_training_partition_only() -> None:
    train = np.asarray([[1.0, np.nan], [3.0, np.nan]], dtype=np.float64)
    medians = fit_imputation(train)
    assert medians.tolist() == [2.0, 0.0]
    applied = apply_imputation(np.asarray([[np.nan, 4.0]], dtype=np.float64), medians)
    assert applied.tolist() == [[2.0, 4.0]]
    with pytest.raises(ValueError, match="dimensions do not match"):
        apply_imputation(np.zeros((2, 2)), np.zeros(1))


def test_score_probabilities_and_temperature_are_race_local() -> None:
    scores = np.asarray([3.0, 2.0, 1.0, 1.0, 2.0, 3.0], dtype=np.float64)
    races = np.asarray(["r1", "r1", "r1", "r2", "r2", "r2"], dtype=np.str_)
    probabilities = scores_to_probabilities(scores, races, temperature=1.0)
    assert probabilities[:3].sum() == pytest.approx(1.0)
    assert probabilities[3:].sum() == pytest.approx(1.0)
    assert fit_temperature(scores, races, np.asarray([1, 2, 3, 3, 2, 1], dtype=np.int64)) > 0.0
    with pytest.raises(ValueError, match="must align"):
        scores_to_probabilities(scores[:2], races, temperature=1.0)
    with pytest.raises(ValueError, match="must be positive"):
        scores_to_probabilities(scores, races, temperature=0.0)


def test_nested_masks_and_fold_evaluation_are_chronological(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dataset = _dataset()
    train, calibration, test = nested_masks(dataset.race_dates, 2024)
    assert train.sum() == calibration.sum() == test.sum() == 3

    def fake_fit_ranker(*_args: object, **_kwargs: object) -> FakeRanker:
        return FakeRanker()

    monkeypatch.setattr(subject, "fit_ranker", fake_fit_ranker)
    result, predictions = evaluate_tree_fold(
        dataset, RankerKind.LIGHTGBM_LAMBDARANK, 2024, estimators=2, threads=1
    )
    assert result.train_rows == result.calibration_rows == result.test_rows == 3
    assert result.metrics.races == 1
    assert predictions.race_dates.tolist() == ["20240101"] * 3
    prediction_path = tmp_path / "test-tree-predictions.parquet"
    write_prediction_frame(prediction_path, predictions)
    try:
        loaded = read_prediction_frame(prediction_path)
        assert loaded.horse_ids.tolist() == predictions.horse_ids.tolist()
        assert loaded.win_probabilities.tolist() == pytest.approx(
            predictions.win_probabilities.tolist()
        )
    finally:
        prediction_path.unlink(missing_ok=True)
    with pytest.raises(ValueError, match="empty nested partition"):
        nested_masks(dataset.race_dates, 2025)


def test_write_tree_report_records_nonproduction_contract(tmp_path: Path) -> None:
    metrics = ProbabilityMetrics(3, 1, 0.1, 0.1, 0.1, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 0.0)
    result = TreeFoldResult("model", 2024, 3, 3, 3, 1.0, metrics)
    path = tmp_path / "report.json"
    write_tree_report(path, (result,))
    report = json.loads(path.read_text())
    assert report["production_integration"] is False
    assert report["results"][0]["year"] == 2024
