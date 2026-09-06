from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from timesfm_finish_position.ablation import (
    NamedPredictionFrame,
    evaluate_stacking_ablation,
    stacking_features,
    validate_aligned_predictions,
)
from timesfm_finish_position.lab_domain import PredictionFrame


def _frame(probability_shift: float = 0.0) -> PredictionFrame:
    return PredictionFrame(
        race_ids=np.asarray(
            ["r1", "r1", "r1", "r2", "r2", "r2", "r3", "r3", "r3", "r4", "r4", "r4"],
            dtype=np.str_,
        ),
        race_dates=np.asarray(
            ["20240101"] * 3 + ["20240201"] * 3 + ["20250101"] * 3 + ["20250201"] * 3,
            dtype=np.str_,
        ),
        horse_ids=np.asarray([f"h{index}" for index in range(12)], dtype=np.str_),
        finish_positions=np.asarray([1, 2, 3, 2, 1, 3, 1, 3, 2, 3, 2, 1], dtype=np.int64),
        decimal_odds=np.asarray([2.0, 3.0, 4.0] * 4, dtype=np.float64),
        win_probabilities=np.asarray(
            [0.6, 0.3, 0.1, 0.5, 0.3, 0.2, 0.5, 0.2, 0.3, 0.2, 0.3, 0.5],
            dtype=np.float64,
        )
        + probability_shift,
        ranking_scores=np.asarray(
            [3.0, 2.0, 1.0, 3.0, 2.0, 1.0, 3.0, 1.0, 2.0, 1.0, 2.0, 3.0],
            dtype=np.float64,
        ),
    )


def test_stacking_features_and_ablation_use_earlier_oof_only() -> None:
    predictions = (
        NamedPredictionFrame("baseline", _frame()),
        NamedPredictionFrame("candidate", _frame(0.01)),
        NamedPredictionFrame("candidate-two", _frame(0.02)),
    )
    features = stacking_features(predictions)
    assert features.shape == (12, 6)
    results = evaluate_stacking_ablation(predictions, train_end="20241231", test_start="20250101")
    assert [result.excluded_model for result in results] == [
        None,
        "baseline",
        "candidate",
        "candidate-two",
    ]
    assert all(result.metrics.races == 2 for result in results)
    assert results[0].to_dict()["excluded_model"] is None


def test_validate_aligned_predictions_rejects_contract_mismatches() -> None:
    one = NamedPredictionFrame("one", _frame())
    with pytest.raises(ValueError, match="at least two"):
        validate_aligned_predictions((one,))
    with pytest.raises(ValueError, match="names must be unique"):
        validate_aligned_predictions((one, NamedPredictionFrame("one", _frame())))
    bad_identity = replace(_frame(), horse_ids=np.asarray(["wrong"] * 12, dtype=np.str_))
    with pytest.raises(ValueError, match="runner identity mismatch"):
        validate_aligned_predictions((one, NamedPredictionFrame("two", bad_identity)))
    bad_dates = replace(_frame(), race_dates=np.asarray(["20200101"] * 12, dtype=np.str_))
    with pytest.raises(ValueError, match="race date mismatch"):
        validate_aligned_predictions((one, NamedPredictionFrame("two", bad_dates)))
    bad_labels = replace(_frame(), finish_positions=np.asarray([1] * 12, dtype=np.int64))
    with pytest.raises(ValueError, match="label mismatch"):
        validate_aligned_predictions((one, NamedPredictionFrame("two", bad_labels)))
    bad_odds = replace(_frame(), decimal_odds=np.asarray([9.0] * 12, dtype=np.float64))
    with pytest.raises(ValueError, match="odds mismatch"):
        validate_aligned_predictions((one, NamedPredictionFrame("two", bad_odds)))


def test_evaluate_stacking_ablation_rejects_invalid_or_empty_chronology() -> None:
    predictions = (
        NamedPredictionFrame("one", _frame()),
        NamedPredictionFrame("two", _frame(0.01)),
    )
    with pytest.raises(ValueError, match="at least three models"):
        evaluate_stacking_ablation(predictions, train_end="20241231", test_start="20250101")
    predictions = (*predictions, NamedPredictionFrame("three", _frame(0.02)))
    with pytest.raises(ValueError, match="must predate"):
        evaluate_stacking_ablation(predictions, train_end="20250101", test_start="20250101")
    with pytest.raises(ValueError, match="partition is empty"):
        evaluate_stacking_ablation(predictions, train_end="20200101", test_start="20250101")
