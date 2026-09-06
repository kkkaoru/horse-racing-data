from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.lab_domain import PredictionFrame
from timesfm_finish_position.lab_metrics import (
    evaluate_prediction_frame,
    normalize_probabilities_by_race,
)


def test_normalize_probabilities_by_race_preserves_simplexes() -> None:
    race_ids = np.asarray(["r1", "r1", "r2", "r2"], dtype=np.str_)
    raw = np.asarray([2.0, 1.0, 0.0, 0.0], dtype=np.float64)
    normalized = normalize_probabilities_by_race(race_ids, raw)
    assert normalized.tolist() == pytest.approx([2.0 / 3.0, 1.0 / 3.0, 0.5, 0.5])


def test_normalize_probabilities_rejects_bad_shape_and_values() -> None:
    with pytest.raises(ValueError, match="must align"):
        normalize_probabilities_by_race(
            np.asarray(["r1"], dtype=np.str_), np.asarray([0.5, 0.5], dtype=np.float64)
        )
    with pytest.raises(ValueError, match="finite and nonnegative"):
        normalize_probabilities_by_race(
            np.asarray(["r1"], dtype=np.str_), np.asarray([-0.5], dtype=np.float64)
        )


def test_evaluate_prediction_frame_computes_probability_ranking_and_betting_metrics() -> None:
    frame = PredictionFrame(
        race_ids=np.asarray(["r1", "r1", "r1", "r2", "r2", "r2"], dtype=np.str_),
        race_dates=np.asarray(
            ["20240101", "20240101", "20240101", "20240102", "20240102", "20240102"],
            dtype=np.str_,
        ),
        horse_ids=np.asarray(["h1", "h2", "h3", "h4", "h5", "h6"], dtype=np.str_),
        finish_positions=np.asarray([1, 2, 3, 2, 1, 3], dtype=np.int64),
        decimal_odds=np.asarray([2.0, 3.0, 4.0, 2.0, 2.5, 5.0], dtype=np.float64),
        win_probabilities=np.asarray([0.6, 0.3, 0.1, 0.5, 0.3, 0.2], dtype=np.float64),
        ranking_scores=np.asarray([3.0, 2.0, 1.0, 3.0, 2.0, 1.0], dtype=np.float64),
    )
    metrics = evaluate_prediction_frame(frame)
    assert metrics.runners == 6
    assert metrics.races == 2
    assert metrics.top1_accuracy == 0.5
    assert metrics.winner_in_top3 == 1.0
    assert metrics.top3_set_accuracy == 1.0
    assert metrics.roi == 1.0
    assert metrics.yield_rate == 0.0
    assert metrics.maximum_drawdown == 1.0
    assert 0.0 < metrics.log_loss < 1.0
    assert 0.0 < metrics.brier_score < 0.2
    assert 0.0 <= metrics.expected_calibration_error < 0.5
    assert 0.0 < metrics.ndcg_at_3 <= 1.0


def test_evaluate_prediction_frame_rejects_empty_misaligned_and_invalid_rows() -> None:
    empty = PredictionFrame(
        race_ids=np.asarray([], dtype=np.str_),
        race_dates=np.asarray([], dtype=np.str_),
        horse_ids=np.asarray([], dtype=np.str_),
        finish_positions=np.asarray([], dtype=np.int64),
        decimal_odds=np.asarray([], dtype=np.float64),
        win_probabilities=np.asarray([], dtype=np.float64),
        ranking_scores=np.asarray([], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="must not be empty"):
        evaluate_prediction_frame(empty)
    invalid = PredictionFrame(
        race_ids=np.asarray(["r1"], dtype=np.str_),
        race_dates=np.asarray(["20240101"], dtype=np.str_),
        horse_ids=np.asarray(["h1"], dtype=np.str_),
        finish_positions=np.asarray([0], dtype=np.int64),
        decimal_odds=np.asarray([2.0], dtype=np.float64),
        win_probabilities=np.asarray([0.5], dtype=np.float64),
        ranking_scores=np.asarray([1.0], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="finish positions must be positive"):
        evaluate_prediction_frame(invalid)
    bad_probability = PredictionFrame(
        race_ids=np.asarray(["r1"], dtype=np.str_),
        race_dates=np.asarray(["20240101"], dtype=np.str_),
        horse_ids=np.asarray(["h1"], dtype=np.str_),
        finish_positions=np.asarray([1], dtype=np.int64),
        decimal_odds=np.asarray([2.0], dtype=np.float64),
        win_probabilities=np.asarray([np.nan], dtype=np.float64),
        ranking_scores=np.asarray([1.0], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="win probabilities must be finite"):
        evaluate_prediction_frame(bad_probability)
