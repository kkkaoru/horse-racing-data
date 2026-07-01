from __future__ import annotations

from collections.abc import Callable, Mapping
import json
from pathlib import Path
from typing import cast

import lightgbm as lgb
import numpy as np
import polars as pl
import pytest

import running_style_lightgbm as subject


def test_class_labels_match_running_style_order():
    assert subject.CLASS_LABELS == ("nige", "senkou", "sashi", "oikomi")


def test_probability_columns_align_with_class_labels():
    assert subject.PROBABILITY_COLUMNS == ("p_nige", "p_senkou", "p_sashi", "p_oikomi")


def test_default_training_params_uses_documented_defaults():
    params = subject.default_training_params()
    assert params["num_leaves"] == 63
    assert params["lambda_l1"] == 0.1
    assert params["lambda_l2"] == 0.1
    assert params["feature_fraction"] == 0.8
    assert params["num_threads"] == subject.AUTO_NUM_THREADS


def test_resolve_feature_columns_excludes_target_running_style_class():
    columns = [
        "race_id",
        "umaban",
        "target_running_style_class",
        "target_corner_1_norm",
        "finish_position",
        "speed_index_avg_5",
    ]
    features = subject.resolve_feature_columns(columns)
    assert features == ["speed_index_avg_5"]


def test_resolve_feature_columns_excludes_bamei_identifier():
    columns = [
        "bamei",
        "speed_index_avg_5",
        "corner_pace_avg_3",
        "target_running_style_class",
    ]
    features = subject.resolve_feature_columns(columns)
    assert features == ["speed_index_avg_5", "corner_pace_avg_3"]


def test_resolve_feature_columns_excludes_rs_p_leak_columns():
    columns = [
        "speed_index_avg_5",
        "rs_p_nige",
        "rs_p_senkou",
        "rs_p_sashi",
        "rs_p_oikomi",
        "corner_pace_avg_3",
        "target_running_style_class",
    ]
    features = subject.resolve_feature_columns(columns)
    assert features == ["speed_index_avg_5", "corner_pace_avg_3"]


def test_leak_columns_constant_contains_all_four_rs_p_columns():
    assert subject.LEAK_COLUMNS == ("rs_p_nige", "rs_p_senkou", "rs_p_sashi", "rs_p_oikomi")


def test_detect_categorical_features_returns_only_known_categoricals():
    feature_columns = [
        "track_code",
        "kyori_band",
        "speed_index_avg_5",
        "grade_code",
        "kyoso_joken_code",
        "nar_subclass",
    ]
    detected = subject.detect_categorical_features(feature_columns)
    assert sorted(detected) == [
        "grade_code",
        "kyori_band",
        "kyoso_joken_code",
        "nar_subclass",
        "track_code",
    ]


def test_encode_categoricals_accepts_numeric_category_columns():
    frame = pl.DataFrame(
        {
            "kyori_band": [2, 2, 3],
            "track_code": ["10", "10", "20"],
            "speed_index_avg_5": [1.0, 2.0, 3.0],
        }
    )
    encoded = subject.encode_categoricals(frame, ["kyori_band", "track_code"])
    assert encoded.schema["kyori_band"] == pl.Categorical
    assert encoded.schema["track_code"] == pl.Categorical
    assert encoded["kyori_band"].cast(pl.Utf8).to_list() == ["2", "2", "3"]


def test_to_lgb_frame_keeps_categorical_columns_as_pandas_category():
    frame = subject.encode_categoricals(
        pl.DataFrame({"kyoso_joken_code": ["701", "702"], "feature_a": [1.0, 2.0]}),
        ["kyoso_joken_code"],
    )
    pandas_frame = subject.to_lgb_frame(frame, ["kyoso_joken_code"])
    assert str(pandas_frame["kyoso_joken_code"].dtype) == "category"


def test_compute_inverse_frequency_weights_balances_classes():
    labels = pl.Series([0, 0, 1, 1, 1, 1, 2, 2, 2, 3])
    weights = subject.compute_inverse_frequency_weights(labels)
    assert weights.shape == (10,)
    nige_weight = weights[0]
    senkou_weight = weights[2]
    sashi_weight = weights[6]
    oikomi_weight = weights[9]
    assert nige_weight == pytest.approx(10 / (4 * 2))
    assert senkou_weight == pytest.approx(10 / (4 * 4))
    assert sashi_weight == pytest.approx(10 / (4 * 3))
    assert oikomi_weight == pytest.approx(10 / (4 * 1))


def test_compute_inverse_frequency_weights_avoids_zero_division():
    labels = pl.Series([0, 0, 1, 1])
    weights = subject.compute_inverse_frequency_weights(labels)
    assert not np.any(np.isnan(weights))
    assert not np.any(np.isinf(weights))


def test_compute_accuracy_for_perfect_prediction():
    assert subject.compute_accuracy(np.array([0, 1, 2]), np.array([0, 1, 2])) == 1.0


def test_compute_accuracy_for_partial_match():
    assert subject.compute_accuracy(np.array([0, 1, 0]), np.array([0, 0, 0])) == pytest.approx(2 / 3)


def test_compute_accuracy_returns_nan_for_empty_input():
    assert np.isnan(subject.compute_accuracy(np.array([]), np.array([])))


def test_compute_per_class_precision_recall_for_balanced_predictions():
    predicted = np.array([0, 0, 1, 1, 2, 3])
    actual = np.array([0, 1, 1, 2, 2, 3])
    precision, recall, support = subject.compute_per_class_precision_recall(predicted, actual)
    assert precision["nige"] == pytest.approx(0.5)
    assert precision["senkou"] == pytest.approx(0.5)
    assert recall["nige"] == pytest.approx(1.0)
    assert recall["oikomi"] == pytest.approx(1.0)
    assert support["nige"] == 1
    assert support["sashi"] == 2


def test_macro_f1_from_precision_recall_average_of_per_class_f1():
    precision = {"nige": 0.5, "senkou": 0.5, "sashi": 1.0, "oikomi": 1.0}
    recall = {"nige": 1.0, "senkou": 1.0, "sashi": 0.5, "oikomi": 1.0}
    macro_f1 = subject.macro_f1_from_precision_recall(precision, recall)
    # f1(nige)=2*0.5*1.0/1.5≈0.667, f1(senkou)=0.667, f1(sashi)=0.667, f1(oikomi)=1.0
    expected = (2.0 * 0.5 * 1.0 / 1.5 + 2.0 * 0.5 * 1.0 / 1.5 + 2.0 * 1.0 * 0.5 / 1.5 + 1.0) / 4
    assert macro_f1 == pytest.approx(expected)


def test_macro_f1_treats_undefined_classes_as_zero():
    precision = {"nige": float("nan"), "senkou": 1.0, "sashi": 0.5, "oikomi": 0.5}
    recall = {"nige": float("nan"), "senkou": 1.0, "sashi": 1.0, "oikomi": 0.5}
    macro_f1 = subject.macro_f1_from_precision_recall(precision, recall)
    expected_sashi = 2.0 * 0.5 * 1.0 / 1.5
    expected_oikomi = 2.0 * 0.5 * 0.5 / 1.0
    assert macro_f1 == pytest.approx((0.0 + 1.0 + expected_sashi + expected_oikomi) / 4.0)


def test_compute_top2_accuracy_clamps_k_to_available_probability_columns():
    probabilities = np.array([[0.9], [0.2], [0.8]], dtype=np.float64)
    actual = np.array([0, 0, 0], dtype=np.int64)
    assert subject.compute_top2_accuracy(probabilities, actual) == pytest.approx(1.0)


def test_compute_top2_accuracy_rejects_row_label_mismatch():
    probabilities = np.array([[0.9, 0.1]], dtype=np.float64)
    actual = np.array([0, 1], dtype=np.int64)
    with pytest.raises(ValueError, match="one row per label"):
        subject.compute_top2_accuracy(probabilities, actual)


def test_filter_labeled_rows_drops_null_target():
    df = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "target_running_style_class": [0, None, 2],
        }
    )
    filtered = subject.filter_labeled_rows(df)
    assert len(filtered) == 2
    assert filtered["a"].to_list() == [1, 3]


def test_split_by_year_separates_train_and_validation():
    df = pl.DataFrame(
        {
            "race_date": ["20230101", "20240615", "20250301"],
            "race_year": [2023, 2024, 2025],
            "value": [1, 2, 3],
        }
    )
    train, valid = subject.split_by_year(df, "20230101", 2025)
    assert train["value"].to_list() == [1, 2]
    assert valid["value"].to_list() == [3]


def test_compute_predicted_labels_picks_argmax_per_row():
    probabilities = np.array(
        [
            [0.1, 0.7, 0.1, 0.1],
            [0.4, 0.1, 0.4, 0.1],
            [0.0, 0.0, 0.0, 1.0],
        ]
    )
    labels = subject.compute_predicted_labels(probabilities)
    assert labels.tolist() == [1, 0, 3]


def test_build_predictions_df_emits_probabilities_and_label():
    valid_df = pl.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["h1", "h2"],
            "umaban": [1, 2],
            "race_year": [2025, 2025],
            "target_running_style_class": [0, 2],
        }
    )
    probabilities = np.array([[0.6, 0.2, 0.1, 0.1], [0.1, 0.1, 0.7, 0.1]])
    output = subject.build_predictions_df(valid_df, probabilities)
    assert output["predicted_label"].to_list() == ["nige", "sashi"]
    assert output["predicted_class"].to_list() == [0, 2]
    assert output["p_nige"].to_list() == [0.6, 0.1]
    assert output["p_sashi"].to_list() == [0.1, 0.7]


def test_lgb_params_for_multiclass_sets_objective_and_num_class(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(subject, "resolve_auto_num_threads", lambda: 3)
    params = subject.lgb_params_for_multiclass(subject.default_training_params())
    assert params["objective"] == "multiclass"
    assert params["num_class"] == 4
    assert params["metric"] == "multi_logloss"
    assert params["num_threads"] == 3


def test_parse_validation_years_accepts_multiple_years():
    assert subject.parse_validation_years("2024,2025") == [2024, 2025]


def test_parse_args_walk_forward_uses_21_year_default_train_start():
    args = subject.parse_args(
        [
            "walk-forward",
            "--csv",
            "tmp/in",
            "--output-predictions-dir",
            "tmp/out",
        ]
    )
    assert args.train_start_date == "20050101"
    assert args.validation_years == "2024,2025"


def test_parse_args_train_production_uses_21_year_default_train_range():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
        ]
    )
    assert args.train_start_date == "20050101"
    assert args.train_end_date == "20261231"
    assert args.valid_start_date == "20260101"


def test_default_train_start_date_constant_is_2005_for_21_year_range():
    assert subject.DEFAULT_TRAIN_START_DATE == "20050101"


def test_default_train_end_date_constant_is_2026_for_21_year_range():
    assert subject.DEFAULT_TRAIN_END_DATE == "20261231"


def test_default_valid_start_date_constant_holds_out_2026_partial_year():
    assert subject.DEFAULT_VALID_START_DATE == "20260101"


def test_write_predictions_jsonl_writes_each_row(tmp_path: Path):
    df = pl.DataFrame(
        {
            "race_id": ["r1"],
            "umaban": [1],
            "p_nige": [0.6],
            "predicted_label": ["nige"],
        }
    )
    output_path = tmp_path / "out.jsonl"
    subject.write_predictions_jsonl(df, output_path)
    parsed = json.loads(output_path.read_text(encoding="utf-8").strip())
    assert parsed == {"race_id": "r1", "umaban": 1, "p_nige": 0.6, "predicted_label": "nige"}


def test_write_walk_forward_report_aggregates_accuracy_and_macro_f1(tmp_path: Path):
    folds: list[subject.FoldMetrics] = [
        {
            "validation_year": 2024,
            "train_rows": 100,
            "valid_rows": 50,
            "accuracy": 0.55,
            "macro_f1": 0.40,
            "per_class_precision": {"nige": 0.5, "senkou": 0.5, "sashi": 0.5, "oikomi": 0.5},
            "per_class_recall": {"nige": 0.5, "senkou": 0.5, "sashi": 0.5, "oikomi": 0.5},
            "per_class_support": {"nige": 5, "senkou": 10, "sashi": 20, "oikomi": 15},
        },
        {
            "validation_year": 2025,
            "train_rows": 150,
            "valid_rows": 60,
            "accuracy": 0.57,
            "macro_f1": 0.42,
            "per_class_precision": {"nige": 0.5, "senkou": 0.5, "sashi": 0.5, "oikomi": 0.5},
            "per_class_recall": {"nige": 0.5, "senkou": 0.5, "sashi": 0.5, "oikomi": 0.5},
            "per_class_support": {"nige": 6, "senkou": 12, "sashi": 22, "oikomi": 20},
        },
    ]
    output_path = tmp_path / "report.json"
    subject.write_walk_forward_report(folds, output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["aggregate"]["accuracy_mean"] == pytest.approx(0.56)
    assert payload["aggregate"]["macro_f1_mean"] == pytest.approx(0.41)


def test_default_walk_forward_windows_constant_covers_2023_to_2026():
    assert subject.DEFAULT_WALK_FORWARD_WINDOWS == "2023,2024,2025,2026"


def test_walk_forward_precision_gap_threshold_is_thirty_percentage_points():
    assert subject.WALK_FORWARD_PRECISION_GAP_THRESHOLD_PP == pytest.approx(30.0)


def test_parse_walk_forward_windows_parses_single_year():
    assert subject.parse_walk_forward_windows("2024") == [2024]


def test_parse_walk_forward_windows_parses_multi_year_with_whitespace():
    assert subject.parse_walk_forward_windows(" 2023 , 2024 , 2025 ") == [2023, 2024, 2025]


def test_parse_walk_forward_windows_default_returns_four_year_window():
    assert subject.parse_walk_forward_windows(subject.DEFAULT_WALK_FORWARD_WINDOWS) == [2023, 2024, 2025, 2026]


def test_parse_args_train_production_walk_forward_eval_disabled_by_default():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
        ]
    )
    assert args.enable_walk_forward_eval is False
    assert args.walk_forward_windows == "2023,2024,2025,2026"


def test_parse_args_train_production_enable_walk_forward_eval_flag_parses():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--enable-walk-forward-eval",
            "--walk-forward-windows",
            "2024,2025",
        ]
    )
    assert args.enable_walk_forward_eval is True
    assert args.walk_forward_windows == "2024,2025"


def test_derive_walk_forward_train_end_uses_previous_year_dec_31():
    assert subject.derive_walk_forward_train_end(2024) == "20231231"


def test_derive_walk_forward_valid_range_covers_full_holdout_year():
    assert subject.derive_walk_forward_valid_range(2024) == ("20240101", "20241231")


def test_compute_binary_log_loss_nige_for_perfectly_confident_correct_prediction():
    probabilities = np.array(
        [
            [0.99, 0.005, 0.003, 0.002],
            [0.01, 0.49, 0.30, 0.20],
        ]
    )
    actual = np.array([0, 1], dtype=np.int64)
    loss = subject.compute_binary_log_loss_nige(probabilities, actual)
    expected = -(np.log(0.99) + np.log(1.0 - 0.01)) / 2.0
    assert loss == pytest.approx(expected)


def test_compute_binary_log_loss_nige_returns_nan_for_empty_input():
    probabilities = np.zeros((0, 4))
    actual = np.array([], dtype=np.int64)
    assert np.isnan(subject.compute_binary_log_loss_nige(probabilities, actual))


def test_compute_multi_log_loss_picks_actual_class_probability():
    probabilities = np.array(
        [
            [0.6, 0.2, 0.1, 0.1],
            [0.1, 0.7, 0.1, 0.1],
        ]
    )
    actual = np.array([0, 1], dtype=np.int64)
    loss = subject.compute_multi_log_loss(probabilities, actual)
    expected = -(np.log(0.6) + np.log(0.7)) / 2.0
    assert loss == pytest.approx(expected)


def test_compute_multi_log_loss_returns_nan_for_empty_input():
    probabilities = np.zeros((0, 4))
    actual = np.array([], dtype=np.int64)
    assert np.isnan(subject.compute_multi_log_loss(probabilities, actual))


def test_compute_walk_forward_eval_metrics_assembles_all_required_keys():
    probabilities = np.array(
        [
            [0.8, 0.1, 0.05, 0.05],
            [0.2, 0.6, 0.1, 0.1],
            [0.1, 0.1, 0.7, 0.1],
            [0.1, 0.1, 0.1, 0.7],
        ]
    )
    actual = np.array([0, 1, 2, 3], dtype=np.int64)
    metrics = subject.compute_walk_forward_eval_metrics(
        probabilities, actual, 2024, "20050101", "20231231", 9999,
    )
    assert metrics["holdout_year"] == 2024
    assert metrics["train_start_date"] == "20050101"
    assert metrics["train_end_date"] == "20231231"
    assert metrics["train_rows"] == 9999
    assert metrics["valid_rows"] == 4
    assert metrics["precision_nige"] == pytest.approx(1.0)
    assert metrics["recall_nige"] == pytest.approx(1.0)
    assert metrics["accuracy"] == pytest.approx(1.0)
    assert metrics["log_loss_nige"] > 0
    assert metrics["multi_log_loss"] > 0


def test_detect_walk_forward_precision_regression_flags_30pp_gap():
    assert subject.detect_walk_forward_precision_regression(0.55, 0.93) is True


def test_detect_walk_forward_precision_regression_ignores_small_gap():
    assert subject.detect_walk_forward_precision_regression(0.70, 0.80) is False


def test_detect_walk_forward_precision_regression_returns_false_on_nan_inputs():
    assert subject.detect_walk_forward_precision_regression(float("nan"), 0.93) is False
    assert subject.detect_walk_forward_precision_regression(0.50, float("nan")) is False


def test_detect_walk_forward_precision_regression_honors_custom_threshold():
    assert subject.detect_walk_forward_precision_regression(0.80, 0.85, gap_threshold_pp=2.0) is True
    assert subject.detect_walk_forward_precision_regression(0.80, 0.85, gap_threshold_pp=10.0) is False


def test_emit_walk_forward_warnings_prints_warning_when_gap_exceeds_threshold(capsys: pytest.CaptureFixture[str]):
    walk_forward_results: dict[str, subject.WalkForwardEvalMetrics] = {
        "2024": {
            "holdout_year": 2024,
            "train_start_date": "20050101",
            "train_end_date": "20231231",
            "train_rows": 1000,
            "valid_rows": 100,
            "precision_nige": 0.52,
            "recall_nige": 0.18,
            "log_loss_nige": 0.50,
            "multi_log_loss": 1.20,
            "accuracy": 0.40,
        }
    }
    warnings = subject.emit_walk_forward_warnings(walk_forward_results, 0.93)
    captured = capsys.readouterr()
    assert len(warnings) == 1
    assert "walk-forward 2024" in warnings[0]
    assert "Train leakage suspected" in warnings[0]
    assert "walk-forward 2024" in captured.out


def test_emit_walk_forward_warnings_returns_empty_when_no_regression(capsys: pytest.CaptureFixture[str]):
    walk_forward_results: dict[str, subject.WalkForwardEvalMetrics] = {
        "2024": {
            "holdout_year": 2024,
            "train_start_date": "20050101",
            "train_end_date": "20231231",
            "train_rows": 1000,
            "valid_rows": 100,
            "precision_nige": 0.70,
            "recall_nige": 0.40,
            "log_loss_nige": 0.30,
            "multi_log_loss": 0.90,
            "accuracy": 0.55,
        }
    }
    warnings = subject.emit_walk_forward_warnings(walk_forward_results, 0.75)
    captured = capsys.readouterr()
    assert warnings == []
    assert "WARNING" not in captured.out


def test_run_walk_forward_eval_for_year_returns_nan_when_train_subset_empty(monkeypatch: pytest.MonkeyPatch):
    df = pl.DataFrame(
        {
            "race_date": ["20240115"],
            "race_year": [2024],
            "target_running_style_class": [0],
            "feature_a": [1.0],
        }
    )

    def _should_not_train(*_args: object, **_kwargs: object) -> tuple[object, np.ndarray]:
        raise AssertionError("train_running_style_head should not be invoked when split is empty")

    monkeypatch.setattr(subject, "train_running_style_head", _should_not_train)
    metrics = subject.run_walk_forward_eval_for_year(
        df, 2024, "20050101", ["feature_a"], [], subject.default_training_params(),
    )
    assert metrics["train_rows"] == 0
    assert metrics["valid_rows"] == 1
    assert np.isnan(metrics["precision_nige"])
    assert np.isnan(metrics["multi_log_loss"])


def test_run_walk_forward_eval_for_year_invokes_training_and_returns_metrics(monkeypatch: pytest.MonkeyPatch):
    df = pl.DataFrame(
        {
            "race_date": ["20230101", "20230102", "20240101", "20240102"],
            "race_year": [2023, 2023, 2024, 2024],
            "target_running_style_class": [0, 1, 0, 2],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    fake_probs = np.array([[0.7, 0.1, 0.1, 0.1], [0.1, 0.1, 0.7, 0.1]])
    captured_train_rows: dict[str, int] = {}

    def _fake_train(
        train_df: pl.DataFrame,
        valid_df: pl.DataFrame,
        _feature_columns: list[str],
        _categorical_features: list[str],
        _params: subject.TrainingParams,
    ) -> tuple[object, np.ndarray]:
        captured_train_rows["train"] = len(train_df)
        captured_train_rows["valid"] = len(valid_df)
        return object(), fake_probs

    monkeypatch.setattr(subject, "train_running_style_head", _fake_train)
    metrics = subject.run_walk_forward_eval_for_year(
        df, 2024, "20050101", ["feature_a"], [], subject.default_training_params(),
    )
    assert captured_train_rows == {"train": 2, "valid": 2}
    assert metrics["holdout_year"] == 2024
    assert metrics["train_end_date"] == "20231231"
    assert metrics["precision_nige"] == pytest.approx(1.0)
    assert metrics["accuracy"] == pytest.approx(1.0)


def test_run_walk_forward_eval_for_windows_prints_each_year_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    captured_years: list[int] = []

    def _fake_year(
        _df: pl.DataFrame,
        holdout_year: int,
        train_start_date: str,
        _features: list[str],
        _cats: list[str],
        _params: subject.TrainingParams,
    ) -> subject.WalkForwardEvalMetrics:
        captured_years.append(holdout_year)
        return {
            "holdout_year": holdout_year,
            "train_start_date": train_start_date,
            "train_end_date": subject.derive_walk_forward_train_end(holdout_year),
            "train_rows": 1,
            "valid_rows": 1,
            "precision_nige": 0.5,
            "recall_nige": 0.5,
            "log_loss_nige": 0.5,
            "multi_log_loss": 1.0,
            "accuracy": 0.5,
        }

    monkeypatch.setattr(subject, "run_walk_forward_eval_for_year", _fake_year)
    df = pl.DataFrame({"race_date": [], "race_year": [], "target_running_style_class": []})
    results = subject.run_walk_forward_eval_for_windows(
        df, [2024, 2025], "20050101", [], [], subject.default_training_params(),
    )
    captured = capsys.readouterr()
    assert captured_years == [2024, 2025]
    assert sorted(results.keys()) == ["2024", "2025"]
    assert "walk_forward_eval" in captured.out


def test_compute_production_precision_nige_returns_nan_when_valid_empty(monkeypatch: pytest.MonkeyPatch):
    train_subset = pl.DataFrame(
        {
            "race_date": ["20240101", "20240102"],
            "target_running_style_class": [0, 1],
            "feature_a": [1.0, 0.5],
        }
    )

    def _should_not_predict(*_args: object, **_kwargs: object) -> np.ndarray:
        raise AssertionError("predict_softmax should not be invoked when validation slice is empty")

    monkeypatch.setattr(subject, "predict_softmax", _should_not_predict)
    result = subject.compute_production_precision_nige(
        cast(lgb.Booster, object()),
        train_subset,
        ["feature_a"],
        [],
        "20260101",
    )
    assert np.isnan(result)


def test_compute_production_precision_nige_computes_from_predict_softmax(monkeypatch: pytest.MonkeyPatch):
    train_subset = pl.DataFrame(
        {
            "race_date": ["20250101", "20250102", "20260101", "20260102"],
            "target_running_style_class": [0, 1, 0, 0],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    fake_probs = np.array([[0.8, 0.1, 0.05, 0.05], [0.7, 0.1, 0.1, 0.1]])
    captured_rows: dict[str, int] = {}

    def _fake_predict(
        _booster: object,
        frame: pl.DataFrame,
        _feature_columns: list[str],
        _categorical_features: list[str],
    ) -> np.ndarray:
        captured_rows["rows"] = len(frame)
        return fake_probs

    monkeypatch.setattr(subject, "predict_softmax", _fake_predict)
    result = subject.compute_production_precision_nige(
        cast(lgb.Booster, object()),
        train_subset,
        ["feature_a"],
        [],
        "20260101",
    )
    assert captured_rows == {"rows": 2}
    assert result == pytest.approx(1.0)


def test_write_model_metadata_includes_walk_forward_results_when_provided(tmp_path: Path):
    walk_forward_results: dict[str, subject.WalkForwardEvalMetrics] = {
        "2024": {
            "holdout_year": 2024,
            "train_start_date": "20050101",
            "train_end_date": "20231231",
            "train_rows": 1000,
            "valid_rows": 100,
            "precision_nige": 0.52,
            "recall_nige": 0.18,
            "log_loss_nige": 0.50,
            "multi_log_loss": 1.20,
            "accuracy": 0.40,
        }
    }
    subject.write_model_metadata(
        tmp_path,
        "test-version",
        ["feature_a"],
        [],
        1000,
        "20050101",
        "20261231",
        with_field_features=True,
        walk_forward_results=walk_forward_results,
        production_precision_nige=0.93,
    )
    metadata = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["walk_forward_results"]["2024"]["precision_nige"] == pytest.approx(0.52)
    assert metadata["production_precision_nige"] == pytest.approx(0.93)


def test_write_model_metadata_omits_walk_forward_results_when_absent(tmp_path: Path):
    subject.write_model_metadata(
        tmp_path,
        "test-version",
        ["feature_a"],
        [],
        500,
        "20050101",
        "20261231",
        with_field_features=False,
    )
    metadata = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
    assert "walk_forward_results" not in metadata
    assert "production_precision_nige" not in metadata
    assert metadata["feature_schema_version"] == "v1"


def test_write_model_metadata_drops_production_precision_when_nan(tmp_path: Path):
    subject.write_model_metadata(
        tmp_path,
        "test-version",
        ["feature_a"],
        [],
        500,
        "20050101",
        "20261231",
        with_field_features=True,
        walk_forward_results={},
        production_precision_nige=float("nan"),
    )
    metadata = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["walk_forward_results"] == {}
    assert "production_precision_nige" not in metadata


def test_run_train_production_command_default_skips_walk_forward_eval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "data.parquet"
    output_dir = tmp_path / "model"
    df = pl.DataFrame(
        {
            "race_date": ["20240101", "20240102", "20260101", "20260102"],
            "target_running_style_class": [0, 1, 0, 1],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    df.write_parquet(csv_path)

    class _FakeBooster:
        def save_model(self, path: str) -> None:
            Path(path).write_text("fake-model", encoding="utf-8")

    def _fake_load(_path: Path) -> pl.DataFrame:
        return df.clone()

    def _fake_train_full(
        _train_df: pl.DataFrame,
        _features: list[str],
        _cats: list[str],
        _params: subject.TrainingParams,
        *,
        valid_start_date: str | None = None,
        class_weight_scheme: str = "inverse_freq",
    ) -> _FakeBooster:
        assert valid_start_date == "20260101"
        assert class_weight_scheme == "inverse_freq"
        return _FakeBooster()

    def _no_walk_forward(*_args: object, **_kwargs: object) -> dict[str, subject.WalkForwardEvalMetrics]:
        raise AssertionError("walk-forward eval must not run when --enable-walk-forward-eval is absent")

    monkeypatch.setattr(subject, "load_dataset_parquet", _fake_load)
    monkeypatch.setattr(subject, "maybe_enrich_with_field_features", lambda frame, _enabled: frame)
    monkeypatch.setattr(subject, "train_full_dataset", _fake_train_full)
    monkeypatch.setattr(subject, "run_walk_forward_eval_for_windows", _no_walk_forward)
    monkeypatch.setattr("builtins.print", lambda *_args, **_kwargs: None)
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            str(csv_path),
            "--model-version",
            "test-v1",
            "--output-model-dir",
            str(output_dir),
            "--train-start-date",
            "20240101",
            "--train-end-date",
            "20261231",
            "--valid-start-date",
            "20260101",
            "--no-with-field-features",
        ]
    )
    subject.run_train_production_command(args)
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert "walk_forward_results" not in metadata
    assert "production_precision_nige" not in metadata


def test_parse_args_train_production_bagging_fraction_override_parses():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--bagging-fraction",
            "0.6",
        ]
    )
    assert args.bagging_fraction == pytest.approx(0.6)


def test_parse_args_train_production_bagging_freq_override_parses():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--bagging-freq",
            "5",
        ]
    )
    assert args.bagging_freq == 5


def test_parse_args_train_production_feature_fraction_override_parses():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--feature-fraction",
            "0.85",
        ]
    )
    assert args.feature_fraction == pytest.approx(0.85)


def test_parse_args_train_production_reg_alpha_override_parses():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--reg-alpha",
            "0.2",
        ]
    )
    assert args.reg_alpha == pytest.approx(0.2)


def test_parse_args_train_production_reg_lambda_override_parses():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--reg-lambda",
            "0.2",
        ]
    )
    assert args.reg_lambda == pytest.approx(0.2)


def test_parse_args_train_production_hyperparam_defaults_match_documented_values():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
        ]
    )
    assert args.bagging_fraction == pytest.approx(0.8)
    assert args.bagging_freq == 1
    assert args.feature_fraction == pytest.approx(0.8)
    assert args.reg_alpha == pytest.approx(0.1)
    assert args.reg_lambda == pytest.approx(0.1)


def test_parse_args_walk_forward_bagging_fraction_override_parses():
    args = subject.parse_args(
        [
            "walk-forward",
            "--csv",
            "tmp/in",
            "--output-predictions-dir",
            "tmp/out",
            "--bagging-fraction",
            "0.6",
        ]
    )
    assert args.bagging_fraction == pytest.approx(0.6)


def test_parse_args_walk_forward_feature_fraction_override_parses():
    args = subject.parse_args(
        [
            "walk-forward",
            "--csv",
            "tmp/in",
            "--output-predictions-dir",
            "tmp/out",
            "--feature-fraction",
            "0.85",
        ]
    )
    assert args.feature_fraction == pytest.approx(0.85)


def test_parse_args_walk_forward_reg_alpha_override_parses():
    args = subject.parse_args(
        [
            "walk-forward",
            "--csv",
            "tmp/in",
            "--output-predictions-dir",
            "tmp/out",
            "--reg-alpha",
            "0.2",
        ]
    )
    assert args.reg_alpha == pytest.approx(0.2)


def test_parse_args_walk_forward_reg_lambda_override_parses():
    args = subject.parse_args(
        [
            "walk-forward",
            "--csv",
            "tmp/in",
            "--output-predictions-dir",
            "tmp/out",
            "--reg-lambda",
            "0.2",
        ]
    )
    assert args.reg_lambda == pytest.approx(0.2)


def test_parse_args_walk_forward_hyperparam_defaults_match_documented_values():
    args = subject.parse_args(
        [
            "walk-forward",
            "--csv",
            "tmp/in",
            "--output-predictions-dir",
            "tmp/out",
        ]
    )
    assert args.bagging_fraction == pytest.approx(0.8)
    assert args.bagging_freq == 1
    assert args.feature_fraction == pytest.approx(0.8)
    assert args.reg_alpha == pytest.approx(0.1)
    assert args.reg_lambda == pytest.approx(0.1)


def test_training_params_from_args_binds_p4b_recommended_overrides():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--bagging-fraction",
            "0.6",
            "--bagging-freq",
            "5",
            "--min-child-samples",
            "50",
            "--feature-fraction",
            "0.85",
            "--reg-alpha",
            "0.2",
            "--reg-lambda",
            "0.2",
        ]
    )
    params = subject.training_params_from_args(args)
    assert params["bagging_fraction"] == pytest.approx(0.6)
    assert params["bagging_freq"] == 5
    assert params["min_child_samples"] == 50
    assert params["feature_fraction"] == pytest.approx(0.85)
    assert params["lambda_l1"] == pytest.approx(0.2)
    assert params["lambda_l2"] == pytest.approx(0.2)


def test_training_params_from_args_preserves_legacy_defaults_when_overrides_absent():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
        ]
    )
    params = subject.training_params_from_args(args)
    assert params["bagging_fraction"] == pytest.approx(0.8)
    assert params["bagging_freq"] == 1
    assert params["feature_fraction"] == pytest.approx(0.8)
    assert params["lambda_l1"] == pytest.approx(0.1)
    assert params["lambda_l2"] == pytest.approx(0.1)
    assert params["num_threads"] == subject.AUTO_NUM_THREADS


def test_training_params_from_args_accepts_num_threads_override():
    args = subject.parse_args(
        [
            "train-cells",
            "--csv",
            "tmp/in",
            "--model-version",
            "rs-cell-v1",
            "--output-root",
            "tmp/model",
            "--output-routing-json",
            "tmp/routing.json",
            "--num-threads",
            "1",
        ]
    )
    params = subject.training_params_from_args(args)
    assert params["num_threads"] == 1


def test_training_params_from_args_accepts_auto_num_threads():
    args = subject.parse_args(
        [
            "train-cells",
            "--csv",
            "tmp/in",
            "--model-version",
            "rs-cell-v1",
            "--output-root",
            "tmp/model",
            "--output-routing-json",
            "tmp/routing.json",
            "--num-threads",
            "auto",
        ]
    )
    params = subject.training_params_from_args(args)
    assert params["num_threads"] == subject.AUTO_NUM_THREADS


def test_resolve_auto_num_threads_scales_with_resource_snapshot():
    healthy: subject.LocalResourceSnapshot = {
        "cpu_count": 10,
        "load_1m": 2.0,
        "total_memory_bytes": 40 * 1024**3,
        "available_memory_bytes": 24 * 1024**3,
        "compressor_bytes": 0,
    }
    pressured: subject.LocalResourceSnapshot = {
        **healthy,
        "available_memory_bytes": 3 * 1024**3,
    }

    assert subject.resolve_auto_num_threads(healthy) == 4
    assert subject.resolve_auto_num_threads(pressured) == 1


def test_resolve_auto_fit_concurrency_scales_with_resource_snapshot():
    healthy: subject.LocalResourceSnapshot = {
        "cpu_count": 12,
        "load_1m": 2.0,
        "total_memory_bytes": 48 * 1024**3,
        "available_memory_bytes": 32 * 1024**3,
        "compressor_bytes": 0,
    }
    loaded: subject.LocalResourceSnapshot = {
        **healthy,
        "load_1m": 8.0,
    }
    pressured: subject.LocalResourceSnapshot = {
        **healthy,
        "available_memory_bytes": 6 * 1024**3,
    }

    assert subject.resolve_auto_fit_concurrency(healthy) == 3
    assert subject.resolve_auto_fit_concurrency(loaded) == 2
    assert subject.resolve_auto_fit_concurrency(pressured) == 1


def test_parse_vm_stat_pages_extracts_numeric_page_counts():
    pages = subject._parse_vm_stat_pages(
        """
Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               804083.
Pages active:                             992273.
Pages occupied by compressor:                  7.
"""
    )

    assert pages["Pages free"] == 804083
    assert pages["Pages active"] == 992273
    assert pages["Pages occupied by compressor"] == 7


def test_resolve_training_params_for_fit_replaces_auto_num_threads(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(subject, "resolve_auto_num_threads", lambda: 2)
    params = subject.default_training_params()
    resolved = subject.resolve_training_params_for_fit(params)
    assert params["num_threads"] == subject.AUTO_NUM_THREADS
    assert resolved["num_threads"] == 2


def test_write_model_metadata_records_hyperparameters_when_provided(tmp_path: Path):
    hyperparameters: subject.TrainingParams = {
        "num_leaves": 63,
        "learning_rate": 0.05,
        "min_child_samples": 50,
        "lambda_l1": 0.2,
        "lambda_l2": 0.2,
        "feature_fraction": 0.85,
        "bagging_fraction": 0.6,
        "bagging_freq": 5,
        "num_iterations": 2000,
        "early_stopping_rounds": 100,
        "num_threads": subject.AUTO_NUM_THREADS,
    }
    subject.write_model_metadata(
        tmp_path,
        "test-version",
        ["feature_a"],
        [],
        500,
        "20050101",
        "20261231",
        with_field_features=True,
        hyperparameters=hyperparameters,
    )
    metadata = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["hyperparameters"]["bagging_fraction"] == pytest.approx(0.6)
    assert metadata["hyperparameters"]["bagging_freq"] == 5
    assert metadata["hyperparameters"]["feature_fraction"] == pytest.approx(0.85)
    assert metadata["hyperparameters"]["lambda_l1"] == pytest.approx(0.2)
    assert metadata["hyperparameters"]["lambda_l2"] == pytest.approx(0.2)


def test_write_model_metadata_omits_hyperparameters_when_absent(tmp_path: Path):
    subject.write_model_metadata(
        tmp_path,
        "test-version",
        ["feature_a"],
        [],
        500,
        "20050101",
        "20261231",
        with_field_features=True,
    )
    metadata = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
    assert "hyperparameters" not in metadata


def test_write_model_metadata_records_class_weight_scheme_when_provided(tmp_path: Path):
    subject.write_model_metadata(
        tmp_path,
        "test-version",
        ["feature_a"],
        [],
        500,
        "20050101",
        "20261231",
        with_field_features=True,
        class_weight_scheme="balanced2",
    )
    metadata = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["class_weight_scheme"] == "balanced2"


def test_write_model_metadata_omits_class_weight_scheme_when_absent(tmp_path: Path):
    subject.write_model_metadata(
        tmp_path,
        "test-version",
        ["feature_a"],
        [],
        500,
        "20050101",
        "20261231",
        with_field_features=True,
    )
    metadata = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
    assert "class_weight_scheme" not in metadata


def test_run_train_production_command_writes_hyperparameters_to_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "data.parquet"
    output_dir = tmp_path / "model"
    df = pl.DataFrame(
        {
            "race_date": ["20240101", "20240102", "20260101", "20260102"],
            "target_running_style_class": [0, 1, 0, 1],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    df.write_parquet(csv_path)

    class _FakeBooster:
        def save_model(self, path: str) -> None:
            Path(path).write_text("fake-model", encoding="utf-8")

    monkeypatch.setattr(subject, "load_dataset_parquet", lambda _path: df.clone())
    monkeypatch.setattr(subject, "maybe_enrich_with_field_features", lambda frame, _enabled: frame)
    monkeypatch.setattr(subject, "train_full_dataset", lambda *_args, **_kwargs: _FakeBooster())
    monkeypatch.setattr("builtins.print", lambda *_args, **_kwargs: None)
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            str(csv_path),
            "--model-version",
            "test-v1",
            "--output-model-dir",
            str(output_dir),
            "--train-start-date",
            "20240101",
            "--train-end-date",
            "20261231",
            "--valid-start-date",
            "20260101",
            "--no-with-field-features",
            "--bagging-fraction",
            "0.6",
            "--bagging-freq",
            "5",
            "--feature-fraction",
            "0.85",
            "--reg-alpha",
            "0.2",
            "--reg-lambda",
            "0.2",
            "--min-child-samples",
            "50",
        ]
    )
    subject.run_train_production_command(args)
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["hyperparameters"]["bagging_fraction"] == pytest.approx(0.6)
    assert metadata["hyperparameters"]["bagging_freq"] == 5
    assert metadata["hyperparameters"]["feature_fraction"] == pytest.approx(0.85)
    assert metadata["hyperparameters"]["lambda_l1"] == pytest.approx(0.2)
    assert metadata["hyperparameters"]["lambda_l2"] == pytest.approx(0.2)
    assert metadata["hyperparameters"]["min_child_samples"] == 50


def test_compute_weighted_sample_weights_balanced2_multiplies_base():
    # nige=class 0 (2 samples), senkou=class 1 (2 samples) — equal base weights before multiplier
    labels = pl.Series([0, 0, 1, 1])
    multipliers: tuple[float, float, float, float] = (0.65, 1.0, 1.0, 0.85)
    weights = subject.compute_weighted_sample_weights(labels, multipliers)
    nige_weight = weights[0]
    senkou_weight = weights[2]
    assert nige_weight == pytest.approx(senkou_weight * 0.65)


def test_resolve_sample_weights_inverse_freq_passthrough():
    labels = pl.Series([0, 0, 1, 1, 2, 3])
    expected = subject.compute_inverse_frequency_weights(labels)
    result = subject.resolve_sample_weights(labels, "inverse_freq")
    np.testing.assert_array_almost_equal(result, expected)


def test_resolve_sample_weights_balanced2_scheme():
    labels = pl.Series([0, 0, 1, 1])
    inverse_freq = subject.compute_inverse_frequency_weights(labels)
    balanced2 = subject.resolve_sample_weights(labels, "balanced2")
    # nige weights should be scaled down vs inverse_freq
    assert balanced2[0] < inverse_freq[0]
    # senkou weights keep 1.0 multiplier so they match inverse_freq
    assert balanced2[2] == pytest.approx(inverse_freq[2])


def test_parse_args_class_weight_scheme_default():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
        ]
    )
    assert args.class_weight_scheme == "inverse_freq"


def test_parse_args_class_weight_scheme_balanced2():
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            "tmp/in",
            "--model-version",
            "prod-v2",
            "--output-model-dir",
            "tmp/model",
            "--class-weight-scheme",
            "balanced2",
        ]
    )
    assert args.class_weight_scheme == "balanced2"


def test_run_train_production_command_enable_walk_forward_writes_results(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "data.parquet"
    output_dir = tmp_path / "model"
    df = pl.DataFrame(
        {
            "race_date": ["20240101", "20240102", "20260101", "20260102"],
            "target_running_style_class": [0, 1, 0, 0],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    df.write_parquet(csv_path)

    class _FakeBooster:
        def save_model(self, path: str) -> None:
            Path(path).write_text("fake-model", encoding="utf-8")

    fake_results: dict[str, subject.WalkForwardEvalMetrics] = {
        "2024": {
            "holdout_year": 2024,
            "train_start_date": "20240101",
            "train_end_date": "20231231",
            "train_rows": 2,
            "valid_rows": 2,
            "precision_nige": 0.52,
            "recall_nige": 0.18,
            "log_loss_nige": 0.50,
            "multi_log_loss": 1.20,
            "accuracy": 0.40,
        }
    }

    monkeypatch.setattr(subject, "load_dataset_parquet", lambda _path: df.clone())
    monkeypatch.setattr(subject, "maybe_enrich_with_field_features", lambda frame, _enabled: frame)
    monkeypatch.setattr(
        subject,
        "train_full_dataset",
        lambda *_args, **_kwargs: _FakeBooster(),
    )
    monkeypatch.setattr(
        subject,
        "run_walk_forward_eval_for_windows",
        lambda *_args, **_kwargs: fake_results,
    )
    monkeypatch.setattr(subject, "compute_production_precision_nige", lambda *_args, **_kwargs: 0.93)
    monkeypatch.setattr("builtins.print", lambda *_args, **_kwargs: None)
    args = subject.parse_args(
        [
            "train-production",
            "--csv",
            str(csv_path),
            "--model-version",
            "test-v1",
            "--output-model-dir",
            str(output_dir),
            "--train-start-date",
            "20240101",
            "--train-end-date",
            "20261231",
            "--valid-start-date",
            "20260101",
            "--no-with-field-features",
            "--enable-walk-forward-eval",
            "--walk-forward-windows",
            "2024",
        ]
    )
    subject.run_train_production_command(args)
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["walk_forward_results"]["2024"]["precision_nige"] == pytest.approx(0.52)
    assert metadata["production_precision_nige"] == pytest.approx(0.93)


def test_derive_running_style_cell_key_matches_production_router_dimensions():
    derive_cell_key = cast(
        Callable[[dict[str, object]], subject.RunningStyleCellKey],
        getattr(subject, "derive_running_style_cell_key"),
    )
    cell = derive_cell_key(
        {
            "source": "jra",
            "grade_code": "G1",
            "kyori": 1599,
            "kaisai_tsukihi": "0415",
            "track_code": "1",
            "keibajo_code": 5,
            "kyoso_joken_code": "OPEN",
        }
    )
    assert cell.category == "jra"
    assert cell.class_label == "G1"
    assert cell.distance_band == "mile"
    assert cell.season == "spring"
    assert cell.surface == "turf"
    assert cell.venue == "05"
    assert cell.subgroup == "OPEN"


@pytest.mark.parametrize(
    ("kyori", "expected"),
    [
        (1199, "sprint"),
        (1200, "mile"),
        (1599, "mile"),
        (1600, "intermediate"),
        (1999, "intermediate"),
        (2000, "long"),
        (2399, "long"),
        (2400, "extended"),
    ],
)
def test_derive_running_style_distance_band_boundaries(kyori: int, expected: str):
    assert subject.derive_running_style_distance_band(kyori) == expected


def test_derive_running_style_cell_key_uses_nar_subclass_for_nar_subgroup():
    cell = subject.derive_running_style_cell_key(
        {
            "source": "nar",
            "grade_code": "C1",
            "kyori": 1400,
            "kaisai_tsukihi": "1201",
            "track_code": "23",
            "keibajo_code": "44",
            "kyoso_joken_code": "JRA_CONDITION",
            "nar_subclass": "NAR_SUB",
        }
    )
    assert cell.category == "nar"
    assert cell.surface == "dirt"
    assert cell.season == "winter"
    assert cell.subgroup == "NAR_SUB"


def test_derive_running_style_cell_key_detects_banei_from_keibajo_code():
    cell = subject.derive_running_style_cell_key(
        {
            "source": "nar",
            "grade_code": "E",
            "kyori": 200,
            "kaisai_tsukihi": "0101",
            "track_code": "23",
            "keibajo_code": "83",
        }
    )
    assert cell.category == "ban-ei"
    assert cell.surface == "dirt"


def test_derive_running_style_distance_band_returns_none_for_blank_or_nan():
    assert subject.derive_running_style_distance_band("") is None
    assert subject.derive_running_style_distance_band(float("nan")) is None


def test_compute_running_style_metrics_includes_top2_logloss_and_per_class_values():
    compute_metrics = cast(
        Callable[[np.ndarray, np.ndarray], dict[str, object]],
        getattr(subject, "compute_running_style_metrics"),
    )
    probabilities = np.array(
        [
            [0.70, 0.20, 0.05, 0.05],
            [0.40, 0.35, 0.20, 0.05],
            [0.10, 0.20, 0.60, 0.10],
            [0.10, 0.10, 0.20, 0.60],
        ]
    )
    actual = np.array([0, 1, 2, 3], dtype=np.int64)
    metrics = compute_metrics(probabilities, actual)
    per_class_accuracy = cast("dict[str, float]", metrics["per_class_accuracy"])
    per_class_f1 = cast("dict[str, float]", metrics["per_class_f1"])
    per_class_precision = cast("dict[str, float]", metrics["per_class_precision"])
    per_class_recall = cast("dict[str, float]", metrics["per_class_recall"])
    per_class_support = cast("dict[str, int]", metrics["per_class_support"])
    race_level = cast("dict[str, float]", metrics["race_level"])
    expected_log_loss = -(np.log(0.70) + np.log(0.35) + np.log(0.60) + np.log(0.60)) / 4.0

    assert metrics["accuracy"] == pytest.approx(0.75)
    assert metrics["top2_accuracy"] == pytest.approx(1.0)
    assert metrics["multi_log_loss"] == pytest.approx(expected_log_loss)
    assert metrics["prediction_count"] == 4
    assert metrics["top2_hit_count"] == 4
    assert per_class_accuracy == {
        "nige": pytest.approx(1.0),
        "senkou": pytest.approx(0.0),
        "sashi": pytest.approx(1.0),
        "oikomi": pytest.approx(1.0),
    }
    assert per_class_f1 == {
        "nige": pytest.approx(2.0 / 3.0),
        "senkou": pytest.approx(0.0),
        "sashi": pytest.approx(1.0),
        "oikomi": pytest.approx(1.0),
    }
    assert per_class_precision["nige"] == pytest.approx(0.5)
    assert per_class_recall["senkou"] == pytest.approx(0.0)
    assert per_class_support == {"nige": 1, "senkou": 1, "sashi": 1, "oikomi": 1}
    assert race_level["race_count"] == 1
    assert race_level["style_distribution_mae"] == pytest.approx(0.125)
    assert race_level["style_count_mae"] == {
        "nige": pytest.approx(1.0),
        "senkou": pytest.approx(1.0),
        "sashi": pytest.approx(0.0),
        "oikomi": pytest.approx(0.0),
    }
    assert race_level["style_count_bias"] == {
        "nige": pytest.approx(1.0),
        "senkou": pytest.approx(-1.0),
        "sashi": pytest.approx(0.0),
        "oikomi": pytest.approx(0.0),
    }
    assert race_level["nige_count_mae"] == pytest.approx(1.0)
    assert race_level["front_group_count_mae"] == pytest.approx(0.0)
    assert np.isnan(race_level["corner_rank_spearman"])
    assert np.isnan(race_level["finish_weighted_accuracy"])
    assert np.isnan(race_level["top1_finish_style_accuracy"])
    assert np.isnan(race_level["top3_finish_style_accuracy"])
    assert metrics["predicted_class_support"] == {
        "nige": 2,
        "senkou": 0,
        "sashi": 1,
        "oikomi": 1,
    }
    assert metrics["confusion_matrix"] == {
        "nige": {"nige": 1, "senkou": 0, "sashi": 0, "oikomi": 0},
        "senkou": {"nige": 1, "senkou": 0, "sashi": 0, "oikomi": 0},
        "sashi": {"nige": 0, "senkou": 0, "sashi": 1, "oikomi": 0},
        "oikomi": {"nige": 0, "senkou": 0, "sashi": 0, "oikomi": 1},
    }
    log_loss_sum = cast("dict[str, float]", metrics["per_class_log_loss_sum"])
    log_loss_count = cast("dict[str, int]", metrics["per_class_log_loss_count"])
    assert log_loss_sum["senkou"] == pytest.approx(-np.log(0.35))
    assert log_loss_count == {"nige": 1, "senkou": 1, "sashi": 1, "oikomi": 1}


def test_compute_running_style_metrics_compares_race_corner_order_and_finish_position():
    compute_metrics = cast(
        Callable[..., dict[str, object]],
        getattr(subject, "compute_running_style_metrics"),
    )
    probabilities = np.array(
        [
            [0.80, 0.15, 0.05, 0.00],
            [0.10, 0.80, 0.10, 0.00],
            [0.00, 0.10, 0.15, 0.75],
        ],
        dtype=np.float64,
    )
    actual = np.array([0, 1, 3], dtype=np.int64)

    metrics = compute_metrics(
        probabilities,
        actual,
        race_ids=np.array(["race-a", "race-a", "race-a"], dtype=object),
        corner1_norm=np.array([0.0, 0.5, 1.0], dtype=np.float64),
        finish_positions=np.array([1.0, 2.0, 3.0], dtype=np.float64),
    )

    race_level = cast("dict[str, float]", metrics["race_level"])
    assert race_level == {
        "race_count": 1,
        "style_distribution_mae": pytest.approx(0.0),
        "style_count_mae": {
            "nige": pytest.approx(0.0),
            "senkou": pytest.approx(0.0),
            "sashi": pytest.approx(0.0),
            "oikomi": pytest.approx(0.0),
        },
        "style_count_bias": {
            "nige": pytest.approx(0.0),
            "senkou": pytest.approx(0.0),
            "sashi": pytest.approx(0.0),
            "oikomi": pytest.approx(0.0),
        },
        "nige_count_mae": pytest.approx(0.0),
        "front_group_count_mae": pytest.approx(0.0),
        "corner_rank_spearman": pytest.approx(1.0),
        "finish_weighted_accuracy": pytest.approx(1.0),
        "top1_finish_style_accuracy": pytest.approx(1.0),
        "top3_finish_style_accuracy": pytest.approx(1.0),
    }


def test_running_style_cell_metrics_for_adoption_maps_running_style_metrics():
    cell = subject.RunningStyleCellKey(
        category="jra",
        class_label="G1",
        distance_band="mile",
        season="spring",
        surface="turf",
        venue="05",
        subgroup="OPEN",
    )
    metrics = subject.compute_running_style_metrics(
        np.array(
            [
                [0.7, 0.2, 0.1, 0.0],
                [0.7, 0.2, 0.1, 0.0],
            ],
            dtype=np.float64,
        ),
        np.array([0, 1], dtype=np.int64),
    )
    adoption = subject.running_style_cell_metrics_for_adoption(
        cell,
        metrics,
        feature_set_hash="hash123",
        race_count=2,
    )
    assert adoption["prediction_target"] == "running_style"
    assert adoption["top1_accuracy"] == pytest.approx(metrics["accuracy"])
    assert adoption["place2_accuracy"] == pytest.approx(metrics["top2_accuracy"])
    assert adoption["place3_accuracy"] == pytest.approx(metrics["macro_f1"])
    assert adoption["accuracy_vector"][:3] == [
        metrics["accuracy"],
        metrics["top2_accuracy"],
        metrics["macro_f1"],
    ]
    assert adoption["cell_vector"] == ["jra", "turf", "mile", "G1", "spring", "05"]
    assert adoption["metric_mapping"] == {
        "top1_accuracy": "accuracy",
        "place2_accuracy": "top2_accuracy",
        "place3_accuracy": "macro_f1",
    }


def test_compute_top2_accuracy_returns_fractional_hit_rate():
    probabilities = np.array(
        [
            [0.6, 0.3, 0.1, 0.0],
            [0.6, 0.3, 0.1, 0.0],
        ],
        dtype=np.float64,
    )
    actual = np.array([1, 2], dtype=np.int64)
    assert subject.compute_top2_accuracy(probabilities, actual) == pytest.approx(0.5)


def test_compute_top2_accuracy_returns_nan_for_empty_actual():
    probabilities = np.empty((0, 4), dtype=np.float64)
    actual = np.array([], dtype=np.int64)
    assert np.isnan(subject.compute_top2_accuracy(probabilities, actual))


def test_compute_per_class_log_loss_clips_extreme_probabilities():
    probabilities = np.array(
        [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
        ],
        dtype=np.float64,
    )
    actual = np.array([0, 1], dtype=np.int64)
    losses = subject.compute_per_class_log_loss(probabilities, actual)
    assert losses["nige"] == pytest.approx(-np.log(1.0 - subject.LOG_LOSS_EPS))
    assert losses["senkou"] == pytest.approx(-np.log(1.0 - subject.LOG_LOSS_EPS))
    assert np.isnan(losses["sashi"])
    assert np.isnan(losses["oikomi"])


def test_json_ready_converts_nested_non_finite_metrics_to_none():
    result = subject.json_ready(
        {
            "metrics": {
                "macro_f1": float("nan"),
                "per_class_log_loss": {"nige": float("inf")},
            }
        }
    )
    assert result == {
        "metrics": {
            "macro_f1": None,
            "per_class_log_loss": {"nige": None},
        }
    }


def test_build_running_style_cell_routing_config_emits_default_variant_and_cell_rule_shape():
    build_routing = cast(
        Callable[[list[dict[str, object]]], dict[str, object]],
        getattr(subject, "build_running_style_cell_routing_config"),
    )
    routing = build_routing(
        [
            {
                "category": "jra",
                "variant_id": "cell-jra-class-nige-dist-sprint-season-spring-surface-turf-venue-05-subgroup-open",
                "model_key": "running-style/models/jra/cells/rs-cell-v1-cell-jra.flatbin",
                "conditions": [
                    {"dimension": "class", "values": ["nige"]},
                    {"dimension": "distance_band", "values": ["sprint"]},
                    {"dimension": "season", "values": ["spring"]},
                    {"dimension": "surface", "values": ["turf"]},
                    {"dimension": "venue", "values": ["05"]},
                    {"dimension": "subgroup", "values": ["OPEN"]},
                ],
            }
        ]
    )
    jra_routing = cast("dict[str, object]", routing["jra"])
    variants = cast("dict[str, dict[str, str]]", jra_routing["variants"])
    rules = cast("list[dict[str, object]]", jra_routing["rules"])
    rule = rules[0]

    assert jra_routing["defaultVariantId"] == "latest"
    assert variants["latest"] == {"modelKey": "running-style/models/jra/latest.flatbin"}
    assert variants["cell-jra-class-nige-dist-sprint-season-spring-surface-turf-venue-05-subgroup-open"] == {
        "modelKey": "running-style/models/jra/cells/rs-cell-v1-cell-jra.flatbin",
    }
    assert rule == {
        "conditions": [
            {"dimension": "class", "values": ["nige"]},
            {"dimension": "distance_band", "values": ["sprint"]},
            {"dimension": "season", "values": ["spring"]},
            {"dimension": "surface", "values": ["turf"]},
            {"dimension": "venue", "values": ["05"]},
            {"dimension": "subgroup", "values": ["OPEN"]},
        ],
        "variantId": "cell-jra-class-nige-dist-sprint-season-spring-surface-turf-venue-05-subgroup-open",
    }


def test_eligibility_rejections_requires_validation_class_coverage():
    train_df = pl.DataFrame({"target_running_style_class": [0, 1, 0, 1]})
    valid_df = pl.DataFrame({"target_running_style_class": [0, 0, 0]})
    rejections = subject.eligibility_rejections(
        train_df,
        valid_df,
        min_train_rows=1,
        min_valid_rows=1,
        min_classes=2,
    )
    assert rejections == ["valid_classes 1 < 2"]


def test_parse_args_train_cells_accepts_cells_json_and_routing_output():
    args = subject.parse_args(
        [
            "train-cells",
            "--csv",
            "tmp/in",
            "--model-version",
            "rs-cell-v1",
            "--output-root",
            "tmp/models",
            "--output-routing-json",
            "tmp/cell_routing.json",
            "--output-metrics-json",
            "tmp/cell_metrics.json",
        ]
    )
    assert args.command == "train-cells"
    assert args.train_start_date == "20050101"
    assert args.train_end_date == "20261231"
    assert args.valid_start_date == "20260101"
    assert args.class_weight_scheme == "inverse_freq"
    assert args.with_field_features is True
    assert args.cell_feature_selection_json is None


def test_resolve_cell_feature_selection_uses_matching_routing_rule(tmp_path: Path):
    selection_path = tmp_path / "cell_routing.json"
    selection_path.write_text(
        json.dumps(
            {
                "jra": {
                    "default_variant": "sim",
                    "variants": {
                        "sim": {"model_version": "default"},
                        "cell-hashAAAA": {
                            "model_version": "cell-hashAAAA",
                            "feature_set_hash": "hashAAAA1",
                            "feature_names": ["feature_b", "feature_a"],
                        },
                    },
                    "rules": [
                        {
                            "conditions": [
                                {"dimension": "class", "values": ["G1"]},
                                {"dimension": "distance_band", "values": ["sprint"]},
                                {"dimension": "season", "values": ["spring"]},
                                {"dimension": "surface", "values": ["turf"]},
                                {"dimension": "venue", "values": ["05"]},
                            ],
                            "variant": "cell-hashAAAA",
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    rules = subject.load_cell_feature_selection_rules(selection_path)
    features, feature_set_hash = subject.resolve_cell_feature_selection(
        subject.RunningStyleCellKey(
            category="jra",
            class_label="G1",
            distance_band="sprint",
            season="spring",
            surface="turf",
            venue="05",
            subgroup="OPEN",
        ),
        ["feature_a", "feature_b", "feature_c"],
        rules,
    )
    assert features == ["feature_a", "feature_b"]
    assert feature_set_hash == "hashAAAA1"


def test_load_cell_feature_selection_rules_accepts_worker_variant_id(
    tmp_path: Path,
):
    selection_path = tmp_path / "cell_routing.json"
    selection_path.write_text(
        json.dumps(
            {
                "jra": {
                    "defaultVariantId": "latest",
                    "variants": {
                        "latest": {"modelKey": "running-style/models/jra/latest.flatbin"},
                        "tokyo-turf": {
                            "modelKey": "running-style/models/jra/cells/tokyo-turf.flatbin",
                            "feature_set_hash": "hashTOKYO1",
                            "feature_names": ["feature_b", "feature_a"],
                        },
                    },
                    "rules": [
                        {
                            "conditions": [
                                {"dimension": "venue", "values": ["05"]},
                                {"dimension": "surface", "values": ["turf"]},
                            ],
                            "variantId": "tokyo-turf",
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    rules = subject.load_cell_feature_selection_rules(selection_path)

    assert rules == [
        subject.CellFeatureSelectionRule(
            category="jra",
            conditions=(
                ("venue", ("05",)),
                ("surface", ("turf",)),
            ),
            feature_names=("feature_a", "feature_b"),
            feature_set_hash="hashTOKYO1",
        )
    ]


def test_load_cell_feature_selection_rules_accepts_explicit_schema(
    tmp_path: Path,
):
    selection_path = tmp_path / "feature_selection.json"
    selection_path.write_text(
        json.dumps(
            {
                "rules": [
                    {
                        "category": "nar",
                        "conditions": [
                            {"dimension": "class", "values": ["A"]},
                        ],
                        "feature_set_hash": "hashNAR1",
                        "feature_names": ["feature_c", "feature_a"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    rules = subject.load_cell_feature_selection_rules(selection_path)

    assert rules == [
        subject.CellFeatureSelectionRule(
            category="nar",
            conditions=(("class", ("A",)),),
            feature_names=("feature_a", "feature_c"),
            feature_set_hash="hashNAR1",
        )
    ]


def test_load_cell_feature_selection_rules_rejects_rules_without_feature_names(
    tmp_path: Path,
):
    selection_path = tmp_path / "cell_routing.json"
    selection_path.write_text(
        json.dumps(
            {
                "jra": {
                    "defaultVariantId": "latest",
                    "variants": {
                        "latest": {"modelKey": "running-style/models/jra/latest.flatbin"},
                        "tokyo-turf": {
                            "modelKey": "running-style/models/jra/cells/tokyo-turf.flatbin"
                        },
                    },
                    "rules": [
                        {
                            "conditions": [
                                {"dimension": "venue", "values": ["05"]},
                            ],
                            "variantId": "tokyo-turf",
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no usable feature-selection rules"):
        subject.load_cell_feature_selection_rules(selection_path)


def test_run_train_cells_command_trains_cells_saves_models_and_writes_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    run_command = cast(
        Callable[[object], None],
        getattr(subject, "run_train_cells_command"),
    )
    df = pl.DataFrame(
        {
            "source": ["jra", "jra", "jra", "jra"],
            "grade_code": ["G1", "G1", "G2", "G2"],
            "target_running_style_class": [0, 0, 1, 1],
            "kyori": [1000, 1000, 1500, 1500],
            "kaisai_tsukihi": ["0415", "0416", "0715", "0716"],
            "track_code": ["1", "1", "2", "2"],
            "keibajo_code": ["05", "05", "06", "06"],
            "kyoso_joken_code": ["OPEN", "OPEN", "OPEN", "OPEN"],
            "race_date": ["20250101", "20260101", "20250102", "20260102"],
            "feature_a": [1.0, 0.9, 0.2, 0.1],
        }
    )
    trained_shapes: list[tuple[int, int]] = []
    saved_paths: list[str] = []

    class _FakeBooster:
        def save_model(self, path: str) -> None:
            saved_paths.append(path)
            Path(path).write_text("fake-model", encoding="utf-8")

    def _fake_train_head(
        train_df: pl.DataFrame,
        valid_df: pl.DataFrame,
        _feature_columns: list[str],
        _categorical_features: list[str],
        _params: subject.TrainingParams,
        *,
        class_weight_scheme: str = "inverse_freq",
    ) -> tuple[_FakeBooster, np.ndarray]:
        assert class_weight_scheme == "balanced2"
        trained_shapes.append((len(train_df), len(valid_df)))
        probabilities = np.repeat(
            np.array([[0.70, 0.20, 0.05, 0.05]], dtype=np.float64),
            len(valid_df),
            axis=0,
        )
        return _FakeBooster(), probabilities

    output_root = tmp_path / "models"
    routing_path = tmp_path / "cell_routing.json"
    metrics_path = tmp_path / "cell_metrics.json"
    monkeypatch.setattr(subject, "load_dataset_parquet", lambda _path: df.clone())
    monkeypatch.setattr(subject, "maybe_enrich_with_field_features", lambda frame, _enabled: frame)
    monkeypatch.setattr(subject, "train_running_style_head", _fake_train_head)
    monkeypatch.setattr("builtins.print", lambda *_args, **_kwargs: None)
    args = subject.parse_args(
        [
            "train-cells",
            "--csv",
            "tmp/in",
            "--model-version",
            "rs-cell-v1",
            "--output-root",
            str(output_root),
            "--output-routing-json",
            str(routing_path),
            "--output-metrics-json",
            str(metrics_path),
            "--train-start-date",
            "20250101",
            "--train-end-date",
            "20261231",
            "--valid-start-date",
            "20260101",
            "--min-train-rows",
            "1",
            "--min-valid-rows",
            "1",
            "--min-classes",
            "1",
            "--class-weight-scheme",
            "balanced2",
            "--no-with-field-features",
        ]
    )
    run_command(args)

    assert trained_shapes == [(1, 1), (1, 1)]
    assert len(saved_paths) == 2
    assert routing_path.exists()
    assert metrics_path.exists()
    routing = json.loads(routing_path.read_text(encoding="utf-8"))
    jra_routing = routing["jra"]
    model_keys = [
        variant["modelKey"]
        for variant_id, variant in jra_routing["variants"].items()
        if variant_id != "latest"
    ]
    assert model_keys
    assert all(str(model_key).endswith(".flatbin") for model_key in model_keys)
    metrics_payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    trained_cells = metrics_payload["trained_cells"]
    assert trained_cells[0]["feature_count"] > 0
    assert len(str(trained_cells[0]["feature_set_hash"])) == 64
    cell_eval = trained_cells[0]["cell_training_evaluation"]
    assert cell_eval["prediction_target"] == "running_style"
    assert cell_eval["subgroup"] == trained_cells[0]["cell"]["subgroup"]
    assert cell_eval["top1_accuracy"] == trained_cells[0]["metrics"]["accuracy"]
    assert cell_eval["place2_accuracy"] == trained_cells[0]["metrics"]["top2_accuracy"]
    assert cell_eval["place3_accuracy"] == trained_cells[0]["metrics"]["macro_f1"]
    assert cell_eval["prediction_count"] == trained_cells[0]["metrics"]["prediction_count"]


def test_save_running_style_cell_training_evaluations_uses_cell_accuracy_store(
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[dict[str, object]] = []

    class _FakeStore:
        def __init__(self, pg_url: str) -> None:
            self.pg_url: str = pg_url

        def __enter__(self) -> "_FakeStore":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def save_cell_metrics(
            self,
            feature_set_hash: str,
            feature_count: int,
            metrics: list[object],
            feature_names: list[str],
            *,
            prediction_target: str,
        ) -> int:
            calls.append(
                {
                    "pg_url": self.pg_url,
                    "feature_set_hash": feature_set_hash,
                    "feature_count": feature_count,
                    "metrics": metrics,
                    "feature_names": feature_names,
                    "prediction_target": prediction_target,
                }
            )
            return len(metrics)

    monkeypatch.setattr("learning.continuous_learner.CellAccuracyStore", _FakeStore)
    metrics = {
        "prediction_target": "running_style",
        "feature_set_hash": "a" * 64,
        "category": "jra",
        "surface": "turf",
        "distance_band": "sprint",
        "class_label": "open",
        "season": "spring",
        "venue": "05",
        "subgroup": "OPEN",
        "race_count": 12,
        "ndcg_at_3": 0.4,
        "top1_accuracy": 0.4,
        "place2_accuracy": 0.6,
        "place3_accuracy": 0.3,
        "place4_accuracy": 0.0,
        "place5_accuracy": 0.0,
        "place6_accuracy": 0.0,
        "top3_box_accuracy": 0.0,
    }

    saved = subject.save_running_style_cell_training_evaluations(
        [
            {
                "feature_set_hash": "a" * 64,
                "feature_columns": ["feature_b", "feature_a"],
                "cell_training_evaluation": metrics,
            },
            {
                "feature_set_hash": "a" * 64,
                "feature_columns": ["feature_b", "feature_a"],
                "cell_training_evaluation": {**metrics, "venue": "06"},
            },
            {
                "feature_set_hash": "b" * 64,
                "feature_columns": ["feature_c"],
            },
        ],
        pg_url="postgresql://local/test",
    )

    assert saved == 2
    assert len(calls) == 1
    assert calls[0]["pg_url"] == "postgresql://local/test"
    assert calls[0]["feature_set_hash"] == "a" * 64
    forwarded_metrics = calls[0]["metrics"]
    assert isinstance(forwarded_metrics, list)
    first_metric = cast(Mapping[str, object], forwarded_metrics[0])
    assert first_metric["subgroup"] == "OPEN"
    assert calls[0]["feature_count"] == 2
    assert calls[0]["feature_names"] == ["feature_b", "feature_a"]
    assert calls[0]["prediction_target"] == "running_style"
    assert len(cast(list[object], calls[0]["metrics"])) == 2


def test_parse_train_cells_accepts_postgres_persistence_options(tmp_path: Path):
    args = subject.parse_args(
        [
            "train-cells",
            "--csv",
            "tmp/in",
            "--model-version",
            "rs-cell-v1",
            "--output-root",
            str(tmp_path / "models"),
            "--output-routing-json",
            str(tmp_path / "routing.json"),
            "--pg-url",
            "postgresql://local/test",
            "--save-cell-metrics-to-postgres",
        ]
    )

    assert args.pg_url == "postgresql://local/test"
    assert args.save_cell_metrics_to_postgres is True
