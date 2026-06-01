from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import lightgbm as lgb
import numpy as np
import pandas as pd
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


def test_detect_categorical_features_returns_only_known_categoricals():
    feature_columns = ["track_code", "kyori_band", "speed_index_avg_5", "grade_code"]
    detected = subject.detect_categorical_features(feature_columns)
    assert sorted(detected) == ["grade_code", "kyori_band", "track_code"]


def test_compute_inverse_frequency_weights_balances_classes():
    labels = pd.Series([0, 0, 1, 1, 1, 1, 2, 2, 2, 3])
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
    labels = pd.Series([0, 0, 1, 1])
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


def test_macro_f1_skips_classes_with_undefined_precision():
    precision = {"nige": float("nan"), "senkou": 1.0, "sashi": 0.5, "oikomi": 0.5}
    recall = {"nige": float("nan"), "senkou": 1.0, "sashi": 1.0, "oikomi": 0.5}
    macro_f1 = subject.macro_f1_from_precision_recall(precision, recall)
    # nige skipped, only 3 classes contribute
    assert not np.isnan(macro_f1)


def test_filter_labeled_rows_drops_null_target():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "target_running_style_class": [0, None, 2],
        }
    )
    filtered = subject.filter_labeled_rows(df)
    assert len(filtered) == 2
    assert filtered["a"].tolist() == [1, 3]


def test_split_by_year_separates_train_and_validation():
    df = pd.DataFrame(
        {
            "race_date": ["20230101", "20240615", "20250301"],
            "race_year": [2023, 2024, 2025],
            "value": [1, 2, 3],
        }
    )
    train, valid = subject.split_by_year(df, "20230101", 2025)
    assert train["value"].tolist() == [1, 2]
    assert valid["value"].tolist() == [3]


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
    valid_df = pd.DataFrame(
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
    assert output["predicted_label"].tolist() == ["nige", "sashi"]
    assert output["predicted_class"].tolist() == [0, 2]
    assert output["p_nige"].tolist() == [0.6, 0.1]
    assert output["p_sashi"].tolist() == [0.1, 0.7]


def test_lgb_params_for_multiclass_sets_objective_and_num_class():
    params = subject.lgb_params_for_multiclass(subject.default_training_params())
    assert params["objective"] == "multiclass"
    assert params["num_class"] == 4
    assert params["metric"] == "multi_logloss"


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
    df = pd.DataFrame(
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
    df = pd.DataFrame(
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
    df = pd.DataFrame(
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
        train_df: pd.DataFrame,
        valid_df: pd.DataFrame,
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
        _df: pd.DataFrame,
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
    df = pd.DataFrame({"race_date": [], "race_year": [], "target_running_style_class": []})
    results = subject.run_walk_forward_eval_for_windows(
        df, [2024, 2025], "20050101", [], [], subject.default_training_params(),
    )
    captured = capsys.readouterr()
    assert captured_years == [2024, 2025]
    assert sorted(results.keys()) == ["2024", "2025"]
    assert "walk_forward_eval" in captured.out


def test_compute_production_precision_nige_returns_nan_when_valid_empty(monkeypatch: pytest.MonkeyPatch):
    train_subset = pd.DataFrame(
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
    train_subset = pd.DataFrame(
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
        frame: pd.DataFrame,
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
    df = pd.DataFrame(
        {
            "race_date": ["20240101", "20240102", "20260101", "20260102"],
            "target_running_style_class": [0, 1, 0, 1],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    df.to_parquet(csv_path)

    class _FakeBooster:
        def save_model(self, path: str) -> None:
            Path(path).write_text("fake-model", encoding="utf-8")

    def _fake_load(_path: Path) -> pd.DataFrame:
        return df.copy()

    def _fake_train_full(
        _train_df: pd.DataFrame,
        _features: list[str],
        _cats: list[str],
        _params: subject.TrainingParams,
        *,
        valid_start_date: str | None = None,
    ) -> _FakeBooster:
        assert valid_start_date == "20260101"
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


def test_run_train_production_command_writes_hyperparameters_to_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "data.parquet"
    output_dir = tmp_path / "model"
    df = pd.DataFrame(
        {
            "race_date": ["20240101", "20240102", "20260101", "20260102"],
            "target_running_style_class": [0, 1, 0, 1],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    df.to_parquet(csv_path)

    class _FakeBooster:
        def save_model(self, path: str) -> None:
            Path(path).write_text("fake-model", encoding="utf-8")

    monkeypatch.setattr(subject, "load_dataset_parquet", lambda _path: df.copy())
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


def test_run_train_production_command_enable_walk_forward_writes_results(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "data.parquet"
    output_dir = tmp_path / "model"
    df = pd.DataFrame(
        {
            "race_date": ["20240101", "20240102", "20260101", "20260102"],
            "target_running_style_class": [0, 1, 0, 0],
            "feature_a": [1.0, 0.5, 0.4, 0.2],
        }
    )
    df.to_parquet(csv_path)

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

    monkeypatch.setattr(subject, "load_dataset_parquet", lambda _path: df.copy())
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
