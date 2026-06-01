from __future__ import annotations

import json
from pathlib import Path

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
