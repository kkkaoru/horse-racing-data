from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import corner_position_lightgbm as subject


def test_default_training_params_uses_documented_defaults():
    params = subject.default_training_params()
    assert params["num_leaves"] == 63
    assert params["learning_rate"] == 0.05
    assert params["min_child_samples"] == 30
    assert params["lambda_l1"] == 0.1
    assert params["lambda_l2"] == 0.1
    assert params["feature_fraction"] == 0.8


def test_resolve_feature_columns_excludes_meta_and_label_columns():
    columns = [
        "source",
        "race_date",
        "kaisai_nen",
        "race_id",
        "finish_position",
        "finish_norm",
        "target_corner_1_norm",
        "target_running_style_class",
        "speed_index_avg_5",
        "past_nige_rate_self",
    ]
    features = subject.resolve_feature_columns(columns)
    assert features == ["speed_index_avg_5", "past_nige_rate_self"]


def test_detect_categorical_features_returns_only_known_categoricals():
    feature_columns = ["track_code", "grade_code", "kyori_band", "speed_index_avg_5"]
    detected = subject.detect_categorical_features(feature_columns)
    assert detected == ["track_code", "grade_code", "kyori_band"]


def test_parse_validation_years_accepts_comma_separated():
    assert subject.parse_validation_years("2024,2025") == [2024, 2025]
    assert subject.parse_validation_years(" 2024 , 2025 ") == [2024, 2025]


def test_parse_validation_years_handles_single_year():
    assert subject.parse_validation_years("2025") == [2025]


def test_parse_args_walk_forward_requires_csv_and_output():
    args = subject.parse_args(
        [
            "walk-forward",
            "--csv",
            "tmp/in",
            "--output-predictions-dir",
            "tmp/out",
        ]
    )
    assert args.command == "walk-forward"
    assert args.csv == Path("tmp/in")
    assert args.output_predictions_dir == Path("tmp/out")
    assert args.train_start_date == "20160101"
    assert args.validation_years == "2024,2025"


def test_corner_head_targets_cover_three_corners():
    assert set(subject.CORNER_HEAD_TARGETS.keys()) == {"corner_1", "corner_3", "corner_4"}
    assert subject.CORNER_HEAD_TARGETS["corner_1"] == "target_corner_1_norm"


def test_mae_for_head_ignores_null_targets():
    predictions = np.array([0.10, 0.20, 0.40])
    target = pl.Series([0.15, None, 0.30])
    mae = subject.mae_for_head(predictions, target)
    expected = (abs(0.10 - 0.15) + abs(0.40 - 0.30)) / 2
    assert mae == pytest.approx(expected)


def test_mae_for_head_returns_nan_when_all_targets_null():
    predictions = np.array([0.1, 0.2])
    target = pl.Series([None, None], dtype=pl.Float64)
    mae = subject.mae_for_head(predictions, target)
    assert np.isnan(mae)


def test_filter_target_rows_drops_null_targets():
    df = pl.DataFrame({"a": [1, 2, 3], "target_corner_1_norm": [0.1, None, 0.5]})
    filtered = subject.filter_target_rows(df, "target_corner_1_norm")
    assert filtered.height == 2
    assert filtered["a"].to_list() == [1, 3]


def test_split_by_year_partitions_train_and_validation():
    df = pl.DataFrame(
        {
            "race_date": ["20230101", "20240615", "20250301", "20250915"],
            "race_year": [2023, 2024, 2025, 2025],
            "value": [1, 2, 3, 4],
        }
    )
    train, valid = subject.split_by_year(df, "20230101", 2025)
    assert train["value"].to_list() == [1, 2]
    assert valid["value"].to_list() == [3, 4]


def test_split_by_year_excludes_rows_before_train_start():
    df = pl.DataFrame(
        {
            "race_date": ["20100101", "20240615"],
            "race_year": [2010, 2024],
            "value": [9, 8],
        }
    )
    train, _ = subject.split_by_year(df, "20200101", 2025)
    assert train["value"].to_list() == [8]


def test_encode_categoricals_converts_listed_columns_to_category():
    df = pl.DataFrame({"track_code": ["11", "12"], "speed_index_avg_5": [1.0, 2.0]})
    encoded = subject.encode_categoricals(df, ["track_code"])
    assert encoded["track_code"].dtype == pl.Categorical
    assert encoded["speed_index_avg_5"].dtype == pl.Float64


def test_compute_corner_1_top3_agreement_perfect_match():
    df = pl.DataFrame(
        {
            "race_id": ["r1", "r1", "r1", "r1"],
            "corner_1_pred": [0.10, 0.20, 0.30, 0.80],
            "target_corner_1_norm": [0.10, 0.20, 0.30, 0.80],
        }
    )
    agreement = subject.compute_corner_1_top3_agreement(df)
    assert agreement == pytest.approx(1.0)


def test_compute_corner_1_top3_agreement_full_mismatch():
    df = pl.DataFrame(
        {
            "race_id": ["r1", "r1", "r1", "r1"],
            "corner_1_pred": [0.80, 0.70, 0.60, 0.10],
            "target_corner_1_norm": [0.10, 0.20, 0.30, 0.80],
        }
    )
    agreement = subject.compute_corner_1_top3_agreement(df)
    assert agreement < 1.0


def test_build_predictions_df_emits_required_columns():
    valid_df = pl.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["h1", "h2"],
            "umaban": [1, 2],
            "race_year": [2025, 2025],
            "target_corner_1_norm": [0.1, 0.5],
            "target_corner_3_norm": [0.2, 0.4],
            "target_corner_4_norm": [0.3, 0.3],
        }
    )
    head_preds = {
        "corner_1": np.array([0.12, 0.48]),
        "corner_3": np.array([0.22, 0.42]),
        "corner_4": np.array([0.32, 0.32]),
    }
    predictions_df = subject.build_predictions_df(valid_df, head_preds)
    assert "corner_1_pred" in predictions_df.columns
    assert "corner_3_pred" in predictions_df.columns
    assert "corner_4_pred" in predictions_df.columns
    assert predictions_df["corner_1_pred"].to_list() == [0.12, 0.48]


def test_write_predictions_jsonl_writes_each_row(tmp_path: Path):
    df = pl.DataFrame(
        {
            "race_id": ["r1"],
            "umaban": [1],
            "corner_1_pred": [0.1],
        }
    )
    output_path = tmp_path / "out.jsonl"
    subject.write_predictions_jsonl(df, output_path)
    contents = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(contents) == 1
    parsed = json.loads(contents[0])
    assert parsed == {"race_id": "r1", "umaban": 1, "corner_1_pred": 0.1}


def test_write_walk_forward_report_aggregates_per_head_mae(tmp_path: Path):
    folds: list[subject.FoldMetrics] = [
        {
            "validation_year": 2024,
            "train_rows": 100,
            "valid_rows": 50,
            "per_head_mae": {"corner_1": 0.15, "corner_3": 0.10, "corner_4": 0.12},
            "corner_1_top3_agreement": 0.35,
        },
        {
            "validation_year": 2025,
            "train_rows": 150,
            "valid_rows": 60,
            "per_head_mae": {"corner_1": 0.13, "corner_3": 0.09, "corner_4": 0.11},
            "corner_1_top3_agreement": 0.40,
        },
    ]
    output_path = tmp_path / "report.json"
    subject.write_walk_forward_report(folds, output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["aggregate"]["per_head_mae_mean"]["corner_1"] == pytest.approx(0.14)
    assert payload["aggregate"]["corner_1_top3_agreement_mean"] == pytest.approx(0.375)
    assert len(payload["folds"]) == 2
