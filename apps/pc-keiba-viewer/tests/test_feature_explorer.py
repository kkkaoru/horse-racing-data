"""Tests for feature_explorer module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import feature_explorer as subject
from feature_registry import FeatureRegistry
from finish_position_lightgbm import META_COLUMNS


def _make_df() -> pd.DataFrame:
    rows = []
    for year in [2022, 2023]:
        for race in range(5):
            for horse in range(4):
                rows.append({
                    "source": "jra",
                    "race_date": f"{year}0601",
                    "kaisai_nen": str(year),
                    "kaisai_tsukihi": "0601",
                    "keibajo_code": "10",
                    "race_bango": f"{race:02d}",
                    "ketto_toroku_bango": f"horse_{horse:03d}",
                    "umaban": horse + 1,
                    "category": "jra",
                    "race_id": f"{year}_race_{race:02d}",
                    "race_year": year,
                    "feature_schema_version": "1",
                    "finish_position": horse + 1,
                    "finish_norm": 0.5,
                    "target_corner_1_norm": 0.5,
                    "target_corner_3_norm": 0.5,
                    "target_corner_4_norm": 0.5,
                    "target_running_style_class": 0,
                    "feat_speed": float(horse),
                    "feat_jockey": 0.3,
                })
    return pd.DataFrame(rows)


def test_select_features_keeps_selected_feature_columns() -> None:
    df = _make_df()
    mask = {"feat_speed": True, "feat_jockey": False}
    result = subject.select_features(df, mask)
    assert "feat_speed" in result.columns
    assert "feat_jockey" not in result.columns


def test_select_features_always_keeps_meta_columns() -> None:
    df = _make_df()
    mask = {"feat_speed": False, "feat_jockey": False}
    result = subject.select_features(df, mask)
    for col in META_COLUMNS:
        if col in df.columns:
            assert col in result.columns


def test_select_features_keeps_label_columns() -> None:
    df = _make_df()
    mask = {"feat_speed": True, "feat_jockey": False}
    result = subject.select_features(df, mask)
    assert "finish_position" in result.columns
    assert "finish_norm" in result.columns


def test_select_features_all_false_mask_returns_only_meta_label_cols() -> None:
    df = _make_df()
    mask = {"feat_speed": False, "feat_jockey": False}
    result = subject.select_features(df, mask)
    assert "feat_speed" not in result.columns
    assert "feat_jockey" not in result.columns
    assert "finish_position" in result.columns
    assert "race_id" in result.columns


def test_evaluate_feature_set_returns_mean_of_both_folds() -> None:
    rows = []
    for year in [2021, 2022, 2023]:
        for race in range(5):
            for horse in range(4):
                rows.append({
                    "source": "jra",
                    "race_date": f"{year}0601",
                    "kaisai_nen": str(year),
                    "kaisai_tsukihi": "0601",
                    "keibajo_code": "10",
                    "race_bango": f"{race:02d}",
                    "ketto_toroku_bango": f"horse_{horse:03d}",
                    "umaban": horse + 1,
                    "category": "jra",
                    "race_id": f"{year}_race_{race:02d}",
                    "race_year": year,
                    "feature_schema_version": "1",
                    "finish_position": horse + 1,
                    "finish_norm": 0.5,
                    "target_corner_1_norm": 0.5,
                    "target_corner_3_norm": 0.5,
                    "target_corner_4_norm": 0.5,
                    "target_running_style_class": 0,
                    "feat_speed": float(horse),
                    "feat_jockey": 0.3,
                })
    df = pd.DataFrame(rows)
    mock_metrics_2022 = {
        "ndcg_at_3": 0.60,
        "race_count": 5,
        "top1_accuracy": 0.3,
        "top3_box_accuracy": 0.1,
        "top3_exact_accuracy": 0.02,
        "valid_rows": 20,
        "valid_year": 2022,
    }
    mock_metrics_2023 = {
        "ndcg_at_3": 0.90,
        "race_count": 5,
        "top1_accuracy": 0.3,
        "top3_box_accuracy": 0.1,
        "top3_exact_accuracy": 0.02,
        "valid_rows": 20,
        "valid_year": 2023,
    }
    params = subject.DEFAULT_PARAMS
    with patch(
        "feature_explorer.run_walk_forward_fold",
        side_effect=[
            (MagicMock(), MagicMock(), mock_metrics_2022),
            (MagicMock(), MagicMock(), mock_metrics_2023),
        ],
    ):
        result = subject.evaluate_feature_set(
            df, ["feat_speed", "feat_jockey"], [2022, 2023], "20160101", params
        )
    assert result == pytest.approx(0.75)


def test_evaluate_feature_set_single_fold_returns_that_ndcg() -> None:
    df = _make_df()
    mock_metrics = {
        "ndcg_at_3": 0.75,
        "race_count": 100,
        "top1_accuracy": 0.3,
        "top3_box_accuracy": 0.1,
        "top3_exact_accuracy": 0.02,
        "valid_rows": 1000,
        "valid_year": 2023,
    }
    params = subject.DEFAULT_PARAMS
    with patch(
        "feature_explorer.run_walk_forward_fold",
        return_value=(MagicMock(), MagicMock(), mock_metrics),
    ):
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2023], "20160101", params
        )
    assert result == pytest.approx(0.75)


def test_evaluate_feature_set_skips_year_with_empty_train() -> None:
    rows = []
    for year in [2022, 2023]:
        for race in range(5):
            for horse in range(4):
                rows.append({
                    "source": "jra",
                    "race_date": f"{year}0601",
                    "kaisai_nen": str(year),
                    "kaisai_tsukihi": "0601",
                    "keibajo_code": "10",
                    "race_bango": f"{race:02d}",
                    "ketto_toroku_bango": f"horse_{horse:03d}",
                    "umaban": horse + 1,
                    "category": "jra",
                    "race_id": f"{year}_race_{race:02d}",
                    "race_year": year,
                    "feature_schema_version": "1",
                    "finish_position": horse + 1,
                    "finish_norm": 0.5,
                    "target_corner_1_norm": 0.5,
                    "target_corner_3_norm": 0.5,
                    "target_corner_4_norm": 0.5,
                    "target_running_style_class": 0,
                    "feat_speed": float(horse),
                    "feat_jockey": 0.3,
                })
    df = pd.DataFrame(rows)
    mock_metrics = {
        "ndcg_at_3": 0.75,
        "race_count": 100,
        "top1_accuracy": 0.3,
        "top3_box_accuracy": 0.1,
        "top3_exact_accuracy": 0.02,
        "valid_rows": 1000,
        "valid_year": 2023,
    }
    params = subject.DEFAULT_PARAMS
    with patch(
        "feature_explorer.run_walk_forward_fold",
        return_value=(MagicMock(), MagicMock(), mock_metrics),
    ) as mock_fold:
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2022, 2023], "20160101", params
        )
    assert mock_fold.call_count == 1
    assert result == pytest.approx(0.75)


def test_evaluate_feature_set_no_valid_folds_returns_zero() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    with patch("feature_explorer.run_walk_forward_fold") as mock_fold:
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2020], "20250101", params
        )
    mock_fold.assert_not_called()
    assert result == 0.0


def test_build_objective_triggers_run_walk_forward_fold_and_maybe_promote() -> None:
    rows = []
    for year in [2022, 2023]:
        for race in range(5):
            for horse in range(4):
                rows.append({
                    "source": "jra",
                    "race_date": f"{year}0601",
                    "kaisai_nen": str(year),
                    "kaisai_tsukihi": "0601",
                    "keibajo_code": "10",
                    "race_bango": f"{race:02d}",
                    "ketto_toroku_bango": f"horse_{horse:03d}",
                    "umaban": horse + 1,
                    "category": "jra",
                    "race_id": f"{year}_race_{race:02d}",
                    "race_year": year,
                    "feature_schema_version": "1",
                    "finish_position": horse + 1,
                    "finish_norm": 0.5,
                    "target_corner_1_norm": 0.5,
                    "target_corner_3_norm": 0.5,
                    "target_corner_4_norm": 0.5,
                    "target_running_style_class": 0,
                    "feat_a": float(horse),
                    "feat_b": 0.3,
                    "feat_c": 0.1,
                    "feat_d": 0.2,
                    "feat_e": 0.4,
                    "feat_f": 0.5,
                })
    df = pd.DataFrame(rows)
    mock_metrics = {
        "ndcg_at_3": 0.75,
        "race_count": 100,
        "top1_accuracy": 0.3,
        "top3_box_accuracy": 0.1,
        "top3_exact_accuracy": 0.02,
        "valid_rows": 1000,
        "valid_year": 2023,
    }
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "feature_explorer.run_walk_forward_fold",
            return_value=(MagicMock(), MagicMock(), mock_metrics),
        ) as mock_fold:
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
            )
            trial = MagicMock()
            trial.number = 0
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            result = objective(trial)
        assert mock_fold.call_count >= 1
        assert result == pytest.approx(0.75)
        assert registry.get_best_ndcg() == pytest.approx(0.75)


def test_build_objective_returns_zero_when_selected_below_min_features() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_speed", "feat_jockey"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch("feature_explorer.run_walk_forward_fold") as mock_fold:
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
            )
            trial = MagicMock()
            trial.number = 0
            trial.suggest_categorical.side_effect = [False, False]
            result = objective(trial)
        mock_fold.assert_not_called()
    assert result == 0.0


def test_run_exploration_returns_list_of_exploration_results() -> None:
    df = _make_df()
    mock_metrics = {
        "ndcg_at_3": 0.75,
        "race_count": 100,
        "top1_accuracy": 0.3,
        "top3_box_accuracy": 0.1,
        "top3_exact_accuracy": 0.02,
        "valid_rows": 1000,
        "valid_year": 2023,
    }
    params = subject.DEFAULT_PARAMS
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "feature_explorer.run_walk_forward_fold",
            return_value=(MagicMock(), MagicMock(), mock_metrics),
        ):
            results = subject.run_exploration(
                df,
                registry,
                n_trials=1,
                validation_years=[2023],
                train_start="20160101",
                params=params,
                study_name="test_exploration",
            )
    assert isinstance(results, list)
    assert len(results) >= 0
    for r in results:
        assert "trial_id" in r
        assert "ndcg_at_3" in r
        assert "feature_names" in r
        assert "promoted" in r


def test_run_exploration_excludes_trials_with_none_value() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    mock_trial_with_value = MagicMock()
    mock_trial_with_value.value = 0.75
    mock_trial_with_value.number = 0
    mock_trial_with_value.params = {}

    mock_trial_none = MagicMock()
    mock_trial_none.value = None
    mock_trial_none.number = 1
    mock_trial_none.params = {}

    mock_study = MagicMock()
    mock_study.trials = [mock_trial_with_value, mock_trial_none]

    with FeatureRegistry(Path(":memory:")) as registry:
        with patch("feature_explorer.optuna.create_study", return_value=mock_study):
            with patch("feature_explorer.run_walk_forward_fold"):
                results = subject.run_exploration(
                    df,
                    registry,
                    n_trials=2,
                    validation_years=[2023],
                    train_start="20160101",
                    params=params,
                    study_name="test_exploration",
                )
    assert len(results) == 1
    assert results[0]["trial_id"] == "test_exploration_trial_0"
    assert results[0]["ndcg_at_3"] == pytest.approx(0.75)


def test_select_features_returns_copy_not_view() -> None:
    df = _make_df()
    mask = {"feat_speed": True, "feat_jockey": True}
    result = subject.select_features(df, mask)
    result["feat_speed"] = 999.0
    assert df["feat_speed"].iloc[0] != 999.0


def test_evaluate_feature_set_filters_to_correct_columns() -> None:
    df = _make_df()
    mock_metrics = {
        "ndcg_at_3": 0.80,
        "race_count": 5,
        "top1_accuracy": 0.4,
        "top3_box_accuracy": 0.2,
        "top3_exact_accuracy": 0.05,
        "valid_rows": 20,
        "valid_year": 2023,
    }
    params = subject.DEFAULT_PARAMS
    captured_fold: list[object] = []
    with patch(
        "feature_explorer.run_walk_forward_fold",
        side_effect=lambda fold, p: captured_fold.append(fold) or (MagicMock(), MagicMock(), mock_metrics),
    ):
        subject.evaluate_feature_set(
            df, ["feat_speed"], [2023], "20160101", params
        )
    assert len(captured_fold) == 1
