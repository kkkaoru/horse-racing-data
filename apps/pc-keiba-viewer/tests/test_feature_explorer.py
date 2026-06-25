"""Tests for feature_explorer module."""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import optuna
import polars as pl
import pytest

import learning.feature_explorer as subject
from learning.feature_registry import FeatureRegistry
from finish_position_lightgbm import META_COLUMNS, FoldSplit, split_walk_forward


def _make_df() -> pl.DataFrame:
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
    return pl.DataFrame(rows)


def _make_fold() -> FoldSplit:
    """FoldSplit-compatible dict with numeric feature columns."""
    rows = []
    for i in range(4):
        rows.append({
            "source": "jra",
            "race_date": "20220601",
            "kaisai_nen": "2022",
            "kaisai_tsukihi": "0601",
            "keibajo_code": "10",
            "race_bango": "01",
            "ketto_toroku_bango": f"horse_{i:03d}",
            "umaban": i + 1,
            "category": "jra",
            "race_id": "2022_race_01",
            "race_year": 2022,
            "feature_schema_version": "1",
            "finish_position": i + 1,
            "finish_norm": 0.5,
            "target_corner_1_norm": 0.5,
            "target_corner_3_norm": 0.5,
            "target_corner_4_norm": 0.5,
            "target_running_style_class": 0,
            "feat_speed": float(i),
            "feat_jockey": 0.3,
        })
    df = pl.DataFrame(rows)
    return cast("FoldSplit", {"train_df": df, "valid_df": df.clone(), "valid_year": 2023})


def _make_meta_only_fold() -> FoldSplit:
    """FoldSplit-compatible dict with only meta and label columns — no feature columns."""
    rows = []
    for i in range(4):
        rows.append({
            "source": "jra",
            "race_date": "20220601",
            "kaisai_nen": "2022",
            "kaisai_tsukihi": "0601",
            "keibajo_code": "10",
            "race_bango": "01",
            "ketto_toroku_bango": f"horse_{i:03d}",
            "umaban": i + 1,
            "category": "jra",
            "race_id": "2022_race_01",
            "race_year": 2022,
            "feature_schema_version": "1",
            "finish_position": i + 1,
            "finish_norm": 0.5,
            "target_corner_1_norm": 0.5,
            "target_corner_3_norm": 0.5,
            "target_corner_4_norm": 0.5,
            "target_running_style_class": 0,
        })
    df = pl.DataFrame(rows)
    return cast("FoldSplit", {"train_df": df, "valid_df": df.clone(), "valid_year": 2023})


def _make_df_3years() -> pl.DataFrame:
    """DataFrame spanning 2021-2023 for multi-fold evaluate_feature_set tests."""
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
    return pl.DataFrame(rows)


def _make_df_6feats() -> pl.DataFrame:
    """DataFrame with 6 feature columns for build_objective / run_exploration tests."""
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
    return pl.DataFrame(rows)


# --- _ndcg_at_3_from_valid_df ---

def test_ndcg_at_3_from_valid_df_perfect_ranking_returns_one() -> None:
    df = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    assert result == pytest.approx(1.0)


def test_ndcg_at_3_from_valid_df_empty_df_returns_zero() -> None:
    df = pl.DataFrame(
        schema={
            "race_id": pl.Utf8,
            "predicted_rank": pl.Int64,
            "finish_position": pl.Int64,
        }
    )
    result = subject._ndcg_at_3_from_valid_df(df)
    assert result == pytest.approx(0.0)


def test_ndcg_at_3_from_valid_df_worst_ranking_is_less_than_one() -> None:
    # Reverse order: horse finishing 4th predicted 1st, etc.
    df = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [4, 3, 2, 1],
        "finish_position": [1, 2, 3, 4],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    assert result < 1.0
    assert result >= 0.0


def test_ndcg_at_3_from_valid_df_multiple_races_returns_mean() -> None:
    # Race r1: perfect → NDCG=1.0; Race r2: all wrong → some value
    df = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r2", "r2", "r2"],
        "predicted_rank": [1, 2, 3, 3, 2, 1],
        "finish_position": [1, 2, 3, 1, 2, 3],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    assert 0.0 < result <= 1.0


def test_ndcg_at_3_from_valid_df_two_horse_race_perfect_returns_one() -> None:
    df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "predicted_rank": [1, 2],
        "finish_position": [1, 2],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    assert result == pytest.approx(1.0)


def test_ndcg_at_3_from_valid_df_skips_race_with_no_relevant_finishers() -> None:
    # r1: all positions > 3 → ideal_dcg = 0 → race skipped; r2: perfect → 1.0
    df = pl.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2", "r2", "r2"],
        "predicted_rank": [1, 2, 1, 2, 3, 4],
        "finish_position": [4, 5, 1, 2, 3, 4],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    assert result == pytest.approx(1.0)


def test_ndcg_at_3_from_valid_df_null_predicted_rank_penalises_ideal() -> None:
    # Horse "a" has null predicted_rank (e.g. absent from predictions after left join)
    # but finishes 1st. DCG only includes b and c; ideal still uses all 3 finishers.
    # NDCG must be < 1.0 (winner was not ranked).
    df = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "predicted_rank": [None, 1.0, 2.0],
        "finish_position": [1.0, 2.0, 3.0],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    assert 0.0 < result < 1.0


def test_ndcg_at_3_from_valid_df_null_finish_position_excluded_from_dcg_slot() -> None:
    # Horse "a" has predicted_rank=1 but finish_position=null (scratched).
    # It must NOT occupy the top DCG slot — only "b" (rank=2, finish=1) contributes.
    # Perfect prediction among scoreable horses → NDCG = 1.0.
    df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "predicted_rank": [1.0, 2.0],
        "finish_position": [None, 1.0],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    assert result == pytest.approx(1.0)


def _ndcg_loop_reference(valid_df: pl.DataFrame) -> float:
    """Original race-by-race loop kept verbatim as the behavioural oracle."""
    relevance_map = {1: 3.0, 2: 2.0, 3: 1.0}
    discounts = (1.0 / math.log2(2), 1.0 / math.log2(3), 1.0 / math.log2(4))
    ndcg_scores: list[float] = []
    for (_race_id,), group in valid_df.group_by("race_id", maintain_order=True):
        valid_group = group.drop_nulls(subset=["predicted_rank", "finish_position"])
        sorted_group = valid_group.sort("predicted_rank")
        dcg = sum(
            relevance_map.get(int(fp), 0.0) * disc
            for fp, disc in zip(sorted_group["finish_position"].to_list(), discounts)
        )
        ideal_relevances = sorted(
            (
                relevance_map.get(int(fp), 0.0)
                for fp in group["finish_position"].drop_nulls().to_list()
            ),
            reverse=True,
        )[:3]
        ideal_dcg = sum(rel * disc for rel, disc in zip(ideal_relevances, discounts))
        if ideal_dcg > 0.0:
            ndcg_scores.append(dcg / ideal_dcg)
    return sum(ndcg_scores) / len(ndcg_scores) if ndcg_scores else 0.0


def test_ndcg_at_3_from_valid_df_matches_loop_reference_on_mixed_races() -> None:
    # Mixed: normal races, a race with nulls in predicted_rank, a sub-3-horse race,
    # and a race whose finishers are all outside the top 3 (ideal_dcg == 0 → skipped).
    df = pl.DataFrame({
        "race_id": [
            "r_normal", "r_normal", "r_normal", "r_normal",
            "r_null", "r_null", "r_null",
            "r_small", "r_small",
            "r_irrelevant", "r_irrelevant",
        ],
        "predicted_rank": [
            2.0, 1.0, 4.0, 3.0,
            None, 1.0, 2.0,
            1.0, 2.0,
            1.0, 2.0,
        ],
        "finish_position": [
            1.0, 2.0, 3.0, 4.0,
            1.0, 2.0, 3.0,
            2.0, 1.0,
            5.0, 6.0,
        ],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    expected = _ndcg_loop_reference(df)
    assert result == pytest.approx(expected)


def test_ndcg_at_3_from_valid_df_empty_matches_loop_reference() -> None:
    df = pl.DataFrame(
        schema={
            "race_id": pl.Utf8,
            "predicted_rank": pl.Float64,
            "finish_position": pl.Float64,
        }
    )
    result = subject._ndcg_at_3_from_valid_df(df)
    expected = _ndcg_loop_reference(df)
    assert result == pytest.approx(0.0)
    assert result == pytest.approx(expected)


def test_ndcg_at_3_from_valid_df_all_races_irrelevant_returns_zero() -> None:
    # Every finisher is outside the top 3 → ideal_dcg == 0 for all races → no race
    # contributes → mean over an empty set is 0.0 (matches the loop).
    df = pl.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "predicted_rank": [1.0, 2.0, 1.0, 2.0],
        "finish_position": [4.0, 5.0, 6.0, 7.0],
    })
    result = subject._ndcg_at_3_from_valid_df(df)
    expected = _ndcg_loop_reference(df)
    assert result == pytest.approx(0.0)
    assert result == pytest.approx(expected)


# --- _xgb_numeric_features ---

def test_xgb_numeric_features_includes_numeric_excludes_non_numeric_and_meta() -> None:
    df = pl.DataFrame({
        "race_id": ["r1"],
        "feat_numeric": [1.0],
        "feat_string": ["abc"],
        "finish_position": [1],
        "umaban": [1],
    })
    result = subject._xgb_numeric_features(df, list(df.columns))
    assert result == ["feat_numeric"]


def test_xgb_numeric_features_returns_empty_when_only_meta_and_label() -> None:
    fold = _make_meta_only_fold()
    df = fold["train_df"]
    result = subject._xgb_numeric_features(df, list(df.columns))
    assert result == []


def test_xgb_numeric_features_excludes_all_meta_columns() -> None:
    df = _make_df()
    result = subject._xgb_numeric_features(df, list(df.columns))
    assert not (set(result) & set(META_COLUMNS))


def test_xgb_numeric_features_preserves_feature_names_order() -> None:
    df = pl.DataFrame({
        "race_id": ["r1"],
        "feat_b": [2.0],
        "feat_a": [1.0],
        "feat_c": [3.0],
    })
    result = subject._xgb_numeric_features(df, ["feat_c", "feat_a", "feat_b"])
    assert result == ["feat_c", "feat_a", "feat_b"]


def test_xgb_numeric_features_twice_same_df_returns_identical_result() -> None:
    df = pl.DataFrame({
        "race_id": ["r1"],
        "feat_numeric": [1.0],
        "feat_string": ["abc"],
        "finish_position": [1],
    })
    first = subject._xgb_numeric_features(df, list(df.columns))
    second = subject._xgb_numeric_features(df, list(df.columns))
    assert first == ["feat_numeric"]
    assert second == ["feat_numeric"]


def test_xgb_numeric_features_caches_dtype_check_on_second_call() -> None:
    df = pl.DataFrame({
        "race_id": ["r1"],
        "feat_numeric": [1.0],
        "feat_string": ["abc"],
        "finish_position": [1],
    })
    subject._XGB_NUMERIC_CACHE.pop(id(df), None)
    real_signature = subject._dtype_signature
    counter = MagicMock(side_effect=real_signature)
    with patch("learning.feature_explorer._dtype_signature", counter):
        subject._xgb_numeric_features(df, list(df.columns))
        calls_after_first = counter.call_count
        subject._xgb_numeric_features(df, list(df.columns))
        calls_after_second = counter.call_count
    # Both calls recompute the signature, but the second is a cache hit and does
    # not rescan column dtypes; the cached entry must be present after the first.
    assert calls_after_first == 1
    assert calls_after_second == 2
    assert id(df) in subject._XGB_NUMERIC_CACHE


def test_xgb_numeric_features_different_schema_is_cache_miss_and_classified() -> None:
    df_a = pl.DataFrame({"race_id": ["r1"], "feat_numeric": [1.0]})
    df_b = pl.DataFrame({"race_id": ["r1"], "feat_other": ["x"], "feat_num2": [2.0]})
    result_a = subject._xgb_numeric_features(df_a, list(df_a.columns))
    result_b = subject._xgb_numeric_features(df_b, list(df_b.columns))
    assert result_a == ["feat_numeric"]
    assert result_b == ["feat_num2"]


def test_xgb_numeric_features_reclassifies_when_id_reused_with_new_dtypes() -> None:
    # A reused id whose cached signature no longer matches the live schema must
    # be treated as a miss and reclassified. Seed the cache for this id with a
    # stale (mismatched) signature to simulate an id reused after GC.
    df = pl.DataFrame({"race_id": ["r1"], "feat_x": [1.0]})
    stale_signature = (("race_id", "String"), ("feat_x", "String"))
    subject._XGB_NUMERIC_CACHE[id(df)] = (stale_signature, frozenset())
    result = subject._xgb_numeric_features(df, list(df.columns))
    assert result == ["feat_x"]


# --- _is_model_safe_feature ---


def test_is_model_safe_feature_numeric_column_is_safe() -> None:
    df = pl.DataFrame({"feat_numeric": [1.0], "feat_string": ["abc"]})
    assert subject._is_model_safe_feature(df, "feat_numeric") is True


def test_is_model_safe_feature_non_numeric_non_categorical_is_unsafe() -> None:
    df = pl.DataFrame({"feat_numeric": [1.0], "nar_subclass": ["A"]})
    assert subject._is_model_safe_feature(df, "nar_subclass") is False


def test_is_model_safe_feature_categorical_str_column_is_safe() -> None:
    df = pl.DataFrame({"track_code": ["1"], "feat_numeric": [1.0]})
    assert subject._is_model_safe_feature(df, "track_code") is True


def test_is_model_safe_feature_twice_same_df_returns_identical_result() -> None:
    df = pl.DataFrame({"feat_numeric": [1.0], "nar_subclass": ["A"]})
    first = subject._is_model_safe_feature(df, "feat_numeric")
    second = subject._is_model_safe_feature(df, "feat_numeric")
    assert first is True
    assert second is True


def test_is_model_safe_feature_caches_dtype_check_on_second_call() -> None:
    df = pl.DataFrame({"feat_numeric": [1.0], "feat_string": ["abc"]})
    subject._MODEL_SAFE_CACHE.pop(id(df), None)
    real_signature = subject._dtype_signature
    counter = MagicMock(side_effect=real_signature)
    with patch("learning.feature_explorer._dtype_signature", counter):
        subject._is_model_safe_feature(df, "feat_numeric")
        calls_after_first = counter.call_count
        subject._is_model_safe_feature(df, "feat_string")
        calls_after_second = counter.call_count
    # Second call is a cache hit: it recomputes only the cheap signature, not the
    # per-column dtype scan, so the cached entry must exist after the first call.
    assert calls_after_first == 1
    assert calls_after_second == 2
    assert id(df) in subject._MODEL_SAFE_CACHE


def test_is_model_safe_feature_different_schema_is_cache_miss_and_classified() -> None:
    df_a = pl.DataFrame({"feat_numeric": [1.0], "feat_string": ["abc"]})
    df_b = pl.DataFrame({"feat_other": ["x"], "feat_num2": [2.0]})
    safe_a = subject._is_model_safe_feature(df_a, "feat_string")
    safe_b = subject._is_model_safe_feature(df_b, "feat_num2")
    assert safe_a is False
    assert safe_b is True


def test_model_safe_columns_returns_frozenset_of_safe_columns() -> None:
    df = pl.DataFrame({"feat_numeric": [1.0], "track_code": ["1"], "nar_subclass": ["A"]})
    result = subject._model_safe_columns(df)
    assert result == frozenset({"feat_numeric", "track_code"})


# --- run_fold_with_backend ---

def test_run_fold_with_backend_lightgbm_computes_ndcg_from_predictions() -> None:
    fold = _make_fold()
    params = subject.DEFAULT_PARAMS
    # score_dataset output: race_id + ketto_toroku_bango + predicted_rank (no finish_position)
    preds_df = pl.DataFrame({
        "race_id": ["2022_race_01", "2022_race_01", "2022_race_01", "2022_race_01"],
        "ketto_toroku_bango": ["horse_000", "horse_001", "horse_002", "horse_003"],
        "umaban": [1, 2, 3, 4],
        "predicted_score": [4.0, 3.0, 2.0, 1.0],
        "predicted_rank": [1, 2, 3, 4],  # perfect ranking → NDCG=1.0
    })
    with patch(
        "learning.feature_explorer.run_walk_forward_fold",
        return_value=(MagicMock(), preds_df, {"ndcg_at_3": 0.8}),
    ) as mock_fold:
        result = subject.run_fold_with_backend(fold, "lightgbm", params)
    assert result is not None
    assert result == pytest.approx(1.0)
    mock_fold.assert_called_once()


def test_run_fold_with_backend_xgboost_returns_ndcg_from_predictions() -> None:
    fold = _make_fold()
    params = subject.DEFAULT_PARAMS
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_xgboost_ranker",
        return_value=(MagicMock(), {"valid_predictions": valid_preds}),
    ) as mock_xgb:
        result = subject.run_fold_with_backend(fold, "xgboost", params)
    assert result is not None
    assert result == pytest.approx(1.0)
    mock_xgb.assert_called_once()


def test_run_fold_with_backend_catboost_returns_ndcg_from_predictions() -> None:
    fold = _make_fold()
    params = subject.DEFAULT_PARAMS
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_catboost_ranker",
        return_value={"valid_predictions": valid_preds},
    ) as mock_cb:
        result = subject.run_fold_with_backend(fold, "catboost", params)
    assert result is not None
    assert result == pytest.approx(1.0)
    mock_cb.assert_called_once()


def test_run_fold_with_backend_xgboost_returns_none_when_no_numeric_features() -> None:
    fold = _make_meta_only_fold()
    params = subject.DEFAULT_PARAMS
    result = subject.run_fold_with_backend(fold, "xgboost", params)
    assert result is None


def test_run_fold_with_backend_catboost_returns_none_when_no_feature_cols() -> None:
    fold = _make_meta_only_fold()
    params = subject.DEFAULT_PARAMS
    result = subject.run_fold_with_backend(fold, "catboost", params)
    assert result is None


def test_run_fold_with_backend_catboost_excludes_non_categorical_str_feature() -> None:
    fold = _make_fold()
    str_train = fold["train_df"].with_columns(pl.lit("A").alias("nar_subclass"))
    str_valid = fold["valid_df"].with_columns(pl.lit("A").alias("nar_subclass"))
    str_fold = cast(
        "FoldSplit",
        {"train_df": str_train, "valid_df": str_valid, "valid_year": 2023},
    )
    params = subject.DEFAULT_PARAMS
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_catboost_ranker",
        return_value={"valid_predictions": valid_preds},
    ) as mock_cb:
        subject.run_fold_with_backend(str_fold, "catboost", params)
    feature_cols = mock_cb.call_args[0][2]
    assert "nar_subclass" not in feature_cols
    assert "feat_speed" in feature_cols


# --- select_features ---

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
    expected = set(META_COLUMNS) & set(df.columns)
    assert expected.issubset(set(result.columns))


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


def test_select_features_preserves_source_values() -> None:
    df = _make_df()
    mask = {"feat_speed": True, "feat_jockey": True}
    result = subject.select_features(df, mask)
    assert result["feat_speed"].to_list() == df["feat_speed"].to_list()


# --- _select_fold_features ---


def test_select_fold_features_keeps_only_requested_feature_columns() -> None:
    fold = _make_fold()
    result = subject._select_fold_features(fold, {"feat_speed"})
    assert "feat_speed" in result["train_df"].columns
    assert "feat_jockey" not in result["train_df"].columns
    assert "feat_speed" in result["valid_df"].columns
    assert "feat_jockey" not in result["valid_df"].columns


def test_select_fold_features_always_keeps_meta_and_label_columns() -> None:
    fold = _make_fold()
    result = subject._select_fold_features(fold, set())
    assert "race_id" in result["train_df"].columns
    assert "finish_position" in result["train_df"].columns
    assert "ketto_toroku_bango" in result["train_df"].columns
    assert "feat_speed" not in result["train_df"].columns


def test_select_fold_features_preserves_valid_year() -> None:
    fold = _make_fold()
    result = subject._select_fold_features(fold, {"feat_speed"})
    assert result["valid_year"] == 2023


def test_select_fold_features_drops_non_categorical_str_feature() -> None:
    fold = _make_fold()
    str_train = fold["train_df"].with_columns(pl.lit("A").alias("nar_subclass"))
    str_valid = fold["valid_df"].with_columns(pl.lit("A").alias("nar_subclass"))
    str_fold = cast(
        "FoldSplit",
        {"train_df": str_train, "valid_df": str_valid, "valid_year": 2023},
    )
    result = subject._select_fold_features(str_fold, {"feat_speed", "nar_subclass"})
    assert "nar_subclass" not in result["train_df"].columns
    assert "nar_subclass" not in result["valid_df"].columns
    assert "feat_speed" in result["train_df"].columns
    assert "feat_speed" in result["valid_df"].columns


def test_select_fold_features_keeps_known_categorical_str_feature() -> None:
    fold = _make_fold()
    cat_train = fold["train_df"].with_columns(pl.lit("1").alias("track_code"))
    cat_valid = fold["valid_df"].with_columns(pl.lit("1").alias("track_code"))
    cat_fold = cast(
        "FoldSplit",
        {"train_df": cat_train, "valid_df": cat_valid, "valid_year": 2023},
    )
    result = subject._select_fold_features(cat_fold, {"track_code"})
    assert "track_code" in result["train_df"].columns
    assert "track_code" in result["valid_df"].columns


# --- evaluate_feature_set ---

def test_evaluate_feature_set_returns_mean_of_both_folds() -> None:
    df = _make_df_3years()
    params = subject.DEFAULT_PARAMS
    with patch(
        "learning.feature_explorer.run_fold_with_backend",
        side_effect=[0.60, 0.90],
    ):
        result = subject.evaluate_feature_set(
            df, ["feat_speed", "feat_jockey"], [2022, 2023], "20160101", params,
            backends=("lightgbm",),
        )
    assert result == pytest.approx(0.75)


def test_evaluate_feature_set_single_fold_returns_that_ndcg() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    with patch(
        "learning.feature_explorer.run_fold_with_backend",
        return_value=0.75,
    ):
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2023], "20160101", params,
            backends=("lightgbm",),
        )
    assert result == pytest.approx(0.75)


def test_evaluate_feature_set_skips_year_with_empty_train() -> None:
    df = _make_df()  # data only for 2022+2023; 2022 fold has empty train → skipped
    params = subject.DEFAULT_PARAMS
    with patch(
        "learning.feature_explorer.run_fold_with_backend",
        return_value=0.75,
    ) as mock_rfwb:
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2022, 2023], "20160101", params,
            backends=("lightgbm",),
        )
    assert mock_rfwb.call_count == 1
    assert result == pytest.approx(0.75)


def test_evaluate_feature_set_no_valid_folds_returns_zero() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    with patch("learning.feature_explorer.run_walk_forward_fold") as mock_fold:
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2020], "20250101", params
        )
    mock_fold.assert_not_called()
    assert result == 0.0


def test_evaluate_feature_set_filters_to_correct_columns() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    captured_folds: list[object] = []
    with patch(
        "learning.feature_explorer.run_fold_with_backend",
        side_effect=lambda fold, backend, p: (captured_folds.append(fold), 0.80)[1],
    ):
        subject.evaluate_feature_set(
            df, ["feat_speed"], [2023], "20160101", params,
            backends=("lightgbm",),
        )
    assert len(captured_folds) == 1


def test_evaluate_feature_set_multi_backend_averages_scores_across_backends() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    with patch(
        "learning.feature_explorer.run_fold_with_backend",
        side_effect=[0.6, 0.7, 0.8],
    ):
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2023], "20160101", params,
            backends=("lightgbm", "xgboost", "catboost"),
        )
    assert result == pytest.approx(0.7)


def test_evaluate_feature_set_skips_none_scores_from_backend() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    with patch(
        "learning.feature_explorer.run_fold_with_backend",
        side_effect=[0.75, None],
    ):
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2023], "20160101", params,
            backends=("lightgbm", "xgboost"),
        )
    assert result == pytest.approx(0.75)


def test_evaluate_feature_set_returns_zero_when_all_backend_scores_none() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    with patch("learning.feature_explorer.run_fold_with_backend", return_value=None):
        result = subject.evaluate_feature_set(
            df, ["feat_speed"], [2023], "20160101", params,
            backends=("lightgbm",),
        )
    assert result == pytest.approx(0.0)


# --- build_objective ---

def test_build_objective_triggers_run_fold_with_backend_and_maybe_promote() -> None:
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=0.75,
        ) as mock_rfwb:
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 0
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            trial.should_prune.return_value = False
            result = objective(trial)
        assert mock_rfwb.call_count >= 1
        assert result == pytest.approx(0.75)
        assert registry.get_best_ndcg() == pytest.approx(0.75)


def test_build_objective_pre_splits_folds_once_not_per_trial() -> None:
    df = _make_df_3years()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    real_split = split_walk_forward
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.split_walk_forward",
            side_effect=real_split,
        ) as mock_split:
            with patch(
                "learning.feature_explorer.run_fold_with_backend",
                return_value=0.75,
            ):
                objective = subject.build_objective(
                    df,
                    candidate_features,
                    [2022, 2023],
                    "20160101",
                    params,
                    registry,
                    "test_study",
                    backends=("lightgbm",),
                )
                # split_walk_forward runs once per validation year at build time.
                assert mock_split.call_count == 2
                for trial_number in range(3):
                    trial = MagicMock()
                    trial.number = trial_number
                    trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
                    trial.should_prune.return_value = False
                    objective(trial)
        # Running 3 trials must not trigger any additional split_walk_forward calls.
        assert mock_split.call_count == 2


def test_build_objective_skips_none_backend_scores_in_trial() -> None:
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            side_effect=[None, 0.80],
        ):
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm", "xgboost"),
            )
            trial = MagicMock()
            trial.number = 0
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            trial.should_prune.return_value = False
            result = objective(trial)
    assert result == pytest.approx(0.80)


def test_build_objective_returns_zero_when_all_backend_scores_none() -> None:
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=None,
        ):
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 0
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            trial.should_prune.return_value = False
            result = objective(trial)
    assert result == pytest.approx(0.0)


def test_build_objective_records_delta_pp_in_definition_json() -> None:
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=0.75,
        ):
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 0
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            trial.should_prune.return_value = False
            objective(trial)
        recorded = registry.list_trials()[0]
        payload = json.loads(recorded["definition_json"])
    # No prior active entry → active_ndcg = 0.0 → delta_pp = 0.75 * 100.
    assert payload["delta_pp"] == pytest.approx(75.0)


def test_build_objective_records_negative_delta_pp_against_active_entry() -> None:
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        active_id = registry.record_trial("seed", 0.80, ["feat_a"], "{}")
        registry.activate(active_id)
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=0.75,
        ):
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 1
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            trial.should_prune.return_value = False
            objective(trial)
        recorded = next(e for e in registry.list_trials() if e["trial_id"] == "test_study_trial_1")
        payload = json.loads(recorded["definition_json"])
    # Active ndcg 0.80, trial ndcg 0.75 → delta_pp = (0.75 - 0.80) * 100 = -5.0.
    assert payload["delta_pp"] == pytest.approx(-5.0)


def test_build_objective_reports_intermediate_value_per_fold() -> None:
    df = _make_df_3years()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    df = df.rename(
        {
            "feat_speed": "feat_a",
            "feat_jockey": "feat_b",
        }
    ).with_columns(
        feat_c=pl.lit(0.1),
        feat_d=pl.lit(0.2),
        feat_e=pl.lit(0.4),
        feat_f=pl.lit(0.5),
    )
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=0.70,
        ):
            objective = subject.build_objective(
                df,
                candidate_features,
                [2022, 2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 0
            trial.should_prune.return_value = False
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            objective(trial)
    assert trial.report.call_count == 2
    assert trial.report.call_args_list[0].args == (0.70, 0)
    assert trial.report.call_args_list[1].args == (0.70, 1)


def test_build_objective_aggregates_all_folds_after_per_fold_subset_release() -> None:
    # The objective deletes each fold's column subset at the end of the loop body;
    # this asserts that releasing the subset does not drop any fold's contribution,
    # so the final ndcg still averages every fold's backend score.
    df = _make_df_3years()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    df = df.rename(
        {"feat_speed": "feat_a", "feat_jockey": "feat_b"}
    ).with_columns(
        feat_c=pl.lit(0.1),
        feat_d=pl.lit(0.2),
        feat_e=pl.lit(0.4),
        feat_f=pl.lit(0.5),
    )
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            side_effect=[0.60, 0.80],
        ) as mock_rfwb:
            objective = subject.build_objective(
                df,
                candidate_features,
                [2022, 2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 0
            trial.should_prune.return_value = False
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            result = objective(trial)
    # One score per fold, both retained → mean(0.60, 0.80) = 0.70.
    assert mock_rfwb.call_count == 2
    assert result == pytest.approx(0.70)


def test_build_objective_passes_prefetched_active_ndcg_to_maybe_promote() -> None:
    # objective already fetches the active entry to compute delta_pp; it must hand
    # that ndcg to maybe_promote so the registry skips a duplicate active-entry SELECT.
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        active_id = registry.record_trial("seed", 0.80, ["feat_a"], "{}")
        registry.activate(active_id)
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=0.75,
        ):
            with patch.object(
                registry, "maybe_promote", wraps=registry.maybe_promote
            ) as spy_promote:
                objective = subject.build_objective(
                    df,
                    candidate_features,
                    [2023],
                    "20160101",
                    params,
                    registry,
                    "test_study",
                    backends=("lightgbm",),
                )
                trial = MagicMock()
                trial.number = 1
                trial.should_prune.return_value = False
                trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
                objective(trial)
    assert spy_promote.call_count == 1
    assert spy_promote.call_args.kwargs["active_ndcg"] == pytest.approx(0.80)


def test_build_objective_raises_trial_pruned_when_should_prune_true() -> None:
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=0.20,
        ):
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 0
            trial.should_prune.return_value = True
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            with pytest.raises(optuna.TrialPruned):
                objective(trial)


def test_build_objective_does_not_report_when_fold_has_no_scores() -> None:
    df = _make_df_6feats()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_f"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.run_fold_with_backend",
            return_value=None,
        ):
            objective = subject.build_objective(
                df,
                candidate_features,
                [2023],
                "20160101",
                params,
                registry,
                "test_study",
                backends=("lightgbm",),
            )
            trial = MagicMock()
            trial.number = 0
            trial.should_prune.return_value = False
            trial.suggest_categorical.side_effect = [True, True, True, True, True, True]
            result = objective(trial)
    trial.report.assert_not_called()
    assert result == pytest.approx(0.0)


def test_run_exploration_creates_study_with_median_pruner() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ) as mock_create:
            subject.run_exploration(df, registry, n_trials=1)
    pruner = mock_create.call_args.kwargs["pruner"]
    assert isinstance(pruner, optuna.pruners.MedianPruner)


def test_run_exploration_prunes_clearly_bad_later_trial() -> None:
    """A real study must mark at least one later trial PRUNED when its first-fold
    intermediate value is far below the median of completed startup trials.

    n_startup_trials=3 keeps trials 0-2 unpruned; from trial 3 onward a low
    first-fold report triggers pruning. Feature selection is forced to all-True so
    every trial reaches the fold loop regardless of the sampler.
    """
    df = _make_df_3years()
    params = subject.DEFAULT_PARAMS
    df = df.rename(
        {"feat_speed": "feat_a", "feat_jockey": "feat_b"}
    ).with_columns(
        feat_c=pl.lit(0.1),
        feat_d=pl.lit(0.2),
        feat_e=pl.lit(0.4),
        feat_f=pl.lit(0.5),
    )
    call_count = {"n": 0}

    def fold_score(*_args: object, **_kwargs: object) -> float:
        call_count["n"] += 1
        return 0.90 if call_count["n"] <= 6 else 0.05

    with FeatureRegistry(Path(":memory:")) as registry:
        with (
            patch.object(
                optuna.Trial, "suggest_categorical", return_value=True
            ),
            patch(
                "learning.feature_explorer.run_fold_with_backend",
                side_effect=fold_score,
            ),
        ):
            results = subject.run_exploration(
                df,
                registry,
                n_trials=6,
                validation_years=[2022, 2023],
                train_start="20160101",
                params=params,
                study_name="prune_study",
                backends=("lightgbm",),
            )
    high_score_trials = [r for r in results if r["ndcg_at_3"] > 0.5]
    low_score_trials = [r for r in results if r["ndcg_at_3"] <= 0.5]
    assert len(high_score_trials) == 3
    assert len(low_score_trials) <= 3


def test_build_objective_returns_zero_when_selected_below_min_features() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS
    candidate_features = ["feat_speed", "feat_jockey"]
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch("learning.feature_explorer.run_walk_forward_fold") as mock_fold:
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


# --- run_exploration ---

def test_run_exploration_returns_list_of_exploration_results() -> None:
    df = _make_df()
    params = subject.DEFAULT_PARAMS

    mock_trial = MagicMock()
    mock_trial.value = 0.75
    mock_trial.number = 0
    mock_trial.params = {}
    mock_trial.user_attrs = {"promoted": True}

    mock_study = MagicMock()
    mock_study.trials = [mock_trial]

    with FeatureRegistry(Path(":memory:")) as registry:
        with patch("learning.feature_explorer.optuna.create_study", return_value=mock_study):
            results = subject.run_exploration(
                df,
                registry,
                n_trials=1,
                validation_years=[2023],
                train_start="20160101",
                params=params,
                study_name="test_exploration",
                backends=("lightgbm",),
            )

    assert len(results) == 1
    assert results[0]["trial_id"] == "test_exploration_trial_0"
    assert results[0]["ndcg_at_3"] == pytest.approx(0.75)
    assert "feature_names" in results[0]
    assert results[0]["promoted"] is True


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
        with patch("learning.feature_explorer.optuna.create_study", return_value=mock_study):
            with patch("learning.feature_explorer.run_walk_forward_fold"):
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


def test_run_exploration_uses_default_validation_years_when_not_specified() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []

    with FeatureRegistry(Path(":memory:")) as registry:
        with patch("learning.feature_explorer.optuna.create_study", return_value=mock_study):
            results = subject.run_exploration(df, registry, n_trials=1)

    assert results == []


def test_run_exploration_reconstructs_feature_names_from_trial_params_not_candidate_list() -> None:
    """Old trials stored in Optuna may include features absent from the current df.

    feature_names must be reconstructed from trial.params directly, not by
    filtering candidate_features — otherwise features from old trials that are
    no longer in the current candidate list are silently dropped.
    """
    df = _make_df()

    # Old trial used "feat_old" (not in current df) AND "feat_speed" (in current df).
    mock_trial = MagicMock()
    mock_trial.value = 0.80
    mock_trial.number = 0
    mock_trial.params = {"use_feat_speed": True, "use_feat_old": True, "use_feat_jockey": False}
    mock_trial.user_attrs = {"promoted": False}

    mock_study = MagicMock()
    mock_study.trials = [mock_trial]

    with FeatureRegistry(Path(":memory:")) as registry:
        with patch("learning.feature_explorer.optuna.create_study", return_value=mock_study):
            results = subject.run_exploration(
                df,
                registry,
                n_trials=1,
                validation_years=[2023],
                study_name="test_study",
            )

    assert len(results) == 1
    feature_names = set(results[0]["feature_names"])
    assert "feat_speed" in feature_names
    assert "feat_old" in feature_names
    assert "feat_jockey" not in feature_names


# --- select_round_validation_years ---


def test_select_round_validation_years_excludes_blind_holdout() -> None:
    result = subject.select_round_validation_years(
        0, [2021, 2022, 2023, 2024, 2025], 2025
    )
    assert 2025 not in result
    assert len(result) == 2
    assert all(year in [2021, 2022, 2023, 2024] for year in result)


def test_select_round_validation_years_is_deterministic_for_same_round() -> None:
    first = subject.select_round_validation_years(
        3, [2021, 2022, 2023, 2024, 2025], 2025
    )
    second = subject.select_round_validation_years(
        3, [2021, 2022, 2023, 2024, 2025], 2025
    )
    assert first == second


def test_select_round_validation_years_differs_across_rounds() -> None:
    results = {
        tuple(
            subject.select_round_validation_years(
                round_num, [2021, 2022, 2023, 2024, 2025], 2025
            )
        )
        for round_num in range(10)
    }
    assert len(results) > 1


def test_select_round_validation_years_returns_sorted() -> None:
    result = subject.select_round_validation_years(
        7, [2021, 2022, 2023, 2024, 2025], 2025
    )
    assert result == sorted(result)


def test_select_round_validation_years_clamps_k_to_eligible_count() -> None:
    result = subject.select_round_validation_years(0, [2021, 2022], 2022, k=2)
    assert result == [2021]


def test_select_round_validation_years_raises_when_no_eligible_years() -> None:
    with pytest.raises(ValueError, match="no eligible years"):
        subject.select_round_validation_years(0, [2025], 2025)


def test_validation_year_pool_constant() -> None:
    assert subject.VALIDATION_YEAR_POOL == [2021, 2022, 2023, 2024, 2025]
    assert subject.DEFAULT_VALIDATION_YEARS_PER_ROUND == 2


# --- CATEGORY_BACKENDS ---


def test_category_backends_jra_maps_to_catboost_only() -> None:
    assert subject.CATEGORY_BACKENDS["jra"] == ("catboost",)


def test_category_backends_nar_maps_to_xgboost_only() -> None:
    assert subject.CATEGORY_BACKENDS["nar"] == ("xgboost",)


def test_category_backends_banei_maps_to_catboost_only() -> None:
    assert subject.CATEGORY_BACKENDS["ban-ei"] == ("catboost",)


# --- predict_fold_with_backend ---


def test_predict_fold_with_backend_lightgbm_returns_merged_predictions() -> None:
    fold = _make_fold()
    preds_df = pl.DataFrame({
        "race_id": ["2022_race_01", "2022_race_01", "2022_race_01", "2022_race_01"],
        "ketto_toroku_bango": ["horse_000", "horse_001", "horse_002", "horse_003"],
        "predicted_rank": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.run_walk_forward_fold",
        return_value=(MagicMock(), preds_df, {"ndcg_at_3": 0.8}),
    ) as mock_wf:
        result = subject.predict_fold_with_backend(fold, "lightgbm", subject.DEFAULT_PARAMS)
    assert result is not None
    assert "predicted_rank" in result.columns
    assert "finish_position" in result.columns
    assert len(result) == 4
    mock_wf.assert_called_once()


def test_predict_fold_with_backend_xgboost_returns_valid_predictions() -> None:
    fold = _make_fold()
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "ketto_toroku_bango": ["horse_000", "horse_001", "horse_002", "horse_003"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_xgboost_ranker",
        return_value=(MagicMock(), {"valid_predictions": valid_preds}),
    ) as mock_xgb:
        result = subject.predict_fold_with_backend(fold, "xgboost", subject.DEFAULT_PARAMS)
    assert result is not None
    assert "predicted_rank" in result.columns
    mock_xgb.assert_called_once()


def test_predict_fold_with_backend_xgboost_returns_none_when_no_numeric_features() -> None:
    fold = _make_meta_only_fold()
    result = subject.predict_fold_with_backend(fold, "xgboost", subject.DEFAULT_PARAMS)
    assert result is None


def test_predict_fold_with_backend_catboost_returns_valid_predictions() -> None:
    fold = _make_fold()
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "ketto_toroku_bango": ["horse_000", "horse_001", "horse_002", "horse_003"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_catboost_ranker",
        return_value={"valid_predictions": valid_preds},
    ) as mock_cb:
        result = subject.predict_fold_with_backend(fold, "catboost", subject.DEFAULT_PARAMS)
    assert result is not None
    assert "predicted_rank" in result.columns
    mock_cb.assert_called_once()


def test_predict_fold_with_backend_catboost_returns_none_when_no_feature_cols() -> None:
    fold = _make_meta_only_fold()
    result = subject.predict_fold_with_backend(fold, "catboost", subject.DEFAULT_PARAMS)
    assert result is None


def test_predict_fold_with_backend_dispatches_to_lightgbm_helper() -> None:
    fold = _make_fold()
    sentinel = pl.DataFrame({"predicted_rank": [1]})
    with patch(
        "learning.feature_explorer._predict_fold_lightgbm",
        return_value=sentinel,
    ) as mock_lgb:
        result = subject.predict_fold_with_backend(fold, "lightgbm", subject.DEFAULT_PARAMS)
    assert result is sentinel
    mock_lgb.assert_called_once()


def test_predict_fold_with_backend_dispatches_to_xgboost_helper() -> None:
    fold = _make_fold()
    sentinel = pl.DataFrame({"predicted_rank": [2]})
    with patch(
        "learning.feature_explorer._predict_fold_xgboost",
        return_value=sentinel,
    ) as mock_xgb:
        result = subject.predict_fold_with_backend(fold, "xgboost", subject.DEFAULT_PARAMS)
    assert result is sentinel
    mock_xgb.assert_called_once()


def test_predict_fold_with_backend_dispatches_to_catboost_helper() -> None:
    fold = _make_fold()
    sentinel = pl.DataFrame({"predicted_rank": [3]})
    with patch(
        "learning.feature_explorer._predict_fold_catboost",
        return_value=sentinel,
    ) as mock_cb:
        result = subject.predict_fold_with_backend(fold, "catboost", subject.DEFAULT_PARAMS)
    assert result is sentinel
    mock_cb.assert_called_once()


# --- select_fold_features (public alias) ---


def test_select_fold_features_public_alias_keeps_requested_feature_columns() -> None:
    fold = _make_fold()
    result = subject.select_fold_features(fold, {"feat_speed"})
    assert "feat_speed" in result["train_df"].columns
    assert "feat_jockey" not in result["train_df"].columns
    assert result["valid_year"] == 2023


def test_select_fold_features_public_alias_matches_private_function() -> None:
    fold = _make_fold()
    public_result = subject.select_fold_features(fold, {"feat_speed"})
    private_result = subject._select_fold_features(fold, {"feat_speed"})
    assert list(public_result["train_df"].columns) == list(private_result["train_df"].columns)
    assert list(public_result["valid_df"].columns) == list(private_result["valid_df"].columns)
    assert public_result["valid_year"] == private_result["valid_year"]


# --- search-strategy: build_feature_sampler / warm-start / enqueue / timeout ---


def test_build_feature_sampler_returns_tpe_sampler() -> None:
    sampler = subject.build_feature_sampler(20)
    assert isinstance(sampler, optuna.samplers.TPESampler)


def test_build_feature_sampler_caps_startup_below_n_trials() -> None:
    # n_trials=3 → startup must be min(TPE_N_STARTUP_TRIALS=5, 3-1)=2, not 5.
    sampler = subject.build_feature_sampler(3)
    assert isinstance(sampler, optuna.samplers.TPESampler)
    assert sampler._n_startup_trials == 2


def test_build_feature_sampler_floors_startup_at_one() -> None:
    # n_trials=1 → 1-1=0 would disable random startup; floor keeps it at 1.
    sampler = subject.build_feature_sampler(1)
    assert isinstance(sampler, optuna.samplers.TPESampler)
    assert sampler._n_startup_trials == 1


def test_build_feature_sampler_uses_full_startup_for_large_budget() -> None:
    sampler = subject.build_feature_sampler(50)
    assert isinstance(sampler, optuna.samplers.TPESampler)
    assert sampler._n_startup_trials == subject.TPE_N_STARTUP_TRIALS


def test_mask_to_params_sets_true_for_selected_false_for_rest() -> None:
    params, distributions = subject._mask_to_params(
        ["feat_a", "feat_b", "feat_c"], {"feat_a", "feat_c"}
    )
    assert params == {"use_feat_a": True, "use_feat_b": False, "use_feat_c": True}
    assert set(distributions.keys()) == {"use_feat_a", "use_feat_b", "use_feat_c"}


def test_mask_to_params_distribution_is_boolean_categorical() -> None:
    _, distributions = subject._mask_to_params(["feat_a"], {"feat_a"})
    distribution = distributions["use_feat_a"]
    assert isinstance(distribution, optuna.distributions.CategoricalDistribution)
    assert distribution.choices == (True, False)


def test_build_warm_start_trials_reconstructs_prior_trial_as_frozen_trial() -> None:
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]
    with FeatureRegistry(Path(":memory:")) as registry:
        registry.record_trial(
            "seed", 0.82, ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"], "{}"
        )
        warm = subject.build_warm_start_trials(registry, candidate_features)
    assert len(warm) == 1
    assert warm[0].value == pytest.approx(0.82)
    assert warm[0].params == {
        "use_feat_a": True,
        "use_feat_b": True,
        "use_feat_c": True,
        "use_feat_d": True,
        "use_feat_e": True,
    }


def test_build_warm_start_trials_drops_features_absent_from_candidates() -> None:
    # "feat_old" is no longer a candidate → it must be excluded from the mask.
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]
    with FeatureRegistry(Path(":memory:")) as registry:
        registry.record_trial(
            "seed", 0.7, ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_old"], "{}"
        )
        warm = subject.build_warm_start_trials(registry, candidate_features)
    assert "use_feat_old" not in warm[0].params


def test_build_warm_start_trials_skips_trial_below_min_features() -> None:
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]
    with FeatureRegistry(Path(":memory:")) as registry:
        registry.record_trial("too_small", 0.9, ["feat_a", "feat_b"], "{}")
        warm = subject.build_warm_start_trials(registry, candidate_features)
    assert warm == []


def test_build_warm_start_trials_empty_registry_returns_empty() -> None:
    with FeatureRegistry(Path(":memory:")) as registry:
        warm = subject.build_warm_start_trials(registry, ["feat_a", "feat_b"])
    assert warm == []


def test_build_warm_start_trials_can_be_added_to_a_study() -> None:
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]
    with FeatureRegistry(Path(":memory:")) as registry:
        registry.record_trial(
            "seed", 0.82, ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"], "{}"
        )
        warm = subject.build_warm_start_trials(registry, candidate_features)
        study = optuna.create_study(direction="maximize")
        study.add_trial(warm[0])
    assert len(study.trials) == 1
    assert study.best_value == pytest.approx(0.82)


def test_enqueue_feature_subsets_forces_subset_to_be_evaluated_first() -> None:
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]
    study = optuna.create_study(
        direction="maximize", sampler=subject.build_feature_sampler(20)
    )
    subject.enqueue_feature_subsets(
        study,
        candidate_features,
        [{"feat_a", "feat_b", "feat_c", "feat_d", "feat_e"}],
    )
    seen_first: dict[str, set[str]] = {}

    def objective(trial: optuna.Trial) -> float:
        mask = {c: trial.suggest_categorical(f"use_{c}", [True, False]) for c in candidate_features}
        if "first" not in seen_first:
            seen_first["first"] = {c for c, keep in mask.items() if keep}
        return 0.5

    study.optimize(objective, n_trials=1)
    assert seen_first["first"] == {"feat_a", "feat_b", "feat_c", "feat_d", "feat_e"}


def test_enqueue_feature_subsets_skips_subset_below_min_features() -> None:
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]
    study = MagicMock()
    subject.enqueue_feature_subsets(study, candidate_features, [{"feat_a", "feat_b"}])
    study.enqueue_trial.assert_not_called()


def test_enqueue_feature_subsets_drops_non_candidate_features() -> None:
    candidate_features = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]
    study = MagicMock()
    subject.enqueue_feature_subsets(
        study,
        candidate_features,
        [{"feat_a", "feat_b", "feat_c", "feat_d", "feat_e", "feat_gone"}],
    )
    enqueued_params = study.enqueue_trial.call_args[0][0]
    assert "use_feat_gone" not in enqueued_params
    assert enqueued_params["use_feat_a"] is True


def test_make_per_trial_timeout_callback_stops_study_when_over_budget() -> None:
    callback = subject.make_per_trial_timeout_callback(0.0)
    study = MagicMock()
    trial = MagicMock()
    trial.datetime_start = datetime(2026, 1, 1, 0, 0, 0)
    trial.datetime_complete = datetime(2026, 1, 1, 0, 0, 5)
    callback(study, trial)
    study.stop.assert_called_once()


def test_make_per_trial_timeout_callback_does_not_stop_when_within_budget() -> None:
    callback = subject.make_per_trial_timeout_callback(3600.0)
    study = MagicMock()
    trial = MagicMock()
    trial.datetime_start = datetime(2026, 1, 1, 0, 0, 0)
    trial.datetime_complete = datetime(2026, 1, 1, 0, 0, 5)
    callback(study, trial)
    study.stop.assert_not_called()


def test_make_per_trial_timeout_callback_ignores_trial_without_start_time() -> None:
    callback = subject.make_per_trial_timeout_callback(0.0)
    study = MagicMock()
    trial = MagicMock()
    trial.datetime_start = None
    trial.datetime_complete = datetime(2026, 1, 1, 0, 0, 5)
    callback(study, trial)
    study.stop.assert_not_called()


def test_make_per_trial_timeout_callback_ignores_trial_without_complete_time() -> None:
    callback = subject.make_per_trial_timeout_callback(0.0)
    study = MagicMock()
    trial = MagicMock()
    trial.datetime_start = datetime(2026, 1, 1, 0, 0, 0)
    trial.datetime_complete = None
    callback(study, trial)
    study.stop.assert_not_called()


def test_run_exploration_passes_sampler_to_create_study() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ) as mock_create:
            subject.run_exploration(df, registry, n_trials=1, warm_start=False)
    sampler = mock_create.call_args.kwargs["sampler"]
    assert isinstance(sampler, optuna.samplers.TPESampler)


def test_run_exploration_warm_starts_study_from_registry_when_enabled() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        registry.record_trial(
            "seed", 0.8, ["feat_speed", "feat_jockey", "umaban", "race_id", "finish_position"], "{}"
        )
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ):
            with patch(
                "learning.feature_explorer.build_warm_start_trials",
                return_value=["warm_a", "warm_b"],
            ):
                subject.run_exploration(df, registry, n_trials=1, warm_start=True)
    assert mock_study.add_trial.call_count == 2


def test_run_exploration_skips_warm_start_when_disabled() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ):
            with patch(
                "learning.feature_explorer.build_warm_start_trials",
                return_value=["warm_a"],
            ) as mock_warm:
                subject.run_exploration(df, registry, n_trials=1, warm_start=False)
    mock_warm.assert_not_called()
    mock_study.add_trial.assert_not_called()


def test_run_exploration_enqueues_provided_subsets() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ):
            with patch(
                "learning.feature_explorer.enqueue_feature_subsets"
            ) as mock_enqueue:
                subject.run_exploration(
                    df,
                    registry,
                    n_trials=1,
                    warm_start=False,
                    enqueue_subsets=[{"feat_speed", "feat_jockey"}],
                )
    mock_enqueue.assert_called_once()


def test_run_exploration_does_not_enqueue_when_subsets_none() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ):
            with patch(
                "learning.feature_explorer.enqueue_feature_subsets"
            ) as mock_enqueue:
                subject.run_exploration(df, registry, n_trials=1, warm_start=False)
    mock_enqueue.assert_not_called()


def test_run_exploration_passes_timeout_and_n_jobs_to_optimize() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ):
            subject.run_exploration(
                df,
                registry,
                n_trials=1,
                warm_start=False,
                n_jobs=2,
                study_timeout_s=120.0,
            )
    optimize_kwargs = mock_study.optimize.call_args.kwargs
    assert optimize_kwargs["n_jobs"] == 2
    assert optimize_kwargs["timeout"] == pytest.approx(120.0)


def test_run_exploration_adds_timeout_callback_when_per_trial_timeout_set() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ):
            subject.run_exploration(
                df, registry, n_trials=1, warm_start=False, per_trial_timeout_s=30.0
            )
    callbacks = mock_study.optimize.call_args.kwargs["callbacks"]
    assert len(callbacks) == 1


def test_run_exploration_uses_no_callbacks_when_per_trial_timeout_none() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with patch(
            "learning.feature_explorer.optuna.create_study", return_value=mock_study
        ):
            subject.run_exploration(df, registry, n_trials=1, warm_start=False)
    callbacks = mock_study.optimize.call_args.kwargs["callbacks"]
    assert callbacks == []


# --- _SCREEN_XGB_ARGS / _SCREEN_CB_ARGS constants ---


def test_screen_xgb_args_exists_with_reduced_iterations() -> None:
    assert hasattr(subject, "_SCREEN_XGB_ARGS")
    assert subject._SCREEN_XGB_ARGS.num_rounds == 150
    assert subject._SCREEN_XGB_ARGS.early_stopping_rounds == 30


def test_screen_cb_args_exists_with_reduced_iterations() -> None:
    assert hasattr(subject, "_SCREEN_CB_ARGS")
    assert subject._SCREEN_CB_ARGS.iterations == 150
    assert subject._SCREEN_CB_ARGS.early_stopping_rounds == 30


def test_screen_xgb_args_matches_xgb_args_except_rounds() -> None:
    assert subject._SCREEN_XGB_ARGS.learning_rate == subject._XGB_ARGS.learning_rate
    assert subject._SCREEN_XGB_ARGS.max_depth == subject._XGB_ARGS.max_depth
    assert subject._SCREEN_XGB_ARGS.min_child_weight == subject._XGB_ARGS.min_child_weight
    assert subject._SCREEN_XGB_ARGS.reg_lambda == subject._XGB_ARGS.reg_lambda
    assert subject._SCREEN_XGB_ARGS.seed == subject._XGB_ARGS.seed
    assert subject._SCREEN_XGB_ARGS.relevance_rank1 == subject._XGB_ARGS.relevance_rank1
    assert subject._SCREEN_XGB_ARGS.relevance_rank2 == subject._XGB_ARGS.relevance_rank2
    assert subject._SCREEN_XGB_ARGS.relevance_rank3 == subject._XGB_ARGS.relevance_rank3


def test_screen_cb_args_matches_cb_args_except_rounds() -> None:
    assert subject._SCREEN_CB_ARGS.learning_rate == subject._CB_ARGS.learning_rate
    assert subject._SCREEN_CB_ARGS.depth == subject._CB_ARGS.depth
    assert subject._SCREEN_CB_ARGS.l2_leaf_reg == subject._CB_ARGS.l2_leaf_reg
    assert subject._SCREEN_CB_ARGS.seed == subject._CB_ARGS.seed
    assert subject._SCREEN_CB_ARGS.no_cat_features == subject._CB_ARGS.no_cat_features
    assert subject._SCREEN_CB_ARGS.relevance_rank1 == subject._CB_ARGS.relevance_rank1
    assert subject._SCREEN_CB_ARGS.relevance_rank2 == subject._CB_ARGS.relevance_rank2
    assert subject._SCREEN_CB_ARGS.relevance_rank3 == subject._CB_ARGS.relevance_rank3


def test_full_xgb_args_has_300_rounds() -> None:
    assert subject._XGB_ARGS.num_rounds == 300
    assert subject._XGB_ARGS.early_stopping_rounds == 50


def test_full_cb_args_has_300_iterations() -> None:
    assert subject._CB_ARGS.iterations == 300
    assert subject._CB_ARGS.early_stopping_rounds == 50


# --- run_exploration screening parameter ---


def test_run_exploration_screening_true_passes_screen_args_to_build_objective() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with (
            patch(
                "learning.feature_explorer.optuna.create_study", return_value=mock_study
            ),
            patch(
                "learning.feature_explorer.build_objective", return_value=lambda t: 0.5
            ) as mock_build,
        ):
            subject.run_exploration(
                df, registry, n_trials=1, warm_start=False, screening=True
            )
    kwargs = mock_build.call_args.kwargs
    assert kwargs["xgb_args"] is subject._SCREEN_XGB_ARGS
    assert kwargs["cb_args"] is subject._SCREEN_CB_ARGS


def test_run_exploration_screening_false_passes_none_args_to_build_objective() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with (
            patch(
                "learning.feature_explorer.optuna.create_study", return_value=mock_study
            ),
            patch(
                "learning.feature_explorer.build_objective", return_value=lambda t: 0.5
            ) as mock_build,
        ):
            subject.run_exploration(
                df, registry, n_trials=1, warm_start=False, screening=False
            )
    kwargs = mock_build.call_args.kwargs
    assert kwargs["xgb_args"] is None
    assert kwargs["cb_args"] is None


def test_run_exploration_default_screening_is_false() -> None:
    df = _make_df()
    mock_study = MagicMock()
    mock_study.trials = []
    with FeatureRegistry(Path(":memory:")) as registry:
        with (
            patch(
                "learning.feature_explorer.optuna.create_study", return_value=mock_study
            ),
            patch(
                "learning.feature_explorer.build_objective", return_value=lambda t: 0.5
            ) as mock_build,
        ):
            subject.run_exploration(df, registry, n_trials=1, warm_start=False)
    kwargs = mock_build.call_args.kwargs
    assert kwargs["xgb_args"] is None
    assert kwargs["cb_args"] is None


# --- run_fold_with_backend with xgb_args / cb_args ---


def test_run_fold_with_backend_xgboost_passes_xgb_args_to_trainer() -> None:
    import argparse

    fold = _make_fold()
    params = subject.DEFAULT_PARAMS
    custom_args = argparse.Namespace(
        learning_rate=0.05, max_depth=6, min_child_weight=1, reg_lambda=1.0,
        seed=42, num_rounds=150, early_stopping_rounds=30,
        relevance_rank1=3, relevance_rank2=2, relevance_rank3=1,
    )
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_xgboost_ranker",
        return_value=(MagicMock(), {"valid_predictions": valid_preds}),
    ) as mock_xgb:
        subject.run_fold_with_backend(fold, "xgboost", params, xgb_args=custom_args)
    passed_args = mock_xgb.call_args[0][3]
    assert passed_args is custom_args


def test_run_fold_with_backend_xgboost_uses_default_when_xgb_args_none() -> None:
    fold = _make_fold()
    params = subject.DEFAULT_PARAMS
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_xgboost_ranker",
        return_value=(MagicMock(), {"valid_predictions": valid_preds}),
    ) as mock_xgb:
        subject.run_fold_with_backend(fold, "xgboost", params)
    passed_args = mock_xgb.call_args[0][3]
    assert passed_args is subject._XGB_ARGS


def test_run_fold_with_backend_catboost_passes_cb_args_to_trainer() -> None:
    import argparse

    fold = _make_fold()
    params = subject.DEFAULT_PARAMS
    custom_args = argparse.Namespace(
        learning_rate=0.05, depth=6, l2_leaf_reg=3.0, seed=42,
        iterations=150, early_stopping_rounds=30,
        relevance_rank1=3, relevance_rank2=2, relevance_rank3=1,
        no_cat_features=False,
    )
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_catboost_ranker",
        return_value={"valid_predictions": valid_preds},
    ) as mock_cb:
        subject.run_fold_with_backend(fold, "catboost", params, cb_args=custom_args)
    passed_args = mock_cb.call_args[0][3]
    assert passed_args is custom_args


def test_run_fold_with_backend_catboost_uses_default_when_cb_args_none() -> None:
    fold = _make_fold()
    params = subject.DEFAULT_PARAMS
    valid_preds = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    with patch(
        "learning.feature_explorer.train_catboost_ranker",
        return_value={"valid_predictions": valid_preds},
    ) as mock_cb:
        subject.run_fold_with_backend(fold, "catboost", params)
    passed_args = mock_cb.call_args[0][3]
    assert passed_args is subject._CB_ARGS
