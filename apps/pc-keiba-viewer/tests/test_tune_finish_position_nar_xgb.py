"""Unit tests for ``tune_finish_position_nar_xgb`` (Iter 12 Optuna HPO).

NOT-DRY: each test is fully self-contained. Mocks Optuna ``Trial``,
xgboost ``train``, and filesystem I/O. No real disk reads beyond per-test
``tmp_path`` writes.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import tune_finish_position_nar_xgb as mod


def test_to_relevance_none_returns_zero() -> None:
    assert mod.to_relevance(None) == 0


def test_to_relevance_nan_returns_zero() -> None:
    assert mod.to_relevance(float("nan")) == 0


def test_to_relevance_rank1() -> None:
    assert mod.to_relevance(1) == 3


def test_to_relevance_rank2() -> None:
    assert mod.to_relevance(2) == 2


def test_to_relevance_rank3() -> None:
    assert mod.to_relevance(3) == 1


def test_to_relevance_rank_other_returns_zero() -> None:
    assert mod.to_relevance(7) == 0


def test_resolve_feature_columns_drops_meta_and_label() -> None:
    df = pd.DataFrame(
        {
            "race_id": ["r1"],
            "finish_position": [1.0],
            "feature_a": [0.5],
            "feature_b": [1],
        },
    )
    assert mod.resolve_feature_columns(df) == ["feature_a", "feature_b"]


def test_resolve_feature_columns_drops_bool_and_object() -> None:
    df = pd.DataFrame(
        {
            "feature_a": [0.5],
            "is_flag": [True],
            "bamei": ["abc"],
        },
    )
    assert mod.resolve_feature_columns(df) == ["feature_a"]


def test_resolve_feature_columns_drops_extra_non_feature() -> None:
    df = pd.DataFrame(
        {
            "feature_a": [0.5],
            "target_race_id": ["r1"],
            "kyori_band": [1],
            "season_band": [1],
            "feature_schema_version": [1],
            "futan_weight_class": [1],
            "keibajo_code": ["01"],
        },
    )
    assert mod.resolve_feature_columns(df) == ["feature_a"]


def test_load_year_parquet_missing_year_dir(tmp_path: Path) -> None:
    assert mod.load_year_parquet(tmp_path, 1999) is None


def test_load_year_parquet_empty_year_dir(tmp_path: Path) -> None:
    (tmp_path / "race_year=2024").mkdir(parents=True)
    assert mod.load_year_parquet(tmp_path, 2024) is None


def test_load_year_parquet_single_file(tmp_path: Path) -> None:
    year_dir = tmp_path / "race_year=2024"
    year_dir.mkdir(parents=True)
    df = pd.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["h1", "h2"],
            "finish_position": [1.0, 2.0],
        },
    )
    df.to_parquet(year_dir / "data_0.parquet", index=False)
    loaded = mod.load_year_parquet(tmp_path, 2024)
    assert loaded is not None
    assert int(loaded["race_year"].iloc[0]) == 2024
    assert len(loaded) == 2


def test_load_year_parquet_multi_file_concat(tmp_path: Path) -> None:
    year_dir = tmp_path / "race_year=2024"
    year_dir.mkdir(parents=True)
    df1 = pd.DataFrame(
        {
            "race_id": ["r1"],
            "ketto_toroku_bango": ["h1"],
            "finish_position": [1.0],
        },
    )
    df2 = pd.DataFrame(
        {
            "race_id": ["r2"],
            "ketto_toroku_bango": ["h2"],
            "finish_position": [1.0],
        },
    )
    df1.to_parquet(year_dir / "data_0.parquet", index=False)
    df2.to_parquet(year_dir / "data_1.parquet", index=False)
    loaded = mod.load_year_parquet(tmp_path, 2024)
    assert loaded is not None
    assert len(loaded) == 2


def test_load_year_parquet_dedups_duplicate_rows(tmp_path: Path) -> None:
    year_dir = tmp_path / "race_year=2024"
    year_dir.mkdir(parents=True)
    df = pd.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["h1", "h1"],
            "finish_position": [1.0, 1.0],
        },
    )
    df.to_parquet(year_dir / "data_0.parquet", index=False)
    loaded = mod.load_year_parquet(tmp_path, 2024)
    assert loaded is not None
    assert len(loaded) == 1


def test_load_bucket_year_missing_dir(tmp_path: Path) -> None:
    assert mod.load_bucket_year(tmp_path, 2024) is None


def test_load_bucket_year_empty_dir(tmp_path: Path) -> None:
    (tmp_path / "category=nar" / "race_year=2024").mkdir(parents=True)
    assert mod.load_bucket_year(tmp_path, 2024) is None


def test_load_bucket_year_returns_first_file(tmp_path: Path) -> None:
    year_dir = tmp_path / "category=nar" / "race_year=2024"
    year_dir.mkdir(parents=True)
    df = pd.DataFrame({"race_id": ["r1"], "bucket_grade_code": ["A"]})
    df.to_parquet(year_dir / "membership.parquet", index=False)
    loaded = mod.load_bucket_year(tmp_path, 2024)
    assert loaded is not None
    assert list(loaded["bucket_grade_code"]) == ["A"]


def test_build_group_sizes_single_race() -> None:
    df = pd.DataFrame({"race_id": ["r1", "r1", "r1"]})
    assert mod.build_group_sizes(df) == [3]


def test_build_group_sizes_multi_race() -> None:
    df = pd.DataFrame({"race_id": ["r1", "r1", "r2"]})
    assert mod.build_group_sizes(df) == [2, 1]


def test_dcg_at_k_empty() -> None:
    assert mod.dcg_at_k([]) == 0.0


def test_dcg_at_k_simple() -> None:
    # head [3, 2, 1]
    # 3/log2(2) + 2/log2(3) + 1/log2(4) = 3 + 2/1.585 + 0.5
    val = mod.dcg_at_k([3, 2, 1])
    assert val == pytest.approx(3 + 2 / float(np.log2(3.0)) + 0.5, rel=1e-6)


def test_ndcg_at_k_per_race_empty() -> None:
    assert mod.ndcg_at_k_per_race(np.array([]), np.array([])) == 0.0


def test_ndcg_at_k_per_race_perfect() -> None:
    preds = np.array([3.0, 2.0, 1.0])
    labels = np.array([3, 2, 1])
    assert mod.ndcg_at_k_per_race(preds, labels) == pytest.approx(1.0)


def test_ndcg_at_k_per_race_zero_idcg_returns_zero() -> None:
    preds = np.array([3.0, 2.0, 1.0])
    labels = np.array([0, 0, 0])
    assert mod.ndcg_at_k_per_race(preds, labels) == 0.0


def test_ndcg_at_k_per_race_imperfect() -> None:
    preds = np.array([1.0, 2.0, 3.0])
    labels = np.array([3, 2, 1])
    # ranked labels [1, 2, 3], dcg vs ideal [3, 2, 1]
    val = mod.ndcg_at_k_per_race(preds, labels)
    assert 0.0 < val < 1.0


def test_compute_global_ndcg_two_races() -> None:
    preds = np.array([3.0, 2.0, 3.0, 1.0])
    labels = np.array([3, 2, 3, 1])
    group_sizes = [2, 2]
    val = mod.compute_global_ndcg(preds, labels, group_sizes)
    assert val == pytest.approx(1.0)


def test_compute_global_ndcg_empty() -> None:
    assert mod.compute_global_ndcg(np.array([]), np.array([]), []) == 0.0


def test_compute_worst_bucket_ndcg_no_keys() -> None:
    val = mod.compute_worst_bucket_ndcg(
        np.array([1.0]),
        np.array([1]),
        [1],
        [],
    )
    assert val == 0.0


def test_compute_worst_bucket_ndcg_below_support_returns_zero() -> None:
    preds = np.array([3.0, 2.0])
    labels = np.array([3, 2])
    group_sizes = [2]
    val = mod.compute_worst_bucket_ndcg(preds, labels, group_sizes, ["A"])
    assert val == 0.0


def test_compute_worst_bucket_ndcg_with_support() -> None:
    n_races = 60
    rng = np.random.default_rng(seed=11)
    preds_chunks: list[np.ndarray] = []
    labels_chunks: list[np.ndarray] = []
    group_sizes: list[int] = []
    bucket_keys: list[str] = []
    for i in range(n_races):
        size = 5
        # alternate bucket A (perfect) vs B (random)
        if i % 2 == 0:
            preds_chunks.append(np.array([3.0, 2.0, 1.0, 0.5, 0.1]))
            labels_chunks.append(np.array([3, 2, 1, 0, 0]))
            bucket_keys.append("A")
        else:
            preds_chunks.append(rng.random(size))
            labels_chunks.append(np.array([3, 2, 1, 0, 0]))
            bucket_keys.append("B")
        group_sizes.append(size)
    preds = np.concatenate(preds_chunks)
    labels = np.concatenate(labels_chunks)
    worst = mod.compute_worst_bucket_ndcg(preds, labels, group_sizes, bucket_keys)
    # bucket B mean should be < bucket A mean (1.0)
    assert worst < 1.0


def test_assert_no_race_overlap_clean() -> None:
    train_df = pd.DataFrame({"race_id": ["r1", "r2"]})
    valid_df = pd.DataFrame({"race_id": ["r3"]})
    mod.assert_no_race_overlap(train_df, valid_df)


def test_assert_no_race_overlap_raises() -> None:
    train_df = pd.DataFrame({"race_id": ["r1", "r2"]})
    valid_df = pd.DataFrame({"race_id": ["r2"]})
    with pytest.raises(AssertionError):
        mod.assert_no_race_overlap(train_df, valid_df)


def _write_year_parquet(root: Path, year: int, race_ids: list[str]) -> None:
    year_dir = root / f"race_year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "race_id": race_ids,
            "ketto_toroku_bango": [f"h{i}" for i in range(len(race_ids))],
            "umaban": list(range(1, len(race_ids) + 1)),
            "finish_position": [float(i + 1) for i in range(len(race_ids))],
            "feature_a": [float(i) * 0.1 for i in range(len(race_ids))],
            "feature_b": [float(i) * 0.2 for i in range(len(race_ids))],
        },
    )
    df.to_parquet(year_dir / "data_0.parquet", index=False)


def test_build_fold_frames_returns_correct_shape(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    bucket_root = tmp_path / "bucket"
    _write_year_parquet(features_root, 2023, ["r2023a", "r2023b"])
    _write_year_parquet(features_root, 2024, ["r2024a", "r2024b"])
    _write_year_parquet(features_root, 2025, ["r2025a", "r2025b"])
    frames = mod.build_fold_frames(
        features_root, bucket_root, (2023, 2024, 2025), 2025, ["feature_a", "feature_b"],
    )
    assert len(frames.train_df) == 4
    assert len(frames.valid_df) == 2
    assert frames.feature_cols == ["feature_a", "feature_b"]
    assert frames.bucket_df is None


def test_build_fold_frames_no_train_raises(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    _write_year_parquet(features_root, 2025, ["r2025a"])
    with pytest.raises(RuntimeError):
        mod.build_fold_frames(
            features_root, None, (2025,), 2025, ["feature_a"],
        )


def test_build_fold_frames_no_valid_raises(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    _write_year_parquet(features_root, 2023, ["r1"])
    _write_year_parquet(features_root, 2024, ["r2"])
    with pytest.raises(RuntimeError):
        mod.build_fold_frames(
            features_root, None, (2023, 2024, 2025), 2025, ["feature_a"],
        )


def test_build_fold_frames_with_bucket(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    bucket_root = tmp_path / "bucket"
    _write_year_parquet(features_root, 2023, ["r2023a"])
    _write_year_parquet(features_root, 2024, ["r2024a"])
    bucket_dir = bucket_root / "category=nar" / "race_year=2024"
    bucket_dir.mkdir(parents=True)
    pd.DataFrame(
        {"race_id": ["r2024a"], "bucket_grade_code": ["A"]},
    ).to_parquet(bucket_dir / "membership.parquet", index=False)
    frames = mod.build_fold_frames(
        features_root, bucket_root, (2023, 2024), 2024, ["feature_a", "feature_b"],
    )
    assert frames.bucket_df is not None
    assert list(frames.bucket_df["bucket_grade_code"]) == ["A"]


def test_build_fold_frames_leave_one_year_out_includes_future_non_held_years(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    _write_year_parquet(features_root, 2023, ["r2023a"])
    _write_year_parquet(features_root, 2024, ["r2024a"])
    _write_year_parquet(features_root, 2025, ["r2025a"])
    frames = mod.build_fold_frames(
        features_root, None, (2023, 2024, 2025), 2023, ["feature_a"],
    )
    train_race_ids = set(frames.train_df["race_id"].tolist())
    assert "r2023a" not in train_race_ids
    assert "r2024a" in train_race_ids
    assert "r2025a" in train_race_ids


def test_build_fold_frames_excludes_col_missing_from_train(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    train_dir = features_root / "race_year=2023"
    train_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "race_id": ["r2023a"],
            "ketto_toroku_bango": ["h0"],
            "umaban": [1],
            "finish_position": [1.0],
            "feature_a": [0.1],
        },
    ).to_parquet(train_dir / "data_0.parquet", index=False)
    valid_dir = features_root / "race_year=2024"
    valid_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "race_id": ["r2024a"],
            "ketto_toroku_bango": ["h1"],
            "umaban": [1],
            "finish_position": [1.0],
            "feature_a": [0.2],
            "feature_new": [0.3],
        },
    ).to_parquet(valid_dir / "data_0.parquet", index=False)
    frames = mod.build_fold_frames(
        features_root, None, (2023, 2024), 2024, ["feature_a", "feature_new"],
    )
    assert frames.feature_cols == ["feature_a"]


def test_attach_bucket_keys_no_bucket_returns_all_default() -> None:
    valid_df = pd.DataFrame({"race_id": ["r1", "r1", "r2"]})
    keys = mod.attach_bucket_keys(valid_df, None)
    assert keys == ["__all__", "__all__"]


def test_attach_bucket_keys_bucket_missing_race_id() -> None:
    valid_df = pd.DataFrame({"race_id": ["r1", "r1", "r2"]})
    bucket_df = pd.DataFrame({"some_other_col": ["x"]})
    keys = mod.attach_bucket_keys(valid_df, bucket_df)
    assert keys == ["__all__", "__all__"]


def test_attach_bucket_keys_no_recognized_key_col() -> None:
    valid_df = pd.DataFrame({"race_id": ["r1", "r2"]})
    bucket_df = pd.DataFrame({"race_id": ["r1", "r2"], "irrelevant": ["x", "y"]})
    keys = mod.attach_bucket_keys(valid_df, bucket_df)
    assert keys == ["__all__", "__all__"]


def test_attach_bucket_keys_uses_bucket_grade_code() -> None:
    valid_df = pd.DataFrame({"race_id": ["r1", "r2", "r3"]})
    bucket_df = pd.DataFrame(
        {"race_id": ["r1", "r2", "r3"], "bucket_grade_code": ["A", "B", "A"]},
    )
    keys = mod.attach_bucket_keys(valid_df, bucket_df)
    assert keys == ["A", "B", "A"]


def test_attach_bucket_keys_unknown_race_gets_unknown_key() -> None:
    valid_df = pd.DataFrame({"race_id": ["r1", "r99"]})
    bucket_df = pd.DataFrame(
        {"race_id": ["r1"], "bucket_grade_code": ["A"]},
    )
    keys = mod.attach_bucket_keys(valid_df, bucket_df)
    assert keys == ["A", "__unknown__"]


def test_attach_bucket_keys_uses_grade_code_fallback() -> None:
    valid_df = pd.DataFrame({"race_id": ["r1"]})
    bucket_df = pd.DataFrame(
        {"race_id": ["r1"], "grade_code": ["C"]},
    )
    keys = mod.attach_bucket_keys(valid_df, bucket_df)
    assert keys == ["C"]


def test_attach_bucket_keys_uses_kyoso_joken_code_fallback() -> None:
    valid_df = pd.DataFrame({"race_id": ["r1"]})
    bucket_df = pd.DataFrame(
        {"race_id": ["r1"], "kyoso_joken_code": ["KJ"]},
    )
    keys = mod.attach_bucket_keys(valid_df, bucket_df)
    assert keys == ["KJ"]


def test_suggest_params_returns_all_expected_keys() -> None:
    trial = MagicMock()
    trial.suggest_int.side_effect = [7, 5, 600]  # depth, min_child, n_estimators
    trial.suggest_float.side_effect = [0.05, 1.5, 0.8, 0.9]  # lr, reg_lambda, sub, col
    params = mod.suggest_params(cast(mod.TrialLike, trial))
    assert set(params.keys()) == {
        "max_depth",
        "learning_rate",
        "reg_lambda",
        "min_child_weight",
        "subsample",
        "colsample_bytree",
        "n_estimators",
    }
    assert params["max_depth"] == 7
    assert params["learning_rate"] == 0.05


def test_enforce_stability_floor_clamps_lr() -> None:
    out = mod.enforce_stability_floor({"learning_rate": 0.01})
    assert out["learning_rate"] == mod.LR_FLOOR


def test_enforce_stability_floor_keeps_lr_above_floor() -> None:
    out = mod.enforce_stability_floor({"learning_rate": 0.06})
    assert out["learning_rate"] == 0.06


def test_enforce_stability_floor_clamps_reg_lambda() -> None:
    out = mod.enforce_stability_floor({"reg_lambda": 0.5})
    assert out["reg_lambda"] == mod.REG_LAMBDA_FLOOR


def test_enforce_stability_floor_clamps_max_depth() -> None:
    out = mod.enforce_stability_floor({"max_depth": 15})
    assert out["max_depth"] == mod.MAX_DEPTH_CEIL


def test_enforce_stability_floor_defaults_when_missing() -> None:
    out = mod.enforce_stability_floor({})
    assert out["learning_rate"] == mod.LR_FLOOR
    assert out["reg_lambda"] == mod.REG_LAMBDA_FLOOR
    assert out["max_depth"] == mod.MAX_DEPTH_CEIL


def test_train_xgb_fold_invokes_xgb_train_and_returns_array() -> None:
    train_df = pd.DataFrame(
        {
            "race_id": ["r1", "r1", "r2", "r2"],
            "finish_position": [1.0, 2.0, 1.0, 2.0],
            "feature_a": [0.1, 0.2, 0.3, 0.4],
        },
    )
    valid_df = pd.DataFrame(
        {
            "race_id": ["r3", "r3"],
            "finish_position": [1.0, 2.0],
            "feature_a": [0.5, 0.6],
        },
    )
    params = {
        "max_depth": 5,
        "learning_rate": 0.05,
        "reg_lambda": 1.0,
        "min_child_weight": 1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "n_estimators": 50,
    }
    fake_booster = MagicMock()
    fake_booster.best_iteration = 10
    fake_booster.predict.return_value = np.array([0.5, 0.4])
    with patch("xgboost.train", return_value=fake_booster) as train_mock, \
         patch("xgboost.DMatrix") as dmatrix_mock:
        dmatrix_mock.return_value = MagicMock()
        result = mod.train_xgb_fold(
            train_df, valid_df, ["feature_a"], params, seed=42,
        )
    assert result.shape == (2,)
    train_mock.assert_called_once()


def test_evaluate_params_aggregates_across_folds() -> None:
    valid_df_2024 = pd.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "finish_position": [1.0, 2.0],
            "feature_a": [0.5, 0.6],
        },
    )
    train_df_2024 = pd.DataFrame(
        {
            "race_id": ["r99"],
            "finish_position": [1.0],
            "feature_a": [0.0],
        },
    )

    def fake_fold_frames(held_out: int, cols: list[str]) -> mod.FoldFrames:
        return mod.FoldFrames(
            train_df=train_df_2024,
            valid_df=valid_df_2024,
            bucket_df=None,
            feature_cols=["feature_a"],
        )

    def fake_train(
        train: pd.DataFrame,
        valid: pd.DataFrame,
        feats: list[str],
        p: Mapping[str, object],
        seed: int,
    ) -> np.ndarray:
        return np.array([3.0, 2.0])

    deps = mod.FoldDeps(fold_frames=fake_fold_frames, train_fn=fake_train)
    g, w = mod.evaluate_params(
        {"max_depth": 5}, (2024,), 42, deps, ["feature_a"],
    )
    assert g == pytest.approx(1.0)
    assert w == 0.0  # bucket support below threshold


def test_evaluate_params_skips_fold_with_no_features() -> None:
    train_df = pd.DataFrame({"race_id": ["r1"], "finish_position": [1.0]})
    valid_df = pd.DataFrame({"race_id": ["r2"], "finish_position": [1.0]})

    def fake_fold_frames(held_out: int, cols: list[str]) -> mod.FoldFrames:
        return mod.FoldFrames(
            train_df=train_df, valid_df=valid_df, bucket_df=None, feature_cols=[],
        )

    def fake_train(
        train: pd.DataFrame,
        valid: pd.DataFrame,
        feats: list[str],
        p: Mapping[str, object],
        seed: int,
    ) -> np.ndarray:
        raise AssertionError("should not be called")

    deps = mod.FoldDeps(fold_frames=fake_fold_frames, train_fn=fake_train)
    g, w = mod.evaluate_params({}, (2024,), 42, deps, [])
    assert g == 0.0
    assert w == 0.0


def test_picker_score_weighting() -> None:
    s = mod.picker_score(0.8, 0.6)
    assert s == pytest.approx(0.7 * 0.8 + 0.3 * 0.6)


def test_pick_best_trial_empty_raises() -> None:
    with pytest.raises(ValueError):
        mod.pick_best_trial([])


def test_pick_best_trial_selects_highest_picker_score() -> None:
    trials = [
        {
            "trial_number": 0,
            "params": {"max_depth": 5},
            "global_ndcg": 0.7,
            "worst_ndcg": 0.5,
        },
        {
            "trial_number": 1,
            "params": {"max_depth": 6},
            "global_ndcg": 0.8,
            "worst_ndcg": 0.6,
        },
    ]
    best = mod.pick_best_trial(trials)
    assert best["trial_number"] == 1
    assert best["params"] == {"max_depth": 6}
    assert best["picker_score"] == pytest.approx(0.7 * 0.8 + 0.3 * 0.6)


def test_write_outputs_writes_three_files(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    best = {"trial_number": 0, "params": {"max_depth": 6}, "picker_score": 0.7}
    trials = [
        {
            "trial_number": 0,
            "params": {"max_depth": 6},
            "global_ndcg": 0.8,
            "worst_ndcg": 0.6,
        },
    ]
    study_meta = {"n_trials_completed": 1, "cv_years": [2024], "random_seed": 42}
    mod.write_outputs(out_dir, best, trials, study_meta)
    assert (out_dir / "best-params.json").exists()
    assert (out_dir / "pareto-front.json").exists()
    assert (out_dir / "study-summary.json").exists()
    loaded_best = json.loads((out_dir / "best-params.json").read_text(encoding="utf-8"))
    assert loaded_best["trial_number"] == 0


def test_build_arg_parser_required_args() -> None:
    parser = mod.build_arg_parser()
    args = parser.parse_args(
        [
            "--features-parquet-root", "/tmp/feats",
            "--bucket-membership-parquet", "/tmp/bucket",
            "--output-dir", "/tmp/out",
        ],
    )
    assert args.features_parquet_root == Path("/tmp/feats")
    assert args.n_trials == mod.DEFAULT_N_TRIALS


def test_normalize_args_converts_paths_and_types() -> None:
    parser = mod.build_arg_parser()
    args = parser.parse_args(
        [
            "--features-parquet-root", "/tmp/feats",
            "--bucket-membership-parquet", "/tmp/bucket",
            "--output-dir", "/tmp/out",
            "--n-trials", "20",
            "--timeout", "3600",
            "--random-seed", "11",
            "--cv-years", "2024", "2025",
        ],
    )
    norm = mod.normalize_args(args)
    assert norm.features_parquet_root == Path("/tmp/feats")
    assert norm.n_trials == 20
    assert norm.timeout_seconds == 3600
    assert norm.random_seed == 11
    assert norm.cv_years == (2024, 2025)


def test_run_study_writes_outputs(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    bucket_root = tmp_path / "bucket"
    _write_year_parquet(features_root, 2024, ["r1", "r2"])
    _write_year_parquet(features_root, 2025, ["r3", "r4"])
    args = mod.TuneArgs(
        features_parquet_root=features_root,
        bucket_membership_parquet_root=bucket_root,
        output_dir=tmp_path / "out",
        n_trials=2,
        timeout_seconds=60,
        random_seed=42,
        cv_years=(2024, 2025),
    )

    def fake_train(
        train: pd.DataFrame,
        valid: pd.DataFrame,
        feats: list[str],
        p: Mapping[str, object],
        seed: int,
    ) -> np.ndarray:
        return np.array([float(i) for i in range(len(valid), 0, -1)])

    with patch.object(mod, "train_xgb_fold", side_effect=fake_train):
        result = mod.run_study(args)
    assert result["n_trials"] == 2
    assert (tmp_path / "out" / "best-params.json").exists()


def test_run_study_raises_when_sample_year_missing(tmp_path: Path) -> None:
    args = mod.TuneArgs(
        features_parquet_root=tmp_path / "missing",
        bucket_membership_parquet_root=tmp_path / "bucket",
        output_dir=tmp_path / "out",
        n_trials=1,
        timeout_seconds=10,
        random_seed=42,
        cv_years=(2024,),
    )
    with pytest.raises(RuntimeError):
        mod.run_study(args)


def test_run_study_unions_feature_columns_across_all_cv_years(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    bucket_root = tmp_path / "bucket"
    for year, race_prefix, has_feature_c in ((2022, "r22", False), (2023, "r23", True), (2024, "r24", True)):
        year_dir = features_root / f"race_year={year}"
        year_dir.mkdir(parents=True)
        row: dict[str, object] = {
            "race_id": [f"{race_prefix}a", f"{race_prefix}b"],
            "ketto_toroku_bango": ["h1", "h2"],
            "umaban": [1, 2],
            "finish_position": [1.0, 2.0],
            "feature_a": [0.1, 0.2],
            "feature_b": [0.3, 0.4],
        }
        if has_feature_c:
            row["feature_c"] = [0.9, 1.0]
        pd.DataFrame(row).to_parquet(year_dir / "data_0.parquet", index=False)

    discovered_cols: list[list[str]] = []

    def fake_train(
        train: pd.DataFrame,
        valid: pd.DataFrame,
        feats: list[str],
        p: Mapping[str, object],
        seed: int,
    ) -> np.ndarray:
        discovered_cols.append(feats)
        return np.array([float(i) for i in range(len(valid), 0, -1)])

    args = mod.TuneArgs(
        features_parquet_root=features_root,
        bucket_membership_parquet_root=bucket_root,
        output_dir=tmp_path / "out",
        n_trials=1,
        timeout_seconds=60,
        random_seed=42,
        cv_years=(2022, 2023, 2024),
    )
    with patch.object(mod, "train_xgb_fold", side_effect=fake_train):
        mod.run_study(args)
    all_discovered = {col for cols in discovered_cols for col in cols}
    assert "feature_c" in all_discovered


def test_main_returns_zero(tmp_path: Path) -> None:
    features_root = tmp_path / "feats"
    bucket_root = tmp_path / "bucket"
    _write_year_parquet(features_root, 2024, ["r1", "r2"])
    _write_year_parquet(features_root, 2025, ["r3", "r4"])
    argv = [
        "--features-parquet-root", str(features_root),
        "--bucket-membership-parquet", str(bucket_root),
        "--output-dir", str(tmp_path / "out"),
        "--n-trials", "1",
        "--timeout", "60",
        "--random-seed", "42",
        "--cv-years", "2024", "2025",
    ]

    def fake_train(
        train: pd.DataFrame,
        valid: pd.DataFrame,
        feats: list[str],
        p: Mapping[str, object],
        seed: int,
    ) -> np.ndarray:
        return np.array([float(i) for i in range(len(valid), 0, -1)])

    with patch.object(mod, "train_xgb_fold", side_effect=fake_train):
        rc = mod.main(argv)
    assert rc == 0
