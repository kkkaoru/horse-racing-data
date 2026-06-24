from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

import train_finish_position_catboost_walk_forward as subject


def _feature_df() -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "race_date": ["20240512", "20240512", "20240519", "20240519"],
        "race_year": [2024, 2024, 2024, 2024],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "umaban": [1, 2, 1, 2],
        "finish_position": [1.0, 2.0, 1.0, 2.0],
        "feature_a": [0.1, 0.2, 0.3, 0.4],
    })


def _base_args(tmp_path: Path) -> subject.TrainCatBoostArgs:
    return {
        "features_parquet": tmp_path / "feat",
        "category": "jra",
        "walk_forward_namespace": "jra-cb-v8-iter1-wf-21y",
        "year_from": 2024,
        "year_to": 2025,
        "train_start_date": "20060101",
        "model_root": tmp_path / "models",
        "iteration_id": 1,
        "alpha_bucket_weight": 0.0,
        "hpo_params_path": None,
        "bucket_membership_parquet": None,
        "resume_from_checkpoint": False,
        "fine_tune_final_folds": 0,
        "fine_tune_lr_divisor": 10,
        "focus_features": None,
        "exclude_features": None,
        "iterations": 500,
        "depth": 8,
        "l2_leaf_reg": 3.0,
        "bagging_temperature": None,
        "random_strength": None,
        "learning_rate": 0.05,
    }


def _make_fake_deps(
    df: pd.DataFrame,
    feature_cols: list[str] | None = None,
    bucket_df: pd.DataFrame | None = None,
) -> subject.TrainDeps:
    return {
        "parquet_reader": MagicMock(return_value=df),
        "feature_resolver": MagicMock(return_value=feature_cols or ["feature_a"]),
        "fold_trainer": MagicMock(
            return_value={
                "valid_predictions": df,
                "metrics": {},
                "best_iteration": 100,
            },
        ),
        "bucket_reader": MagicMock(return_value=bucket_df if bucket_df is not None else pd.DataFrame()),
    }


def test_parse_args_required_set():
    args = subject.parse_args([
        "--features-parquet",
        "tmp/feat",
        "--category",
        "jra",
        "--walk-forward-namespace",
        "ns",
        "--year-from",
        "2024",
        "--year-to",
        "2025",
        "--model-root",
        "tmp/models",
    ])
    assert args.features_parquet == Path("tmp/feat")
    assert args.iteration_id == 0
    assert args.alpha_bucket_weight == 0.0
    assert args.resume_from_checkpoint is False


def test_parse_args_accepts_all_v8_flags():
    args = subject.parse_args([
        "--features-parquet",
        "tmp/feat",
        "--category",
        "jra",
        "--walk-forward-namespace",
        "ns",
        "--year-from",
        "2024",
        "--year-to",
        "2025",
        "--model-root",
        "tmp/models",
        "--iteration-id",
        "7",
        "--alpha-bucket-weight",
        "0.5",
        "--hpo-params-path",
        "tmp/hpo.json",
        "--bucket-membership-parquet",
        "tmp/buckets",
        "--resume-from-checkpoint",
        "--fine-tune-final-folds",
        "3",
        "--fine-tune-lr-divisor",
        "20",
    ])
    assert args.iteration_id == 7
    assert args.alpha_bucket_weight == 0.5
    assert args.hpo_params_path == Path("tmp/hpo.json")
    assert args.bucket_membership_parquet == Path("tmp/buckets")
    assert args.resume_from_checkpoint is True
    assert args.fine_tune_final_folds == 3
    assert args.fine_tune_lr_divisor == 20


def test_normalize_args_converts_paths_and_floats():
    raw = subject.parse_args([
        "--features-parquet",
        "tmp/feat",
        "--category",
        "jra",
        "--walk-forward-namespace",
        "ns",
        "--year-from",
        "2024",
        "--year-to",
        "2025",
        "--model-root",
        "tmp/models",
        "--alpha-bucket-weight",
        "0.4",
    ])
    normalized = subject.normalize_args(raw)
    assert normalized["features_parquet"] == Path("tmp/feat")
    assert normalized["category"] == "jra"
    assert normalized["alpha_bucket_weight"] == 0.4
    assert normalized["hpo_params_path"] is None
    assert normalized["bucket_membership_parquet"] is None


def test_load_hpo_params_returns_empty_for_none_path():
    assert subject.load_hpo_params(None) == {}


def test_load_hpo_params_returns_parsed(tmp_path: Path):
    path = tmp_path / "hpo.json"
    path.write_text(json.dumps({"iterations": 999, "depth": 10}), encoding="utf-8")
    out = subject.load_hpo_params(path)
    assert out == {"iterations": 999, "depth": 10}


def test_load_hpo_params_raises_when_root_not_object(tmp_path: Path):
    path = tmp_path / "hpo.json"
    path.write_text(json.dumps([1, 2]), encoding="utf-8")
    with pytest.raises(ValueError) as info:
        subject.load_hpo_params(path)
    assert "JSON object" in str(info.value)


def test_load_hpo_params_extracts_nested_params_key(tmp_path: Path):
    """Tune scripts write {trial_number, params: {...}, global_ndcg}; load_hpo_params
    must return only the inner params dict, not the top-level wrapper."""
    path = tmp_path / "hpo.json"
    path.write_text(
        json.dumps({
            "trial_number": 3,
            "params": {"iterations": 800, "depth": 10, "bagging_temperature": 1.5},
            "global_ndcg": 0.75,
        }),
        encoding="utf-8",
    )
    result = subject.load_hpo_params(path)
    assert result == {"iterations": 800, "depth": 10, "bagging_temperature": 1.5}


def test_apply_hpo_params_overrides_each_field(tmp_path: Path):
    base = _base_args(tmp_path)
    merged = subject.apply_hpo_params(
        base,
        {"iterations": 800, "depth": 10, "l2_leaf_reg": 5.0, "learning_rate": 0.1},
    )
    assert merged["iterations"] == 800
    assert merged["depth"] == 10
    assert merged["l2_leaf_reg"] == 5.0
    assert merged["learning_rate"] == 0.1


def test_apply_hpo_params_applies_bagging_temperature_and_random_strength(tmp_path: Path):
    base = _base_args(tmp_path)
    merged = subject.apply_hpo_params(
        base, {"bagging_temperature": 1.0, "random_strength": 2.5},
    )
    assert merged["bagging_temperature"] == pytest.approx(1.0)
    assert merged["random_strength"] == pytest.approx(2.5)


def test_apply_hpo_params_skips_unknown_keys(tmp_path: Path):
    base = _base_args(tmp_path)
    merged = subject.apply_hpo_params(base, {"bogus": 1})
    assert merged["iterations"] == base["iterations"]


def test_resolve_fold_random_seed_is_base_plus_year():
    assert subject.resolve_fold_random_seed(2025) == subject.RANDOM_SEED_BASE + 2025


def test_build_per_fold_model_dir_includes_iteration_and_fold(tmp_path: Path):
    args = _base_args(tmp_path)
    path = subject.build_per_fold_model_dir(args, 2025)
    assert path == tmp_path / "models" / "jra" / "iter1" / "fold-2025"


def test_resolve_fold_learning_rate_returns_base_when_no_fine_tune(tmp_path: Path):
    args = _base_args(tmp_path)
    assert subject.resolve_fold_learning_rate(args, 2025, [2023, 2024, 2025]) == 0.05


def test_resolve_fold_learning_rate_divides_in_tail_folds(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 2
    args["fine_tune_lr_divisor"] = 5
    fold_years = [2023, 2024, 2025]
    assert subject.resolve_fold_learning_rate(args, 2023, fold_years) == 0.05
    assert subject.resolve_fold_learning_rate(args, 2024, fold_years) == pytest.approx(0.01)
    assert subject.resolve_fold_learning_rate(args, 2025, fold_years) == pytest.approx(0.01)


def test_resolve_fold_learning_rate_handles_divisor_below_one(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    args["fine_tune_lr_divisor"] = 0
    assert subject.resolve_fold_learning_rate(args, 2024, [2024]) == 0.05


def test_resolve_fold_learning_rate_handles_more_tail_than_folds(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 5
    fold_years = [2024]
    assert subject.resolve_fold_learning_rate(args, 2024, fold_years) == pytest.approx(0.005)


def test_resolve_fold_learning_rate_returns_base_when_tail_clamps_to_zero(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    assert subject.resolve_fold_learning_rate(args, 2024, []) == 0.05


def test_merge_bucket_weights_returns_train_unchanged_when_no_bucket_df():
    train_df = _feature_df()
    out = subject.merge_bucket_weights_into_train(train_df, None)
    assert out.equals(train_df)


def test_merge_bucket_weights_joins_score_column():
    train_df = _feature_df()
    bucket_df = pd.DataFrame({
        "race_id": ["r1", "r2"],
        "is_weak_bucket_score": [1.0, 0.0],
    })
    out = subject.merge_bucket_weights_into_train(train_df, bucket_df)
    assert "is_weak_bucket_score" in out.columns
    assert out["is_weak_bucket_score"].tolist() == [1.0, 1.0, 0.0, 0.0]


def test_merge_bucket_weights_raises_when_race_id_missing():
    train_df = _feature_df()
    bucket_df = pd.DataFrame({"is_weak_bucket_score": [1.0]})
    with pytest.raises(ValueError) as info:
        subject.merge_bucket_weights_into_train(train_df, bucket_df)
    assert "race_id" in str(info.value)


def test_merge_bucket_weights_raises_when_score_missing():
    train_df = _feature_df()
    bucket_df = pd.DataFrame({"race_id": ["r1"]})
    with pytest.raises(ValueError) as info:
        subject.merge_bucket_weights_into_train(train_df, bucket_df)
    assert "is_weak_bucket_score" in str(info.value)


def test_merge_bucket_weights_deduplicates_bucket_df_by_race_id():
    """Duplicate race_ids in bucket_df must not multiply training rows."""
    train_df = _feature_df()  # 4 rows: 2 horses × 2 races
    bucket_df = pd.DataFrame({
        "race_id": ["r1", "r1", "r2"],  # r1 appears twice
        "is_weak_bucket_score": [1.0, 0.5, 0.0],
    })
    out = subject.merge_bucket_weights_into_train(train_df, bucket_df)
    assert len(out) == len(train_df)  # no row multiplication


def test_attach_sample_weights_uses_time_decay_only_when_no_bucket_column():
    train_df = _feature_df()
    out = subject.attach_sample_weights(train_df, alpha=0.5)
    assert "sample_weight" in out.columns
    assert all(0.5 <= w <= 1.0 for w in out["sample_weight"].tolist())


def test_attach_sample_weights_combines_with_bucket_scores_when_alpha_gt_zero():
    train_df = _feature_df().assign(is_weak_bucket_score=[1.0, 1.0, 0.0, 0.0])
    out = subject.attach_sample_weights(train_df, alpha=0.5)
    weights = out["sample_weight"].tolist()
    assert weights[0] > weights[2]


def test_attach_sample_weights_uses_time_only_when_alpha_is_zero_even_with_buckets():
    train_df = _feature_df().assign(is_weak_bucket_score=[1.0, 1.0, 0.0, 0.0])
    out = subject.attach_sample_weights(train_df, alpha=0.0)
    weights = out["sample_weight"].tolist()
    assert weights[0] == weights[2]


def test_attach_sample_weights_raises_when_race_year_missing():
    train_df = _feature_df().drop(columns=["race_year"])
    with pytest.raises(ValueError) as info:
        subject.attach_sample_weights(train_df, alpha=0.0)
    assert "race_year" in str(info.value)


def test_build_fold_namespace_sets_seed_and_lr(tmp_path: Path):
    args = _base_args(tmp_path)
    ns = subject.build_fold_namespace(args, 2025, [2024, 2025])
    assert ns.seed == subject.RANDOM_SEED_BASE + 2025
    assert ns.learning_rate == 0.05
    assert ns.iterations == 500


def test_build_fold_namespace_applies_fine_tune_lr_for_tail(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    args["fine_tune_lr_divisor"] = 10
    ns = subject.build_fold_namespace(args, 2025, [2024, 2025])
    assert ns.learning_rate == pytest.approx(0.005)


def test_build_fold_namespace_propagates_bagging_temperature_and_random_strength(tmp_path: Path):
    args = _base_args(tmp_path)
    args["bagging_temperature"] = 1.5
    args["random_strength"] = 2.0
    ns = subject.build_fold_namespace(args, 2025, [2024, 2025])
    assert ns.bagging_temperature == pytest.approx(1.5)
    assert ns.random_strength == pytest.approx(2.0)


def test_build_fold_namespace_propagates_none_bagging_temperature(tmp_path: Path):
    args = _base_args(tmp_path)
    ns = subject.build_fold_namespace(args, 2025, [2024, 2025])
    assert ns.bagging_temperature is None
    assert ns.random_strength is None


def test_build_fold_namespace_sets_presorted_true(tmp_path: Path):
    """run() sorts the full dataset once, so each fold passes presorted=True to
    let train_catboost_ranker skip its redundant per-fold sort_values."""
    args = _base_args(tmp_path)
    ns = subject.build_fold_namespace(args, 2025, [2024, 2025])
    assert ns.presorted is True


def test_build_fold_namespace_sets_no_cat_features_false(tmp_path: Path):
    """no_cat_features=False in the namespace must be consistent with
    default_feature_resolver using use_cat_features=True so that
    categorical columns are actually included in feature_cols and
    passed to the CatBoost Pool."""
    args = _base_args(tmp_path)
    ns = subject.build_fold_namespace(args, 2025, [2024, 2025])
    assert ns.no_cat_features is False


def test_train_fold_skips_when_checkpoint_completed(tmp_path: Path):
    args = _base_args(tmp_path)
    args["resume_from_checkpoint"] = True
    args["model_root"] = tmp_path
    model_dir = subject.build_per_fold_model_dir(args, 2024)
    model_dir.mkdir(parents=True)
    (model_dir / "metadata.json").write_text(
        json.dumps({"status": "completed", "fold_year": 2024}),
        encoding="utf-8",
    )
    deps = _make_fake_deps(_feature_df())
    out = subject.train_fold(_feature_df(), ["feature_a"], args, 2024, [2024], deps, None)
    assert out["status"] == "completed"
    assert out["resumed"] is True
    cast(MagicMock, deps["fold_trainer"]).assert_not_called()


def test_train_fold_skips_empty_and_writes_skip_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    deps = _make_fake_deps(_feature_df())
    monkeypatch.setattr(
        subject,
        "split_train_valid",
        lambda *_args, **_kw: (pd.DataFrame(), pd.DataFrame()),
    )
    out = subject.train_fold(_feature_df(), ["feature_a"], args, 2024, [2024], deps, None)
    assert out["status"] == "skipped"
    assert out["rows"] == 0
    metadata_path = subject.build_per_fold_model_dir(args, 2024) / "metadata.json"
    parsed = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert parsed["status"] == "skipped"
    assert parsed["reason"] == "empty-train-or-valid"


def test_train_fold_trains_and_writes_completed_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    args["alpha_bucket_weight"] = 0.5
    df = _feature_df()
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    out = subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, None)
    assert out["status"] == "completed"
    assert out["resumed"] is False
    assert out["rows"] == 4
    metadata_path = subject.build_per_fold_model_dir(args, 2024) / "metadata.json"
    parsed = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert parsed["status"] == "completed"
    assert parsed["random_seed"] == subject.RANDOM_SEED_BASE + 2024
    assert parsed["alpha_bucket_weight"] == 0.5


def test_train_fold_passes_bucket_df_through(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    args["alpha_bucket_weight"] = 0.5
    df = _feature_df()
    bucket_df = pd.DataFrame({
        "race_id": ["r1", "r2"],
        "is_weak_bucket_score": [1.0, 0.0],
    })
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, bucket_df)
    train_call = cast(MagicMock, deps["fold_trainer"]).call_args
    weighted_train = train_call.args[0]
    assert "is_weak_bucket_score" in weighted_train.columns
    assert "sample_weight" in weighted_train.columns


def test_train_fold_saves_model_json_when_fold_trainer_returns_model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """train_fold must call model.save_model(..., format='json') when
    the fold trainer returns a 'model' key, so continuous_learner can
    stage model.json to the prediction container."""
    args = _base_args(tmp_path)
    df = _feature_df()
    mock_model = MagicMock()
    deps = _make_fake_deps(df)
    cast(MagicMock, deps["fold_trainer"]).return_value = {
        "valid_predictions": df,
        "best_iteration": 10,
        "model": mock_model,
    }
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, None)
    model_dir = subject.build_per_fold_model_dir(args, 2024)
    expected_path = str(model_dir / "model.json")
    mock_model.save_model.assert_called_once_with(expected_path, format="json")


def test_train_fold_creates_model_dir_before_save_model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """model_dir must exist when model.save_model() is called; CatBoost
    does not auto-create parent directories and raises otherwise."""
    args = _base_args(tmp_path)
    df = _feature_df()
    model_dir = subject.build_per_fold_model_dir(args, 2024)
    dir_existed_at_save: list[bool] = []

    def check_dir(path: str, format: str) -> None:
        dir_existed_at_save.append(model_dir.exists())

    mock_model = MagicMock()
    mock_model.save_model.side_effect = check_dir
    deps = _make_fake_deps(df)
    cast(MagicMock, deps["fold_trainer"]).return_value = {
        "valid_predictions": df,
        "best_iteration": 10,
        "model": mock_model,
    }
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, None)
    assert dir_existed_at_save == [True]


def test_train_fold_does_not_save_model_when_fold_trainer_omits_model_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """Fold trainers that don't return a 'model' key (e.g. mocks or eval-only
    runs) must not crash — save_model is simply skipped."""
    args = _base_args(tmp_path)
    df = _feature_df()
    deps = _make_fake_deps(df)  # mock returns dict without 'model' key
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    out = subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, None)
    assert out["status"] == "completed"
    model_dir = subject.build_per_fold_model_dir(args, 2024)
    assert not (model_dir / "model.json").exists()


def test_resolve_fold_years_inclusive(tmp_path: Path):
    args = _base_args(tmp_path)
    args["year_from"] = 2023
    args["year_to"] = 2025
    assert subject.resolve_fold_years(args) == [2023, 2024, 2025]


def test_resolve_fold_years_raises_when_to_before_from(tmp_path: Path):
    args = _base_args(tmp_path)
    args["year_from"] = 2026
    args["year_to"] = 2024
    with pytest.raises(ValueError):
        subject.resolve_fold_years(args)


def test_filter_feature_cols_raises_when_both_focus_and_exclude():
    with pytest.raises(ValueError) as info:
        subject.filter_feature_cols(["a", "b"], ["a"], ["b"])
    assert "mutually exclusive" in str(info.value)


def test_filter_feature_cols_focus_keeps_listed_plus_categorical():
    resolved = ["barei", "seibetsu_code", "feature_x", "keibajo_code", "umaban"]
    out = subject.filter_feature_cols(resolved, ["barei", "seibetsu_code"], None)
    assert out == ["barei", "seibetsu_code", "keibajo_code", "umaban"]


def test_filter_feature_cols_exclude_removes_listed():
    resolved = ["barei", "seibetsu_code", "feature_x"]
    out = subject.filter_feature_cols(resolved, None, ["barei"])
    assert out == ["seibetsu_code", "feature_x"]


def test_filter_feature_cols_focus_raises_for_unknown_feature():
    with pytest.raises(ValueError) as info:
        subject.filter_feature_cols(["barei"], ["barei", "nonexistent"], None)
    assert "nonexistent" in str(info.value)


def test_filter_feature_cols_focus_allows_categorical_not_in_resolved_without_error():
    out = subject.filter_feature_cols(["barei"], ["barei", "keibajo_code"], None)
    assert out == ["barei"]


def test_filter_feature_cols_returns_unchanged_when_neither_specified():
    resolved = ["barei", "seibetsu_code", "feature_x"]
    out = subject.filter_feature_cols(resolved, None, None)
    assert out == ["barei", "seibetsu_code", "feature_x"]


def test_normalize_args_splits_focus_features():
    raw = subject.parse_args([
        "--features-parquet", "tmp/feat",
        "--category", "jra",
        "--walk-forward-namespace", "ns",
        "--year-from", "2024",
        "--year-to", "2025",
        "--model-root", "tmp/models",
        "--focus-features", "barei, seibetsu_code",
    ])
    normalized = subject.normalize_args(raw)
    assert normalized["focus_features"] == ["barei", "seibetsu_code"]
    assert normalized["exclude_features"] is None


def test_run_applies_exclude_features_and_reports_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    args["exclude_features"] = ["feature_a"]
    df = _feature_df()
    deps = _make_fake_deps(df, feature_cols=["feature_a", "feature_b"])
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    result = subject.run(args, deps)
    assert result["feature_count"] == 1
    assert result["exclude_features"] == ["feature_a"]
    assert result["focus_features"] is None


def test_run_applies_hpo_overrides_and_iterates_folds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    hpo_path = tmp_path / "hpo.json"
    hpo_path.write_text(json.dumps({"iterations": 999}), encoding="utf-8")
    args["hpo_params_path"] = hpo_path
    df = _feature_df()
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    result = subject.run(args, deps)
    assert result["fold_count"] == 2
    assert result["iteration_id"] == 1
    cast(MagicMock, deps["parquet_reader"]).assert_called_once()


def test_run_sorts_full_dataset_once_before_folds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """run() must sort the loaded frame by (race_id, umaban) exactly once so the
    per-fold trainer can run with presorted=True; the feature resolver therefore
    sees the sorted frame, not the raw load order."""
    args = _base_args(tmp_path)
    unsorted = pd.DataFrame({
        "race_id": ["r2", "r1", "r2", "r1"],
        "race_date": ["20240519", "20240512", "20240519", "20240512"],
        "race_year": [2024, 2024, 2024, 2024],
        "ketto_toroku_bango": ["c", "a", "d", "b"],
        "umaban": [1, 1, 2, 2],
        "finish_position": [1.0, 1.0, 2.0, 2.0],
        "feature_a": [0.3, 0.1, 0.4, 0.2],
    })
    deps = _make_fake_deps(unsorted)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (unsorted, unsorted))
    subject.run(args, deps)
    seen = cast(MagicMock, deps["feature_resolver"]).call_args.args[0]
    assert seen["race_id"].tolist() == ["r1", "r1", "r2", "r2"]
    assert seen["umaban"].tolist() == [1, 2, 1, 2]


def test_run_reads_bucket_parquet_when_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    args["bucket_membership_parquet"] = tmp_path / "buckets"
    df = _feature_df()
    bucket_df = pd.DataFrame({"race_id": ["r1"], "is_weak_bucket_score": [0.5]})
    deps = _make_fake_deps(df, bucket_df=bucket_df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    subject.run(args, deps)
    cast(MagicMock, deps["bucket_reader"]).assert_called_once_with(tmp_path / "buckets")


def test_build_default_deps_returns_callable_set():
    deps = subject.build_default_deps()
    assert callable(deps["parquet_reader"])
    assert callable(deps["feature_resolver"])
    assert callable(deps["fold_trainer"])
    assert callable(deps["bucket_reader"])


def test_default_parquet_reader_delegates_to_finish_position_catboost(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    import finish_position_catboost as cb_walk

    sentinel = pd.DataFrame({"x": [1]})
    loader = MagicMock(return_value=sentinel)
    monkeypatch.setattr(cb_walk, "load_parquet_dir", loader)
    out = subject.default_parquet_reader(tmp_path)
    loader.assert_called_once_with(tmp_path)
    assert out is sentinel


def test_default_feature_resolver_delegates_with_cat_features(monkeypatch: pytest.MonkeyPatch):
    import finish_position_catboost as cb_walk

    resolver = MagicMock(return_value=["x"])
    monkeypatch.setattr(cb_walk, "resolve_feature_columns", resolver)
    out = subject.default_feature_resolver(pd.DataFrame({"x": [1]}))
    assert out == ["x"]
    resolver.assert_called_once()
    _, kwargs = resolver.call_args
    assert kwargs.get("use_cat_features") is True


def test_default_fold_trainer_calls_train_catboost_ranker(monkeypatch: pytest.MonkeyPatch):
    import finish_position_catboost as cb_walk

    expected = {"valid_predictions": pd.DataFrame(), "metrics": {}, "best_iteration": 1}
    trainer = MagicMock(return_value=expected)
    monkeypatch.setattr(cb_walk, "train_catboost_ranker", trainer)
    out = subject.default_fold_trainer(
        pd.DataFrame(), pd.DataFrame(), ["x"], argparse.Namespace(),
    )
    assert out is expected
    trainer.assert_called_once()


def test_default_bucket_reader_calls_pandas_read_parquet(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    sentinel = pd.DataFrame({"x": [1]})
    monkeypatch.setattr(pd, "read_parquet", MagicMock(return_value=sentinel))
    out = subject.default_bucket_reader(tmp_path)
    assert out is sentinel


def test_main_prints_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    fake_run = MagicMock(
        return_value={"category": "jra", "fold_count": 0, "folds": [], "iteration_id": 1},
    )
    monkeypatch.setattr(subject, "run", fake_run)
    monkeypatch.setattr(subject, "build_default_deps", MagicMock(return_value={}))
    subject.main([
        "--features-parquet",
        str(tmp_path / "feat"),
        "--category",
        "jra",
        "--walk-forward-namespace",
        "ns",
        "--year-from",
        "2024",
        "--year-to",
        "2024",
        "--model-root",
        str(tmp_path / "models"),
    ])
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["category"] == "jra"


def test_split_train_valid_filters_dates_and_labels():
    df = pd.DataFrame({
        "race_date": ["20230101", "20230515", "20240101", "20250101"],
        "race_id": ["r1", "r2", "r3", "r4"],
        "umaban": [1, 1, 1, 1],
        "finish_position": [1.0, 2.0, 1.0, None],
    })
    train_df, valid_df = subject.split_train_valid(df, "20220101", 2024)
    assert train_df["race_id"].tolist() == ["r1", "r2"]
    assert valid_df["race_id"].tolist() == ["r3"]
