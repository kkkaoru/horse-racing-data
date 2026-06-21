from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import train_finish_position_xgboost_walk_forward as subject


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


def _base_args(tmp_path: Path) -> subject.TrainXgboostArgs:
    return {
        "features_parquet": tmp_path / "feat",
        "category": "nar",
        "walk_forward_namespace": "nar-xgb-v8-iter1-wf-21y",
        "year_from": 2024,
        "year_to": 2025,
        "train_start_date": "20060101",
        "model_root": tmp_path / "models",
        "iteration_id": 1,
        "alpha_bucket_weight": 0.0,
        "objective": "pairwise",
        "hpo_params_path": None,
        "bucket_membership_parquet": None,
        "resume_from_checkpoint": False,
        "fine_tune_final_folds": 0,
        "fine_tune_lr_divisor": 10,
        "num_rounds": 450,
        "max_depth": 6,
        "learning_rate": 0.05,
    }


def _make_fake_deps(
    df: pd.DataFrame,
    bucket_df: pd.DataFrame | None = None,
) -> subject.TrainDeps:
    return {
        "parquet_reader": MagicMock(return_value=df),
        "feature_resolver": MagicMock(return_value=["feature_a"]),
        "fold_trainer": MagicMock(
            return_value=(MagicMock(), {"valid_predictions": df, "best_iteration": 100}),
        ),
        "bucket_reader": MagicMock(return_value=bucket_df if bucket_df is not None else pd.DataFrame()),
    }


def test_parse_args_defaults_objective_to_pairwise():
    args = subject.parse_args([
        "--features-parquet",
        "tmp/feat",
        "--category",
        "nar",
        "--walk-forward-namespace",
        "ns",
        "--year-from",
        "2024",
        "--year-to",
        "2024",
        "--model-root",
        "tmp/models",
    ])
    assert args.objective == "pairwise"


def test_parse_args_accepts_ndcg_objective():
    args = subject.parse_args([
        "--features-parquet",
        "tmp/feat",
        "--category",
        "nar",
        "--walk-forward-namespace",
        "ns",
        "--year-from",
        "2024",
        "--year-to",
        "2024",
        "--model-root",
        "tmp/models",
        "--objective",
        "ndcg",
    ])
    assert args.objective == "ndcg"


def test_parse_args_rejects_unknown_objective():
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet",
            "tmp/feat",
            "--category",
            "nar",
            "--walk-forward-namespace",
            "ns",
            "--year-from",
            "2024",
            "--year-to",
            "2024",
            "--model-root",
            "tmp/models",
            "--objective",
            "bogus",
        ])


def test_normalize_args_propagates_objective_and_paths():
    raw = subject.parse_args([
        "--features-parquet",
        "tmp/feat",
        "--category",
        "nar",
        "--walk-forward-namespace",
        "ns",
        "--year-from",
        "2024",
        "--year-to",
        "2024",
        "--model-root",
        "tmp/models",
        "--objective",
        "ndcg",
        "--hpo-params-path",
        "tmp/hpo.json",
        "--bucket-membership-parquet",
        "tmp/buckets",
    ])
    normalized = subject.normalize_args(raw)
    assert normalized["objective"] == "ndcg"
    assert normalized["hpo_params_path"] == Path("tmp/hpo.json")
    assert normalized["bucket_membership_parquet"] == Path("tmp/buckets")


def test_load_hpo_params_returns_empty_when_none():
    assert subject.load_hpo_params(None) == {}


def test_load_hpo_params_returns_parsed(tmp_path: Path):
    path = tmp_path / "hpo.json"
    path.write_text(json.dumps({"num_rounds": 600}), encoding="utf-8")
    assert subject.load_hpo_params(path) == {"num_rounds": 600}


def test_load_hpo_params_raises_when_root_not_object(tmp_path: Path):
    path = tmp_path / "hpo.json"
    path.write_text(json.dumps([1, 2]), encoding="utf-8")
    with pytest.raises(ValueError) as info:
        subject.load_hpo_params(path)
    assert "JSON object" in str(info.value)


def test_apply_hpo_params_overrides_each_field(tmp_path: Path):
    base = _base_args(tmp_path)
    merged = subject.apply_hpo_params(
        base, {"num_rounds": 700, "max_depth": 9, "learning_rate": 0.02},
    )
    assert merged["num_rounds"] == 700
    assert merged["max_depth"] == 9
    assert merged["learning_rate"] == 0.02


def test_apply_hpo_params_returns_unchanged_for_empty_dict(tmp_path: Path):
    base = _base_args(tmp_path)
    merged = subject.apply_hpo_params(base, {})
    assert merged["num_rounds"] == base["num_rounds"]


def test_resolve_fold_random_seed_offsets_by_year():
    assert subject.resolve_fold_random_seed(2024) == subject.RANDOM_SEED_BASE + 2024


def test_build_per_fold_model_dir_includes_iteration_and_fold(tmp_path: Path):
    args = _base_args(tmp_path)
    assert subject.build_per_fold_model_dir(args, 2024) == (
        tmp_path / "models" / "nar" / "iter1" / "fold-2024"
    )


def test_resolve_fold_learning_rate_no_fine_tune_returns_base(tmp_path: Path):
    args = _base_args(tmp_path)
    assert subject.resolve_fold_learning_rate(args, 2025, [2023, 2024, 2025]) == 0.05


def test_resolve_fold_learning_rate_handles_tail_division(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 2
    args["fine_tune_lr_divisor"] = 10
    assert subject.resolve_fold_learning_rate(
        args, 2025, [2023, 2024, 2025],
    ) == pytest.approx(0.005)


def test_resolve_fold_learning_rate_returns_base_when_fold_years_empty(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    assert subject.resolve_fold_learning_rate(args, 2024, []) == 0.05


def test_resolve_fold_learning_rate_handles_divisor_below_one(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    args["fine_tune_lr_divisor"] = -5
    assert subject.resolve_fold_learning_rate(args, 2024, [2024]) == 0.05


def test_resolve_fold_learning_rate_returns_base_for_pre_tail_folds(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    args["fine_tune_lr_divisor"] = 10
    assert subject.resolve_fold_learning_rate(args, 2023, [2023, 2024, 2025]) == 0.05


def test_merge_bucket_weights_returns_train_unchanged_when_none():
    train_df = _feature_df()
    assert subject.merge_bucket_weights_into_train(train_df, None).equals(train_df)


def test_merge_bucket_weights_joins_score_column():
    train_df = _feature_df()
    bucket_df = pd.DataFrame({"race_id": ["r1", "r2"], "is_weak_bucket_score": [1.0, 0.0]})
    out = subject.merge_bucket_weights_into_train(train_df, bucket_df)
    assert out["is_weak_bucket_score"].tolist() == [1.0, 1.0, 0.0, 0.0]


def test_merge_bucket_weights_raises_when_race_id_missing():
    bucket_df = pd.DataFrame({"is_weak_bucket_score": [1.0]})
    with pytest.raises(ValueError):
        subject.merge_bucket_weights_into_train(_feature_df(), bucket_df)


def test_merge_bucket_weights_raises_when_score_missing():
    bucket_df = pd.DataFrame({"race_id": ["r1"]})
    with pytest.raises(ValueError):
        subject.merge_bucket_weights_into_train(_feature_df(), bucket_df)


def test_merge_bucket_weights_deduplicates_bucket_df_by_race_id():
    """Duplicate race_ids in bucket_df must not multiply training rows."""
    bucket_df = pd.DataFrame({
        "race_id": ["r1", "r1", "r2"],  # r1 appears twice
        "is_weak_bucket_score": [1.0, 0.5, 0.0],
    })
    out = subject.merge_bucket_weights_into_train(_feature_df(), bucket_df)
    assert len(out) == len(_feature_df())  # no row multiplication


def test_attach_sample_weights_uses_time_only_when_bucket_absent():
    train_df = _feature_df()
    out = subject.attach_sample_weights(train_df, alpha=0.0)
    assert "sample_weight" in out.columns


def test_attach_sample_weights_combines_when_alpha_gt_zero():
    train_df = _feature_df().assign(is_weak_bucket_score=[1.0, 1.0, 0.0, 0.0])
    out = subject.attach_sample_weights(train_df, alpha=0.5)
    assert out["sample_weight"].iloc[0] > out["sample_weight"].iloc[2]


def test_attach_sample_weights_raises_when_race_year_missing():
    train_df = _feature_df().drop(columns=["race_year"])
    with pytest.raises(ValueError):
        subject.attach_sample_weights(train_df, alpha=0.0)


def test_build_fold_namespace_sets_relevance_rank3_to_one(tmp_path: Path):
    args = _base_args(tmp_path)
    ns = subject.build_fold_namespace(args, 2024, [2024])
    assert ns.relevance_rank3 == 1


def test_build_fold_namespace_includes_lambdarank_extras_when_ndcg(tmp_path: Path):
    args = _base_args(tmp_path)
    args["objective"] = "ndcg"
    ns = subject.build_fold_namespace(args, 2024, [2024])
    assert ns.objective == "ndcg"
    assert ns.lambdarank_pair_method == "topk"
    assert ns.lambdarank_num_pair_per_sample == 3


def test_build_fold_namespace_omits_lambdarank_extras_for_pairwise(tmp_path: Path):
    args = _base_args(tmp_path)
    ns = subject.build_fold_namespace(args, 2024, [2024])
    assert not hasattr(ns, "lambdarank_pair_method")


def test_train_fold_skips_when_checkpoint_completed(tmp_path: Path):
    args = _base_args(tmp_path)
    args["resume_from_checkpoint"] = True
    model_dir = subject.build_per_fold_model_dir(args, 2024)
    model_dir.mkdir(parents=True)
    (model_dir / "metadata.json").write_text(
        json.dumps({"status": "completed", "fold_year": 2024}),
        encoding="utf-8",
    )
    deps = _make_fake_deps(_feature_df())
    out = subject.train_fold(_feature_df(), ["feature_a"], args, 2024, [2024], deps, None)
    assert out["resumed"] is True
    cast(MagicMock, deps["fold_trainer"]).assert_not_called()


def test_train_fold_skips_empty_and_writes_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    deps = _make_fake_deps(_feature_df())
    monkeypatch.setattr(
        subject, "split_train_valid", lambda *_a, **_k: (pd.DataFrame(), pd.DataFrame()),
    )
    out = subject.train_fold(_feature_df(), ["feature_a"], args, 2024, [2024], deps, None)
    assert out["status"] == "skipped"


def test_train_fold_completes_and_writes_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    args["objective"] = "ndcg"
    df = _feature_df()
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    out = subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, None)
    assert out["status"] == "completed"
    metadata_path = subject.build_per_fold_model_dir(args, 2024) / "metadata.json"
    parsed = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert parsed["objective"] == "ndcg"
    assert parsed["random_seed"] == subject.RANDOM_SEED_BASE + 2024


def test_train_fold_passes_bucket_df_through(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    args["alpha_bucket_weight"] = 0.5
    df = _feature_df()
    bucket_df = pd.DataFrame({"race_id": ["r1", "r2"], "is_weak_bucket_score": [1.0, 0.0]})
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, bucket_df)
    train_call = cast(MagicMock, deps["fold_trainer"]).call_args
    weighted_train = train_call.args[0]
    assert "is_weak_bucket_score" in weighted_train.columns


def test_train_fold_saves_model_json_via_booster(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """train_fold must call booster.save_model(path) so continuous_learner
    can stage model.json to the prediction container.  The booster is the
    first element of the tuple returned by the fold trainer."""
    args = _base_args(tmp_path)
    df = _feature_df()
    mock_booster = MagicMock()
    deps = _make_fake_deps(df)
    cast(MagicMock, deps["fold_trainer"]).return_value = (
        mock_booster,
        {"valid_predictions": df, "best_iteration": 10},
    )
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, None)
    model_dir = subject.build_per_fold_model_dir(args, 2024)
    expected_path = str(model_dir / "model.json")
    mock_booster.save_model.assert_called_once_with(expected_path)


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


def test_run_applies_hpo_overrides_and_iterates_folds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    hpo_path = tmp_path / "hpo.json"
    hpo_path.write_text(json.dumps({"num_rounds": 600}), encoding="utf-8")
    args["hpo_params_path"] = hpo_path
    df = _feature_df()
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    result = subject.run(args, deps)
    assert result["fold_count"] == 2
    assert result["objective"] == "pairwise"


def test_run_reads_bucket_parquet_when_path_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    args["bucket_membership_parquet"] = tmp_path / "buckets"
    df = _feature_df()
    bucket_df = pd.DataFrame({"race_id": ["r1"], "is_weak_bucket_score": [0.5]})
    deps = _make_fake_deps(df, bucket_df=bucket_df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    subject.run(args, deps)
    cast(MagicMock, deps["bucket_reader"]).assert_called_once()


def test_build_default_deps_returns_callable_set():
    deps = subject.build_default_deps()
    assert callable(deps["parquet_reader"])
    assert callable(deps["feature_resolver"])
    assert callable(deps["fold_trainer"])
    assert callable(deps["bucket_reader"])


def test_default_parquet_reader_delegates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import finish_position_xgboost as xgb_walk

    sentinel = pd.DataFrame({"x": [1]})
    loader = MagicMock(return_value=sentinel)
    monkeypatch.setattr(xgb_walk, "load_parquet_dir", loader)
    out = subject.default_parquet_reader(tmp_path)
    loader.assert_called_once_with(tmp_path)
    assert out is sentinel


def test_default_feature_resolver_delegates(monkeypatch: pytest.MonkeyPatch):
    import finish_position_xgboost as xgb_walk

    resolver = MagicMock(return_value=["x"])
    monkeypatch.setattr(xgb_walk, "resolve_feature_columns", resolver)
    out = subject.default_feature_resolver(pd.DataFrame({"x": [1]}))
    assert out == ["x"]


def test_default_fold_trainer_delegates(monkeypatch: pytest.MonkeyPatch):
    import finish_position_xgboost as xgb_walk

    expected = (MagicMock(), {"valid_predictions": pd.DataFrame(), "best_iteration": 1})
    trainer = MagicMock(return_value=expected)
    monkeypatch.setattr(xgb_walk, "train_xgboost_ranker", trainer)
    out = subject.default_fold_trainer(
        pd.DataFrame(), pd.DataFrame(), ["x"], argparse.Namespace(),
    )
    assert out is expected


def test_default_bucket_reader_uses_pandas_read_parquet(
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
        return_value={"category": "nar", "fold_count": 0, "folds": [], "iteration_id": 1},
    )
    monkeypatch.setattr(subject, "run", fake_run)
    monkeypatch.setattr(subject, "build_default_deps", MagicMock(return_value={}))
    subject.main([
        "--features-parquet",
        str(tmp_path / "feat"),
        "--category",
        "nar",
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
    assert payload["category"] == "nar"


def test_train_xgboost_ranker_uses_pairwise_objective_by_default(monkeypatch: pytest.MonkeyPatch):
    import finish_position_xgboost as fp_xgb
    import xgboost as xgb

    captured_params: list[dict[str, object]] = []

    def fake_train(params: dict[str, object], *args: object, **kwargs: object) -> object:
        captured_params.append(dict(params))
        fake_booster = MagicMock()
        fake_booster.best_iteration = 5
        fake_booster.predict.return_value = np.array([0.9, 0.5])
        return fake_booster

    monkeypatch.setattr(xgb, "train", fake_train)
    df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1, 2],
        "finish_position": [1.0, 2.0],
    })
    ns = argparse.Namespace(
        relevance_rank1=3, relevance_rank2=2, relevance_rank3=1,
        learning_rate=0.05, max_depth=6, min_child_weight=30, reg_lambda=1.0,
        seed=42, verbosity=1, num_rounds=10, early_stopping_rounds=5,
    )
    fp_xgb.train_xgboost_ranker(df, df, [], ns)
    assert captured_params[0]["objective"] == "rank:pairwise"
    assert "lambdarank_pair_method" not in captured_params[0]


def test_train_xgboost_ranker_uses_ndcg_objective_when_set(monkeypatch: pytest.MonkeyPatch):
    import finish_position_xgboost as fp_xgb
    import xgboost as xgb

    captured_params: list[dict[str, object]] = []

    def fake_train(params: dict[str, object], *args: object, **kwargs: object) -> object:
        captured_params.append(dict(params))
        fake_booster = MagicMock()
        fake_booster.best_iteration = 5
        fake_booster.predict.return_value = np.array([0.9, 0.5])
        return fake_booster

    monkeypatch.setattr(xgb, "train", fake_train)
    df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1, 2],
        "finish_position": [1.0, 2.0],
    })
    ns = argparse.Namespace(
        relevance_rank1=3, relevance_rank2=2, relevance_rank3=1,
        learning_rate=0.05, max_depth=6, min_child_weight=30, reg_lambda=1.0,
        seed=42, verbosity=1, num_rounds=10, early_stopping_rounds=5,
        objective="ndcg", lambdarank_pair_method="topk", lambdarank_num_pair_per_sample=3,
    )
    fp_xgb.train_xgboost_ranker(df, df, [], ns)
    assert captured_params[0]["objective"] == "rank:ndcg"
    assert captured_params[0]["lambdarank_pair_method"] == "topk"
    assert captured_params[0]["lambdarank_num_pair_per_sample"] == 3


def test_split_train_valid_filters_dates_and_labels():
    df = pd.DataFrame({
        "race_date": ["20230101", "20240101", "20250101"],
        "race_id": ["r1", "r2", "r3"],
        "umaban": [1, 1, 1],
        "finish_position": [1.0, 2.0, None],
    })
    train_df, valid_df = subject.split_train_valid(df, "20220101", 2024)
    assert train_df["race_id"].tolist() == ["r1"]
    assert valid_df["race_id"].tolist() == ["r2"]
