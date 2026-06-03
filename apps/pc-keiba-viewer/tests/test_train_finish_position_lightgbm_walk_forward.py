from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

import train_finish_position_lightgbm_walk_forward as subject


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


def _base_args(tmp_path: Path) -> subject.TrainLightgbmArgs:
    return {
        "features_parquet": tmp_path / "feat",
        "category": "jra",
        "walk_forward_namespace": "jra-lgbm-v8-iter3-wf-21y",
        "year_from": 2024,
        "year_to": 2025,
        "train_start_date": "20060101",
        "model_root": tmp_path / "models",
        "iteration_id": 3,
        "alpha_bucket_weight": 0.0,
        "objective": "lambdarank",
        "lambdarank_truncation_level": 3,
        "hpo_params_path": None,
        "bucket_membership_parquet": None,
        "resume_from_checkpoint": False,
        "fine_tune_final_folds": 0,
        "fine_tune_lr_divisor": 10,
        "num_iterations": 500,
        "num_leaves": 63,
        "learning_rate": 0.05,
        "min_child_samples": 20,
        "lambda_l2": 0.0,
    }


def _make_fake_deps(
    df: pd.DataFrame,
    bucket_df: pd.DataFrame | None = None,
) -> subject.TrainDeps:
    return {
        "parquet_reader": MagicMock(return_value=df),
        "feature_resolver": MagicMock(return_value=["feature_a"]),
        "fold_trainer": MagicMock(
            return_value={"valid_predictions": df, "booster": MagicMock()},
        ),
        "bucket_reader": MagicMock(
            return_value=bucket_df if bucket_df is not None else pd.DataFrame(),
        ),
    }


def test_parse_args_defaults_objective_to_lambdarank():
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
        "2024",
        "--model-root",
        "tmp/models",
    ])
    assert args.objective == "lambdarank"
    assert args.lambdarank_truncation_level == 3


def test_parse_args_accepts_rank_xendcg():
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
        "2024",
        "--model-root",
        "tmp/models",
        "--objective",
        "rank_xendcg",
        "--lambdarank-truncation-level",
        "5",
    ])
    assert args.objective == "rank_xendcg"
    assert args.lambdarank_truncation_level == 5


def test_parse_args_rejects_unknown_objective():
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet",
            "tmp/feat",
            "--category",
            "jra",
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


def test_normalize_args_propagates_objective_and_truncation():
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
        "2024",
        "--model-root",
        "tmp/models",
        "--objective",
        "rank_xendcg",
        "--lambdarank-truncation-level",
        "10",
        "--hpo-params-path",
        "tmp/hpo.json",
        "--bucket-membership-parquet",
        "tmp/buckets",
    ])
    normalized = subject.normalize_args(raw)
    assert normalized["objective"] == "rank_xendcg"
    assert normalized["lambdarank_truncation_level"] == 10
    assert normalized["hpo_params_path"] == Path("tmp/hpo.json")
    assert normalized["bucket_membership_parquet"] == Path("tmp/buckets")


def test_load_hpo_params_returns_empty_when_none():
    assert subject.load_hpo_params(None) == {}


def test_load_hpo_params_returns_parsed(tmp_path: Path):
    path = tmp_path / "hpo.json"
    path.write_text(json.dumps({"num_iterations": 700, "num_leaves": 127}), encoding="utf-8")
    out = subject.load_hpo_params(path)
    assert out["num_iterations"] == 700
    assert out["num_leaves"] == 127


def test_load_hpo_params_raises_when_root_not_object(tmp_path: Path):
    path = tmp_path / "hpo.json"
    path.write_text("[1]", encoding="utf-8")
    with pytest.raises(ValueError):
        subject.load_hpo_params(path)


def test_apply_hpo_params_overrides_each_field(tmp_path: Path):
    base = _base_args(tmp_path)
    merged = subject.apply_hpo_params(
        base,
        {
            "num_iterations": 700,
            "num_leaves": 127,
            "learning_rate": 0.02,
            "min_child_samples": 40,
            "lambda_l2": 0.1,
        },
    )
    assert merged["num_iterations"] == 700
    assert merged["num_leaves"] == 127
    assert merged["learning_rate"] == 0.02
    assert merged["min_child_samples"] == 40
    assert merged["lambda_l2"] == 0.1


def test_apply_hpo_params_returns_unchanged_for_empty_dict(tmp_path: Path):
    base = _base_args(tmp_path)
    merged = subject.apply_hpo_params(base, {})
    assert merged["num_iterations"] == base["num_iterations"]


def test_resolve_fold_random_seed_offsets_by_year():
    assert subject.resolve_fold_random_seed(2025) == subject.RANDOM_SEED_BASE + 2025


def test_build_per_fold_model_dir_includes_iteration_and_fold(tmp_path: Path):
    args = _base_args(tmp_path)
    assert subject.build_per_fold_model_dir(args, 2024) == (
        tmp_path / "models" / "jra" / "iter3" / "fold-2024"
    )


def test_resolve_fold_learning_rate_no_fine_tune(tmp_path: Path):
    args = _base_args(tmp_path)
    assert subject.resolve_fold_learning_rate(args, 2025, [2023, 2024, 2025]) == 0.05


def test_resolve_fold_learning_rate_divides_tail(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 2
    args["fine_tune_lr_divisor"] = 10
    assert subject.resolve_fold_learning_rate(
        args, 2024, [2023, 2024, 2025],
    ) == pytest.approx(0.005)


def test_resolve_fold_learning_rate_returns_base_for_empty_fold_years(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    assert subject.resolve_fold_learning_rate(args, 2024, []) == 0.05


def test_resolve_fold_learning_rate_divisor_below_one(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    args["fine_tune_lr_divisor"] = 0
    assert subject.resolve_fold_learning_rate(args, 2024, [2024]) == 0.05


def test_resolve_fold_learning_rate_returns_base_for_pre_tail_folds(tmp_path: Path):
    args = _base_args(tmp_path)
    args["fine_tune_final_folds"] = 1
    args["fine_tune_lr_divisor"] = 10
    assert subject.resolve_fold_learning_rate(args, 2023, [2023, 2024, 2025]) == 0.05


def test_resolve_feature_columns_excludes_meta_and_labels():
    df = pd.DataFrame({
        "race_id": ["r1"],
        "race_date": ["20240101"],
        "finish_position": [1.0],
        "feature_a": [0.1],
        "feature_b": [0.2],
    })
    cols = subject.resolve_feature_columns(df)
    assert cols == ["feature_a", "feature_b"]


def test_resolve_feature_columns_drops_non_numeric():
    df = pd.DataFrame({
        "race_id": ["r1"],
        "race_date": ["20240101"],
        "finish_position": [1.0],
        "feature_a": [0.1],
        "label_text": ["text"],
    })
    cols = subject.resolve_feature_columns(df)
    assert cols == ["feature_a"]


def test_merge_bucket_weights_returns_train_when_none():
    train_df = _feature_df()
    assert subject.merge_bucket_weights_into_train(train_df, None).equals(train_df)


def test_merge_bucket_weights_attaches_column():
    train_df = _feature_df()
    bucket_df = pd.DataFrame({"race_id": ["r1", "r2"], "is_weak_bucket_score": [1.0, 0.0]})
    out = subject.merge_bucket_weights_into_train(train_df, bucket_df)
    assert out["is_weak_bucket_score"].tolist() == [1.0, 1.0, 0.0, 0.0]


def test_merge_bucket_weights_raises_for_missing_race_id():
    bucket_df = pd.DataFrame({"is_weak_bucket_score": [1.0]})
    with pytest.raises(ValueError):
        subject.merge_bucket_weights_into_train(_feature_df(), bucket_df)


def test_merge_bucket_weights_raises_for_missing_score():
    bucket_df = pd.DataFrame({"race_id": ["r1"]})
    with pytest.raises(ValueError):
        subject.merge_bucket_weights_into_train(_feature_df(), bucket_df)


def test_attach_sample_weights_time_only_when_alpha_zero_and_no_bucket():
    train_df = _feature_df()
    out = subject.attach_sample_weights(train_df, alpha=0.0)
    assert "sample_weight" in out.columns


def test_attach_sample_weights_combines_with_buckets_when_alpha_gt_zero():
    train_df = _feature_df().assign(is_weak_bucket_score=[1.0, 1.0, 0.0, 0.0])
    out = subject.attach_sample_weights(train_df, alpha=0.5)
    assert out["sample_weight"].iloc[0] > out["sample_weight"].iloc[2]


def test_attach_sample_weights_raises_when_race_year_missing():
    train_df = _feature_df().drop(columns=["race_year"])
    with pytest.raises(ValueError):
        subject.attach_sample_weights(train_df, alpha=0.0)


def test_build_fold_namespace_propagates_objective_and_truncation(tmp_path: Path):
    args = _base_args(tmp_path)
    args["objective"] = "rank_xendcg"
    args["lambdarank_truncation_level"] = 5
    ns = subject.build_fold_namespace(args, 2024, [2024])
    assert ns.objective == "rank_xendcg"
    assert ns.lambdarank_truncation_level == 5


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


def test_train_fold_skips_empty_train_or_valid(
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
    args["lambdarank_truncation_level"] = 5
    df = _feature_df()
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    out = subject.train_fold(df, ["feature_a"], args, 2024, [2024], deps, None)
    assert out["status"] == "completed"
    metadata_path = subject.build_per_fold_model_dir(args, 2024) / "metadata.json"
    parsed = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert parsed["objective"] == "lambdarank"
    assert parsed["lambdarank_truncation_level"] == 5
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
    assert "sample_weight" in train_call.args[0].columns


def test_resolve_fold_years_inclusive(tmp_path: Path):
    args = _base_args(tmp_path)
    args["year_from"] = 2023
    args["year_to"] = 2025
    assert subject.resolve_fold_years(args) == [2023, 2024, 2025]


def test_resolve_fold_years_raises_when_to_before_from(tmp_path: Path):
    args = _base_args(tmp_path)
    args["year_from"] = 2025
    args["year_to"] = 2024
    with pytest.raises(ValueError):
        subject.resolve_fold_years(args)


def test_run_iterates_folds_and_applies_hpo(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path)
    hpo_path = tmp_path / "hpo.json"
    hpo_path.write_text(json.dumps({"num_iterations": 800}), encoding="utf-8")
    args["hpo_params_path"] = hpo_path
    df = _feature_df()
    deps = _make_fake_deps(df)
    monkeypatch.setattr(subject, "split_train_valid", lambda *_a, **_k: (df, df))
    result = subject.run(args, deps)
    assert result["fold_count"] == 2
    assert result["objective"] == "lambdarank"


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


def test_default_parquet_reader_reads_partition_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    (tmp_path / "race_year=2024").mkdir(parents=True)
    parquet_path = tmp_path / "race_year=2024" / "0.parquet"
    parquet_path.write_text("placeholder", encoding="utf-8")
    sentinel = pd.DataFrame({"x": [1]})
    monkeypatch.setattr(pd, "read_parquet", MagicMock(return_value=sentinel))
    out = subject.default_parquet_reader(tmp_path)
    assert out["x"].tolist() == [1]


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
        return_value={
            "category": "jra",
            "fold_count": 0,
            "folds": [],
            "iteration_id": 3,
            "objective": "lambdarank",
        },
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
        "race_date": ["20230101", "20240101", "20250101"],
        "race_id": ["r1", "r2", "r3"],
        "umaban": [1, 1, 1],
        "finish_position": [1.0, 2.0, None],
    })
    train_df, valid_df = subject.split_train_valid(df, "20220101", 2024)
    assert train_df["race_id"].tolist() == ["r1"]
    assert valid_df["race_id"].tolist() == ["r2"]


def test_make_to_relevance_maps_finish_positions():
    to_relevance = subject.make_to_relevance()
    assert to_relevance(1) == 3
    assert to_relevance(2) == 2
    assert to_relevance(3) == 1
    assert to_relevance(4) == 0


def test_make_to_relevance_returns_zero_for_nan():
    to_relevance = subject.make_to_relevance()
    assert to_relevance(float("nan")) == 0


def test_make_to_relevance_returns_zero_for_none():
    to_relevance = subject.make_to_relevance()
    assert to_relevance(None) == 0


def test_build_group_sizes_returns_per_race_count():
    df = pd.DataFrame({"race_id": ["r1", "r1", "r1", "r2", "r2"]})
    assert subject.build_group_sizes(df) == [3, 2]


def test_default_fold_trainer_invokes_lightgbm_train(monkeypatch: pytest.MonkeyPatch):
    import lightgbm as lgb

    train_df = _feature_df().assign(sample_weight=[1.0, 1.0, 1.0, 1.0])
    valid_df = _feature_df()
    fake_dataset = MagicMock()
    monkeypatch.setattr(lgb, "Dataset", MagicMock(return_value=fake_dataset))
    fake_booster = MagicMock()
    fake_booster.predict = MagicMock(return_value=[0.9, 0.4, 0.8, 0.2])
    monkeypatch.setattr(lgb, "train", MagicMock(return_value=fake_booster))
    monkeypatch.setattr(lgb, "early_stopping", MagicMock(return_value=MagicMock()))
    ns = argparse.Namespace(
        objective="lambdarank",
        num_iterations=10,
        num_leaves=15,
        learning_rate=0.05,
        min_child_samples=5,
        lambda_l2=0.0,
        lambdarank_truncation_level=3,
        early_stopping_rounds=5,
        seed=42,
    )
    out = subject.default_fold_trainer(train_df, valid_df, ["feature_a"], ns)
    assert "valid_predictions" in out
    assert "booster" in out
    valid_predictions = cast(pd.DataFrame, out["valid_predictions"])
    assert "predicted_score" in valid_predictions.columns
    assert "predicted_rank" in valid_predictions.columns


def test_default_fold_trainer_supports_train_without_sample_weight(
    monkeypatch: pytest.MonkeyPatch,
):
    import lightgbm as lgb

    train_df = _feature_df()
    valid_df = _feature_df()
    monkeypatch.setattr(lgb, "Dataset", MagicMock())
    fake_booster = MagicMock()
    fake_booster.predict = MagicMock(return_value=[0.9, 0.4, 0.8, 0.2])
    monkeypatch.setattr(lgb, "train", MagicMock(return_value=fake_booster))
    monkeypatch.setattr(lgb, "early_stopping", MagicMock(return_value=MagicMock()))
    ns = argparse.Namespace(
        objective="lambdarank",
        num_iterations=10,
        num_leaves=15,
        learning_rate=0.05,
        min_child_samples=5,
        lambda_l2=0.0,
        lambdarank_truncation_level=3,
        early_stopping_rounds=5,
        seed=42,
    )
    out = subject.default_fold_trainer(train_df, valid_df, ["feature_a"], ns)
    assert "valid_predictions" in out
