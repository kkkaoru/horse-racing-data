from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

import finish_position_catboost as cb_walk
import finish_position_xgboost as xgb_walk
import score_finish_position_walk_forward as subject


def _base_args(tmp_path: Path, category: str) -> subject.WalkForwardArguments:
    return {
        "features_parquet": tmp_path / "features",
        "category": category,
        "walk_forward_namespace": f"{category}-v7-lineage-wf-21y",
        "year_from": 2024,
        "year_to": 2025,
        "train_start_date": "20060101",
        "output_parquet_root": tmp_path / "parquet",
        "output_jsonl_dir": tmp_path / "jsonl",
        "running_style_feature_version": "v3",
        "finish_position_version": "v1",
        "iterations": 500,
        "depth": 8,
        "l2_leaf_reg": 3.0,
        "learning_rate": 0.05,
        "num_rounds": 450,
        "max_depth": 6,
        "relevance_rank1": 3,
        "relevance_rank2": 2,
        "relevance_rank3": 1,
        "early_stopping_rounds": 30,
        "seed": 20260519,
        "iteration_id": 0,
        "calibration_path": None,
        "focus_features": None,
        "exclude_features": None,
    }


def _predictions_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": ["jra:2024:0512:05:11", "jra:2024:0512:05:11"],
        "ketto_toroku_bango": ["2019100001", "2019100002"],
        "umaban": [1, 2],
        "predicted_score": [0.91, 0.42],
        "predicted_rank": [1, 2],
    })


def _feature_df() -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": ["jra:2024:0512:05:11", "jra:2024:0512:05:11"],
        "race_date": ["20240512", "20240512"],
        "ketto_toroku_bango": ["2019100001", "2019100002"],
        "umaban": [1, 2],
        "finish_position": [1.0, 2.0],
    })


@dataclass
class FakeDeps:
    deps: subject.ScoreFoldDeps
    write_parquet: MagicMock
    write_jsonl: MagicMock
    cb_trainer: MagicMock
    xgb_trainer: MagicMock


def _make_fake_deps(predictions: pd.DataFrame, df: pd.DataFrame) -> FakeDeps:
    write_parquet = MagicMock()
    write_jsonl = MagicMock()
    cb_trainer = MagicMock(return_value=predictions)
    xgb_trainer = MagicMock(return_value=predictions)
    deps: subject.ScoreFoldDeps = {
        "parquet_reader": MagicMock(return_value=df),
        "catboost_resolver": MagicMock(return_value=[f"f{i}" for i in range(138)]),
        "xgboost_resolver": MagicMock(return_value=[f"f{i}" for i in range(126)]),
        "catboost_trainer": cb_trainer,
        "xgboost_trainer": xgb_trainer,
        "write_parquet": write_parquet,
        "write_jsonl": write_jsonl,
    }
    return FakeDeps(deps, write_parquet, write_jsonl, cb_trainer, xgb_trainer)


def test_parse_args_full_jra_set():
    args = subject.parse_args([
        "--features-parquet",
        "tmp/feat-jra-v7-final",
        "--category",
        "jra",
        "--walk-forward-namespace",
        "jra-cb-v7-lineage-wf-21y",
        "--year-from",
        "2007",
        "--year-to",
        "2026",
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/parquet",
        "--output-jsonl-dir",
        "tmp/jsonl",
    ])
    assert args.features_parquet == Path("tmp/feat-jra-v7-final")
    assert args.category == "jra"
    assert args.walk_forward_namespace == "jra-cb-v7-lineage-wf-21y"
    assert args.year_from == 2007
    assert args.year_to == 2026


def test_parse_args_rejects_unknown_category():
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet",
            "x",
            "--category",
            "bogus",
            "--walk-forward-namespace",
            "x",
            "--year-from",
            "2007",
            "--year-to",
            "2026",
            "--train-start-date",
            "20060101",
            "--output-parquet-root",
            "tmp/p",
            "--output-jsonl-dir",
            "tmp/j",
        ])


def test_default_iterations_for_category_jra_is_500():
    assert subject.default_iterations_for_category("jra") == 500


def test_default_iterations_for_category_banei_is_300():
    assert subject.default_iterations_for_category("banei") == 300


def test_normalize_arguments_uses_300_iterations_for_banei_when_unset():
    raw = subject.parse_args([
        "--features-parquet",
        "tmp/feat-ban-ei-v7-grade",
        "--category",
        "banei",
        "--walk-forward-namespace",
        "banei-cb-v7-lineage-wf-21y",
        "--year-from",
        "2008",
        "--year-to",
        "2026",
        "--train-start-date",
        "20070101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
    ])
    normalized = subject.normalize_arguments(raw)
    assert normalized["iterations"] == 300
    assert normalized["relevance_rank3"] == 1


def test_normalize_arguments_uses_xgb_relevance_321_for_nar():
    raw = subject.parse_args([
        "--features-parquet",
        "tmp/feat-nar-v7-baba",
        "--category",
        "nar",
        "--walk-forward-namespace",
        "nar-xgb-v7-lineage-wf-21y",
        "--year-from",
        "2007",
        "--year-to",
        "2026",
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
    ])
    normalized = subject.normalize_arguments(raw)
    assert normalized["relevance_rank1"] == 3
    assert normalized["relevance_rank2"] == 2
    assert normalized["relevance_rank3"] == 1


def test_normalize_arguments_honors_explicit_iterations_override():
    raw = subject.parse_args([
        "--features-parquet",
        "tmp/feat-jra-v7-final",
        "--category",
        "jra",
        "--walk-forward-namespace",
        "jra-cb-v7-lineage-wf-21y",
        "--year-from",
        "2007",
        "--year-to",
        "2026",
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
        "--iterations",
        "999",
        "--relevance-rank3",
        "5",
    ])
    normalized = subject.normalize_arguments(raw)
    assert normalized["iterations"] == 999
    assert normalized["relevance_rank3"] == 5


def test_resolve_relevance_returns_xgb_default_for_nar():
    assert subject.resolve_relevance("nar", None, 1, 2) == 2


def test_resolve_relevance_returns_cb_default_for_jra():
    assert subject.resolve_relevance("jra", None, 1, 2) == 1


def test_resolve_relevance_returns_raw_when_supplied():
    assert subject.resolve_relevance("nar", 7, 1, 2) == 7


def test_resolve_fold_years_is_inclusive_range():
    args = _base_args(Path("/tmp"), "jra")
    args["year_from"] = 2024
    args["year_to"] = 2026
    assert subject.resolve_fold_years(args) == [2024, 2025, 2026]


def test_resolve_fold_years_raises_when_to_before_from():
    args = _base_args(Path("/tmp"), "jra")
    args["year_from"] = 2026
    args["year_to"] = 2024
    with pytest.raises(ValueError) as info:
        subject.resolve_fold_years(args)
    assert "must be >=" in str(info.value)


def test_assert_feature_count_passes_on_exact_138_for_jra():
    subject.assert_feature_count("jra", [f"f{i}" for i in range(138)])


def test_assert_feature_count_raises_on_wrong_count_for_jra():
    with pytest.raises(ValueError) as info:
        subject.assert_feature_count("jra", [f"f{i}" for i in range(137)])
    assert "expected 138 features but resolved 137" in str(info.value)


def test_assert_feature_count_raises_on_wrong_count_for_nar():
    with pytest.raises(ValueError) as info:
        subject.assert_feature_count("nar", [f"f{i}" for i in range(137)])
    assert "expected 138 features but resolved 137" in str(info.value)


def test_assert_feature_count_passes_on_exact_138_for_banei():
    subject.assert_feature_count("banei", [f"f{i}" for i in range(138)])


def test_resolve_feature_columns_for_category_uses_xgboost_resolver_for_nar():
    df = pd.DataFrame({"a": [1, 2]})
    cb_resolver = MagicMock(return_value=["cb"])
    xgb_resolver = MagicMock(return_value=["xgb1", "xgb2"])
    cols = subject.resolve_feature_columns_for_category(
        df, "nar", catboost_resolver=cb_resolver, xgboost_resolver=xgb_resolver,
    )
    assert cols == ["xgb1", "xgb2"]
    xgb_resolver.assert_called_once_with(df, use_cat_features=False)
    cb_resolver.assert_not_called()


def test_resolve_feature_columns_for_category_disables_cat_features_for_jra():
    df = pd.DataFrame({"a": [1, 2]})
    cb_resolver = MagicMock(return_value=["cb1"])
    xgb_resolver = MagicMock(return_value=["xgb"])
    cols = subject.resolve_feature_columns_for_category(
        df, "jra", catboost_resolver=cb_resolver, xgboost_resolver=xgb_resolver,
    )
    assert cols == ["cb1"]
    cb_resolver.assert_called_once_with(df, use_cat_features=False)
    xgb_resolver.assert_not_called()


def test_build_fold_namespace_args_sets_train_end_to_prior_year_and_no_cat_for_jra():
    args = _base_args(Path("/tmp"), "jra")
    ns = subject.build_fold_namespace_args(args, 2025)
    assert ns.train_end_date == "20241231"
    assert ns.train_start_date == "20060101"
    assert ns.no_cat_features is True
    assert ns.iterations == 500


def test_build_fold_namespace_args_keeps_cat_features_enabled_for_nar():
    args = _base_args(Path("/tmp"), "nar")
    ns = subject.build_fold_namespace_args(args, 2024)
    assert ns.train_end_date == "20231231"
    assert ns.no_cat_features is False
    assert ns.num_rounds == 450
    assert ns.max_depth == 6


def test_resolve_fold_trainer_dispatches_xgboost_for_nar():
    cb_trainer = MagicMock()
    xgb_trainer = MagicMock()
    trainer = subject.resolve_fold_trainer(
        "nar", catboost_trainer=cb_trainer, xgboost_trainer=xgb_trainer,
    )
    assert trainer is xgb_trainer


def test_resolve_fold_trainer_dispatches_catboost_for_banei():
    cb_trainer = MagicMock()
    xgb_trainer = MagicMock()
    trainer = subject.resolve_fold_trainer(
        "banei", catboost_trainer=cb_trainer, xgboost_trainer=xgb_trainer,
    )
    assert trainer is cb_trainer


def test_split_race_id_column_explodes_five_parts():
    frame = pd.DataFrame({"race_id": ["nar:2024:0512:30:11"]})
    out = subject.split_race_id_column(frame)
    assert out["source"].tolist() == ["nar"]
    assert out["kaisai_nen"].tolist() == ["2024"]
    assert out["kaisai_tsukihi"].tolist() == ["0512"]
    assert out["keibajo_code"].tolist() == ["30"]
    assert out["race_bango"].tolist() == ["11"]


def test_split_race_id_column_raises_on_wrong_part_count():
    frame = pd.DataFrame({"race_id": ["nar:2024:0512"]})
    with pytest.raises(ValueError) as info:
        subject.split_race_id_column(frame)
    assert "5 colon-separated parts" in str(info.value)


def test_to_parquet_frame_emits_exactly_the_17_partitioned_columns():
    args = _base_args(Path("/tmp"), "jra")
    frame = subject.to_parquet_frame(_predictions_frame(), args, 2024)
    assert list(frame.columns) == [
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "umaban",
        "predicted_score",
        "predicted_rank",
        "predicted_top1_prob",
        "predicted_top3_prob",
        "predicted_finish_position",
        "model_version",
        "running_style_feature_version",
        "finish_position_version",
        "category",
        "race_year",
    ]


def test_to_parquet_frame_leaves_prob_columns_null_for_rank_only_models():
    args = _base_args(Path("/tmp"), "jra")
    frame = subject.to_parquet_frame(_predictions_frame(), args, 2024)
    assert frame["predicted_top1_prob"].tolist() == [None, None]
    assert frame["predicted_top3_prob"].tolist() == [None, None]
    assert frame["predicted_finish_position"].tolist() == [1, 2]
    assert frame["model_version"].tolist() == [
        "jra-v7-lineage-wf-21y",
        "jra-v7-lineage-wf-21y",
    ]
    assert frame["race_year"].tolist() == [2024, 2024]


def test_to_parquet_frame_stamps_versions_and_source_keys():
    args = _base_args(Path("/tmp"), "jra")
    frame = subject.to_parquet_frame(_predictions_frame(), args, 2024)
    assert frame["source"].tolist() == ["jra", "jra"]
    assert frame["kaisai_nen"].tolist() == ["2024", "2024"]
    assert frame["keibajo_code"].tolist() == ["05", "05"]
    assert frame["running_style_feature_version"].tolist() == ["v3", "v3"]
    assert frame["finish_position_version"].tolist() == ["v1", "v1"]


def test_to_parquet_frame_category_uses_jra_partition_for_jra():
    args = _base_args(Path("/tmp"), "jra")
    frame = subject.to_parquet_frame(_predictions_frame(), args, 2024)
    assert frame["category"].tolist() == ["jra", "jra"]


def test_to_parquet_frame_category_uses_nar_partition_for_nar():
    args = _base_args(Path("/tmp"), "nar")
    frame = subject.to_parquet_frame(_predictions_frame(), args, 2024)
    assert frame["category"].tolist() == ["nar", "nar"]


def test_to_parquet_frame_category_uses_hyphenated_ban_ei_partition_for_banei():
    args = _base_args(Path("/tmp"), "banei")
    frame = subject.to_parquet_frame(_predictions_frame(), args, 2024)
    assert frame["category"].tolist() == ["ban-ei", "ban-ei"]


def test_default_write_jsonl_writes_five_column_records(tmp_path: Path):
    output_path = tmp_path / "jra-v7-lineage-wf-21y-2024.jsonl"
    subject.default_write_jsonl(_predictions_frame(), output_path)
    lines = output_path.read_text(encoding="utf-8").strip().split("\n")
    first = json.loads(lines[0])
    assert first == {
        "race_id": "jra:2024:0512:05:11",
        "ketto_toroku_bango": "2019100001",
        "umaban": 1,
        "predicted_score": 0.91,
        "predicted_rank": 1,
    }
    assert len(lines) == 2


def test_default_write_jsonl_emits_null_umaban_when_missing(tmp_path: Path):
    frame = pd.DataFrame({
        "race_id": ["jra:2024:0512:05:11"],
        "ketto_toroku_bango": ["2019100001"],
        "umaban": [None],
        "predicted_score": [0.5],
        "predicted_rank": [3],
    })
    output_path = tmp_path / "out.jsonl"
    subject.default_write_jsonl(frame, output_path)
    record = json.loads(output_path.read_text(encoding="utf-8").strip())
    assert record["umaban"] is None
    assert record["predicted_rank"] == 3


def test_default_write_parquet_partitions_by_category_and_year(tmp_path: Path):
    frame = MagicMock(spec=pd.DataFrame)
    output_dir = tmp_path / "parquet"
    subject.default_write_parquet(frame, output_dir)
    assert output_dir.exists()
    frame.to_parquet.assert_called_once_with(
        output_dir.as_posix(),
        partition_cols=["category", "race_year"],
        index=False,
        existing_data_behavior="delete_matching",
    )


def test_build_jsonl_filename_uses_category_and_year():
    args = _base_args(Path("/tmp"), "nar")
    assert subject.build_jsonl_filename(args, 2026) == "nar-v7-lineage-wf-21y-2026.jsonl"


def test_xgboost_numeric_resolver_ignores_use_cat_features(monkeypatch: pytest.MonkeyPatch):
    df = pd.DataFrame({"a": [1, 2]})
    inner = MagicMock(return_value=["x1", "x2", "x3"])
    monkeypatch.setattr(xgb_walk, "resolve_feature_columns", inner)
    cols = subject.xgboost_numeric_resolver(df, use_cat_features=True)
    assert cols == ["x1", "x2", "x3"]
    inner.assert_called_once_with(df)


def test_score_fold_skips_when_no_train_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    args = _base_args(tmp_path, "jra")
    fake = _make_fake_deps(_predictions_frame(), _feature_df())
    monkeypatch.setattr(
        subject, "build_fold_train_valid", MagicMock(return_value=(pd.DataFrame(), _feature_df())),
    )
    result = subject.score_fold(_feature_df(), [f"f{i}" for i in range(138)], args, 2024, fake.deps)
    assert result == {"fold_year": 2024, "skipped": True, "rows": 0}
    fake.write_parquet.assert_not_called()
    fake.write_jsonl.assert_not_called()


def test_score_fold_skips_when_no_valid_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    args = _base_args(tmp_path, "jra")
    fake = _make_fake_deps(_predictions_frame(), _feature_df())
    monkeypatch.setattr(
        subject, "build_fold_train_valid", MagicMock(return_value=(_feature_df(), pd.DataFrame())),
    )
    result = subject.score_fold(_feature_df(), [f"f{i}" for i in range(138)], args, 2025, fake.deps)
    assert result == {"fold_year": 2025, "skipped": True, "rows": 0}


def test_score_fold_trains_writes_parquet_and_jsonl_for_jra(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path, "jra")
    fake = _make_fake_deps(_predictions_frame(), _feature_df())
    monkeypatch.setattr(
        subject,
        "build_fold_train_valid",
        MagicMock(return_value=(_feature_df(), _feature_df())),
    )
    result = subject.score_fold(_feature_df(), [f"f{i}" for i in range(138)], args, 2024, fake.deps)
    assert result == {"fold_year": 2024, "skipped": False, "rows": 2}
    fake.cb_trainer.assert_called_once()
    fake.xgb_trainer.assert_not_called()
    fake.write_parquet.assert_called_once()
    jsonl_call = fake.write_jsonl.call_args
    assert jsonl_call.args[1] == tmp_path / "jsonl" / "jra-v7-lineage-wf-21y-2024.jsonl"


def test_score_fold_uses_xgboost_trainer_for_nar(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path, "nar")
    predictions = pd.DataFrame({
        "race_id": ["nar:2024:0512:30:11", "nar:2024:0512:30:11"],
        "ketto_toroku_bango": ["a", "b"],
        "umaban": [1, 2],
        "predicted_score": [0.8, 0.2],
        "predicted_rank": [1, 2],
    })
    fake = _make_fake_deps(predictions, _feature_df())
    monkeypatch.setattr(
        subject,
        "build_fold_train_valid",
        MagicMock(return_value=(_feature_df(), _feature_df())),
    )
    result = subject.score_fold(_feature_df(), [f"f{i}" for i in range(126)], args, 2024, fake.deps)
    assert result == {"fold_year": 2024, "skipped": False, "rows": 2}
    fake.xgb_trainer.assert_called_once()
    fake.cb_trainer.assert_not_called()


def test_run_resolves_features_asserts_parity_and_iterates_folds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path, "jra")
    df = _feature_df()
    fake = _make_fake_deps(_predictions_frame(), df)
    monkeypatch.setattr(
        subject,
        "build_fold_train_valid",
        MagicMock(return_value=(df, df)),
    )
    result = subject.run(args, fake.deps)
    assert result["category"] == "jra"
    assert result["model_version"] == "jra-v7-lineage-wf-21y"
    assert result["fold_count"] == 2
    assert result["feature_count"] == 138


def test_run_raises_when_feature_parity_guard_fails(tmp_path: Path):
    args = _base_args(tmp_path, "jra")
    fake = _make_fake_deps(_predictions_frame(), _feature_df())
    fake.deps["catboost_resolver"] = MagicMock(return_value=[f"f{i}" for i in range(100)])
    with pytest.raises(ValueError) as info:
        subject.run(args, fake.deps)
    assert "expected 138 features but resolved 100" in str(info.value)


def test_default_train_catboost_fold_unwraps_valid_predictions(monkeypatch: pytest.MonkeyPatch):
    predictions = _predictions_frame()
    trainer = MagicMock(return_value={"valid_predictions": predictions, "metrics": {}})
    monkeypatch.setattr(cb_walk, "train_catboost_ranker", trainer)
    out = subject.default_train_catboost_fold(
        pd.DataFrame(), pd.DataFrame(), ["f0"], argparse.Namespace(),
    )
    assert out["predicted_rank"].tolist() == [1, 2]
    trainer.assert_called_once()


def test_default_train_xgboost_fold_unwraps_valid_predictions(monkeypatch: pytest.MonkeyPatch):
    predictions = _predictions_frame()
    trainer = MagicMock(return_value=(MagicMock(), {"valid_predictions": predictions}))
    monkeypatch.setattr(xgb_walk, "train_xgboost_ranker", trainer)
    out = subject.default_train_xgboost_fold(
        pd.DataFrame(), pd.DataFrame(), ["f0"], argparse.Namespace(),
    )
    assert out["predicted_rank"].tolist() == [1, 2]
    trainer.assert_called_once()


def test_build_default_deps_wires_real_helpers():
    deps = subject.build_default_deps()
    assert deps["parquet_reader"] is cb_walk.load_parquet_dir
    assert deps["catboost_resolver"] is cb_walk.resolve_feature_columns
    assert deps["xgboost_resolver"] is subject.xgboost_numeric_resolver
    assert deps["catboost_trainer"] is subject.default_train_catboost_fold
    assert deps["xgboost_trainer"] is subject.default_train_xgboost_fold


def test_main_runs_and_prints_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
    fake_run = MagicMock(
        return_value={
            "category": "jra",
            "model_version": "jra-cb-v7-lineage-wf-21y",
            "fold_count": 2,
            "folds": [],
            "feature_count": 138,
        },
    )
    monkeypatch.setattr(subject, "run", fake_run)
    monkeypatch.setattr(subject, "build_default_deps", MagicMock(return_value={}))
    subject.main([
        "--features-parquet",
        "tmp/feat-jra-v7-final",
        "--category",
        "jra",
        "--walk-forward-namespace",
        "jra-cb-v7-lineage-wf-21y",
        "--year-from",
        "2007",
        "--year-to",
        "2026",
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/parquet",
        "--output-jsonl-dir",
        "tmp/jsonl",
    ])
    fake_run.assert_called_once()
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["category"] == "jra"
    assert payload["model_version"] == "jra-cb-v7-lineage-wf-21y"
    assert payload["feature_count"] == 138


def test_build_fold_train_valid_filters_by_prior_year_end(monkeypatch: pytest.MonkeyPatch):
    df = _feature_df()
    train_mock = MagicMock(return_value=df.iloc[:1])
    year_mock = MagicMock(return_value=df.iloc[1:])
    monkeypatch.setattr(cb_walk, "filter_range", train_mock)
    monkeypatch.setattr(cb_walk, "filter_year", year_mock)
    args = _base_args(Path("/tmp"), "jra")
    train_df, valid_df = subject.build_fold_train_valid(df, args, 2025)
    train_mock.assert_called_once_with(df, "20060101", "20241231")
    year_mock.assert_called_once_with(df, 2025)
    assert len(train_df) == 1
    assert len(valid_df) == 1


def test_parse_args_accepts_iteration_id_and_calibration_path():
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
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
        "--iteration-id",
        "3",
        "--calibration-path",
        "tmp/calib.json",
    ])
    assert args.iteration_id == 3
    assert args.calibration_path == Path("tmp/calib.json")


def test_normalize_arguments_propagates_iteration_id_and_calibration_path():
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
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
        "--iteration-id",
        "5",
        "--calibration-path",
        "tmp/iso.json",
    ])
    normalized = subject.normalize_arguments(raw)
    assert normalized["iteration_id"] == 5
    assert normalized["calibration_path"] == Path("tmp/iso.json")


def test_normalize_arguments_defaults_iteration_id_to_zero_and_calibration_to_none():
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
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
    ])
    normalized = subject.normalize_arguments(raw)
    assert normalized["iteration_id"] == 0
    assert normalized["calibration_path"] is None


def test_load_calibration_map_returns_empty_when_path_none():
    assert subject.load_calibration_map(None) == {}


def test_load_calibration_map_returns_parsed_pairs(tmp_path: Path):
    path = tmp_path / "iso.json"
    path.write_text(json.dumps({"jra": [[0.0, 0.1], [1.0, 0.9]]}), encoding="utf-8")
    out = subject.load_calibration_map(path)
    assert out == {"jra": [[0.0, 0.1], [1.0, 0.9]]}


def test_load_calibration_map_raises_when_root_not_object(tmp_path: Path):
    path = tmp_path / "iso.json"
    path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with pytest.raises(ValueError) as info:
        subject.load_calibration_map(path)
    assert "top-level object" in str(info.value)


def test_load_calibration_map_raises_when_breakpoints_not_monotone(tmp_path: Path):
    path = tmp_path / "iso.json"
    path.write_text(
        json.dumps({"jra": [[0.0, 0.1], [0.8, 0.7], [0.5, 0.5]]}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError) as info:
        subject.load_calibration_map(path)
    assert "monotonically non-decreasing" in str(info.value)
    assert "jra" in str(info.value)


def test_apply_calibration_returns_input_when_map_empty():
    predictions = _predictions_frame()
    out = subject.apply_calibration(predictions, {}, "jra")
    assert out.equals(predictions)


def test_apply_calibration_returns_input_when_bucket_missing():
    predictions = _predictions_frame()
    out = subject.apply_calibration(predictions, {"nar": [[0.0, 0.5]]}, "jra")
    assert out.equals(predictions)


def test_apply_calibration_interpolates_and_reranks():
    predictions = pd.DataFrame({
        "race_id": ["jra:2024:0512:05:11", "jra:2024:0512:05:11"],
        "ketto_toroku_bango": ["a", "b"],
        "umaban": [1, 2],
        "predicted_score": [0.5, 0.9],
        "predicted_rank": [2, 1],
    })
    calibration_map = {"jra": [[0.0, 0.1], [1.0, 0.7]]}
    out = subject.apply_calibration(predictions, calibration_map, "jra")
    assert out["predicted_score"].iloc[0] == pytest.approx(0.4)
    assert out["predicted_score"].iloc[1] == pytest.approx(0.64)
    assert out["predicted_rank"].tolist() == [2, 1]


def test_apply_calibration_clamps_to_first_pair_when_score_below_min():
    predictions = pd.DataFrame({
        "race_id": ["r", "r"],
        "ketto_toroku_bango": ["a", "b"],
        "umaban": [1, 2],
        "predicted_score": [-1.0, 2.0],
        "predicted_rank": [1, 2],
    })
    calibration_map = {"jra": [[0.0, 0.1], [1.0, 0.7]]}
    out = subject.apply_calibration(predictions, calibration_map, "jra")
    assert out["predicted_score"].iloc[0] == pytest.approx(0.1)
    assert out["predicted_score"].iloc[1] == pytest.approx(0.7)


def test_apply_calibration_handles_zero_span_segment_without_division_error():
    predictions = pd.DataFrame({
        "race_id": ["r"],
        "ketto_toroku_bango": ["a"],
        "umaban": [1],
        "predicted_score": [0.5],
        "predicted_rank": [1],
    })
    calibration_map = {"jra": [[0.5, 0.3], [0.5, 0.4], [1.0, 0.9]]}
    out = subject.apply_calibration(predictions, calibration_map, "jra")
    assert out["predicted_score"].iloc[0] == pytest.approx(0.3)


def test_apply_calibration_returns_input_when_bucket_pairs_empty():
    predictions = _predictions_frame()
    out = subject.apply_calibration(predictions, {"jra": []}, "jra")
    assert out.equals(predictions)


def test_interp_calibrated_returns_score_when_pairs_empty():
    assert subject.interp_calibrated(0.5, []) == 0.5


def test_filter_features_by_patterns_focus_only():
    cols = ["venue_temp", "venue_humidity", "sire_win_rate", "legacy_speed"]
    result = subject.filter_features_by_patterns(cols, "venue_*", None)
    assert result == ["venue_temp", "venue_humidity"]


def test_filter_features_by_patterns_exclude_only():
    cols = ["venue_temp", "legacy_speed", "legacy_pace", "sire_win_rate"]
    result = subject.filter_features_by_patterns(cols, None, "legacy_*")
    assert result == ["venue_temp", "sire_win_rate"]


def test_filter_features_by_patterns_focus_and_exclude():
    cols = ["venue_temp", "venue_legacy_flag", "sire_win_rate", "legacy_speed"]
    result = subject.filter_features_by_patterns(cols, "venue_*", "*legacy*")
    assert result == ["venue_temp"]


def test_filter_features_by_patterns_no_match_returns_empty():
    cols = ["sire_win_rate", "legacy_speed", "odds_rank"]
    result = subject.filter_features_by_patterns(cols, "venue_*", None)
    assert result == []


def test_filter_features_by_patterns_none_returns_all():
    cols = ["venue_temp", "sire_win_rate", "legacy_speed"]
    result = subject.filter_features_by_patterns(cols, None, None)
    assert result == ["venue_temp", "sire_win_rate", "legacy_speed"]


def test_filter_features_by_patterns_multiple_patterns():
    cols = ["venue_temp", "sire_win_rate", "legacy_speed", "odds_rank"]
    result = subject.filter_features_by_patterns(cols, "venue_*,sire_*", None)
    assert result == ["venue_temp", "sire_win_rate"]


def test_parse_args_accepts_focus_and_exclude_features():
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
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
        "--focus-features",
        "venue_*,sire_*",
        "--exclude-features",
        "legacy_*",
    ])
    assert args.focus_features == "venue_*,sire_*"
    assert args.exclude_features == "legacy_*"


def test_normalize_arguments_propagates_focus_and_exclude_features():
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
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
        "--focus-features",
        "venue_*",
        "--exclude-features",
        "odds_*",
    ])
    normalized = subject.normalize_arguments(raw)
    assert normalized["focus_features"] == "venue_*"
    assert normalized["exclude_features"] == "odds_*"


def test_normalize_arguments_defaults_focus_and_exclude_to_none():
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
        "--train-start-date",
        "20060101",
        "--output-parquet-root",
        "tmp/p",
        "--output-jsonl-dir",
        "tmp/j",
    ])
    normalized = subject.normalize_arguments(raw)
    assert normalized["focus_features"] is None
    assert normalized["exclude_features"] is None


def test_run_applies_feature_filter_and_skips_parity_when_focus_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path, "jra")
    args["focus_features"] = "f1*"
    df = _feature_df()
    fake = _make_fake_deps(_predictions_frame(), df)
    fake.deps["catboost_resolver"] = MagicMock(return_value=["f1", "f10", "f2", "f3"])
    monkeypatch.setattr(
        subject,
        "build_fold_train_valid",
        MagicMock(return_value=(df, df)),
    )
    result = subject.run(args, fake.deps)
    assert result["feature_count"] == 2
    cb_call = fake.cb_trainer.call_args
    assert cb_call.args[2] == ["f1", "f10"]


def test_run_applies_exclude_filter_and_skips_parity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    args = _base_args(tmp_path, "jra")
    args["exclude_features"] = "legacy_*"
    df = _feature_df()
    fake = _make_fake_deps(_predictions_frame(), df)
    fake.deps["catboost_resolver"] = MagicMock(return_value=["venue_temp", "legacy_speed"])
    monkeypatch.setattr(
        subject,
        "build_fold_train_valid",
        MagicMock(return_value=(df, df)),
    )
    result = subject.run(args, fake.deps)
    assert result["feature_count"] == 1


def test_run_raises_when_no_features_remain_after_filtering(tmp_path: Path):
    args = _base_args(tmp_path, "jra")
    args["focus_features"] = "nonexistent_*"
    fake = _make_fake_deps(_predictions_frame(), _feature_df())
    fake.deps["catboost_resolver"] = MagicMock(return_value=["venue_temp", "sire_rate"])
    with pytest.raises(ValueError) as info:
        subject.run(args, fake.deps)
    assert "No features remaining after filtering" in str(info.value)


def test_apply_calibration_assigns_bottom_rank_to_nan_score():
    predictions = pd.DataFrame({
        "race_id": ["r", "r"],
        "ketto_toroku_bango": ["a", "b"],
        "umaban": [1, 2],
        "predicted_score": [float("nan"), 0.8],
        "predicted_rank": [1, 2],
    })
    calibration_map = {"jra": [[0.0, 0.1], [1.0, 0.9]]}
    out = subject.apply_calibration(predictions, calibration_map, "jra")
    # NaN score propagates through calibration and must get bottom rank (2), not crash
    assert out["predicted_rank"].tolist() == [2, 1]
