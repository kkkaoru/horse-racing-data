from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import finish_position_catboost as subject


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _small_df() -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "race_date": ["20230101", "20230101", "20230201", "20230201"],
        "race_year": [2023, 2023, 2023, 2023],
        "source": ["jra", "jra", "jra", "jra"],
        "kaisai_nen": [2023, 2023, 2023, 2023],
        "kaisai_tsukihi": ["0101", "0101", "0201", "0201"],
        "race_bango": [1, 1, 1, 1],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "bamei": ["H1", "H2", "H3", "H4"],
        "kishumei_ryakusho": ["J1", "J2", "J3", "J4"],
        "chokyoshimei_ryakusho": ["T1", "T2", "T3", "T4"],
        "category": ["jra", "jra", "jra", "jra"],
        "keibajo_code": ["01", "01", "02", "02"],
        "track_code": ["A", "A", "B", "B"],
        "grade_code": ["G1", "G1", "G2", "G2"],
        "umaban": [1, 2, 1, 2],
        "finish_position": [1.0, 2.0, 1.0, 2.0],
        "finish_norm": [1.0, 0.5, 1.0, 0.5],
        "feature_a": [0.1, 0.2, 0.3, 0.4],
    })


def _make_args(**overrides: object) -> argparse.Namespace:
    ns = argparse.Namespace(
        train_end_date=None,
        validation_from_date=None,
        validation_to_date=None,
        train_start_date="20200101",
        validation_years="2024,2025",
        relevance_rank1=3,
        relevance_rank2=2,
        relevance_rank3=1,
        iterations=10,
        learning_rate=0.05,
        depth=3,
        l2_leaf_reg=3.0,
        early_stopping_rounds=5,
        seed=42,
        no_cat_features=False,
    )
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


# ---------------------------------------------------------------------------
# subtract_one_day
# ---------------------------------------------------------------------------

def test_subtract_one_day_normal_date():
    assert subject.subtract_one_day("20240101") == "20231231"


def test_subtract_one_day_within_month():
    assert subject.subtract_one_day("20240115") == "20240114"


def test_subtract_one_day_month_boundary():
    assert subject.subtract_one_day("20240301") == "20240229"


def test_subtract_one_day_year_boundary():
    assert subject.subtract_one_day("20250101") == "20241231"


# ---------------------------------------------------------------------------
# filter_range
# ---------------------------------------------------------------------------

def test_filter_range_excludes_rows_outside_range():
    df = pd.DataFrame({
        "race_date": ["20230101", "20230601", "20231231"],
        "race_id": ["r1", "r2", "r3"],
        "finish_position": [1.0, 2.0, 3.0],
    })
    out = subject.filter_range(df, "20230101", "20230601")
    assert out["race_id"].tolist() == ["r1", "r2"]


def test_filter_range_excludes_nan_finish_position():
    df = pd.DataFrame({
        "race_date": ["20230101", "20230101"],
        "race_id": ["r1", "r2"],
        "finish_position": [1.0, float("nan")],
    })
    out = subject.filter_range(df, "20230101", "20231231")
    assert out["race_id"].tolist() == ["r1"]


# ---------------------------------------------------------------------------
# filter_year
# ---------------------------------------------------------------------------

def test_filter_year_keeps_only_target_year():
    df = pd.DataFrame({
        "race_date": ["20231201", "20240101", "20240601"],
        "race_id": ["r1", "r2", "r3"],
        "finish_position": [1.0, 1.0, 2.0],
    })
    out = subject.filter_year(df, 2024)
    assert out["race_id"].tolist() == ["r2", "r3"]


def test_filter_year_excludes_nan_finish_position():
    df = pd.DataFrame({
        "race_date": ["20240101", "20240101"],
        "race_id": ["r1", "r2"],
        "finish_position": [1.0, float("nan")],
    })
    out = subject.filter_year(df, 2024)
    assert out["race_id"].tolist() == ["r1"]


# ---------------------------------------------------------------------------
# resolve_feature_columns
# ---------------------------------------------------------------------------

def test_resolve_feature_columns_excludes_meta_and_label():
    df = _small_df()
    cols = subject.resolve_feature_columns(df, use_cat_features=True)
    assert "race_id" not in cols
    assert "finish_position" not in cols
    assert "feature_a" in cols
    assert "keibajo_code" in cols


def test_resolve_feature_columns_no_cat_excludes_categorical_names():
    df = _small_df()
    cols = subject.resolve_feature_columns(df, use_cat_features=False)
    assert "keibajo_code" not in cols
    assert "umaban" not in cols
    assert "feature_a" in cols


# ---------------------------------------------------------------------------
# resolve_cat_feature_indices
# ---------------------------------------------------------------------------

def test_resolve_cat_feature_indices_returns_indices_for_cat_columns():
    df = _small_df()
    feature_cols = ["feature_a", "keibajo_code", "umaban"]
    indices = subject.resolve_cat_feature_indices(df, feature_cols, use_cat_features=True)
    assert 1 in indices
    assert 2 in indices
    assert 0 not in indices


def test_resolve_cat_feature_indices_returns_empty_when_disabled():
    df = _small_df()
    feature_cols = ["feature_a", "keibajo_code"]
    indices = subject.resolve_cat_feature_indices(df, feature_cols, use_cat_features=False)
    assert indices == []


# ---------------------------------------------------------------------------
# make_to_relevance
# ---------------------------------------------------------------------------

def test_make_to_relevance_maps_ranks():
    fn = subject.make_to_relevance(3, 2, 1)
    assert fn(1) == 3
    assert fn(2) == 2
    assert fn(3) == 1
    assert fn(4) == 0


def test_make_to_relevance_handles_nan():
    fn = subject.make_to_relevance(3, 2, 1)
    assert fn(float("nan")) == 0
    assert fn(None) == 0


# ---------------------------------------------------------------------------
# race_group_ids
# ---------------------------------------------------------------------------

def test_race_group_ids_returns_integer_codes():
    df = pd.DataFrame({"race_id": ["r1", "r1", "r2", "r2"]})
    codes = subject.race_group_ids(df)
    assert codes.dtype in (np.int8, np.int16, np.int32, np.int64)
    assert codes[0] == codes[1]
    assert codes[2] == codes[3]
    assert codes[0] != codes[2]


# ---------------------------------------------------------------------------
# _cb_top1_hit
# ---------------------------------------------------------------------------

def test_cb_top1_hit_returns_one_when_predicted_rank1_finished_first():
    g = pd.DataFrame({
        "predicted_rank": [1, 2, 3],
        "finish_position": [1.0, 2.0, 3.0],
    })
    assert subject.cb_top1_hit(g) == 1


def test_cb_top1_hit_returns_zero_when_predicted_rank1_did_not_finish_first():
    g = pd.DataFrame({
        "predicted_rank": [1, 2, 3],
        "finish_position": [2.0, 1.0, 3.0],
    })
    assert subject.cb_top1_hit(g) == 0


# ---------------------------------------------------------------------------
# _cb_top3_box_hit
# ---------------------------------------------------------------------------

def test_cb_top3_box_hit_returns_one_when_all_three_finish_in_top3():
    g = pd.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 3.0, 2.0, 4.0],
    })
    assert subject.cb_top3_box_hit(g) == 1


def test_cb_top3_box_hit_returns_zero_when_one_of_top3_does_not_finish_in_top3():
    g = pd.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 2.0, 4.0, 3.0],
    })
    assert subject.cb_top3_box_hit(g) == 0


def test_cb_top3_box_hit_returns_zero_when_nan_finish_position_in_top3_predicted():
    # A scratched horse (NaN) among the top-3 predicted means we can't confirm
    # a full 3-horse box — returning 0 is correct strict behavior.
    g = pd.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, float("nan"), 3.0, 2.0],
    })
    assert subject.cb_top3_box_hit(g) == 0


# ---------------------------------------------------------------------------
# compute_fold_metrics
# ---------------------------------------------------------------------------

def test_compute_fold_metrics_returns_expected_keys():
    valid_df = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3],
        "predicted_score": [0.9, 0.5, 0.3],
        "finish_position": [1.0, 2.0, 3.0],
    })
    metrics = subject.compute_fold_metrics(valid_df)
    assert set(metrics.keys()) == {"race_count", "valid_rows", "top1_accuracy", "top3_box_accuracy"}
    assert metrics["race_count"] == 1
    assert metrics["valid_rows"] == 3
    assert metrics["top1_accuracy"] == 1.0
    assert metrics["top3_box_accuracy"] == 1.0


def test_compute_fold_metrics_with_empty_df_returns_zeros():
    valid_df = pd.DataFrame({
        "race_id": pd.Series([], dtype=str),
        "predicted_rank": pd.Series([], dtype=int),
        "predicted_score": pd.Series([], dtype=float),
        "finish_position": pd.Series([], dtype=float),
    })
    metrics = subject.compute_fold_metrics(valid_df)
    assert metrics["top1_accuracy"] == 0.0
    assert metrics["top3_box_accuracy"] == 0.0
    assert metrics["race_count"] == 0


# ---------------------------------------------------------------------------
# write_predictions_jsonl
# ---------------------------------------------------------------------------

def test_write_predictions_jsonl_writes_one_line_per_row(tmp_path: Path):
    df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "umaban": [1.0, 2.0],
        "predicted_score": [0.9, 0.5],
        "predicted_rank": [1, 2],
    })
    out = tmp_path / "out.jsonl"
    subject.write_predictions_jsonl(df, out)
    lines = out.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["race_id"] == "r1"
    assert first["predicted_rank"] == 1
    assert first["umaban"] == 1


def test_write_predictions_jsonl_writes_null_for_nan_umaban(tmp_path: Path):
    df = pd.DataFrame({
        "race_id": ["r1"],
        "ketto_toroku_bango": ["a"],
        "umaban": [float("nan")],
        "predicted_score": [0.9],
        "predicted_rank": [1],
    })
    out = tmp_path / "out.jsonl"
    subject.write_predictions_jsonl(df, out)
    first = json.loads(out.read_text(encoding="utf-8").strip())
    assert first["umaban"] is None


# ---------------------------------------------------------------------------
# Bug 1: OOT train_end date leakage — subtract_one_day when train_end_date is absent
# ---------------------------------------------------------------------------

def test_run_walk_forward_oot_train_end_is_day_before_validation_from(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """When --train-end-date is not set, train must end the day BEFORE
    --validation-from-date so the first validation day is not leaked
    into training data."""
    captured_ends: list[str] = []
    original_filter_range = subject.filter_range

    def fake_filter_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
        captured_ends.append(end)
        return original_filter_range(df, start, end)

    monkeypatch.setattr(subject, "filter_range", fake_filter_range)
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(_small_df().columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=_small_df()))
    monkeypatch.setattr(subject, "train_catboost_ranker", MagicMock(return_value={
        "valid_predictions": _small_df().assign(predicted_score=0.5, predicted_rank=1),
        "metrics": {"race_count": 1, "valid_rows": 2, "top1_accuracy": 0.5, "top3_box_accuracy": 0.5},
        "best_iteration": 10,
    }))

    args = _make_args(
        csv=tmp_path / "feat",
        validation_from_date="20230201",
        validation_to_date="20231231",
        train_end_date=None,
        train_start_date="20200101",
        output_report=tmp_path / "report.json",
        output_predictions_dir=tmp_path / "preds",
    )
    subject.run_walk_forward(args)

    # The first call to filter_range is for train data — its end must be one day
    # before validation_from_date ("20230201" - 1 day = "20230131").
    assert captured_ends[0] == "20230131", (
        f"Expected train end to be 20230131 (one day before validation start), got {captured_ends[0]!r}"
    )


def test_run_walk_forward_oot_uses_explicit_train_end_date_when_provided(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """When --train-end-date is explicitly set, it overrides the subtract_one_day logic."""
    captured_ends: list[str] = []
    original_filter_range = subject.filter_range

    def fake_filter_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
        captured_ends.append(end)
        return original_filter_range(df, start, end)

    monkeypatch.setattr(subject, "filter_range", fake_filter_range)
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(_small_df().columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=_small_df()))
    monkeypatch.setattr(subject, "train_catboost_ranker", MagicMock(return_value={
        "valid_predictions": _small_df().assign(predicted_score=0.5, predicted_rank=1),
        "metrics": {"race_count": 1, "valid_rows": 2, "top1_accuracy": 0.5, "top3_box_accuracy": 0.5},
        "best_iteration": 10,
    }))

    args = _make_args(
        csv=tmp_path / "feat",
        validation_from_date="20230201",
        validation_to_date="20231231",
        train_end_date="20221231",
        train_start_date="20200101",
        output_report=tmp_path / "report.json",
        output_predictions_dir=tmp_path / "preds",
    )
    subject.run_walk_forward(args)

    assert captured_ends[0] == "20221231"


# ---------------------------------------------------------------------------
# run_walk_forward year-based path
# ---------------------------------------------------------------------------

def _multi_year_df() -> pd.DataFrame:
    """DataFrame that spans 2022 and 2023 so year-based walk-forward has non-empty splits."""
    base = _small_df()
    older = base.copy()
    older["race_date"] = ["20220101", "20220101", "20220201", "20220201"]
    older["race_year"] = [2022, 2022, 2022, 2022]
    return pd.concat([older, base], ignore_index=True)


def test_run_walk_forward_year_based_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    df = _multi_year_df()
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(df.columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=df))
    monkeypatch.setattr(subject, "train_catboost_ranker", MagicMock(return_value={
        "valid_predictions": _small_df().assign(predicted_score=0.5, predicted_rank=1),
        "metrics": {"race_count": 1, "valid_rows": 2, "top1_accuracy": 0.5, "top3_box_accuracy": 0.5},
        "best_iteration": 10,
    }))
    args = _make_args(
        csv=tmp_path / "feat",
        validation_from_date=None,
        validation_to_date=None,
        train_start_date="20200101",
        validation_years="2023",
        output_report=tmp_path / "report.json",
        output_predictions_dir=tmp_path / "preds",
    )
    subject.run_walk_forward(args)
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["aggregate"]["fold_count"] == 1


def test_run_walk_forward_year_based_skips_empty_folds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(_small_df().columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=_small_df()))
    # Return empty df for train to trigger skip
    monkeypatch.setattr(subject, "filter_range", MagicMock(return_value=pd.DataFrame()))
    args = _make_args(
        csv=tmp_path / "feat",
        validation_from_date=None,
        validation_to_date=None,
        train_start_date="20200101",
        validation_years="2030",
        output_report=tmp_path / "report.json",
        output_predictions_dir=tmp_path / "preds",
    )
    subject.run_walk_forward(args)
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["aggregate"]["fold_count"] == 0


def test_run_walk_forward_oot_skips_when_train_or_valid_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(_small_df().columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=_small_df()))
    monkeypatch.setattr(subject, "filter_range", MagicMock(return_value=pd.DataFrame()))
    args = _make_args(
        csv=tmp_path / "feat",
        validation_from_date="20230201",
        validation_to_date="20231231",
        train_end_date=None,
        train_start_date="20200101",
        output_report=tmp_path / "report.json",
        output_predictions_dir=tmp_path / "preds",
    )
    subject.run_walk_forward(args)
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["aggregate"]["fold_count"] == 0


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------

def test_parse_args_walk_forward_defaults():
    args = subject.parse_args([
        "walk-forward",
        "--csv", "tmp/feat",
        "--output-report", "tmp/report.json",
        "--output-predictions-dir", "tmp/preds",
    ])
    assert args.cmd == "walk-forward"
    assert args.train_start_date == "20160101"
    assert args.validation_years == "2024,2025"
    assert args.train_end_date is None
    assert args.validation_from_date is None
    assert args.no_cat_features is False


def test_parse_args_oot_flags_are_accepted():
    args = subject.parse_args([
        "walk-forward",
        "--csv", "tmp/feat",
        "--output-report", "tmp/report.json",
        "--output-predictions-dir", "tmp/preds",
        "--train-end-date", "20221231",
        "--validation-from-date", "20230101",
        "--validation-to-date", "20231231",
        "--no-cat-features",
    ])
    assert args.train_end_date == "20221231"
    assert args.validation_from_date == "20230101"
    assert args.validation_to_date == "20231231"
    assert args.no_cat_features is True


# ---------------------------------------------------------------------------
# _prepare_feature_matrix
# ---------------------------------------------------------------------------

def test_prepare_feature_matrix_converts_numeric_to_float32():
    df = pd.DataFrame({
        "feature_a": [1, 2, 3],
        "keibajo_code": ["01", "02", None],
    })
    feature_cols = ["feature_a", "keibajo_code"]
    cat_indices = [1]
    out = subject.prepare_feature_matrix(df, feature_cols, cat_indices)
    assert out["feature_a"].dtype == np.float32


def test_prepare_feature_matrix_fills_cat_nan_with_missing_sentinel():
    df = pd.DataFrame({
        "feature_a": [1.0, 2.0],
        "keibajo_code": ["01", None],
    })
    feature_cols = ["feature_a", "keibajo_code"]
    cat_indices = [1]
    out = subject.prepare_feature_matrix(df, feature_cols, cat_indices)
    assert out["keibajo_code"].tolist() == ["01", "__missing__"]


def test_prepare_feature_matrix_no_cat_indices_only_float():
    df = pd.DataFrame({"feature_a": [1.0, 2.0], "feature_b": [3.0, 4.0]})
    out = subject.prepare_feature_matrix(df, ["feature_a", "feature_b"], [])
    assert out["feature_a"].dtype == np.float32
    assert out["feature_b"].dtype == np.float32


# ---------------------------------------------------------------------------
# load_parquet_dir
# ---------------------------------------------------------------------------

def test_load_parquet_dir_reads_and_concatenates_parquet_files(tmp_path: Path):
    year_dir = tmp_path / "race_year=2023"
    year_dir.mkdir(parents=True)
    df1 = pd.DataFrame({"x": [1, 2]})
    df2 = pd.DataFrame({"x": [3, 4]})
    pq.write_table(pa.Table.from_pandas(df1), year_dir / "part1.parquet")
    pq.write_table(pa.Table.from_pandas(df2), year_dir / "part2.parquet")
    result = subject.load_parquet_dir(tmp_path)
    assert sorted(result["x"].tolist()) == [1, 2, 3, 4]


def test_load_parquet_dir_raises_on_empty_directory(tmp_path: Path):
    with pytest.raises(ValueError, match="no parquet files found"):
        subject.load_parquet_dir(tmp_path)


def test_load_parquet_dir_year_max_filters_out_later_years(tmp_path: Path):
    older = tmp_path / "race_year=2022"
    older.mkdir(parents=True)
    newer = tmp_path / "race_year=2024"
    newer.mkdir(parents=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame({"x": [1, 2]})), older / "part.parquet")
    pq.write_table(pa.Table.from_pandas(pd.DataFrame({"x": [9, 9]})), newer / "part.parquet")
    result = subject.load_parquet_dir(tmp_path, year_max=2022)
    assert sorted(result["x"].tolist()) == [1, 2]


def test_load_parquet_dir_year_max_keeps_boundary_year(tmp_path: Path):
    boundary = tmp_path / "race_year=2024"
    boundary.mkdir(parents=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame({"x": [5, 6]})), boundary / "part.parquet")
    result = subject.load_parquet_dir(tmp_path, year_max=2024)
    assert sorted(result["x"].tolist()) == [5, 6]


def test_load_parquet_dir_year_max_raises_when_all_filtered(tmp_path: Path):
    newer = tmp_path / "race_year=2025"
    newer.mkdir(parents=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame({"x": [1]})), newer / "part.parquet")
    with pytest.raises(ValueError, match="for year_max=2020"):
        subject.load_parquet_dir(tmp_path, year_max=2020)


def test_extract_year_reads_partition_directory_name(tmp_path: Path):
    part = tmp_path / "race_year=2023" / "part-0.parquet"
    assert subject._extract_year(part) == 2023


def test_extract_year_returns_fallback_when_no_partition(tmp_path: Path):
    part = tmp_path / "no-partition" / "part-0.parquet"
    assert subject._extract_year(part) == 9999


def test_load_parquet_dir_columns_projects_only_requested_columns(tmp_path: Path):
    year_dir = tmp_path / "race_year=2023"
    year_dir.mkdir(parents=True)
    df = pd.DataFrame({"race_id": ["r1", "r2"], "feature_a": [0.1, 0.2], "bamei": ["H1", "H2"]})
    pq.write_table(pa.Table.from_pandas(df), year_dir / "part.parquet")
    result = subject.load_parquet_dir(tmp_path, columns=["race_id", "feature_a"])
    assert result.columns.tolist() == ["race_id", "feature_a"]


# ---------------------------------------------------------------------------
# read_parquet_schema_names
# ---------------------------------------------------------------------------

def test_read_parquet_schema_names_returns_first_file_columns(tmp_path: Path):
    year_dir = tmp_path / "race_year=2023"
    year_dir.mkdir(parents=True)
    df = pd.DataFrame({"race_id": ["r1"], "feature_a": [0.1], "finish_position": [1.0]})
    pq.write_table(pa.Table.from_pandas(df), year_dir / "part.parquet")
    names = subject.read_parquet_schema_names(tmp_path)
    assert names == ["race_id", "feature_a", "finish_position"]


def test_read_parquet_schema_names_raises_on_empty_directory(tmp_path: Path):
    with pytest.raises(ValueError, match="no parquet files found"):
        subject.read_parquet_schema_names(tmp_path)


# ---------------------------------------------------------------------------
# resolve_projection_columns
# ---------------------------------------------------------------------------

def test_resolve_projection_columns_keeps_features_runtime_and_categoricals():
    schema_names = [
        "source", "bamei", "finish_norm", "race_id", "finish_position",
        "keibajo_code", "umaban", "feat_a",
    ]
    projection = subject.resolve_projection_columns(schema_names)
    assert "source" not in projection
    assert "bamei" not in projection
    assert "finish_norm" not in projection
    assert "race_id" in projection
    assert "finish_position" in projection
    assert "keibajo_code" in projection
    assert "umaban" in projection
    assert "feat_a" in projection


def test_resolve_projection_columns_omits_runtime_column_absent_from_schema():
    schema_names = ["race_id", "race_date", "finish_position", "feat_a"]
    projection = subject.resolve_projection_columns(schema_names)
    assert "sample_weight" not in projection


# ---------------------------------------------------------------------------
# train_catboost_ranker (with CatBoost mocked out)
# ---------------------------------------------------------------------------

def test_train_catboost_ranker_returns_metrics_and_predictions(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pd.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    fake_model = MagicMock()
    fake_model.predict.return_value = np.array([0.9, 0.5])
    fake_model.get_best_iteration.return_value = 10
    fake_model.tree_count_ = 10

    mock_catboost_cls = MagicMock(return_value=fake_model)
    mock_pool_cls = MagicMock()

    monkeypatch.setattr(subject, "CatBoost", mock_catboost_cls)
    monkeypatch.setattr(subject, "Pool", mock_pool_cls)

    args = _make_args(
        relevance_rank1=3,
        relevance_rank2=2,
        relevance_rank3=1,
        iterations=10,
        learning_rate=0.05,
        depth=3,
        l2_leaf_reg=3.0,
        early_stopping_rounds=5,
        seed=42,
        no_cat_features=False,
    )
    result = subject.train_catboost_ranker(train_df, valid_df, ["feature_a"], args)
    assert result["best_iteration"] == 10
    assert "valid_predictions" in result
    metrics = cast(dict[str, float], result["metrics"])
    assert "top1_accuracy" in metrics
    assert "top3_box_accuracy" in metrics


def test_train_catboost_ranker_uses_no_cat_features_when_flag_set(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = train_df.copy()
    valid_df["race_id"] = ["r2", "r2"]

    fake_model = MagicMock()
    fake_model.predict.return_value = np.array([0.9, 0.5])
    fake_model.get_best_iteration.return_value = None
    fake_model.tree_count_ = 5

    monkeypatch.setattr(subject, "CatBoost", MagicMock(return_value=fake_model))
    monkeypatch.setattr(subject, "Pool", MagicMock())

    args = _make_args(no_cat_features=True)
    result = subject.train_catboost_ranker(train_df, valid_df, ["feature_a"], args)
    # get_best_iteration returns None so tree_count_ is used
    assert result["best_iteration"] == 5


def test_train_catboost_ranker_best_iteration_zero_uses_zero_not_tree_count(
    monkeypatch: pytest.MonkeyPatch,
):
    """get_best_iteration() returning 0 must not fall back to tree_count_ (falsy-zero bug)."""
    train_df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pd.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })
    fake_model = MagicMock()
    fake_model.predict.return_value = np.array([0.9, 0.5])
    fake_model.get_best_iteration.return_value = 0  # 0 is falsy — the bug returned tree_count_
    fake_model.tree_count_ = 99
    monkeypatch.setattr(subject, "CatBoost", MagicMock(return_value=fake_model))
    monkeypatch.setattr(subject, "Pool", MagicMock())
    args = _make_args()
    result = subject.train_catboost_ranker(train_df, valid_df, ["feature_a"], args)
    assert result["best_iteration"] == 0, "best_iteration=0 must not fall back to tree_count_"


def test_train_catboost_ranker_assigns_bottom_rank_to_nan_prediction(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pd.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })
    fake_model = MagicMock()
    fake_model.predict.return_value = np.array([float("nan"), 0.5])
    fake_model.get_best_iteration.return_value = 1
    fake_model.tree_count_ = 1
    monkeypatch.setattr(subject, "CatBoost", MagicMock(return_value=fake_model))
    monkeypatch.setattr(subject, "Pool", MagicMock())
    args = _make_args()
    result = subject.train_catboost_ranker(train_df, valid_df, ["feature_a"], args)
    preds = cast(pd.DataFrame, result["valid_predictions"])
    # NaN score (row 0) must get bottom rank; valid score 0.5 (row 1) gets rank 1
    assert preds["predicted_rank"].tolist() == [2, 1]


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def test_main_calls_run_walk_forward(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import sys
    fake_run = MagicMock()
    monkeypatch.setattr(subject, "run_walk_forward", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "finish_position_catboost",
            "walk-forward",
            "--csv", str(tmp_path / "feat"),
            "--output-report", str(tmp_path / "report.json"),
            "--output-predictions-dir", str(tmp_path / "preds"),
        ],
    )
    subject.main()
    fake_run.assert_called_once()
