from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import numpy as np
import polars as pl
import pytest

import finish_position_xgboost as subject


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _small_df() -> pl.DataFrame:
    return pl.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "race_date": ["20230101", "20230101", "20230201", "20230201"],
        "race_year": [2023, 2023, 2023, 2023],
        "source": ["jra", "jra", "jra", "jra"],
        "kaisai_nen": [2023, 2023, 2023, 2023],
        "kaisai_tsukihi": ["0101", "0101", "0201", "0201"],
        "keibajo_code": ["01", "01", "02", "02"],
        "race_bango": [1, 1, 1, 1],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "umaban": [1.0, 2.0, 1.0, 2.0],
        "bamei": ["H1", "H2", "H3", "H4"],
        "kishumei_ryakusho": ["J1", "J2", "J3", "J4"],
        "chokyoshimei_ryakusho": ["T1", "T2", "T3", "T4"],
        "category": ["jra", "jra", "jra", "jra"],
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
        num_rounds=10,
        learning_rate=0.05,
        max_depth=3,
        min_child_weight=5,
        reg_lambda=1.0,
        early_stopping_rounds=5,
        seed=42,
        objective="pairwise",
        nthread=6,
    )
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


def _predicted_small_df() -> pl.DataFrame:
    return _small_df().with_columns(
        pl.lit(0.5).alias("predicted_score"),
        pl.lit(1).alias("predicted_rank"),
    )


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
    df = pl.DataFrame({
        "race_date": ["20230101", "20230601", "20231231"],
        "race_id": ["r1", "r2", "r3"],
        "finish_position": [1.0, 2.0, 3.0],
    })
    out = subject.filter_range(df, "20230101", "20230601")
    assert out["race_id"].to_list() == ["r1", "r2"]


def test_filter_range_excludes_nan_finish_position():
    df = pl.DataFrame({
        "race_date": ["20230101", "20230101"],
        "race_id": ["r1", "r2"],
        "finish_position": [1.0, None],
    })
    out = subject.filter_range(df, "20230101", "20231231")
    assert out["race_id"].to_list() == ["r1"]


# ---------------------------------------------------------------------------
# filter_year
# ---------------------------------------------------------------------------

def test_filter_year_keeps_only_target_year():
    df = pl.DataFrame({
        "race_date": ["20231201", "20240101", "20240601"],
        "race_id": ["r1", "r2", "r3"],
        "finish_position": [1.0, 1.0, 2.0],
    })
    out = subject.filter_year(df, 2024)
    assert out["race_id"].to_list() == ["r2", "r3"]


def test_filter_year_excludes_nan_finish_position():
    df = pl.DataFrame({
        "race_date": ["20240101", "20240101"],
        "race_id": ["r1", "r2"],
        "finish_position": [1.0, None],
    })
    out = subject.filter_year(df, 2024)
    assert out["race_id"].to_list() == ["r1"]


# ---------------------------------------------------------------------------
# resolve_feature_columns
# ---------------------------------------------------------------------------

def test_resolve_feature_columns_excludes_meta_and_label():
    df = _small_df()
    cols = subject.resolve_feature_columns(df)
    assert "race_id" not in cols
    assert "finish_position" not in cols
    assert "feature_a" in cols


def test_resolve_feature_columns_excludes_keibajo_code_as_meta():
    df = _small_df()
    cols = subject.resolve_feature_columns(df)
    assert "keibajo_code" not in cols


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
# relevance_labels
# ---------------------------------------------------------------------------

def test_relevance_labels_maps_ranks_and_zeroes_others():
    df = pl.DataFrame({"finish_position": [1.0, 2.0, 3.0, 4.0, None]})
    labels = subject.relevance_labels(df, 3, 2, 1)
    assert labels.tolist() == [3, 2, 1, 0, 0]
    assert labels.dtype == np.int32


# ---------------------------------------------------------------------------
# build_group_sizes
# ---------------------------------------------------------------------------

def test_build_group_sizes_returns_sizes_per_race():
    df = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r2", "r2"],
        "umaban": [1.0, 2.0, 3.0, 1.0, 2.0],
    })
    sizes = subject.build_group_sizes(df)
    assert 3 in sizes
    assert 2 in sizes


def test_build_group_sizes_single_race():
    df = pl.DataFrame({"race_id": ["r1", "r1"], "umaban": [1.0, 2.0]})
    assert subject.build_group_sizes(df) == [2]


def test_sort_train_valid_for_grouping_sorts_when_not_presorted():
    train_df = pl.DataFrame({"race_id": ["r2", "r1", "r1"], "umaban": [1.0, 2.0, 1.0]})
    valid_df = pl.DataFrame({"race_id": ["r4", "r3"], "umaban": [2.0, 1.0]})
    args = argparse.Namespace(presorted=False)
    out_train, out_valid = subject.sort_train_valid_for_grouping(train_df, valid_df, args)
    assert out_train["race_id"].to_list() == ["r1", "r1", "r2"]
    assert out_train["umaban"].to_list() == [1.0, 2.0, 1.0]
    assert out_valid["race_id"].to_list() == ["r3", "r4"]


def test_sort_train_valid_for_grouping_skips_sort_when_presorted():
    train_df = pl.DataFrame({"race_id": ["r2", "r1"], "umaban": [1.0, 1.0]})
    valid_df = pl.DataFrame({"race_id": ["r4", "r3"], "umaban": [1.0, 1.0]})
    args = argparse.Namespace(presorted=True)
    out_train, out_valid = subject.sort_train_valid_for_grouping(train_df, valid_df, args)
    assert out_train["race_id"].to_list() == ["r2", "r1"]
    assert out_valid["race_id"].to_list() == ["r4", "r3"]


def test_sort_train_valid_for_grouping_defaults_to_sorting_when_attr_absent():
    train_df = pl.DataFrame({"race_id": ["r2", "r1"], "umaban": [1.0, 1.0]})
    valid_df = pl.DataFrame({"race_id": ["r4", "r3"], "umaban": [1.0, 1.0]})
    args = argparse.Namespace()
    out_train, _ = subject.sort_train_valid_for_grouping(train_df, valid_df, args)
    assert out_train["race_id"].to_list() == ["r1", "r2"]


# ---------------------------------------------------------------------------
# top1_hit
# ---------------------------------------------------------------------------

def test_top1_hit_returns_one_when_predicted_rank1_finished_first():
    g = pl.DataFrame({
        "predicted_rank": [1, 2, 3],
        "finish_position": [1.0, 2.0, 3.0],
    })
    assert subject.top1_hit(g) == 1


def test_top1_hit_returns_zero_when_predicted_rank1_did_not_finish_first():
    g = pl.DataFrame({
        "predicted_rank": [1, 2, 3],
        "finish_position": [2.0, 1.0, 3.0],
    })
    assert subject.top1_hit(g) == 0


# ---------------------------------------------------------------------------
# top3_box_hit
# ---------------------------------------------------------------------------

def test_top3_box_hit_returns_one_when_all_three_finish_in_top3():
    g = pl.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 3.0, 2.0, 4.0],
    })
    assert subject.top3_box_hit(g) == 1


def test_top3_box_hit_returns_zero_when_one_of_top3_does_not_finish_in_top3():
    g = pl.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 2.0, 4.0, 3.0],
    })
    assert subject.top3_box_hit(g) == 0


# ---------------------------------------------------------------------------
# top3_exact_hit
# ---------------------------------------------------------------------------

def test_top3_exact_hit_returns_one_when_all_ranks_match():
    g = pl.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 2.0, 3.0, 4.0],
    })
    assert subject.top3_exact_hit(g) == 1


def test_top3_exact_hit_returns_zero_when_only_two_match():
    g = pl.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 2.0, 4.0, 3.0],
    })
    assert subject.top3_exact_hit(g) == 0


def test_top3_exact_hit_returns_zero_when_first_place_wrong():
    g = pl.DataFrame({
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [2.0, 1.0, 3.0, 4.0],
    })
    assert subject.top3_exact_hit(g) == 0


# ---------------------------------------------------------------------------
# compute_fold_metrics
# ---------------------------------------------------------------------------

def test_compute_fold_metrics_returns_expected_keys():
    valid_df = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3],
        "predicted_score": [0.9, 0.5, 0.3],
        "finish_position": [1.0, 2.0, 3.0],
    })
    metrics = subject.compute_fold_metrics(valid_df)
    assert set(metrics.keys()) == {
        "race_count", "valid_rows", "top1_accuracy", "top3_box_accuracy", "top3_exact_accuracy",
    }
    assert metrics["race_count"] == 1
    assert metrics["valid_rows"] == 3
    assert metrics["top1_accuracy"] == 1.0
    assert metrics["top3_box_accuracy"] == 1.0
    assert metrics["top3_exact_accuracy"] == 1.0


def test_compute_fold_metrics_with_empty_df_returns_zeros():
    valid_df = pl.DataFrame(
        schema={
            "race_id": pl.String,
            "predicted_rank": pl.Int64,
            "predicted_score": pl.Float64,
            "finish_position": pl.Float64,
        },
    )
    metrics = subject.compute_fold_metrics(valid_df)
    assert metrics["top1_accuracy"] == 0.0
    assert metrics["top3_box_accuracy"] == 0.0
    assert metrics["top3_exact_accuracy"] == 0.0
    assert metrics["race_count"] == 0


# ---------------------------------------------------------------------------
# write_predictions_jsonl
# ---------------------------------------------------------------------------

def test_write_predictions_jsonl_writes_one_line_per_row(tmp_path: Path):
    df = pl.DataFrame({
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
    df = pl.DataFrame({
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
# load_parquet_dir
# ---------------------------------------------------------------------------

def test_load_parquet_dir_reads_and_concatenates_parquet_files(tmp_path: Path):
    year_dir = tmp_path / "race_year=2023"
    year_dir.mkdir(parents=True)
    pl.DataFrame({"x": [1, 2]}).write_parquet(year_dir / "part1.parquet")
    pl.DataFrame({"x": [3, 4]}).write_parquet(year_dir / "part2.parquet")
    result = subject.load_parquet_dir(tmp_path)
    assert sorted(result["x"].to_list()) == [1, 2, 3, 4]


def test_load_parquet_dir_with_columns_returns_only_requested_columns(tmp_path: Path):
    year_dir = tmp_path / "race_year=2023"
    year_dir.mkdir(parents=True)
    pl.DataFrame({
        "race_id": ["r1", "r2"],
        "feat_a": [0.1, 0.2],
        "bamei": ["H1", "H2"],
        "finish_position": [1.0, 2.0],
    }).write_parquet(year_dir / "part.parquet")
    result = subject.load_parquet_dir(tmp_path, columns=["race_id", "feat_a"])
    assert result.columns == ["race_id", "feat_a"]


def test_load_parquet_dir_raises_on_empty_directory(tmp_path: Path):
    with pytest.raises(ValueError, match="no parquet files found"):
        subject.load_parquet_dir(tmp_path)


def test_load_parquet_dir_year_max_filters_out_later_years(tmp_path: Path):
    older = tmp_path / "race_year=2022"
    older.mkdir(parents=True)
    newer = tmp_path / "race_year=2024"
    newer.mkdir(parents=True)
    pl.DataFrame({"x": [1, 2]}).write_parquet(older / "part.parquet")
    pl.DataFrame({"x": [9, 9]}).write_parquet(newer / "part.parquet")
    result = subject.load_parquet_dir(tmp_path, year_max=2022)
    assert sorted(result["x"].to_list()) == [1, 2]


def test_load_parquet_dir_year_max_keeps_boundary_year(tmp_path: Path):
    boundary = tmp_path / "race_year=2024"
    boundary.mkdir(parents=True)
    pl.DataFrame({"x": [5, 6]}).write_parquet(boundary / "part.parquet")
    result = subject.load_parquet_dir(tmp_path, year_max=2024)
    assert sorted(result["x"].to_list()) == [5, 6]


def test_load_parquet_dir_year_max_raises_when_all_filtered(tmp_path: Path):
    newer = tmp_path / "race_year=2025"
    newer.mkdir(parents=True)
    pl.DataFrame({"x": [1]}).write_parquet(newer / "part.parquet")
    with pytest.raises(ValueError, match="for year_max=2020"):
        subject.load_parquet_dir(tmp_path, year_max=2020)


def test_extract_year_reads_partition_directory_name(tmp_path: Path):
    part = tmp_path / "race_year=2023" / "part-0.parquet"
    assert subject._extract_year(part) == 2023


def test_extract_year_returns_fallback_when_no_partition(tmp_path: Path):
    part = tmp_path / "no-partition" / "part-0.parquet"
    assert subject._extract_year(part) == 9999


# ---------------------------------------------------------------------------
# read_parquet_schema_names
# ---------------------------------------------------------------------------

def test_read_parquet_schema_names_returns_first_parquet_columns(tmp_path: Path):
    year_dir = tmp_path / "race_year=2023"
    year_dir.mkdir(parents=True)
    pl.DataFrame({
        "race_id": ["r1"],
        "feat_a": [0.1],
        "finish_position": [1.0],
    }).write_parquet(year_dir / "part.parquet")
    names = subject.read_parquet_schema_names(tmp_path)
    assert names == ["race_id", "feat_a", "finish_position"]


def test_read_parquet_schema_names_raises_on_empty_directory(tmp_path: Path):
    with pytest.raises(ValueError, match="no parquet files found"):
        subject.read_parquet_schema_names(tmp_path)


# ---------------------------------------------------------------------------
# resolve_projection_columns
# ---------------------------------------------------------------------------

def test_resolve_projection_columns_keeps_features_and_present_runtime_columns():
    schema_names = ["source", "bamei", "finish_norm", "race_id", "finish_position", "feat_a", "feat_b"]
    cols = subject.resolve_projection_columns(schema_names)
    assert "source" not in cols
    assert "bamei" not in cols
    assert "finish_norm" not in cols
    assert "race_id" in cols
    assert "finish_position" in cols
    assert "feat_a" in cols
    assert "feat_b" in cols


def test_resolve_projection_columns_omits_runtime_column_absent_from_schema():
    schema_names = ["race_id", "finish_position", "feat_a"]
    cols = subject.resolve_projection_columns(schema_names)
    assert "sample_weight" not in cols


# ---------------------------------------------------------------------------
# OOT fence: train_end must be day BEFORE validation_from_date
# ---------------------------------------------------------------------------

def test_run_walk_forward_oot_train_end_is_day_before_validation_from(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """When --train-end-date is not set, train must end the day BEFORE
    --validation-from-date to prevent data leakage."""
    captured_ends: list[str] = []
    original_filter_range = subject.filter_range

    def fake_filter_range(df: pl.DataFrame, start: str, end: str) -> pl.DataFrame:
        captured_ends.append(end)
        return original_filter_range(df, start, end)

    monkeypatch.setattr(subject, "filter_range", fake_filter_range)
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(_small_df().columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=_small_df()))
    fake_booster = MagicMock()
    fake_booster.best_iteration = 5
    monkeypatch.setattr(subject, "train_xgboost_ranker", MagicMock(return_value=(
        fake_booster,
        {
            "valid_predictions": _predicted_small_df(),
            "metrics": {
                "race_count": 1, "valid_rows": 2,
                "top1_accuracy": 0.5, "top3_box_accuracy": 0.5, "top3_exact_accuracy": 0.0,
            },
            "best_iteration": 5,
        },
    )))

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

    assert captured_ends[0] == "20230131", (
        f"Expected train end to be 20230131 (one day before validation start), got {captured_ends[0]!r}"
    )


def test_run_walk_forward_oot_uses_explicit_train_end_date_when_provided(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    captured_ends: list[str] = []
    original_filter_range = subject.filter_range

    def fake_filter_range(df: pl.DataFrame, start: str, end: str) -> pl.DataFrame:
        captured_ends.append(end)
        return original_filter_range(df, start, end)

    monkeypatch.setattr(subject, "filter_range", fake_filter_range)
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(_small_df().columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=_small_df()))
    fake_booster = MagicMock()
    fake_booster.best_iteration = 5
    monkeypatch.setattr(subject, "train_xgboost_ranker", MagicMock(return_value=(
        fake_booster,
        {
            "valid_predictions": _predicted_small_df(),
            "metrics": {
                "race_count": 1, "valid_rows": 2,
                "top1_accuracy": 0.5, "top3_box_accuracy": 0.5, "top3_exact_accuracy": 0.0,
            },
            "best_iteration": 5,
        },
    )))

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
# run_walk_forward — year-based path
# ---------------------------------------------------------------------------

def _multi_year_df() -> pl.DataFrame:
    base = _small_df()
    older = base.with_columns(
        pl.Series("race_date", ["20220101", "20220101", "20220201", "20220201"]),
        pl.Series("race_year", [2022, 2022, 2022, 2022]),
    )
    return pl.concat([older, base])


def test_run_walk_forward_year_based_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    df = _multi_year_df()
    monkeypatch.setattr(
        subject, "read_parquet_schema_names",
        MagicMock(return_value=list(df.columns)),
    )
    monkeypatch.setattr(subject, "load_parquet_dir", MagicMock(return_value=df))
    fake_booster = MagicMock()
    fake_booster.best_iteration = 5
    monkeypatch.setattr(subject, "train_xgboost_ranker", MagicMock(return_value=(
        fake_booster,
        {
            "valid_predictions": _predicted_small_df(),
            "metrics": {
                "race_count": 1, "valid_rows": 2,
                "top1_accuracy": 0.5, "top3_box_accuracy": 0.5, "top3_exact_accuracy": 0.0,
            },
            "best_iteration": 5,
        },
    )))
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
    monkeypatch.setattr(subject, "filter_range", MagicMock(return_value=pl.DataFrame()))
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
    monkeypatch.setattr(subject, "filter_range", MagicMock(return_value=pl.DataFrame()))
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
    assert args.num_rounds == 500
    assert args.seed == 20260519
    assert args.nthread == 6


def test_parse_args_walk_forward_accepts_nthread_override():
    args = subject.parse_args([
        "walk-forward",
        "--csv", "tmp/feat",
        "--output-report", "tmp/report.json",
        "--output-predictions-dir", "tmp/preds",
        "--nthread", "4",
    ])
    assert args.nthread == 4


def test_parse_args_oot_flags_are_accepted():
    args = subject.parse_args([
        "walk-forward",
        "--csv", "tmp/feat",
        "--output-report", "tmp/report.json",
        "--output-predictions-dir", "tmp/preds",
        "--train-end-date", "20221231",
        "--validation-from-date", "20230101",
        "--validation-to-date", "20231231",
    ])
    assert args.train_end_date == "20221231"
    assert args.validation_from_date == "20230101"
    assert args.validation_to_date == "20231231"


# ---------------------------------------------------------------------------
# train_xgboost_ranker — with xgb mocked
# ---------------------------------------------------------------------------

def _make_mock_booster(best_iteration: int = 5) -> MagicMock:
    booster = MagicMock()
    booster.best_iteration = best_iteration
    booster.predict.return_value = np.array([0.9, 0.5, 0.3, 0.8])
    return booster


def test_train_xgboost_ranker_returns_metrics_and_booster(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    fake_dmatrix = MagicMock()
    fake_booster = _make_mock_booster(best_iteration=7)
    fake_booster.predict.return_value = np.array([0.9, 0.5])

    monkeypatch.setattr(subject.xgb, "DMatrix", MagicMock(return_value=fake_dmatrix))
    monkeypatch.setattr(subject.xgb, "train", MagicMock(return_value=fake_booster))

    args = _make_args()
    booster, result = subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)
    assert booster is fake_booster
    assert result["best_iteration"] == 7
    assert "valid_predictions" in result
    metrics = cast(dict[str, float], result["metrics"])
    assert "top1_accuracy" in metrics
    assert "top3_box_accuracy" in metrics
    assert "top3_exact_accuracy" in metrics


def test_train_xgboost_ranker_presorted_matches_unsorted_labels_and_groups(
    monkeypatch: pytest.MonkeyPatch,
):
    """presorted=True on an already-sorted frame must produce the same DMatrix
    labels and race group sizes as presorted=False on the shuffled frame, proving
    the fast-path only skips redundant work and never changes training inputs."""
    sorted_df = pl.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "umaban": [1.0, 2.0, 1.0, 2.0],
        "finish_position": [1.0, 2.0, 2.0, 1.0],
        "feature_a": [0.1, 0.2, 0.3, 0.4],
    })
    shuffled_df = sorted_df[[2, 0, 3, 1]]
    valid_df = pl.DataFrame({
        "race_id": ["r3", "r3"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.5, 0.6],
    })

    labels: list[object] = []
    groups: list[object] = []

    class FakeDMatrix:
        def __init__(self, data: object, label: object = None, weight: object = None) -> None:
            labels.append(label)

        def set_group(self, group: object) -> None:
            groups.append(group)

    fake_booster = _make_mock_booster()
    fake_booster.predict.return_value = np.array([0.9, 0.5])
    monkeypatch.setattr(subject.xgb, "DMatrix", FakeDMatrix)
    monkeypatch.setattr(subject.xgb, "train", MagicMock(return_value=fake_booster))

    subject.train_xgboost_ranker(
        shuffled_df, valid_df, ["feature_a"], _make_args(presorted=False),
    )
    subject.train_xgboost_ranker(
        sorted_df, valid_df, ["feature_a"], _make_args(presorted=True),
    )
    np.testing.assert_array_equal(cast(np.ndarray, labels[0]), cast(np.ndarray, labels[2]))
    assert groups[0] == groups[2]


def test_train_xgboost_ranker_passes_sample_weight_to_dmatrix(
    monkeypatch: pytest.MonkeyPatch,
):
    """When sample_weight column is present, it must be forwarded to xgb.DMatrix."""
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
        "sample_weight": [1.5, 2.0],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    dmatrix_calls: list[dict[str, object]] = []

    class FakeDMatrix:
        def __init__(self, data: object, label: object = None, weight: object = None) -> None:
            dmatrix_calls.append({"label": label, "weight": weight})

        def set_group(self, groups: object) -> None:
            pass

    fake_booster = _make_mock_booster()
    fake_booster.predict.return_value = np.array([0.9, 0.5])
    monkeypatch.setattr(subject.xgb, "DMatrix", FakeDMatrix)
    monkeypatch.setattr(subject.xgb, "train", MagicMock(return_value=fake_booster))

    args = _make_args()
    subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)

    train_call = dmatrix_calls[0]
    assert train_call["weight"] is not None, "sample_weight was not passed to xgb.DMatrix"
    np.testing.assert_array_almost_equal(
        cast(np.ndarray, train_call["weight"]), np.array([1.5, 2.0]),
    )


def test_train_xgboost_ranker_passes_none_weight_when_no_sample_weight_column(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    dmatrix_calls: list[dict[str, object]] = []

    class FakeDMatrix:
        def __init__(self, data: object, label: object = None, weight: object = None) -> None:
            dmatrix_calls.append({"label": label, "weight": weight})

        def set_group(self, groups: object) -> None:
            pass

    fake_booster = _make_mock_booster()
    fake_booster.predict.return_value = np.array([0.9, 0.5])
    monkeypatch.setattr(subject.xgb, "DMatrix", FakeDMatrix)
    monkeypatch.setattr(subject.xgb, "train", MagicMock(return_value=fake_booster))

    args = _make_args()
    subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)

    train_call = dmatrix_calls[0]
    assert train_call["weight"] is None


def test_train_xgboost_ranker_uses_ndcg_objective_when_specified(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    captured_params: list[dict[str, object]] = []

    class FakeDMatrix:
        def __init__(self, data: object, label: object = None, weight: object = None) -> None:
            pass

        def set_group(self, groups: object) -> None:
            pass

    fake_booster = _make_mock_booster()
    fake_booster.predict.return_value = np.array([0.9, 0.5])

    def fake_train(
        params: dict[str, object], dtrain: object, **kwargs: object
    ) -> MagicMock:
        captured_params.append(params)
        return fake_booster

    monkeypatch.setattr(subject.xgb, "DMatrix", FakeDMatrix)
    monkeypatch.setattr(subject.xgb, "train", fake_train)

    args = _make_args(objective="ndcg")
    subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)

    assert captured_params[0]["objective"] == "rank:ndcg"


def test_train_xgboost_ranker_sets_nthread_to_six(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    captured_params: list[dict[str, object]] = []

    class FakeDMatrix:
        def __init__(self, data: object, label: object = None, weight: object = None) -> None:
            pass

        def set_group(self, groups: object) -> None:
            pass

    fake_booster = _make_mock_booster()
    fake_booster.predict.return_value = np.array([0.9, 0.5])

    def fake_train(
        params: dict[str, object], dtrain: object, **kwargs: object
    ) -> MagicMock:
        captured_params.append(params)
        return fake_booster

    monkeypatch.setattr(subject.xgb, "DMatrix", FakeDMatrix)
    monkeypatch.setattr(subject.xgb, "train", fake_train)

    args = _make_args(nthread=6)
    subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)

    assert captured_params[0]["nthread"] == 6


def test_train_xgboost_ranker_clamps_nthread_above_six(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    captured_params: list[dict[str, object]] = []

    class FakeDMatrix:
        def __init__(self, data: object, label: object = None, weight: object = None) -> None:
            pass

        def set_group(self, groups: object) -> None:
            pass

    fake_booster = _make_mock_booster()
    fake_booster.predict.return_value = np.array([0.9, 0.5])

    def fake_train(
        params: dict[str, object], dtrain: object, **kwargs: object
    ) -> MagicMock:
        captured_params.append(params)
        return fake_booster

    monkeypatch.setattr(subject.xgb, "DMatrix", FakeDMatrix)
    monkeypatch.setattr(subject.xgb, "train", fake_train)

    args = _make_args(nthread=16)
    subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)

    assert captured_params[0]["nthread"] == 6


def test_train_xgboost_ranker_defaults_nthread_when_arg_absent(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })

    captured_params: list[dict[str, object]] = []

    class FakeDMatrix:
        def __init__(self, data: object, label: object = None, weight: object = None) -> None:
            pass

        def set_group(self, groups: object) -> None:
            pass

    fake_booster = _make_mock_booster()
    fake_booster.predict.return_value = np.array([0.9, 0.5])

    def fake_train(
        params: dict[str, object], dtrain: object, **kwargs: object
    ) -> MagicMock:
        captured_params.append(params)
        return fake_booster

    monkeypatch.setattr(subject.xgb, "DMatrix", FakeDMatrix)
    monkeypatch.setattr(subject.xgb, "train", fake_train)

    args = argparse.Namespace(
        relevance_rank1=3,
        relevance_rank2=2,
        relevance_rank3=1,
        num_rounds=10,
        learning_rate=0.05,
        max_depth=3,
        min_child_weight=5,
        reg_lambda=1.0,
        early_stopping_rounds=5,
        seed=42,
        objective="pairwise",
    )
    subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)

    assert captured_params[0]["nthread"] == 6


def test_train_xgboost_ranker_assigns_bottom_rank_to_nan_prediction(
    monkeypatch: pytest.MonkeyPatch,
):
    train_df = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.1, 0.2],
    })
    valid_df = pl.DataFrame({
        "race_id": ["r2", "r2"],
        "umaban": [1.0, 2.0],
        "finish_position": [1.0, 2.0],
        "feature_a": [0.3, 0.4],
    })
    fake_booster = _make_mock_booster(best_iteration=1)
    fake_booster.predict.return_value = np.array([float("nan"), 0.8])
    monkeypatch.setattr(subject.xgb, "DMatrix", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(subject.xgb, "train", MagicMock(return_value=fake_booster))
    args = _make_args()
    _, result = subject.train_xgboost_ranker(train_df, valid_df, ["feature_a"], args)
    preds = cast(pl.DataFrame, result["valid_predictions"])
    # NaN score (row 0) must get bottom rank; valid score 0.8 (row 1) gets rank 1
    assert preds["predicted_rank"].to_list() == [2, 1]


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
            "finish_position_xgboost",
            "walk-forward",
            "--csv", str(tmp_path / "feat"),
            "--output-report", str(tmp_path / "report.json"),
            "--output-predictions-dir", str(tmp_path / "preds"),
        ],
    )
    subject.main()
    fake_run.assert_called_once()
