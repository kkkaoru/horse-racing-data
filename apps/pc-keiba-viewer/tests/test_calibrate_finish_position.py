from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import calibrate_finish_position as subject


FIXED_NOW: datetime = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_NOW


def _g1_predictions_frame_600() -> pd.DataFrame:
    rng = np.random.default_rng(seed=20260604)
    n_races = 60
    rows: list[dict[str, object]] = []
    for race_idx in range(n_races):
        # 10 horses per race -> 600 rows total
        scores = rng.uniform(0.0, 1.0, size=10)
        order = np.argsort(-scores)
        # winner = the one with highest "true" affinity but with noise
        true_winner_in_order = int(order[0])
        for horse_idx in range(10):
            rank = int(np.where(order == horse_idx)[0][0]) + 1
            actual = 1 if horse_idx == true_winner_in_order else (rank if rank > 1 else 2)
            rows.append({
                "race_id": f"jra:2024:0512:05:{race_idx:02d}",
                "ketto_toroku_bango": f"201910{race_idx:04d}{horse_idx:02d}",
                "predicted_score": float(scores[horse_idx]),
                "predicted_rank": rank,
                "predicted_top1_prob": float(scores[horse_idx]),
                "predicted_top3_prob": float(min(scores[horse_idx] * 3.0, 1.0)),
                "actual_finish_position": float(actual),
                "kyoso_joken_code": "999",
                "grade_code": "A",
            })
    return pd.DataFrame(rows)


def _small_bucket_frame_300() -> pd.DataFrame:
    rng = np.random.default_rng(seed=20260605)
    n_races = 30
    rows: list[dict[str, object]] = []
    for race_idx in range(n_races):
        scores = rng.uniform(0.0, 1.0, size=10)
        order = np.argsort(-scores)
        true_winner = int(order[0])
        for horse_idx in range(10):
            rank = int(np.where(order == horse_idx)[0][0]) + 1
            actual = 1 if horse_idx == true_winner else rank
            rows.append({
                "race_id": f"jra:2024:0413:05:{race_idx:02d}",
                "ketto_toroku_bango": f"201910{race_idx:04d}{horse_idx:02d}",
                "predicted_score": float(scores[horse_idx]),
                "predicted_rank": rank,
                "predicted_top1_prob": float(scores[horse_idx]),
                "predicted_top3_prob": float(min(scores[horse_idx] * 3.0, 1.0)),
                "actual_finish_position": float(actual),
                "kyoso_joken_code": "005",
                "grade_code": "B",
            })
    return pd.DataFrame(rows)


def _apply_input_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": [
            "jra:2024:0512:05:01",
            "jra:2024:0512:05:01",
            "jra:2024:0512:05:01",
            "jra:2024:0512:05:02",
            "jra:2024:0512:05:02",
            "jra:2024:0512:05:02",
        ],
        "ketto_toroku_bango": [
            "2019100001",
            "2019100002",
            "2019100003",
            "2019100004",
            "2019100005",
            "2019100006",
        ],
        "umaban": [1, 2, 3, 1, 2, 3],
        "predicted_score": [0.9, 0.5, 0.2, 0.85, 0.45, 0.1],
        "predicted_rank": [1, 2, 3, 1, 2, 3],
        "predicted_top1_prob": [0.9, 0.5, 0.2, 0.85, 0.45, 0.1],
        "predicted_top3_prob": [1.0, 0.8, 0.4, 1.0, 0.7, 0.3],
        "kyoso_joken_code": ["999", "999", "999", "999", "999", "999"],
        "category": ["jra", "jra", "jra", "jra", "jra", "jra"],
        "race_year": [2024, 2024, 2024, 2024, 2024, 2024],
    })


def _g1_iso_top1_curve() -> subject.CalibrationCurve:
    return {
        "schema_version": 1,
        "cat": "jra",
        "bucket_key": "999",
        "target": "top1",
        "n_samples": 600,
        "iso_x": [0.0, 0.5, 1.0],
        "iso_y": [0.05, 0.40, 0.90],
        "fit_at": "2026-06-04T12:00:00Z",
        "brier_score_before": 0.30,
        "brier_score_after": 0.20,
    }


def _g1_iso_top3_curve() -> subject.CalibrationCurve:
    return {
        "schema_version": 1,
        "cat": "jra",
        "bucket_key": "999",
        "target": "top3",
        "n_samples": 600,
        "iso_x": [0.0, 0.5, 1.0],
        "iso_y": [0.10, 0.55, 0.95],
        "fit_at": "2026-06-04T12:00:00Z",
        "brier_score_before": 0.30,
        "brier_score_after": 0.18,
    }


def _cat_global_iso_top1_curve() -> subject.CalibrationCurve:
    return {
        "schema_version": 1,
        "cat": "jra",
        "bucket_key": "_cat_global",
        "target": "top1",
        "n_samples": 1200,
        "iso_x": [0.0, 1.0],
        "iso_y": [0.05, 0.85],
        "fit_at": "2026-06-04T12:00:00Z",
        "brier_score_before": 0.30,
        "brier_score_after": 0.22,
    }


def _cat_global_iso_top3_curve() -> subject.CalibrationCurve:
    return {
        "schema_version": 1,
        "cat": "jra",
        "bucket_key": "_cat_global",
        "target": "top3",
        "n_samples": 1200,
        "iso_x": [0.0, 1.0],
        "iso_y": [0.10, 0.95],
        "fit_at": "2026-06-04T12:00:00Z",
        "brier_score_before": 0.30,
        "brier_score_after": 0.20,
    }


def test_parse_args_fit_full_set():
    args = subject.parse_args([
        "--mode",
        "fit",
        "--cat",
        "jra",
        "--predictions-root",
        "tmp/preds",
        "--output-dir",
        "out",
        "--min-bucket-samples",
        "500",
        "--bucket-dim",
        "kyoso_joken",
    ])
    assert args.mode == "fit"
    assert args.cat == "jra"
    assert args.predictions_root == Path("tmp/preds")
    assert args.output_dir == Path("out")
    assert args.min_bucket_samples == 500
    assert args.bucket_dim == "kyoso_joken"


def test_parse_args_apply_full_set():
    args = subject.parse_args([
        "--mode",
        "apply",
        "--cat",
        "nar",
        "--input-predictions-root",
        "tmp/in",
        "--calibration-dir",
        "tmp/cal",
        "--output-predictions-root",
        "tmp/out",
    ])
    assert args.mode == "apply"
    assert args.cat == "nar"
    assert args.input_predictions_root == Path("tmp/in")
    assert args.calibration_dir == Path("tmp/cal")
    assert args.output_predictions_root == Path("tmp/out")


def test_parse_args_rejects_unknown_mode():
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--mode",
            "bogus",
            "--cat",
            "jra",
        ])


def test_parse_args_rejects_unknown_cat():
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--mode",
            "fit",
            "--cat",
            "bogus",
        ])


def test_parse_args_rejects_unknown_bucket_dim():
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--mode",
            "fit",
            "--cat",
            "jra",
            "--bucket-dim",
            "bogus",
        ])


def test_normalize_fit_arguments_returns_typeddict():
    raw = subject.parse_args([
        "--mode",
        "fit",
        "--cat",
        "jra",
        "--predictions-root",
        "tmp/p",
        "--output-dir",
        "tmp/o",
    ])
    normalized = subject.normalize_fit_arguments(raw)
    assert normalized["mode"] == "fit"
    assert normalized["cat"] == "jra"
    assert normalized["predictions_root"] == Path("tmp/p")
    assert normalized["output_dir"] == Path("tmp/o")
    assert normalized["min_bucket_samples"] == 500
    assert normalized["bucket_dim"] == "kyoso_joken"


def test_normalize_fit_arguments_raises_without_predictions_root():
    raw = subject.parse_args([
        "--mode",
        "fit",
        "--cat",
        "jra",
        "--output-dir",
        "tmp/o",
    ])
    with pytest.raises(ValueError):
        subject.normalize_fit_arguments(raw)


def test_normalize_fit_arguments_raises_without_output_dir():
    raw = subject.parse_args([
        "--mode",
        "fit",
        "--cat",
        "jra",
        "--predictions-root",
        "tmp/p",
    ])
    with pytest.raises(ValueError):
        subject.normalize_fit_arguments(raw)


def test_normalize_apply_arguments_returns_typeddict():
    raw = subject.parse_args([
        "--mode",
        "apply",
        "--cat",
        "nar",
        "--input-predictions-root",
        "tmp/in",
        "--calibration-dir",
        "tmp/cal",
        "--output-predictions-root",
        "tmp/out",
    ])
    normalized = subject.normalize_apply_arguments(raw)
    assert normalized["mode"] == "apply"
    assert normalized["cat"] == "nar"
    assert normalized["input_predictions_root"] == Path("tmp/in")
    assert normalized["calibration_dir"] == Path("tmp/cal")
    assert normalized["output_predictions_root"] == Path("tmp/out")


def test_normalize_apply_arguments_raises_without_input_predictions_root():
    raw = subject.parse_args([
        "--mode",
        "apply",
        "--cat",
        "nar",
        "--calibration-dir",
        "tmp/cal",
        "--output-predictions-root",
        "tmp/out",
    ])
    with pytest.raises(ValueError):
        subject.normalize_apply_arguments(raw)


def test_normalize_apply_arguments_raises_without_calibration_dir():
    raw = subject.parse_args([
        "--mode",
        "apply",
        "--cat",
        "nar",
        "--input-predictions-root",
        "tmp/in",
        "--output-predictions-root",
        "tmp/out",
    ])
    with pytest.raises(ValueError):
        subject.normalize_apply_arguments(raw)


def test_normalize_apply_arguments_raises_without_output_predictions_root():
    raw = subject.parse_args([
        "--mode",
        "apply",
        "--cat",
        "nar",
        "--input-predictions-root",
        "tmp/in",
        "--calibration-dir",
        "tmp/cal",
    ])
    with pytest.raises(ValueError):
        subject.normalize_apply_arguments(raw)


def test_format_fit_timestamp_uses_z_suffix():
    assert subject.format_fit_timestamp(FIXED_NOW) == "2026-06-04T12:00:00Z"


def test_now_utc_is_timezone_aware():
    now = subject.now_utc()
    assert now.tzinfo is not None


def test_normalize_bucket_key_returns_text_for_string():
    assert subject.normalize_bucket_key("G1") == "G1"


def test_normalize_bucket_key_returns_unknown_for_none():
    assert subject.normalize_bucket_key(None) == "_unknown"


def test_normalize_bucket_key_returns_unknown_for_nan():
    assert subject.normalize_bucket_key(float("nan")) == "_unknown"


def test_normalize_bucket_key_returns_unknown_for_empty_string():
    assert subject.normalize_bucket_key("   ") == "_unknown"


def test_normalize_bucket_key_stringifies_int():
    assert subject.normalize_bucket_key(999) == "999"


def test_derive_top1_prob_uses_existing_column():
    frame = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "predicted_rank": [1, 2],
        "predicted_top1_prob": [0.9, 0.4],
    })
    result = subject.derive_top1_prob(frame)
    assert result.iloc[0] == 0.9
    assert result.iloc[1] == 0.4


def test_derive_top1_prob_falls_back_to_rank_when_column_missing():
    frame = pd.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
    })
    result = subject.derive_top1_prob(frame)
    assert result.iloc[0] == 1.0
    assert result.iloc[3] == 0.25


def test_derive_top1_prob_falls_back_when_existing_column_all_null():
    frame = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "predicted_rank": [1, 2],
        "predicted_top1_prob": [None, None],
    })
    result = subject.derive_top1_prob(frame)
    assert result.iloc[0] == 1.0
    assert result.iloc[1] == 0.5


def test_derive_top3_prob_uses_existing_column():
    frame = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "predicted_rank": [1, 2],
        "predicted_top3_prob": [0.95, 0.6],
    })
    result = subject.derive_top3_prob(frame)
    assert result.iloc[0] == 0.95
    assert result.iloc[1] == 0.6


def test_derive_top3_prob_falls_back_to_rank_when_column_missing():
    # 4-horse race, top_n=3: proxy = (race_size - rank + 1) / race_size * 3
    # Rank 1: (4/4)*3=3.0, Rank 2: (3/4)*3=2.25, Rank 3: (2/4)*3=1.5, Rank 4: (1/4)*3=0.75
    frame = pd.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "predicted_rank": [1, 2, 3, 4],
    })
    result = subject.derive_top3_prob(frame)
    assert result.iloc[0] == pytest.approx(3.0)
    assert result.iloc[2] == pytest.approx(1.5)
    assert result.iloc[3] == pytest.approx(0.75)


def test_derive_top3_prob_falls_back_when_existing_column_all_null():
    # 2-horse race, top_n=3: Rank 1: (2/2)*3=3.0, Rank 2: (1/2)*3=1.5
    frame = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "predicted_rank": [1, 2],
        "predicted_top3_prob": [None, None],
    })
    result = subject.derive_top3_prob(frame)
    assert result.iloc[0] == pytest.approx(3.0)
    assert result.iloc[1] == pytest.approx(1.5)


def test_derive_prob_from_rank_returns_zeros_when_rank_column_missing():
    frame = pd.DataFrame({"race_id": ["r1", "r1"]})
    result = subject.derive_prob_from_rank(frame, top_n=1)
    assert result.iloc[0] == 0.0
    assert result.iloc[1] == 0.0


def test_derive_prob_from_rank_top3_allows_values_above_one():
    """top_n=3 proxy must NOT be clipped to 1.0 — top-ranked horses need
    distinguishable proxy values > 1.0 so isotonic regression can learn a
    meaningful calibration curve over them."""
    frame = pd.DataFrame({
        "race_id": ["r1"] * 10,
        "predicted_rank": list(range(1, 11)),
    })
    result = subject.derive_prob_from_rank(frame, top_n=3)
    # Rank 1 in a 10-horse race: (10/10)*3 = 3.0 — well above 1.0
    assert result.iloc[0] > 1.0, "rank-1 proxy for top_n=3 must exceed 1.0"
    # Rank 10: (1/10)*3 = 0.3 — below 1.0
    assert result.iloc[9] < 1.0
    # All values must be positive
    assert (result > 0.0).all()
    # Proxy must decrease with rank
    assert result.iloc[0] > result.iloc[4] > result.iloc[9]


def test_derive_prob_from_rank_top1_stays_within_zero_to_one():
    frame = pd.DataFrame({
        "race_id": ["r1"] * 5,
        "predicted_rank": [1, 2, 3, 4, 5],
    })
    result = subject.derive_prob_from_rank(frame, top_n=1)
    assert (result >= 0.0).all()
    assert (result <= 1.0).all()


def test_win_indicator_marks_rank_one_only():
    frame = pd.DataFrame({"actual_finish_position": [1.0, 2.0, 3.0, 4.0]})
    result = subject.win_indicator(frame)
    assert result.iloc[0] == 1.0
    assert result.iloc[1] == 0.0
    assert result.iloc[3] == 0.0


def test_top3_indicator_marks_top_three():
    frame = pd.DataFrame({"actual_finish_position": [1.0, 2.0, 3.0, 4.0]})
    result = subject.top3_indicator(frame)
    assert result.iloc[0] == 1.0
    assert result.iloc[1] == 1.0
    assert result.iloc[2] == 1.0
    assert result.iloc[3] == 0.0


def test_fit_single_curve_returns_schema_v1_and_finite_brier():
    probs = np.linspace(0.0, 1.0, num=200)
    targets = (probs > 0.5).astype(float)
    curve = subject.fit_single_curve(
        probs,
        targets,
        cat="jra",
        bucket_key="999",
        target_name="top1",
        now=FIXED_NOW,
    )
    assert curve["schema_version"] == 1
    assert curve["cat"] == "jra"
    assert curve["bucket_key"] == "999"
    assert curve["target"] == "top1"
    assert curve["n_samples"] == 200
    assert curve["fit_at"] == "2026-06-04T12:00:00Z"
    assert curve["brier_score_after"] <= curve["brier_score_before"]
    assert len(curve["iso_x"]) == len(curve["iso_y"])


def test_fit_run_writes_one_curve_pair_per_bucket_when_threshold_met(tmp_path: Path):
    frame = _g1_predictions_frame_600()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    json_writer = MagicMock()
    deps: subject.FitDeps = {
        "parquet_reader": parquet_reader,
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "now": _fixed_now,
    }
    args: subject.FitArguments = {
        "mode": "fit",
        "cat": "jra",
        "predictions_root": tmp_path / "preds",
        "output_dir": tmp_path / "out",
        "min_bucket_samples": 50,
        "bucket_dim": "kyoso_joken",
    }
    result = subject.fit_run(args, deps)
    assert result["buckets_written"] == 1
    assert result["fallback_used"] is False
    assert result["race_count"] == 60
    # 2 files per bucket (iso.json + iso_top3.json)
    assert json_writer.call_count == 2


def test_fit_run_falls_back_to_cat_global_when_no_bucket_meets_threshold(tmp_path: Path):
    frame = _small_bucket_frame_300()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    json_writer = MagicMock()
    deps: subject.FitDeps = {
        "parquet_reader": parquet_reader,
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "now": _fixed_now,
    }
    args: subject.FitArguments = {
        "mode": "fit",
        "cat": "jra",
        "predictions_root": tmp_path / "preds",
        "output_dir": tmp_path / "out",
        "min_bucket_samples": 500,
        "bucket_dim": "kyoso_joken",
    }
    result = subject.fit_run(args, deps)
    assert result["buckets_written"] == 1
    assert result["fallback_used"] is True
    # cat-global path: 2 files written (iso.json + iso_top3.json)
    assert json_writer.call_count == 2
    # both writes target the cat-global bucket directory
    first_call_path: Path = cast(Path, json_writer.call_args_list[0].args[1])
    second_call_path: Path = cast(Path, json_writer.call_args_list[1].args[1])
    assert "bucket__cat_global" in first_call_path.as_posix()
    assert "bucket__cat_global" in second_call_path.as_posix()


def test_fit_run_emits_warning_for_empty_parquet(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=pd.DataFrame()),
    )
    json_writer = MagicMock()
    deps: subject.FitDeps = {
        "parquet_reader": parquet_reader,
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "now": _fixed_now,
    }
    args: subject.FitArguments = {
        "mode": "fit",
        "cat": "jra",
        "predictions_root": tmp_path / "preds",
        "output_dir": tmp_path / "out",
        "min_bucket_samples": 500,
        "bucket_dim": "kyoso_joken",
    }
    result = subject.fit_run(args, deps)
    captured = capsys.readouterr()
    assert "empty predictions parquet" in captured.err
    assert result["buckets_written"] == 0
    assert result["race_count"] == 0
    json_writer.assert_not_called()


def test_fit_run_raises_when_actual_finish_position_missing(tmp_path: Path):
    frame = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "predicted_score": [0.5, 0.4],
        "predicted_rank": [1, 2],
        "kyoso_joken_code": ["999", "999"],
    })
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    json_writer = MagicMock()
    deps: subject.FitDeps = {
        "parquet_reader": parquet_reader,
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "now": _fixed_now,
    }
    args: subject.FitArguments = {
        "mode": "fit",
        "cat": "jra",
        "predictions_root": tmp_path / "preds",
        "output_dir": tmp_path / "out",
        "min_bucket_samples": 500,
        "bucket_dim": "kyoso_joken",
    }
    with pytest.raises(ValueError):
        subject.fit_run(args, deps)


def test_fit_run_with_grade_dim_uses_grade_code_column(tmp_path: Path):
    frame = _g1_predictions_frame_600()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    json_writer = MagicMock()
    deps: subject.FitDeps = {
        "parquet_reader": parquet_reader,
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "now": _fixed_now,
    }
    args: subject.FitArguments = {
        "mode": "fit",
        "cat": "nar",
        "predictions_root": tmp_path / "preds",
        "output_dir": tmp_path / "out",
        "min_bucket_samples": 50,
        "bucket_dim": "grade",
    }
    result = subject.fit_run(args, deps)
    # All rows share grade_code="A" -> 1 bucket written
    assert result["buckets_written"] == 1
    assert result["fallback_used"] is False
    # bucket_A is keyed by grade_code value
    first_call_path: Path = cast(Path, json_writer.call_args_list[0].args[1])
    assert "bucket_A" in first_call_path.as_posix()


def test_fit_run_falls_back_when_bucket_dim_column_missing(tmp_path: Path):
    frame = _g1_predictions_frame_600().drop(columns=["kyoso_joken_code"])
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    json_writer = MagicMock()
    deps: subject.FitDeps = {
        "parquet_reader": parquet_reader,
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "now": _fixed_now,
    }
    args: subject.FitArguments = {
        "mode": "fit",
        "cat": "jra",
        "predictions_root": tmp_path / "preds",
        "output_dir": tmp_path / "out",
        "min_bucket_samples": 50,
        "bucket_dim": "kyoso_joken",
    }
    result = subject.fit_run(args, deps)
    assert result["fallback_used"] is True
    assert result["buckets_written"] == 1


def test_isotonic_transform_uses_numpy_interp():
    probs = pd.Series([0.0, 0.5, 1.0])
    curve = _g1_iso_top1_curve()
    out = subject.isotonic_transform(probs, curve)
    assert out.iloc[0] == 0.05
    assert out.iloc[1] == 0.40
    assert out.iloc[2] == 0.90


def test_isotonic_transform_returns_identity_when_xs_empty():
    probs = pd.Series([0.3, 0.7])
    empty_curve: subject.CalibrationCurve = {
        "schema_version": 1,
        "cat": "jra",
        "bucket_key": "999",
        "target": "top1",
        "n_samples": 0,
        "iso_x": [],
        "iso_y": [],
        "fit_at": "2026-06-04T12:00:00Z",
        "brier_score_before": 0.0,
        "brier_score_after": 0.0,
    }
    out = subject.isotonic_transform(probs, empty_curve)
    assert out.iloc[0] == 0.3
    assert out.iloc[1] == 0.7


def test_resolve_bucket_column_prefers_kyoso_joken():
    frame = pd.DataFrame({"kyoso_joken_code": ["999"], "grade_code": ["A"]})
    assert subject.resolve_bucket_column(frame) == "kyoso_joken_code"


def test_resolve_bucket_column_falls_back_to_grade_code():
    frame = pd.DataFrame({"grade_code": ["A"]})
    assert subject.resolve_bucket_column(frame) == "grade_code"


def test_resolve_bucket_column_returns_none_when_neither_present():
    frame = pd.DataFrame({"race_id": ["r1"]})
    assert subject.resolve_bucket_column(frame) is None


def test_lookup_curve_returns_bucket_when_present(tmp_path: Path):
    bucket_path = tmp_path / "bucket_999" / "iso.json"
    bucket_path.parent.mkdir(parents=True)
    bucket_path.write_text("{}", encoding="utf-8")
    json_reader: subject.JsonReaderLike = cast(
        subject.JsonReaderLike, MagicMock(return_value=_g1_iso_top1_curve()),
    )
    path_exists: subject.PathExistsLike = lambda path: path == bucket_path
    curve, source = subject.lookup_curve(
        calibration_dir=tmp_path,
        bucket_key="999",
        filename="iso.json",
        json_reader=json_reader,
        path_exists=path_exists,
    )
    assert source == "bucket"
    assert curve is not None
    assert curve["bucket_key"] == "999"


def test_lookup_curve_falls_back_to_cat_global(tmp_path: Path):
    cat_global_path = tmp_path / "bucket__cat_global" / "iso.json"
    json_reader: subject.JsonReaderLike = cast(
        subject.JsonReaderLike, MagicMock(return_value=_cat_global_iso_top1_curve()),
    )
    path_exists: subject.PathExistsLike = lambda path: path == cat_global_path
    curve, source = subject.lookup_curve(
        calibration_dir=tmp_path,
        bucket_key="999",
        filename="iso.json",
        json_reader=json_reader,
        path_exists=path_exists,
    )
    assert source == "cat-global"
    assert curve is not None
    assert curve["bucket_key"] == "_cat_global"


def test_lookup_curve_returns_uncalibrated_when_neither_present(tmp_path: Path):
    json_reader: subject.JsonReaderLike = cast(subject.JsonReaderLike, MagicMock())
    path_exists: subject.PathExistsLike = lambda path: False
    curve, source = subject.lookup_curve(
        calibration_dir=tmp_path,
        bucket_key="999",
        filename="iso.json",
        json_reader=json_reader,
        path_exists=path_exists,
    )
    assert source == "uncalibrated"
    assert curve is None


def test_apply_run_writes_calibrated_columns_when_bucket_present(tmp_path: Path):
    frame = _apply_input_frame()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    parquet_writer = MagicMock()
    json_reader_mock = MagicMock(
        side_effect=[
            _g1_iso_top1_curve(),
            _g1_iso_top3_curve(),
        ],
    )
    path_exists: subject.PathExistsLike = lambda path: "bucket_999" in path.as_posix()
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": cast(subject.JsonReaderLike, json_reader_mock),
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    result = subject.apply_run(args, deps)
    assert result["rows_written"] == 6
    parquet_writer.assert_called_once()
    written_frame = cast(pd.DataFrame, parquet_writer.call_args.args[0])
    assert "predicted_top1_prob_calibrated" in written_frame.columns
    assert "predicted_top3_prob_calibrated" in written_frame.columns
    assert "calibration_source" in written_frame.columns
    assert (written_frame["calibration_source"] == "bucket").all()


def test_apply_run_falls_back_to_cat_global(tmp_path: Path):
    frame = _apply_input_frame()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    parquet_writer = MagicMock()
    json_reader_mock = MagicMock(
        side_effect=[
            _cat_global_iso_top1_curve(),
            _cat_global_iso_top3_curve(),
        ],
    )
    path_exists: subject.PathExistsLike = lambda path: "_cat_global" in path.as_posix()
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": cast(subject.JsonReaderLike, json_reader_mock),
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    result = subject.apply_run(args, deps)
    assert result["rows_written"] == 6
    written_frame = cast(pd.DataFrame, parquet_writer.call_args.args[0])
    assert (written_frame["calibration_source"] == "cat-global").all()


def test_apply_run_marks_uncalibrated_when_no_curve_found(tmp_path: Path):
    frame = _apply_input_frame()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    parquet_writer = MagicMock()
    json_reader: subject.JsonReaderLike = cast(subject.JsonReaderLike, MagicMock())
    path_exists: subject.PathExistsLike = lambda path: False
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": json_reader,
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    result = subject.apply_run(args, deps)
    assert result["rows_written"] == 6
    written_frame = cast(pd.DataFrame, parquet_writer.call_args.args[0])
    assert (written_frame["calibration_source"] == "uncalibrated").all()
    # Without calibration, top1 prob unchanged
    assert written_frame["predicted_top1_prob_calibrated"].iloc[0] == 0.9
    assert written_frame["predicted_top1_prob_calibrated"].iloc[3] == 0.85


def test_apply_run_re_ranks_per_race_by_calibrated_top1(tmp_path: Path):
    # Build a curve that inverts the ordering: higher raw prob maps lower
    inverting_top1: subject.CalibrationCurve = {
        "schema_version": 1,
        "cat": "jra",
        "bucket_key": "999",
        "target": "top1",
        "n_samples": 100,
        "iso_x": [0.0, 1.0],
        "iso_y": [1.0, 0.0],
        "fit_at": "2026-06-04T12:00:00Z",
        "brier_score_before": 0.30,
        "brier_score_after": 0.20,
    }
    frame = _apply_input_frame()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    parquet_writer = MagicMock()
    json_reader_mock = MagicMock(
        side_effect=[inverting_top1, _g1_iso_top3_curve()],
    )
    path_exists: subject.PathExistsLike = lambda path: "bucket_999" in path.as_posix()
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": cast(subject.JsonReaderLike, json_reader_mock),
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    subject.apply_run(args, deps)
    written_frame = cast(pd.DataFrame, parquet_writer.call_args.args[0])
    # After inverting calibration, the prev rank=3 horse (lowest raw prob) -> highest calibrated -> rank 1
    race1_mask = written_frame["race_id"] == "jra:2024:0512:05:01"
    race1 = written_frame[race1_mask].reset_index(drop=True)
    # umaban=3 had raw 0.2, becomes top after inversion
    rank_for_umaban_3 = int(race1.loc[race1["umaban"] == 3, "predicted_rank"].iloc[0])
    rank_for_umaban_1 = int(race1.loc[race1["umaban"] == 1, "predicted_rank"].iloc[0])
    assert rank_for_umaban_3 == 1
    assert rank_for_umaban_1 == 3


def test_apply_run_handles_empty_input(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=pd.DataFrame()),
    )
    parquet_writer = MagicMock()
    json_reader: subject.JsonReaderLike = cast(subject.JsonReaderLike, MagicMock())
    path_exists: subject.PathExistsLike = lambda path: False
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": json_reader,
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    result = subject.apply_run(args, deps)
    captured = capsys.readouterr()
    assert "empty input predictions" in captured.err
    assert result["rows_written"] == 0
    parquet_writer.assert_not_called()


def test_apply_run_handles_top3_calibration_independently(tmp_path: Path):
    frame = _apply_input_frame()
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    parquet_writer = MagicMock()
    json_reader_mock = MagicMock(
        side_effect=[_g1_iso_top1_curve(), _g1_iso_top3_curve()],
    )
    path_exists: subject.PathExistsLike = lambda path: "bucket_999" in path.as_posix()
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": cast(subject.JsonReaderLike, json_reader_mock),
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    subject.apply_run(args, deps)
    written_frame = cast(pd.DataFrame, parquet_writer.call_args.args[0])
    # top3 curve maps 1.0 -> 0.95, raw input row 0 has top3=1.0
    assert written_frame["predicted_top3_prob_calibrated"].iloc[0] == 0.95
    # top3 curve maps 0.4 by interp(0.4, [0.0, 0.5, 1.0], [0.10, 0.55, 0.95]) = 0.10 + (0.4/0.5)*(0.55-0.10) = 0.46
    assert written_frame["predicted_top3_prob_calibrated"].iloc[2] == pytest.approx(0.46, abs=1e-9)


def test_apply_run_uses_grade_code_when_kyoso_joken_absent(tmp_path: Path):
    frame = _apply_input_frame().drop(columns=["kyoso_joken_code"])
    frame["grade_code"] = "A"
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    parquet_writer = MagicMock()
    json_reader_mock = MagicMock(side_effect=[_g1_iso_top1_curve(), _g1_iso_top3_curve()])
    path_exists: subject.PathExistsLike = lambda path: "bucket_A" in path.as_posix()
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": cast(subject.JsonReaderLike, json_reader_mock),
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    result = subject.apply_run(args, deps)
    assert result["rows_written"] == 6
    written_frame = cast(pd.DataFrame, parquet_writer.call_args.args[0])
    assert (written_frame["calibration_source"] == "bucket").all()


def test_apply_run_uses_unknown_bucket_when_dim_column_missing(tmp_path: Path):
    frame = _apply_input_frame().drop(columns=["kyoso_joken_code"])
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    parquet_writer = MagicMock()
    json_reader_mock = MagicMock(
        side_effect=[_cat_global_iso_top1_curve(), _cat_global_iso_top3_curve()],
    )
    path_exists: subject.PathExistsLike = lambda path: "_cat_global" in path.as_posix()
    deps: subject.ApplyDeps = {
        "parquet_reader": parquet_reader,
        "parquet_writer": cast(subject.ParquetWriterLike, parquet_writer),
        "json_reader": cast(subject.JsonReaderLike, json_reader_mock),
        "path_exists": path_exists,
    }
    args: subject.ApplyArguments = {
        "mode": "apply",
        "cat": "jra",
        "input_predictions_root": tmp_path / "in",
        "calibration_dir": tmp_path / "cal",
        "output_predictions_root": tmp_path / "out",
    }
    result = subject.apply_run(args, deps)
    assert result["rows_written"] == 6


def test_race_count_from_frame_returns_zero_when_race_id_missing():
    frame = pd.DataFrame({"predicted_score": [0.5]})
    assert subject.race_count_from_frame(frame) == 0


def test_race_count_from_frame_uses_nunique():
    frame = pd.DataFrame({"race_id": ["r1", "r1", "r2"]})
    assert subject.race_count_from_frame(frame) == 2


def test_log_source_distribution_summarizes_counts(capsys: pytest.CaptureFixture[str]):
    series = pd.Series(["bucket", "bucket", "cat-global", "uncalibrated"])
    subject.log_source_distribution(series)
    captured = capsys.readouterr()
    assert "bucket=2/4" in captured.err
    assert "cat-global=1/4" in captured.err
    assert "uncalibrated=1/4" in captured.err


def test_log_source_distribution_empty_emits_nothing(capsys: pytest.CaptureFixture[str]):
    series = pd.Series([], dtype=str)
    subject.log_source_distribution(series)
    captured = capsys.readouterr()
    assert captured.err == ""


def test_fit_run_uses_row_count_not_race_count_for_bucket_threshold(tmp_path: Path):
    """min_bucket_samples compares against len(bucket_frame), not unique race count."""
    # 1 race, 10 horses → row_count=10, race_count=1.
    # min_bucket_samples=5: row_count (10) >= 5 → bucket must be written (not fallback).
    frame = pd.DataFrame({
        "race_id": ["r1"] * 10,
        "ketto_toroku_bango": [f"horse_{i}" for i in range(10)],
        "predicted_score": [float(i) / 10 for i in range(10)],
        "predicted_rank": list(range(1, 11)),
        "predicted_top1_prob": [float(i) / 10 for i in range(10)],
        "predicted_top3_prob": [min(float(i) / 10 * 3, 1.0) for i in range(10)],
        "actual_finish_position": [float(i + 1) for i in range(10)],
        "kyoso_joken_code": ["016"] * 10,
        "grade_code": ["B"] * 10,
    })
    parquet_reader: subject.ParquetDirReaderLike = cast(
        subject.ParquetDirReaderLike, MagicMock(return_value=frame),
    )
    json_writer = MagicMock()
    deps: subject.FitDeps = {
        "parquet_reader": parquet_reader,
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "now": _fixed_now,
    }
    args: subject.FitArguments = {
        "mode": "fit",
        "cat": "jra",
        "predictions_root": tmp_path / "preds",
        "output_dir": tmp_path / "out",
        "min_bucket_samples": 5,
        "bucket_dim": "kyoso_joken",
    }
    result = subject.fit_run(args, deps)
    assert result["buckets_written"] == 1
    assert result["fallback_used"] is False


def test_supported_categories_includes_banei():
    assert subject.CATEGORY_BANEI == "ban-ei"
    assert "ban-ei" in subject.SUPPORTED_CATEGORIES


def test_default_write_calibration_json_round_trip(tmp_path: Path):
    payload = _g1_iso_top1_curve()
    out_path = tmp_path / "bucket_999" / "iso.json"
    subject.default_write_calibration_json(payload, out_path)
    parsed = json.loads(out_path.read_text(encoding="utf-8"))
    assert parsed["bucket_key"] == "999"
    assert parsed["iso_x"][0] == 0.0


def test_default_read_calibration_json_returns_dict(tmp_path: Path):
    out_path = tmp_path / "iso.json"
    out_path.write_text(json.dumps(_g1_iso_top1_curve()), encoding="utf-8")
    parsed = subject.default_read_calibration_json(out_path)
    assert parsed["bucket_key"] == "999"


def test_default_read_parquet_dir_concatenates_partitions(tmp_path: Path):
    part1 = tmp_path / "year=2024"
    part1.mkdir()
    pd.DataFrame({"a": [1, 2]}).to_parquet((part1 / "data.parquet").as_posix())
    part2 = tmp_path / "year=2025"
    part2.mkdir()
    pd.DataFrame({"a": [3]}).to_parquet((part2 / "data.parquet").as_posix())
    frame = subject.default_read_parquet_dir(tmp_path)
    assert len(frame) == 3


def test_default_read_parquet_dir_returns_empty_when_no_files(tmp_path: Path):
    frame = subject.default_read_parquet_dir(tmp_path)
    assert frame.empty


def test_default_read_parquet_dir_reads_single_file(tmp_path: Path):
    file_path = tmp_path / "data.parquet"
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(file_path.as_posix())
    frame = subject.default_read_parquet_dir(file_path)
    assert len(frame) == 3


def test_default_write_parquet_writes_partitioned(tmp_path: Path):
    frame = pd.DataFrame({
        "ketto_toroku_bango": ["A", "B"],
        "predicted_score": [0.9, 0.4],
        "category": ["jra", "jra"],
        "race_year": [2024, 2024],
    })
    subject.default_write_parquet(frame, tmp_path / "out")
    parquet_files = list((tmp_path / "out").rglob("*.parquet"))
    assert len(parquet_files) >= 1


def test_default_path_exists_returns_true_for_existing(tmp_path: Path):
    file_path = tmp_path / "foo"
    file_path.write_text("x", encoding="utf-8")
    assert subject.default_path_exists(file_path) is True


def test_default_path_exists_returns_false_for_missing(tmp_path: Path):
    assert subject.default_path_exists(tmp_path / "nope") is False


def test_build_default_fit_deps_returns_all_keys():
    deps = subject.build_default_fit_deps()
    assert callable(deps["parquet_reader"])
    assert callable(deps["json_writer"])
    assert callable(deps["now"])


def test_build_default_apply_deps_returns_all_keys():
    deps = subject.build_default_apply_deps()
    assert callable(deps["parquet_reader"])
    assert callable(deps["parquet_writer"])
    assert callable(deps["json_reader"])
    assert callable(deps["path_exists"])


def test_main_dispatches_to_fit(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    fake_fit_run = MagicMock(return_value={"cat": "jra", "buckets_written": 1})
    fake_apply_run = MagicMock()
    monkeypatch.setattr(subject, "fit_run", fake_fit_run)
    monkeypatch.setattr(subject, "apply_run", fake_apply_run)
    subject.main([
        "--mode",
        "fit",
        "--cat",
        "jra",
        "--predictions-root",
        "tmp/preds",
        "--output-dir",
        "tmp/out",
    ])
    captured = capsys.readouterr()
    assert "buckets_written" in captured.out
    fake_fit_run.assert_called_once()
    fake_apply_run.assert_not_called()


def test_main_dispatches_to_apply(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    fake_fit_run = MagicMock()
    fake_apply_run = MagicMock(return_value={"cat": "nar", "rows_written": 42})
    monkeypatch.setattr(subject, "fit_run", fake_fit_run)
    monkeypatch.setattr(subject, "apply_run", fake_apply_run)
    subject.main([
        "--mode",
        "apply",
        "--cat",
        "nar",
        "--input-predictions-root",
        "tmp/in",
        "--calibration-dir",
        "tmp/cal",
        "--output-predictions-root",
        "tmp/out",
    ])
    captured = capsys.readouterr()
    assert "rows_written" in captured.out
    fake_apply_run.assert_called_once()
    fake_fit_run.assert_not_called()
