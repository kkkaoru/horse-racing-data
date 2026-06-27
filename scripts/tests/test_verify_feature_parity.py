"""Tests for verify_feature_parity.

I/O is exercised with tiny real parquet/json files written under tmp_path
(no network, no mocks for the parquet schema path) plus targeted unit tests on
the pure comparison / decision functions for full branch coverage.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import verify_feature_parity as vfp


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_parquet(path: Path, columns: list[str]) -> Path:
    """Write a 1-row parquet whose schema has exactly *columns*."""
    table = pa.table({col: [0] for col in columns})
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def _write_metadata(path: Path, feature_names: list[str], feature_count: int) -> Path:
    path.write_text(
        json.dumps({"feature_names": feature_names, "feature_count": feature_count}),
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------------------
# find_first_parquet
# ---------------------------------------------------------------------------


class TestFindFirstParquet:
    def test_returns_file_when_path_is_file(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["a"])
        assert vfp.find_first_parquet(f) == f

    def test_globs_first_year_shard_in_dir(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "race_year=2017" / "data_0.parquet", ["a"])
        _write_parquet(tmp_path / "race_year=2016" / "data_0.parquet", ["a"])
        result = vfp.find_first_parquet(tmp_path)
        assert result is not None
        # sorted glob -> 2016 shard comes first
        assert result.parent.name == "race_year=2016"

    def test_returns_none_for_empty_dir(self, tmp_path: Path) -> None:
        assert vfp.find_first_parquet(tmp_path) is None

    def test_returns_none_for_missing_path(self, tmp_path: Path) -> None:
        assert vfp.find_first_parquet(tmp_path / "does-not-exist") is None


# ---------------------------------------------------------------------------
# read_parquet_columns
# ---------------------------------------------------------------------------


class TestReadParquetColumns:
    def test_reads_schema_column_names(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["umaban", "kyori", "weather"])
        assert vfp.read_parquet_columns(f) == {"umaban", "kyori", "weather"}


# ---------------------------------------------------------------------------
# load_metadata
# ---------------------------------------------------------------------------


class TestLoadMetadata:
    def test_returns_names_and_count(self, tmp_path: Path) -> None:
        f = _write_metadata(tmp_path / "metadata.json", ["a", "b"], 2)
        names, count = vfp.load_metadata(f)
        assert names == ["a", "b"]
        assert count == 2

    def test_missing_feature_names_yields_empty_list(self, tmp_path: Path) -> None:
        f = tmp_path / "metadata.json"
        f.write_text(json.dumps({"feature_count": 0}), encoding="utf-8")
        names, count = vfp.load_metadata(f)
        assert names == []
        assert count == 0

    def test_missing_feature_count_falls_back_to_len(self, tmp_path: Path) -> None:
        f = tmp_path / "metadata.json"
        f.write_text(json.dumps({"feature_names": ["a", "b", "c"]}), encoding="utf-8")
        names, count = vfp.load_metadata(f)
        assert names == ["a", "b", "c"]
        assert count == 3

    def test_non_list_feature_names_yields_empty_list(self, tmp_path: Path) -> None:
        f = tmp_path / "metadata.json"
        f.write_text(json.dumps({"feature_names": "oops"}), encoding="utf-8")
        names, _ = vfp.load_metadata(f)
        assert names == []


# ---------------------------------------------------------------------------
# load_model_meta_count
# ---------------------------------------------------------------------------


class TestLoadModelMetaCount:
    def test_returns_count_for_category(self, tmp_path: Path) -> None:
        f = tmp_path / "model_meta.json"
        f.write_text(json.dumps({"feature_counts": {"jra": 244}}), encoding="utf-8")
        assert vfp.load_model_meta_count(f, "jra") == 244

    def test_none_category_returns_none(self, tmp_path: Path) -> None:
        f = tmp_path / "model_meta.json"
        f.write_text(json.dumps({"feature_counts": {"jra": 244}}), encoding="utf-8")
        assert vfp.load_model_meta_count(f, None) is None

    def test_missing_category_returns_none(self, tmp_path: Path) -> None:
        f = tmp_path / "model_meta.json"
        f.write_text(json.dumps({"feature_counts": {"jra": 244}}), encoding="utf-8")
        assert vfp.load_model_meta_count(f, "nar") is None

    def test_non_dict_feature_counts_returns_none(self, tmp_path: Path) -> None:
        f = tmp_path / "model_meta.json"
        f.write_text(json.dumps({"feature_counts": []}), encoding="utf-8")
        assert vfp.load_model_meta_count(f, "jra") is None

    def test_non_int_count_returns_none(self, tmp_path: Path) -> None:
        f = tmp_path / "model_meta.json"
        f.write_text(
            json.dumps({"feature_counts": {"jra": "244"}}), encoding="utf-8"
        )
        assert vfp.load_model_meta_count(f, "jra") is None


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------


class TestCompare:
    def test_all_consistent(self) -> None:
        report = vfp.compare(["a", "b"], 2, {"a", "b", "c"}, 2)
        assert report.missing_in_parquet == []
        assert report.extra_in_parquet == ["c"]
        assert report.count_mismatch is False
        assert report.model_meta_mismatch is False
        assert report.is_fatal is False
        assert report.has_warning is False

    def test_missing_column_is_fatal(self) -> None:
        report = vfp.compare(["a", "b", "weather"], 3, {"a", "b"}, None)
        assert report.missing_in_parquet == ["weather"]
        assert report.is_fatal is True

    def test_count_mismatch_is_warning(self) -> None:
        report = vfp.compare(["a", "b"], 99, {"a", "b"}, None)
        assert report.count_mismatch is True
        assert report.has_warning is True
        assert report.is_fatal is False

    def test_model_meta_mismatch_is_warning(self) -> None:
        report = vfp.compare(["a", "b"], 2, {"a", "b"}, 5)
        assert report.model_meta_mismatch is True
        assert report.has_warning is True

    def test_model_meta_none_skips_that_check(self) -> None:
        report = vfp.compare(["a", "b"], 2, {"a", "b"}, None)
        assert report.model_meta_mismatch is False

    def test_model_meta_match_no_warning(self) -> None:
        report = vfp.compare(["a", "b"], 2, {"a", "b"}, 2)
        assert report.model_meta_mismatch is False


# ---------------------------------------------------------------------------
# decide_exit_code
# ---------------------------------------------------------------------------


class TestDecideExitCode:
    def test_fatal_returns_1(self) -> None:
        report = vfp.compare(["a", "x"], 2, {"a"}, None)
        assert vfp.decide_exit_code(report, strict=False) == 1

    def test_warning_non_strict_returns_0(self) -> None:
        report = vfp.compare(["a"], 99, {"a"}, None)
        assert vfp.decide_exit_code(report, strict=False) == 0

    def test_warning_strict_returns_1(self) -> None:
        report = vfp.compare(["a"], 99, {"a"}, None)
        assert vfp.decide_exit_code(report, strict=True) == 1

    def test_clean_returns_0(self) -> None:
        report = vfp.compare(["a"], 1, {"a"}, None)
        assert vfp.decide_exit_code(report, strict=True) == 0


# ---------------------------------------------------------------------------
# report_and_exit_code (logging branches)
# ---------------------------------------------------------------------------


class TestReportAndExitCode:
    def test_fatal_logs_error_and_returns_1(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        report = vfp.compare(["a", "weather"], 2, {"a"}, None)
        with caplog.at_level(logging.ERROR):
            code = vfp.report_and_exit_code(
                report, ["a", "weather"], 2, None, strict=False
            )
        assert code == 1
        assert "FATAL" in caplog.text
        assert "weather" in caplog.text

    def test_count_mismatch_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        report = vfp.compare(["a"], 99, {"a"}, None)
        with caplog.at_level(logging.WARNING):
            vfp.report_and_exit_code(report, ["a"], 99, None, strict=False)
        assert "feature_count" in caplog.text

    def test_model_meta_mismatch_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        report = vfp.compare(["a"], 1, {"a"}, 7)
        with caplog.at_level(logging.WARNING):
            vfp.report_and_exit_code(report, ["a"], 1, 7, strict=False)
        assert "model_meta" in caplog.text

    def test_extra_columns_log_info(self, caplog: pytest.LogCaptureFixture) -> None:
        report = vfp.compare(["a"], 1, {"a", "extra"}, None)
        with caplog.at_level(logging.INFO):
            vfp.report_and_exit_code(report, ["a"], 1, None, strict=False)
        assert "not in feature_names" in caplog.text

    def test_clean_logs_ok(self, caplog: pytest.LogCaptureFixture) -> None:
        report = vfp.compare(["a"], 1, {"a"}, 1)
        with caplog.at_level(logging.INFO):
            code = vfp.report_and_exit_code(report, ["a"], 1, 1, strict=False)
        assert code == 0
        assert "OK" in caplog.text

    def test_warning_strict_no_ok_message(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        report = vfp.compare(["a"], 99, {"a"}, None)
        with caplog.at_level(logging.INFO):
            code = vfp.report_and_exit_code(report, ["a"], 99, None, strict=True)
        assert code == 1
        assert "OK:" not in caplog.text


# ---------------------------------------------------------------------------
# run_verification (integration with real files)
# ---------------------------------------------------------------------------


class TestRunVerification:
    def test_no_parquet_returns_1(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        meta = _write_metadata(tmp_path / "metadata.json", ["a"], 1)
        with caplog.at_level(logging.ERROR):
            code = vfp.run_verification(
                tmp_path / "empty", meta, None, None, strict=False
            )
        assert code == 1
        assert "No parquet file found" in caplog.text

    def test_clean_dir_returns_0(self, tmp_path: Path) -> None:
        _write_parquet(
            tmp_path / "feat" / "race_year=2016" / "data_0.parquet", ["a", "b", "extra"]
        )
        meta = _write_metadata(tmp_path / "metadata.json", ["a", "b"], 2)
        code = vfp.run_verification(
            tmp_path / "feat", meta, None, None, strict=False
        )
        assert code == 0

    def test_missing_column_returns_1(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["a"])
        meta = _write_metadata(tmp_path / "metadata.json", ["a", "weather"], 2)
        code = vfp.run_verification(f, meta, None, None, strict=False)
        assert code == 1

    def test_with_model_meta_match(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["a", "b"])
        meta = _write_metadata(tmp_path / "metadata.json", ["a", "b"], 2)
        model_meta = tmp_path / "model_meta.json"
        model_meta.write_text(
            json.dumps({"feature_counts": {"jra": 2}}), encoding="utf-8"
        )
        code = vfp.run_verification(f, meta, model_meta, "jra", strict=False)
        assert code == 0

    def test_with_model_meta_mismatch_strict_returns_1(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["a", "b"])
        meta = _write_metadata(tmp_path / "metadata.json", ["a", "b"], 2)
        model_meta = tmp_path / "model_meta.json"
        model_meta.write_text(
            json.dumps({"feature_counts": {"jra": 99}}), encoding="utf-8"
        )
        code = vfp.run_verification(f, meta, model_meta, "jra", strict=True)
        assert code == 1


# ---------------------------------------------------------------------------
# _parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    def test_required_args(self) -> None:
        args = vfp._parse_args(
            ["--features-parquet", "/feat", "--metadata", "/meta.json"]
        )
        assert args.features_parquet == "/feat"
        assert args.metadata == "/meta.json"
        assert args.model_meta is None
        assert args.category is None
        assert args.strict is False

    def test_optional_args(self) -> None:
        args = vfp._parse_args(
            [
                "--features-parquet",
                "/feat",
                "--metadata",
                "/meta.json",
                "--model-meta",
                "/mm.json",
                "--category",
                "jra",
                "--strict",
            ]
        )
        assert args.model_meta == "/mm.json"
        assert args.category == "jra"
        assert args.strict is True


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    def test_main_clean_returns_0(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["a", "b"])
        meta = _write_metadata(tmp_path / "metadata.json", ["a", "b"], 2)
        code = vfp.main(
            ["--features-parquet", str(f), "--metadata", str(meta)]
        )
        assert code == 0

    def test_main_fatal_returns_1(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["a"])
        meta = _write_metadata(tmp_path / "metadata.json", ["a", "weather"], 2)
        code = vfp.main(
            ["--features-parquet", str(f), "--metadata", str(meta)]
        )
        assert code == 1

    def test_main_passes_model_meta(self, tmp_path: Path) -> None:
        f = _write_parquet(tmp_path / "data.parquet", ["a", "b"])
        meta = _write_metadata(tmp_path / "metadata.json", ["a", "b"], 2)
        model_meta = tmp_path / "model_meta.json"
        model_meta.write_text(
            json.dumps({"feature_counts": {"jra": 2}}), encoding="utf-8"
        )
        code = vfp.main(
            [
                "--features-parquet",
                str(f),
                "--metadata",
                str(meta),
                "--model-meta",
                str(model_meta),
                "--category",
                "jra",
            ]
        )
        assert code == 0
