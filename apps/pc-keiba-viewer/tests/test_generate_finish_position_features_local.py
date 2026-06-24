from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import finish_position_features_duckdb
import generate_finish_position_features_local as subject


def test_parse_args_full_set():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1/predictions",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "jra",
            "--threads",
            "4",
            "--memory-limit",
            "8GB",
        ]
    )
    assert args.pg_url == "postgresql://u:p@h/db"
    assert args.running_style_parquet == Path("tmp/run/v1/predictions")
    assert args.output_dir == Path("tmp/finish/v1/features")
    assert args.finish_position_version == "v1"
    assert args.running_style_feature_version == "v1"
    assert args.year_from == 2020
    assert args.year_to == 2024
    assert args.category == "jra"
    assert args.threads == 4
    assert args.memory_limit == "8GB"
    assert args.resume is False
    assert args.incremental is False
    assert args.temp_dir is None


def test_parse_args_resume_flag_sets_true():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
            "--resume",
        ]
    )
    assert args.resume is True


def test_parse_args_resume_flag_defaults_false():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
        ]
    )
    assert args.resume is False


def test_parse_args_incremental_flag_sets_true():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
            "--incremental",
        ]
    )
    assert args.incremental is True


def test_parse_args_incremental_flag_defaults_false():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
        ]
    )
    assert args.incremental is False


def test_parse_args_temp_dir_sets_path():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
            "--temp-dir",
            "/some/path",
        ]
    )
    assert args.temp_dir == Path("/some/path")


def test_parse_args_temp_dir_defaults_none():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
        ]
    )
    assert args.temp_dir is None


def test_parse_args_defaults_threads_and_memory():
    args = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run/v1",
            "--output-dir",
            "tmp/finish/v1/features",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
        ]
    )
    assert args.threads == 8
    assert args.memory_limit == "16GB"


def test_parse_args_rejects_unknown_category():
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--pg-url",
                "x",
                "--running-style-parquet",
                "x",
                "--output-dir",
                "x",
                "--finish-position-version",
                "v1",
                "--running-style-feature-version",
                "v1",
                "--year-from",
                "2020",
                "--year-to",
                "2024",
                "--category",
                "bogus",
            ]
        )


def test_normalize_arguments_converts_paths_and_ints():
    raw = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run",
            "--output-dir",
            "tmp/finish",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "ban-ei",
        ]
    )
    normalized = subject.normalize_arguments(raw)
    assert normalized["running_style_parquet"] == Path("tmp/run")
    assert normalized["output_dir"] == Path("tmp/finish")
    assert normalized["year_from"] == 2020
    assert normalized["year_to"] == 2024
    assert normalized["category"] == "ban-ei"


def test_normalize_arguments_maps_checkpoint_options_when_set():
    raw = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run",
            "--output-dir",
            "tmp/finish",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
            "--resume",
            "--incremental",
            "--temp-dir",
            "/spill/here",
        ]
    )
    normalized = subject.normalize_arguments(raw)
    assert normalized["resume"] is True
    assert normalized["incremental"] is True
    assert normalized["temp_dir"] == Path("/spill/here")


def test_normalize_arguments_checkpoint_options_default_off():
    raw = subject.parse_args(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            "tmp/run",
            "--output-dir",
            "tmp/finish",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "nar",
        ]
    )
    normalized = subject.normalize_arguments(raw)
    assert normalized["resume"] is False
    assert normalized["incremental"] is False
    assert normalized["temp_dir"] is None


def test_resolve_temp_dir_returns_explicit_when_provided():
    args: subject.PhaseAArguments = {
        "pg_url": "postgresql://u:p@h/db",
        "running_style_parquet": Path("tmp/run"),
        "output_dir": Path("tmp/finish/features"),
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "year_from": 2020,
        "year_to": 2024,
        "category": "nar",
        "threads": 8,
        "memory_limit": "16GB",
        "resume": True,
        "incremental": True,
        "temp_dir": Path("/explicit/spill"),
    }
    assert subject.resolve_temp_dir(args) == Path("/explicit/spill")


def test_resolve_temp_dir_returns_per_category_default_when_none():
    args: subject.PhaseAArguments = {
        "pg_url": "postgresql://u:p@h/db",
        "running_style_parquet": Path("tmp/run"),
        "output_dir": Path("tmp/finish/features"),
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "year_from": 2020,
        "year_to": 2024,
        "category": "nar",
        "threads": 8,
        "memory_limit": "16GB",
        "resume": False,
        "incremental": False,
        "temp_dir": None,
    }
    assert subject.resolve_temp_dir(args) == Path("tmp/finish/spill-nar")


def test_assert_running_style_parquet_present_rejects_missing(tmp_path: Path):
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError) as info:
        subject.assert_running_style_parquet_present(missing)
    assert "Running-style parquet input not found" in str(info.value)


def test_assert_running_style_parquet_present_accepts_existing(tmp_path: Path):
    existing = tmp_path / "exists"
    existing.mkdir()
    subject.assert_running_style_parquet_present(existing)


def test_sql_quote_literal_doubles_single_quotes():
    assert subject.sql_quote_literal("foo'bar") == "foo''bar"


def test_sql_quote_literal_passes_through_plain():
    assert subject.sql_quote_literal("hello") == "hello"


def test_build_feature_builder_args_sets_clean_output_and_dates():
    args: subject.PhaseAArguments = {
        "pg_url": "postgresql://u:p@h/db",
        "running_style_parquet": Path("tmp/run"),
        "output_dir": Path("tmp/finish/features"),
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "year_from": 2020,
        "year_to": 2024,
        "category": "jra",
        "threads": 4,
        "memory_limit": "8GB",
        "resume": False,
        "incremental": False,
        "temp_dir": None,
    }
    raw_dir = Path("tmp/finish/_raw-jra-2020-2024")
    builder = subject.build_feature_builder_args(args, raw_dir)
    assert builder.category == "jra"
    assert builder.from_date == "20200101"
    assert builder.to_date == "20241231"
    assert builder.target_date is None
    assert builder.output_dir == raw_dir
    assert builder.pg_url == "postgresql://u:p@h/db"
    assert builder.threads == 4
    assert builder.memory_limit == "8GB"
    assert builder.skip_count is False
    assert builder.keep_existing_output is False
    assert builder.force_clean_output is True
    assert builder.temp_dir == Path("tmp/finish/spill-jra")
    assert builder.status_file is None
    assert builder.log_file is None
    assert builder.resume is False
    assert builder.incremental is False
    assert builder.venue_weather_dir is None
    assert builder.realtime_odds is None


def test_build_feature_builder_args_passes_checkpoint_flags_and_explicit_temp_dir():
    args: subject.PhaseAArguments = {
        "pg_url": "postgresql://u:p@h/db",
        "running_style_parquet": Path("tmp/run"),
        "output_dir": Path("tmp/finish/features"),
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "year_from": 2020,
        "year_to": 2024,
        "category": "nar",
        "threads": 4,
        "memory_limit": "8GB",
        "resume": True,
        "incremental": True,
        "temp_dir": Path("/explicit/spill"),
    }
    raw_dir = Path("tmp/finish/_raw-nar-2020-2024")
    builder = subject.build_feature_builder_args(args, raw_dir)
    assert builder.resume is True
    assert builder.incremental is True
    assert builder.temp_dir == Path("/explicit/spill")


def test_build_raw_output_dir_uses_category_and_years():
    args: subject.PhaseAArguments = {
        "pg_url": "x",
        "running_style_parquet": Path("tmp/run"),
        "output_dir": Path("tmp/finish/v1/features"),
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "year_from": 2020,
        "year_to": 2024,
        "category": "nar",
        "threads": 8,
        "memory_limit": "16GB",
        "resume": False,
        "incremental": False,
        "temp_dir": None,
    }
    raw = subject.build_raw_output_dir(args)
    assert raw == Path("tmp/finish/v1/_raw-nar-2020-2024")


def test_build_attach_running_style_sql_uses_hive_partitioning():
    sql = subject.build_attach_running_style_sql(Path("tmp/run-style/v1/predictions"))
    assert sql == (
        "create or replace view running_style_local as "
        "select * from read_parquet('tmp/run-style/v1/predictions/**/*.parquet', hive_partitioning=1)"
    )


def test_build_stamp_features_sql_embeds_versions_and_category():
    sql = subject.build_stamp_features_sql(
        Path("tmp/_raw-jra-2020-2024"),
        "jra",
        "v1",
        "v1",
    )
    assert sql == (
        "create or replace temp table stamped_features as "
        "select 'jra' as category, *, "
        "'v1' as finish_position_version, "
        "'v1' as running_style_feature_version "
        "from read_parquet('tmp/_raw-jra-2020-2024/**/*.parquet', hive_partitioning=1)"
    )


def test_build_copy_stamped_features_sql_partitions_by_category_and_race_year():
    sql = subject.build_copy_stamped_features_sql(Path("tmp/finish/v1/features"))
    assert sql == (
        "copy stamped_features "
        "to 'tmp/finish/v1/features' "
        "(format parquet, partition_by (category, race_year), overwrite_or_ignore true)"
    )


def test_stamp_versions_and_rewrite_parquet_runs_expected_sql(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    final_dir = tmp_path / "final"
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (42,)
    rows = subject.stamp_versions_and_rewrite_parquet(
        con,
        raw_dir,
        final_dir,
        "v1",
        "v1",
        "jra",
    )
    assert rows == 42
    assert final_dir.exists()
    assert con.execute.call_count == 3
    first_sql = con.execute.call_args_list[0].args[0]
    assert "stamped_features" in first_sql
    second_sql = con.execute.call_args_list[1].args[0]
    assert "copy stamped_features" in second_sql
    third_sql = con.execute.call_args_list[2].args[0]
    assert third_sql == "select count(*) from stamped_features"


def test_stamp_versions_and_rewrite_parquet_handles_none_fetchone(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    final_dir = tmp_path / "final"
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = None
    rows = subject.stamp_versions_and_rewrite_parquet(
        con, raw_dir, final_dir, "v1", "v1", "jra"
    )
    assert rows == 0


def test_stamp_versions_and_rewrite_parquet_removes_existing_final(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    final_dir = tmp_path / "final"
    final_dir.mkdir()
    sentinel = final_dir / "sentinel.txt"
    sentinel.write_text("x", encoding="utf-8")
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (1,)
    subject.stamp_versions_and_rewrite_parquet(
        con, raw_dir, final_dir, "v1", "v1", "jra"
    )
    assert not sentinel.exists()
    assert final_dir.exists()


def test_configure_local_duckdb_sets_threads_and_memory_limit(monkeypatch: pytest.MonkeyPatch):
    fake_con = MagicMock()
    fake_duckdb = MagicMock()
    fake_duckdb.connect.return_value = fake_con
    monkeypatch.setitem(__import__("sys").modules, "duckdb", fake_duckdb)
    con = subject.configure_local_duckdb(8, "16GB")
    assert con is fake_con
    fake_duckdb.connect.assert_called_once_with(":memory:")
    set_calls = [call.args[0] for call in fake_con.execute.call_args_list]
    assert set_calls == ["SET threads = 8", "SET memory_limit = '16GB'"]


def test_run_phase_a_orchestrates_feature_build_and_postprocess(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    rs_parquet = tmp_path / "run-style" / "v1" / "predictions"
    rs_parquet.mkdir(parents=True)
    output_dir = tmp_path / "finish" / "v1" / "features"
    raw_dir = output_dir.parent / "_raw-jra-2020-2024"
    raw_dir.mkdir(parents=True)
    feature_builder_run = MagicMock(return_value=None)
    monkeypatch.setattr(finish_position_features_duckdb, "run", feature_builder_run)
    fake_con = MagicMock()
    fake_con.execute.return_value.fetchone.return_value = (17,)
    monkeypatch.setattr(subject, "configure_local_duckdb", MagicMock(return_value=fake_con))
    args: subject.PhaseAArguments = {
        "pg_url": "postgresql://u:p@h/db",
        "running_style_parquet": rs_parquet,
        "output_dir": output_dir,
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "year_from": 2020,
        "year_to": 2024,
        "category": "jra",
        "threads": 8,
        "memory_limit": "16GB",
        "resume": False,
        "incremental": False,
        "temp_dir": None,
    }
    result = subject.run_phase_a(args)
    assert result["rows_written"] == 17
    assert result["category"] == "jra"
    assert result["year_from"] == 2020
    assert result["year_to"] == 2024
    assert result["finish_position_version"] == "v1"
    assert result["running_style_feature_version"] == "v1"
    assert result["output_dir"] == output_dir.as_posix()
    assert feature_builder_run.call_count == 1
    fake_con.close.assert_called_once()
    assert not raw_dir.exists()


def test_run_phase_a_aborts_when_running_style_parquet_missing(tmp_path: Path):
    args: subject.PhaseAArguments = {
        "pg_url": "postgresql://u:p@h/db",
        "running_style_parquet": tmp_path / "does-not-exist",
        "output_dir": tmp_path / "finish",
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "year_from": 2020,
        "year_to": 2024,
        "category": "jra",
        "threads": 8,
        "memory_limit": "16GB",
        "resume": False,
        "incremental": False,
        "temp_dir": None,
    }
    with pytest.raises(FileNotFoundError):
        subject.run_phase_a(args)


def test_main_prints_json_result(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
):
    rs_parquet = tmp_path / "run"
    rs_parquet.mkdir()
    output_dir = tmp_path / "finish"
    fake_result: subject.PhaseAResult = {
        "elapsed_seconds": 1.5,
        "output_dir": output_dir.as_posix(),
        "rows_written": 100,
        "category": "jra",
        "year_from": 2020,
        "year_to": 2024,
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
    }
    monkeypatch.setattr(subject, "run_phase_a", MagicMock(return_value=fake_result))
    subject.main(
        [
            "--pg-url",
            "postgresql://u:p@h/db",
            "--running-style-parquet",
            rs_parquet.as_posix(),
            "--output-dir",
            output_dir.as_posix(),
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--year-from",
            "2020",
            "--year-to",
            "2024",
            "--category",
            "jra",
        ]
    )
    captured = capsys.readouterr()
    import json as json_module

    assert json_module.loads(captured.out.strip()) == fake_result
