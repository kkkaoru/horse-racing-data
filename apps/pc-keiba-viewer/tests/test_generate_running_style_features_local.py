"""Tests for generate_running_style_features_local (Phase A)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import generate_running_style_features_local as subject


def test_parse_args_required_fields() -> None:
    args = subject.parse_args([
        "--pg-url", "postgres://x",
        "--output-dir", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert args.pg_url == "postgres://x"


def test_parse_args_year_range_parsed_as_int() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2010",
        "--year-to", "2020",
        "--category", "nar",
    ])
    assert args.year_from == 2010
    assert args.year_to == 2020


def test_parse_args_default_threads_is_eight() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2007",
        "--category", "jra",
    ])
    assert args.threads == 8


def test_parse_args_default_memory_limit_is_16gb() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2007",
        "--category", "jra",
    ])
    assert args.memory_limit == "16GB"


def test_parse_args_invalid_category_raises() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--pg-url", "u",
            "--output-dir", "/tmp",
            "--running-style-feature-version", "v1",
            "--year-from", "2006",
            "--year-to", "2007",
            "--category", "invalid",
        ])


def test_apply_duckdb_resources_sets_threads_memory_temp() -> None:
    con = MagicMock()
    subject.apply_duckdb_resources(
        con, threads=8, memory_limit="16GB", temp_dir_limit="200GB",
    )
    assert con.execute.call_args_list[0].args[0] == "SET threads TO 8"
    assert con.execute.call_args_list[1].args[0] == "SET memory_limit = '16GB'"
    assert con.execute.call_args_list[2].args[0] == "SET max_temp_directory_size = '200GB'"


def test_attach_postgres_runs_install_load_attach_and_session_sets() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    calls = [call.args[0] for call in con.execute.call_args_list]
    assert calls[0] == "INSTALL postgres"
    assert calls[1] == "LOAD postgres"
    assert calls[2] == "ATTACH 'postgres://abc' AS pg (TYPE postgres, READ_ONLY)"


def test_build_source_filter_for_ban_ei_uses_keibajo_83() -> None:
    assert subject.build_source_filter_sql("ban-ei") == "se.keibajo_code = '83'"


def test_build_source_filter_for_nar_uses_source_nar() -> None:
    assert subject.build_source_filter_sql("nar") == "se.source = 'nar'"


def test_build_source_filter_for_jra_excludes_ban_ei() -> None:
    assert (
        subject.build_source_filter_sql("jra")
        == "se.source = 'jra' AND se.keibajo_code <> '83'"
    )


def test_build_se_table_name_jra_is_jvd_se() -> None:
    assert subject.build_se_table_name("jra") == "jvd_se"


def test_build_se_table_name_nar_is_nvd_se() -> None:
    assert subject.build_se_table_name("nar") == "nvd_se"


def test_build_se_table_name_ban_ei_is_nvd_se() -> None:
    assert subject.build_se_table_name("ban-ei") == "nvd_se"


def test_build_ra_table_name_jra_is_jvd_ra() -> None:
    assert subject.build_ra_table_name("jra") == "jvd_ra"


def test_build_ra_table_name_nar_is_nvd_ra() -> None:
    assert subject.build_ra_table_name("nar") == "nvd_ra"


def test_build_year_feature_query_embeds_year_and_version() -> None:
    sql = subject.build_year_feature_query(
        category="jra", year=2026, feature_version="v1",
    )
    assert "se.kaisai_nen = '2026'" in sql
    assert "CAST('v1' AS VARCHAR) AS running_style_feature_version" in sql


def test_build_year_feature_query_embeds_category_literal() -> None:
    sql = subject.build_year_feature_query(
        category="ban-ei", year=2020, feature_version="v1",
    )
    assert "CAST('ban-ei' AS VARCHAR) AS category" in sql


def test_build_hive_copy_sql_emits_partition_by() -> None:
    sql = subject.build_hive_copy_sql(
        select_sql="SELECT 1", output_dir="/tmp/out",
    )
    assert "PARTITION_BY (category, race_year)" in sql


def test_build_hive_copy_sql_uses_overwrite_or_ignore() -> None:
    sql = subject.build_hive_copy_sql(
        select_sql="SELECT 1", output_dir="/tmp/out",
    )
    assert "OVERWRITE_OR_IGNORE TRUE" in sql


def test_export_year_features_calls_con_execute_with_copy() -> None:
    con = MagicMock()
    subject.export_year_features(
        con=con,
        category="jra",
        year=2026,
        output_dir="/tmp/out",
        feature_version="v1",
    )
    executed_sql = con.execute.call_args.args[0]
    assert executed_sql.startswith("COPY (")


def test_ensure_output_dir_calls_mkdir_with_parents(tmp_path: Path) -> None:
    target = str(tmp_path / "nested" / "dir")
    subject.ensure_output_dir(target)
    assert Path(target).is_dir()


def test_run_orchestrates_year_loop_and_closes_connection() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir") as ensure_mock:
        subject.run(args, connect)
    ensure_mock.assert_called_once_with("/tmp/x")
    connect.assert_called_once_with(":memory:")
    con.close.assert_called_once_with()


def test_run_emits_one_copy_per_year() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2022",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect)
    copy_calls = [
        call for call in con.execute.call_args_list
        if call.args[0].startswith("COPY (")
    ]
    assert len(copy_calls) == 3


def test_main_uses_duckdb_module() -> None:
    fake_duckdb = MagicMock()
    fake_con = MagicMock()
    fake_duckdb.connect.return_value = fake_con
    argv = [
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/y",
        "--running-style-feature-version", "v1",
        "--year-from", "2025",
        "--year-to", "2025",
        "--category", "nar",
    ]
    with patch.dict("sys.modules", {"duckdb": fake_duckdb}):
        with patch.object(subject, "ensure_output_dir"):
            subject.main(argv)
    fake_duckdb.connect.assert_called_once_with(":memory:")
