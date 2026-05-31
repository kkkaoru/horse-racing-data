"""Tests for generate_running_style_features_local (Phase A; TS-SQL delegated)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import generate_running_style_features_local as subject


def test_parse_args_required_fields_pg_url() -> None:
    args = subject.parse_args([
        "--pg-url", "postgres://x",
        "--output-dir", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert args.pg_url == "postgres://x"


def test_parse_args_required_fields_output_dir() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/out-a",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert args.output_dir == "/tmp/out-a"


def test_parse_args_required_fields_feature_version() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v3",
        "--year-from", "2006",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert args.running_style_feature_version == "v3"


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


def test_parse_args_overrides_threads() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2007",
        "--category", "jra",
        "--threads", "12",
    ])
    assert args.threads == 12


def test_parse_args_overrides_memory_limit() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2007",
        "--category", "jra",
        "--memory-limit", "24GB",
    ])
    assert args.memory_limit == "24GB"


def test_parse_args_rejects_invalid_category() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--pg-url", "u",
            "--output-dir", "/tmp",
            "--running-style-feature-version", "v1",
            "--year-from", "2006",
            "--year-to", "2007",
            "--category", "invalid",
        ])


def test_parse_args_rejects_ban_ei_category() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--pg-url", "u",
            "--output-dir", "/tmp",
            "--running-style-feature-version", "v1",
            "--year-from", "2006",
            "--year-to", "2007",
            "--category", "ban-ei",
        ])


def test_parse_args_accepts_jra() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2007",
        "--category", "jra",
    ])
    assert args.category == "jra"


def test_parse_args_accepts_nar() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2006",
        "--year-to", "2007",
        "--category", "nar",
    ])
    assert args.category == "nar"


def test_validate_year_range_accepts_equal() -> None:
    subject.validate_year_range(2020, 2020)


def test_validate_year_range_accepts_ascending() -> None:
    subject.validate_year_range(2006, 2026)


def test_validate_year_range_rejects_descending() -> None:
    with pytest.raises(ValueError, match="year_from"):
        subject.validate_year_range(2026, 2006)


def test_year_to_yyyymmdd_from_pads_year() -> None:
    assert subject.year_to_yyyymmdd_from(2006) == "20060101"


def test_year_to_yyyymmdd_to_pads_year() -> None:
    assert subject.year_to_yyyymmdd_to(2026) == "20261231"


def test_build_print_sql_command_uses_bun_run() -> None:
    command = subject.build_print_sql_command(
        category="jra", from_date="20060101", to_date="20261231", feature_version="v1",
    )
    assert command[0] == "bun"
    assert command[1] == "run"


def test_build_print_sql_command_targets_print_script() -> None:
    command = subject.build_print_sql_command(
        category="jra", from_date="20060101", to_date="20261231", feature_version="v1",
    )
    assert command[2] == (
        "apps/pc-keiba-viewer/src/scripts/finish-position-features/print-running-style-feature-sql.ts"
    )


def test_print_sql_script_is_repo_root_relative_apps_prefix() -> None:
    assert subject.PRINT_SQL_SCRIPT.startswith("apps/")


def test_print_sql_script_constant_matches_repo_root_relative_path() -> None:
    assert subject.PRINT_SQL_SCRIPT == (
        "apps/pc-keiba-viewer/src/scripts/finish-position-features/print-running-style-feature-sql.ts"
    )


def test_build_print_sql_command_passes_source_flag() -> None:
    command = subject.build_print_sql_command(
        category="jra", from_date="20060101", to_date="20261231", feature_version="v1",
    )
    assert "--source" in command
    assert "jra" in command


def test_build_print_sql_command_passes_from_to_dates() -> None:
    command = subject.build_print_sql_command(
        category="nar", from_date="20060101", to_date="20261231", feature_version="v1",
    )
    assert "20060101" in command
    assert "20261231" in command


def test_build_print_sql_command_passes_feature_version() -> None:
    command = subject.build_print_sql_command(
        category="jra", from_date="20060101", to_date="20261231", feature_version="v7",
    )
    assert "--feature-version" in command
    assert "v7" in command


def test_fetch_running_style_sql_returns_stdout() -> None:
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1 AS race_year"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    sql = subject.fetch_running_style_sql(
        category="jra",
        from_date="20060101",
        to_date="20261231",
        feature_version="v1",
        runner=runner,
    )
    assert sql == "SELECT 1 AS race_year"


def test_fetch_running_style_sql_invokes_runner_with_command() -> None:
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    subject.fetch_running_style_sql(
        category="nar",
        from_date="20100101",
        to_date="20201231",
        feature_version="v1",
        runner=runner,
    )
    call_args = runner.call_args
    cmd = call_args.args[0]
    assert cmd[0] == "bun"


def test_fetch_running_style_sql_passes_capture_output_and_text() -> None:
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    subject.fetch_running_style_sql(
        category="jra",
        from_date="20100101",
        to_date="20201231",
        feature_version="v1",
        runner=runner,
    )
    kwargs = runner.call_args.kwargs
    assert kwargs["capture_output"] is True
    assert kwargs["text"] is True


def test_fetch_running_style_sql_raises_on_nonzero_returncode() -> None:
    completed = MagicMock()
    completed.returncode = 1
    completed.stdout = ""
    completed.stderr = "boom"
    runner = MagicMock(return_value=completed)
    with pytest.raises(RuntimeError, match="exited with code 1"):
        subject.fetch_running_style_sql(
            category="jra",
            from_date="20060101",
            to_date="20261231",
            feature_version="v1",
            runner=runner,
        )


def test_fetch_running_style_sql_raises_on_empty_stdout() -> None:
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "   "
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    with pytest.raises(RuntimeError, match="empty stdout"):
        subject.fetch_running_style_sql(
            category="jra",
            from_date="20060101",
            to_date="20261231",
            feature_version="v1",
            runner=runner,
        )


def test_apply_duckdb_resources_sets_threads() -> None:
    con = MagicMock()
    subject.apply_duckdb_resources(con, threads=8, memory_limit="16GB")
    assert con.execute.call_args_list[0].args[0] == "SET threads TO 8"


def test_apply_duckdb_resources_sets_memory_limit() -> None:
    con = MagicMock()
    subject.apply_duckdb_resources(con, threads=8, memory_limit="24GB")
    assert con.execute.call_args_list[1].args[0] == "SET memory_limit = '24GB'"


def test_apply_duckdb_resources_disables_progress_bar() -> None:
    con = MagicMock()
    subject.apply_duckdb_resources(con, threads=8, memory_limit="16GB")
    assert con.execute.call_args_list[2].args[0] == "SET enable_progress_bar_print = false"


def test_apply_duckdb_resources_sets_timezone_to_utc() -> None:
    con = MagicMock()
    subject.apply_duckdb_resources(con, threads=8, memory_limit="16GB")
    assert con.execute.call_args_list[3].args[0] == "SET timezone = 'UTC'"


def test_apply_duckdb_resources_emits_exactly_four_execute_calls() -> None:
    con = MagicMock()
    subject.apply_duckdb_resources(con, threads=8, memory_limit="16GB")
    assert con.execute.call_count == 4


def test_attach_postgres_installs_extension() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_args_list[0].args[0] == "INSTALL postgres"


def test_attach_postgres_loads_extension() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_args_list[1].args[0] == "LOAD postgres"


def test_attach_postgres_attaches_read_only() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_args_list[2].args[0] == (
        "ATTACH 'postgres://abc' AS pg (TYPE postgres, READ_ONLY)"
    )


def test_attach_postgres_switches_to_pg_catalog_after_attach() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_args_list[3].args[0] == "USE pg"


def test_attach_postgres_sets_pg_use_binary_copy_true() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_args_list[4].args[0] == "SET pg_use_binary_copy = true"


def test_attach_postgres_sets_pg_experimental_filter_pushdown_true() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_args_list[5].args[0] == (
        "SET pg_experimental_filter_pushdown = true"
    )


def test_attach_postgres_sets_pg_pages_per_task_to_1000() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_args_list[6].args[0] == "SET pg_pages_per_task = 1000"


def test_attach_postgres_pg_use_binary_copy_after_use_pg() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    issued = [call.args[0] for call in con.execute.call_args_list]
    use_index = issued.index("USE pg")
    pragma_index = issued.index("SET pg_use_binary_copy = true")
    assert use_index < pragma_index


def test_attach_postgres_emits_exactly_seven_execute_calls() -> None:
    con = MagicMock()
    subject.attach_postgres(con, "postgres://abc")
    assert con.execute.call_count == 7


def test_build_hive_copy_sql_starts_with_copy_open_paren() -> None:
    sql = subject.build_hive_copy_sql(select_sql="SELECT 1", output_dir="/tmp/out")
    assert sql.startswith("COPY (")


def test_build_hive_copy_sql_wraps_inner_in_postgres_query() -> None:
    sql = subject.build_hive_copy_sql(select_sql="SELECT 1", output_dir="/tmp/out")
    assert "SELECT * FROM postgres_query('pg', 'SELECT 1')" in sql


def test_build_hive_copy_sql_targets_output_dir() -> None:
    sql = subject.build_hive_copy_sql(select_sql="SELECT 1", output_dir="/tmp/out")
    assert "TO '/tmp/out'" in sql


def test_build_hive_copy_sql_partition_by_race_year() -> None:
    sql = subject.build_hive_copy_sql(select_sql="SELECT 1", output_dir="/tmp/out")
    assert "PARTITION_BY (race_year)" in sql


def test_build_hive_copy_sql_uses_overwrite_or_ignore() -> None:
    sql = subject.build_hive_copy_sql(select_sql="SELECT 1", output_dir="/tmp/out")
    assert "OVERWRITE_OR_IGNORE TRUE" in sql


def test_build_hive_copy_sql_escapes_single_quotes_in_inner_sql() -> None:
    sql = subject.build_hive_copy_sql(
        select_sql="SELECT to_char(now(), 'YYYY')", output_dir="/tmp/out",
    )
    assert "postgres_query('pg', 'SELECT to_char(now(), ''YYYY'')')" in sql


def test_escape_sql_for_postgres_query_doubles_single_quotes() -> None:
    assert subject.escape_sql_for_postgres_query("a'b") == "a''b"


def test_escape_sql_for_postgres_query_strips_surrounding_whitespace() -> None:
    assert subject.escape_sql_for_postgres_query("  SELECT 1  \n") == "SELECT 1"


def test_escape_sql_for_postgres_query_leaves_plain_sql_untouched() -> None:
    assert subject.escape_sql_for_postgres_query("SELECT race_year FROM t") == (
        "SELECT race_year FROM t"
    )


def test_build_postgres_query_subselect_starts_with_select_star() -> None:
    assert subject.build_postgres_query_subselect("SELECT 1").startswith(
        "SELECT * FROM postgres_query(",
    )


def test_build_postgres_query_subselect_uses_pg_attachment_alias() -> None:
    assert "postgres_query('pg'," in subject.build_postgres_query_subselect("SELECT 1")


def test_ensure_output_dir_creates_nested_directory(tmp_path: Path) -> None:
    target = str(tmp_path / "nested" / "dir")
    subject.ensure_output_dir(target)
    assert Path(target).is_dir()


def test_ensure_output_dir_idempotent(tmp_path: Path) -> None:
    target = str(tmp_path / "dir")
    subject.ensure_output_dir(target)
    subject.ensure_output_dir(target)
    assert Path(target).is_dir()


def test_run_calls_ensure_output_dir() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1 AS race_year"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir") as ensure_mock:
        subject.run(args, connect, runner)
    ensure_mock.assert_called_once_with("/tmp/x")


def test_run_opens_in_memory_duckdb() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1 AS race_year"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    connect.assert_called_once_with(":memory:")


def test_run_closes_connection() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1 AS race_year"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    con.close.assert_called_once_with()


def test_run_emits_single_copy_statement() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.foo"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2022",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    copy_calls = [
        call for call in con.execute.call_args_list
        if call.args[0].startswith("COPY (")
    ]
    assert len(copy_calls) == 1


def test_run_propagates_subprocess_error() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 7
    completed.stdout = ""
    completed.stderr = "fail"
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        with pytest.raises(RuntimeError, match="exited with code 7"):
            subject.run(args, connect, runner)


def test_run_validates_year_range_before_subprocess() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    runner = MagicMock()
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2020",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        with pytest.raises(ValueError, match="year_from"):
            subject.run(args, connect, runner)
    runner.assert_not_called()
    connect.assert_not_called()


def test_run_executes_set_threads_and_memory_limit() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1 AS race_year"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
        "--threads", "16",
        "--memory-limit", "32GB",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    issued = [call.args[0] for call in con.execute.call_args_list]
    assert "SET threads TO 16" in issued
    assert "SET memory_limit = '32GB'" in issued


def test_run_executes_attach_postgres_with_pg_url() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1 AS race_year"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    issued = [call.args[0] for call in con.execute.call_args_list]
    assert "ATTACH 'postgres://u' AS pg (TYPE postgres, READ_ONLY)" in issued


def test_run_emits_pg_pragma_settings_after_use_pg() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM race_entry_corner_features"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    issued = [call.args[0] for call in con.execute.call_args_list]
    assert "SET pg_use_binary_copy = true" in issued
    assert "SET pg_experimental_filter_pushdown = true" in issued
    assert "SET pg_pages_per_task = 1000" in issued


def test_run_emits_timezone_utc_setting() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM race_entry_corner_features"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    issued = [call.args[0] for call in con.execute.call_args_list]
    assert "SET timezone = 'UTC'" in issued


def test_run_issues_use_pg_after_attach_for_catalog_lookup() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM race_entry_corner_features"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    issued = [call.args[0] for call in con.execute.call_args_list]
    attach_index = issued.index("ATTACH 'postgres://u' AS pg (TYPE postgres, READ_ONLY)")
    use_index = issued.index("USE pg")
    copy_index = next(idx for idx, sql in enumerate(issued) if sql.startswith("COPY ("))
    assert attach_index < use_index < copy_index


def test_run_passes_ts_sql_into_copy_wrapper() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.race_entry_corner_features"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    copy_call = next(
        call for call in con.execute.call_args_list if call.args[0].startswith("COPY (")
    )
    assert "SELECT race_year FROM pg.race_entry_corner_features" in copy_call.args[0]


def test_run_wraps_ts_sql_in_postgres_query_call() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT to_char(now(), 'YYYY')"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    copy_call = next(
        call for call in con.execute.call_args_list if call.args[0].startswith("COPY (")
    )
    assert "postgres_query('pg', 'SELECT to_char(now(), ''YYYY'')')" in copy_call.args[0]


def test_run_closes_connection_even_on_execute_failure() -> None:
    con = MagicMock()
    con.execute.side_effect = RuntimeError("duckdb boom")
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT 1 AS race_year"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2020",
        "--year-to", "2021",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        with pytest.raises(RuntimeError, match="duckdb boom"):
            subject.run(args, connect, runner)
    con.close.assert_called_once_with()


def test_main_uses_duckdb_module() -> None:
    fake_duckdb = MagicMock()
    fake_con = MagicMock()
    fake_duckdb.connect.return_value = fake_con
    fake_completed = MagicMock()
    fake_completed.returncode = 0
    fake_completed.stdout = "SELECT 1 AS race_year"
    fake_completed.stderr = ""
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
            with patch("generate_running_style_features_local.subprocess.run", return_value=fake_completed):
                subject.main(argv)
    fake_duckdb.connect.assert_called_once_with(":memory:")


def test_main_passes_subprocess_run_as_runner() -> None:
    fake_duckdb = MagicMock()
    fake_con = MagicMock()
    fake_duckdb.connect.return_value = fake_con
    fake_completed = MagicMock()
    fake_completed.returncode = 0
    fake_completed.stdout = "SELECT 1 AS race_year"
    fake_completed.stderr = ""
    argv = [
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/y",
        "--running-style-feature-version", "v1",
        "--year-from", "2025",
        "--year-to", "2025",
        "--category", "jra",
    ]
    with patch.dict("sys.modules", {"duckdb": fake_duckdb}):
        with patch.object(subject, "ensure_output_dir"):
            with patch("generate_running_style_features_local.subprocess.run", return_value=fake_completed) as run_mock:
                subject.main(argv)
    run_mock.assert_called_once()


def test_parse_args_month_from_defaults_to_none() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert args.month_from is None


def test_parse_args_month_to_defaults_to_none() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert args.month_to is None


def test_parse_args_accepts_month_from_january() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
        "--month-from", "1",
    ])
    assert args.month_from == 1


def test_parse_args_accepts_month_to_december() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
        "--month-to", "12",
    ])
    assert args.month_to == 12


def test_parse_args_accepts_mid_month_pair() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
        "--month-from", "6",
        "--month-to", "7",
    ])
    assert args.month_from == 6
    assert args.month_to == 7


def test_parse_args_rejects_month_from_below_one() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--pg-url", "u",
            "--output-dir", "/tmp",
            "--running-style-feature-version", "v1",
            "--year-from", "2026",
            "--year-to", "2026",
            "--category", "jra",
            "--month-from", "0",
        ])


def test_parse_args_rejects_month_to_above_twelve() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--pg-url", "u",
            "--output-dir", "/tmp",
            "--running-style-feature-version", "v1",
            "--year-from", "2026",
            "--year-to", "2026",
            "--category", "jra",
            "--month-to", "13",
        ])


def test_resolve_month_from_defaults_to_one_when_none() -> None:
    assert subject.resolve_month_from(None) == 1


def test_resolve_month_from_returns_supplied_value() -> None:
    assert subject.resolve_month_from(4) == 4


def test_resolve_month_to_defaults_to_twelve_when_none() -> None:
    assert subject.resolve_month_to(None) == 12


def test_resolve_month_to_returns_supplied_value() -> None:
    assert subject.resolve_month_to(8) == 8


def test_build_from_date_defaults_month_to_january() -> None:
    assert subject.build_from_date(2026, None) == "20260101"


def test_build_from_date_uses_supplied_month() -> None:
    assert subject.build_from_date(2026, 5) == "20260501"


def test_build_from_date_zero_pads_single_digit_month() -> None:
    assert subject.build_from_date(2026, 3) == "20260301"


def test_build_to_date_defaults_month_to_december_with_31_days() -> None:
    assert subject.build_to_date(2026, None) == "20261231"


def test_build_to_date_uses_calendar_last_day_for_april() -> None:
    assert subject.build_to_date(2026, 4) == "20260430"


def test_build_to_date_returns_28_for_february_non_leap_year() -> None:
    assert subject.build_to_date(2025, 2) == "20250228"


def test_build_to_date_returns_29_for_february_leap_year() -> None:
    assert subject.build_to_date(2024, 2) == "20240229"


def test_build_from_to_dates_full_year_when_months_omitted() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert subject.build_from_to_dates(args) == ("20260101", "20261231")


def test_build_from_to_dates_one_month_chunk_for_february_leap() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    assert subject.build_from_to_dates(args) == ("20240201", "20240229")


def test_build_from_to_dates_mid_month_pair() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
        "--month-from", "3",
        "--month-to", "4",
    ])
    assert subject.build_from_to_dates(args) == ("20260301", "20260430")


def test_run_passes_month_chunk_dates_to_print_sql() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.t"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    cmd = runner.call_args.args[0]
    assert "20240201" in cmd
    assert "20240229" in cmd


def test_run_passes_full_year_dates_when_months_omitted() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.t"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    cmd = runner.call_args.args[0]
    assert "20260101" in cmd
    assert "20261231" in cmd


def test_is_month_chunk_true_when_both_months_set() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    assert subject.is_month_chunk(args) is True


def test_is_month_chunk_false_when_months_omitted() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    assert subject.is_month_chunk(args) is False


def test_is_month_chunk_false_when_only_month_from() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
        "--month-from", "3",
    ])
    assert subject.is_month_chunk(args) is False


def test_is_month_chunk_false_when_only_month_to() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
        "--month-to", "5",
    ])
    assert subject.is_month_chunk(args) is False


def test_build_month_chunk_output_path_single_month() -> None:
    assert subject.build_month_chunk_output_path(
        output_dir="/tmp/out", year=2024, month_from=2, month_to=2,
    ) == "/tmp/out/race_year=2024/data_2024_02_02.parquet"


def test_build_month_chunk_output_path_zero_pads_month() -> None:
    assert subject.build_month_chunk_output_path(
        output_dir="/tmp/out", year=2006, month_from=1, month_to=1,
    ) == "/tmp/out/race_year=2006/data_2006_01_01.parquet"


def test_build_month_chunk_output_path_december() -> None:
    assert subject.build_month_chunk_output_path(
        output_dir="/tmp/out", year=2026, month_from=12, month_to=12,
    ) == "/tmp/out/race_year=2026/data_2026_12_12.parquet"


def test_build_month_chunk_output_path_multi_month_range() -> None:
    assert subject.build_month_chunk_output_path(
        output_dir="/tmp/out", year=2024, month_from=3, month_to=4,
    ) == "/tmp/out/race_year=2024/data_2024_03_04.parquet"


def test_build_month_chunk_copy_sql_starts_with_copy_open_paren() -> None:
    sql = subject.build_month_chunk_copy_sql(
        select_sql="SELECT 1",
        output_path="/tmp/out/race_year=2024/data_2024_02_02.parquet",
    )
    assert sql.startswith("COPY (")


def test_build_month_chunk_copy_sql_targets_output_path() -> None:
    sql = subject.build_month_chunk_copy_sql(
        select_sql="SELECT 1",
        output_path="/tmp/out/race_year=2024/data_2024_02_02.parquet",
    )
    assert "TO '/tmp/out/race_year=2024/data_2024_02_02.parquet'" in sql


def test_build_month_chunk_copy_sql_has_no_partition_by() -> None:
    sql = subject.build_month_chunk_copy_sql(
        select_sql="SELECT 1",
        output_path="/tmp/out/race_year=2024/data_2024_02_02.parquet",
    )
    assert "PARTITION_BY" not in sql


def test_build_month_chunk_copy_sql_has_no_overwrite_or_ignore() -> None:
    sql = subject.build_month_chunk_copy_sql(
        select_sql="SELECT 1",
        output_path="/tmp/out/race_year=2024/data_2024_02_02.parquet",
    )
    assert "OVERWRITE_OR_IGNORE" not in sql


def test_build_month_chunk_copy_sql_uses_zstd_compression() -> None:
    sql = subject.build_month_chunk_copy_sql(
        select_sql="SELECT 1",
        output_path="/tmp/out/race_year=2024/data_2024_02_02.parquet",
    )
    assert "COMPRESSION ZSTD" in sql


def test_build_month_chunk_copy_sql_wraps_inner_in_postgres_query() -> None:
    sql = subject.build_month_chunk_copy_sql(
        select_sql="SELECT 1",
        output_path="/tmp/out/race_year=2024/data_2024_02_02.parquet",
    )
    assert "SELECT * FROM postgres_query('pg', 'SELECT 1')" in sql


def test_build_copy_sql_for_args_year_chunk_uses_hive_partition() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    sql = subject.build_copy_sql_for_args(select_sql="SELECT 1", args=args)
    assert "PARTITION_BY (race_year)" in sql


def test_build_copy_sql_for_args_year_chunk_uses_overwrite_or_ignore() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    sql = subject.build_copy_sql_for_args(select_sql="SELECT 1", args=args)
    assert "OVERWRITE_OR_IGNORE TRUE" in sql


def test_build_copy_sql_for_args_month_chunk_no_partition_by() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    sql = subject.build_copy_sql_for_args(select_sql="SELECT 1", args=args)
    assert "PARTITION_BY" not in sql


def test_build_copy_sql_for_args_month_chunk_targets_per_month_file() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    sql = subject.build_copy_sql_for_args(select_sql="SELECT 1", args=args)
    assert "TO '/tmp/out/race_year=2024/data_2024_02_02.parquet'" in sql


def test_build_month_chunk_race_year_dir_joins_with_race_year_prefix() -> None:
    assert subject.build_month_chunk_race_year_dir("/tmp/out", 2024) == (
        "/tmp/out/race_year=2024"
    )


def test_ensure_output_parents_for_args_year_chunk_creates_only_output_dir() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/y",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir") as ensure_mock:
        subject.ensure_output_parents_for_args(args)
    ensure_mock.assert_called_once_with("/tmp/y")


def test_ensure_output_parents_for_args_month_chunk_creates_race_year_subdir() -> None:
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/y",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    with patch.object(subject, "ensure_output_dir") as ensure_mock:
        subject.ensure_output_parents_for_args(args)
    calls = [call.args[0] for call in ensure_mock.call_args_list]
    assert calls == ["/tmp/y", "/tmp/y/race_year=2024"]


def test_execute_copy_for_args_year_chunk_emits_partition_by_sql() -> None:
    con = MagicMock()
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/y",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    subject.execute_copy_for_args(con, select_sql="SELECT 1", args=args)
    issued = con.execute.call_args.args[0]
    assert "PARTITION_BY (race_year)" in issued


def test_execute_copy_for_args_month_chunk_emits_per_month_file_sql() -> None:
    con = MagicMock()
    args = subject.parse_args([
        "--pg-url", "u",
        "--output-dir", "/tmp/y",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "5",
        "--month-to", "5",
    ])
    subject.execute_copy_for_args(con, select_sql="SELECT 1", args=args)
    issued = con.execute.call_args.args[0]
    assert "TO '/tmp/y/race_year=2024/data_2024_05_05.parquet'" in issued


def test_run_month_chunk_writes_to_per_month_file_path() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.t"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    copy_call = next(
        call for call in con.execute.call_args_list if call.args[0].startswith("COPY (")
    )
    assert "TO '/tmp/x/race_year=2024/data_2024_02_02.parquet'" in copy_call.args[0]


def test_run_month_chunk_has_no_partition_by_in_copy_sql() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.t"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    copy_call = next(
        call for call in con.execute.call_args_list if call.args[0].startswith("COPY (")
    )
    assert "PARTITION_BY" not in copy_call.args[0]


def test_run_month_chunk_creates_race_year_subdir() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.t"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "2",
        "--month-to", "2",
    ])
    with patch.object(subject, "ensure_output_dir") as ensure_mock:
        subject.run(args, connect, runner)
    calls = [call.args[0] for call in ensure_mock.call_args_list]
    assert calls == ["/tmp/x", "/tmp/x/race_year=2024"]


def test_run_year_chunk_legacy_still_uses_partition_by_sql() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.t"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2026",
        "--year-to", "2026",
        "--category", "jra",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    copy_call = next(
        call for call in con.execute.call_args_list if call.args[0].startswith("COPY (")
    )
    assert "PARTITION_BY (race_year)" in copy_call.args[0]
    assert "OVERWRITE_OR_IGNORE TRUE" in copy_call.args[0]


def test_run_month_chunk_uses_zstd_compression() -> None:
    con = MagicMock()
    connect = MagicMock(return_value=con)
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = "SELECT race_year FROM pg.t"
    completed.stderr = ""
    runner = MagicMock(return_value=completed)
    args = subject.parse_args([
        "--pg-url", "postgres://u",
        "--output-dir", "/tmp/x",
        "--running-style-feature-version", "v1",
        "--year-from", "2024",
        "--year-to", "2024",
        "--category", "jra",
        "--month-from", "7",
        "--month-to", "7",
    ])
    with patch.object(subject, "ensure_output_dir"):
        subject.run(args, connect, runner)
    copy_call = next(
        call for call in con.execute.call_args_list if call.args[0].startswith("COPY (")
    )
    assert "COMPRESSION ZSTD" in copy_call.args[0]
