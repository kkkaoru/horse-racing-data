from __future__ import annotations

import io
import json
import sys
from typing import IO, cast
from unittest.mock import MagicMock

import pytest

import load_bucket_predictions as subject


def _build_argv(**overrides: str) -> list[str]:
    defaults: dict[str, str] = {
        "--pg-url": "postgresql://u:p@h/db",
        "--predictions-parquet-glob": "tmp/bucket-eval/finish-position/v1/predictions/**/*.parquet",
        "--temp-table-name": "tmp_bucket_eval_finish_position_predictions",
        "--running-style-feature-version": "v1",
        "--finish-position-version": "v1",
        "--category": "jra",
        "--year-from": "2004",
        "--year-to": "2025",
    }
    defaults.update(overrides)
    argv: list[str] = []
    for key, value in defaults.items():
        argv.append(key)
        argv.append(value)
    return argv


def _sample_args(**overrides: object) -> subject.LoadArguments:
    base: dict[str, object] = {
        "pg_url": "postgresql://u:p@h/db",
        "predictions_parquet_glob": "tmp/preds/**/*.parquet",
        "temp_table_name": "tmp_bucket_eval_finish_position_predictions",
        "running_style_feature_version": "v1",
        "finish_position_version": "v1",
        "category": "jra",
        "year_from": 2024,
        "year_to": 2024,
    }
    base.update(overrides)
    return cast(subject.LoadArguments, base)


def test_parse_args_full_set():
    args = subject.parse_args(_build_argv())
    assert args.pg_url == "postgresql://u:p@h/db"
    assert (
        args.predictions_parquet_glob
        == "tmp/bucket-eval/finish-position/v1/predictions/**/*.parquet"
    )
    assert args.temp_table_name == "tmp_bucket_eval_finish_position_predictions"
    assert args.running_style_feature_version == "v1"
    assert args.finish_position_version == "v1"
    assert args.category == "jra"
    assert args.year_from == 2004
    assert args.year_to == 2025


def test_parse_args_defaults_temp_table_name():
    argv = _build_argv()
    idx = argv.index("--temp-table-name")
    del argv[idx : idx + 2]
    args = subject.parse_args(argv)
    assert args.temp_table_name == subject.DEFAULT_TEMP_TABLE


def test_parse_args_rejects_unknown_category():
    with pytest.raises(SystemExit):
        subject.parse_args(_build_argv(**{"--category": "bogus"}))


def test_parse_args_requires_predictions_parquet_glob():
    argv = _build_argv()
    idx = argv.index("--predictions-parquet-glob")
    del argv[idx : idx + 2]
    with pytest.raises(SystemExit):
        subject.parse_args(argv)


def test_normalize_arguments_converts_years_to_int():
    args = subject.parse_args(_build_argv())
    normalized = subject.normalize_arguments(args)
    assert normalized["year_from"] == 2004
    assert normalized["year_to"] == 2025
    assert normalized["category"] == "jra"
    assert normalized["temp_table_name"] == "tmp_bucket_eval_finish_position_predictions"


def test_assert_safe_identifier_accepts_snake_case():
    assert subject.assert_safe_identifier("tmp_bucket_eval_123") == "tmp_bucket_eval_123"


def test_assert_safe_identifier_rejects_empty():
    with pytest.raises(ValueError):
        subject.assert_safe_identifier("")


def test_assert_safe_identifier_rejects_semicolon():
    with pytest.raises(ValueError):
        subject.assert_safe_identifier("tmp; DROP TABLE x")


def test_sql_quote_literal_escapes_single_quote():
    assert subject.sql_quote_literal("o'malley") == "o''malley"


def test_build_select_from_parquet_sql_filters_versions_and_years():
    args = _sample_args(
        predictions_parquet_glob="tmp/preds/**/*.parquet",
        running_style_feature_version="rsX",
        finish_position_version="fpY",
        year_from=2010,
        year_to=2025,
    )
    sql = subject.build_select_from_parquet_sql(args)
    assert "read_parquet('tmp/preds/**/*.parquet', hive_partitioning=1)" in sql
    assert "category = 'jra'" in sql
    assert "between 2010 and 2025" in sql
    assert "finish_position_version = 'fpY'" in sql
    assert "running_style_feature_version = 'rsX'" in sql


def test_build_version_mismatch_check_sql_counts_mismatched_rows():
    args = _sample_args(
        running_style_feature_version="rsX",
        finish_position_version="fpY",
        category="nar",
        year_from=2020,
        year_to=2024,
    )
    sql = subject.build_version_mismatch_check_sql(args)
    assert "select count(*) from read_parquet" in sql
    assert "finish_position_version <> 'fpY'" in sql
    assert "running_style_feature_version <> 'rsX'" in sql
    assert "category = 'nar'" in sql


def test_build_create_temp_table_sql_includes_all_columns_and_on_commit_drop():
    sql = subject.build_create_temp_table_sql("tmp_bucket_eval_xyz")
    assert sql.startswith("CREATE TEMP TABLE tmp_bucket_eval_xyz (")
    assert "source text" in sql
    assert "kaisai_nen text" in sql
    assert "kaisai_tsukihi text" in sql
    assert "keibajo_code text" in sql
    assert "race_bango text" in sql
    assert "ketto_toroku_bango text" in sql
    assert "predicted_score numeric" in sql
    assert "predicted_rank integer" in sql
    assert "predicted_top1_prob numeric" in sql
    assert "predicted_top3_prob numeric" in sql
    assert "predicted_finish_position integer" in sql
    assert "model_version text" in sql
    assert "running_style_feature_version text" in sql
    assert "finish_position_version text" in sql
    assert sql.endswith(") ON COMMIT DROP")


def test_build_create_temp_table_sql_rejects_unsafe_name():
    with pytest.raises(ValueError):
        subject.build_create_temp_table_sql("tmp;DROP")


def test_build_copy_sql_uses_csv_format_and_explicit_columns():
    sql = subject.build_copy_sql("tmp_bucket_eval_xyz")
    assert sql.startswith("COPY tmp_bucket_eval_xyz (")
    assert "FROM STDIN WITH (FORMAT CSV)" in sql
    assert "source" in sql
    assert "finish_position_version" in sql


def test_build_copy_sql_rejects_unsafe_name():
    with pytest.raises(ValueError):
        subject.build_copy_sql("bad name")


def test_csv_encode_field_handles_none_returns_empty():
    assert subject.csv_encode_field(None) == ""


def test_csv_encode_field_handles_true_returns_true_literal():
    assert subject.csv_encode_field(True) == "true"


def test_csv_encode_field_handles_false_returns_false_literal():
    assert subject.csv_encode_field(False) == "false"


def test_csv_encode_field_handles_int_returns_str():
    assert subject.csv_encode_field(42) == "42"


def test_csv_encode_field_handles_float_returns_repr():
    assert subject.csv_encode_field(0.1) == repr(0.1)


def test_csv_encode_field_quotes_field_containing_comma():
    assert subject.csv_encode_field("a,b") == '"a,b"'


def test_csv_encode_field_escapes_embedded_double_quote():
    assert subject.csv_encode_field('a"b') == '"a""b"'


def test_csv_encode_field_quotes_field_containing_newline():
    assert subject.csv_encode_field("a\nb") == '"a\nb"'


def test_csv_encode_field_quotes_field_containing_carriage_return():
    assert subject.csv_encode_field("a\rb") == '"a\rb"'


def test_csv_encode_field_plain_text_unchanged():
    assert subject.csv_encode_field("hello") == "hello"


def test_csv_encode_field_falls_back_to_str_for_unknown_type():
    from typing import override

    class Custom:
        @override
        def __str__(self) -> str:
            return "custom-obj"

    assert subject.csv_encode_field(Custom()) == "custom-obj"


def test_csv_encode_row_joins_fields_with_comma():
    assert subject.csv_encode_row(("a", 1, None)) == "a,1,"


def test_stream_csv_rows_joins_rows_with_newline():
    rows: list[tuple[object, ...]] = [("a", 1, None), ("b,c", 2, 3.5)]
    csv = subject.stream_csv_rows(rows)
    assert csv == "a,1,\n" + '"b,c",2,' + repr(3.5) + "\n"


def test_serve_sql_rpc_emits_ready_then_closes_on_exit():
    stdin = io.StringIO('{"type":"exit"}\n')
    stdout = io.StringIO()
    cursor = MagicMock()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 4)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert lines[0] == {"type": "ready", "loadedRows": 4}
    assert lines[1] == {"type": "closed"}
    cursor.execute.assert_not_called()


def test_serve_sql_rpc_returns_on_empty_stdin_eof():
    stdin = io.StringIO("")
    stdout = io.StringIO()
    cursor = MagicMock()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert lines == [{"type": "ready", "loadedRows": 0}]


def test_serve_sql_rpc_handles_select_with_chunked_rows():
    cursor = MagicMock()
    description_col = MagicMock()
    description_col.name = "value"
    cursor.description = [description_col]
    long_rows = [(index,) for index in range(2500)]
    cursor.fetchall.return_value = long_rows
    request = json.dumps(
        {"type": "sql", "id": "q1", "query": "select 1", "params": []}
    )
    stdin = io.StringIO(request + "\n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert lines[0] == {"type": "ready", "loadedRows": 0}
    rows_responses = [line for line in lines if line["type"] == "rows"]
    assert len(rows_responses) == 3
    assert rows_responses[0]["seq"] == 0
    assert rows_responses[0]["done"] is False
    assert rows_responses[2]["seq"] == 2
    assert rows_responses[2]["done"] is True
    assert rows_responses[0]["rows"][0] == {"value": 0}
    assert lines[-1] == {"type": "closed"}


def test_serve_sql_rpc_emits_ok_for_non_select_statement():
    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 9
    request = json.dumps(
        {"type": "sql", "id": "u1", "query": "update x set y = 1", "params": []}
    )
    stdin = io.StringIO(request + "\n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    ok_response = [line for line in lines if line["type"] == "ok"][0]
    assert ok_response == {"type": "ok", "id": "u1", "rowcount": 9}


def test_serve_sql_rpc_emits_error_for_invalid_request_type():
    cursor = MagicMock()
    bad_request = json.dumps({"type": "bogus", "id": "x"})
    stdin = io.StringIO(bad_request + "\n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    error_response = [line for line in lines if line["type"] == "error"][0]
    assert error_response["id"] == "x"
    assert "Unknown RPC request type" in error_response["message"]


def test_serve_sql_rpc_passes_params_to_cursor_execute():
    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 1
    request = json.dumps(
        {"type": "sql", "id": "q2", "query": "insert into x values ($1)", "params": [42]}
    )
    stdin = io.StringIO(request + "\n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    cursor.execute.assert_called_once_with("insert into x values ($1)", [42])


def test_serve_sql_rpc_handles_copy_request():
    copy_stream = MagicMock()
    copy_ctx = MagicMock()
    copy_ctx.__enter__.return_value = copy_stream
    copy_ctx.__exit__.return_value = False
    cursor = MagicMock()
    cursor.copy.return_value = copy_ctx
    cursor.rowcount = 2
    request = json.dumps(
        {"type": "copy", "id": "c1", "query": "COPY tmp FROM STDIN", "csv": "a,1\n"}
    )
    stdin = io.StringIO(request + "\n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    copy_stream.write.assert_called_once_with("a,1\n")
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    ok = [line for line in lines if line["type"] == "ok"][0]
    assert ok == {"type": "ok", "id": "c1", "rowcount": 2}


def test_serve_sql_rpc_skips_blank_lines():
    cursor = MagicMock()
    stdin = io.StringIO("\n   \n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert lines[-1] == {"type": "closed"}


def test_serve_sql_rpc_reports_error_with_null_id_when_request_unparsable():
    cursor = MagicMock()
    stdin = io.StringIO("not-json\n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    error_response = [line for line in lines if line["type"] == "error"][0]
    assert error_response["id"] is None
    assert "Expecting value" in error_response["message"] or "json" in error_response["message"].lower()


def test_chunk_rows_for_response_returns_one_empty_chunk_for_empty_rows():
    assert subject.chunk_rows_for_response([]) == [[]]


def test_chunk_rows_for_response_splits_at_rpc_rows_chunk_size():
    rows: list[tuple[object, ...]] = [(index,) for index in range(2500)]
    chunks = subject.chunk_rows_for_response(rows)
    assert len(chunks) == 3
    assert len(chunks[0]) == subject.RPC_ROWS_CHUNK_SIZE
    assert len(chunks[2]) == 500


def test_row_to_jsonable_returns_list_when_description_none():
    assert subject.row_to_jsonable((1, "a"), None) == [1, "a"]


def test_row_to_jsonable_returns_dict_when_description_tuple_pairs():
    columns: list[object] = [("col_a",), ("col_b",)]
    assert subject.row_to_jsonable((1, "a"), columns) == {"col_a": 1, "col_b": "a"}


def test_row_to_jsonable_returns_dict_when_description_objects_with_name():
    column_a = MagicMock()
    column_a.name = "col_a"
    column_b = MagicMock()
    column_b.name = "col_b"
    columns: list[object] = [column_a, column_b]
    assert subject.row_to_jsonable((1, "a"), columns) == {"col_a": 1, "col_b": "a"}


def test_parse_rpc_line_returns_none_for_blank_line():
    assert subject.parse_rpc_line("   \n") is None


def test_parse_rpc_line_rejects_non_object_json():
    with pytest.raises(ValueError):
        subject.parse_rpc_line('"oops"')


def test_dispatch_rpc_request_rejects_unknown_type():
    with pytest.raises(ValueError):
        subject.dispatch_rpc_request(
            {"type": "weird", "id": "x"}, MagicMock(), cast(IO[str], io.StringIO())
        )


def test_report_rpc_error_uses_null_id_when_request_is_none():
    stdout = io.StringIO()
    subject.report_rpc_error(cast(IO[str], stdout), None, ValueError("boom"))
    payload = json.loads(stdout.getvalue().strip())
    assert payload == {
        "type": "error",
        "id": None,
        "message": "boom",
        "details": "ValueError",
    }


def test_report_rpc_error_uses_null_id_when_request_id_is_none():
    stdout = io.StringIO()
    subject.report_rpc_error(cast(IO[str], stdout), {"id": None}, RuntimeError("x"))
    payload = json.loads(stdout.getvalue().strip())
    assert payload["id"] is None


def test_report_rpc_error_uses_id_when_present():
    stdout = io.StringIO()
    subject.report_rpc_error(cast(IO[str], stdout), {"id": "abc"}, RuntimeError("x"))
    payload = json.loads(stdout.getvalue().strip())
    assert payload["id"] == "abc"


def test_write_response_appends_newline_and_flushes():
    stdout = io.StringIO()
    subject.write_response(cast(IO[str], stdout), {"type": "ready", "loadedRows": 0})
    assert stdout.getvalue() == '{"type": "ready", "loadedRows": 0}\n'


def test_load_predictions_into_temp_table_invokes_copy_and_returns_row_count():
    rows: list[tuple[object, ...]] = [
        ("jra", "2024", "0101", "05", "01", "a1", 0.9, 1, 0.5, 0.8, 1, "m7", "v1", "v1"),
        ("jra", "2024", "0101", "05", "01", "a2", 0.6, 2, 0.3, 0.7, 2, "m7", "v1", "v1"),
    ]
    read_predictions = MagicMock(return_value=(0, rows))
    copy_into_pg = MagicMock()
    args = _sample_args()
    stdout = io.StringIO()
    stdin = io.StringIO('{"type":"exit"}\n')
    loaded = subject.load_predictions_into_temp_table(
        args,
        read_predictions=read_predictions,
        copy_into_pg=copy_into_pg,
        stdout=cast(IO[str], stdout),
        stdin=cast(IO[str], stdin),
    )
    assert loaded == 2
    read_predictions.assert_called_once_with(args)
    copy_into_pg.assert_called_once()
    passed_args, passed_rows, passed_stdout, passed_stdin = copy_into_pg.call_args.args
    assert passed_args == args
    assert passed_rows == rows
    assert passed_stdout is stdout
    assert passed_stdin is stdin


def test_load_predictions_into_temp_table_aborts_on_version_mismatch():
    read_predictions = MagicMock(return_value=(3, []))
    copy_into_pg = MagicMock()
    args = _sample_args()
    with pytest.raises(RuntimeError) as info:
        subject.load_predictions_into_temp_table(
            args,
            read_predictions=read_predictions,
            copy_into_pg=copy_into_pg,
            stdout=cast(IO[str], io.StringIO()),
            stdin=cast(IO[str], io.StringIO('{"type":"exit"}\n')),
        )
    assert "3 mismatched rows" in str(info.value)
    copy_into_pg.assert_not_called()


def test_load_predictions_into_temp_table_empty_glob_still_calls_copy_with_zero_rows():
    read_predictions = MagicMock(return_value=(0, []))
    copy_into_pg = MagicMock()
    args = _sample_args()
    stdout = io.StringIO()
    stdin = io.StringIO('{"type":"exit"}\n')
    loaded = subject.load_predictions_into_temp_table(
        args,
        read_predictions=read_predictions,
        copy_into_pg=copy_into_pg,
        stdout=cast(IO[str], stdout),
        stdin=cast(IO[str], stdin),
    )
    assert loaded == 0
    copy_into_pg.assert_called_once()
    passed_rows = copy_into_pg.call_args.args[1]
    assert passed_rows == []


def test_load_predictions_into_temp_table_defaults_to_real_stdio(
    monkeypatch: pytest.MonkeyPatch,
):
    read_predictions = MagicMock(return_value=(0, []))
    copy_into_pg = MagicMock()
    args = _sample_args()
    fake_stdout = io.StringIO()
    fake_stdin = io.StringIO('{"type":"exit"}\n')
    monkeypatch.setattr(sys, "stdout", fake_stdout)
    monkeypatch.setattr(sys, "stdin", fake_stdin)
    loaded = subject.load_predictions_into_temp_table(
        args,
        read_predictions=read_predictions,
        copy_into_pg=copy_into_pg,
    )
    assert loaded == 0
    passed_stdout = copy_into_pg.call_args.args[2]
    passed_stdin = copy_into_pg.call_args.args[3]
    assert passed_stdout is fake_stdout
    assert passed_stdin is fake_stdin


def test_stdout_returns_sys_stdout(monkeypatch: pytest.MonkeyPatch):
    fake = io.StringIO()
    monkeypatch.setattr(sys, "stdout", fake)
    assert subject.default_stdout() is fake


def test_stdin_returns_sys_stdin(monkeypatch: pytest.MonkeyPatch):
    fake = io.StringIO()
    monkeypatch.setattr(sys, "stdin", fake)
    assert subject.default_stdin() is fake


def test_default_read_predictions_reads_rows_when_no_mismatch(
    monkeypatch: pytest.MonkeyPatch,
):
    duckdb_con = MagicMock()
    duckdb_module = MagicMock()
    duckdb_module.connect.return_value = duckdb_con

    mismatch_call = MagicMock()
    mismatch_call.fetchone.return_value = (0,)
    select_call = MagicMock()
    select_call.fetchall.return_value = [("jra", "2024")]
    duckdb_con.execute.side_effect = [mismatch_call, select_call]

    import importlib as _importlib

    monkeypatch.setattr(
        _importlib, "import_module", MagicMock(return_value=duckdb_module)
    )
    args = _sample_args()
    mismatched, rows = subject.default_read_predictions(args)
    assert mismatched == 0
    assert rows == [("jra", "2024")]
    duckdb_con.close.assert_called_once()


def test_default_read_predictions_returns_mismatch_count_when_versions_drifted(
    monkeypatch: pytest.MonkeyPatch,
):
    duckdb_con = MagicMock()
    duckdb_module = MagicMock()
    duckdb_module.connect.return_value = duckdb_con
    mismatch_call = MagicMock()
    mismatch_call.fetchone.return_value = (5,)
    duckdb_con.execute.return_value = mismatch_call

    import importlib as _importlib

    monkeypatch.setattr(
        _importlib, "import_module", MagicMock(return_value=duckdb_module)
    )
    mismatched, rows = subject.default_read_predictions(_sample_args())
    assert mismatched == 5
    assert rows == []
    duckdb_con.close.assert_called_once()


def test_default_read_predictions_treats_none_mismatch_row_as_zero(
    monkeypatch: pytest.MonkeyPatch,
):
    duckdb_con = MagicMock()
    duckdb_module = MagicMock()
    duckdb_module.connect.return_value = duckdb_con
    mismatch_call = MagicMock()
    mismatch_call.fetchone.return_value = None
    select_call = MagicMock()
    select_call.fetchall.return_value = []
    duckdb_con.execute.side_effect = [mismatch_call, select_call]

    import importlib as _importlib

    monkeypatch.setattr(
        _importlib, "import_module", MagicMock(return_value=duckdb_module)
    )
    mismatched, rows = subject.default_read_predictions(_sample_args())
    assert mismatched == 0
    assert rows == []


def test_default_copy_into_pg_runs_begin_set_create_copy_and_waits_for_exit(
    monkeypatch: pytest.MonkeyPatch,
):
    copy_stream = MagicMock()
    copy_ctx = MagicMock()
    copy_ctx.__enter__.return_value = copy_stream
    copy_ctx.__exit__.return_value = False

    cursor = MagicMock()
    cursor.copy.return_value = copy_ctx
    cursor_ctx = MagicMock()
    cursor_ctx.__enter__.return_value = cursor
    cursor_ctx.__exit__.return_value = False

    pg_con = MagicMock()
    pg_con.cursor.return_value = cursor_ctx

    psycopg_module = MagicMock()
    psycopg_module.connect.return_value = pg_con

    import importlib as _importlib

    monkeypatch.setattr(
        _importlib, "import_module", MagicMock(return_value=psycopg_module)
    )
    args = _sample_args()
    rows: list[tuple[object, ...]] = [
        ("jra", "2024", "0101", "05", "01", "a1", 0.9, 1, 0.5, 0.8, 1, "m7", "v1", "v1"),
    ]
    stdout = io.StringIO()
    stdin = io.StringIO('{"type":"exit"}\n')
    subject.default_copy_into_pg(args, rows, cast(IO[str], stdout), cast(IO[str], stdin))
    issued_sqls = [call.args[0] for call in cursor.execute.call_args_list]
    assert issued_sqls[0] == b"BEGIN"
    assert issued_sqls[1] == b"SET LOCAL statement_timeout = '15min'"
    assert issued_sqls[2].startswith(b"CREATE TEMP TABLE tmp_bucket_eval_finish_position_predictions")
    cursor.copy.assert_called_once()
    copy_query_arg = cursor.copy.call_args.args[0]
    assert copy_query_arg.startswith(b"COPY tmp_bucket_eval_finish_position_predictions")
    copy_stream.write.assert_called_once()
    written_payload = copy_stream.write.call_args.args[0]
    assert "a1" in written_payload
    emitted = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert emitted[0] == {"type": "ready", "loadedRows": 1}
    assert emitted[-1] == {"type": "closed"}
    pg_con.commit.assert_called_once()
    pg_con.close.assert_called_once()


def test_default_copy_into_pg_skips_copy_when_rows_empty(
    monkeypatch: pytest.MonkeyPatch,
):
    cursor = MagicMock()
    cursor_ctx = MagicMock()
    cursor_ctx.__enter__.return_value = cursor
    cursor_ctx.__exit__.return_value = False
    pg_con = MagicMock()
    pg_con.cursor.return_value = cursor_ctx
    psycopg_module = MagicMock()
    psycopg_module.connect.return_value = pg_con

    import importlib as _importlib

    monkeypatch.setattr(
        _importlib, "import_module", MagicMock(return_value=psycopg_module)
    )
    args = _sample_args()
    stdout = io.StringIO()
    stdin = io.StringIO('{"type":"exit"}\n')
    subject.default_copy_into_pg(args, [], cast(IO[str], stdout), cast(IO[str], stdin))
    cursor.copy.assert_not_called()
    emitted = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert emitted[0] == {"type": "ready", "loadedRows": 0}
    assert emitted[-1] == {"type": "closed"}
    pg_con.commit.assert_called_once()


def test_main_invokes_load_and_prints_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    fake_load = MagicMock(return_value=7)
    monkeypatch.setattr(subject, "load_predictions_into_temp_table", fake_load)
    subject.main(_build_argv())
    fake_load.assert_called_once()
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())
    assert payload["loaded_rows"] == 7
    assert payload["category"] == "jra"
    assert payload["year_from"] == 2004
    assert payload["year_to"] == 2025
    assert payload["finish_position_version"] == "v1"
    assert payload["running_style_feature_version"] == "v1"
    assert payload["temp_table_name"] == "tmp_bucket_eval_finish_position_predictions"
