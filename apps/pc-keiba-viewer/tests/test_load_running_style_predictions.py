from __future__ import annotations

import decimal
import io
import json
import sys
from typing import IO, cast
from unittest.mock import MagicMock

import numpy as np
import pytest

import load_running_style_predictions as subject


def _build_argv(**overrides: str) -> list[str]:
    defaults: dict[str, str] = {
        "--pg-url": "postgresql://u:p@h/db",
        "--predictions-parquet-glob": "tmp/bucket-eval/running-style/v1/predictions/**/*.parquet",
        "--temp-table-name": "bucket_running_style_predictions_loaded",
        "--running-style-feature-version": "v1",
        "--model-version": "jra-running-style-lgbm-v1.0",
        "--category": "jra",
        "--year-from": "2005",
        "--year-to": "2026",
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
        "temp_table_name": "bucket_running_style_predictions_loaded",
        "running_style_feature_version": "v1",
        "model_version": "jra-running-style-lgbm-v1.0",
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
        == "tmp/bucket-eval/running-style/v1/predictions/**/*.parquet"
    )
    assert args.temp_table_name == "bucket_running_style_predictions_loaded"
    assert args.running_style_feature_version == "v1"
    assert args.model_version == "jra-running-style-lgbm-v1.0"
    assert args.category == "jra"
    assert args.year_from == 2005
    assert args.year_to == 2026


def test_parse_args_requires_model_version():
    argv = _build_argv()
    idx = argv.index("--model-version")
    del argv[idx : idx + 2]
    with pytest.raises(SystemExit):
        subject.parse_args(argv)


def test_parse_args_accepts_nar_model_version():
    args = subject.parse_args(
        _build_argv(**{"--category": "nar", "--model-version": "nar-running-style-lgbm-v2.0"})
    )
    assert args.model_version == "nar-running-style-lgbm-v2.0"


def test_parse_args_defaults_temp_table_name():
    argv = _build_argv()
    idx = argv.index("--temp-table-name")
    del argv[idx : idx + 2]
    args = subject.parse_args(argv)
    assert args.temp_table_name == subject.DEFAULT_TEMP_TABLE


def test_parse_args_rejects_unknown_category():
    with pytest.raises(SystemExit):
        subject.parse_args(_build_argv(**{"--category": "bogus"}))


def test_parse_args_rejects_ban_ei_category():
    with pytest.raises(SystemExit):
        subject.parse_args(_build_argv(**{"--category": "ban-ei"}))


def test_parse_args_accepts_nar_category():
    args = subject.parse_args(_build_argv(**{"--category": "nar"}))
    assert args.category == "nar"


def test_parse_args_requires_predictions_parquet_glob():
    argv = _build_argv()
    idx = argv.index("--predictions-parquet-glob")
    del argv[idx : idx + 2]
    with pytest.raises(SystemExit):
        subject.parse_args(argv)


def test_parse_args_requires_running_style_feature_version():
    argv = _build_argv()
    idx = argv.index("--running-style-feature-version")
    del argv[idx : idx + 2]
    with pytest.raises(SystemExit):
        subject.parse_args(argv)


def test_normalize_arguments_converts_years_to_int():
    args = subject.parse_args(_build_argv())
    normalized = subject.normalize_arguments(args)
    assert normalized["year_from"] == 2005
    assert normalized["year_to"] == 2026
    assert normalized["category"] == "jra"
    assert normalized["temp_table_name"] == "bucket_running_style_predictions_loaded"
    assert normalized["running_style_feature_version"] == "v1"
    assert normalized["model_version"] == "jra-running-style-lgbm-v1.0"


def test_assert_safe_identifier_accepts_snake_case():
    assert subject.assert_safe_identifier("bucket_running_style_123") == "bucket_running_style_123"


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
        predictions_parquet_glob="tmp/preds/category=jra",
        running_style_feature_version="rsX",
        year_from=2010,
        year_to=2025,
    )
    sql = subject.build_select_from_parquet_sql(args)
    assert "read_parquet('tmp/preds/category=jra')" in sql
    assert "cast(kaisai_nen as integer) between 2010 and 2025" in sql
    assert "running_style_feature_version = 'rsX'" in sql
    assert "target_running_style_class" in sql
    assert "predicted_class" in sql
    assert "p_nige" in sql
    assert "p_oikomi" in sql


def test_build_select_from_parquet_sql_omits_hive_partitioning_and_category_predicate():
    args = _sample_args(
        predictions_parquet_glob="tmp/preds/category=jra",
        running_style_feature_version="rsX",
        category="jra",
        year_from=2010,
        year_to=2025,
    )
    sql = subject.build_select_from_parquet_sql(args)
    assert "hive_partitioning" not in sql
    assert "category =" not in sql
    assert "race_year" not in sql


def test_build_version_mismatch_check_sql_counts_mismatched_rows():
    args = _sample_args(
        predictions_parquet_glob="tmp/preds/category=nar",
        running_style_feature_version="rsX",
        category="nar",
        year_from=2020,
        year_to=2024,
    )
    sql = subject.build_version_mismatch_check_sql(args)
    assert "select count(*) from read_parquet('tmp/preds/category=nar')" in sql
    assert "running_style_feature_version is null" in sql
    assert "running_style_feature_version <> 'rsX'" in sql
    assert "cast(kaisai_nen as integer) between 2020 and 2024" in sql


def test_build_version_mismatch_check_sql_omits_hive_partitioning_and_category_predicate():
    args = _sample_args(
        predictions_parquet_glob="tmp/preds/category=nar",
        running_style_feature_version="rsX",
        category="nar",
        year_from=2020,
        year_to=2024,
    )
    sql = subject.build_version_mismatch_check_sql(args)
    assert "hive_partitioning" not in sql
    assert "category =" not in sql
    assert "race_year" not in sql


def test_build_create_temp_table_sql_includes_all_columns_and_on_commit_drop():
    sql = subject.build_create_temp_table_sql("bucket_running_style_predictions_loaded")
    assert sql.startswith("CREATE TEMP TABLE bucket_running_style_predictions_loaded (")
    assert "source text not null" in sql
    assert "kaisai_nen text not null" in sql
    assert "kaisai_tsukihi text not null" in sql
    assert "keibajo_code text not null" in sql
    assert "race_bango text not null" in sql
    assert "ketto_toroku_bango text not null" in sql
    assert "predicted_class integer not null" in sql
    assert "second_predicted_class integer not null" in sql
    assert "target_running_style_class integer," in sql
    assert "target_running_style_class integer not null" not in sql
    assert "p_nige numeric not null" in sql
    assert "p_senkou numeric not null" in sql
    assert "p_sashi numeric not null" in sql
    assert "p_oikomi numeric not null" in sql
    assert "model_version text not null" in sql
    assert "running_style_feature_version text not null" in sql
    assert "race_date date not null" in sql
    assert sql.endswith(") ON COMMIT DROP")


def test_temp_table_schema_includes_driver_required_columns():
    column_names = [name for name, _ in subject.TEMP_TABLE_SCHEMA]
    assert "model_version" in column_names
    assert "running_style_feature_version" in column_names
    assert "race_date" in column_names


def test_temp_table_schema_target_running_style_class_is_nullable():
    schema_map = dict(subject.TEMP_TABLE_SCHEMA)
    assert schema_map["target_running_style_class"] == "integer"


def test_temp_table_schema_predicted_class_remains_not_null():
    schema_map = dict(subject.TEMP_TABLE_SCHEMA)
    assert schema_map["predicted_class"] == "integer not null"
    assert schema_map["second_predicted_class"] == "integer not null"


def test_csv_encode_row_emits_empty_field_for_none_target_running_style_class():
    row: tuple[object, ...] = (
        "jra",
        "2006",
        "0131",
        "56",
        "04",
        "2002103391",
        3,
        2,
        None,
        0.10,
        0.20,
        0.30,
        0.40,
        "jra-running-style-lgbm-prod-v2",
        "v1",
        "2006-01-31",
    )
    encoded = subject.csv_encode_row(row)
    fields = encoded.split(",")
    assert fields[8] == ""


def test_default_copy_into_pg_writes_empty_csv_field_for_null_target(
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
        (
            "jra",
            "2006",
            "0131",
            "56",
            "04",
            "2002103391",
            3,
            2,
            None,
            0.10,
            0.20,
            0.30,
            0.40,
            "jra-running-style-lgbm-prod-v2",
            "v1",
            "2006-01-31",
        ),
    ]
    stdout = io.StringIO()
    stdin = io.StringIO('{"type":"exit"}\n')
    subject.default_copy_into_pg(args, rows, cast(IO[str], stdout), cast(IO[str], stdin))
    written_payload = copy_stream.write.call_args.args[0]
    assert "2002103391,3,2,,0.1," in written_payload


def test_load_columns_includes_driver_required_columns():
    assert "model_version" in subject.LOAD_COLUMNS
    assert "running_style_feature_version" in subject.LOAD_COLUMNS
    assert "race_date" in subject.LOAD_COLUMNS


def test_parquet_select_columns_includes_model_version():
    assert "model_version" in subject.PARQUET_SELECT_COLUMNS
    assert "running_style_feature_version" in subject.PARQUET_SELECT_COLUMNS


def test_build_create_temp_table_sql_rejects_unsafe_name():
    with pytest.raises(ValueError):
        subject.build_create_temp_table_sql("tmp;DROP")


def test_build_copy_sql_uses_csv_format_and_explicit_columns():
    sql = subject.build_copy_sql("bucket_running_style_predictions_loaded")
    assert sql.startswith("COPY bucket_running_style_predictions_loaded (")
    assert "FROM STDIN WITH (FORMAT CSV)" in sql
    assert "source" in sql
    assert "predicted_class" in sql
    assert "second_predicted_class" in sql
    assert "target_running_style_class" in sql


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


def test_coerce_int_handles_int():
    assert subject.coerce_int(42) == 42


def test_coerce_int_handles_float():
    assert subject.coerce_int(3.7) == 3


def test_coerce_int_handles_str():
    assert subject.coerce_int("5") == 5


def test_coerce_int_rejects_list():
    with pytest.raises(TypeError):
        subject.coerce_int([1, 2])


def test_coerce_float_handles_float():
    assert subject.coerce_float(0.5) == 0.5


def test_coerce_float_handles_int():
    assert subject.coerce_float(2) == 2.0


def test_coerce_float_handles_str():
    assert subject.coerce_float("0.25") == 0.25


def test_coerce_float_handles_decimal():
    converted = subject.coerce_float(decimal.Decimal("0.123"))
    assert isinstance(converted, float)
    assert converted == 0.123


def test_coerce_float_handles_decimal_zero():
    converted = subject.coerce_float(decimal.Decimal("0"))
    assert isinstance(converted, float)
    assert converted == 0.0


def test_coerce_float_rejects_dict():
    with pytest.raises(TypeError):
        subject.coerce_float({"key": "value"})


def test_compute_second_predicted_class_picks_second_rank():
    probs = (0.05, 0.40, 0.30, 0.25)
    second = subject.compute_second_predicted_class(probs, predicted_class=1)
    assert second == 2


def test_compute_second_predicted_class_falls_back_to_third_when_second_equals_predicted():
    probs = (0.05, 0.40, 0.30, 0.25)
    second = subject.compute_second_predicted_class(probs, predicted_class=2)
    assert second == 3


def test_compute_second_predicted_class_stable_argsort_deterministic_on_ties():
    probs = (0.5, 0.5, 0.0, 0.0)
    first_call = subject.compute_second_predicted_class(probs, predicted_class=1)
    second_call = subject.compute_second_predicted_class(probs, predicted_class=1)
    assert first_call == second_call
    assert first_call == 0


def test_compute_second_predicted_class_stable_argsort_known_value_for_tie_with_third_index_zero():
    probs = (0.0, 0.0, 0.5, 0.5)
    second = subject.compute_second_predicted_class(probs, predicted_class=3)
    assert second == 2


def test_compute_second_predicted_class_stable_kind_required_for_determinism():
    """Document that stable kind is required: this test asserts the contract
    in the implementation. With kind='stable' the index of the tied
    larger element is the smaller numpy index; if kind were unstable some
    libc qsort implementations would not guarantee that property.
    """
    probs = np.array([0.5, 0.5, 0.0, 0.0])
    stable = np.argsort(probs, kind="stable")
    assert int(stable[-1]) == 1
    assert int(stable[-2]) == 0


def test_attach_second_predicted_class_inserts_second_into_row():
    parquet_row = (
        "jra",
        "2024",
        "0101",
        "05",
        "01",
        "ABC123",
        1,
        2,
        0.05,
        0.40,
        0.30,
        0.25,
        "v1",
        "jra-running-style-lgbm-prod-v2",
    )
    attached = subject.attach_second_predicted_class(parquet_row)
    assert attached == (
        "jra",
        "2024",
        "0101",
        "05",
        "01",
        "ABC123",
        1,
        2,
        2,
        0.05,
        0.40,
        0.30,
        0.25,
        "jra-running-style-lgbm-prod-v2",
        "v1",
        "2024-01-01",
    )


def test_attach_second_predicted_class_falls_back_when_second_equals_predicted():
    parquet_row = (
        "nar",
        "2024",
        "0101",
        "55",
        "01",
        "XYZ999",
        2,
        0,
        0.05,
        0.40,
        0.30,
        0.25,
        "v1",
        "nar-running-style-lgbm-v2.0",
    )
    attached = subject.attach_second_predicted_class(parquet_row)
    assert attached[7] == 3


def test_attach_second_predicted_class_appends_model_and_feature_version():
    parquet_row = (
        "jra",
        "2023",
        "1231",
        "05",
        "01",
        "ABC123",
        1,
        2,
        0.05,
        0.40,
        0.30,
        0.25,
        "v1",
        "jra-running-style-lgbm-prod-v2",
    )
    attached = subject.attach_second_predicted_class(parquet_row)
    assert attached[13] == "jra-running-style-lgbm-prod-v2"
    assert attached[14] == "v1"
    assert attached[15] == "2023-12-31"


def test_derive_race_date_builds_iso_string_from_nen_and_tsukihi():
    assert subject.derive_race_date("2024", "0101") == "2024-01-01"


def test_derive_race_date_handles_year_end_date():
    assert subject.derive_race_date("2023", "1231") == "2023-12-31"


def test_derive_race_date_handles_mid_year_date():
    assert subject.derive_race_date("2010", "0715") == "2010-07-15"


def test_derive_race_date_rejects_short_kaisai_nen():
    with pytest.raises(ValueError):
        subject.derive_race_date("24", "0101")


def test_derive_race_date_rejects_non_digit_kaisai_nen():
    with pytest.raises(ValueError):
        subject.derive_race_date("20a4", "0101")


def test_derive_race_date_rejects_short_kaisai_tsukihi():
    with pytest.raises(ValueError):
        subject.derive_race_date("2024", "101")


def test_derive_race_date_rejects_non_digit_kaisai_tsukihi():
    with pytest.raises(ValueError):
        subject.derive_race_date("2024", "01x1")


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


def test_encode_rpc_value_passes_through_int():
    assert subject.encode_rpc_value(42) == 42


def test_encode_rpc_value_passes_through_float():
    assert subject.encode_rpc_value(0.125) == 0.125


def test_encode_rpc_value_converts_decimal_to_float():
    converted = subject.encode_rpc_value(decimal.Decimal("0.25"))
    assert isinstance(converted, float)
    assert converted == 0.25


def test_encode_rpc_value_passes_through_str():
    assert subject.encode_rpc_value("hello") == "hello"


def test_encode_rpc_value_passes_through_none():
    assert subject.encode_rpc_value(None) is None


def test_row_to_jsonable_converts_decimal_to_float_in_list_form():
    row: tuple[object, ...] = (1, decimal.Decimal("0.5"), "z")
    result = subject.row_to_jsonable(row, None)
    assert result == [1, 0.5, "z"]
    assert isinstance(cast(list[object], result)[1], float)


def test_row_to_jsonable_converts_decimal_to_float_in_dict_form():
    columns: list[object] = [("count",), ("ratio",)]
    row: tuple[object, ...] = (10, decimal.Decimal("0.875"))
    result = subject.row_to_jsonable(row, columns)
    assert result == {"count": 10, "ratio": 0.875}
    assert isinstance(cast(dict[str, object], result)["ratio"], float)


def test_emit_sql_rows_serializes_decimal_row_without_json_error():
    stdout = io.StringIO()
    column = MagicMock()
    column.name = "avg_prob"
    rows: list[tuple[object, ...]] = [(decimal.Decimal("0.3333333333"),)]
    subject.emit_sql_rows(cast(IO[str], stdout), "rid", rows, [column])
    payloads = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert len(payloads) == 1
    assert payloads[0]["type"] == "rows"
    assert payloads[0]["id"] == "rid"
    assert payloads[0]["seq"] == 0
    assert payloads[0]["done"] is True
    assert payloads[0]["rows"] == [{"avg_prob": 0.3333333333}]


def test_serve_sql_rpc_handles_select_returning_decimal_column():
    cursor = MagicMock()
    description_col = MagicMock()
    description_col.name = "bucket_avg"
    cursor.description = [description_col]
    cursor.fetchall.return_value = [
        (decimal.Decimal("0.10"),),
        (decimal.Decimal("0.20"),),
        (decimal.Decimal("0.30"),),
    ]
    request = json.dumps(
        {"type": "sql", "id": "agg1", "query": "select avg(p_nige) from t", "params": []}
    )
    stdin = io.StringIO(request + "\n" + json.dumps({"type": "exit"}) + "\n")
    stdout = io.StringIO()
    subject.serve_sql_rpc(cursor, cast(IO[str], stdin), cast(IO[str], stdout), 0)
    lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
    rows_response = [line for line in lines if line["type"] == "rows"][0]
    assert rows_response == {
        "type": "rows",
        "id": "agg1",
        "seq": 0,
        "rows": [
            {"bucket_avg": 0.10},
            {"bucket_avg": 0.20},
            {"bucket_avg": 0.30},
        ],
        "done": True,
    }


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
        (
            "jra",
            "2024",
            "0101",
            "05",
            "01",
            "a1",
            1,
            2,
            1,
            0.05,
            0.60,
            0.20,
            0.15,
            "jra-running-style-lgbm-v1.0",
            "v1",
            "2024-01-01",
        ),
        (
            "jra",
            "2024",
            "0101",
            "05",
            "01",
            "a2",
            2,
            1,
            2,
            0.05,
            0.25,
            0.50,
            0.20,
            "jra-running-style-lgbm-v1.0",
            "v1",
            "2024-01-01",
        ),
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
    select_call.fetchall.return_value = [
        (
            "jra",
            "2024",
            "0101",
            "05",
            "01",
            "a1",
            1,
            2,
            0.05,
            0.60,
            0.20,
            0.15,
            "v1",
            "jra-running-style-lgbm-v1.0",
        ),
    ]
    duckdb_con.execute.side_effect = [mismatch_call, select_call]

    import importlib as _importlib

    monkeypatch.setattr(
        _importlib, "import_module", MagicMock(return_value=duckdb_module)
    )
    args = _sample_args()
    mismatched, rows = subject.default_read_predictions(args)
    assert mismatched == 0
    assert len(rows) == 1
    assert rows[0] == (
        "jra",
        "2024",
        "0101",
        "05",
        "01",
        "a1",
        1,
        2,
        2,
        0.05,
        0.60,
        0.20,
        0.15,
        "jra-running-style-lgbm-v1.0",
        "v1",
        "2024-01-01",
    )
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
        (
            "jra",
            "2024",
            "0101",
            "05",
            "01",
            "a1",
            1,
            2,
            1,
            0.05,
            0.60,
            0.20,
            0.15,
            "jra-running-style-lgbm-v1.0",
            "v1",
            "2024-01-01",
        ),
    ]
    stdout = io.StringIO()
    stdin = io.StringIO('{"type":"exit"}\n')
    subject.default_copy_into_pg(args, rows, cast(IO[str], stdout), cast(IO[str], stdin))
    issued_sqls = [call.args[0] for call in cursor.execute.call_args_list]
    assert issued_sqls[0] == b"BEGIN"
    assert issued_sqls[1] == b"SET LOCAL statement_timeout = '15min'"
    assert issued_sqls[2].startswith(
        b"CREATE TEMP TABLE bucket_running_style_predictions_loaded"
    )
    cursor.copy.assert_called_once()
    copy_stream_arg = cursor.copy.call_args.args[0]
    assert copy_stream_arg.startswith(b"COPY bucket_running_style_predictions_loaded")
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


def test_main_invokes_load_and_prints_summary_to_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    fake_load = MagicMock(return_value=7)
    monkeypatch.setattr(subject, "load_predictions_into_temp_table", fake_load)
    subject.main(_build_argv())
    fake_load.assert_called_once()
    captured = capsys.readouterr()
    payload = json.loads(captured.err.strip())
    assert payload["loaded_rows"] == 7
    assert payload["category"] == "jra"
    assert payload["year_from"] == 2005
    assert payload["year_to"] == 2026
    assert payload["running_style_feature_version"] == "v1"
    assert payload["temp_table_name"] == "bucket_running_style_predictions_loaded"
    assert payload["model_version"] == "jra-running-style-lgbm-v1.0"


def test_main_does_not_print_summary_to_stdout(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    fake_load = MagicMock(return_value=12)
    monkeypatch.setattr(subject, "load_predictions_into_temp_table", fake_load)
    subject.main(_build_argv())
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "loaded_rows" not in captured.out
