"""Agent H of the bucket-eval pipeline: load Agent G's local finish-position
predictions parquet into a session-scoped PostgreSQL temp table and serve a
JSONL stdin/stdout SQL RPC against the same PG session.

This script is spawned by the TypeScript driver (Agent A
``evaluate-bucket-21y.ts``). After the temp table is populated it prints
``{"type":"ready","loadedRows":N}`` (one JSON object per line) on stdout
and then reads JSONL requests from stdin so the parent process can issue
aggregate / upsert SQL against the same PG session that owns the temp
table. Supported request types are ``sql`` / ``copy`` / ``exit``. Responses
are streamed as ``rows`` (chunked, 1000 rows per chunk, ordered by
``seq``), ``ok`` (for non-SELECT statements), ``error`` (per-request or
fatal), and ``closed`` (after a clean shutdown).

Reads parquet via in-memory DuckDB (no PG round-trips for the parquet
scan), filters by ``--year-from`` / ``--year-to`` / ``--category``, asserts
that every row's ``finish_position_version`` and ``running_style_feature_version``
match the requested versions (so a stale parquet cannot silently bias the
bucket aggregate), then bulk-loads via ``COPY <temp_table> FROM STDIN
WITH (FORMAT CSV)``.

Run with: ``uv run python src/scripts/load_bucket_predictions.py ...``.
"""

from __future__ import annotations

import argparse
import importlib
import io
import json
import sys
from typing import IO, Callable, TypedDict

SUPPORTED_CATEGORIES: tuple[str, str, str] = ("jra", "nar", "ban-ei")
DEFAULT_TEMP_TABLE: str = "tmp_bucket_eval_finish_position_predictions"

RPC_ROWS_CHUNK_SIZE: int = 1000

RPC_REQUEST_TYPE_SQL: str = "sql"
RPC_REQUEST_TYPE_COPY: str = "copy"
RPC_REQUEST_TYPE_EXIT: str = "exit"

RPC_RESPONSE_TYPE_READY: str = "ready"
RPC_RESPONSE_TYPE_ROWS: str = "rows"
RPC_RESPONSE_TYPE_OK: str = "ok"
RPC_RESPONSE_TYPE_ERROR: str = "error"
RPC_RESPONSE_TYPE_CLOSED: str = "closed"

LOAD_COLUMNS: tuple[str, ...] = (
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
    "model_version",
    "running_style_feature_version",
    "finish_position_version",
)

TEMP_TABLE_SCHEMA: tuple[tuple[str, str], ...] = (
    ("source", "text"),
    ("kaisai_nen", "text"),
    ("kaisai_tsukihi", "text"),
    ("keibajo_code", "text"),
    ("race_bango", "text"),
    ("ketto_toroku_bango", "text"),
    ("predicted_score", "numeric"),
    ("predicted_rank", "integer"),
    ("predicted_top1_prob", "numeric"),
    ("predicted_top3_prob", "numeric"),
    ("predicted_finish_position", "integer"),
    ("model_version", "text"),
    ("running_style_feature_version", "text"),
    ("finish_position_version", "text"),
)

SINGLE_QUOTE: str = "'"
DOUBLED_SINGLE_QUOTE: str = "''"


class LoadArguments(TypedDict):
    pg_url: str
    predictions_parquet_glob: str
    temp_table_name: str
    running_style_feature_version: str
    finish_position_version: str
    category: str
    year_from: int
    year_to: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="load_bucket_predictions")
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--predictions-parquet-glob", type=str, required=True)
    parser.add_argument("--temp-table-name", type=str, default=DEFAULT_TEMP_TABLE)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
    parser.add_argument("--finish-position-version", type=str, required=True)
    parser.add_argument("--category", type=str, choices=list(SUPPORTED_CATEGORIES), required=True)
    parser.add_argument("--year-from", type=int, required=True)
    parser.add_argument("--year-to", type=int, required=True)
    return parser.parse_args(argv)


def normalize_arguments(args: argparse.Namespace) -> LoadArguments:
    return {
        "pg_url": args.pg_url,
        "predictions_parquet_glob": args.predictions_parquet_glob,
        "temp_table_name": args.temp_table_name,
        "running_style_feature_version": args.running_style_feature_version,
        "finish_position_version": args.finish_position_version,
        "category": args.category,
        "year_from": int(args.year_from),
        "year_to": int(args.year_to),
    }


def assert_safe_identifier(name: str) -> str:
    """Reject anything that is not a plain SQL identifier so we can safely
    interpolate the temp-table name into DDL/DML."""
    if name == "" or not all(ch.isalnum() or ch == "_" for ch in name):
        raise ValueError(f"Unsafe temp table name: {name!r}")
    return name


def sql_quote_literal(value: str) -> str:
    return value.replace(SINGLE_QUOTE, DOUBLED_SINGLE_QUOTE)


def build_select_from_parquet_sql(args: LoadArguments) -> str:
    safe_glob = sql_quote_literal(args["predictions_parquet_glob"])
    safe_category = sql_quote_literal(args["category"])
    safe_fp = sql_quote_literal(args["finish_position_version"])
    safe_rs = sql_quote_literal(args["running_style_feature_version"])
    columns = ", ".join(LOAD_COLUMNS)
    return (
        f"select {columns} from read_parquet('{safe_glob}', hive_partitioning=1) "
        f"where category = '{safe_category}' "
        f"and cast(race_year as integer) between {args['year_from']} and {args['year_to']} "
        f"and finish_position_version = '{safe_fp}' "
        f"and running_style_feature_version = '{safe_rs}'"
    )


def build_version_mismatch_check_sql(args: LoadArguments) -> str:
    safe_glob = sql_quote_literal(args["predictions_parquet_glob"])
    safe_category = sql_quote_literal(args["category"])
    safe_fp = sql_quote_literal(args["finish_position_version"])
    safe_rs = sql_quote_literal(args["running_style_feature_version"])
    return (
        f"select count(*) from read_parquet('{safe_glob}', hive_partitioning=1) "
        f"where category = '{safe_category}' "
        f"and cast(race_year as integer) between {args['year_from']} and {args['year_to']} "
        f"and (finish_position_version <> '{safe_fp}' "
        f"or running_style_feature_version <> '{safe_rs}')"
    )


def build_create_temp_table_sql(temp_table: str) -> str:
    safe_name = assert_safe_identifier(temp_table)
    columns = ", ".join(f"{col} {col_type}" for col, col_type in TEMP_TABLE_SCHEMA)
    return f"CREATE TEMP TABLE {safe_name} ({columns}) ON COMMIT DROP"


def build_copy_sql(temp_table: str) -> str:
    safe_name = assert_safe_identifier(temp_table)
    return f"COPY {safe_name} ({', '.join(LOAD_COLUMNS)}) FROM STDIN WITH (FORMAT CSV)"


def csv_encode_field(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    text = str(value)
    if any(ch in text for ch in (",", "\"", "\n", "\r")):
        escaped = text.replace("\"", "\"\"")
        return f'"{escaped}"'
    return text


def csv_encode_row(row: tuple[object, ...]) -> str:
    return ",".join(csv_encode_field(value) for value in row)


def stream_csv_rows(rows: list[tuple[object, ...]]) -> str:
    buffer = io.StringIO()
    for row in rows:
        buffer.write(csv_encode_row(row))
        buffer.write("\n")
    return buffer.getvalue()


def default_read_predictions(args: LoadArguments) -> tuple[int, list[tuple[object, ...]]]:
    """Default DuckDB-backed predictions reader.

    Returns ``(mismatched_count, rows)`` so the caller can fail fast when the
    parquet contains stale rows without us having to either pre-load them into
    PG or duplicate the COUNT(*) at the caller. ``mismatched_count`` is the
    number of rows in the requested (category, year-range) slice whose
    versions do not equal the requested ones.
    """
    duckdb_module = importlib.import_module("duckdb")
    duckdb_con = duckdb_module.connect(":memory:")
    try:
        mismatch_row = duckdb_con.execute(build_version_mismatch_check_sql(args)).fetchone()
        mismatched = int(mismatch_row[0]) if mismatch_row is not None else 0
        if mismatched > 0:
            return mismatched, []
        rows = duckdb_con.execute(build_select_from_parquet_sql(args)).fetchall()
    finally:
        duckdb_con.close()
    return 0, list(rows)


def write_response(stdout: IO[str], payload: dict[str, object]) -> None:
    stdout.write(json.dumps(payload, ensure_ascii=False))
    stdout.write("\n")
    stdout.flush()


def chunk_rows_for_response(rows: list[tuple[object, ...]]) -> list[list[tuple[object, ...]]]:
    if len(rows) == 0:
        return [[]]
    chunks: list[list[tuple[object, ...]]] = []
    for start in range(0, len(rows), RPC_ROWS_CHUNK_SIZE):
        chunks.append(rows[start : start + RPC_ROWS_CHUNK_SIZE])
    return chunks


def row_to_jsonable(row: tuple[object, ...], description: list[object] | None) -> object:
    if description is None:
        return list(row)
    columns = [
        col[0] if isinstance(col, (tuple, list)) else getattr(col, "name", str(col))
        for col in description
    ]
    return {column: row[index] for index, column in enumerate(columns)}


def emit_sql_rows(
    stdout: IO[str],
    request_id: str,
    rows: list[tuple[object, ...]],
    description: list[object] | None,
) -> None:
    chunks = chunk_rows_for_response(rows)
    last_index = len(chunks) - 1
    for seq, chunk in enumerate(chunks):
        write_response(
            stdout,
            {
                "type": RPC_RESPONSE_TYPE_ROWS,
                "id": request_id,
                "seq": seq,
                "rows": [row_to_jsonable(row, description) for row in chunk],
                "done": seq == last_index,
            },
        )


def handle_sql_request(
    cursor: object,
    stdout: IO[str],
    request_id: str,
    query: str,
    params: list[object],
) -> None:
    execute = getattr(cursor, "execute")
    execute(query, params) if len(params) > 0 else execute(query)
    description = getattr(cursor, "description", None)
    if description is None:
        rowcount = int(getattr(cursor, "rowcount", 0) or 0)
        write_response(
            stdout,
            {"type": RPC_RESPONSE_TYPE_OK, "id": request_id, "rowcount": rowcount},
        )
        return
    fetchall = getattr(cursor, "fetchall")
    rows: list[tuple[object, ...]] = list(fetchall())
    emit_sql_rows(stdout, request_id, rows, description)


def handle_copy_request(
    cursor: object,
    stdout: IO[str],
    request_id: str,
    query: str,
    csv_payload: str,
) -> None:
    copy = getattr(cursor, "copy")
    with copy(query) as copy_stream:
        copy_stream.write(csv_payload)
    rowcount = int(getattr(cursor, "rowcount", 0) or 0)
    write_response(
        stdout,
        {"type": RPC_RESPONSE_TYPE_OK, "id": request_id, "rowcount": rowcount},
    )


def parse_rpc_line(line: str) -> dict[str, object] | None:
    stripped = line.strip()
    if stripped == "":
        return None
    parsed = json.loads(stripped)
    if not isinstance(parsed, dict):
        raise ValueError("RPC request is not a JSON object")
    return parsed


def dispatch_rpc_request(
    request: dict[str, object],
    cursor: object,
    stdout: IO[str],
) -> str:
    request_type = request.get("type")
    if request_type == RPC_REQUEST_TYPE_EXIT:
        return RPC_REQUEST_TYPE_EXIT
    request_id = str(request.get("id", ""))
    if request_type == RPC_REQUEST_TYPE_SQL:
        query = str(request.get("query", ""))
        params_raw = request.get("params", [])
        params = list(params_raw) if isinstance(params_raw, list) else []
        handle_sql_request(cursor, stdout, request_id, query, params)
        return RPC_REQUEST_TYPE_SQL
    if request_type == RPC_REQUEST_TYPE_COPY:
        query = str(request.get("query", ""))
        csv_payload = str(request.get("csv", ""))
        handle_copy_request(cursor, stdout, request_id, query, csv_payload)
        return RPC_REQUEST_TYPE_COPY
    raise ValueError(f"Unknown RPC request type: {request_type!r}")


def report_rpc_error(stdout: IO[str], request: dict[str, object] | None, error: Exception) -> None:
    request_id = None
    if request is not None and "id" in request:
        candidate = request.get("id")
        if candidate is not None:
            request_id = str(candidate)
    write_response(
        stdout,
        {
            "type": RPC_RESPONSE_TYPE_ERROR,
            "id": request_id,
            "message": str(error),
            "details": type(error).__name__,
        },
    )


def serve_sql_rpc(
    cursor: object,
    stdin: IO[str],
    stdout: IO[str],
    loaded_rows: int,
) -> None:
    write_response(
        stdout,
        {"type": RPC_RESPONSE_TYPE_READY, "loadedRows": loaded_rows},
    )
    while True:
        line = stdin.readline()
        if line == "":
            return
        request: dict[str, object] | None = None
        try:
            request = parse_rpc_line(line)
            if request is None:
                continue
            outcome = dispatch_rpc_request(request, cursor, stdout)
            if outcome == RPC_REQUEST_TYPE_EXIT:
                write_response(stdout, {"type": RPC_RESPONSE_TYPE_CLOSED})
                return
        except Exception as error:
            report_rpc_error(stdout, request, error)


def default_copy_into_pg(
    args: LoadArguments,
    rows: list[tuple[object, ...]],
    stdout: IO[str],
    stdin: IO[str],
) -> None:
    psycopg_module = importlib.import_module("psycopg")
    pg_con = psycopg_module.connect(args["pg_url"])
    try:
        with pg_con.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute("SET LOCAL statement_timeout = '15min'")
            cursor.execute(build_create_temp_table_sql(args["temp_table_name"]))
            if len(rows) > 0:
                with cursor.copy(build_copy_sql(args["temp_table_name"])) as copy_stream:
                    copy_stream.write(stream_csv_rows(rows))
            serve_sql_rpc(cursor, stdin, stdout, len(rows))
            pg_con.commit()
    finally:
        pg_con.close()


ReadPredictionsFn = Callable[[LoadArguments], tuple[int, list[tuple[object, ...]]]]
CopyIntoPgFn = Callable[
    [LoadArguments, list[tuple[object, ...]], IO[str], IO[str]], None
]


def load_predictions_into_temp_table(
    args: LoadArguments,
    *,
    read_predictions: ReadPredictionsFn = default_read_predictions,
    copy_into_pg: CopyIntoPgFn = default_copy_into_pg,
    stdout: IO[str] | None = None,
    stdin: IO[str] | None = None,
) -> int:
    out: IO[str] = stdout if stdout is not None else default_stdout()
    inp: IO[str] = stdin if stdin is not None else default_stdin()
    mismatched, rows = read_predictions(args)
    if mismatched > 0:
        raise RuntimeError(
            "Predictions parquet contains rows whose finish_position_version or "
            "running_style_feature_version does not match the requested versions; "
            f"refusing to load {mismatched} mismatched rows."
        )
    copy_into_pg(args, rows, out, inp)
    return len(rows)


def default_stdout() -> IO[str]:
    return sys.stdout


def default_stdin() -> IO[str]:
    return sys.stdin


def main(argv: list[str] | None = None) -> None:
    args = normalize_arguments(parse_args(argv))
    loaded = load_predictions_into_temp_table(args)
    print(
        json.dumps(
            {
                "loaded_rows": loaded,
                "temp_table_name": args["temp_table_name"],
                "category": args["category"],
                "year_from": args["year_from"],
                "year_to": args["year_to"],
                "finish_position_version": args["finish_position_version"],
                "running_style_feature_version": args["running_style_feature_version"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
