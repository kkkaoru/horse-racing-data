"""Agent RS-F of the running-style bucket-eval pipeline: load Agent F's local
running-style predictions parquet into a session-scoped PostgreSQL temp table
and serve a JSONL stdin/stdout SQL RPC against the same PG session.

This script is spawned by the TypeScript driver (Agent RS-A
``evaluate-running-style-bucket-21y.ts``). After the temp table is populated it
prints ``{"type":"ready","loadedRows":N}`` (one JSON object per line) on stdout
and then reads JSONL requests from stdin so the parent process can issue
aggregate / upsert SQL against the same PG session that owns the temp table.
Supported request types are ``sql`` / ``copy`` / ``exit``. Responses are streamed
as ``rows`` (chunked, 1000 rows per chunk, ordered by ``seq``), ``ok`` (for
non-SELECT statements), ``error`` (per-request or fatal), and ``closed`` (after
a clean shutdown).

Reads parquet via in-memory DuckDB (no PG round-trips for the parquet scan),
filters by ``--year-from`` / ``--year-to`` / ``--category``, asserts that every
row's ``running_style_feature_version`` matches the requested version (so a stale
parquet cannot silently bias the bucket aggregate), then bulk-loads via
``COPY <temp_table> FROM STDIN WITH (FORMAT CSV)``.

Top-2 calculation: at load time, ``numpy.argsort`` with ``kind='stable'`` is used
on the per-row probability vector ``(p_nige, p_senkou, p_sashi, p_oikomi)`` to
derive ``second_predicted_class`` deterministically (same probability values
produce the same second-predicted class across runs). When the stable argsort
top-2 candidate equals ``predicted_class``, the third-rank candidate is taken
as the fallback so the pair is always distinct.

Run with: ``uv run python src/scripts/load_running_style_predictions.py ...``.
"""

from __future__ import annotations

import argparse
import importlib
import io
import json
import sys
from typing import IO, Callable, Sequence, TypedDict

import numpy as np

SUPPORTED_CATEGORIES: tuple[str, str] = ("jra", "nar")
DEFAULT_TEMP_TABLE: str = "bucket_running_style_predictions_loaded"

RPC_ROWS_CHUNK_SIZE: int = 1000

RPC_REQUEST_TYPE_SQL: str = "sql"
RPC_REQUEST_TYPE_COPY: str = "copy"
RPC_REQUEST_TYPE_EXIT: str = "exit"

RPC_RESPONSE_TYPE_READY: str = "ready"
RPC_RESPONSE_TYPE_ROWS: str = "rows"
RPC_RESPONSE_TYPE_OK: str = "ok"
RPC_RESPONSE_TYPE_ERROR: str = "error"
RPC_RESPONSE_TYPE_CLOSED: str = "closed"

CLASS_COUNT: int = 4
TOP_INDEX: int = -1
SECOND_INDEX: int = -2
THIRD_INDEX: int = -3

LOAD_COLUMNS: tuple[str, ...] = (
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "predicted_class",
    "second_predicted_class",
    "target_running_style_class",
    "p_nige",
    "p_senkou",
    "p_sashi",
    "p_oikomi",
)

PARQUET_SELECT_COLUMNS: tuple[str, ...] = (
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "predicted_class",
    "target_running_style_class",
    "p_nige",
    "p_senkou",
    "p_sashi",
    "p_oikomi",
    "running_style_feature_version",
)

TEMP_TABLE_SCHEMA: tuple[tuple[str, str], ...] = (
    ("source", "text not null"),
    ("kaisai_nen", "text not null"),
    ("kaisai_tsukihi", "text not null"),
    ("keibajo_code", "text not null"),
    ("race_bango", "text not null"),
    ("ketto_toroku_bango", "text not null"),
    ("predicted_class", "integer not null"),
    ("second_predicted_class", "integer not null"),
    ("target_running_style_class", "integer not null"),
    ("p_nige", "numeric not null"),
    ("p_senkou", "numeric not null"),
    ("p_sashi", "numeric not null"),
    ("p_oikomi", "numeric not null"),
)

SINGLE_QUOTE: str = "'"
DOUBLED_SINGLE_QUOTE: str = "''"

P_NIGE_INDEX_IN_ROW: int = 8
P_OIKOMI_INDEX_IN_ROW: int = 11
PREDICTED_CLASS_INDEX_IN_ROW: int = 6


class LoadArguments(TypedDict):
    pg_url: str
    predictions_parquet_glob: str
    temp_table_name: str
    running_style_feature_version: str
    model_version: str
    category: str
    year_from: int
    year_to: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="load_running_style_predictions")
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--predictions-parquet-glob", type=str, required=True)
    parser.add_argument("--temp-table-name", type=str, default=DEFAULT_TEMP_TABLE)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
    parser.add_argument("--model-version", type=str, required=True)
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
        "model_version": args.model_version,
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
    safe_rs = sql_quote_literal(args["running_style_feature_version"])
    columns = ", ".join(PARQUET_SELECT_COLUMNS)
    return (
        f"select {columns} from read_parquet('{safe_glob}', hive_partitioning=1) "
        f"where category = '{safe_category}' "
        f"and cast(race_year as integer) between {args['year_from']} and {args['year_to']} "
        f"and running_style_feature_version = '{safe_rs}'"
    )


def build_version_mismatch_check_sql(args: LoadArguments) -> str:
    safe_glob = sql_quote_literal(args["predictions_parquet_glob"])
    safe_category = sql_quote_literal(args["category"])
    safe_rs = sql_quote_literal(args["running_style_feature_version"])
    return (
        f"select count(*) from read_parquet('{safe_glob}', hive_partitioning=1) "
        f"where category = '{safe_category}' "
        f"and cast(race_year as integer) between {args['year_from']} and {args['year_to']} "
        f"and running_style_feature_version <> '{safe_rs}'"
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


def compute_second_predicted_class(
    probabilities: Sequence[float], predicted_class: int
) -> int:
    """Stable argsort over the 4-class probability vector and pick the second
    rank. When the second rank equals ``predicted_class`` (can happen when the
    top-1 was overridden by race-level constraints), fall back to the third
    rank so the (predicted, second) pair is always distinct.
    """
    array = np.asarray(probabilities, dtype=np.float64)
    sorted_indices = np.argsort(array, kind="stable")
    second = int(sorted_indices[SECOND_INDEX])
    if second == predicted_class:
        return int(sorted_indices[THIRD_INDEX])
    return second


def coerce_int(value: object) -> int:
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"Cannot coerce value of type {type(value).__name__} to int.")


def coerce_float(value: object) -> float:
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"Cannot coerce value of type {type(value).__name__} to float.")


def attach_second_predicted_class(
    parquet_row: tuple[object, ...],
) -> tuple[object, ...]:
    """Insert ``second_predicted_class`` into the 14-tuple parquet row after
    ``predicted_class``, producing the 13-column tuple expected by the temp
    table COPY (race-key 6 + predicted_class + second_predicted_class + target +
    4 probabilities)."""
    head = parquet_row[:PREDICTED_CLASS_INDEX_IN_ROW]
    predicted_class = coerce_int(parquet_row[PREDICTED_CLASS_INDEX_IN_ROW])
    target = parquet_row[PREDICTED_CLASS_INDEX_IN_ROW + 1]
    probabilities = (
        coerce_float(parquet_row[P_NIGE_INDEX_IN_ROW]),
        coerce_float(parquet_row[P_NIGE_INDEX_IN_ROW + 1]),
        coerce_float(parquet_row[P_NIGE_INDEX_IN_ROW + 2]),
        coerce_float(parquet_row[P_OIKOMI_INDEX_IN_ROW]),
    )
    second = compute_second_predicted_class(probabilities, predicted_class)
    return (*head, predicted_class, second, target, *probabilities)


def default_read_predictions(args: LoadArguments) -> tuple[int, list[tuple[object, ...]]]:
    """Default DuckDB-backed predictions reader.

    Returns ``(mismatched_count, rows)`` where ``rows`` already have
    ``second_predicted_class`` inserted between ``predicted_class`` and
    ``target_running_style_class``. ``mismatched_count`` is the number of rows in
    the requested (category, year-range) slice whose
    ``running_style_feature_version`` does not equal the requested one — when it
    is non-zero we abort the load.
    """
    duckdb_module = importlib.import_module("duckdb")
    duckdb_con = duckdb_module.connect(":memory:")
    try:
        mismatch_row = duckdb_con.execute(build_version_mismatch_check_sql(args)).fetchone()
        mismatched = int(mismatch_row[0]) if mismatch_row is not None else 0
        if mismatched > 0:
            return mismatched, []
        raw_rows = duckdb_con.execute(build_select_from_parquet_sql(args)).fetchall()
    finally:
        duckdb_con.close()
    return 0, [attach_second_predicted_class(row) for row in raw_rows]


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
            cursor.execute(b"BEGIN")
            cursor.execute(b"SET LOCAL statement_timeout = '15min'")
            cursor.execute(build_create_temp_table_sql(args["temp_table_name"]).encode("utf-8"))
            if len(rows) > 0:
                copy_query = build_copy_sql(args["temp_table_name"]).encode("utf-8")
                with cursor.copy(copy_query) as copy_stream:
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
            "Predictions parquet contains rows whose running_style_feature_version "
            "does not match the requested version; "
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
                "running_style_feature_version": args["running_style_feature_version"],
                "model_version": args["model_version"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
