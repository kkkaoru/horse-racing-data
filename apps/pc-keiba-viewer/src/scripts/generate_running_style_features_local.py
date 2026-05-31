"""Phase A: generate running-style feature parquet locally (no PG/R2/D1/KV writeback).

Pure executor: SQL ownership is delegated to the TypeScript builder
``apps/pc-keiba-viewer/src/scripts/finish-position-features/print-running-style-feature-sql.ts``.
This module only:

1. Invokes the TS builder via ``subprocess`` to obtain the SQL string,
2. Connects to DuckDB, attaches PostgreSQL read-only,
3. Wraps the SQL in ``COPY (FROM postgres_query('pg', '<inner-sql>')) TO
   <output-dir> (PARTITION_BY (race_year))`` so the inner SELECT is executed
   PG-side. This avoids DuckDB local-parse failures on PG-only scalar
   functions (``to_char`` / ``to_date`` / ``interval`` etc.).

Run with (cwd MUST be repo root so the TS print-sql subprocess can resolve its
repo-root-relative path):
    uv run python apps/pc-keiba-viewer/src/scripts/generate_running_style_features_local.py \\
        --pg-url $DATABASE_URL_LOCAL \\
        --output-dir apps/pc-keiba-viewer/tmp/bucket-eval/running-style/v1/features \\
        --running-style-feature-version v1 \\
        --year-from 2006 --year-to 2026 --category jra \\
        --threads 8 --memory-limit 16GB
"""

from __future__ import annotations

import argparse
import calendar
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

DEFAULT_THREADS: int = 8
DEFAULT_MEMORY_LIMIT: str = "16GB"
SUPPORTED_CATEGORIES: tuple[str, str] = ("jra", "nar")
PRINT_SQL_SCRIPT: str = (
    "apps/pc-keiba-viewer/src/scripts/finish-position-features/print-running-style-feature-sql.ts"
)

DuckDBConnector = Callable[[str], "duckdb.DuckDBPyConnection"]
SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate running-style features parquet (local only, TS-SQL delegated).",
    )
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--running-style-feature-version", required=True)
    parser.add_argument("--year-from", required=True, type=int)
    parser.add_argument("--year-to", required=True, type=int)
    parser.add_argument(
        "--month-from",
        type=int,
        choices=list(range(1, 13)),
        required=False,
        default=None,
        help="(optional) Start month 1-12. Defaults to 1 when unspecified.",
    )
    parser.add_argument(
        "--month-to",
        type=int,
        choices=list(range(1, 13)),
        required=False,
        default=None,
        help="(optional) End month 1-12. Defaults to 12 when unspecified.",
    )
    parser.add_argument("--category", required=True, choices=list(SUPPORTED_CATEGORIES))
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def validate_year_range(year_from: int, year_to: int) -> None:
    if year_from > year_to:
        raise ValueError(f"year_from ({year_from}) must be <= year_to ({year_to}).")


def year_to_yyyymmdd_from(year: int) -> str:
    return f"{year:04d}0101"


def year_to_yyyymmdd_to(year: int) -> str:
    return f"{year:04d}1231"


def resolve_month_from(month_from: int | None) -> int:
    return month_from if month_from is not None else 1


def resolve_month_to(month_to: int | None) -> int:
    return month_to if month_to is not None else 12


def build_from_date(year_from: int, month_from: int | None) -> str:
    month = resolve_month_from(month_from)
    return f"{year_from:04d}{month:02d}01"


def build_to_date(year_to: int, month_to: int | None) -> str:
    month = resolve_month_to(month_to)
    last_day = calendar.monthrange(year_to, month)[1]
    return f"{year_to:04d}{month:02d}{last_day:02d}"


def build_from_to_dates(args: argparse.Namespace) -> tuple[str, str]:
    return (
        build_from_date(args.year_from, args.month_from),
        build_to_date(args.year_to, args.month_to),
    )


def build_print_sql_command(
    *,
    category: str,
    from_date: str,
    to_date: str,
    feature_version: str,
) -> list[str]:
    return [
        "bun",
        "run",
        PRINT_SQL_SCRIPT,
        "--source",
        category,
        "--from-date",
        from_date,
        "--to-date",
        to_date,
        "--feature-version",
        feature_version,
    ]


def fetch_running_style_sql(
    *,
    category: str,
    from_date: str,
    to_date: str,
    feature_version: str,
    runner: SubprocessRunner,
) -> str:
    command = build_print_sql_command(
        category=category,
        from_date=from_date,
        to_date=to_date,
        feature_version=feature_version,
    )
    completed = runner(command, capture_output=True, check=False, text=True)
    if completed.returncode != 0:
        message = (
            f"TS print-sql subprocess exited with code {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )
        raise RuntimeError(message)
    sql = completed.stdout.strip()
    if sql == "":
        raise RuntimeError("TS print-sql subprocess returned empty stdout.")
    return sql


def apply_duckdb_resources(
    con: duckdb.DuckDBPyConnection, *, threads: int, memory_limit: str,
) -> None:
    con.execute(f"SET threads TO {threads}")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET enable_progress_bar_print = false")
    con.execute("SET timezone = 'UTC'")


def attach_postgres(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    con.execute("USE pg")
    con.execute("SET pg_use_binary_copy = true")
    con.execute("SET pg_experimental_filter_pushdown = true")
    con.execute("SET pg_pages_per_task = 1000")


def escape_sql_for_postgres_query(select_sql: str) -> str:
    return select_sql.strip().replace("'", "''")


def build_postgres_query_subselect(select_sql: str) -> str:
    escaped = escape_sql_for_postgres_query(select_sql)
    return f"SELECT * FROM postgres_query('pg', '{escaped}')"


def build_hive_copy_sql(*, select_sql: str, output_dir: str) -> str:
    inner = build_postgres_query_subselect(select_sql)
    return (
        f"COPY ({inner}) TO '{output_dir}' "
        "(FORMAT PARQUET, PARTITION_BY (race_year), OVERWRITE_OR_IGNORE TRUE)"
    )


def ensure_output_dir(output_dir: str) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)


def configure_connection(
    con: duckdb.DuckDBPyConnection, args: argparse.Namespace,
) -> None:
    apply_duckdb_resources(con, threads=args.threads, memory_limit=args.memory_limit)
    attach_postgres(con, args.pg_url)


def execute_copy(
    con: duckdb.DuckDBPyConnection, *, select_sql: str, output_dir: str,
) -> None:
    con.execute(build_hive_copy_sql(select_sql=select_sql, output_dir=output_dir))


def run(
    args: argparse.Namespace,
    duckdb_connect: DuckDBConnector,
    runner: SubprocessRunner,
) -> None:
    validate_year_range(args.year_from, args.year_to)
    ensure_output_dir(args.output_dir)
    from_date, to_date = build_from_to_dates(args)
    select_sql = fetch_running_style_sql(
        category=args.category,
        from_date=from_date,
        to_date=to_date,
        feature_version=args.running_style_feature_version,
        runner=runner,
    )
    con = duckdb_connect(":memory:")
    try:
        configure_connection(con, args)
        execute_copy(con, select_sql=select_sql, output_dir=args.output_dir)
    finally:
        con.close()


def main(argv: list[str] | None = None) -> None:
    import duckdb

    args = parse_args(argv)
    run(args, duckdb.connect, subprocess.run)


if __name__ == "__main__":
    main()
