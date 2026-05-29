"""Phase A: generate running-style feature parquet locally (no PG/R2/D1/KV writeback).

Run with:
    uv run python src/scripts/generate_running_style_features_local.py \
        --pg-url $DATABASE_URL_LOCAL \
        --output-dir apps/pc-keiba-viewer/tmp/bucket-eval/running-style/v1/features \
        --running-style-feature-version v1 \
        --year-from 2006 --year-to 2026 --category jra \
        --threads 8 --memory-limit 16GB
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Protocol

DEFAULT_THREADS: int = 8
DEFAULT_MEMORY_LIMIT: str = "16GB"
DEFAULT_TEMP_DIR_LIMIT: str = "200GB"
SUPPORTED_CATEGORIES: tuple[str, str, str] = ("jra", "nar", "ban-ei")
STATEMENT_TIMEOUT: str = "15min"
WORK_MEM: str = "256MB"


class DuckDBConnectionLike(Protocol):
    def execute(self, query: str) -> object: ...
    def close(self) -> None: ...


class DuckDBConnectorLike(Protocol):
    def __call__(self, database: str) -> DuckDBConnectionLike: ...


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate running-style features parquet (local only).",
    )
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--running-style-feature-version", required=True)
    parser.add_argument("--year-from", required=True, type=int)
    parser.add_argument("--year-to", required=True, type=int)
    parser.add_argument("--category", required=True, choices=list(SUPPORTED_CATEGORIES))
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT)
    parser.add_argument("--temp-dir-limit", default=DEFAULT_TEMP_DIR_LIMIT)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def apply_duckdb_resources(
    con: DuckDBConnectionLike, *, threads: int, memory_limit: str, temp_dir_limit: str,
) -> None:
    con.execute(f"SET threads TO {threads}")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET max_temp_directory_size = '{temp_dir_limit}'")


def attach_postgres(con: DuckDBConnectionLike, pg_url: str) -> None:
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    con.execute(f"CALL postgres_execute('pg', 'SET statement_timeout = ''{STATEMENT_TIMEOUT}''')")
    con.execute(f"CALL postgres_execute('pg', 'SET work_mem = ''{WORK_MEM}''')")


def build_source_filter_sql(category: str) -> str:
    if category == "ban-ei":
        return "se.keibajo_code = '83'"
    if category == "nar":
        return "se.source = 'nar'"
    return "se.source = 'jra' AND se.keibajo_code <> '83'"


def build_se_table_name(category: str) -> str:
    return "nvd_se" if category in ("nar", "ban-ei") else "jvd_se"


def build_ra_table_name(category: str) -> str:
    return "nvd_ra" if category in ("nar", "ban-ei") else "jvd_ra"


def build_year_feature_query(*, category: str, year: int, feature_version: str) -> str:
    se_table = build_se_table_name(category)
    ra_table = build_ra_table_name(category)
    source_filter = build_source_filter_sql(category)
    return f"""
        SELECT
            se.source,
            se.kaisai_nen,
            se.kaisai_tsukihi,
            se.keibajo_code,
            se.race_bango,
            se.ketto_toroku_bango,
            se.umaban,
            se.corner1_norm,
            ra.kyori,
            ra.track_code,
            ra.grade_code,
            CAST({year} AS INTEGER) AS race_year,
            CAST('{category}' AS VARCHAR) AS category,
            CAST('{feature_version}' AS VARCHAR) AS running_style_feature_version
        FROM pg.{se_table} se
        JOIN pg.{ra_table} ra
            ON se.kaisai_nen = ra.kaisai_nen
            AND se.kaisai_tsukihi = ra.kaisai_tsukihi
            AND se.keibajo_code = ra.keibajo_code
            AND se.race_bango = ra.race_bango
        WHERE se.kaisai_nen = '{year:04d}'
          AND {source_filter}
    """


def build_hive_copy_sql(*, select_sql: str, output_dir: str) -> str:
    return (
        f"COPY ({select_sql.strip()}) TO '{output_dir}' "
        "(FORMAT PARQUET, PARTITION_BY (category, race_year), OVERWRITE_OR_IGNORE TRUE)"
    )


def export_year_features(
    *,
    con: DuckDBConnectionLike,
    category: str,
    year: int,
    output_dir: str,
    feature_version: str,
) -> None:
    select_sql = build_year_feature_query(
        category=category, year=year, feature_version=feature_version,
    )
    con.execute(build_hive_copy_sql(select_sql=select_sql, output_dir=output_dir))


def ensure_output_dir(output_dir: str) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)


def run(args: argparse.Namespace, duckdb_connect: DuckDBConnectorLike) -> None:
    ensure_output_dir(args.output_dir)
    con = duckdb_connect(":memory:")
    try:
        apply_duckdb_resources(
            con,
            threads=args.threads,
            memory_limit=args.memory_limit,
            temp_dir_limit=args.temp_dir_limit,
        )
        attach_postgres(con, args.pg_url)
        for year in range(args.year_from, args.year_to + 1):
            export_year_features(
                con=con,
                category=args.category,
                year=year,
                output_dir=args.output_dir,
                feature_version=args.running_style_feature_version,
            )
    finally:
        con.close()


def main(argv: list[str] | None = None) -> None:
    import duckdb

    args = parse_args(argv)
    run(args, duckdb.connect)


if __name__ == "__main__":
    main()
