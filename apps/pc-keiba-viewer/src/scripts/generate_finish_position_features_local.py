"""Phase A' of Agent G: generate finish-position features locally.

This wrapper invokes the public ``run()`` entry point of
``finish_position_features_duckdb`` for a (category, year-range) slice, then
post-processes the resulting parquet with DuckDB to:

1. read Agent F's running-style prediction parquet via ``read_parquet`` so the
   running-style coverage can be verified alongside the features,
2. stamp ``finish_position_version`` and ``running_style_feature_version``
   onto every row so consumers cannot lose track of which version produced
   the parquet,
3. write the result back as a Hive-partitioned parquet directory
   (``category=<c>/race_year=<y>/part-*.parquet``) under
   ``apps/pc-keiba-viewer/tmp/bucket-eval/finish-position/<v>/features/``.

We deliberately do not touch ``finish_position_features_duckdb`` itself - it
is imported read-only and only the public ``run()`` entry point is invoked.

Run with: ``uv run python src/scripts/generate_finish_position_features_local.py ...``.
"""

from __future__ import annotations

import argparse
import json
import shutil
from argparse import Namespace
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, TypedDict

import finish_position_features_duckdb as feature_builder

if TYPE_CHECKING:
    import duckdb

CATEGORY_CHOICES: tuple[str, ...] = ("jra", "nar", "ban-ei")
DEFAULT_THREADS: int = 8
DEFAULT_MEMORY_LIMIT: str = "16GB"
DEFAULT_HEARTBEAT_INTERVAL_SECONDS: float = 10.0
RAW_OUTPUT_PREFIX: str = "_raw"
SINGLE_QUOTE: str = "'"
DOUBLED_SINGLE_QUOTE: str = "''"


class PhaseAArguments(TypedDict):
    pg_url: str
    running_style_parquet: Path
    output_dir: Path
    finish_position_version: str
    running_style_feature_version: str
    year_from: int
    year_to: int
    category: str
    threads: int
    memory_limit: str


class PhaseAResult(TypedDict):
    elapsed_seconds: float
    output_dir: str
    rows_written: int
    category: str
    year_from: int
    year_to: int
    finish_position_version: str
    running_style_feature_version: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="generate_finish_position_features_local")
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--running-style-parquet", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--finish-position-version", type=str, required=True)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
    parser.add_argument("--year-from", type=int, required=True)
    parser.add_argument("--year-to", type=int, required=True)
    parser.add_argument("--category", type=str, choices=CATEGORY_CHOICES, required=True)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--memory-limit", type=str, default=DEFAULT_MEMORY_LIMIT)
    return parser.parse_args(argv)


def normalize_arguments(args: argparse.Namespace) -> PhaseAArguments:
    return {
        "pg_url": args.pg_url,
        "running_style_parquet": Path(args.running_style_parquet),
        "output_dir": Path(args.output_dir),
        "finish_position_version": args.finish_position_version,
        "running_style_feature_version": args.running_style_feature_version,
        "year_from": int(args.year_from),
        "year_to": int(args.year_to),
        "category": args.category,
        "threads": int(args.threads),
        "memory_limit": args.memory_limit,
    }


def assert_running_style_parquet_present(running_style_parquet: Path) -> None:
    if not running_style_parquet.exists():
        raise FileNotFoundError(
            f"Running-style parquet input not found: {running_style_parquet}"
        )


def sql_quote_literal(value: str) -> str:
    return value.replace(SINGLE_QUOTE, DOUBLED_SINGLE_QUOTE)


def build_feature_builder_args(args: PhaseAArguments, raw_output_dir: Path) -> Namespace:
    return Namespace(
        category=args["category"],
        from_date=f"{args['year_from']:04d}0101",
        to_date=f"{args['year_to']:04d}1231",
        output_dir=raw_output_dir,
        pg_url=args["pg_url"],
        threads=args["threads"],
        memory_limit=args["memory_limit"],
        status_file=None,
        heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        skip_count=False,
        keep_existing_output=False,
        force_clean_output=True,
        temp_dir=None,
    )


def build_raw_output_dir(args: PhaseAArguments) -> Path:
    name = f"{RAW_OUTPUT_PREFIX}-{args['category']}-{args['year_from']}-{args['year_to']}"
    return args["output_dir"].parent / name


def build_attach_running_style_sql(running_style_parquet: Path) -> str:
    quoted = sql_quote_literal(running_style_parquet.as_posix())
    return (
        "create or replace view running_style_local as "
        f"select * from read_parquet('{quoted}/**/*.parquet', hive_partitioning=1)"
    )


def build_stamp_features_sql(
    raw_output_dir: Path,
    category: str,
    finish_position_version: str,
    running_style_feature_version: str,
) -> str:
    quoted_raw = sql_quote_literal(raw_output_dir.as_posix())
    safe_category = sql_quote_literal(category)
    safe_fp_version = sql_quote_literal(finish_position_version)
    safe_rs_version = sql_quote_literal(running_style_feature_version)
    return (
        "create or replace temp table stamped_features as "
        f"select '{safe_category}' as category, *, "
        f"'{safe_fp_version}' as finish_position_version, "
        f"'{safe_rs_version}' as running_style_feature_version "
        f"from read_parquet('{quoted_raw}/**/*.parquet', hive_partitioning=1)"
    )


def build_copy_stamped_features_sql(final_output_dir: Path) -> str:
    quoted_final = sql_quote_literal(final_output_dir.as_posix())
    return (
        "copy stamped_features "
        f"to '{quoted_final}' "
        "(format parquet, partition_by (category, race_year), overwrite_or_ignore true)"
    )


def attach_running_style_parquet(
    con: "duckdb.DuckDBPyConnection",
    running_style_parquet: Path,
) -> None:
    con.execute(build_attach_running_style_sql(running_style_parquet))


def stamp_versions_and_rewrite_parquet(
    con: "duckdb.DuckDBPyConnection",
    raw_output_dir: Path,
    final_output_dir: Path,
    finish_position_version: str,
    running_style_feature_version: str,
    category: str,
) -> int:
    if final_output_dir.exists():
        shutil.rmtree(final_output_dir)
    final_output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        build_stamp_features_sql(
            raw_output_dir, category, finish_position_version, running_style_feature_version
        )
    )
    con.execute(build_copy_stamped_features_sql(final_output_dir))
    rows = con.execute("select count(*) from stamped_features").fetchone()
    return int(rows[0]) if rows is not None else 0


def configure_local_duckdb(threads: int, memory_limit: str) -> "duckdb.DuckDBPyConnection":
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET threads = {int(threads)}")
    con.execute(f"SET memory_limit = '{sql_quote_literal(memory_limit)}'")
    return con


def run_phase_a(args: PhaseAArguments) -> PhaseAResult:
    assert_running_style_parquet_present(args["running_style_parquet"])
    final_output_dir = args["output_dir"]
    raw_output_dir = build_raw_output_dir(args)
    overall_started = perf_counter()
    builder_args = build_feature_builder_args(args, raw_output_dir)
    feature_builder.run(builder_args)
    con = configure_local_duckdb(args["threads"], args["memory_limit"])
    try:
        attach_running_style_parquet(con, args["running_style_parquet"])
        rows = stamp_versions_and_rewrite_parquet(
            con,
            raw_output_dir,
            final_output_dir,
            args["finish_position_version"],
            args["running_style_feature_version"],
            args["category"],
        )
    finally:
        con.close()
    if raw_output_dir.exists():
        shutil.rmtree(raw_output_dir)
    return {
        "elapsed_seconds": perf_counter() - overall_started,
        "output_dir": final_output_dir.as_posix(),
        "rows_written": rows,
        "category": args["category"],
        "year_from": args["year_from"],
        "year_to": args["year_to"],
        "finish_position_version": args["finish_position_version"],
        "running_style_feature_version": args["running_style_feature_version"],
    }


def main(argv: list[str] | None = None) -> None:
    args = normalize_arguments(parse_args(argv))
    result = run_phase_a(args)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
