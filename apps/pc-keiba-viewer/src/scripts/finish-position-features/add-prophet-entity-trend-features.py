#!/usr/bin/env python3
"""Join precomputed Prophet entity trends into a day/category feature parquet.

This production layer never imports or executes Prophet. Frozen daily forecasts
are generated offline and baked as a small lookup parquet. Runtime work is one
current-card entity query plus indexed DuckDB joins, performed once in DAY_CHAIN
and reused by every per-race prediction.
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path
from typing import Final, Literal

import duckdb
from _catalog_attach import attach_source_catalog
from _resource_defaults import add_resource_args, apply_to_connection

Category = Literal["jra", "nar", "ban-ei"]

PROPHET_FEATURE_NAMES: Final[tuple[str, ...]] = (
    "prophet_venue_performance_yhat",
    "prophet_jockey_performance_yhat",
    "prophet_trainer_performance_yhat",
    "prophet_entity_performance_mean",
    "prophet_entity_coverage",
)
ENTITY_TYPES: Final[tuple[str, ...]] = ("venue", "jockey", "trainer")
TARGET_DATE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d{8}$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_prophet_entity_trend_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--category", choices=("jra", "nar", "ban-ei"), required=True)
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--prophet-lookup", type=Path, required=True)
    add_resource_args(parser)
    return parser.parse_args(argv)


def target_entities_sql(category: Category, target_date: str) -> str:
    """Return a current-card-only entrant query for the selected category."""
    if TARGET_DATE_PATTERN.fullmatch(target_date) is None:
        raise ValueError("target_date must use YYYYMMDD")
    table = "jvd_se" if category == "jra" else "nvd_se"
    venue_predicate = {
        "jra": "true",
        "nar": "se.keibajo_code <> '83'",
        "ban-ei": "se.keibajo_code = '83'",
    }[category]
    source = "jra" if category == "jra" else "nar"
    return f"""
    select distinct
      '{source}' as source,
      se.kaisai_nen,
      se.kaisai_tsukihi,
      se.keibajo_code,
      se.race_bango,
      se.ketto_toroku_bango,
      nullif(trim(se.kishu_code), '') as jockey_code,
      nullif(trim(se.chokyoshi_code), '') as trainer_code
    from pg.{table} se
    where se.kaisai_nen || se.kaisai_tsukihi = '{target_date}'
      and {venue_predicate}
      and coalesce(trim(se.ijo_kubun_code), '0') not in ('1', '2')
    """


def stage_target_entities(
    connection: duckdb.DuckDBPyConnection, category: Category, target_date: str
) -> None:
    connection.execute(
        f"create or replace temp table prophet_target_entities as "
        f"{target_entities_sql(category, target_date)}"
    )
    connection.execute(
        "create index prophet_target_entities_idx on prophet_target_entities "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_prophet_lookup(
    connection: duckdb.DuckDBPyConnection,
    lookup_path: Path,
    category: Category,
    target_date: str,
) -> None:
    """Load one category/day and reject duplicate entity forecasts."""
    if not lookup_path.is_file():
        raise FileNotFoundError(f"Prophet lookup not found: {lookup_path}")
    connection.execute(
        "create or replace temp table prophet_lookup as "
        "select entity_type, cast(entity_code as varchar) as entity_code, yhat "
        "from read_parquet(?) where category = ? and forecast_date = ?",
        [str(lookup_path), category, target_date],
    )
    duplicate_count = connection.execute(
        "select count(*) from ("
        "select entity_type, entity_code from prophet_lookup "
        "group by entity_type, entity_code having count(*) <> 1)"
    ).fetchone()
    if duplicate_count is None or int(duplicate_count[0]) != 0:
        raise ValueError("Prophet lookup contains duplicate entity forecasts")
    connection.execute(
        "create index prophet_lookup_idx on prophet_lookup (entity_type, entity_code)"
    )


def append_features_sql(input_glob: str) -> str:
    """Return the runner-preserving entity forecast joins."""
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ), joined as (
      select
        b.*,
        coalesce(venue.yhat, venue_fallback.yhat)
          as prophet_venue_performance_yhat,
        coalesce(jockey.yhat, jockey_fallback.yhat)
          as prophet_jockey_performance_yhat,
        coalesce(trainer.yhat, trainer_fallback.yhat)
          as prophet_trainer_performance_yhat,
        venue.yhat is not null as prophet_venue_entity_selected,
        jockey.yhat is not null as prophet_jockey_entity_selected,
        trainer.yhat is not null as prophet_trainer_entity_selected
      from base b
      left join prophet_target_entities entities
        on entities.source = b.source
        and entities.kaisai_nen = b.kaisai_nen
        and entities.kaisai_tsukihi = b.kaisai_tsukihi
        and entities.keibajo_code = b.keibajo_code
        and entities.race_bango = b.race_bango
        and entities.ketto_toroku_bango = b.ketto_toroku_bango
      left join prophet_lookup venue
        on venue.entity_type = 'venue'
        and venue.entity_code = cast(b.keibajo_code as varchar)
      left join prophet_lookup jockey
        on jockey.entity_type = 'jockey'
        and jockey.entity_code = entities.jockey_code
      left join prophet_lookup trainer
        on trainer.entity_type = 'trainer'
        and trainer.entity_code = entities.trainer_code
      left join prophet_lookup venue_fallback
        on venue_fallback.entity_type = 'venue'
        and venue_fallback.entity_code = '__fallback__'
      left join prophet_lookup jockey_fallback
        on jockey_fallback.entity_type = 'jockey'
        and jockey_fallback.entity_code = '__fallback__'
      left join prophet_lookup trainer_fallback
        on trainer_fallback.entity_type = 'trainer'
        and trainer_fallback.entity_code = '__fallback__'
    ), summarized as (
      select
        *,
        (cast(prophet_venue_entity_selected as integer)
         + cast(prophet_jockey_entity_selected as integer)
         + cast(prophet_trainer_entity_selected as integer))
          as prophet_entity_coverage
      from joined
    )
    select
      * exclude (
        prophet_venue_entity_selected,
        prophet_jockey_entity_selected,
        prophet_trainer_entity_selected
      ),
      (coalesce(prophet_venue_performance_yhat, 0)
       + coalesce(prophet_jockey_performance_yhat, 0)
       + coalesce(prophet_trainer_performance_yhat, 0))
      / 3 as prophet_entity_performance_mean
    from summarized
    """


def write_partitioned(
    connection: duckdb.DuckDBPyConnection, sql: str, output_dir: Path
) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"copy ({sql}) to '{output_dir.as_posix()}' "
        "(format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )


def run(args: argparse.Namespace) -> None:
    connection = duckdb.connect(":memory:")
    try:
        apply_to_connection(connection, args.threads, args.memory_limit)
        connection.execute("set preserve_insertion_order=false")
        attach_source_catalog(connection, args.pg_url)
        stage_target_entities(connection, args.category, args.target_date)
        stage_prophet_lookup(
            connection, args.prophet_lookup, args.category, args.target_date
        )
        input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
        write_partitioned(connection, append_features_sql(input_glob), args.output_dir)
    finally:
        connection.close()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
