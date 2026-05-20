#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append futan_juryo (handicap weight) features to an existing finish-position
feature parquet, producing a new layer (v5).

Pattern B post-processor:
  - reads input parquet (hive-partitioned by race_year)
  - joins with PG `race_entry_corner_features` for futan_juryo per horse
  - computes per-horse + race-internal features
  - writes new parquet partitioned by race_year

Coverage notes:
  - JRA: race_entry_corner_features has 100% coverage (post-rebuild)
  - NAR: only 2005-2017+2026 in race_entry_corner_features (PG raw incomplete)
  - Ban-ei: separate pipeline

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-futan-juryo-features.py \\
    --input-dir tmp/feat-v20-merged/jra \\
    --output-dir tmp/feat-v20-merged-v5/jra
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
RACE_PARTITION_BY = "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
WEIGHT_BUCKET_BREAKS = (500, 700, 900, 1100)
HIGH_FUTAN_THRESHOLD = 56.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_futan_juryo_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument("--to-date", type=str, default="20991231")
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_futan_juryo(con: duckdb.DuckDBPyConnection, from_date: str, to_date: str) -> None:
    con.execute(
        f"""
        create or replace temp table futan_raw as
        select
          source,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          ketto_toroku_bango,
          try_cast(futan_juryo as double) / 10.0 as futan_juryo
        from pg.race_entry_corner_features
        where race_date between '{from_date}' and '{to_date}'
          and futan_juryo is not null
        """
    )
    con.execute(
        "create index futan_raw_idx on futan_raw (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_horse_history(con: duckdb.DuckDBPyConnection) -> None:
    """Pre-compute per-horse past futan stats for join."""
    con.execute(
        """
        create or replace temp table horse_futan_hist as
        with ranked as (
          select source, ketto_toroku_bango, kaisai_nen, kaisai_tsukihi, futan_juryo,
            row_number() over (
              partition by source, ketto_toroku_bango
              order by kaisai_nen desc, kaisai_tsukihi desc
            ) as rn
          from futan_raw
        )
        select source, ketto_toroku_bango, kaisai_nen, kaisai_tsukihi,
          avg(futan_juryo) filter (where rn between 1 and 5) over horse_window as past_futan_juryo_avg5,
          avg(case when futan_juryo > {threshold} then 1.0 else 0.0 end)
            filter (where rn between 1 and 10) over horse_window as past_high_futan_share
        from ranked
        window horse_window as (partition by source, ketto_toroku_bango)
        """.format(threshold=HIGH_FUTAN_THRESHOLD)
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    joined as (
      select b.*,
        r.futan_juryo,
        h.past_futan_juryo_avg5,
        h.past_high_futan_share
      from base b
      left join futan_raw r using ({RACE_PARTITION}, ketto_toroku_bango)
      left join horse_futan_hist h
        on h.source = b.source
        and h.ketto_toroku_bango = b.ketto_toroku_bango
        and h.kaisai_nen = b.kaisai_nen
        and h.kaisai_tsukihi = b.kaisai_tsukihi
    )
    select
      b.*,
      rank() over race_by_futan_desc as futan_juryo_rank_in_race,
      b.futan_juryo - avg(b.futan_juryo) over (partition by {RACE_PARTITION_BY})
        as futan_juryo_diff_from_race_avg,
      b.futan_juryo - b.past_futan_juryo_avg5 as past_futan_juryo_diff,
      case when b.futan_juryo is null then null
           when b.futan_juryo < 56.0 then 0
           when b.futan_juryo < 58.0 then 1
           when b.futan_juryo < 60.0 then 2
           else 3 end as futan_weight_class
    from joined b
    window race_by_futan_desc as (
      partition by {RACE_PARTITION_BY}
      order by b.futan_juryo desc nulls last
    )
    """


def write_partitioned(con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"copy ({sql}) to '{output_dir.as_posix()}' "
        "(format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    stage_futan_juryo(con, args.from_date, args.to_date)
    stage_horse_history(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
