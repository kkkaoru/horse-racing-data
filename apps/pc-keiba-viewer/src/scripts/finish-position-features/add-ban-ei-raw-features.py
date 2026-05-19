#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append raw race features (futan_juryo, bataiju, corner positions, track condition)
to the cached ban-ei v1 parquet, producing a v2 layer.

Coverage caveat:
  - nvd_se has ban-ei rows only for 2005-2017 + 2026 (2018-2025 missing).
  - For missing years the new columns are NaN; gradient boosting handles them.

Pattern B post-processor (mirrors add-market-signal-features.py):
  - reads input parquet (hive-partitioned by race_year)
  - joins pg.nvd_se on the race composite key for horse-level fields
  - joins pg.nvd_ra on the race key for race-level conditions
  - emits race-internal rank/diff features for futan_juryo and bataiju
  - writes new parquet partitioned by race_year

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-ban-ei-raw-features.py \\
    --input-dir apps/pc-keiba-viewer/tmp/finish-position-features-parquet-ban-ei-v1 \\
    --output-dir tmp/feat-ban-ei-v2
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
RACE_PARTITION_BY = "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"
BAN_EI_KEIBAJO = "83"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
FUTAN_BUCKET_BREAKS = (700, 800, 900)  # ≤700, 701-800, 801-900, 900+


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_ban_ei_raw_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_nvd_se(con: duckdb.DuckDBPyConnection) -> None:
    """Pull ban-ei race entries with futan_juryo, bataiju, corner positions."""
    con.execute(
        f"""
        create or replace temp table se_raw as
        select
          'nar' as source,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
          try_cast(nullif(trim(futan_juryo), '') as double) / 10.0 as futan_juryo,
          try_cast(nullif(trim(bataiju), '') as int) as bataiju,
          try_cast(nullif(trim(corner_1), '') as int) as corner_1_raw,
          try_cast(nullif(trim(corner_3), '') as int) as corner_3_raw,
          try_cast(nullif(trim(corner_4), '') as int) as corner_4_raw,
          try_cast(nullif(trim(soha_time), '') as double) / 10.0 as soha_time_sec,
          try_cast(nullif(trim(tansho_ninkijun), '') as int) as tansho_ninkijun
        from pg.nvd_se
        where keibajo_code = '{BAN_EI_KEIBAJO}'
        """
    )
    con.execute(
        "create index se_raw_idx on se_raw (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_nvd_ra(con: duckdb.DuckDBPyConnection) -> None:
    """Pull race-level conditions (weather, track) for ban-ei.

    Emit ordinal-encoded numeric values only (no raw strings) so downstream
    LightGBM/XGBoost ranker can ingest the columns without preprocessing.
    """
    con.execute(
        f"""
        create or replace temp table ra_raw as
        select
          'nar' as source,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          try_cast(nullif(trim(tenko_code), '') as int) as tenko_code_ord,
          try_cast(nullif(trim(babajotai_code_dirt), '') as int) as babajotai_code_dirt_ord
        from pg.nvd_ra
        where keibajo_code = '{BAN_EI_KEIBAJO}'
        """
    )
    con.execute(
        "create index ra_raw_idx on ra_raw (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)"
    )


def stage_bataiju_history(con: duckdb.DuckDBPyConnection) -> None:
    """Per-horse rolling avg of bataiju over last 5 starts before each race."""
    con.execute(
        """
        create or replace temp table bataiju_hist as
        with ranked as (
          select source, ketto_toroku_bango, kaisai_nen, kaisai_tsukihi, bataiju,
            row_number() over (
              partition by source, ketto_toroku_bango
              order by kaisai_nen desc, kaisai_tsukihi desc
            ) as rn
          from se_raw
          where bataiju is not null and bataiju > 0
        )
        select source, ketto_toroku_bango, kaisai_nen, kaisai_tsukihi,
          avg(bataiju) filter (where rn between 1 and 5) over (partition by source, ketto_toroku_bango) as bataiju_avg5
        from ranked
        """
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    joined as (
      select b.*,
        s.futan_juryo,
        s.bataiju,
        s.corner_1_raw,
        s.corner_3_raw,
        s.corner_4_raw,
        s.soha_time_sec,
        s.tansho_ninkijun,
        r.tenko_code_ord,
        r.babajotai_code_dirt_ord,
        h.bataiju_avg5
      from base b
      left join se_raw s using ({RACE_PARTITION}, ketto_toroku_bango)
      left join ra_raw r using ({RACE_PARTITION})
      left join bataiju_hist h
        on h.source = b.source
        and h.ketto_toroku_bango = b.ketto_toroku_bango
        and h.kaisai_nen = b.kaisai_nen
        and h.kaisai_tsukihi = b.kaisai_tsukihi
    )
    select
      b.*,
      rank() over race_by_futan_desc as futan_juryo_rank_in_race,
      b.futan_juryo - avg(b.futan_juryo) over (partition by {RACE_PARTITION_BY}) as futan_juryo_diff_from_race_avg,
      case
        when b.futan_juryo is null then null
        when b.futan_juryo <= 700 then 0
        when b.futan_juryo <= 800 then 1
        when b.futan_juryo <= 900 then 2
        else 3 end as futan_juryo_bucket,
      b.bataiju - b.bataiju_avg5 as bataiju_diff_from_avg5
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
    con.execute("SET memory_limit='16GB'")
    con.execute("SET threads TO 6")
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    stage_nvd_se(con)
    stage_nvd_ra(con)
    stage_bataiju_history(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
