#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append sectional time + weight trend features (per-horse history aggregations)
to a v3 finish-position feature parquet directory, producing v4.

Reads v3 parquet, joins with PG `race_entry_corner_features` to pull soha_time and
bataiju history (NOT present in v3), aggregates per horse over the last 5 prior
races, and writes a v4 parquet partitioned by race_year.

New features:
  - recent_soha_time_per_meter_avg5: average normalized finish time
  - same_distance_soha_time_per_meter_avg5: same, but restricted to similar distance
  - bataiju_avg5: 5-race average horse weight
  - weight_trend_5: linear regression slope of bataiju over last 5 races (kg/race)
  - weight_volatility_5: stddev of bataiju over last 5 races

Run with:
  .venv/bin/python src/scripts/finish-position-features/add-sectional-and-weight-features.py \
    --input-dir tmp/finish-position-features-parquet-jra-v3 \
    --output-dir tmp/finish-position-features-parquet-jra-v4 \
    --pg-url postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
SAME_DISTANCE_TOLERANCE = 200
RECENT_WINDOW_SIZE = 5
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_sectional_and_weight_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument("--to-date", type=str, default="20991231")
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_history(
    con: duckdb.DuckDBPyConnection, from_date: str, to_date: str
) -> None:
    con.execute(
        f"""
        create or replace temp table bataiju_hist as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
               try_cast(nullif(trim(bataiju), '') as double) as bataiju
        from pg.jvd_se
        where (kaisai_nen || kaisai_tsukihi) between '{from_date}' and '{to_date}'
          and ketto_toroku_bango is not null
        """
    )
    con.execute(
        f"""
        create or replace temp table rec_hist as
        select
          rec.source,
          rec.race_date,
          rec.kaisai_nen,
          rec.kaisai_tsukihi,
          rec.keibajo_code,
          rec.race_bango,
          rec.ketto_toroku_bango,
          cast(rec.soha_time as double) as soha_time,
          cast(rec.kyori as int) as kyori,
          bw.bataiju
        from pg.race_entry_corner_features rec
        left join bataiju_hist bw
          on bw.kaisai_nen = rec.kaisai_nen
          and bw.kaisai_tsukihi = rec.kaisai_tsukihi
          and bw.keibajo_code = rec.keibajo_code
          and bw.race_bango = rec.race_bango
          and bw.ketto_toroku_bango = rec.ketto_toroku_bango
        where rec.race_date between '{from_date}' and '{to_date}'
          and rec.ketto_toroku_bango is not null
        """
    )
    con.execute("create index rec_hist_horse_date on rec_hist (source, ketto_toroku_bango, race_date)")


def stage_horse_history_lookup(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    con.execute(
        f"""
        create or replace temp table base_v3 as
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kyori, race_date
        from read_parquet('{input_glob}', hive_partitioning=true)
        """
    )
    con.execute(
        f"""
        create or replace temp table horse_history_with_rank as
        select
          t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
          t.ketto_toroku_bango,
          t.kyori as target_kyori,
          h.soha_time as hist_soha_time,
          h.kyori as hist_kyori,
          h.bataiju as hist_bataiju,
          row_number() over (
            partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
            order by h.race_date desc
          ) as recent_rank,
          row_number() over (
            partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
            order by case when abs(h.kyori - t.kyori) <= {SAME_DISTANCE_TOLERANCE} then 0 else 1 end,
                     h.race_date desc
          ) as same_distance_recent_rank
        from base_v3 t
        join rec_hist h on h.source = t.source
          and h.ketto_toroku_bango = t.ketto_toroku_bango
          and h.race_date < t.race_date
        """
    )


def stage_horse_history_agg(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        create or replace temp table horse_history_agg as
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
          avg(case when hist_kyori > 0 then hist_soha_time / hist_kyori::double else null end)
            filter (where recent_rank <= {RECENT_WINDOW_SIZE})
            as recent_soha_time_per_meter_avg5,
          avg(case when hist_kyori > 0 then hist_soha_time / hist_kyori::double else null end)
            filter (
              where same_distance_recent_rank <= {RECENT_WINDOW_SIZE}
                and abs(hist_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE}
            )
            as same_distance_soha_time_per_meter_avg5,
          avg(hist_bataiju) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as bataiju_avg5,
          regr_slope(hist_bataiju, (-recent_rank)::double)
            filter (where recent_rank <= {RECENT_WINDOW_SIZE} and hist_bataiju is not null)
            as weight_trend_5,
          stddev_pop(hist_bataiju)
            filter (where recent_rank <= {RECENT_WINDOW_SIZE} and hist_bataiju is not null)
            as weight_volatility_5
        from horse_history_with_rank
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
        """
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base_v3 as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    )
    select
      b.*,
      h.recent_soha_time_per_meter_avg5,
      h.same_distance_soha_time_per_meter_avg5,
      h.bataiju_avg5,
      h.weight_trend_5,
      h.weight_volatility_5
    from base_v3 b
    left join horse_history_agg h
      using ({RACE_PARTITION}, ketto_toroku_bango)
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
    install_and_attach_pg(con, args.pg_url)
    stage_history(con, args.from_date, args.to_date)
    stage_horse_history_lookup(con, input_glob)
    stage_horse_history_agg(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
