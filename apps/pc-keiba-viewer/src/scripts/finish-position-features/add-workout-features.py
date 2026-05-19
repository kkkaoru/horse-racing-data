#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append workout (調教) sectional features to an existing finish-position
feature parquet, producing a new layer.

Pattern B post-processor:
  - reads input parquet (hive-partitioned by race_year)
  - joins with PG `jvd_hc` for per-horse training records (lap_time_*f)
  - aggregates recent workout (within 90 days before each race)
  - writes new parquet partitioned by race_year

Coverage:
  - JRA: ~100% (jvd_hc has 11.78M rows, 23 years)
  - NAR: ~57% (some NAR horses also train at JRA-managed centers)
  - Ban-ei: ~0% (ban-ei horses train separately, not in jvd_hc)

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-workout-features.py \\
    --input-dir tmp/feat-v20-merged-v5/jra \\
    --output-dir tmp/feat-v20-merged-v6/jra
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
LOOKBACK_DAYS = 90
WORKOUT_RECENT_WINDOW = 5
WORKOUT_LONG_WINDOW = 10


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_workout_features")
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


def stage_workout_raw(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """Stage jvd_hc workout records, filtered to lookback window from from_date.

    jvd_hc.lap_time_*f / time_gokei_*f are zero-padded varchar ('000', '166' = 16.6s).
    Values of '000' or empty mean no recording. Cast to numeric and treat 0 as null.
    """
    history_floor = _shift_date_back(from_date, LOOKBACK_DAYS + 30)
    con.execute(
        f"""
        create or replace temp table workout_raw as
        select
          ketto_toroku_bango,
          chokyo_nengappi,
          strptime(chokyo_nengappi, '%Y%m%d')::date as workout_dt,
          tracen_kubun,
          nullif(try_cast(lap_time_1f as double), 0) / 10.0 as lap_1f,
          nullif(try_cast(lap_time_2f as double), 0) / 10.0 as lap_2f,
          nullif(try_cast(lap_time_3f as double), 0) / 10.0 as lap_3f,
          nullif(try_cast(lap_time_4f as double), 0) / 10.0 as lap_4f,
          nullif(try_cast(time_gokei_4f as double), 0) / 10.0 as gokei_4f,
          nullif(try_cast(time_gokei_3f as double), 0) / 10.0 as gokei_3f,
          nullif(try_cast(time_gokei_2f as double), 0) / 10.0 as gokei_2f
        from pg.jvd_hc
        where chokyo_nengappi >= '{history_floor}'
        """
    )
    con.execute(
        "create index workout_raw_idx on workout_raw (ketto_toroku_bango, workout_dt)"
    )


def stage_workout_agg(con: duckdb.DuckDBPyConnection) -> None:
    """Compute per-horse aggregations of recent workouts vs target race date."""
    con.execute(
        f"""
        create or replace temp table workout_agg as
        with race_keys as (
          select distinct ketto_toroku_bango,
            strptime(race_date, '%Y%m%d')::date as race_dt,
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, source
          from base_parquet
        ),
        joined as (
          select rk.source, rk.kaisai_nen, rk.kaisai_tsukihi, rk.keibajo_code, rk.race_bango,
            rk.ketto_toroku_bango, rk.race_dt,
            w.workout_dt, w.lap_1f, w.lap_2f, w.lap_3f, w.lap_4f,
            w.gokei_4f, w.gokei_3f, w.gokei_2f, w.tracen_kubun,
            (rk.race_dt - w.workout_dt) as days_before,
            row_number() over (
              partition by rk.source, rk.kaisai_nen, rk.kaisai_tsukihi, rk.keibajo_code,
                rk.race_bango, rk.ketto_toroku_bango
              order by w.workout_dt desc
            ) as rn
          from race_keys rk
          left join workout_raw w
            on w.ketto_toroku_bango = rk.ketto_toroku_bango
            and w.workout_dt < rk.race_dt
            and w.workout_dt >= rk.race_dt - {LOOKBACK_DAYS}
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
          avg(lap_1f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_lap_1f_avg5,
          avg(lap_2f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_lap_2f_avg5,
          avg(lap_3f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_lap_3f_avg5,
          avg(lap_4f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_lap_4f_avg5,
          min(lap_1f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_lap_1f_best5,
          min(lap_3f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_lap_3f_best5,
          avg(gokei_4f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_gokei_4f_avg5,
          avg(gokei_3f) filter (where rn <= {WORKOUT_RECENT_WINDOW}) as workout_gokei_3f_avg5,
          count(*) filter (where rn <= {WORKOUT_LONG_WINDOW}) as workout_count_recent,
          count(*) filter (where rn is not null and days_before <= 30) as workout_count_30d,
          min(days_before) as days_since_last_workout,
          max(case when rn = 1 then tracen_kubun end) as recent_tracen_kubun
        from joined
        where rn is not null
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
        """
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    select
      b.*,
      a.workout_lap_1f_avg5,
      a.workout_lap_2f_avg5,
      a.workout_lap_3f_avg5,
      a.workout_lap_4f_avg5,
      a.workout_lap_1f_best5,
      a.workout_lap_3f_best5,
      a.workout_gokei_4f_avg5,
      a.workout_gokei_3f_avg5,
      coalesce(a.workout_count_recent, 0) as workout_count_recent,
      coalesce(a.workout_count_30d, 0) as workout_count_30d,
      a.days_since_last_workout,
      case when a.workout_lap_4f_avg5 is not null and a.workout_lap_1f_avg5 is not null
           then a.workout_lap_4f_avg5 - a.workout_lap_1f_avg5
           else null end as workout_pace_progression
    from read_parquet('{input_glob}', hive_partitioning=true) b
    left join workout_agg a using ({RACE_PARTITION}, ketto_toroku_bango)
    """


def write_partitioned(con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"copy ({sql}) to '{output_dir.as_posix()}' "
        "(format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )


def _shift_date_back(date_str: str, days: int) -> str:
    """Compute YYYYMMDD shifted back by N days. Best-effort for SQL date filter."""
    from datetime import date as _date, timedelta
    parsed = _date(int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:8]))
    shifted = parsed - timedelta(days=days)
    return shifted.strftime("%Y%m%d")


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    con.execute("SET memory_limit='24GB'")
    con.execute("SET threads TO 6")
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    con.execute(
        f"create or replace temp table base_parquet as "
        f"select * from read_parquet('{input_glob}', hive_partitioning=true)"
    )
    stage_workout_raw(con, args.from_date)
    stage_workout_agg(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
