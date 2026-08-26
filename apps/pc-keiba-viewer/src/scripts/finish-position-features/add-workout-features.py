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
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

from _catalog_attach import attach_source_catalog

from _resource_defaults import add_resource_args, apply_to_connection

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
LOOKBACK_DAYS = 90
WORKOUT_RECENT_WINDOW = 5
WORKOUT_LONG_WINDOW = 10


@dataclass(frozen=True)
class WorkoutScope:
    history_floor: str
    history_ceiling: str
    horse_ids: tuple[str, ...]


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
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Focused production mode keibajo_code:race_bango. The input parquet "
            "is already scoped by the base builder; this flag narrows workout "
            "staging to target horses."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    attach_source_catalog(con, pg_url)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def workout_scope(con: duckdb.DuckDBPyConnection) -> WorkoutScope | None:
    """Resolve the exact horses and date interval required by the input rows."""
    row = con.execute(
        """
        select min(race_date), max(race_date),
          list(distinct ketto_toroku_bango order by ketto_toroku_bango)
        from base_parquet
        where race_date is not null and ketto_toroku_bango is not null
        """
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        return None

    first_race_date = str(row[0])
    last_race_date = str(row[1])
    raw_horse_ids = row[2]
    if not isinstance(raw_horse_ids, list):
        raise TypeError("base_parquet horse ID aggregation did not return a list")
    horse_ids = tuple(str(horse_id) for horse_id in raw_horse_ids)
    return WorkoutScope(
        history_floor=_shift_date_back(first_race_date, LOOKBACK_DAYS),
        history_ceiling=last_race_date,
        horse_ids=horse_ids,
    )


def stage_workout_raw(
    con: duckdb.DuckDBPyConnection,
    scope: WorkoutScope | None,
) -> None:
    """Stage only workout records that can contribute to the input races.

    jvd_hc.lap_time_*f / time_gokei_*f are zero-padded varchar ('000', '166' = 16.6s).
    Values of '000' or empty mean no recording. Cast to numeric and treat 0 as null.
    """
    if scope is None or not scope.horse_ids:
        source_filter = "false"
    else:
        horse_literals = ", ".join(_sql_literal(value) for value in scope.horse_ids)
        source_filter = (
            f"chokyo_nengappi >= {_sql_literal(scope.history_floor)} "
            f"and chokyo_nengappi < {_sql_literal(scope.history_ceiling)} "
            f"and ketto_toroku_bango in ({horse_literals})"
        )
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
        where {source_filter}
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
            case when w.workout_dt is not null then
              row_number() over (
                partition by rk.source, rk.kaisai_nen, rk.kaisai_tsukihi, rk.keibajo_code,
                  rk.race_bango, rk.ketto_toroku_bango
                order by w.workout_dt desc
              )
            end as rn
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
          count(workout_dt) filter (where rn <= {WORKOUT_LONG_WINDOW})
            as workout_count_recent,
          count(workout_dt) filter (where rn is not null and days_before <= 30)
            as workout_count_30d,
          min(days_before) as days_since_last_workout,
          max(case when rn = 1 then tracen_kubun end) as recent_tracen_kubun
        from joined
        where rn is not null
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
        """
    )


def append_features_sql() -> str:
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
    from base_parquet b
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
    """Compute YYYYMMDD shifted back by N days for a SQL date filter."""
    parsed = date(int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:8]))
    shifted = parsed - timedelta(days=days)
    return shifted.strftime("%Y%m%d")


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    con.execute(
        f"create or replace temp table base_parquet as "
        f"select * from read_parquet('{input_glob}', hive_partitioning=true)"
    )
    stage_workout_raw(con, workout_scope(con))
    stage_workout_agg(con)
    write_partitioned(con, append_features_sql(), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
