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
  - recent_race_relative_time_z_avg5: within-race standardized clock residual
  - recent_condition_par_time_residual_avg5: residual from strictly prior-day condition par
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
from collections.abc import Collection
from pathlib import Path

import duckdb
from _catalog_attach import attach_source_catalog
from _race_time import encoded_race_time_tenths_sql

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
SAME_DISTANCE_TOLERANCE = 200
RECENT_WINDOW_SIZE = 5
APPENDED_FEATURE_NAMES = (
    "recent_soha_time_per_meter_avg5",
    "same_distance_soha_time_per_meter_avg5",
    "recent_race_relative_time_z_avg5",
    "recent_condition_par_time_residual_avg5",
    "bataiju_avg5",
    "weight_trend_5",
    "weight_volatility_5",
)
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
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Focused production mode keibajo_code:race_bango. The input parquet "
            "is already scoped by the base builder; this flag narrows history "
            "staging to target horses."
        ),
    )
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    attach_source_catalog(con, pg_url)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def target_horse_literals(con: duckdb.DuckDBPyConnection) -> str:
    """Return target horse IDs as an escaped SQL literal list for scan pushdown."""
    rows = con.execute(
        """
        select distinct ketto_toroku_bango
        from base_v3
        where ketto_toroku_bango is not null
        order by ketto_toroku_bango
        """
    ).fetchall()
    literals = ", ".join(sql_literal(str(row[0])) for row in rows)
    return literals or "null"


def stage_history(
    con: duckdb.DuckDBPyConnection,
    from_date: str,
    to_date: str,
    focused_target: bool = False,
    horse_literals: str | None = None,
) -> None:
    if horse_literals is not None:
        bataiju_filter = f"and ketto_toroku_bango in ({horse_literals})"
        rec_filter = f"and rec.ketto_toroku_bango in ({horse_literals})"
    elif focused_target:
        bataiju_filter = (
            "and ketto_toroku_bango in "
            "(select distinct ketto_toroku_bango from base_v3)"
        )
        rec_filter = (
            "and rec.ketto_toroku_bango in "
            "(select distinct ketto_toroku_bango from base_v3)"
        )
    else:
        bataiju_filter = ""
        rec_filter = ""
    con.execute(
        f"""
        create or replace temp table bataiju_hist as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
               try_cast(nullif(trim(bataiju), '') as double) as bataiju
        from pg.jvd_se
        where (kaisai_nen || kaisai_tsukihi) between '{from_date}' and '{to_date}'
          and kaisai_nen between '{from_date[:4]}' and '{to_date[:4]}'
          and ketto_toroku_bango is not null
          {bataiju_filter}
        """
    )
    soha_time_tenths = encoded_race_time_tenths_sql("rec.soha_time")
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
          rec.track_code,
          case
            when try_cast(rec.track_code as int) between 10 and 22
              then rec.babajotai_code_shiba
            else rec.babajotai_code_dirt
          end as track_condition,
          cast({soha_time_tenths} as double) as soha_time,
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
          and rec.kaisai_nen between '{from_date[:4]}' and '{to_date[:4]}'
          and rec.ketto_toroku_bango is not null
          {rec_filter}
        """
    )
    con.execute(
        "create index rec_hist_horse_date on rec_hist (source, ketto_toroku_bango, race_date)"
    )


def stage_race_time_context(con: duckdb.DuckDBPyConnection) -> None:
    """Build same-race controls and strictly prior-day condition pars."""
    con.execute(
        """
        create or replace temp table race_clock as
        select
          source, race_date, keibajo_code, race_bango, track_code, track_condition, kyori,
          median(soha_time / nullif(kyori, 0)) as race_time_per_meter_median,
          stddev_pop(soha_time / nullif(kyori, 0)) as race_time_per_meter_std
        from rec_hist
        where soha_time is not null and kyori > 0
        group by source, race_date, keibajo_code, race_bango, track_code, track_condition, kyori
        """
    )
    con.execute(
        """
        create or replace temp table condition_day_clock as
        select
          source, race_date, keibajo_code, track_code, track_condition, kyori,
          median(race_time_per_meter_median) as condition_day_time_per_meter
        from race_clock
        group by source, race_date, keibajo_code, track_code, track_condition, kyori
        """
    )
    con.execute(
        """
        create or replace temp table condition_day_par as
        select
          *,
          median(condition_day_time_per_meter) over (
            partition by source, keibajo_code, track_code, track_condition, kyori
            order by race_date
            rows between 64 preceding and 1 preceding
          ) as prior_condition_time_per_meter
        from condition_day_clock
        """
    )
    con.execute(
        """
        create or replace temp table rec_hist_context as
        select
          h.*,
          (h.soha_time / nullif(h.kyori, 0) - c.race_time_per_meter_median)
            / nullif(c.race_time_per_meter_std, 0) as race_relative_time_z,
          h.soha_time / nullif(h.kyori, 0) - p.prior_condition_time_per_meter
            as prior_condition_time_residual
        from rec_hist h
        left join race_clock c
          using (source, race_date, keibajo_code, race_bango, track_code, track_condition, kyori)
        left join condition_day_par p
          using (source, race_date, keibajo_code, track_code, track_condition, kyori)
        """
    )


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
          h.race_relative_time_z as hist_race_relative_time_z,
          h.prior_condition_time_residual as hist_prior_condition_time_residual,
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
        join rec_hist_context h on h.source = t.source
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
          avg(hist_race_relative_time_z)
            filter (where recent_rank <= {RECENT_WINDOW_SIZE})
            as recent_race_relative_time_z_avg5,
          avg(hist_prior_condition_time_residual)
            filter (where recent_rank <= {RECENT_WINDOW_SIZE})
            as recent_condition_par_time_residual_avg5,
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


def append_features_sql(
    input_glob: str, existing_columns: Collection[str] = frozenset()
) -> str:
    additions = [
        name for name in APPENDED_FEATURE_NAMES if name not in existing_columns
    ]
    feature_projection = "".join(f",\n      h.{name}" for name in additions)
    return f"""
    with base_v3 as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    )
    select
      b.*{feature_projection}
    from base_v3 b
    left join horse_history_agg h
      using ({RACE_PARTITION}, ketto_toroku_bango)
    """


def write_partitioned(
    con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path
) -> None:
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
    con.execute(
        f"""
        create or replace temp table base_v3 as
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kyori, race_date
        from read_parquet('{input_glob}', hive_partitioning=true)
        """
    )
    horse_literals = target_horse_literals(con)
    stage_history(
        con,
        args.from_date,
        args.to_date,
        args.target_race is not None,
        horse_literals,
    )
    stage_race_time_context(con)
    stage_horse_history_lookup(con, input_glob)
    stage_horse_history_agg(con)
    input_columns = {
        str(row[0])
        for row in con.execute(
            f"describe select * from read_parquet('{input_glob}', hive_partitioning=true)"
        ).fetchall()
    }
    write_partitioned(
        con,
        append_features_sql(input_glob, input_columns),
        args.output_dir,
    )
    con.close()


if __name__ == "__main__":
    main()
