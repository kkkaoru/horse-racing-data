#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append horse-locality features (D2a probe layer).

Motivation:
  NAR Funabashi (43) / Kawasaki (44) suffer a ~7pp model-specific top1 gap vs
  the NAR average. Root-cause (d2a-nar-venue-gap-diagnosis.md): ~40% of 43/44
  horses are "locally-anchored" — they run ≥70% of their career at these two
  venues and therefore have NO running-style corner history from other NAR venues.
  XGBoost's learned NULL-routing branch is already competitive with mean
  imputation (d2a-rs-impute-feasibility.md, ABORT). The correct mechanism is to
  give the model an explicit locality signal so it can condition on
  local-anchoredness WITHOUT destroying its NULL-routing path.

Features added (per horse × race):
  - pct_career_at_keibajo        : fraction of prior races at the CURRENT keibajo
                                   (float in [0, 1]; NULL when n_career_races_total == 0)
  - n_career_races_at_keibajo    : prior count at current venue (integer, 0 for debut)
  - n_career_races_total         : prior total races across all venues (integer, 0 for debut)
  - n_distinct_keibajo           : number of distinct venues the horse has raced at prior
                                   (integer, 0 for debut; 1 = only ever raced at one venue)
  - rs_features_null_flag        : 1 when ALL four RS rate columns are simultaneously NULL
                                   (past_nige/senkou/sashi/oikomi_rate_self), else 0

Data leakage prevention:
  All counts and rates are computed from the horse's PRIOR races only.
  The JOIN condition uses race_date STRICTLY LESS THAN the current race's
  race_date. The target race row is excluded from its own career aggregate.

Run with:
  apps/pc-keiba-viewer/.venv/bin/python \\
    apps/pc-keiba-viewer/src/scripts/finish-position-features/add-horse-locality-features.py \\
    --input-dir tmp/feat-nar-v7-final \\
    --output-dir tmp/feat-nar-d2a-locality
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"

# RS rate trigger columns — all four must be NULL to set rs_features_null_flag=1.
RS_NULL_TRIGGER_COLS = (
    "past_nige_rate_self",
    "past_senkou_rate_self",
    "past_sashi_rate_self",
    "past_oikomi_rate_self",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_horse_locality_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_race_history(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """Stage all historical race entries (source-agnostic) for locality aggregation.

    We only need ketto_toroku_bango, race_date, and keibajo_code to compute the
    four locality features. NULL ketto_toroku_bango rows are excluded.
    """
    con.execute(
        f"""
        create or replace temp table race_history as
        select
          source,
          race_date,
          keibajo_code,
          ketto_toroku_bango
        from pg.race_entry_corner_features
        where race_date >= '{from_date}'
          and ketto_toroku_bango is not null
          and trim(ketto_toroku_bango) != ''
        """
    )
    con.execute(
        "create index race_history_locality_idx on race_history (ketto_toroku_bango, race_date)"
    )


def stage_locality_aggregates(con: duckdb.DuckDBPyConnection) -> None:
    """Compute per (horse, race) locality aggregates using strictly-prior-race history.

    For each horse × current race, aggregates all history rows where:
      race_date < current race_date  (strict less-than = no leakage)

    Emits one row per (race_partition, ketto_toroku_bango) with:
      n_prior_total      : total prior races across all venues
      n_prior_at_venue   : prior races at the SAME keibajo_code as the current race
      n_distinct_keibajo : number of distinct keibajo_codes in prior history

    Debut horses (no prior history) will have 0 for counts and NULL for pct.
    """
    con.execute(
        """
        create or replace temp table locality_aggregates as
        with current_races as (
          select distinct
            source,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            race_date,
            ketto_toroku_bango
          from base_input
        ),
        agg as (
          select
            cr.source,
            cr.kaisai_nen,
            cr.kaisai_tsukihi,
            cr.keibajo_code,
            cr.race_bango,
            cr.ketto_toroku_bango,
            count(rh.race_date) as n_prior_total,
            count(
              case when rh.keibajo_code = cr.keibajo_code then 1 end
            ) as n_prior_at_venue,
            count(distinct rh.keibajo_code) as n_distinct_keibajo
          from current_races cr
          left join race_history rh
            on rh.ketto_toroku_bango = cr.ketto_toroku_bango
            and rh.race_date < cr.race_date
          group by
            cr.source,
            cr.kaisai_nen,
            cr.kaisai_tsukihi,
            cr.keibajo_code,
            cr.race_bango,
            cr.ketto_toroku_bango
        )
        select
          source,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          ketto_toroku_bango,
          n_prior_total::integer as n_career_races_total,
          n_prior_at_venue::integer as n_career_races_at_keibajo,
          n_distinct_keibajo::integer as n_distinct_keibajo,
          case
            when n_prior_total > 0
            then n_prior_at_venue::double / n_prior_total::double
            else null
          end as pct_career_at_keibajo
        from agg
        """
    )
    con.execute(
        f"create index locality_agg_idx on locality_aggregates ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def stage_base_input(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """Stage current race identity columns from the input parquet.

    Only the race-identifier columns + RS trigger columns are needed to
    (a) drive the locality aggregation LEFT JOIN and (b) compute rs_features_null_flag.
    """
    rs_null_conds = " and ".join(f"{col} is null" for col in RS_NULL_TRIGGER_COLS)
    con.execute(
        f"""
        create or replace temp table base_input as
        select
          source,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          race_date,
          ketto_toroku_bango,
          case when {rs_null_conds} then 1 else 0 end as rs_features_null_flag
        from read_parquet('{input_glob}', hive_partitioning=true)
        """
    )
    con.execute(
        f"create index base_input_locality_idx on base_input ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    """Return the SQL that LEFT JOINs locality_aggregates onto the base parquet.

    Debut horses (no prior history) receive 0 for n_career_races_total,
    0 for n_career_races_at_keibajo, 0 for n_distinct_keibajo, and NULL for
    pct_career_at_keibajo. rs_features_null_flag is always 0 or 1.
    """
    rs_null_conds = " and ".join(
        f"b.{col} is null" for col in RS_NULL_TRIGGER_COLS
    )
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    joined as (
      select
        b.*,
        coalesce(la.n_career_races_total, 0) as n_career_races_total,
        coalesce(la.n_career_races_at_keibajo, 0) as n_career_races_at_keibajo,
        la.pct_career_at_keibajo,
        coalesce(la.n_distinct_keibajo, 0) as n_distinct_keibajo,
        case when {rs_null_conds} then 1 else 0 end as rs_features_null_flag
      from base b
      left join locality_aggregates la
        on la.source = b.source
        and la.kaisai_nen = b.kaisai_nen
        and la.kaisai_tsukihi = b.kaisai_tsukihi
        and la.keibajo_code = b.keibajo_code
        and la.race_bango = b.race_bango
        and la.ketto_toroku_bango = b.ketto_toroku_bango
    )
    select * from joined
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
    stage_base_input(con, input_glob)
    stage_race_history(con, args.from_date)
    stage_locality_aggregates(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
