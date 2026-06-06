#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append iter26 relationship R1 horse / race physics features (v8 iter26 layer).

Motivation:
  Iter 26 extends iter 14's 260-column parquet with twelve features that
  capture the **馬体重 × 斤量 × 馬齢 × 距離 × タイム** relationship surface.
  The sibling TS PG UPDATE builders (run against the production
  ``race_finish_position_features`` table) live next to this script:
    - build-relationship-physics-sql.ts (within-row + within-race signals)
    - build-relationship-history-sql.ts (past-race normalized speed signals)

  This Python script is the **offline / parquet-side mirror**: it joins iter
  14's base parquet against the same PG history rows but emits the twelve new
  columns directly, so the training dataset can be rebuilt without an
  intermediate PG round-trip.

Features added (per horse x race) — exactly 12 columns, 260 -> 272:

  Group A — within-row physics (3):
    - bataiju_futan_ratio          = futan_juryo / nullif(bataiju, 0)
    - futan_per_barei              = futan_juryo / nullif(barei, 0)
    - bataiju_per_kyori_log        = bataiju / ln(1 + kyori)

  Group B — within-race relative (4):
    - bataiju_diff_from_race_mean         = bataiju - avg(bataiju) over race
    - bataiju_rank_in_race                = rank() over race by bataiju desc
    - futan_minus_bataiju_zscore_in_race  = (joint_ratio - mean) / stddev_pop
    - barei_diff_from_race_mean           = barei - avg(barei) over race

  Group C — history normalized (3, recent <=5 races filter):
    - past_speed_kg_normalized_avg5     = avg(soha_time / kyori * bataiju)
    - past_speed_futan_normalized_avg5  = avg(soha_time / kyori * futan_juryo)
    - past_speed_age_adjusted_avg5      = avg((soha_time / kyori) / barei)

  Group D — consistency (2, recent <=5 races filter):
    - past_speed_volatility_5             = stddev_pop(soha_time / kyori)
    - past_finish_position_volatility_5   = stddev_pop(finish_position::double)

Data leakage prevention: every past-race aggregate uses strict ``race_date <
target_race_date`` so the target row is never included in its own history.
``soha_time``, ``barei``, ``futan_juryo``, ``kyori`` come from PG
``race_entry_corner_features``; ``bataiju`` lives only on the per-category
runner table (``jvd_se`` / ``nvd_se``) and must be JOINed in separately.

Rows without a populated history (new horse / no eligible past race) emit
NULL — gradient boosters treat missing inputs as a learned default so no
imputation is applied here.
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

# How many of the most recent past races contribute to history aggregates.
# Mirrors build-relationship-history-sql.ts RECENT_HISTORY_WINDOW_SIZE.
RECENT_HISTORY_WINDOW_SIZE = 5

# YYYYMMDD-arithmetic history lookback bound (target_race_date - this number).
# Mirrors build-relationship-history-sql.ts HISTORY_LOOKBACK_DAYS_YYYYMMDD
# (kept as the equivalent constant name in the docs even though the value
# represents YYYYMMDD-space delta rather than calendar days).
HISTORY_LOOKBACK_DAYS = 100000


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_relationship_r1_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--category",
        choices=("jra", "nar", "ban-ei", "all"),
        default="jra",
        help=(
            "jra -> pg.jvd_se source filter; "
            "nar -> pg.nvd_se source filter (keibajo_code <> '83'); "
            "ban-ei -> pg.nvd_se source filter (keibajo_code = '83'); "
            "all -> no source / keibajo filter"
        ),
    )
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


def source_filter_sql(category: str) -> str:
    """SQL fragment filtering ``race_entry_corner_features`` (alias rec)."""
    if category == "jra":
        return "rec.source = 'jra'"
    if category == "nar":
        return "rec.source = 'nar' and rec.keibajo_code <> '83'"
    if category == "ban-ei":
        return "rec.source = 'nar' and rec.keibajo_code = '83'"
    return "true"


def se_table_for(category: str) -> str:
    """Per-category runner table (carries ``bataiju``).

    JRA rows live in pg.jvd_se; NAR + Ban-ei both live in pg.nvd_se.
    """
    if category == "jra":
        return "pg.jvd_se"
    return "pg.nvd_se"


def safe_bataiju_cast_sql(alias: str) -> str:
    """SQL CASE that safely parses bataiju (text) -> integer.

    Mirrors build-relationship-physics-sql.ts safeBataijuCast — non-numeric
    text becomes NULL instead of raising.
    """
    return (
        f"case when trim(coalesce({alias}.bataiju, '')) ~ '^-?[0-9]+$' "
        f"then trim({alias}.bataiju)::integer else null end"
    )


def stage_base_input(
    con: duckdb.DuckDBPyConnection,
    input_glob: str,
    category: str,
) -> None:
    """Load iter14 parquet + LEFT JOIN PG to pull kyoso physics scalars.

    The iter14 base parquet does NOT propagate ``futan_juryo`` / ``barei`` /
    ``bataiju`` for the **target** row (they are derived feature inputs, not
    raw columns of the parquet). To compute Group A + Group B without a PG
    round-trip downstream we project them now via:
      - rec.futan_juryo, rec.barei, rec.kyori from race_entry_corner_features
      - bataiju from the per-category se table (text -> integer cast)

    The JOIN key is the standard race-entry tuple including ``umaban`` so the
    correct row in race_entry_corner_features is identified.
    """
    se_table = se_table_for(category)
    bataiju_sql = safe_bataiju_cast_sql("se")
    con.execute(
        f"""
        create or replace temp table base_input as
        select
          b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango,
          b.ketto_toroku_bango, b.race_date, b.race_year,
          rec.kyori::double as kyori,
          rec.futan_juryo::double as futan_juryo,
          rec.barei::double as barei,
          {bataiju_sql}::double as bataiju
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
        left join pg.race_entry_corner_features rec
          on rec.source = b.source
          and rec.kaisai_nen = b.kaisai_nen
          and rec.kaisai_tsukihi = b.kaisai_tsukihi
          and rec.keibajo_code = b.keibajo_code
          and rec.race_bango = b.race_bango
          and rec.umaban = b.umaban
        left join {se_table} se
          on se.kaisai_nen = b.kaisai_nen
          and se.kaisai_tsukihi = b.kaisai_tsukihi
          and se.keibajo_code = b.keibajo_code
          and se.race_bango = b.race_bango
          and se.ketto_toroku_bango = b.ketto_toroku_bango
        """
    )
    con.execute(
        f"create index base_input_idx on base_input ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def stage_race_history(
    con: duckdb.DuckDBPyConnection, from_date: str, category: str
) -> None:
    """horse past races + soha_time + kyori + bataiju + futan + barei + finish_position.

    Joins ``pg.race_entry_corner_features`` against the per-category se_table
    to pick up ``bataiju`` which is not stored on the corner table. Filters
    history to rows where ``soha_time``, ``kyori`` are populated so downstream
    speed-normalized averages can safely divide.
    """
    se_table = se_table_for(category)
    src_filter = source_filter_sql(category)
    bataiju_sql = safe_bataiju_cast_sql("se")
    con.execute(
        f"""
        create or replace temp table race_history as
        select
          rec.source,
          rec.race_date,
          rec.kaisai_nen,
          rec.kaisai_tsukihi,
          rec.keibajo_code,
          rec.race_bango,
          rec.ketto_toroku_bango,
          rec.finish_position::double as finish_position,
          rec.kyori::double as kyori,
          rec.soha_time::double as soha_time,
          rec.futan_juryo::double as futan_juryo,
          rec.barei::double as barei,
          {bataiju_sql}::double as bataiju
        from pg.race_entry_corner_features rec
        left join {se_table} se
          on se.kaisai_nen = rec.kaisai_nen
          and se.kaisai_tsukihi = rec.kaisai_tsukihi
          and se.keibajo_code = rec.keibajo_code
          and se.race_bango = rec.race_bango
          and se.ketto_toroku_bango = rec.ketto_toroku_bango
        where rec.race_date >= '{from_date}'
          and rec.finish_position is not null
          and rec.kyori is not null
          and rec.kyori > 0
          and rec.soha_time is not null
          and {src_filter}
        """
    )
    con.execute(
        "create index race_history_horse_idx on race_history "
        "(source, ketto_toroku_bango, race_date)"
    )


def stage_race_relative(con: duckdb.DuckDBPyConnection) -> None:
    """Group A (within-row physics) + Group B (within-race relative).

    The seven columns are all derivable from ``base_input`` alone — no
    history join required. Window function partition mirrors
    build-relationship-physics-sql.ts so the SQL is symmetric with the PG
    UPDATE path.
    """
    con.execute(
        f"""
        create or replace temp table race_relative as
        with windowed as (
          select
            source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            ketto_toroku_bango,
            futan_juryo / nullif(bataiju, 0) as bataiju_futan_ratio,
            futan_juryo / nullif(barei, 0) as futan_per_barei,
            bataiju / ln(1 + kyori) as bataiju_per_kyori_log,
            bataiju - avg(bataiju) over race_window as bataiju_diff_from_race_mean,
            rank() over (
              partition by {RACE_PARTITION}
              order by bataiju desc nulls last
            ) as bataiju_rank_in_race,
            (futan_juryo / nullif(bataiju, 0)) as joint_ratio,
            avg(futan_juryo / nullif(bataiju, 0)) over race_window
              as joint_ratio_mean,
            stddev_pop(futan_juryo / nullif(bataiju, 0)) over race_window
              as joint_ratio_stddev,
            barei - avg(barei) over race_window as barei_diff_from_race_mean
          from base_input
          window race_window as (partition by {RACE_PARTITION})
        )
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          bataiju_futan_ratio,
          futan_per_barei,
          bataiju_per_kyori_log,
          bataiju_diff_from_race_mean,
          bataiju_rank_in_race,
          case
            when joint_ratio_stddev is null or joint_ratio_stddev = 0
            then null
            else (joint_ratio - joint_ratio_mean) / joint_ratio_stddev
          end as futan_minus_bataiju_zscore_in_race,
          barei_diff_from_race_mean
        from windowed
        """
    )
    con.execute(
        f"create index race_relative_idx on race_relative ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def stage_history_normalized(con: duckdb.DuckDBPyConnection) -> None:
    """Group C (history normalized speed avg) + Group D (consistency stddev).

    Builds the per-target-horse history aggregate from ``race_history`` using
    ``recent_rank <= RECENT_HISTORY_WINDOW_SIZE`` to limit to the 5 most
    recent past races. Strict ``race_date < target_race_date`` join prevents
    target leakage.
    """
    con.execute(
        f"""
        create or replace temp table history_normalized as
        with ranked as (
          select
            bi.source,
            bi.kaisai_nen,
            bi.kaisai_tsukihi,
            bi.keibajo_code,
            bi.race_bango,
            bi.ketto_toroku_bango,
            rh.soha_time as hist_soha_time,
            rh.kyori as hist_kyori,
            rh.bataiju as hist_bataiju,
            rh.futan_juryo as hist_futan_juryo,
            rh.barei as hist_barei,
            rh.finish_position as hist_finish_position,
            row_number() over (
              partition by bi.source, bi.kaisai_nen, bi.kaisai_tsukihi,
                           bi.keibajo_code, bi.race_bango, bi.ketto_toroku_bango
              order by rh.race_date desc
            ) as recent_rank
          from base_input bi
          join race_history rh
            on rh.source = bi.source
            and rh.ketto_toroku_bango = bi.ketto_toroku_bango
            and rh.race_date < bi.race_date
        )
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          avg(hist_soha_time / hist_kyori * hist_bataiju)
            filter (where recent_rank <= {RECENT_HISTORY_WINDOW_SIZE}
                     and hist_bataiju is not null)
            as past_speed_kg_normalized_avg5,
          avg(hist_soha_time / hist_kyori * hist_futan_juryo)
            filter (where recent_rank <= {RECENT_HISTORY_WINDOW_SIZE}
                     and hist_futan_juryo is not null)
            as past_speed_futan_normalized_avg5,
          avg((hist_soha_time / hist_kyori) / nullif(hist_barei, 0))
            filter (where recent_rank <= {RECENT_HISTORY_WINDOW_SIZE}
                     and hist_barei is not null)
            as past_speed_age_adjusted_avg5,
          stddev_pop(hist_soha_time / hist_kyori)
            filter (where recent_rank <= {RECENT_HISTORY_WINDOW_SIZE})
            as past_speed_volatility_5,
          stddev_pop(hist_finish_position::double)
            filter (where recent_rank <= {RECENT_HISTORY_WINDOW_SIZE})
            as past_finish_position_volatility_5
        from ranked
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
                 ketto_toroku_bango
        """
    )
    con.execute(
        f"create index history_normalized_idx on history_normalized ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    """Left-join the twelve iter26 columns onto the iter14 parquet base.

    Both staging tables are keyed on the full
    (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    ketto_toroku_bango) tuple — matches one horse x race row. ``left join``
    preserves all 260 input columns and emits NULL for rows that do not have
    a staging row (no eligible history / etc.).
    """
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
    ),
    joined as (
      select
        b.*,
        rr.bataiju_futan_ratio,
        rr.futan_per_barei,
        rr.bataiju_per_kyori_log,
        rr.bataiju_diff_from_race_mean,
        rr.bataiju_rank_in_race,
        rr.futan_minus_bataiju_zscore_in_race,
        rr.barei_diff_from_race_mean,
        hn.past_speed_kg_normalized_avg5,
        hn.past_speed_futan_normalized_avg5,
        hn.past_speed_age_adjusted_avg5,
        hn.past_speed_volatility_5,
        hn.past_finish_position_volatility_5
      from base b
      left join race_relative rr
        on rr.source = b.source
        and rr.kaisai_nen = b.kaisai_nen
        and rr.kaisai_tsukihi = b.kaisai_tsukihi
        and rr.keibajo_code = b.keibajo_code
        and rr.race_bango = b.race_bango
        and rr.ketto_toroku_bango = b.ketto_toroku_bango
      left join history_normalized hn
        on hn.source = b.source
        and hn.kaisai_nen = b.kaisai_nen
        and hn.kaisai_tsukihi = b.kaisai_tsukihi
        and hn.keibajo_code = b.keibajo_code
        and hn.race_bango = b.race_bango
        and hn.ketto_toroku_bango = b.ketto_toroku_bango
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
    stage_base_input(con, input_glob, args.category)
    stage_race_history(con, args.from_date, args.category)
    stage_race_relative(con)
    stage_history_normalized(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
