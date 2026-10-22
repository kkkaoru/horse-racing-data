#!/usr/bin/env python3
"""Append JRA pedigree-combination ("nick") and higher-grade course features.

* Nick: sire x broodmare-sire (registration ids) win/top-3 rates shrunk toward
  the sire's own rate, the lift over the sire, and broader combinations (sire x
  broodmare-sire lineage, the nick per surface and per distance band).
* Higher-grade course: for each runner, stats from races of a strictly higher
  class at the same venue, exact distance and turf/dirt surface (sire top-3
  rate there, the runner's best time there vs the higher-class winning par,
  the runner's usual 4th-corner position vs where higher-class placers ran).
* Higher-grade par: each past run's time against the open-class-and-above
  winning time for that run's venue, distance, surface and going (per 1000m).

Every value comes from per-key cumulative totals over finished races on
strictly earlier days, joined with ``ASOF ... t.race_day > p.race_day``. The
same join serves historical rows (training) and upcoming rows (serving), and
same-day results never leak (they are not in the catalog at serve time).
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import Final

import duckdb

from _catalog_attach import attach_source_catalog

from _resource_defaults import add_resource_args, apply_to_connection

DEFAULT_PG_URL: Final[str] = "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
JRA: Final[str] = "('01','02','03','04','05','06','07','08','09','10')"
NICK_K: Final[float] = 20.0
COMBO_K: Final[float] = 40.0
COURSE_K: Final[float] = 20.0
BASE_WIN_RATE: Final[float] = 0.0707
BASE_TOP3_RATE: Final[float] = 0.212
# Open class and above set the winning-time par a runner's past times are measured against.
PAR_MIN_CLASS_RANK: Final[int] = 4
PAR_MIN_RACES: Final[int] = 3
# Highest class rank (G1); no race is above it, so its course features stay null.
TOP_CLASS_RANK: Final[int] = 9
KEYS: Final[str] = "kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango"
FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "nick_win_eb",
    "nick_top3_eb",
    "nick_top3_lift",
    "nick_logn",
    "nick_surface_top3_eb",
    "sire_bms_keito_surface_top3_lift",
    "nick_dist_surface_top3_eb",
    "nick_top3_lift_rank_in_race",
    "hg_course_logn",
    "hg_sire_course_top3_eb",
    "hg_sire_course_top3_lift",
    "hg_time_gap",
    "hg_style_gap",
    "hg_time_gap_rank_in_race",
    "hg_par_gap_mean",
    "hg_par_gap_best",
    "hg_par_gap_best_rank_in_race",
)

# Class order: 新馬/未勝利 < 1勝 < 2勝 < 3勝 < OP < L < 重賞/Jpn < G3 < G2 < G1.
CLASS_RANK_SQL: Final[str] = """
  case
    when ra.grade_code = 'A' then 9
    when ra.grade_code = 'B' then 8
    when ra.grade_code = 'C' then 7
    when ra.grade_code in ('D','F','G','H') then 6
    when ra.grade_code = 'L' then 5
    when ra.kyoso_joken_code = '999' then 4
    when ra.kyoso_joken_code = '016' then 3
    when ra.kyoso_joken_code = '010' then 2
    when ra.kyoso_joken_code = '005' then 1
    else 0 end
"""
SURFACE_SQL: Final[str] = """
  case
    when ra.track_code between '10' and '22' then 't'
    when ra.track_code between '23' and '26' then 'd'
    else null end
"""
KYORI_BAND_SQL: Final[str] = """
  case
    when cast(ra.kyori as int) < 1400 then 0
    when cast(ra.kyori as int) < 1800 then 1
    when cast(ra.kyori as int) < 2200 then 2
    else 3 end
"""
# soha_time is MSST (1:34.2 -> '1342'); 0 means no time recorded.
TIME_SECONDS_SQL: Final[str] = """
  case when try_cast(se.soha_time as int) > 0
    then (try_cast(se.soha_time as int) // 1000) * 60
       + (try_cast(se.soha_time as int) % 1000) / 10.0
    end
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_jra_nick_grade_course_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url", type=str, default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL)
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default="20080101",
        help="History lower bound (YYYYMMDD).",
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def runner_select_sql(from_year: int) -> str:
    """JRA flat runners with race attributes and pedigree keys (finished or not)."""
    return f"""
      select
        se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code,
        lpad(cast(cast(se.race_bango as int) as varchar),2,'0') as race_bango,
        se.ketto_toroku_bango,
        cast(se.kaisai_nen || se.kaisai_tsukihi as int) as race_day,
        cast(ra.kyori as int) as kyori,
        {SURFACE_SQL} as surface,
        {KYORI_BAND_SQL} as kyori_band,
        case when ra.track_code between '10' and '22' then ra.babajotai_code_shiba
          else ra.babajotai_code_dirt end as going,
        {CLASS_RANK_SQL} as class_rank,
        nullif(trim(um.ketto_joho_01a), '') as sire_id,
        nullif(trim(um.ketto_joho_05a), '') as bms_id,
        bk.keito_id as bms_keito,
        try_cast(se.kakutei_chakujun as int) as chakujun,
        {TIME_SECONDS_SQL} as time_s,
        case when try_cast(se.corner_4 as int) > 0 and try_cast(ra.shusso_tosu as int) > 0
          then try_cast(se.corner_4 as int) / try_cast(ra.shusso_tosu as double) end
          as corner4_norm
      from pg.jvd_se se
      join pg.jvd_ra ra
        on se.kaisai_nen = ra.kaisai_nen and se.kaisai_tsukihi = ra.kaisai_tsukihi
       and se.keibajo_code = ra.keibajo_code and se.race_bango = ra.race_bango
      join pg.jvd_um um on se.ketto_toroku_bango = um.ketto_toroku_bango
      left join keito_by_name bk on trim(both '　' from trim(um.ketto_joho_05b)) = bk.nm
      where se.keibajo_code in {JRA}
        and cast(se.kaisai_nen as int) >= {from_year}
    """


def stage_runners(con: duckdb.DuckDBPyConnection, from_year: int, input_glob: str) -> None:
    """Stage finished history (``hist``) and the input rows to score (``targets``)."""
    # Lineage resolves through the horse name like the jockey/pedigree cell layer.
    con.execute(
        """
        create or replace temp table keito_by_name as
        select trim(both '　' from trim(hn.bamei)) as nm, min(bt.keito_id) as keito_id
        from pg.jvd_hn hn
        join pg.jvd_bt bt on hn.hanshoku_toroku_bango = bt.hanshoku_toroku_bango
        group by 1
        """
    )
    con.execute(
        f"create or replace temp table runners as {runner_select_sql(from_year)}"
    )
    con.execute(
        """
        create or replace temp table hist as
        select *,
          case when chakujun = 1 then 1.0 else 0.0 end as is_win,
          case when chakujun <= 3 then 1.0 else 0.0 end as is_top3
        from runners
        where chakujun > 0 and surface is not null
        """
    )
    con.execute(
        f"""
        create or replace temp table targets as
        select distinct r.* exclude (chakujun, time_s, corner4_norm)
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
        join runners r
          on b.kaisai_nen = r.kaisai_nen
         and b.kaisai_tsukihi = r.kaisai_tsukihi
         and b.keibajo_code = r.keibajo_code
         and lpad(cast(cast(b.race_bango as int) as varchar),2,'0') = r.race_bango
         and b.ketto_toroku_bango = r.ketto_toroku_bango
        """
    )


def cumulative_sql(table: str, keys: str, aggregates: dict[str, str], where: str = "true") -> str:
    """Per-key, per-day totals accumulated through each day (inclusive)."""
    daily = ", ".join(f"{expr} as {name}" for name, expr in aggregates.items())
    running = ", ".join(
        f"{'min' if name.startswith('min_') else 'sum'}({name}) over w as {name}"
        for name in aggregates
    )
    return f"""
      select {keys}, race_day, {running}
      from (select {keys}, race_day, {daily} from {table} where {where} group by all)
      window w as (partition by {keys} order by race_day rows unbounded preceding)
    """


def stage_cumulative_tables(con: duckdb.DuckDBPyConnection) -> None:
    rates = {"w": "sum(is_win)", "t3": "sum(is_top3)", "n": "count(*)"}
    top3 = {"t3": "sum(is_top3)", "n": "count(*)"}
    tables = {
        "c_sire": cumulative_sql("hist", "sire_id", rates, "sire_id is not null"),
        "c_nick": cumulative_sql(
            "hist", "sire_id, bms_id", rates, "sire_id is not null and bms_id is not null"
        ),
        "c_nick_surface": cumulative_sql(
            "hist", "sire_id, bms_id, surface", top3,
            "sire_id is not null and bms_id is not null",
        ),
        "c_sire_bms_keito": cumulative_sql(
            "hist", "sire_id, bms_keito, surface", top3,
            "sire_id is not null and bms_keito is not null",
        ),
        "c_nick_dist_surface": cumulative_sql(
            "hist", "sire_id, bms_id, surface, kyori_band", top3,
            "sire_id is not null and bms_id is not null",
        ),
        # A finished race of rank r counts for every target tier 1..r (tier = target rank + 1).
        "c_hg_course": cumulative_sql(
            "hist_tiers", "keibajo_code, kyori, surface, tier",
            {
                "win_time_sum": "sum(time_s) filter (where chakujun = 1)",
                "win_time_n": "count(time_s) filter (where chakujun = 1)",
                "c4_sum": "sum(corner4_norm) filter (where chakujun <= 3)",
                "c4_n": "count(corner4_norm) filter (where chakujun <= 3)",
            },
        ),
        "c_hg_sire_course": cumulative_sql(
            "hist_tiers", "sire_id, keibajo_code, kyori, surface, tier", top3,
            "sire_id is not null",
        ),
        "c_horse_course": cumulative_sql(
            "hist", "ketto_toroku_bango, keibajo_code, kyori, surface",
            {"min_time": "min(time_s)"},
        ),
        "c_par": cumulative_sql(
            "hist", "keibajo_code, kyori, surface, going",
            {
                "par_sum": "sum(time_s) filter (where chakujun = 1)",
                "par_n": "count(time_s) filter (where chakujun = 1)",
            },
            f"class_rank >= {PAR_MIN_CLASS_RANK}",
        ),
        "c_horse": cumulative_sql(
            "hist", "ketto_toroku_bango",
            {"c4_sum": "sum(corner4_norm)", "c4_n": "count(corner4_norm)"},
        ),
    }
    con.execute(
        f"""
        create or replace temp table hist_tiers as
        select h.*, tier
        from hist h, range(1, {TOP_CLASS_RANK} + 1) as tiers(tier)
        where tier <= h.class_rank
        """
    )
    for name, sql in tables.items():
        con.execute(f"create or replace temp table {name} as {sql}")
    par_join = asof("p", "c_par", ("keibajo_code", "kyori", "surface", "going"), left="h")
    con.execute(
        f"""
        create or replace temp table hist_par_gap as
        select h.ketto_toroku_bango, h.race_day,
          (h.time_s - p.par_sum / p.par_n) * 1000.0 / h.kyori as par_gap
        from hist h
        {par_join}
        where p.par_n >= {PAR_MIN_RACES} and h.time_s is not null
        """
    )
    horse_par = cumulative_sql(
        "hist_par_gap", "ketto_toroku_bango",
        {"gap_sum": "sum(par_gap)", "gap_n": "count(par_gap)", "min_gap": "min(par_gap)"},
    )
    con.execute(f"create or replace temp table c_horse_par as {horse_par}")


def asof(alias: str, table: str, keys: tuple[str, ...], left: str = "t") -> str:
    conditions = " and ".join(f"{left}.{key} = {alias}.{key}" for key in keys)
    return (
        f"asof left join {table} {alias} on {conditions}"
        f" and {left}.race_day > {alias}.race_day"
    )


def features_sql() -> str:
    joins = "\n".join(
        (
            asof("so", "c_sire", ("sire_id",)),
            asof("nk", "c_nick", ("sire_id", "bms_id")),
            asof("nks", "c_nick_surface", ("sire_id", "bms_id", "surface")),
            asof("sbk", "c_sire_bms_keito", ("sire_id", "bms_keito", "surface")),
            asof("nkd", "c_nick_dist_surface", ("sire_id", "bms_id", "surface", "kyori_band")),
            asof("hc", "c_hg_course", ("keibajo_code", "kyori", "surface", "tier")),
            asof("hs", "c_hg_sire_course", ("sire_id", "keibajo_code", "kyori", "surface", "tier")),
            asof("bt", "c_horse_course", ("ketto_toroku_bango", "keibajo_code", "kyori", "surface")),
            asof("hr", "c_horse", ("ketto_toroku_bango",)),
            asof("hp", "c_horse_par", ("ketto_toroku_bango",)),
        )
    )
    return f"""
    with t as (
      select *, case when class_rank < {TOP_CLASS_RANK} then class_rank + 1 end as tier
      from targets
    ),
    raw as (
      select t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
        t.ketto_toroku_bango, t.sire_id, t.bms_id,
        coalesce(so.w / nullif(so.n, 0), {BASE_WIN_RATE}) as sire_win,
        coalesce(so.t3 / nullif(so.n, 0), {BASE_TOP3_RATE}) as sire_t3,
        nk.w as nk_w, nk.t3 as nk_t3, coalesce(nk.n, 0) as nk_n,
        nks.t3 as nks_t3, coalesce(nks.n, 0) as nks_n,
        sbk.t3 as sbk_t3, coalesce(sbk.n, 0) as sbk_n, t.bms_keito,
        nkd.t3 as nkd_t3, coalesce(nkd.n, 0) as nkd_n,
        t.tier, hc.win_time_sum, hc.win_time_n, hc.c4_sum, hc.c4_n,
        hs.t3 as hs_t3, coalesce(hs.n, 0) as hs_n,
        bt.min_time, hr.c4_sum as horse_c4_sum, hr.c4_n as horse_c4_n,
        hp.gap_sum / nullif(hp.gap_n, 0) as hg_par_gap_mean, hp.min_gap as hg_par_gap_best
      from t
      {joins}
    ),
    f1 as (
      select *,
        (coalesce(nk_w, 0) + {NICK_K} * sire_win) / (nk_n + {NICK_K}) as nick_win_eb,
        (coalesce(nk_t3, 0) + {NICK_K} * sire_t3) / (nk_n + {NICK_K}) as nick_top3_eb
      from raw
    ),
    f2 as (
      select *,
        (coalesce(nks_t3, 0) + {NICK_K} * nick_top3_eb) / (nks_n + {NICK_K}) as nick_surface_top3_eb
      from f1
    ),
    f as (
      select {KEYS},
        case when sire_id is not null and bms_id is not null then nick_win_eb end as nick_win_eb,
        case when sire_id is not null and bms_id is not null then nick_top3_eb end as nick_top3_eb,
        case when sire_id is not null and bms_id is not null then nick_top3_eb - sire_t3 end
          as nick_top3_lift,
        ln(1 + nk_n) as nick_logn,
        case when sire_id is not null and bms_id is not null then nick_surface_top3_eb end
          as nick_surface_top3_eb,
        case when sire_id is not null and bms_keito is not null then
          (coalesce(sbk_t3, 0) + {COMBO_K} * sire_t3) / (sbk_n + {COMBO_K}) - sire_t3 end
          as sire_bms_keito_surface_top3_lift,
        case when sire_id is not null and bms_id is not null then
          (coalesce(nkd_t3, 0) + {NICK_K} * nick_surface_top3_eb) / (nkd_n + {NICK_K}) end
          as nick_dist_surface_top3_eb,
        case when tier is not null then ln(1 + coalesce(win_time_n, 0)) end as hg_course_logn,
        case when tier is not null and sire_id is not null then
          (coalesce(hs_t3, 0) + {COURSE_K} * sire_t3) / (hs_n + {COURSE_K}) end
          as hg_sire_course_top3_eb,
        case when tier is not null and sire_id is not null then
          (coalesce(hs_t3, 0) + {COURSE_K} * sire_t3) / (hs_n + {COURSE_K}) - sire_t3 end
          as hg_sire_course_top3_lift,
        min_time - win_time_sum / nullif(win_time_n, 0) as hg_time_gap,
        abs(horse_c4_sum / nullif(horse_c4_n, 0) - c4_sum / nullif(c4_n, 0)) as hg_style_gap,
        hg_par_gap_mean, hg_par_gap_best
      from f2
    )
    select *,
      {rank_in_race_sql("nick_top3_lift", "desc")} as nick_top3_lift_rank_in_race,
      {rank_in_race_sql("hg_time_gap", "asc")} as hg_time_gap_rank_in_race,
      {rank_in_race_sql("hg_par_gap_best", "asc")} as hg_par_gap_best_rank_in_race
    from f
    """


def rank_in_race_sql(column: str, direction: str) -> str:
    """Average rank among runners with a value (1 = best), null when the value is null."""
    race = "kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
    return f"""case when {column} is null then null else (
      rank() over (partition by {race}, ({column} is null) order by {column} {direction})
      + (count(*) over (partition by {race}, {column}) - 1) / 2.0
    ) end"""


def append_features_sql(input_glob: str) -> str:
    selected = ", ".join(f"x.{column}" for column in FEATURE_COLUMNS)
    return f"""
    select b.*, {selected}
    from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
    left join jra_nick_grade_course_features x
      on b.kaisai_nen = x.kaisai_nen
     and b.kaisai_tsukihi = x.kaisai_tsukihi
     and b.keibajo_code = x.keibajo_code
     and lpad(cast(cast(b.race_bango as int) as varchar),2,'0') = x.race_bango
     and b.ketto_toroku_bango = x.ketto_toroku_bango
    """


def write_partitioned(con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"copy ({sql}) to '{output_dir.as_posix()}' "
        "(format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )


def build(con: duckdb.DuckDBPyConnection, from_year: int, input_glob: str) -> None:
    stage_runners(con, from_year, input_glob)
    stage_cumulative_tables(con)
    con.execute(
        "create or replace temp table jra_nick_grade_course_features as " + features_sql()
    )


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    attach_source_catalog(con, args.pg_url)
    build(con, int(str(args.from_date)[:4]), input_glob)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
