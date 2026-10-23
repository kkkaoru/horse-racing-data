#!/usr/bin/env python3
"""Append JRA pedigree-combination features over several ancestors.

Each feature is the lift of an ancestor pair (e.g. sire's sire x dam's sire's
sire) over the additive expectation of the two ancestors alone, under the same
condition (surface, optionally distance band): the pair's top-3 / win rate is
shrunk toward ``rate(a) + rate(b) - base`` and that expectation is subtracted.
Pairs cover the sire, sire's sire, sire's sire's sire, dam's sire, dam's sire's
sire, dam's dam's sire and the sire / dam-sire lineages (resolved by walking the
paternal line up to the nearest stallion in ``jvd_bt``). Also: the dam's other
runners' record, ancestor duplication in the 3-generation pedigree, and whether
the sire and dam-sire share a broad lineage.

Every value comes from per-key cumulative totals over finished races on strictly
earlier days, joined with ``ASOF ... t.race_day > p.race_day``. Query shape for
serving: every history table is first reduced to the keys the target rows carry
(a semi-join), so a one-day card only accumulates the few thousand history rows
that share an ancestor with that card, and the lineage walk starts only from the
stallions that appear in the staged pedigrees. Training runs the same SQL with
every row as a target, so serving and training values are identical.
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import Final, NamedTuple

import duckdb

from _catalog_attach import attach_source_catalog

from _resource_defaults import add_resource_args, apply_to_connection


class Pair(NamedTuple):
    """Ancestor pair feature: two key columns under a condition."""

    name: str
    left: str
    right: str
    condition: tuple[str, ...]
    rate: str


DEFAULT_PG_URL: Final[str] = "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
JRA: Final[str] = "('01','02','03','04','05','06','07','08','09','10')"
SHRINK_K: Final[float] = 30.0
BASE_RATE: Final[dict[str, float]] = {"t3": 0.212, "w": 0.0707}
# keito_id is hierarchical, two characters per level; six characters is the broad family.
BROAD_LINE_CHARS: Final[int] = 6
LINEAGE_MAX_DEPTH: Final[int] = 25
# jvd_um ketto_joho_NNa: 01 sire, 02 dam, 03 sire's sire, 05 dam's sire, 07 sire's sire's
# sire, 09 sire's dam's sire, 11 dam's sire's sire, 13 dam's dam's sire (14 = 3 generations).
ANCESTOR_COLUMNS: Final[dict[str, str]] = {
    "a01": "ketto_joho_01a",
    "a02": "ketto_joho_02a",
    "a03": "ketto_joho_03a",
    "a05": "ketto_joho_05a",
    "a07": "ketto_joho_07a",
    "a09": "ketto_joho_09a",
    "a11": "ketto_joho_11a",
    "a13": "ketto_joho_13a",
}
PEDIGREE_SIZE: Final[int] = 14
SURF: Final[tuple[str, ...]] = ("surface",)
SURF_DIST: Final[tuple[str, ...]] = ("surface", "kyori_band")
PAIRS: Final[tuple[Pair, ...]] = (
    Pair("ped_s_bms_dist_top3_lift", "a01", "a05", SURF_DIST, "t3"),
    Pair("ped_s_bms_surf_win_lift", "a01", "a05", SURF, "w"),
    Pair("ped_ss_bms_surf_top3_lift", "a03", "a05", SURF, "t3"),
    Pair("ped_s_bmss_surf_top3_lift", "a01", "a11", SURF, "t3"),
    Pair("ped_ss_bmss_surf_top3_lift", "a03", "a11", SURF, "t3"),
    Pair("ped_sss_bms_surf_top3_lift", "a07", "a05", SURF, "t3"),
    Pair("ped_s_dds_surf_top3_lift", "a01", "a13", SURF, "t3"),
    Pair("ped_sds_bms_surf_top3_lift", "a09", "a05", SURF, "t3"),
    Pair("ped_lines_surf_top3_lift", "sline", "bline", SURF, "t3"),
    Pair("ped_lines_dist_top3_lift", "sline", "bline", SURF_DIST, "t3"),
    Pair("ped_s_bline_dist_top3_lift", "a01", "bline", SURF_DIST, "t3"),
    Pair("ped_ss_bline_surf_top3_lift", "a03", "bline", SURF, "t3"),
)
RANKED: Final[tuple[str, ...]] = ("ped_s_bms_dist_top3_lift", "ped_dam_sib_top3_eb")
FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    *(pair.name for pair in PAIRS),
    "ped_pair_logn",
    "ped_dam_sib_top3_eb",
    "ped_dam_sib_win_eb",
    "ped_dam_sib_logn",
    "ped_inbreed_dups",
    "ped_same_broad_line",
    *(f"{column}_rank_in_race" for column in RANKED),
)
KEYS: Final[str] = "kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango"
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
RACE_BANGO_SQL: Final[str] = "lpad(cast(cast({alias}.race_bango as int) as varchar),2,'0')"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_jra_pedigree_combo_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url", type=str, default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL)
    )
    parser.add_argument(
        "--from-date", type=str, default="20080101", help="History lower bound (YYYYMMDD)."
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def stage_pedigrees(con: duckdb.DuckDBPyConnection, from_year: int) -> None:
    """Runner rows (finished or not) with ancestor ids; only the columns the layer reads."""
    ancestors = ", ".join(f"nullif(trim(um.{col}), '') as {key}" for key, col in ANCESTOR_COLUMNS.items())
    pedigree = ", ".join(f"nullif(trim(um.ketto_joho_{i:02d}a), '')" for i in range(1, PEDIGREE_SIZE + 1))
    con.execute(
        f"""
        create or replace temp table runners as
        select
          se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code,
          {RACE_BANGO_SQL.format(alias="se")} as race_bango, se.ketto_toroku_bango,
          cast(se.kaisai_nen || se.kaisai_tsukihi as int) as race_day,
          {SURFACE_SQL} as surface, {KYORI_BAND_SQL} as kyori_band, {ancestors},
          len(list_filter([{pedigree}], x -> x is not null))
            - len(list_distinct(list_filter([{pedigree}], x -> x is not null))) as inbreed_dups,
          try_cast(se.kakutei_chakujun as int) as chakujun
        from pg.jvd_se se
        join pg.jvd_ra ra
          on se.kaisai_nen = ra.kaisai_nen and se.kaisai_tsukihi = ra.kaisai_tsukihi
         and se.keibajo_code = ra.keibajo_code and se.race_bango = ra.race_bango
        join pg.jvd_um um on se.ketto_toroku_bango = um.ketto_toroku_bango
        where se.keibajo_code in {JRA} and se.kaisai_nen >= '{from_year:04d}'
        """
    )


def stage_lineage(con: duckdb.DuckDBPyConnection) -> None:
    """Lineage of every staged sire / dam-sire: walk sire links up to a ``jvd_bt`` stallion."""
    con.execute(
        """
        create or replace temp table stallions as
        select distinct id from (select a01 as id from runners union select a03 from runners
          union select a05 from runners) where id is not null
        """
    )
    con.execute(
        """
        create or replace temp table hn as
        select hanshoku_toroku_bango as id, any_value(nullif(trim(ketto_joho_01a), '')) as sire
        from pg.jvd_hn group by 1
        """
    )
    con.execute(
        "create or replace temp table bt as "
        "select hanshoku_toroku_bango as id, trim(keito_id) as keito from pg.jvd_bt"
    )
    con.execute(
        f"""
        create or replace temp table lineage as
        with recursive walk(start, cur, depth) as (
          select id, id, 0 from stallions
          union all
          select w.start, hn.sire, w.depth + 1
          from walk w join hn on hn.id = w.cur
          where hn.sire is not null and w.depth < {LINEAGE_MAX_DEPTH}
            and not exists (select 1 from bt where bt.id = w.cur)
        )
        select w.start as id, arg_min(bt.keito, w.depth) as keito
        from walk w join bt on bt.id = w.cur
        group by 1
        """
    )


def stage_rows(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """``hist`` = finished history, ``targets`` = the input rows, both with lineages."""
    con.execute(
        """
        create or replace temp table runners_l as
        select r.*, ls.keito as sline, lb.keito as bline,
          left(ls.keito, %d) as sline_broad, left(lb.keito, %d) as bline_broad
        from runners r
        left join lineage ls on ls.id = r.a01
        left join lineage lb on lb.id = r.a05
        """
        % (BROAD_LINE_CHARS, BROAD_LINE_CHARS)
    )
    con.execute(
        """
        create or replace temp table hist as
        select * exclude (chakujun),
          case when chakujun = 1 then 1.0 else 0.0 end as w,
          case when chakujun <= 3 then 1.0 else 0.0 end as t3
        from runners_l
        where chakujun > 0 and surface is not null
        """
    )
    con.execute(
        f"""
        create or replace temp table targets as
        select distinct r.* exclude (chakujun)
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
        join runners_l r
          on b.kaisai_nen = r.kaisai_nen and b.kaisai_tsukihi = r.kaisai_tsukihi
         and b.keibajo_code = r.keibajo_code
         and {RACE_BANGO_SQL.format(alias="b")} = r.race_bango
         and b.ketto_toroku_bango = r.ketto_toroku_bango
        """
    )


def cumulative_sql(keys: tuple[str, ...]) -> str:
    """Per-key, per-day totals accumulated through each day, over target keys only."""
    cols = ", ".join(keys)
    not_null = " and ".join(f"h.{key} is not null" for key in keys)
    match = " and ".join(f"h.{key} = k.{key}" for key in keys)
    return f"""
      select {cols}, race_day,
        sum(w) over win as w, sum(t3) over win as t3, sum(n) over win as n
      from (
        select {", ".join(f"h.{key}" for key in keys)}, h.race_day,
          sum(h.w) as w, sum(h.t3) as t3, count(*) as n
        from hist h
        semi join (select distinct {cols} from targets) k on {match}
        where {not_null}
        group by all
      )
      window win as (partition by {cols} order by race_day rows unbounded preceding)
    """


def table_name(keys: tuple[str, ...]) -> str:
    return "c_" + "_".join(keys)


def required_tables() -> list[tuple[str, ...]]:
    tables: dict[tuple[str, ...], None] = {}
    for pair in PAIRS:
        for keys in ((pair.left, *pair.condition), (pair.right, *pair.condition)):
            tables[keys] = None
        tables[(pair.left, pair.right, *pair.condition)] = None
    tables[("a02",)] = None
    tables[("ketto_toroku_bango",)] = None
    return list(tables)


def stage_cumulative_tables(con: duckdb.DuckDBPyConnection) -> None:
    for keys in required_tables():
        con.execute(f"create or replace temp table {table_name(keys)} as {cumulative_sql(keys)}")


def asof_join(alias: str, keys: tuple[str, ...]) -> str:
    conditions = " and ".join(f"t.{key} = {alias}.{key}" for key in keys)
    return (
        f"asof left join {table_name(keys)} {alias} on {conditions}"
        f" and t.race_day > {alias}.race_day"
    )


def shrunk(hits: str, n: str, prior: str) -> str:
    return f"((coalesce({hits}, 0) + {SHRINK_K} * {prior}) / (coalesce({n}, 0) + {SHRINK_K}))"


def pair_lift_sql(index: int, pair: Pair) -> str:
    """Pair rate shrunk toward the additive expectation of its parts, minus that expectation."""
    base = BASE_RATE[pair.rate]
    left, right, both = f"pl{index}", f"pr{index}", f"pb{index}"
    expected = (
        f"greatest(0.0, {shrunk(f'{left}.{pair.rate}', f'{left}.n', str(base))}"
        f" + {shrunk(f'{right}.{pair.rate}', f'{right}.n', str(base))} - {base})"
    )
    lift = f"{shrunk(f'{both}.{pair.rate}', f'{both}.n', expected)} - {expected}"
    return f"case when t.{pair.left} is not null and t.{pair.right} is not null then {lift} end as {pair.name}"


def pair_joins(index: int, pair: Pair) -> list[str]:
    return [
        asof_join(f"pl{index}", (pair.left, *pair.condition)),
        asof_join(f"pr{index}", (pair.right, *pair.condition)),
        asof_join(f"pb{index}", (pair.left, pair.right, *pair.condition)),
    ]


def rank_in_race_sql(column: str) -> str:
    """Average rank among runners with a value (1 = highest), null when the value is null."""
    race = "kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
    return f"""case when {column} is null then null else (
      rank() over (partition by {race}, ({column} is null) order by {column} desc)
      + (count(*) over (partition by {race}, {column}) - 1) / 2.0
    ) end as {column}_rank_in_race"""


def features_sql() -> str:
    joins = [join for i, pair in enumerate(PAIRS) for join in pair_joins(i, pair)]
    joins += [asof_join("dm", ("a02",)), asof_join("sf", ("ketto_toroku_bango",))]
    sibling_n = "dm.n - coalesce(sf.n, 0)"
    lifts = ",\n        ".join(pair_lift_sql(i, pair) for i, pair in enumerate(PAIRS))
    return f"""
    with f as (
      select t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        {lifts},
        case when t.a01 is not null and t.a05 is not null then ln(1 + coalesce(pb0.n, 0)) end
          as ped_pair_logn,
        case when t.a02 is not null then
          {shrunk("dm.t3 - coalesce(sf.t3, 0)", sibling_n, str(BASE_RATE["t3"]))} end
          as ped_dam_sib_top3_eb,
        case when t.a02 is not null then
          {shrunk("dm.w - coalesce(sf.w, 0)", sibling_n, str(BASE_RATE["w"]))} end
          as ped_dam_sib_win_eb,
        case when t.a02 is not null then ln(1 + greatest(0, coalesce({sibling_n}, 0))) end
          as ped_dam_sib_logn,
        cast(t.inbreed_dups as double) as ped_inbreed_dups,
        case when t.sline_broad is not null and t.bline_broad is not null then
          cast(t.sline_broad = t.bline_broad as double) end as ped_same_broad_line
      from targets t
      {chr(10).join(joins)}
    )
    select *, {", ".join(rank_in_race_sql(column) for column in RANKED)}
    from f
    """


def append_features_sql(input_glob: str) -> str:
    selected = ", ".join(f"x.{column}" for column in FEATURE_COLUMNS)
    return f"""
    select b.*, {selected}
    from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
    left join jra_pedigree_combo_features x
      on b.kaisai_nen = x.kaisai_nen
     and b.kaisai_tsukihi = x.kaisai_tsukihi
     and b.keibajo_code = x.keibajo_code
     and {RACE_BANGO_SQL.format(alias="b")} = x.race_bango
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
    stage_pedigrees(con, from_year)
    stage_lineage(con)
    stage_rows(con, input_glob)
    stage_cumulative_tables(con)
    con.execute("create or replace temp table jra_pedigree_combo_features as " + features_sql())


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
