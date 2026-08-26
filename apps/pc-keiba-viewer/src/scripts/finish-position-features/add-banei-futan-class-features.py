#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append Ban-ei futan_juryo class-specific win rate features (v7 layer).

Motivation:
  Ban-ei は futan_juryo (700/800/900kg+ 級) が race-level identity になっており、
  高斤量レースで勝てる馬は限定される。class-specific career win rate を投入。

Features added (per horse × race):
  - current_futan_class               : current race の bucket (0-6)
  - horse_futan_class_career_starts   : horse の同 class 過去出走数
  - horse_futan_class_career_win_rate : 同 class win rate
  - horse_futan_class_career_top3_rate: 同 class top3 rate
  - sire_futan_class_starts           : sire 産駒の同 class 過去出走数
  - sire_futan_class_win_rate         : sire 産駒の同 class win rate
  - damsire_futan_class_starts        : damsire の同 class 出走数
  - damsire_futan_class_win_rate      : damsire の同 class win rate
  - field_futan_class_avg             : race の平均 class (race-level, 全馬同値)
  - self_futan_minus_field_avg        : 自分 class - field average

Data source:
  - pg.nvd_se with keibajo_code='83' (帯広 = ばんえい)
  - futan_juryo column: **3-char hex string** (e.g. "26C"=620kg, "1F4"=500kg)
    → parsed as: from_hex(...) to int (already in kg, no /10 needed)
  - 既存 add-ban-ei-raw-features.py:67 の `try_cast(... as double)/10.0` は hex 文字列で NULL になり機能していなかった (silent bug)
  - 実データ分布: 450kg-1000kg, p10=500, median=610, p90=690, p99=760, max=1000 (ばんえい記念)
  - bucket: <500→0, 500-549→1, 550-599→2, 600-649→3, 650-699→4, 700-799→5, >=800→6

Data leakage 防止: race_date strictly less than current race_date のみ集計。
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from _catalog_attach import attach_source_catalog

from _resource_defaults import add_resource_args, apply_to_connection

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
BAN_EI_KEIBAJO = "83"

# Bucket boundaries (kg) — 実データ分布 450-1000kg, p10=500 median=610 p99=760 max=1000.
# 7 buckets: 0=<500, 1=500-549, 2=550-599, 3=600-649, 4=650-699, 5=700-799, 6=>=800
FUTAN_BUCKET_SQL = """
  case
    when futan_kg is null then null
    when futan_kg < 500 then 0
    when futan_kg < 550 then 1
    when futan_kg < 600 then 2
    when futan_kg < 650 then 3
    when futan_kg < 700 then 4
    when futan_kg < 800 then 5
    else 6
  end
"""

# Hex string → int parser (DuckDB compatible).
# ban-ei futan_juryo は "26C", "1F4" 等の 3-char hex 表現。先頭 0 埋めしてから to_hex 経由で整数化。
# from_base() で base=16 指定。
FUTAN_HEX_PARSE = """
  case
    when futan_juryo is null or trim(futan_juryo) = '' then null
    else try_cast('0x' || trim(futan_juryo) as integer)
  end
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_banei_futan_class_features")
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
    attach_source_catalog(con, pg_url)


def sql_literal(value: str) -> str:
    """Return a safely quoted DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _literal_list(values: set[str]) -> str:
    return ", ".join(sql_literal(value) for value in sorted(values))


def stage_target_scope(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """Materialize the exact Ban-ei rows requested by the upstream layer."""
    con.execute(
        f"""
        create or replace temp table banei_targets as
        select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code,
          race_bango, ketto_toroku_bango,
          kaisai_nen || kaisai_tsukihi as race_date
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
        where source = 'nar'
          and keibajo_code = '{BAN_EI_KEIBAJO}'
          and ketto_toroku_bango is not null
        """
    )
    con.execute(
        f"create index banei_targets_idx on banei_targets ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def _target_horse_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        "select distinct ketto_toroku_bango from banei_targets order by 1"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _pedigree_union_sql(row_filter: str) -> str:
    """Resolve Ban-ei pedigree from the native master then its NU fallback."""
    return f"""
        select ketto_toroku_bango,
          nullif(trim(ketto_joho_01a), '') as sire_id,
          nullif(trim(ketto_joho_05a), '') as damsire_id,
          1 as priority
        from pg.nvd_um
        where ketto_toroku_bango is not null and ({row_filter})
        union all
        select ketto_toroku_bango,
          nullif(trim(ketto_joho_01a), '') as sire_id,
          nullif(trim(ketto_joho_05a), '') as damsire_id,
          2 as priority
        from pg.nvd_nu
        where ketto_toroku_bango is not null and ({row_filter})
    """


def _history_horse_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        "select distinct ketto_toroku_bango from banei_pedigree order by 1"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _target_runner_predicate(con: duckdb.DuckDBPyConnection) -> str:
    rows = con.execute(
        """
        select min(race_date), max(race_date)
        from banei_targets
        """
    ).fetchall()
    horse_ids = _target_horse_ids(con)
    if not horse_ids or not rows or rows[0][0] is None or rows[0][1] is None:
        return "false"
    from_date = str(rows[0][0])
    to_date = str(rows[0][1])
    return (
        f"se.kaisai_nen between {sql_literal(from_date[:4])} "
        f"and {sql_literal(to_date[:4])} "
        f"and se.kaisai_nen || se.kaisai_tsukihi between "
        f"{sql_literal(from_date)} and {sql_literal(to_date)} "
        f"and se.ketto_toroku_bango in ({_literal_list(horse_ids)})"
    )


def stage_banei_history(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """horse 過去 race + futan_class を Ban-ei filter で取得。

    futan_juryo は hex string ('26C'=620kg etc.) なので '0x' prefix + integer cast でデコード。
    """
    horse_ids = _history_horse_ids(con)
    horse_filter = (
        f"and ketto_toroku_bango in ({_literal_list(horse_ids)})"
        if horse_ids
        else "and false"
    )
    target_to_date = str(
        con.execute(
            "select coalesce(max(race_date), '00000000') from banei_targets"
        ).fetchall()[0][0]
    )
    con.execute(
        f"""
        create or replace temp table banei_history as
        with raw as (
          select
            'nar' as source,
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            ketto_toroku_bango,
            {FUTAN_HEX_PARSE} as futan_kg,
            try_cast(nullif(trim(kakutei_chakujun), '') as int) as finish_position
          from pg.nvd_se
          where keibajo_code = '{BAN_EI_KEIBAJO}'
            and kaisai_nen >= substring('{from_date}', 1, 4)
            and kaisai_nen <= substring({sql_literal(target_to_date)}, 1, 4)
            and kaisai_nen || kaisai_tsukihi < {sql_literal(target_to_date)}
            {horse_filter}
        )
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          futan_kg,
          {FUTAN_BUCKET_SQL} as futan_class,
          finish_position,
          kaisai_nen || kaisai_tsukihi as race_date
        from raw
        where finish_position is not null and futan_kg is not null
        """
    )
    con.execute(
        "create index banei_history_horse_idx on banei_history (source, ketto_toroku_bango, futan_class, race_date)"
    )


def stage_horse_pedigree(con: duckdb.DuckDBPyConnection) -> None:
    """Stage target horses and progeny sharing their resolved sire/damsire."""
    target_horses = _target_horse_ids(con)
    target_filter = (
        f"ketto_toroku_bango in ({_literal_list(target_horses)})"
        if target_horses
        else "false"
    )
    con.execute(
        f"""
        create or replace temp table target_banei_pedigree as
        with combined as ({_pedigree_union_sql(target_filter)})
        select ketto_toroku_bango, sire_id, damsire_id
        from combined
        qualify row_number() over (
          partition by ketto_toroku_bango order by priority
        ) = 1
        """
    )
    lineage_rows = con.execute(
        """
        select sire_id from target_banei_pedigree where sire_id is not null
        union
        select damsire_id from target_banei_pedigree where damsire_id is not null
        order by 1
        """
    ).fetchall()
    lineage_ids = {str(row[0]) for row in lineage_rows}
    if lineage_ids:
        lineage_literals = _literal_list(lineage_ids)
        lineage_filter = (
            f"nullif(trim(ketto_joho_01a), '') in ({lineage_literals}) "
            f"or nullif(trim(ketto_joho_05a), '') in ({lineage_literals})"
        )
        candidate_rows = con.execute(
            f"""
            select distinct ketto_toroku_bango
            from ({_pedigree_union_sql(lineage_filter)}) candidates
            order by 1
            """
        ).fetchall()
        candidate_horses = {str(row[0]) for row in candidate_rows}
    else:
        candidate_horses = set()
    scoped_horses = target_horses | candidate_horses
    scoped_filter = (
        f"ketto_toroku_bango in ({_literal_list(scoped_horses)})"
        if scoped_horses
        else "false"
    )
    con.execute(
        f"""
        create or replace temp table banei_pedigree as
        with combined as ({_pedigree_union_sql(scoped_filter)})
        select ketto_toroku_bango, sire_id, damsire_id
        from combined
        qualify row_number() over (
          partition by ketto_toroku_bango order by priority
        ) = 1
        """
    )
    con.execute(
        "create index banei_pedigree_idx on banei_pedigree (ketto_toroku_bango)"
    )


def stage_horse_futan_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """horse 自身の futan_class 別 cumul stats (lookback)。"""
    con.execute(
        """
        create or replace temp table horse_futan_daily as
        select source, ketto_toroku_bango, futan_class, race_date,
          count(*) as starts_on_day,
          sum(case when finish_position = 1 then 1 else 0 end) as wins_on_day,
          sum(case when finish_position <= 3 then 1 else 0 end) as top3_on_day
        from banei_history
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table horse_futan_cumul as
        select source, ketto_toroku_bango, futan_class, race_date,
          sum(starts_on_day) over horse_futan_career as past_starts,
          sum(wins_on_day) over horse_futan_career as past_wins,
          sum(top3_on_day) over horse_futan_career as past_top3
        from horse_futan_daily
        window horse_futan_career as (
          partition by source, ketto_toroku_bango, futan_class
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index horse_futan_cumul_idx on horse_futan_cumul (source, ketto_toroku_bango, futan_class, race_date)"
    )


def stage_sire_futan_cumul(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp table sire_futan_daily as
        select p.sire_id, h.futan_class, h.race_date,
          count(*) as starts_on_day,
          sum(case when h.finish_position = 1 then 1 else 0 end) as wins_on_day
        from banei_history h
        join banei_pedigree p using (ketto_toroku_bango)
        where p.sire_id is not null
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table sire_futan_cumul as
        select sire_id, futan_class, race_date,
          sum(starts_on_day) over sire_futan_career as past_starts,
          sum(wins_on_day) over sire_futan_career as past_wins
        from sire_futan_daily
        window sire_futan_career as (
          partition by sire_id, futan_class
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index sire_futan_cumul_idx on sire_futan_cumul (sire_id, futan_class, race_date)"
    )


def stage_damsire_futan_cumul(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp table damsire_futan_daily as
        select p.damsire_id, h.futan_class, h.race_date,
          count(*) as starts_on_day,
          sum(case when h.finish_position = 1 then 1 else 0 end) as wins_on_day
        from banei_history h
        join banei_pedigree p using (ketto_toroku_bango)
        where p.damsire_id is not null
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table damsire_futan_cumul as
        select damsire_id, futan_class, race_date,
          sum(starts_on_day) over damsire_futan_career as past_starts,
          sum(wins_on_day) over damsire_futan_career as past_wins
        from damsire_futan_daily
        window damsire_futan_career as (
          partition by damsire_id, futan_class
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index damsire_futan_cumul_idx on damsire_futan_cumul (damsire_id, futan_class, race_date)"
    )


def stage_current_race_futan(con: duckdb.DuckDBPyConnection) -> None:
    """current race の futan_class を horse 単位で取得 + field average。"""
    target_predicate = _target_runner_predicate(con)
    con.execute(
        f"""
        create or replace temp table current_race_futan as
        with se_today as (
          select
            'nar' as source,
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            ketto_toroku_bango,
            {FUTAN_HEX_PARSE} as futan_kg
          from pg.nvd_se se
          where se.keibajo_code = '{BAN_EI_KEIBAJO}'
            and ({target_predicate})
        ),
        bucketed as (
          select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            ketto_toroku_bango, futan_kg, {FUTAN_BUCKET_SQL} as futan_class
          from se_today
        )
        select *, avg(futan_class::double) over (partition by {RACE_PARTITION}) as field_futan_class_avg
        from bucketed
        """
    )
    con.execute(
        f"create index current_race_futan_idx on current_race_futan ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_with_futan as (
      select b.*,
        crf.futan_class as current_futan_class,
        crf.field_futan_class_avg,
        crf.futan_class::double - crf.field_futan_class_avg as self_futan_minus_field_avg
      from base b
      left join current_race_futan crf
        on crf.source = b.source
        and crf.kaisai_nen = b.kaisai_nen
        and crf.kaisai_tsukihi = b.kaisai_tsukihi
        and crf.keibajo_code = b.keibajo_code
        and crf.race_bango = b.race_bango
        and crf.ketto_toroku_bango = b.ketto_toroku_bango
    ),
    base_with_pedigree as (
      select bwf.*, p.sire_id, p.damsire_id
      from base_with_futan bwf
      left join banei_pedigree p on p.ketto_toroku_bango = bwf.ketto_toroku_bango
    ),
    joined as (
      select
        bwp.* exclude (sire_id, damsire_id),
        hfc.past_starts as horse_futan_class_career_starts,
        case when hfc.past_starts > 0
             then hfc.past_wins::double / hfc.past_starts
             else null end as horse_futan_class_career_win_rate,
        case when hfc.past_starts > 0
             then hfc.past_top3::double / hfc.past_starts
             else null end as horse_futan_class_career_top3_rate,
        sfc.past_starts as sire_futan_class_starts,
        case when sfc.past_starts > 0
             then sfc.past_wins::double / sfc.past_starts
             else null end as sire_futan_class_win_rate,
        dfc.past_starts as damsire_futan_class_starts,
        case when dfc.past_starts > 0
             then dfc.past_wins::double / dfc.past_starts
             else null end as damsire_futan_class_win_rate
      from base_with_pedigree bwp
      asof left join horse_futan_cumul hfc
        on hfc.source = bwp.source
        and hfc.ketto_toroku_bango = bwp.ketto_toroku_bango
        and hfc.futan_class = bwp.current_futan_class
        and hfc.race_date < bwp.race_date
      asof left join sire_futan_cumul sfc
        on sfc.sire_id = bwp.sire_id
        and sfc.futan_class = bwp.current_futan_class
        and sfc.race_date < bwp.race_date
      asof left join damsire_futan_cumul dfc
        on dfc.damsire_id = bwp.damsire_id
        and dfc.futan_class = bwp.current_futan_class
        and dfc.race_date < bwp.race_date
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
    stage_target_scope(con, input_glob)
    stage_horse_pedigree(con)
    stage_banei_history(con, args.from_date)
    stage_horse_futan_cumul(con)
    stage_sire_futan_cumul(con)
    stage_damsire_futan_cumul(con)
    stage_current_race_futan(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
