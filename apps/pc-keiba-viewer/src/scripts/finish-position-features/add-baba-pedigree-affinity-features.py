#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append baba × pedigree affinity features (v7 layer).

Motivation:
  雨レース (重/不良馬場) や良馬場での sire/damsire の系統別の win rate は強い signal。
  既存 v6 stack には sire × kyori / sire × grade はあるが、baba_condition 別の集計はない。

Features added (per horse × race):
  - current_baba_condition         : current race の baba (1=良 2=稍重 3=重 4=不良)
  - horse_baba_win_rate            : self horse の同 baba career win rate (lookback)
  - horse_baba_career_starts       : self horse の同 baba 過去出走数
  - sire_baba_win_rate             : sire の同 baba career win rate (lookback)
  - sire_baba_career_starts        : sire の同 baba 過去出走数
  - damsire_baba_win_rate          : damsire の同 baba career win rate (lookback)
  - damsire_baba_career_starts     : damsire の同 baba 過去出走数
  - sire_horse_baba_combined_score : sire と self horse の win rate の平均 (NULL-safe)

Data leakage 防止: race_date strictly less than current race_date のみを集計。
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_baba_pedigree_affinity_features")
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


def stage_race_baba(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """race-level baba_condition を jvd_ra / nvd_ra から取得 (single int 1-4)。"""
    con.execute(
        f"""
        create or replace temp table race_baba as
        select
          'jra' as source,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          coalesce(
            try_cast(nullif(babajotai_code_shiba, '0') as int),
            try_cast(nullif(babajotai_code_dirt, '0') as int)
          ) as baba_cond
        from pg.jvd_ra
        where kaisai_nen >= substring('{from_date}', 1, 4)
        union all
        select
          'nar' as source,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          coalesce(
            try_cast(nullif(babajotai_code_shiba, '0') as int),
            try_cast(nullif(babajotai_code_dirt, '0') as int)
          ) as baba_cond
        from pg.nvd_ra
        where kaisai_nen >= substring('{from_date}', 1, 4)
        """
    )
    con.execute(
        f"create index race_baba_idx on race_baba ({RACE_PARTITION})"
    )


def stage_race_history_with_baba(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """horse の過去レース成績 + baba_condition を join。"""
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
          rec.finish_position,
          rb.baba_cond
        from pg.race_entry_corner_features rec
        left join race_baba rb
          on rb.source = rec.source
          and rb.kaisai_nen = rec.kaisai_nen
          and rb.kaisai_tsukihi = rec.kaisai_tsukihi
          and rb.keibajo_code = rec.keibajo_code
          and rb.race_bango = rec.race_bango
        where rec.race_date >= '{from_date}'
          and rec.finish_position is not null
          and rb.baba_cond is not null
        """
    )
    con.execute(
        "create index race_history_horse_idx on race_history (source, ketto_toroku_bango, baba_cond, race_date)"
    )


def stage_horse_pedigree(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp table horse_pedigree as
        select
          ketto_toroku_bango,
          nullif(trim(ketto_joho_01a), '') as sire_id,
          nullif(trim(ketto_joho_04a), '') as damsire_id
        from pg.jvd_um
        where ketto_toroku_bango is not null
        """
    )
    con.execute(
        "create index horse_pedigree_idx on horse_pedigree (ketto_toroku_bango)"
    )


def stage_horse_baba_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """horse 自身の baba 別 cumul stats (lookback: 当該レース除外)。

    Pre-aggregate to (horse, race_date, baba_cond): starts + wins per day
    → window cumulative.
    """
    con.execute(
        """
        create or replace temp table horse_baba_daily as
        select source, ketto_toroku_bango, baba_cond, race_date,
          count(*) as starts_on_day,
          sum(case when finish_position = 1 then 1 else 0 end) as wins_on_day
        from race_history
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table horse_baba_cumul as
        select source, ketto_toroku_bango, baba_cond, race_date,
          sum(starts_on_day) over horse_baba_career as past_starts,
          sum(wins_on_day) over horse_baba_career as past_wins
        from horse_baba_daily
        window horse_baba_career as (
          partition by source, ketto_toroku_bango, baba_cond
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index horse_baba_cumul_idx on horse_baba_cumul (source, ketto_toroku_bango, baba_cond, race_date)"
    )


def stage_sire_baba_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """sire 単位の baba 別 cumul stats。

    horse_pedigree で sire_id を取得 → race_history と join → 集計。
    """
    con.execute(
        """
        create or replace temp table sire_baba_daily as
        select p.sire_id, h.baba_cond, h.race_date,
          count(*) as starts_on_day,
          sum(case when h.finish_position = 1 then 1 else 0 end) as wins_on_day
        from race_history h
        join horse_pedigree p using (ketto_toroku_bango)
        where p.sire_id is not null
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table sire_baba_cumul as
        select sire_id, baba_cond, race_date,
          sum(starts_on_day) over sire_baba_career as past_starts,
          sum(wins_on_day) over sire_baba_career as past_wins
        from sire_baba_daily
        window sire_baba_career as (
          partition by sire_id, baba_cond
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index sire_baba_cumul_idx on sire_baba_cumul (sire_id, baba_cond, race_date)"
    )


def stage_damsire_baba_cumul(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp table damsire_baba_daily as
        select p.damsire_id, h.baba_cond, h.race_date,
          count(*) as starts_on_day,
          sum(case when h.finish_position = 1 then 1 else 0 end) as wins_on_day
        from race_history h
        join horse_pedigree p using (ketto_toroku_bango)
        where p.damsire_id is not null
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table damsire_baba_cumul as
        select damsire_id, baba_cond, race_date,
          sum(starts_on_day) over damsire_baba_career as past_starts,
          sum(wins_on_day) over damsire_baba_career as past_wins
        from damsire_baba_daily
        window damsire_baba_career as (
          partition by damsire_id, baba_cond
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index damsire_baba_cumul_idx on damsire_baba_cumul (damsire_id, baba_cond, race_date)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_with_baba as (
      select b.*, rb.baba_cond as current_baba_condition
      from base b
      left join race_baba rb
        on rb.source = b.source
        and rb.kaisai_nen = b.kaisai_nen
        and rb.kaisai_tsukihi = b.kaisai_tsukihi
        and rb.keibajo_code = b.keibajo_code
        and rb.race_bango = b.race_bango
    ),
    base_with_pedigree as (
      select bwb.*, hp.sire_id, hp.damsire_id
      from base_with_baba bwb
      left join horse_pedigree hp on hp.ketto_toroku_bango = bwb.ketto_toroku_bango
    ),
    joined as (
      select
        bwp.* exclude (sire_id, damsire_id),
        hbc.past_starts as horse_baba_career_starts,
        case when hbc.past_starts > 0
             then hbc.past_wins::double / hbc.past_starts
             else null end as horse_baba_win_rate,
        sbc.past_starts as sire_baba_career_starts,
        case when sbc.past_starts > 0
             then sbc.past_wins::double / sbc.past_starts
             else null end as sire_baba_win_rate,
        dbc.past_starts as damsire_baba_career_starts,
        case when dbc.past_starts > 0
             then dbc.past_wins::double / dbc.past_starts
             else null end as damsire_baba_win_rate,
        case
          when hbc.past_starts > 0 and sbc.past_starts > 0
            then ((hbc.past_wins::double / hbc.past_starts) + (sbc.past_wins::double / sbc.past_starts)) / 2.0
          when hbc.past_starts > 0 then hbc.past_wins::double / hbc.past_starts
          when sbc.past_starts > 0 then sbc.past_wins::double / sbc.past_starts
          else null
        end as sire_horse_baba_combined_score
      from base_with_pedigree bwp
      left join horse_baba_cumul hbc
        on hbc.source = bwp.source
        and hbc.ketto_toroku_bango = bwp.ketto_toroku_bango
        and hbc.baba_cond = bwp.current_baba_condition
        and hbc.race_date = bwp.race_date
      left join sire_baba_cumul sbc
        on sbc.sire_id = bwp.sire_id
        and sbc.baba_cond = bwp.current_baba_condition
        and sbc.race_date = bwp.race_date
      left join damsire_baba_cumul dbc
        on dbc.damsire_id = bwp.damsire_id
        and dbc.baba_cond = bwp.current_baba_condition
        and dbc.race_date = bwp.race_date
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
    stage_race_baba(con, args.from_date)
    stage_race_history_with_baba(con, args.from_date)
    stage_horse_pedigree(con)
    stage_horse_baba_cumul(con)
    stage_sire_baba_cumul(con)
    stage_damsire_baba_cumul(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
