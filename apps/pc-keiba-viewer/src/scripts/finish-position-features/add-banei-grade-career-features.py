#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append Ban-ei grade-level career features (v7 layer).

Motivation:
  既存 lineage layer は重賞 (target_race_id != null) でのみ機能。Ban-ei では一般戦
  (E grade) で覆える race 数が圧倒的多く、grade ladder 上での horse のキャリア
  軌跡 (どの grade レベルで勝てる、上の grade で good 等) が強い signal になる。
  本 layer は重賞 / 一般戦 問わず全 race で grade 関係 signal を encode する。

Features added (per horse × race):
  - current_race_grade_letter         : current race の grade_code (E/T/S/R/Q/P/etc)
  - horse_grade_E_career_starts/win_rate : horse の E-grade career
  - horse_grade_S_career_starts/win_rate : same for S
  - horse_grade_Q_career_starts/win_rate
  - horse_grade_P_career_starts/win_rate (最高 grade)
  - horse_current_grade_career_win_rate  : horse の current race grade での career win rate
  - horse_current_grade_career_starts
  - horse_higher_grade_starts            : current grade 以上の grade での過去出走数
  - horse_higher_grade_wins              : 同 wins
  - field_avg_career_starts              : field horses の平均 career race count
  - horse_career_starts_minus_field      : self career starts - field avg

Data leakage 防止: race_date strictly less than current race_date のみ集計。

Ban-ei grade hierarchy (highest → lowest):
  P > Q > R > S > T > E > (empty) > others
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
BAN_EI_KEIBAJO = "83"

# Grade rank for "higher than current" comparison. Higher number = higher grade.
GRADE_RANK_SQL = """
  case grade_letter
    when 'P' then 6
    when 'Q' then 5
    when 'R' then 4
    when 'S' then 3
    when 'T' then 2
    when 'E' then 1
    else 0
  end
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_banei_grade_career_features")
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


def stage_banei_grade_history(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """Ban-ei race history (馬単位) with grade_letter (jvd_ra/nvd_ra から)。"""
    con.execute(
        f"""
        create or replace temp table banei_grade_history as
        with se as (
          select 'nar' as source,
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            ketto_toroku_bango,
            try_cast(nullif(trim(kakutei_chakujun), '') as int) as finish_position,
            kaisai_nen || kaisai_tsukihi as race_date
          from pg.nvd_se
          where keibajo_code = '{BAN_EI_KEIBAJO}'
            and kaisai_nen >= substring('{from_date}', 1, 4)
        ),
        ra as (
          select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            coalesce(nullif(trim(grade_code), ''), '_') as grade_letter
          from pg.nvd_ra
          where keibajo_code = '{BAN_EI_KEIBAJO}'
        )
        select s.source, s.kaisai_nen, s.kaisai_tsukihi, s.keibajo_code, s.race_bango,
          s.ketto_toroku_bango, s.finish_position, s.race_date,
          ra.grade_letter,
          ({GRADE_RANK_SQL}) as grade_rank
        from se s
        left join ra
          on ra.kaisai_nen = s.kaisai_nen
          and ra.kaisai_tsukihi = s.kaisai_tsukihi
          and ra.keibajo_code = s.keibajo_code
          and ra.race_bango = s.race_bango
        where s.finish_position is not null
        """
    )
    con.execute(
        "create index banei_grade_history_horse_idx on banei_grade_history (source, ketto_toroku_bango, grade_letter, race_date)"
    )


def stage_horse_grade_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """horse × grade_letter 別 cumulative career (starts, wins)。"""
    con.execute(
        """
        create or replace temp table horse_grade_daily as
        select source, ketto_toroku_bango, grade_letter, grade_rank, race_date,
          count(*) as starts_on_day,
          sum(case when finish_position = 1 then 1 else 0 end) as wins_on_day
        from banei_grade_history
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table horse_grade_cumul as
        select source, ketto_toroku_bango, grade_letter, grade_rank, race_date,
          sum(starts_on_day) over horse_grade_career as past_starts,
          sum(wins_on_day) over horse_grade_career as past_wins
        from horse_grade_daily
        window horse_grade_career as (
          partition by source, ketto_toroku_bango, grade_letter
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index horse_grade_cumul_idx on horse_grade_cumul (source, ketto_toroku_bango, grade_letter, race_date)"
    )


def stage_horse_higher_grade_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """horse × grade_rank ladder (current race grade 以上での過去出走数)。

    各 (horse, race_date) について、grade_rank >= current_rank の race count + wins を集計。
    実装方針: bucketed cumul を rank ごとに用意 (rank 0..6) → join 時に >= で再集計。
    シンプル実装: rank 別に partial cumul, current_rank で filter。
    """
    con.execute(
        """
        create or replace temp table horse_grade_total_career as
        select source, ketto_toroku_bango, race_date,
          sum(starts_on_day) over total_window as total_past_starts,
          sum(wins_on_day) over total_window as total_past_wins
        from (
          select source, ketto_toroku_bango, race_date,
            sum(starts_on_day) as starts_on_day,
            sum(wins_on_day) as wins_on_day
          from horse_grade_daily
          group by all
        )
        window total_window as (
          partition by source, ketto_toroku_bango
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index horse_grade_total_career_idx on horse_grade_total_career (source, ketto_toroku_bango, race_date)"
    )


def stage_current_race_grade(con: duckdb.DuckDBPyConnection) -> None:
    """current race の grade_letter / grade_rank を取得。"""
    con.execute(
        f"""
        create or replace temp table current_race_grade as
        with ra_raw as (
          select 'nar' as source,
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            coalesce(nullif(trim(grade_code), ''), '_') as grade_letter
          from pg.nvd_ra
          where keibajo_code = '{BAN_EI_KEIBAJO}'
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          grade_letter,
          ({GRADE_RANK_SQL}) as grade_rank
        from ra_raw
        """
    )
    con.execute(
        f"create index current_race_grade_idx on current_race_grade ({RACE_PARTITION})"
    )


def stage_field_career_avg(con: duckdb.DuckDBPyConnection) -> None:
    """各 race の field 平均 career starts を計算 (race-level signal)。"""
    con.execute(
        f"""
        create or replace temp table banei_field_career_avg as
        with se as (
          select 'nar' as source,
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            ketto_toroku_bango,
            kaisai_nen || kaisai_tsukihi as race_date
          from pg.nvd_se
          where keibajo_code = '{BAN_EI_KEIBAJO}'
        ),
        joined as (
          select se.*, coalesce(htc.total_past_starts, 0) as starts
          from se
          left join horse_grade_total_career htc
            on htc.source = se.source
            and htc.ketto_toroku_bango = se.ketto_toroku_bango
            and htc.race_date = se.race_date
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          avg(starts) as field_avg_career_starts
        from joined
        group by all
        """
    )
    con.execute(
        f"create index banei_field_career_avg_idx on banei_field_career_avg ({RACE_PARTITION})"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_with_current as (
      select b.*, crg.grade_letter as current_race_grade_letter, crg.grade_rank as current_grade_rank
      from base b
      left join current_race_grade crg
        on crg.source = b.source
        and crg.kaisai_nen = b.kaisai_nen
        and crg.kaisai_tsukihi = b.kaisai_tsukihi
        and crg.keibajo_code = b.keibajo_code
        and crg.race_bango = b.race_bango
    ),
    horse_grade_pivoted as (
      select source, ketto_toroku_bango, race_date,
        max(case when grade_letter = 'E' then past_starts end) as e_starts,
        max(case when grade_letter = 'E' then past_wins end) as e_wins,
        max(case when grade_letter = 'T' then past_starts end) as t_starts,
        max(case when grade_letter = 'T' then past_wins end) as t_wins,
        max(case when grade_letter = 'S' then past_starts end) as s_starts,
        max(case when grade_letter = 'S' then past_wins end) as s_wins,
        max(case when grade_letter = 'R' then past_starts end) as r_starts,
        max(case when grade_letter = 'R' then past_wins end) as r_wins,
        max(case when grade_letter = 'Q' then past_starts end) as q_starts,
        max(case when grade_letter = 'Q' then past_wins end) as q_wins,
        max(case when grade_letter = 'P' then past_starts end) as p_starts,
        max(case when grade_letter = 'P' then past_wins end) as p_wins
      from horse_grade_cumul
      group by all
    ),
    horse_current_grade_only as (
      select source, ketto_toroku_bango, grade_letter, race_date,
        past_starts, past_wins
      from horse_grade_cumul
    ),
    joined as (
      select
        bwc.*,
        coalesce(hgp.e_starts, 0) as horse_grade_E_career_starts,
        case when coalesce(hgp.e_starts, 0) > 0 then hgp.e_wins::double / hgp.e_starts else null end as horse_grade_E_career_win_rate,
        coalesce(hgp.t_starts, 0) as horse_grade_T_career_starts,
        case when coalesce(hgp.t_starts, 0) > 0 then hgp.t_wins::double / hgp.t_starts else null end as horse_grade_T_career_win_rate,
        coalesce(hgp.s_starts, 0) as horse_grade_S_career_starts,
        case when coalesce(hgp.s_starts, 0) > 0 then hgp.s_wins::double / hgp.s_starts else null end as horse_grade_S_career_win_rate,
        coalesce(hgp.r_starts, 0) as horse_grade_R_career_starts,
        case when coalesce(hgp.r_starts, 0) > 0 then hgp.r_wins::double / hgp.r_starts else null end as horse_grade_R_career_win_rate,
        coalesce(hgp.q_starts, 0) as horse_grade_Q_career_starts,
        case when coalesce(hgp.q_starts, 0) > 0 then hgp.q_wins::double / hgp.q_starts else null end as horse_grade_Q_career_win_rate,
        coalesce(hgp.p_starts, 0) as horse_grade_P_career_starts,
        case when coalesce(hgp.p_starts, 0) > 0 then hgp.p_wins::double / hgp.p_starts else null end as horse_grade_P_career_win_rate,
        coalesce(hcgo.past_starts, 0) as horse_current_grade_career_starts,
        case when coalesce(hcgo.past_starts, 0) > 0 then hcgo.past_wins::double / hcgo.past_starts else null end as horse_current_grade_career_win_rate,
        coalesce(htc.total_past_starts, 0) - coalesce(fca.field_avg_career_starts, 0) as horse_career_starts_minus_field,
        coalesce(fca.field_avg_career_starts, 0) as field_avg_career_starts
      from base_with_current bwc
      left join horse_grade_pivoted hgp
        on hgp.source = bwc.source
        and hgp.ketto_toroku_bango = bwc.ketto_toroku_bango
        and hgp.race_date = bwc.race_date
      left join horse_current_grade_only hcgo
        on hcgo.source = bwc.source
        and hcgo.ketto_toroku_bango = bwc.ketto_toroku_bango
        and hcgo.grade_letter = bwc.current_race_grade_letter
        and hcgo.race_date = bwc.race_date
      left join horse_grade_total_career htc
        on htc.source = bwc.source
        and htc.ketto_toroku_bango = bwc.ketto_toroku_bango
        and htc.race_date = bwc.race_date
      left join banei_field_career_avg fca
        on fca.source = bwc.source
        and fca.kaisai_nen = bwc.kaisai_nen
        and fca.kaisai_tsukihi = bwc.kaisai_tsukihi
        and fca.keibajo_code = bwc.keibajo_code
        and fca.race_bango = bwc.race_bango
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
    stage_banei_grade_history(con, args.from_date)
    stage_horse_grade_cumul(con)
    stage_horse_higher_grade_cumul(con)
    stage_current_race_grade(con)
    stage_field_career_avg(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
