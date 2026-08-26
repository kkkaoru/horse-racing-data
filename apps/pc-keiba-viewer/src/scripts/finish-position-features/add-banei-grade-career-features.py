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

from _catalog_attach import attach_source_catalog

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


def _target_race_predicate(con: duckdb.DuckDBPyConnection) -> str:
    rows = con.execute(
        """
        select min(race_date), max(race_date)
        from banei_targets
        """
    ).fetchall()
    if not rows or rows[0][0] is None or rows[0][1] is None:
        return "false"
    from_date = str(rows[0][0])
    to_date = str(rows[0][1])
    return (
        f"ra.kaisai_nen between {sql_literal(from_date[:4])} "
        f"and {sql_literal(to_date[:4])} "
        f"and ra.kaisai_nen || ra.kaisai_tsukihi between "
        f"{sql_literal(from_date)} and {sql_literal(to_date)}"
    )


def stage_banei_grade_history(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """Ban-ei race history (馬単位) with grade_letter (jvd_ra/nvd_ra から)。"""
    horse_ids = _target_horse_ids(con)
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
            and kaisai_nen <= substring({sql_literal(target_to_date)}, 1, 4)
            and kaisai_nen || kaisai_tsukihi < {sql_literal(target_to_date)}
            {horse_filter}
        ),
        ra as (
          select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            coalesce(nullif(trim(grade_code), ''), '_') as grade_letter
          from pg.nvd_ra
          where keibajo_code = '{BAN_EI_KEIBAJO}'
            and kaisai_nen >= substring('{from_date}', 1, 4)
            and kaisai_nen <= substring({sql_literal(target_to_date)}, 1, 4)
            and kaisai_nen || kaisai_tsukihi < {sql_literal(target_to_date)}
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
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index horse_grade_cumul_idx on horse_grade_cumul (source, ketto_toroku_bango, grade_letter, race_date)"
    )


def stage_horse_total_career(con: duckdb.DuckDBPyConnection) -> None:
    """Stage total prior career stats used by the field-relative features."""
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
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index horse_grade_total_career_idx on horse_grade_total_career (source, ketto_toroku_bango, race_date)"
    )


def stage_current_race_grade(con: duckdb.DuckDBPyConnection) -> None:
    """current race の grade_letter / grade_rank を取得。"""
    target_predicate = _target_race_predicate(con)
    con.execute(
        f"""
        create or replace temp table current_race_grade as
        with ra_raw as (
          select 'nar' as source,
            kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            coalesce(nullif(trim(grade_code), ''), '_') as grade_letter
          from pg.nvd_ra ra
          where ra.keibajo_code = '{BAN_EI_KEIBAJO}'
            and ({target_predicate})
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
        with joined as (
          select t.*, coalesce(htc.total_past_starts, 0) as starts
          from banei_targets t
          asof left join horse_grade_total_career htc
            on htc.source = t.source
            and htc.ketto_toroku_bango = t.ketto_toroku_bango
            and htc.race_date < t.race_date
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


def stage_target_grade_careers(con: duckdb.DuckDBPyConnection) -> None:
    """Align inclusive history cumuls to every target via strict-before ASOF."""
    con.execute(
        """
        create or replace temp table target_grade_career as
        with grade_grid as (
          select t.source, t.ketto_toroku_bango, t.race_date, g.grade_letter
          from (
            select distinct source, ketto_toroku_bango, race_date
            from banei_targets
          ) t
          cross join (values ('E'), ('T'), ('S'), ('R'), ('Q'), ('P')) g(grade_letter)
        ), aligned as (
          select g.*, c.past_starts, c.past_wins
          from grade_grid g
          asof left join horse_grade_cumul c
            on c.source = g.source
            and c.ketto_toroku_bango = g.ketto_toroku_bango
            and c.grade_letter = g.grade_letter
            and c.race_date < g.race_date
        )
        select source, ketto_toroku_bango, race_date,
          max(past_starts) filter (where grade_letter = 'E') as e_starts,
          max(past_wins) filter (where grade_letter = 'E') as e_wins,
          max(past_starts) filter (where grade_letter = 'T') as t_starts,
          max(past_wins) filter (where grade_letter = 'T') as t_wins,
          max(past_starts) filter (where grade_letter = 'S') as s_starts,
          max(past_wins) filter (where grade_letter = 'S') as s_wins,
          max(past_starts) filter (where grade_letter = 'R') as r_starts,
          max(past_wins) filter (where grade_letter = 'R') as r_wins,
          max(past_starts) filter (where grade_letter = 'Q') as q_starts,
          max(past_wins) filter (where grade_letter = 'Q') as q_wins,
          max(past_starts) filter (where grade_letter = 'P') as p_starts,
          max(past_wins) filter (where grade_letter = 'P') as p_wins
        from aligned
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table target_current_grade_career as
        select t.source, t.ketto_toroku_bango, t.race_date,
          c.past_starts, c.past_wins
        from (
          select distinct t.source, t.ketto_toroku_bango, t.race_date,
            crg.grade_letter
          from banei_targets t
          left join current_race_grade crg
            on crg.source = t.source
            and crg.kaisai_nen = t.kaisai_nen
            and crg.kaisai_tsukihi = t.kaisai_tsukihi
            and crg.keibajo_code = t.keibajo_code
            and crg.race_bango = t.race_bango
        ) t
        asof left join horse_grade_cumul c
          on c.source = t.source
          and c.ketto_toroku_bango = t.ketto_toroku_bango
          and c.grade_letter = t.grade_letter
          and c.race_date < t.race_date
        """
    )
    con.execute(
        """
        create or replace temp table target_total_career as
        select t.source, t.ketto_toroku_bango, t.race_date,
          c.total_past_starts, c.total_past_wins
        from (
          select distinct source, ketto_toroku_bango, race_date
          from banei_targets
        ) t
        asof left join horse_grade_total_career c
          on c.source = t.source
          and c.ketto_toroku_bango = t.ketto_toroku_bango
          and c.race_date < t.race_date
        """
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
      left join target_grade_career hgp
        on hgp.source = bwc.source
        and hgp.ketto_toroku_bango = bwc.ketto_toroku_bango
        and hgp.race_date = bwc.race_date
      left join target_current_grade_career hcgo
        on hcgo.source = bwc.source
        and hcgo.ketto_toroku_bango = bwc.ketto_toroku_bango
        and hcgo.race_date = bwc.race_date
      left join target_total_career htc
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
    stage_target_scope(con, input_glob)
    stage_banei_grade_history(con, args.from_date)
    stage_horse_grade_cumul(con)
    stage_horse_total_career(con)
    stage_current_race_grade(con)
    stage_field_career_avg(con)
    stage_target_grade_careers(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
