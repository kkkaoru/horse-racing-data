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
from _catalog_attach import attach_source_catalog
from _resource_defaults import add_resource_args, apply_to_connection
from pedigree_staging import stage_horse_pedigree

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
RACE_KEY_COLUMNS = ("kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango")


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
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Compatibility marker for a focused keibajo_code:race_bango input. "
            "History staging is always narrowed to the horses and pedigree "
            "cohorts present in the input parquet."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    attach_source_catalog(con, pg_url)


def baba_condition_sql(alias: str) -> str:
    """Return the legacy race_baba expression over already-carried RA columns."""
    return f"""coalesce(
            try_cast(nullif({alias}.babajotai_code_shiba, '0') as int),
            try_cast(nullif({alias}.babajotai_code_dirt, '0') as int)
          )"""


def sql_string_literal(value: str) -> str:
    """Quote one DuckDB/PostgreSQL string literal without changing its value."""
    return "'" + value.replace("'", "''") + "'"


def string_in_predicate(column: str, values: list[str]) -> str:
    """Build a literal predicate that remote catalog scans can push down."""
    if not values:
        return "false"
    literals = ", ".join(sql_string_literal(value) for value in values)
    return f"{column} in ({literals})"


def race_key_predicate(
    alias: str, race_keys: list[tuple[str, str, str, str]]
) -> str:
    """Build a source-specific literal race-key predicate for live RA lookup."""
    if not race_keys:
        return "false"
    clauses = []
    for race_key in race_keys:
        parts = [
            f"{alias}.{column} = {sql_string_literal(value)}"
            for column, value in zip(RACE_KEY_COLUMNS, race_key, strict=True)
        ]
        clauses.append("(" + " and ".join(parts) + ")")
    return "(" + " or ".join(clauses) + ")"


def stage_base_input(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """Stage target input rows for focused history narrowing."""
    con.execute(
        f"""
        create or replace temp table base_input as
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year
        from read_parquet('{input_glob}', hive_partitioning=true)
        where ketto_toroku_bango is not null
        """
    )
    con.execute("create index base_input_horse_idx on base_input (ketto_toroku_bango)")


def stage_current_race_baba(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """Read live RA baba only for races present in the input parquet.

    Historical baba comes from the direct raw SE/RA history scan, while current
    baba must remain a separate live RA lookup: a day-base input can predate a
    same-day going change. Restricting the lookup to distinct input race keys
    avoids all-years materialisation without freezing that live value at
    day-base time.
    """
    jra_races = con.execute(
        """
        select distinct kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        from base_input
        where source = 'jra'
        order by all
        """
    ).fetchall()
    nar_races = con.execute(
        """
        select distinct kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        from base_input
        where source = 'nar'
        order by all
        """
    ).fetchall()
    jra_predicate = race_key_predicate("ra", jra_races)
    nar_predicate = race_key_predicate("ra", nar_races)
    from_year = sql_string_literal(from_date[:4])
    con.execute(
        f"""
        create or replace temp table race_baba as
        select
          'jra' as source,
          ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,
          {baba_condition_sql("ra")} as baba_cond
        from pg.jvd_ra ra
        where ra.kaisai_nen >= {from_year}
          and {jra_predicate}
        union all
        select
          'nar' as source,
          ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,
          {baba_condition_sql("ra")} as baba_cond
        from pg.nvd_ra ra
        where ra.kaisai_nen >= {from_year}
          and {nar_predicate}
        """
    )
    con.execute(f"create index race_baba_idx on race_baba ({RACE_PARTITION})")


def stage_target_pedigree_context(con: duckdb.DuckDBPyConnection) -> None:
    """Stage input horses and their sire/damsire ids for history narrowing."""
    con.execute(
        """
        create or replace temp table target_horses as
        select distinct ketto_toroku_bango
        from base_input
        where ketto_toroku_bango is not null
        """
    )
    con.execute("create index target_horses_idx on target_horses (ketto_toroku_bango)")
    con.execute(
        """
        create or replace temp table target_pedigree_ids as
        select distinct sire_id, damsire_id
        from base_input b
        left join horse_pedigree hp using (ketto_toroku_bango)
        where sire_id is not null or damsire_id is not null
        """
    )
    con.execute(
        "create index target_pedigree_sire_idx on target_pedigree_ids (sire_id)"
    )
    con.execute(
        "create index target_pedigree_damsire_idx on target_pedigree_ids (damsire_id)"
    )


def stage_race_history_with_baba(
    con: duckdb.DuckDBPyConnection,
    from_date: str,
) -> None:
    """Stage history for input horses and their sire/damsire cohorts."""
    history_horses = con.execute(
        """
        select ketto_toroku_bango
        from (
          select ketto_toroku_bango
          from target_horses
          union
          select hp.ketto_toroku_bango
          from horse_pedigree hp
          inner join (
            select distinct sire_id
            from target_pedigree_ids
            where sire_id is not null
          ) target_sires using (sire_id)
          union
          select hp.ketto_toroku_bango
          from horse_pedigree hp
          inner join (
            select distinct damsire_id
            from target_pedigree_ids
            where damsire_id is not null
          ) target_damsires using (damsire_id)
        ) cohort
        where ketto_toroku_bango is not null
        order by ketto_toroku_bango
        """
    ).fetchall()
    horse_predicate = string_in_predicate(
        "se.ketto_toroku_bango", [row[0] for row in history_horses]
    )
    from_date_literal = sql_string_literal(from_date)
    from_year_literal = sql_string_literal(from_date[:4])

    def raw_branch(source: str, se_table: str, ra_table: str) -> str:
        finish_position = (
            "try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)"
        )
        return f"""
        select
          '{source}' as source,
          se.kaisai_nen || se.kaisai_tsukihi as race_date,
          se.kaisai_nen,
          se.kaisai_tsukihi,
          se.keibajo_code,
          se.race_bango,
          se.ketto_toroku_bango,
          {finish_position} as finish_position,
          {baba_condition_sql("ra")} as baba_cond
        from pg.{se_table} se
        inner join pg.{ra_table} ra
          using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        where se.kaisai_nen >= {from_year_literal}
          and se.kaisai_nen || se.kaisai_tsukihi >= {from_date_literal}
          and {finish_position} is not null
          and {baba_condition_sql("ra")} is not null
          and {horse_predicate}
        """

    con.execute(
        f"""
        create or replace temp table race_history as
        {raw_branch("jra", "jvd_se", "jvd_ra")}
        union all
        {raw_branch("nar", "nvd_se", "nvd_ra")}
        """
    )
    con.execute(
        "create index race_history_horse_idx on race_history (source, ketto_toroku_bango, baba_cond, race_date)"
    )


def stage_horse_baba_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """horse 自身の baba 別 cumul stats (lookback: 当該レース除外)。

    Pre-aggregate to (horse, race_date, baba_cond): starts + wins per day
    → window cumulative.

    The window is INCLUSIVE of the current row (rows ... and current row), not
    exclusive (... and 1 preceding). append_features_sql ASOF-joins this table
    to the target using a STRICT race_date inequality (bwp.race_date >
    hbc.race_date) instead of an exact-date equality, so this resolves the
    horse's latest ACTUAL prior race in the same baba_cond and takes its
    inclusive cumulative -- equivalent to "everything strictly before the
    target's race_date". The old exact-date equality join required
    horse_baba_cumul to carry a row keyed at exactly the target's own
    race_date, which only a COMPLETED race can ever produce (race_history is
    built from settled raw SE rows filtered to finish_position is not null), so
    every live prediction on an upcoming race fell back to NULL. For
    a historical row (target's own race_date IS one of this horse's actual
    race dates in this baba_cond), the inclusive cumulative at the
    immediately-preceding actual race date is byte-identical to the old
    exclusive-window value at the target's own race_date -- both represent
    "every same-baba_cond race with race_date < target.race_date" and there is
    no other same-baba_cond race for this horse strictly between the two
    dates.
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
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index horse_baba_cumul_idx on horse_baba_cumul (source, ketto_toroku_bango, baba_cond, race_date)"
    )


def stage_sire_baba_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """sire 単位の baba 別 cumul stats。

    horse_pedigree で sire_id を取得 → race_history と join → 集計。

    Window is INCLUSIVE of the current row -- see stage_horse_baba_cumul's
    docstring for why (append_features_sql ASOF-joins with a strict race_date
    inequality instead of an exact-date equality).
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
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index sire_baba_cumul_idx on sire_baba_cumul (sire_id, baba_cond, race_date)"
    )


def stage_damsire_baba_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """damsire 単位の baba 別 cumul stats.

    Window is INCLUSIVE of the current row -- see stage_horse_baba_cumul's
    docstring for why (append_features_sql ASOF-joins with a strict race_date
    inequality instead of an exact-date equality).
    """
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
          rows between unbounded preceding and current row
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
      asof left join horse_baba_cumul hbc
        on hbc.source = bwp.source
        and hbc.ketto_toroku_bango = bwp.ketto_toroku_bango
        and hbc.baba_cond = bwp.current_baba_condition
        and bwp.race_date > hbc.race_date
      asof left join sire_baba_cumul sbc
        on sbc.sire_id = bwp.sire_id
        and sbc.baba_cond = bwp.current_baba_condition
        and bwp.race_date > sbc.race_date
      asof left join damsire_baba_cumul dbc
        on dbc.damsire_id = bwp.damsire_id
        and dbc.baba_cond = bwp.current_baba_condition
        and bwp.race_date > dbc.race_date
    )
    select * from joined
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
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    stage_horse_pedigree(con)
    stage_base_input(con, input_glob)
    stage_current_race_baba(con, args.from_date)
    stage_target_pedigree_context(con)
    stage_race_history_with_baba(con, args.from_date)
    stage_horse_baba_cumul(con)
    stage_sire_baba_cumul(con)
    stage_damsire_baba_cumul(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
