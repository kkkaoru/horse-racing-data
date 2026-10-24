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

v2 columns (``*_v2``, every column above): only retrained models select them,
the v1 columns stay byte-identical.
  - sire/damsire keyed on the normalised breeding id (nvd_um 8-digit ==
    nvd_nu/jvd_um 10-digit), so one stallion is no longer split in two.
  - Ban-ei (keibajo 83) going is a sand-moisture reading, not 良/稍/重/不良.
    v1 reads its two code bytes as going codes 1-9 (and drops 'A'-'F' rows as
    NULL). v2 decodes ``babajotai_code_shiba || babajotai_code_dirt`` as two
    hex digits = moisture x 10 (0x00-0x61 = 0.0-9.7%) and buckets it into
    11-14. No JV/UmaConn code table for this is in the repo; the encoding is
    derived from local nvd_ra data: the decoded value is continuous and the
    winners' mean soha_time falls monotonically from 119.2s (0.5-0.9%) to
    103.6s (3.5-3.9%) as moisture rises (2015-2026, ~20k races), i.e. wetter
    sand runs faster. '00' (107 races) is treated as missing.

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
from pedigree_staging import normalized_pedigree_id_sql, stage_horse_pedigree

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
RACE_KEY_COLUMNS = ("kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango")
BAN_EI_KEIBAJO = "83"
V2_SUFFIX = "_v2"
# Ban-ei moisture buckets (moisture x 10 upper bounds) -> codes 11..14, kept
# disjoint from the 1-4 going codes of the other venues.
BAN_EI_MOISTURE_BUCKETS: tuple[tuple[int, int], ...] = ((10, 11), (20, 12), (30, 13))
BAN_EI_MOISTURE_TOP_BUCKET = 14
_BABA_V1_OUTPUT_COLUMNS = (
    "current_baba_condition",
    "horse_baba_career_starts",
    "horse_baba_win_rate",
    "sire_baba_career_starts",
    "sire_baba_win_rate",
    "damsire_baba_career_starts",
    "damsire_baba_win_rate",
    "sire_horse_baba_combined_score",
)
# Columns this layer appends. A serve-time race-day refresh re-runs the layer
# over an input that already carries them, so the base drops them first.
BABA_OUTPUT_COLUMNS = _BABA_V1_OUTPUT_COLUMNS + tuple(
    f"{column}{V2_SUFFIX}" for column in _BABA_V1_OUTPUT_COLUMNS
)


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
    parser.add_argument(
        "--current-going",
        type=Path,
        default=None,
        help=(
            "Serve-time pre-race going side record (parquet keyed by source + "
            "race key with RA-shaped babajotai_code_shiba / babajotai_code_dirt). "
            "Used only for races whose RA carries no going."
        ),
    )
    parser.add_argument(
        "--skip-v2-features",
        action="store_true",
        help="Serving-only: do not compute the *_v2 columns.",
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


def baba_condition_v2_sql(alias: str) -> str:
    """v1 going everywhere except Ban-ei, where the sand moisture is bucketed."""
    moisture = (
        "try_cast('0x' || nullif("
        f"{alias}.babajotai_code_shiba || {alias}.babajotai_code_dirt, '00')"
        " as integer)"
    )
    buckets = " ".join(
        f"when {moisture} < {bound} then {code}" for bound, code in BAN_EI_MOISTURE_BUCKETS
    )
    return f"""case
          when {alias}.keibajo_code = '{BAN_EI_KEIBAJO}' then
            case when {moisture} is null then null {buckets}
              else {BAN_EI_MOISTURE_TOP_BUCKET} end
          else {baba_condition_sql(alias)}
        end"""


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
          {baba_condition_sql("ra")} as baba_cond,
          {baba_condition_v2_sql("ra")} as baba_cond_v2
        from pg.jvd_ra ra
        where ra.kaisai_nen >= {from_year}
          and {jra_predicate}
        union all
        select
          'nar' as source,
          ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,
          {baba_condition_sql("ra")} as baba_cond,
          {baba_condition_v2_sql("ra")} as baba_cond_v2
        from pg.nvd_ra ra
        where ra.kaisai_nen >= {from_year}
          and {nar_predicate}
        """
    )
    con.execute(f"create index race_baba_idx on race_baba ({RACE_PARTITION})")


def apply_current_going(con: duckdb.DuckDBPyConnection, going_path: Path) -> None:
    """Coalesce the race-day going side record under the live RA going.

    The side record is RA-shaped, so the same ``baba_condition_sql`` maps it
    exactly like training maps a results RA row. RA keeps priority: the side
    record only fills races whose RA going is still '0' (pre-race RA).
    ``baba_cond_v2`` takes the same 1-4 code (v2 jra/nar going is v1's);
    Ban-ei v2 is a moisture bucket the side record never carries, so it stays NULL.
    """
    going = sql_string_literal(going_path.as_posix())
    con.execute(
        f"""
        create or replace temp table race_baba as
        select {RACE_PARTITION},
          coalesce(rb.baba_cond, {baba_condition_sql("g")}) as baba_cond,
          coalesce(
            rb.baba_cond_v2,
            case when g.keibajo_code <> '{BAN_EI_KEIBAJO}' then {baba_condition_sql("g")} end
          ) as baba_cond_v2
        from race_baba rb
        full outer join read_parquet({going}) g
          using ({RACE_PARTITION})
        """
    )
    con.execute(f"create index race_baba_idx on race_baba ({RACE_PARTITION})")


def _cohort_id_sql(column: str, include_v2: bool) -> str:
    """Normalised ids widen the cohort to the v2 lineage (a raw-id superset)."""
    return normalized_pedigree_id_sql(column) if include_v2 else column


def stage_target_pedigree_context(
    con: duckdb.DuckDBPyConnection, include_v2: bool = True
) -> None:
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
    # Normalised ids: the cohort then covers the v2 lineage, a superset of the
    # raw-id lineage the v1 aggregates key on.
    con.execute(
        f"""
        create or replace temp table target_pedigree_ids as
        select distinct
          {_cohort_id_sql("sire_id", include_v2)} as sire_id,
          {_cohort_id_sql("damsire_id", include_v2)} as damsire_id
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
    include_v2: bool = True,
) -> None:
    """Stage history for input horses and their sire/damsire cohorts."""
    history_horses = con.execute(
        f"""
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
          ) target_sires
            on target_sires.sire_id = {_cohort_id_sql("hp.sire_id", include_v2)}
          union
          select hp.ketto_toroku_bango
          from horse_pedigree hp
          inner join (
            select distinct damsire_id
            from target_pedigree_ids
            where damsire_id is not null
          ) target_damsires
            on target_damsires.damsire_id = {_cohort_id_sql("hp.damsire_id", include_v2)}
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
    history_condition = (
        f"coalesce({baba_condition_sql('ra')}, {baba_condition_v2_sql('ra')})"
        if include_v2
        else baba_condition_sql("ra")
    )

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
          {baba_condition_sql("ra")} as baba_cond,
          {baba_condition_v2_sql("ra")} as baba_cond_v2
        from pg.{se_table} se
        inner join pg.{ra_table} ra
          using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        where se.kaisai_nen >= {from_year_literal}
          and se.kaisai_nen || se.kaisai_tsukihi >= {from_date_literal}
          and {finish_position} is not null
          and {history_condition} is not null
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


def stage_horse_baba_cumul(con: duckdb.DuckDBPyConnection, suffix: str = "") -> None:
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
        f"""
        create or replace temp table horse_baba_daily{suffix} as
        select source, ketto_toroku_bango, baba_cond{suffix} as baba_cond, race_date,
          count(*) as starts_on_day,
          sum(case when finish_position = 1 then 1 else 0 end) as wins_on_day
        from race_history
        where baba_cond{suffix} is not null
        group by all
        """
    )
    con.execute(
        f"""
        create or replace temp table horse_baba_cumul{suffix} as
        select source, ketto_toroku_bango, baba_cond, race_date,
          sum(starts_on_day) over horse_baba_career as past_starts,
          sum(wins_on_day) over horse_baba_career as past_wins
        from horse_baba_daily{suffix}
        window horse_baba_career as (
          partition by source, ketto_toroku_bango, baba_cond
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        f"create index horse_baba_cumul{suffix}_idx on horse_baba_cumul{suffix} "
        "(source, ketto_toroku_bango, baba_cond, race_date)"
    )


def _lineage_key_sql(column: str, suffix: str) -> str:
    """Raw id for v1; the normalised id for the ``_v2`` aggregates."""
    return normalized_pedigree_id_sql(column) if suffix else column


def stage_sire_baba_cumul(con: duckdb.DuckDBPyConnection, suffix: str = "") -> None:
    """sire 単位の baba 別 cumul stats。

    horse_pedigree で sire_id を取得 → race_history と join → 集計。

    Window is INCLUSIVE of the current row -- see stage_horse_baba_cumul's
    docstring for why (append_features_sql ASOF-joins with a strict race_date
    inequality instead of an exact-date equality).
    """
    con.execute(
        f"""
        create or replace temp table sire_baba_daily{suffix} as
        select {_lineage_key_sql("p.sire_id", suffix)} as sire_id,
          h.baba_cond{suffix} as baba_cond, h.race_date,
          count(*) as starts_on_day,
          sum(case when h.finish_position = 1 then 1 else 0 end) as wins_on_day
        from race_history h
        join horse_pedigree p using (ketto_toroku_bango)
        where p.sire_id is not null and h.baba_cond{suffix} is not null
        group by all
        """
    )
    con.execute(
        f"""
        create or replace temp table sire_baba_cumul{suffix} as
        select sire_id, baba_cond, race_date,
          sum(starts_on_day) over sire_baba_career as past_starts,
          sum(wins_on_day) over sire_baba_career as past_wins
        from sire_baba_daily{suffix}
        window sire_baba_career as (
          partition by sire_id, baba_cond
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        f"create index sire_baba_cumul{suffix}_idx on sire_baba_cumul{suffix} "
        "(sire_id, baba_cond, race_date)"
    )


def stage_damsire_baba_cumul(con: duckdb.DuckDBPyConnection, suffix: str = "") -> None:
    """damsire 単位の baba 別 cumul stats.

    Window is INCLUSIVE of the current row -- see stage_horse_baba_cumul's
    docstring for why (append_features_sql ASOF-joins with a strict race_date
    inequality instead of an exact-date equality).
    """
    con.execute(
        f"""
        create or replace temp table damsire_baba_daily{suffix} as
        select {_lineage_key_sql("p.damsire_id", suffix)} as damsire_id,
          h.baba_cond{suffix} as baba_cond, h.race_date,
          count(*) as starts_on_day,
          sum(case when h.finish_position = 1 then 1 else 0 end) as wins_on_day
        from race_history h
        join horse_pedigree p using (ketto_toroku_bango)
        where p.damsire_id is not null and h.baba_cond{suffix} is not null
        group by all
        """
    )
    con.execute(
        f"""
        create or replace temp table damsire_baba_cumul{suffix} as
        select damsire_id, baba_cond, race_date,
          sum(starts_on_day) over damsire_baba_career as past_starts,
          sum(wins_on_day) over damsire_baba_career as past_wins
        from damsire_baba_daily{suffix}
        window damsire_baba_career as (
          partition by damsire_id, baba_cond
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        f"create index damsire_baba_cumul{suffix}_idx on damsire_baba_cumul{suffix} "
        "(damsire_id, baba_cond, race_date)"
    )


def affinity_columns_sql(suffix: str) -> str:
    """The seven affinity columns from the ``*{suffix}`` cumulative joins."""
    hbc, sbc, dbc = f"hbc{suffix}", f"sbc{suffix}", f"dbc{suffix}"
    return f"""
        {hbc}.past_starts as horse_baba_career_starts{suffix},
        case when {hbc}.past_starts > 0
             then {hbc}.past_wins::double / {hbc}.past_starts
             else null end as horse_baba_win_rate{suffix},
        {sbc}.past_starts as sire_baba_career_starts{suffix},
        case when {sbc}.past_starts > 0
             then {sbc}.past_wins::double / {sbc}.past_starts
             else null end as sire_baba_win_rate{suffix},
        {dbc}.past_starts as damsire_baba_career_starts{suffix},
        case when {dbc}.past_starts > 0
             then {dbc}.past_wins::double / {dbc}.past_starts
             else null end as damsire_baba_win_rate{suffix},
        case
          when {hbc}.past_starts > 0 and {sbc}.past_starts > 0
            then (({hbc}.past_wins::double / {hbc}.past_starts) + ({sbc}.past_wins::double / {sbc}.past_starts)) / 2.0
          when {hbc}.past_starts > 0 then {hbc}.past_wins::double / {hbc}.past_starts
          when {sbc}.past_starts > 0 then {sbc}.past_wins::double / {sbc}.past_starts
          else null
        end as sire_horse_baba_combined_score{suffix}"""


def affinity_joins_sql(suffix: str) -> str:
    """ASOF joins of the ``*{suffix}`` cumulative tables (strictly prior date)."""
    hbc, sbc, dbc = f"hbc{suffix}", f"sbc{suffix}", f"dbc{suffix}"
    condition = f"current_baba_condition{suffix}"
    return f"""
      asof left join horse_baba_cumul{suffix} {hbc}
        on {hbc}.source = bwp.source
        and {hbc}.ketto_toroku_bango = bwp.ketto_toroku_bango
        and {hbc}.baba_cond = bwp.{condition}
        and bwp.race_date > {hbc}.race_date
      asof left join sire_baba_cumul{suffix} {sbc}
        on {sbc}.sire_id = bwp.sire_id{suffix}
        and {sbc}.baba_cond = bwp.{condition}
        and bwp.race_date > {sbc}.race_date
      asof left join damsire_baba_cumul{suffix} {dbc}
        on {dbc}.damsire_id = bwp.damsire_id{suffix}
        and {dbc}.baba_cond = bwp.{condition}
        and bwp.race_date > {dbc}.race_date"""


def append_features_sql(input_glob: str, include_v2: bool = True) -> str:
    v2_columns = f",\n        {affinity_columns_sql(V2_SUFFIX)}" if include_v2 else ""
    v2_joins = affinity_joins_sql(V2_SUFFIX) if include_v2 else ""
    v2_condition = (
        ",\n        rb.baba_cond_v2 as current_baba_condition_v2" if include_v2 else ""
    )
    v2_ids = (
        f""",
        {normalized_pedigree_id_sql("hp.sire_id")} as sire_id_v2,
        {normalized_pedigree_id_sql("hp.damsire_id")} as damsire_id_v2"""
        if include_v2
        else ""
    )
    v2_id_exclude = ", sire_id_v2, damsire_id_v2" if include_v2 else ""
    return f"""
    with base as (
      select columns(c -> c not in {BABA_OUTPUT_COLUMNS})
      from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_with_baba as (
      select b.*, rb.baba_cond as current_baba_condition{v2_condition}
      from base b
      left join race_baba rb
        on rb.source = b.source
        and rb.kaisai_nen = b.kaisai_nen
        and rb.kaisai_tsukihi = b.kaisai_tsukihi
        and rb.keibajo_code = b.keibajo_code
        and rb.race_bango = b.race_bango
    ),
    base_with_pedigree as (
      select bwb.*, hp.sire_id, hp.damsire_id{v2_ids}
      from base_with_baba bwb
      left join horse_pedigree hp on hp.ketto_toroku_bango = bwb.ketto_toroku_bango
    ),
    joined as (
      select
        bwp.* exclude (sire_id, damsire_id{v2_id_exclude}),
        {affinity_columns_sql("")}{v2_columns}
      from base_with_pedigree bwp
      {affinity_joins_sql("")}
      {v2_joins}
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
    if args.current_going is not None:
        apply_current_going(con, args.current_going)
    include_v2 = not args.skip_v2_features
    stage_target_pedigree_context(con, include_v2)
    stage_race_history_with_baba(con, args.from_date, include_v2)
    stage_horse_baba_cumul(con)
    stage_sire_baba_cumul(con)
    stage_damsire_baba_cumul(con)
    if include_v2:
        stage_horse_baba_cumul(con, V2_SUFFIX)
        stage_sire_baba_cumul(con, V2_SUFFIX)
        stage_damsire_baba_cumul(con, V2_SUFFIX)
    write_partitioned(con, append_features_sql(input_glob, include_v2), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
