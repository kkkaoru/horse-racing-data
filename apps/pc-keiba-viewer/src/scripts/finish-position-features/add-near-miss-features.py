#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append "near-miss / 2nd-place specialization" features to an existing
finish-position feature parquet, producing a new layer (v6).

Motivation:
  既存 features (career_win_rate, career_place_rate=3着内率) では 1 着型と 2 着型が
  区別できない。empirical 検証で、3058 馬の career 2 着率 23.9% に対し win 8.7% の
  「2 着型 horse」が確実に存在することを確認 (2026-05-20)。これを直接 encode する。

Features added (8):
  Per-horse (lookback: 当該レース < race_date):
    1. career_place2_rate          — 2 着回数 / 出走数 (career)
    2. career_place2_to_win_ratio  — career_place2_rate / max(career_win_rate, 0.01)
    3. career_avg_2nd_margin_decisec — 2 着時の time_sa 平均 (秒×10、小さいほど僅差)
    4. recent_place2_count_5        — 直近 5 走で 2 着になった回数
    5. recent_2nd_margin_avg_5      — 直近で 2 着になった時の time_sa 平均
  Per-jockey (lookback: 当該レース < race_date):
    6. jockey_career_place2_rate    — 騎手 career 2 着率
  Race-internal:
    7. field_dominant_favorite_indicator — 1 番人気オッズ / 2 番人気オッズ (低いほど本命支配)
    8. horse_popularity_vs_field    — tansho_ninkijun / shusso_tosu (0-1)

Data leakage 防止: window function で rows between unbounded preceding and 1 preceding。
race_date strictly less than current race_date のみを集計。

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-near-miss-features.py \\
    --input-dir tmp/feat-jra-v5-post \\
    --output-dir tmp/feat-jra-v6
"""
from __future__ import annotations

import argparse
import os
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

from _catalog_attach import attach_source_catalog

from _resource_defaults import add_resource_args, apply_to_connection
from pedigree_staging import stage_horse_pedigree

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
RACE_PARTITION_BY = "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
DISTANCE_TOLERANCE_M = 200
UNKNOWN_HORSE_REGISTRATION = "0000000000"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_near_miss_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument("--to-date", type=str, default="20991231")
    parser.add_argument(
        "--target-from-date",
        type=str,
        default=None,
        help="Inclusive target-output start date (YYYYMMDD); history still starts at --from-date.",
    )
    parser.add_argument(
        "--target-to-date",
        type=str,
        default=None,
        help="Inclusive target-output end date (YYYYMMDD); history is capped at this date.",
    )
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Focused production mode keibajo_code:race_bango. The input parquet "
            "is already race-scoped; this restricts history staging to target entities."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def validate_yyyymmdd(value: str, option_name: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
    except ValueError as error:
        raise ValueError(f"{option_name} must be a valid YYYYMMDD date: {value}") from error
    if parsed.strftime("%Y%m%d") != value:
        raise ValueError(f"{option_name} must be a valid YYYYMMDD date: {value}")
    return value


def target_date_filter_sql(
    alias: str, target_from_date: str | None, target_to_date: str | None
) -> str:
    predicates: list[str] = []
    if target_from_date is not None:
        predicates.append(f"{alias}.race_date >= '{target_from_date}'")
    if target_to_date is not None:
        predicates.append(f"{alias}.race_date <= '{target_to_date}'")
    return " and ".join(predicates) if predicates else "true"


def optional_parquet_column_sql(
    con: duckdb.DuckDBPyConnection,
    input_glob: str,
    column_name: str,
    fallback_type: str,
    alias: str = "b",
) -> str:
    """Select an optional input column without weakening minimal input support."""
    columns = {
        str(row[0])
        for row in con.execute(
            f"describe select * from read_parquet('{input_glob}', hive_partitioning=true)"
        ).fetchall()
    }
    if column_name in columns:
        return f"{alias}.{column_name}"
    return f"cast(null as {fallback_type}) as {column_name}"


def require_bounded_bulk(
    focused_target: bool,
    target_from_date: str | None,
    target_to_date: str | None,
) -> None:
    if not focused_target and (
        target_from_date is None or target_to_date is None
    ):
        raise ValueError(
            "offline bulk mode requires --target-from-date and --target-to-date"
        )


def drop_temp_tables(
    con: duckdb.DuckDBPyConnection, table_names: tuple[str, ...]
) -> None:
    """Release materialized intermediates as soon as their last consumer ends."""
    for table_name in table_names:
        con.execute(f"drop table if exists {table_name}")


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    attach_source_catalog(con, pg_url)


def stage_target_entities(
    con: duckdb.DuckDBPyConnection,
    input_glob: str,
    raw_catalog: bool = False,
    target_from_date: str | None = None,
    target_to_date: str | None = None,
) -> None:
    """Stage the scoped target context and its history-filter entities.

    ``target_current`` is the single focused current-row Catalog lookup. Both
    ``target_entities`` (history pushdown) and ``target_context`` (current race
    feature context) are derived from it so the latter does not repeat the
    same external join.
    """
    jockey_select = (
        "coalesce(nullif(trim(jra_se.kishumei_ryakusho), ''), "
        "nullif(trim(nar_se.kishumei_ryakusho), ''))"
        if raw_catalog
        else "nullif(trim(rec.kishumei_ryakusho), '')"
    )
    current_join = (
        """
        left join pg.jvd_se jra_se
          on b.source = 'jra'
          and jra_se.kaisai_nen = b.kaisai_nen
          and jra_se.kaisai_tsukihi = b.kaisai_tsukihi
          and jra_se.keibajo_code = b.keibajo_code
          and jra_se.race_bango = b.race_bango
          and jra_se.ketto_toroku_bango = b.ketto_toroku_bango
        left join pg.nvd_se nar_se
          on b.source = 'nar'
          and nar_se.kaisai_nen = b.kaisai_nen
          and nar_se.kaisai_tsukihi = b.kaisai_tsukihi
          and nar_se.keibajo_code = b.keibajo_code
          and nar_se.race_bango = b.race_bango
          and nar_se.ketto_toroku_bango = b.ketto_toroku_bango
        """
        if raw_catalog
        else """
        left join pg.race_entry_corner_features rec
          on rec.source = b.source
          and rec.kaisai_nen = b.kaisai_nen
          and rec.kaisai_tsukihi = b.kaisai_tsukihi
          and rec.keibajo_code = b.keibajo_code
          and rec.race_bango = b.race_bango
          and rec.ketto_toroku_bango = b.ketto_toroku_bango
        """
    )
    target_filter = target_date_filter_sql(
        "b", target_from_date, target_to_date
    )
    popularity_select = optional_parquet_column_sql(
        con, input_glob, "tansho_ninkijun", "integer"
    )
    field_size_select = optional_parquet_column_sql(
        con, input_glob, "shusso_tosu", "bigint"
    )
    con.execute(
        f"""
        create or replace temp table target_current as
        with target_base as (
          select distinct
            b.source,
            b.race_date,
            b.kaisai_nen,
            b.kaisai_tsukihi,
            b.keibajo_code,
            b.race_bango,
            b.ketto_toroku_bango,
            b.kyori,
            b.track_code,
            b.grade_code,
            {popularity_select},
            {field_size_select}
          from read_parquet('{input_glob}', hive_partitioning=true) b
          where b.ketto_toroku_bango is not null
            and {target_filter}
        )
        select distinct
          b.source,
          b.race_date,
          b.kaisai_nen,
          b.kaisai_tsukihi,
          b.keibajo_code,
          b.race_bango,
          b.ketto_toroku_bango,
          b.kyori,
          b.track_code,
          b.grade_code,
          b.tansho_ninkijun,
          b.shusso_tosu,
          {jockey_select} as kishumei_ryakusho,
          hp.sire_id,
          hp.damsire_id
        from target_base b
        {current_join}
        left join horse_pedigree hp
          on hp.ketto_toroku_bango = b.ketto_toroku_bango
        """
    )
    con.execute(
        """
        create or replace temp table target_entities as
        select distinct source, ketto_toroku_bango, kishumei_ryakusho,
                        sire_id, damsire_id
        from target_current
        """
    )
    con.execute(
        "create index target_entities_horse_idx on target_entities "
        "(source, ketto_toroku_bango)"
    )
    con.execute(
        "create index target_entities_jockey_idx on target_entities "
        "(source, kishumei_ryakusho)"
    )


def race_history_focus_filter_sql(focused_target: bool) -> str:
    if not focused_target:
        return ""
    return """
          and rec.source in (select distinct source from target_entities)
          and (
            exists (
              select 1 from target_entities te
              where te.source = rec.source
                and te.ketto_toroku_bango = rec.ketto_toroku_bango
            )
            or exists (
              select 1 from target_entities te
              where te.source = rec.source
                and te.kishumei_ryakusho is not null
                and te.kishumei_ryakusho = rec.kishumei_ryakusho
            )
            or exists (
              select 1
              from horse_pedigree hp
              join target_entities te
                on te.source = rec.source
                and (
                  (te.sire_id is not null and te.sire_id = hp.sire_id)
                  or (te.damsire_id is not null and te.damsire_id = hp.damsire_id)
                )
              where hp.ketto_toroku_bango = rec.ketto_toroku_bango
            )
          )
        """


def postgres_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def postgres_entity_scoped_race_history_query(
    con: duckdb.DuckDBPyConnection, from_date: str, to_date: str
) -> str:
    """Build a PostgreSQL-native query so entity filtering precedes transfer."""
    rows = con.execute(
        """
        select source, ketto_toroku_bango, kishumei_ryakusho,
          sire_id, damsire_id
        from target_entities
        """
    ).fetchall()
    values_by_source: dict[str, dict[str, set[str]]] = {}
    for raw_source, horse, jockey, sire, damsire in rows:
        source = str(raw_source)
        if source not in {"jra", "nar"}:
            raise ValueError(
                f"unsupported entity-scoped race-history source: {source}"
            )
        source_values = values_by_source.setdefault(
            source,
            {"horse": set(), "jockey": set(), "sire": set(), "damsire": set()},
        )
        for key, value in (
            ("horse", horse),
            ("jockey", jockey),
            ("sire", sire),
            ("damsire", damsire),
        ):
            if value is not None:
                source_values[key].add(str(value))
    if not values_by_source:
        raise ValueError("entity-scoped race-history target cannot be empty")

    remote_columns = {
        "horse": "rec.ketto_toroku_bango",
        "jockey": "rec.kishumei_ryakusho",
        "sire": "hp.sire_id",
        "damsire": "hp.damsire_id",
    }
    source_predicates: list[str] = []
    for source, source_values in sorted(values_by_source.items()):
        entity_predicates = [
            f"{remote_columns[key]} in "
            f"({', '.join(postgres_literal(value) for value in sorted(values))})"
            for key, values in source_values.items()
            if values
        ]
        source_predicates.append(
            f"(rec.source = {postgres_literal(source)} and "
            f"({' or '.join(entity_predicates)}))"
        )
    entity_filter = " or ".join(source_predicates)
    return f"""
        with combined as (
          select ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), '') as sire_id,
            nullif(trim(ketto_joho_05a), '') as damsire_id,
            1 as priority
          from jvd_um where ketto_toroku_bango is not null
          union all
          select ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), ''),
            nullif(trim(ketto_joho_05a), ''), 2
          from nvd_um where ketto_toroku_bango is not null
          union all
          select ketto_toroku_bango,
            nullif(trim(ketto_joho_01a), ''),
            nullif(trim(ketto_joho_05a), ''), 3
          from nvd_nu where ketto_toroku_bango is not null
        ),
        pedigree as (
          select distinct on (ketto_toroku_bango)
            ketto_toroku_bango, sire_id, damsire_id
          from combined order by ketto_toroku_bango, priority
        )
        select rec.source, rec.race_date, rec.kaisai_nen,
          rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
          rec.ketto_toroku_bango, rec.kishumei_ryakusho,
          rec.finish_position, rec.time_sa, rec.tansho_odds,
          rec.tansho_ninkijun, rec.shusso_tosu, rec.kyori,
          rec.track_code, rec.grade_code
        from race_entry_corner_features rec
        left join pedigree hp using (ketto_toroku_bango)
        where rec.race_date >= {postgres_literal(from_date)}
          and rec.race_date <= {postgres_literal(to_date)}
          and rec.kaisai_nen >= {postgres_literal(from_date[:4])}
          and rec.kaisai_nen <= {postgres_literal(to_date[:4])}
          and rec.finish_position is not null
          and ({entity_filter})
        """


def postgres_query_table_sql(remote_query: str) -> str:
    escaped_query = remote_query.replace("'", "''")
    return f"""
        create or replace temp table race_history as
        select * from postgres_query('pg', '{escaped_query}')
        """


def stage_race_history(
    con: duckdb.DuckDBPyConnection,
    from_date: str,
    focused_target: bool = False,
    raw_catalog: bool = False,
    offline_sources: frozenset[str] | None = None,
    to_date: str = "20991231",
    remote_entity_pushdown: bool = False,
) -> None:
    """過去レースの finish_position / time_sa / tansho_odds / ninkijun / kishumei を staging。"""
    if focused_target and raw_catalog:
        source_rows = con.execute(
            "select distinct source from target_entities order by source"
        ).fetchall()
        target_sources = frozenset(str(row[0]) for row in source_rows)
        con.execute(raw_catalog_race_history_sql(from_date, target_sources))
        return
    if focused_target and remote_entity_pushdown:
        remote_query = postgres_entity_scoped_race_history_query(
            con, from_date, to_date
        )
        con.execute(postgres_query_table_sql(remote_query))
        return
    target_filter = race_history_focus_filter_sql(focused_target)
    source_filter = ""
    if not focused_target and offline_sources is not None:
        invalid = sorted(offline_sources.difference({"jra", "nar"}))
        if invalid:
            raise ValueError(f"unsupported offline race-history sources: {invalid}")
        if not offline_sources:
            raise ValueError("offline race-history source scope cannot be empty")
        literals = ", ".join(f"'{source}'" for source in sorted(offline_sources))
        source_filter = f"and rec.source in ({literals})"
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
          rec.kishumei_ryakusho,
          rec.finish_position,
          rec.time_sa,
          rec.tansho_odds,
          rec.tansho_ninkijun,
          rec.shusso_tosu,
          rec.kyori,
          rec.track_code,
          rec.grade_code
        from pg.race_entry_corner_features rec
        where rec.race_date >= '{from_date}'
          and rec.race_date <= '{to_date}'
          and rec.kaisai_nen >= substring('{from_date}', 1, 4)
          and rec.kaisai_nen <= substring('{to_date}', 1, 4)
          and rec.finish_position is not null
          {source_filter}
          {target_filter}
        """
    )


def project_scoped_race_history(con: duckdb.DuckDBPyConnection) -> None:
    """Drop output-only metadata before history windows and aggregations."""
    con.execute(
        """
        create or replace temp table race_history_projected as
        select source, race_date, keibajo_code, ketto_toroku_bango,
          kishumei_ryakusho, finish_position, time_sa, kyori,
          track_code, grade_code
        from race_history
        """
    )
    con.execute("drop table race_history")
    con.execute("alter table race_history_projected rename to race_history")


def raw_catalog_race_history_sql(
    from_date: str,
    target_sources: frozenset[str] = frozenset({"jra", "nar"}),
) -> str:
    """Build focused history directly from raw R2 Catalog SE/RA tables.

    The compatibility view computes a union and field-size aggregation across
    the entire archive before outer predicates can reliably prune it. This
    query puts source, year, target-date, settlement and target-entity
    predicates inside each Iceberg branch.

    Focused history is always strictly older than ``target_current``. The
    odds, popularity and field-size columns are only consumed by the exact
    current-race metadata join in ``append_features_sql``, so they cannot match
    any row staged here. Emit typed NULLs instead of running the old correlated
    field-size count against SE once per historical row.
    """

    def branch(source: str, se_table: str, ra_table: str) -> str:
        return f"""
          select
            '{source}' as source,
            se.kaisai_nen || se.kaisai_tsukihi as race_date,
            se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango,
            se.ketto_toroku_bango,
            se.kishumei_ryakusho,
            try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)
              as finish_position,
            try_cast(nullif(trim(se.time_sa), '0000') as double) / 10 as time_sa,
            cast(null as double) as tansho_odds,
            cast(null as integer) as tansho_ninkijun,
            cast(null as integer) as shusso_tosu,
            try_cast(nullif(trim(ra.kyori), '') as integer) as kyori,
            ra.track_code,
            ra.grade_code
          from pg.{se_table} se
          inner join pg.{ra_table} ra
            using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
          where se.kaisai_nen >= substring('{from_date}', 1, 4)
            and se.kaisai_nen || se.kaisai_tsukihi >= '{from_date}'
            and se.kaisai_nen || se.kaisai_tsukihi
                < (select max(race_date) from target_current)
            and try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)
                is not null
            and try_cast(nullif(trim(se.umaban), '') as integer) is not null
            and try_cast(nullif(trim(ra.kyori), '') as integer) is not null
            and exists (
              select 1 from target_entities te where te.source = '{source}'
            )
            and (
              exists (
                select 1 from target_entities te
                where te.source = '{source}'
                  and te.ketto_toroku_bango = se.ketto_toroku_bango
              )
              or exists (
                select 1 from target_entities te
                where te.source = '{source}'
                  and te.kishumei_ryakusho is not null
                  and te.kishumei_ryakusho = nullif(trim(se.kishumei_ryakusho), '')
              )
              or exists (
                select 1
                from horse_pedigree hp
                join target_entities te
                  on te.source = '{source}'
                  and (
                    (te.sire_id is not null and te.sire_id = hp.sire_id)
                    or (te.damsire_id is not null and te.damsire_id = hp.damsire_id)
                  )
                where hp.ketto_toroku_bango = se.ketto_toroku_bango
              )
            )
        """

    selected_branches = (
        [branch("jra", "jvd_se", "jvd_ra")] if "jra" in target_sources else []
    )
    if "nar" in target_sources:
        selected_branches.append(branch("nar", "nvd_se", "nvd_ra"))
    if not selected_branches:
        selected_branches = [
            branch("jra", "jvd_se", "jvd_ra"),
            branch("nar", "nvd_se", "nvd_ra"),
        ]
    focused_raw_sql = "\n          union all\n".join(selected_branches)
    return f"""
        create or replace temp table race_history as
        with focused_raw as (
          {focused_raw_sql}
        )
        select rec.*
        from focused_raw rec
        where rec.finish_position is not null
        """


def stage_horse_near_miss(con: duckdb.DuckDBPyConnection) -> None:
    """馬ごとの 2 着特化 stats を計算 (lookback only, 当該レース除外)。

    Windows are INCLUSIVE of the current row (rows ... and current row), not
    exclusive (... and 1 preceding). append_features_sql ASOF-joins this table
    to the target using a STRICT race_date inequality (b.race_date >
    h.race_date), which picks the horse's latest ACTUAL prior race and takes
    its inclusive cumulative -- equivalent to "everything strictly before the
    target's race_date". This is what lets an upcoming target race (whose own
    race_date the horse has never raced on) still resolve: the old exact-date
    equality join required horse_near_miss to carry a row keyed at exactly the
    target's own race_date, which only a COMPLETED race can ever produce (see
    stage_race_history's finish_position is not null filter), so every live
    prediction fell back to NULL. For a historical row (where the target's own
    race_date IS one of the horse's actual race dates), the inclusive
    cumulative at the immediately-preceding actual race date is byte-identical
    to the old exclusive-window value at the target's own race_date, because
    both represent "every race with race_date < target.race_date" and there is
    no other race for this horse strictly between the two dates.
    """
    con.execute(
        """
        create or replace temp table horse_near_miss as
        with flagged as (
          select source, ketto_toroku_bango, race_date,
            case when finish_position = 2 then 1 else 0 end as is_p2,
            case when finish_position = 1 then 1 else 0 end as is_p1,
            case when finish_position = 2 then time_sa else null end as p2_timesa
          from race_history
        )
        select source, ketto_toroku_bango, race_date,
          count(*) over horse_career as past_starts,
          sum(is_p2) over horse_career as past_p2_count,
          sum(is_p1) over horse_career as past_p1_count,
          avg(p2_timesa) over horse_career as past_p2_avg_timesa,
          sum(is_p2) over horse_recent_5 as recent_p2_count_5,
          avg(p2_timesa) over horse_recent_5 as recent_p2_avg_timesa_5
        from flagged
        window
          horse_career as (
            partition by source, ketto_toroku_bango
            order by race_date
            rows between unbounded preceding and current row
          ),
          horse_recent_5 as (
            partition by source, ketto_toroku_bango
            order by race_date
            rows between 4 preceding and current row
          )
        """
    )
    con.execute(
        "create index horse_near_miss_idx on horse_near_miss (source, ketto_toroku_bango, race_date)"
    )


def stage_target_context(
    con: duckdb.DuckDBPyConnection, _input_glob: str, use_target_current: bool
) -> None:
    """Stage the target race's OWN key/context (curr), independent of settlement.

    horse_context's self-join previously drew BOTH sides (curr and past) from
    race_history, which is settled-only (stage_race_history filters
    ``finish_position is not null``). An upcoming target race can never have a
    row there, so the old exact-date join in append_features_sql's ``joined``
    CTE (``hc.race_date = b.race_date``) always missed for a live prediction,
    and the four support-denominator rates/counts silently became join-NULL
    instead of an honest zero-support value.

    target_context resolves curr independently of settlement:
      - focused (serve): ``stage_target_entities`` already materialized the
        base parquet's own rows plus the exact current-row Catalog lookup as
        ``target_current``. Reuse that table directly. The base rows carry
        kaisai_nen/kaisai_tsukihi/keibajo_code/race_bango/kyori/track_code
        (result-independent, always populated -- see
        finish_position_features_duckdb.py::base_features_select_sql), but
        NOT the assigned jockey. ``target_current`` resolved that jockey and
        the pedigree ids once, without a settlement filter.
      - offline (bulk/non-focused): race_history IS the exact current row set
        (already staged by stage_race_history, finished-only by construction)
        -- reused directly, no new PostgreSQL query.
    """
    if use_target_current:
        con.execute(
            """
            create or replace temp table target_context as
            select source, race_date, kaisai_nen, kaisai_tsukihi,
                   keibajo_code, race_bango, ketto_toroku_bango,
                   kyori, track_code, grade_code, kishumei_ryakusho,
                   sire_id, damsire_id
            from target_current
            """
        )
    else:
        con.execute(
            """
            create or replace temp table target_context as
            select
              rh.source, rh.race_date, rh.kaisai_nen, rh.kaisai_tsukihi,
              rh.keibajo_code, rh.race_bango, rh.ketto_toroku_bango,
              rh.kyori, rh.track_code, rh.grade_code,
              nullif(trim(rh.kishumei_ryakusho), '') as kishumei_ryakusho,
              hp.sire_id, hp.damsire_id
            from race_history rh
            left join horse_pedigree hp
              on hp.ketto_toroku_bango = rh.ketto_toroku_bango
            """
        )
    con.execute(
        "create index target_context_idx on target_context "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_distance_bridge(con: duckdb.DuckDBPyConnection) -> None:
    """Materialize the tiny integer-distance domain before large joins."""
    con.execute(
        """
        create or replace temp table target_distance_domain as
        select distinct kyori as target_kyori
        from target_context where kyori is not null
        """
    )
    con.execute(
        """
        create or replace temp table history_distance_domain as
        select distinct kyori as past_kyori
        from race_history where kyori is not null
        """
    )
    con.execute(
        f"""
        create or replace temp table distance_bridge as
        select target.target_kyori, history.past_kyori
        from target_distance_domain target
        cross join history_distance_domain history
        where abs(history.past_kyori - target.target_kyori)
          <= {DISTANCE_TOLERANCE_M}
        """
    )
    con.execute(
        "create unique index distance_bridge_idx on distance_bridge "
        "(target_kyori, past_kyori)"
    )


def stage_horse_context(
    con: duckdb.DuckDBPyConnection, focused_target: bool = True
) -> None:
    """Context-specific (same_keibajo / same_distance / same_track / jockey-horse-pair) 2 着率.

    curr comes from target_context -- the target race's OWN key/context,
    resolvable even for an upcoming race that has never appeared as a
    finished race_history row (see stage_target_context's docstring). past is
    aggregated from race_history with daily cumulative windows and strict
    ASOF inequalities (``curr.race_date > cumulative.race_date``). The venue,
    track-prefix and jockey-pair contexts partition directly by their exact
    keys. Distance expands each history row only to target distances within
    200 metres before its cumulative window. This is equivalent to the old
    target-by-history inequality self-join but avoids materializing every
    horse-career pair during an offline full-history build.

    Zero vs NULL semantics: a curr row present in target_context but with
    zero qualifying past rows is a KNOWN context with zero support -- the
    rate is retained as 0.0. A curr row absent from target_context entirely
    (target-context/join failure) leaves every hc.*_starts column NULL via
    the outer LEFT JOIN in append_features_sql's ``joined`` CTE, which must
    propagate as NULL (unknown), never be coalesced to zero.

    same_track additionally requires BOTH curr and past track_code to be
    non-blank before comparing -- the old ``coalesce(track_code, '')``
    comparison let two blank track_codes ('' = '') count as a false match.
    pair additionally requires a non-blank curr (exact target-row) jockey --
    target_context already resolves blank/missing jockeys to NULL, so
    ``curr.kishumei_ryakusho is not null`` is a sufficient guard here. The
    past side is trimmed inline (``nullif(trim(past.kishumei_ryakusho), '')``)
    to match curr's already-trimmed value -- raw JVD jockey names are
    full/half-width-space-padded, and past comes straight from race_history
    (never trimmed there), so comparing it against curr's trimmed value
    without also trimming past would silently fail to match every padded
    name (curr and past used to both be the SAME untrimmed race_history
    table pre-fix, so raw=raw happened to match; that symmetry broke once
    curr moved to target_context's trimmed value).
    """
    stage_distance_bridge(con)
    venue_join = (
        """asof left join venue_cumulative venue
          on venue.source = curr.source
          and venue.ketto_toroku_bango = curr.ketto_toroku_bango
          and venue.keibajo_code = curr.keibajo_code
          and curr.race_date > venue.race_date"""
        if focused_target
        else """left join venue_shifted venue
          on venue.source = curr.source
          and venue.ketto_toroku_bango = curr.ketto_toroku_bango
          and venue.keibajo_code = curr.keibajo_code
          and venue.target_race_date = curr.race_date"""
    )
    distance_join = (
        """asof left join distance_cumulative distance
          on distance.source = curr.source
          and distance.ketto_toroku_bango = curr.ketto_toroku_bango
          and distance.target_kyori = curr.kyori
          and curr.race_date > distance.race_date"""
        if focused_target
        else """left join distance_shifted distance
          on distance.source = curr.source
          and distance.ketto_toroku_bango = curr.ketto_toroku_bango
          and distance.target_kyori = curr.kyori
          and distance.target_race_date = curr.race_date"""
    )
    track_join = (
        """asof left join track_cumulative track
          on track.source = curr.source
          and track.ketto_toroku_bango = curr.ketto_toroku_bango
          and track.track_prefix = curr.track_prefix
          and curr.race_date > track.race_date"""
        if focused_target
        else """left join track_shifted track
          on track.source = curr.source
          and track.ketto_toroku_bango = curr.ketto_toroku_bango
          and track.track_prefix = curr.track_prefix
          and track.target_race_date = curr.race_date"""
    )
    pair_join = (
        """asof left join pair_cumulative pair
          on pair.source = curr.source
          and pair.ketto_toroku_bango = curr.ketto_toroku_bango
          and pair.kishumei_ryakusho = curr.kishumei_ryakusho
          and curr.race_date > pair.race_date"""
        if focused_target
        else """left join pair_shifted pair
          on pair.source = curr.source
          and pair.ketto_toroku_bango = curr.ketto_toroku_bango
          and pair.kishumei_ryakusho = curr.kishumei_ryakusho
          and pair.target_race_date = curr.race_date"""
    )
    con.execute(
        f"""
        create or replace temp table horse_context as
        with venue_daily as (
          select source, ketto_toroku_bango, race_date, keibajo_code,
            count(*) as day_starts,
            sum(case when finish_position = 2 then 1 else 0 end) as day_p2
          from race_history
          group by source, ketto_toroku_bango, race_date, keibajo_code
        ),
        venue_cumulative as (
          select source, ketto_toroku_bango, race_date, keibajo_code,
            sum(day_starts) over history as cumulative_starts,
            sum(day_p2) over history as cumulative_p2
          from venue_daily
          window history as (
            partition by source, ketto_toroku_bango, keibajo_code
            order by race_date rows between unbounded preceding and current row
          )
        ),
        venue_shifted as (
          select source, ketto_toroku_bango, keibajo_code,
            lead(race_date) over history as target_race_date,
            cumulative_starts, cumulative_p2
          from venue_cumulative
          window history as (
            partition by source, ketto_toroku_bango, keibajo_code
            order by race_date
          )
        ),
        distance_daily as (
          select past.source, past.ketto_toroku_bango, past.race_date,
            bridge.target_kyori,
            count(*) as day_starts,
            sum(case when past.finish_position = 2 then 1 else 0 end) as day_p2
          from race_history past
          inner join distance_bridge bridge on bridge.past_kyori = past.kyori
          group by past.source, past.ketto_toroku_bango, past.race_date,
            bridge.target_kyori
        ),
        distance_cumulative as (
          select source, ketto_toroku_bango, race_date, target_kyori,
            sum(day_starts) over history as cumulative_starts,
            sum(day_p2) over history as cumulative_p2
          from distance_daily
          window history as (
            partition by source, ketto_toroku_bango, target_kyori
            order by race_date rows between unbounded preceding and current row
          )
        ),
        distance_shifted as (
          select source, ketto_toroku_bango, target_kyori,
            lead(race_date) over history as target_race_date,
            cumulative_starts, cumulative_p2
          from distance_cumulative
          window history as (
            partition by source, ketto_toroku_bango, target_kyori
            order by race_date
          )
        ),
        track_daily as (
          select source, ketto_toroku_bango, race_date,
            left(trim(track_code), 1) as track_prefix,
            count(*) as day_starts,
            sum(case when finish_position = 2 then 1 else 0 end) as day_p2
          from race_history
          where nullif(trim(track_code), '') is not null
          group by source, ketto_toroku_bango, race_date,
            left(trim(track_code), 1)
        ),
        track_cumulative as (
          select source, ketto_toroku_bango, race_date, track_prefix,
            sum(day_starts) over history as cumulative_starts,
            sum(day_p2) over history as cumulative_p2
          from track_daily
          window history as (
            partition by source, ketto_toroku_bango, track_prefix
            order by race_date rows between unbounded preceding and current row
          )
        ),
        track_shifted as (
          select source, ketto_toroku_bango, track_prefix,
            lead(race_date) over history as target_race_date,
            cumulative_starts, cumulative_p2
          from track_cumulative
          window history as (
            partition by source, ketto_toroku_bango, track_prefix
            order by race_date
          )
        ),
        pair_daily as (
          select source, ketto_toroku_bango, race_date,
            nullif(trim(kishumei_ryakusho), '') as kishumei_ryakusho,
            count(*) as day_starts,
            sum(case when finish_position = 2 then 1 else 0 end) as day_p2
          from race_history
          where nullif(trim(kishumei_ryakusho), '') is not null
          group by source, ketto_toroku_bango, race_date,
            nullif(trim(kishumei_ryakusho), '')
        ),
        pair_cumulative as (
          select source, ketto_toroku_bango, race_date, kishumei_ryakusho,
            sum(day_starts) over history as cumulative_starts,
            sum(day_p2) over history as cumulative_p2
          from pair_daily
          window history as (
            partition by source, ketto_toroku_bango, kishumei_ryakusho
            order by race_date rows between unbounded preceding and current row
          )
        ),
        pair_shifted as (
          select source, ketto_toroku_bango, kishumei_ryakusho,
            lead(race_date) over history as target_race_date,
            cumulative_starts, cumulative_p2
          from pair_cumulative
          window history as (
            partition by source, ketto_toroku_bango, kishumei_ryakusho
            order by race_date
          )
        ),
        current_context as (
          select *,
            case when nullif(trim(track_code), '') is not null
              then left(trim(track_code), 1) else null end as track_prefix
          from target_context
        )
        select
          curr.source, curr.kaisai_nen, curr.kaisai_tsukihi, curr.keibajo_code, curr.race_bango,
          curr.ketto_toroku_bango,
          coalesce(venue.cumulative_starts, 0) as same_keibajo_starts,
          coalesce(venue.cumulative_p2, 0) as same_keibajo_p2,
          coalesce(distance.cumulative_starts, 0) as same_distance_starts,
          coalesce(distance.cumulative_p2, 0) as same_distance_p2,
          coalesce(track.cumulative_starts, 0) as same_track_starts,
          coalesce(track.cumulative_p2, 0) as same_track_p2,
          coalesce(pair.cumulative_starts, 0) as pair_starts,
          coalesce(pair.cumulative_p2, 0) as pair_p2
        from current_context curr
        {venue_join}
        {distance_join}
        {track_join}
        {pair_join}
        """
    )
    con.execute(
        "create index horse_context_idx on horse_context "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_pedigree_cumulatives(con: duckdb.DuckDBPyConnection) -> None:
    """Pre-aggregate + window cumulative for sire / damsire stats by kyori and grade.

    Cardinality control: we pre-aggregate to one row per (parent_id, race_date, kyori|grade)
    and then compute a cumulative window. ASOF joins later reuse these compact tables
    without exploding the row count.
    """
    con.execute(
        """
        create or replace temp table sire_daily_kyori as
        select p.sire_id, h.race_date, h.kyori,
          count(*) as day_starts,
          sum(case when h.finish_position = 2 then 1 else 0 end) as day_p2
        from race_history h
        join horse_pedigree p using (ketto_toroku_bango)
        where p.sire_id is not null and h.kyori is not null
        group by p.sire_id, h.race_date, h.kyori
        """
    )
    con.execute(
        """
        create or replace temp table sire_kyori_cumul as
        select sire_id, kyori, race_date,
          sum(day_starts) over w as cum_starts,
          sum(day_p2) over w as cum_p2
        from sire_daily_kyori
        window w as (
          partition by sire_id, kyori order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        """
        create or replace temp table sire_daily_grade as
        select p.sire_id, h.race_date, coalesce(h.grade_code, '') as grade_code,
          count(*) as day_starts,
          sum(case when h.finish_position = 2 then 1 else 0 end) as day_p2
        from race_history h
        join horse_pedigree p using (ketto_toroku_bango)
        where p.sire_id is not null
        group by p.sire_id, h.race_date, coalesce(h.grade_code, '')
        """
    )
    con.execute(
        """
        create or replace temp table sire_grade_cumul as
        select sire_id, grade_code, race_date,
          sum(day_starts) over w as cum_starts,
          sum(day_p2) over w as cum_p2
        from sire_daily_grade
        window w as (
          partition by sire_id, grade_code order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        """
        create or replace temp table damsire_daily_kyori as
        select p.damsire_id, h.race_date, h.kyori,
          count(*) as day_starts,
          sum(case when h.finish_position = 2 then 1 else 0 end) as day_p2
        from race_history h
        join horse_pedigree p using (ketto_toroku_bango)
        where p.damsire_id is not null and h.kyori is not null
        group by p.damsire_id, h.race_date, h.kyori
        """
    )
    con.execute(
        """
        create or replace temp table damsire_kyori_cumul as
        select damsire_id, kyori, race_date,
          sum(day_starts) over w as cum_starts,
          sum(day_p2) over w as cum_p2
        from damsire_daily_kyori
        window w as (
          partition by damsire_id, kyori order by race_date
          rows between unbounded preceding and current row
        )
        """
    )


def _stage_horse_pedigree_context_bulk(con: duckdb.DuckDBPyConnection) -> None:
    """Resolve strict prior-date pedigree cumulatives without inequality joins.

    A target date is not necessarily a date on which another offspring of the
    same sire/damsire ran, so a simple ``lead(race_date)`` shift is not exact.
    Instead, target rows are inserted before same-day history rows on a shared
    timeline. ``last_value(... ignore nulls)`` then carries only an earlier
    day's cumulative value into each target row.
    """
    con.execute(
        f"""
        create or replace temp table sire_distance_stats as
        with target_expanded as (
          select t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code,
            t.race_bango, t.ketto_toroku_bango, t.sire_id, sk.kyori as past_kyori
          from pedigree_target t
          join distance_bridge bridge on bridge.target_kyori = t.kyori
          join (select distinct sire_id, kyori from sire_kyori_cumul) sk
            on sk.sire_id = t.sire_id and sk.kyori = bridge.past_kyori
          where t.sire_id is not null and t.kyori is not null
        ),
        timeline as (
          select source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
            race_bango, ketto_toroku_bango, sire_id, past_kyori,
            0 as event_order, cast(null as hugeint) as cum_starts,
            cast(null as hugeint) as cum_p2, true as is_target
          from target_expanded
          union all
          select cast(null as varchar), race_date, cast(null as varchar),
            cast(null as varchar), cast(null as varchar), cast(null as varchar),
            cast(null as varchar), sire_id, kyori, 1, cum_starts, cum_p2, false
          from sire_kyori_cumul
        ),
        carried as (
          select *,
            last_value(cum_starts ignore nulls) over prior as prior_starts,
            last_value(cum_p2 ignore nulls) over prior as prior_p2
          from timeline
          window prior as (
            partition by sire_id, past_kyori order by race_date, event_order
            rows between unbounded preceding and current row
          )
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          sum(coalesce(prior_starts, 0)) as sire_distance_starts,
          sum(coalesce(prior_p2, 0)) as sire_distance_p2
        from carried where is_target
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango
        """
    )
    con.execute(
        """
        create or replace temp table sire_grade_stats as
        with target_rows as (
          select source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
            race_bango, ketto_toroku_bango, sire_id, grade_code
          from pedigree_target where sire_id is not null
        ),
        timeline as (
          select source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
            race_bango, ketto_toroku_bango, sire_id, grade_code,
            0 as event_order, cast(null as hugeint) as cum_starts,
            cast(null as hugeint) as cum_p2, true as is_target
          from target_rows
          union all
          select cast(null as varchar), race_date, cast(null as varchar),
            cast(null as varchar), cast(null as varchar), cast(null as varchar),
            cast(null as varchar), sire_id, grade_code, 1, cum_starts, cum_p2, false
          from sire_grade_cumul
        ),
        carried as (
          select *,
            last_value(cum_starts ignore nulls) over prior as prior_starts,
            last_value(cum_p2 ignore nulls) over prior as prior_p2
          from timeline
          window prior as (
            partition by sire_id, grade_code order by race_date, event_order
            rows between unbounded preceding and current row
          )
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          coalesce(prior_starts, 0) as sire_grade_starts,
          coalesce(prior_p2, 0) as sire_grade_p2
        from carried where is_target
        """
    )
    con.execute(
        f"""
        create or replace temp table damsire_distance_stats as
        with target_expanded as (
          select t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code,
            t.race_bango, t.ketto_toroku_bango, t.damsire_id, dk.kyori as past_kyori
          from pedigree_target t
          join distance_bridge bridge on bridge.target_kyori = t.kyori
          join (select distinct damsire_id, kyori from damsire_kyori_cumul) dk
            on dk.damsire_id = t.damsire_id and dk.kyori = bridge.past_kyori
          where t.damsire_id is not null and t.kyori is not null
        ),
        timeline as (
          select source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
            race_bango, ketto_toroku_bango, damsire_id, past_kyori,
            0 as event_order, cast(null as hugeint) as cum_starts,
            cast(null as hugeint) as cum_p2, true as is_target
          from target_expanded
          union all
          select cast(null as varchar), race_date, cast(null as varchar),
            cast(null as varchar), cast(null as varchar), cast(null as varchar),
            cast(null as varchar), damsire_id, kyori, 1, cum_starts, cum_p2, false
          from damsire_kyori_cumul
        ),
        carried as (
          select *,
            last_value(cum_starts ignore nulls) over prior as prior_starts,
            last_value(cum_p2 ignore nulls) over prior as prior_p2
          from timeline
          window prior as (
            partition by damsire_id, past_kyori order by race_date, event_order
            rows between unbounded preceding and current row
          )
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          sum(coalesce(prior_starts, 0)) as damsire_distance_starts,
          sum(coalesce(prior_p2, 0)) as damsire_distance_p2
        from carried where is_target
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango
        """
    )


def stage_horse_pedigree_context(
    con: duckdb.DuckDBPyConnection, focused_target: bool = True
) -> None:
    """ASOF-join target × cumulative pedigree stats.

    pedigree_target previously sourced its "curr" rows from race_history
    (settled-only), the same defect family as the old horse_context: an
    upcoming target race can never have a row there, so it silently fell out
    of sire_distance_stats/sire_grade_stats/damsire_distance_stats entirely,
    and the old exact-date-equality join in append_features_sql's ``joined``
    (``hp.race_date = b.race_date``) never had a chance to match anyway.
    pedigree_target now sources from target_context -- the same
    settlement-independent curr resolution horse_context uses -- so this is
    a no-op for offline/bulk builds (target_context's offline branch is
    literally race_history + horse_pedigree, byte-identical to the old
    source) and a real fix only for the focused/serve path.

    Distance tolerance (±200m) is implemented by expanding each target row to all
    matching past kyori values (typically 1-5 discrete JRA distances within ±200m),
    then ASOF-joining the cumulative row per (parent_id, exact past_kyori) with
    target.race_date as the strict-greater inequality (unchanged from before --
    only the curr-row source changed).

    Grade match is exact (single bucket per target), no expansion needed.

    Output keyed on the FULL race key (source, kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, horse) rather than race_date alone, matching
    horse_context's full-race-key-isolation fix and the corresponding join
    update in append_features_sql's ``joined`` CTE.
    """
    con.execute(
        """
        create or replace temp table pedigree_target as
        select
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, kyori, coalesce(grade_code, '') as grade_code,
          sire_id, damsire_id
        from target_context
        """
    )
    if not focused_target:
        _stage_horse_pedigree_context_bulk(con)
        con.execute(
            """
            create or replace temp table horse_pedigree_context as
            select
              coalesce(sd.source, sg.source, dd.source) as source,
              coalesce(sd.kaisai_nen, sg.kaisai_nen, dd.kaisai_nen) as kaisai_nen,
              coalesce(sd.kaisai_tsukihi, sg.kaisai_tsukihi, dd.kaisai_tsukihi) as kaisai_tsukihi,
              coalesce(sd.keibajo_code, sg.keibajo_code, dd.keibajo_code) as keibajo_code,
              coalesce(sd.race_bango, sg.race_bango, dd.race_bango) as race_bango,
              coalesce(sd.ketto_toroku_bango, sg.ketto_toroku_bango, dd.ketto_toroku_bango) as ketto_toroku_bango,
              sd.sire_distance_starts, sd.sire_distance_p2,
              sg.sire_grade_starts, sg.sire_grade_p2,
              dd.damsire_distance_starts, dd.damsire_distance_p2
            from sire_distance_stats sd
            full outer join sire_grade_stats sg
              on sd.source = sg.source and sd.kaisai_nen = sg.kaisai_nen
              and sd.kaisai_tsukihi = sg.kaisai_tsukihi
              and sd.keibajo_code = sg.keibajo_code and sd.race_bango = sg.race_bango
              and sd.ketto_toroku_bango = sg.ketto_toroku_bango
            full outer join damsire_distance_stats dd
              on coalesce(sd.source, sg.source) = dd.source
              and coalesce(sd.kaisai_nen, sg.kaisai_nen) = dd.kaisai_nen
              and coalesce(sd.kaisai_tsukihi, sg.kaisai_tsukihi) = dd.kaisai_tsukihi
              and coalesce(sd.keibajo_code, sg.keibajo_code) = dd.keibajo_code
              and coalesce(sd.race_bango, sg.race_bango) = dd.race_bango
              and coalesce(sd.ketto_toroku_bango, sg.ketto_toroku_bango) = dd.ketto_toroku_bango
            """
        )
        con.execute(
            "create index horse_pedigree_context_idx on horse_pedigree_context "
            "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
        )
        return
    con.execute(
        f"""
        create or replace temp table sire_distance_stats as
        with target_expanded as (
          select t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code,
            t.race_bango, t.ketto_toroku_bango, t.sire_id, t.kyori as t_kyori,
            sk.kyori as past_kyori
          from pedigree_target t
          join distance_bridge bridge on bridge.target_kyori = t.kyori
          join (select distinct sire_id, kyori from sire_kyori_cumul) sk
            on sk.sire_id = t.sire_id
            and sk.kyori = bridge.past_kyori
          where t.sire_id is not null and t.kyori is not null
        )
        select te.source, te.kaisai_nen, te.kaisai_tsukihi, te.keibajo_code, te.race_bango,
          te.ketto_toroku_bango,
          sum(coalesce(s.cum_starts, 0)) as sire_distance_starts,
          sum(coalesce(s.cum_p2, 0)) as sire_distance_p2
        from target_expanded te
        asof left join sire_kyori_cumul s
          on te.sire_id = s.sire_id
          and te.past_kyori = s.kyori
          and te.race_date > s.race_date
        group by te.source, te.kaisai_nen, te.kaisai_tsukihi, te.keibajo_code, te.race_bango,
          te.ketto_toroku_bango
        """
    )
    con.execute(
        """
        create or replace temp table sire_grade_stats as
        select
          t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
          t.ketto_toroku_bango,
          coalesce(s.cum_starts, 0) as sire_grade_starts,
          coalesce(s.cum_p2, 0) as sire_grade_p2
        from pedigree_target t
        asof left join sire_grade_cumul s
          on t.sire_id = s.sire_id
          and t.grade_code = s.grade_code
          and t.race_date > s.race_date
        where t.sire_id is not null
        """
    )
    con.execute(
        f"""
        create or replace temp table damsire_distance_stats as
        with target_expanded as (
          select t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code,
            t.race_bango, t.ketto_toroku_bango, t.damsire_id, t.kyori as t_kyori,
            dk.kyori as past_kyori
          from pedigree_target t
          join distance_bridge bridge on bridge.target_kyori = t.kyori
          join (select distinct damsire_id, kyori from damsire_kyori_cumul) dk
            on dk.damsire_id = t.damsire_id
            and dk.kyori = bridge.past_kyori
          where t.damsire_id is not null and t.kyori is not null
        )
        select te.source, te.kaisai_nen, te.kaisai_tsukihi, te.keibajo_code, te.race_bango,
          te.ketto_toroku_bango,
          sum(coalesce(d.cum_starts, 0)) as damsire_distance_starts,
          sum(coalesce(d.cum_p2, 0)) as damsire_distance_p2
        from target_expanded te
        asof left join damsire_kyori_cumul d
          on te.damsire_id = d.damsire_id
          and te.past_kyori = d.kyori
          and te.race_date > d.race_date
        group by te.source, te.kaisai_nen, te.kaisai_tsukihi, te.keibajo_code, te.race_bango,
          te.ketto_toroku_bango
        """
    )
    con.execute(
        """
        create or replace temp table horse_pedigree_context as
        select
          coalesce(sd.source, sg.source, dd.source) as source,
          coalesce(sd.kaisai_nen, sg.kaisai_nen, dd.kaisai_nen) as kaisai_nen,
          coalesce(sd.kaisai_tsukihi, sg.kaisai_tsukihi, dd.kaisai_tsukihi) as kaisai_tsukihi,
          coalesce(sd.keibajo_code, sg.keibajo_code, dd.keibajo_code) as keibajo_code,
          coalesce(sd.race_bango, sg.race_bango, dd.race_bango) as race_bango,
          coalesce(sd.ketto_toroku_bango, sg.ketto_toroku_bango, dd.ketto_toroku_bango) as ketto_toroku_bango,
          sd.sire_distance_starts, sd.sire_distance_p2,
          sg.sire_grade_starts, sg.sire_grade_p2,
          dd.damsire_distance_starts, dd.damsire_distance_p2
        from sire_distance_stats sd
        full outer join sire_grade_stats sg
          on sd.source = sg.source
          and sd.kaisai_nen = sg.kaisai_nen
          and sd.kaisai_tsukihi = sg.kaisai_tsukihi
          and sd.keibajo_code = sg.keibajo_code
          and sd.race_bango = sg.race_bango
          and sd.ketto_toroku_bango = sg.ketto_toroku_bango
        full outer join damsire_distance_stats dd
          on coalesce(sd.source, sg.source) = dd.source
          and coalesce(sd.kaisai_nen, sg.kaisai_nen) = dd.kaisai_nen
          and coalesce(sd.kaisai_tsukihi, sg.kaisai_tsukihi) = dd.kaisai_tsukihi
          and coalesce(sd.keibajo_code, sg.keibajo_code) = dd.keibajo_code
          and coalesce(sd.race_bango, sg.race_bango) = dd.race_bango
          and coalesce(sd.ketto_toroku_bango, sg.ketto_toroku_bango) = dd.ketto_toroku_bango
        """
    )
    con.execute(
        "create index horse_pedigree_context_idx on horse_pedigree_context "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_horse_distance_grade(
    con: duckdb.DuckDBPyConnection, focused_target: bool = True
) -> None:
    """この馬の (kyori, grade) ペアの過去累積を pre-aggregate + ASOF で計算。

    horse_daily_kyori_grade / horse_kyori_grade_cumul (the "past" cumulative
    side) are unchanged -- they correctly aggregate only settled race_history
    rows. The ``target`` CTE (the "curr" side) previously also sourced from
    race_history, the same settled-only defect family as the old
    horse_context: an upcoming target race silently had no row there, and the
    old exact-date-equality join in append_features_sql's ``joined``
    (``hdg.race_date = b.race_date``) never had a chance to match anyway.
    ``target`` now sources from target_context -- the same
    settlement-independent curr resolution horse_context uses -- a no-op for
    offline/bulk builds and a real fix only for the focused/serve path.

    Distance tolerance ±200m は target row を kyori 候補で expand して ASOF join
    (unchanged -- only the curr-row source changed).

    Output keyed on the FULL race key (source, kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, horse) rather than race_date alone, matching
    horse_context's full-race-key-isolation fix.
    """
    con.execute(
        """
        create or replace temp table horse_daily_kyori_grade as
        select
          source, ketto_toroku_bango, kyori, coalesce(grade_code, '') as grade_code, race_date,
          count(*) as day_starts,
          sum(case when finish_position = 2 then 1 else 0 end) as day_p2
        from race_history
        where kyori is not null
        group by source, ketto_toroku_bango, kyori, coalesce(grade_code, ''), race_date
        """
    )
    con.execute(
        """
        create or replace temp table horse_kyori_grade_cumul as
        select source, ketto_toroku_bango, kyori, grade_code, race_date,
          sum(day_starts) over w as cum_starts,
          sum(day_p2) over w as cum_p2
        from horse_daily_kyori_grade
        window w as (
          partition by source, ketto_toroku_bango, kyori, grade_code order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    prior_cte = (
        """"""
        if focused_target
        else """,
        horse_kyori_grade_prior as (
          select source, ketto_toroku_bango, kyori, grade_code,
            race_date as target_race_date,
            lag(cum_starts) over history as prior_starts,
            lag(cum_p2) over history as prior_p2
          from horse_kyori_grade_cumul
          window history as (
            partition by source, ketto_toroku_bango, kyori, grade_code
            order by race_date
          )
        )"""
    )
    history_join = (
        """asof left join horse_kyori_grade_cumul h
          on te.source = h.source
          and te.ketto_toroku_bango = h.ketto_toroku_bango
          and te.grade_code = h.grade_code
          and te.past_kyori = h.kyori
          and te.race_date > h.race_date"""
        if focused_target
        else """left join horse_kyori_grade_prior h
          on te.source = h.source
          and te.ketto_toroku_bango = h.ketto_toroku_bango
          and te.grade_code = h.grade_code
          and te.past_kyori = h.kyori
          and te.race_date = h.target_race_date"""
    )
    starts_column = "h.cum_starts" if focused_target else "h.prior_starts"
    p2_column = "h.cum_p2" if focused_target else "h.prior_p2"
    con.execute(
        f"""
        create or replace temp table horse_distance_grade as
        with target as (
          select source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            ketto_toroku_bango, kyori, coalesce(grade_code, '') as grade_code
          from target_context
          where kyori is not null
        ),
        target_expanded as (
          select t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code,
            t.race_bango, t.ketto_toroku_bango, t.kyori as t_kyori,
            t.grade_code, hk.kyori as past_kyori
          from target t
          join distance_bridge bridge on bridge.target_kyori = t.kyori
          join (select distinct source, ketto_toroku_bango, kyori, grade_code
                from horse_kyori_grade_cumul) hk
            on hk.source = t.source
            and hk.ketto_toroku_bango = t.ketto_toroku_bango
            and hk.grade_code = t.grade_code
            and hk.kyori = bridge.past_kyori
        ){prior_cte}
        select te.source, te.kaisai_nen, te.kaisai_tsukihi, te.keibajo_code, te.race_bango,
          te.ketto_toroku_bango,
          sum(coalesce({starts_column}, 0)) as dg_starts,
          sum(coalesce({p2_column}, 0)) as dg_p2
        from target_expanded te
        {history_join}
        group by te.source, te.kaisai_nen, te.kaisai_tsukihi, te.keibajo_code, te.race_bango,
          te.ketto_toroku_bango
        """
    )
    con.execute(
        "create index horse_distance_grade_idx on horse_distance_grade "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_jockey_near_miss(con: duckdb.DuckDBPyConnection) -> None:
    """騎手ごとの 2 着率を race_history (PG-staged) から計算。
    同一日に同騎手が複数騎乗する → date 単位 deduplicate して 1 行/日とする。

    Window is INCLUSIVE of the current row (see stage_horse_near_miss's
    docstring for why -- append_features_sql ASOF-joins this table with a
    strict race_date inequality so an upcoming target race resolves against
    the jockey's latest actual prior ride).
    """
    con.execute(
        """
        create or replace temp table jockey_daily as
        select source, kishumei_ryakusho, race_date,
          count(*) as rides_on_day,
          sum(case when finish_position = 2 then 1 else 0 end) as p2_on_day
        from race_history
        where kishumei_ryakusho is not null
        group by source, kishumei_ryakusho, race_date
        """
    )
    con.execute(
        """
        create or replace temp table jockey_near_miss as
        select source, kishumei_ryakusho, race_date,
          sum(rides_on_day) over jockey_career as past_rides,
          sum(p2_on_day) over jockey_career as past_jockey_p2_count
        from jockey_daily
        window jockey_career as (
          partition by source, kishumei_ryakusho
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index jockey_near_miss_idx on jockey_near_miss (source, kishumei_ryakusho, race_date)"
    )


def stage_bulk_prior_date_lookups(con: duckdb.DuckDBPyConnection) -> None:
    """Shift inclusive history stats to the next observed date for bulk joins.

    Offline targets are settled historical races, so each target date already
    exists in ``race_history``.  Mapping the end-of-day cumulative state to the
    next distinct date is exactly the strict prior-date ASOF result, but turns
    the multi-million-row final join into an equality join.  Live/focused
    inference must keep ASOF because an upcoming target date is not present in
    history and therefore has no shifted lookup row.
    """
    con.execute(
        """
        create or replace temp table horse_near_miss_target as
        with daily_end as (
          select source, ketto_toroku_bango, race_date,
            max(past_starts) as past_starts,
            first(past_p2_count order by past_starts desc) as past_p2_count,
            first(past_p1_count order by past_starts desc) as past_p1_count,
            first(past_p2_avg_timesa order by past_starts desc) as past_p2_avg_timesa,
            first(recent_p2_count_5 order by past_starts desc) as recent_p2_count_5,
            first(recent_p2_avg_timesa_5 order by past_starts desc)
              as recent_p2_avg_timesa_5
          from horse_near_miss
          group by source, ketto_toroku_bango, race_date
        ), shifted as (
          select source, ketto_toroku_bango,
            lead(race_date) over history as target_race_date,
            past_starts, past_p2_count, past_p1_count, past_p2_avg_timesa,
            recent_p2_count_5, recent_p2_avg_timesa_5
          from daily_end
          window history as (
            partition by source, ketto_toroku_bango order by race_date
          )
        )
        select source, ketto_toroku_bango, target_race_date as race_date,
          past_starts, past_p2_count, past_p1_count, past_p2_avg_timesa,
          recent_p2_count_5, recent_p2_avg_timesa_5
        from shifted where target_race_date is not null
        """
    )
    con.execute(
        "create index horse_near_miss_target_idx on horse_near_miss_target "
        "(source, ketto_toroku_bango, race_date)"
    )
    con.execute(
        """
        create or replace temp table jockey_near_miss_target as
        with shifted as (
          select source, kishumei_ryakusho,
            lead(race_date) over jockey_history as target_race_date,
            past_rides, past_jockey_p2_count
          from jockey_near_miss
          window jockey_history as (
            partition by source, kishumei_ryakusho order by race_date
          )
        )
        select source, kishumei_ryakusho, target_race_date as race_date,
          past_rides, past_jockey_p2_count
        from shifted where target_race_date is not null
        """
    )
    con.execute(
        "create index jockey_near_miss_target_idx on jockey_near_miss_target "
        "(source, kishumei_ryakusho, race_date)"
    )


def append_features_sql(
    input_glob: str,
    focused_target: bool = True,
    target_from_date: str | None = None,
    target_to_date: str | None = None,
    use_target_current: bool = False,
) -> str:
    horse_history_join = (
        """asof left join horse_near_miss h
        on h.source = b.source
        and h.ketto_toroku_bango = b.ketto_toroku_bango
        and nullif(trim(b.ketto_toroku_bango), '0000000000') is not null
        and b.race_date > h.race_date"""
        if focused_target
        else """left join horse_near_miss_target h
        on h.source = b.source
        and h.ketto_toroku_bango = b.ketto_toroku_bango
        and nullif(trim(b.ketto_toroku_bango), '0000000000') is not null
        and b.race_date = h.race_date"""
    )
    jockey_history_join = (
        """asof left join jockey_near_miss j
        on j.source = b.source
        and j.kishumei_ryakusho = b.kishumei_ryakusho
        and b.race_date > j.race_date"""
        if focused_target
        else """left join jockey_near_miss_target j
        on j.source = b.source
        and j.kishumei_ryakusho = b.kishumei_ryakusho
        and b.race_date = j.race_date"""
    )
    target_filter = target_date_filter_sql(
        "b", target_from_date, target_to_date
    )
    current_meta_table = "target_current" if use_target_current else "race_history"
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true) b
      where {target_filter}
    ),
    base_with_meta as (
      select b.*, rh.kishumei_ryakusho, rh.tansho_ninkijun, rh.shusso_tosu
      from base b
      left join {current_meta_table} rh
        on rh.source = b.source
        and rh.kaisai_nen = b.kaisai_nen
        and rh.kaisai_tsukihi = b.kaisai_tsukihi
        and rh.keibajo_code = b.keibajo_code
        and rh.race_bango = b.race_bango
        and rh.ketto_toroku_bango = b.ketto_toroku_bango
    ),
    fav_ranked as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        tansho_odds,
        row_number() over (
          partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
          order by tansho_ninkijun asc nulls last
        ) as ninki_rank
      from base
      where tansho_odds is not null and tansho_ninkijun is not null
    ),
    fav_pivoted as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        max(case when ninki_rank = 1 then tansho_odds end) as odds_rank1,
        max(case when ninki_rank = 2 then tansho_odds end) as odds_rank2
      from fav_ranked group by 1,2,3,4,5
    ),
    race_favorite_dominance as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        odds_rank1 / nullif(odds_rank2, 0) as field_dominant_favorite_indicator
      from fav_pivoted
    ),
    joined as (
      select
        b.* exclude (kishumei_ryakusho, tansho_ninkijun, shusso_tosu),
        -- Re-emit a canonical all-NULL ``shusso_tosu`` alongside the rh-join
        -- ``shusso_tosu_1`` that survives the EXCLUDE above. The base parquet
        -- carries a populated ``shusso_tosu``, but the rh re-join (line above)
        -- collides on the name so DuckDB renames the rh copy to
        -- ``shusso_tosu_1`` and the EXCLUDE then drops the populated base copy.
        -- The v8 NAR models were trained on a parquet whose ``shusso_tosu``
        -- column was constant-NULL (BIGINT) at feature index 2, with the real
        -- signal living in ``shusso_tosu_1`` — and the CatBoost split on index 2
        -- learned ``nan_value_treatment=AsFalse`` against that all-NULL column.
        -- Without this line the inference parquet has no ``shusso_tosu`` at all,
        -- so every per-class NAR ensemble member hits a one-column coverage gap
        -- and falls back to the iter12 baseline. Emitting it as a NULL BIGINT
        -- reproduces the trained distribution exactly (the model keeps taking
        -- the AsFalse branch) and is a no-op for JRA models, which reference
        -- only ``shusso_tosu_1``.
        cast(null as bigint) as shusso_tosu,
        case when h.past_starts > 0
             then h.past_p2_count::double / h.past_starts
             else null end as career_place2_rate,
        case when h.past_starts > 0 and h.past_p1_count > 0
             then (h.past_p2_count::double / h.past_starts)
                / greatest(h.past_p1_count::double / h.past_starts, 0.01)
             else null end as career_place2_to_win_ratio,
        h.past_p2_avg_timesa as career_avg_2nd_margin_decisec,
        h.recent_p2_count_5 as recent_place2_count_5,
        h.recent_p2_avg_timesa_5 as recent_2nd_margin_avg_5,
        case when j.past_rides > 0
             then j.past_jockey_p2_count::double / j.past_rides
             else null end as jockey_career_place2_rate,
        f.field_dominant_favorite_indicator,
        b.tansho_ninkijun::double / nullif(b.shusso_tosu, 0)
          as horse_popularity_vs_field,
        case when hc.same_keibajo_starts is null then null
             when hc.same_keibajo_starts = 0 then 0.0
             else hc.same_keibajo_p2::double / hc.same_keibajo_starts
             end as same_keibajo_place2_rate,
        case when hc.same_keibajo_starts is null then null
             else ln(1 + hc.same_keibajo_starts)
             end as log1p_same_keibajo_starts,
        case when hc.same_distance_starts is null then null
             when hc.same_distance_starts = 0 then 0.0
             else hc.same_distance_p2::double / hc.same_distance_starts
             end as same_distance_place2_rate,
        case when hc.same_distance_starts is null then null
             else ln(1 + hc.same_distance_starts)
             end as log1p_same_distance_starts,
        case when hc.same_track_starts is null then null
             when hc.same_track_starts = 0 then 0.0
             else hc.same_track_p2::double / hc.same_track_starts
             end as same_track_place2_rate,
        case when hc.same_track_starts is null then null
             else ln(1 + hc.same_track_starts)
             end as log1p_same_track_starts,
        case when hc.pair_starts is null then null
             when hc.pair_starts = 0 then 0.0
             else hc.pair_p2::double / hc.pair_starts
             end as jockey_horse_pair_place2_rate,
        case when hc.pair_starts is null then null
             else ln(1 + hc.pair_starts)
             end as log1p_pair_starts,
        case when hp.sire_distance_starts > 0
             then hp.sire_distance_p2::double / hp.sire_distance_starts
             else null end as sire_distance_place2_rate,
        case when hp.sire_grade_starts > 0
             then hp.sire_grade_p2::double / hp.sire_grade_starts
             else null end as sire_grade_place2_rate,
        case when hp.damsire_distance_starts > 0
             then hp.damsire_distance_p2::double / hp.damsire_distance_starts
             else null end as damsire_distance_place2_rate,
        case when hdg.dg_starts > 0
             then hdg.dg_p2::double / hdg.dg_starts
             else null end as horse_distance_grade_place2_rate
      from base_with_meta b
      {horse_history_join}
      left join horse_context hc
        on hc.source = b.source
        and hc.kaisai_nen = b.kaisai_nen
        and hc.kaisai_tsukihi = b.kaisai_tsukihi
        and hc.keibajo_code = b.keibajo_code
        and hc.race_bango = b.race_bango
        and hc.ketto_toroku_bango = b.ketto_toroku_bango
        and nullif(trim(b.ketto_toroku_bango), '{UNKNOWN_HORSE_REGISTRATION}') is not null
      left join horse_pedigree_context hp
        on hp.source = b.source
        and hp.kaisai_nen = b.kaisai_nen
        and hp.kaisai_tsukihi = b.kaisai_tsukihi
        and hp.keibajo_code = b.keibajo_code
        and hp.race_bango = b.race_bango
        and hp.ketto_toroku_bango = b.ketto_toroku_bango
        and nullif(trim(b.ketto_toroku_bango), '{UNKNOWN_HORSE_REGISTRATION}') is not null
      left join horse_distance_grade hdg
        on hdg.source = b.source
        and hdg.kaisai_nen = b.kaisai_nen
        and hdg.kaisai_tsukihi = b.kaisai_tsukihi
        and hdg.keibajo_code = b.keibajo_code
        and hdg.race_bango = b.race_bango
        and hdg.ketto_toroku_bango = b.ketto_toroku_bango
        and nullif(trim(b.ketto_toroku_bango), '{UNKNOWN_HORSE_REGISTRATION}') is not null
      {jockey_history_join}
      left join race_favorite_dominance f
        on f.source = b.source
        and f.kaisai_nen = b.kaisai_nen
        and f.kaisai_tsukihi = b.kaisai_tsukihi
        and f.keibajo_code = b.keibajo_code
        and f.race_bango = b.race_bango
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
    from_date = validate_yyyymmdd(args.from_date, "--from-date")
    to_date = validate_yyyymmdd(args.to_date, "--to-date")
    if from_date > to_date:
        raise ValueError("--from-date must be on or before --to-date")
    target_bounds = (args.target_from_date, args.target_to_date)
    if (target_bounds[0] is None) != (target_bounds[1] is None):
        raise ValueError(
            "--target-from-date and --target-to-date must be provided together"
        )
    target_from_date = (
        validate_yyyymmdd(target_bounds[0], "--target-from-date")
        if target_bounds[0] is not None
        else None
    )
    target_to_date = (
        validate_yyyymmdd(target_bounds[1], "--target-to-date")
        if target_bounds[1] is not None
        else None
    )
    if (
        target_from_date is not None
        and target_to_date is not None
        and target_from_date > target_to_date
    ):
        raise ValueError("--target-from-date must be on or before --target-to-date")
    history_to_date = target_to_date or to_date
    if history_to_date > to_date:
        raise ValueError("--target-to-date must be on or before --to-date")
    focused_target = args.target_race is not None
    require_bounded_bulk(focused_target, target_from_date, target_to_date)
    bounded_bulk = not focused_target and target_from_date is not None
    use_target_current = focused_target or bounded_bulk
    resource_threads = args.threads
    resource_memory_limit = args.memory_limit
    if bounded_bulk:
        resource_threads = resource_threads if resource_threads is not None else 4
        resource_memory_limit = (
            resource_memory_limit
            if resource_memory_limit is not None
            else "2GB"
        )
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, resource_threads, resource_memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    stage_horse_pedigree(con)
    raw_catalog = focused_target and args.pg_url.startswith("r2-catalog://")
    if use_target_current:
        stage_target_entities(
            con,
            input_glob,
            raw_catalog,
            target_from_date,
            target_to_date,
        )
    input_source_filter = target_date_filter_sql(
        "b", target_from_date, target_to_date
    )
    input_sources = frozenset(
        str(row[0])
        for row in con.execute(
            f"select distinct source from read_parquet('{input_glob}', hive_partitioning=true) b "
            f"where {input_source_filter}"
        ).fetchall()
    )
    entity_scoped_history = focused_target or bounded_bulk
    remote_entity_pushdown = entity_scoped_history and args.pg_url.startswith(
        ("postgresql://", "postgres://")
    )
    stage_race_history(
        con,
        from_date,
        entity_scoped_history,
        raw_catalog,
        input_sources if not entity_scoped_history else None,
        to_date=history_to_date,
        remote_entity_pushdown=remote_entity_pushdown,
    )
    if bounded_bulk:
        project_scoped_race_history(con)
    if use_target_current:
        drop_temp_tables(con, ("target_entities",))
    stage_horse_near_miss(con)
    stage_target_context(con, input_glob, use_target_current)
    stage_horse_context(con, focused_target)
    stage_pedigree_cumulatives(con)
    if use_target_current:
        drop_temp_tables(
            con,
            (
                "horse_pedigree",
                "sire_daily_kyori",
                "sire_daily_grade",
                "damsire_daily_kyori",
            ),
        )
    stage_horse_pedigree_context(con, focused_target)
    if use_target_current:
        drop_temp_tables(
            con,
            (
                "pedigree_target",
                "sire_kyori_cumul",
                "sire_grade_cumul",
                "damsire_kyori_cumul",
                "sire_distance_stats",
                "sire_grade_stats",
                "damsire_distance_stats",
            ),
        )
    stage_horse_distance_grade(con, focused_target)
    if use_target_current:
        drop_temp_tables(
            con,
            (
                "horse_daily_kyori_grade",
                "horse_kyori_grade_cumul",
                "horse_kyori_grade_prior",
                "target_context",
                "target_distance_domain",
                "history_distance_domain",
                "distance_bridge",
            ),
        )
    stage_jockey_near_miss(con)
    if use_target_current:
        drop_temp_tables(con, ("jockey_daily", "race_history"))
    if not focused_target:
        stage_bulk_prior_date_lookups(con)
        drop_temp_tables(
            con, ("horse_near_miss", "jockey_near_miss")
        )
    write_partitioned(
        con,
        append_features_sql(
            input_glob,
            focused_target,
            target_from_date,
            target_to_date,
            use_target_current,
        ),
        args.output_dir,
    )
    con.close()


if __name__ == "__main__":
    main()
