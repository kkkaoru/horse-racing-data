#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append sire × venue × surface × distance bias features (v9 layer).

Motivation:
  産駒の競馬場 × 馬場種別 (芝/ダ) × 距離 別の勝率は強い種牡馬 signal。
  既存 stack には sire × kyori / sire × baba はあるが、競馬場 × surface ×
  distance を組み合わせた cumulative bias は無い。

Features added (per horse × race):
  - sire_venue_surface_dist_win_rate   : 同 venue × surface × distance での sire の expanding-window 勝率
  - sire_venue_surface_dist_place_rate : 同 venue × surface × distance での sire の place (≤3) 率
  - sire_venue_surface_dist_runs       : 同 venue × surface × distance での sire の過去出走数
  - sire_venue_surface_win_rate        : 同 venue × surface での sire の勝率 (距離を緩めた広い集計)
  - sire_venue_surface_place_rate      : 同 venue × surface での sire の place 率

Data leakage 防止: 集計は ``race_date`` が当該レースより strictly 前のものだけ
(ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)。

カテゴリ別の入力ソース (pg.race_entry_corner_features は source / keibajo_code を
持つ leak-free history テーブル):
  - jra    : source='jra'
  - nar    : source='nar' かつ keibajo_code <> '83'
  - ban-ei : source='nar' かつ keibajo_code  = '83'

Run with::

  uv run python src/scripts/finish-position-features/add-sire-venue-bias-features.py \\
    --input-dir tmp/feat-jra-v9-similar \\
    --output-dir tmp/feat-jra-v9-sirevenue \\
    --category jra \\
    --pg-url postgresql://horse_racing:***@127.0.0.1:15432/horse_racing
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection
from pedigree_staging import stage_horse_pedigree

DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"

# Ban-ei is always keibajo_code '83'; for nar we exclude it, for ban-ei we keep
# only it. JRA is isolated by source='jra' alone.
BAN_EI_KEIBAJO_CODE = "83"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_sire_venue_bias_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--category",
        choices=("jra", "nar", "ban-ei"),
        default="jra",
        help="jra -> source='jra'; nar/ban-ei -> source='nar' (split on keibajo 83)",
    )
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20000101")
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Focused production mode keibajo_code:race_bango. The input parquet "
            "is already race-scoped; this restricts sire history to target sires."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def _category_predicates(category: str) -> tuple[str, str]:
    """Return (source_value, keibajo_predicate) for race_entry_corner_features.

    The keibajo predicate references the ``h`` alias used in stage 1.
    """
    source_value = "jra" if category == "jra" else "nar"
    if category == "ban-ei":
        keibajo_predicate = f"h.keibajo_code = '{BAN_EI_KEIBAJO_CODE}'"
    elif category == "nar":
        keibajo_predicate = (
            f"(h.keibajo_code is null or h.keibajo_code <> '{BAN_EI_KEIBAJO_CODE}')"
        )
    else:
        keibajo_predicate = "true"
    return source_value, keibajo_predicate


def _surface_sql(track_code_col: str) -> str:
    """track_code -> surface_type: '1%'=turf, '2%'=dirt, else other (Ban-ei -> 'other')."""
    return (
        f"case "
        f"when {track_code_col} like '1%' then 'turf' "
        f"when {track_code_col} like '2%' then 'dirt' "
        f"else 'other' end"
    )


def stage_target_sires(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """Stage the sire IDs needed by the scoped input parquet."""
    con.execute(
        f"""
        create or replace temp table target_sires as
        select distinct hp.sire_id
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
        join horse_pedigree hp on hp.ketto_toroku_bango = b.ketto_toroku_bango
        where hp.sire_id is not null
        """
    )
    con.execute("create index target_sires_idx on target_sires (sire_id)")


def sire_history_focus_filter_sql(focused_target: bool) -> str:
    if focused_target:
        return "and exists (select 1 from target_sires ts where ts.sire_id = p.sire_id)"
    return ""


def stage_sire_race_history(
    con: duckdb.DuckDBPyConnection,
    from_date: str,
    category: str,
    focused_target: bool = False,
) -> None:
    """Stage finished race rows joined to their sire, with venue / surface / dist.

    One row per (horse, race) carrying sire_id + keibajo_code + surface_type +
    kyori + race_date + finish_position, filtered to the requested category.
    """
    source_value, keibajo_predicate = _category_predicates(category)
    target_filter = sire_history_focus_filter_sql(focused_target)
    con.execute(
        f"""
        create or replace temp table sire_race_history as
        select
          p.sire_id,
          h.keibajo_code,
          {_surface_sql("h.track_code")} as surface_type,
          h.kyori,
          h.race_date,
          h.finish_position
        from pg.race_entry_corner_features h
        join horse_pedigree p on p.ketto_toroku_bango = h.ketto_toroku_bango
        where h.source = '{source_value}'
          and h.race_date >= '{from_date}'
          and p.sire_id is not null
          and h.finish_position is not null
          and h.ketto_toroku_bango is not null
          and {keibajo_predicate}
          {target_filter}
        """
    )
    con.execute(
        "create index sire_race_history_idx on sire_race_history "
        "(sire_id, keibajo_code, surface_type, kyori, race_date)"
    )


def stage_svsd_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """Expanding-window cumulative stats per (sire, venue, surface, distance).

    Pre-aggregate to one row per (sire, venue, surface, dist, race_date) daily
    stat, then cumulate strictly-before-current-date (1 PRECEDING) for leak-free
    values.
    """
    con.execute(
        """
        create or replace temp table sire_svsd_daily as
        select
          sire_id, keibajo_code, surface_type, kyori, race_date,
          count(*) as starts_on_day,
          sum(case when finish_position = 1 then 1 else 0 end) as wins_on_day,
          sum(case when finish_position <= 3 then 1 else 0 end) as places_on_day
        from sire_race_history
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table sire_svsd_cumul as
        select
          sire_id, keibajo_code, surface_type, kyori, race_date,
          sum(starts_on_day) over w as past_starts,
          sum(wins_on_day) over w as past_wins,
          sum(places_on_day) over w as past_places
        from sire_svsd_daily
        window w as (
          partition by sire_id, keibajo_code, surface_type, kyori
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index sire_svsd_cumul_idx on sire_svsd_cumul "
        "(sire_id, keibajo_code, surface_type, kyori, race_date)"
    )


def stage_svs_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """Expanding-window cumulative stats per (sire, venue, surface) — broader.

    Same leak-free pattern as the SVSD level, but drops the distance dimension so
    sparse (sire, venue, surface, dist) cells still get a populated fallback.
    """
    con.execute(
        """
        create or replace temp table sire_svs_daily as
        select
          sire_id, keibajo_code, surface_type, race_date,
          count(*) as starts_on_day,
          sum(case when finish_position = 1 then 1 else 0 end) as wins_on_day,
          sum(case when finish_position <= 3 then 1 else 0 end) as places_on_day
        from sire_race_history
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table sire_svs_cumul as
        select
          sire_id, keibajo_code, surface_type, race_date,
          sum(starts_on_day) over w as past_starts,
          sum(wins_on_day) over w as past_wins,
          sum(places_on_day) over w as past_places
        from sire_svs_daily
        window w as (
          partition by sire_id, keibajo_code, surface_type
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index sire_svs_cumul_idx on sire_svs_cumul "
        "(sire_id, keibajo_code, surface_type, race_date)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
    ),
    base_with_sire as (
      select b.*, hp.sire_id as _sire_id
      from base b
      left join horse_pedigree hp on hp.ketto_toroku_bango = b.ketto_toroku_bango
    ),
    base_with_surface as (
      select bws.*,
        {_surface_sql("bws.track_code")} as _surface_type
      from base_with_sire bws
    ),
    joined as (
      select
        bwsf.* exclude (_sire_id, _surface_type),
        case when svsd.past_starts > 0
             then svsd.past_wins::double / svsd.past_starts
             else null end as sire_venue_surface_dist_win_rate,
        case when svsd.past_starts > 0
             then svsd.past_places::double / svsd.past_starts
             else null end as sire_venue_surface_dist_place_rate,
        svsd.past_starts as sire_venue_surface_dist_runs,
        case when svs.past_starts > 0
             then svs.past_wins::double / svs.past_starts
             else null end as sire_venue_surface_win_rate,
        case when svs.past_starts > 0
             then svs.past_places::double / svs.past_starts
             else null end as sire_venue_surface_place_rate
      from base_with_surface bwsf
      left join sire_svsd_cumul svsd
        on svsd.sire_id = bwsf._sire_id
        and svsd.keibajo_code = bwsf.keibajo_code
        and svsd.surface_type = bwsf._surface_type
        and svsd.kyori = bwsf.kyori
        and svsd.race_date = bwsf.race_date
      left join sire_svs_cumul svs
        on svs.sire_id = bwsf._sire_id
        and svs.keibajo_code = bwsf.keibajo_code
        and svs.surface_type = bwsf._surface_type
        and svs.race_date = bwsf.race_date
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
    stage_horse_pedigree(con)
    if args.target_race is not None:
        stage_target_sires(con, input_glob)
    stage_sire_race_history(
        con, args.from_date, args.category, args.target_race is not None
    )
    stage_svsd_cumul(con)
    stage_svs_cumul(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
