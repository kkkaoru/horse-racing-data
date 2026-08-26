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
from _catalog_attach import attach_source_catalog
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
    attach_source_catalog(con, pg_url)


def _category_predicates(category: str, alias: str = "h") -> tuple[str, str]:
    """Return (source_value, keibajo_predicate) for race_entry_corner_features.

    The keibajo predicate references ``alias`` (``h`` by default).
    """
    source_value = "jra" if category == "jra" else "nar"
    if category == "ban-ei":
        keibajo_predicate = f"{alias}.keibajo_code = '{BAN_EI_KEIBAJO_CODE}'"
    elif category == "nar":
        keibajo_predicate = (
            f"({alias}.keibajo_code is null or "
            f"{alias}.keibajo_code <> '{BAN_EI_KEIBAJO_CODE}')"
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


def sql_literal(value: str) -> str:
    """Return a safely quoted DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


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


def stage_target_sire_cells(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """Stage only the target cells for which aggregates are required.

    Production inputs contain upcoming rows, so their race date is absent from
    settled history. Keeping the target date here lets the aggregate stages use
    a strict ``history.race_date < target.race_date`` join instead of requiring
    an impossible exact history row on the upcoming date.
    """
    con.execute(
        f"""
        create or replace temp table target_sire_cells as
        select distinct
          hp.sire_id,
          b.keibajo_code,
          {_surface_sql("b.track_code")} as surface_type,
          try_cast(b.kyori as integer) as kyori,
          b.race_date
        from read_parquet(
          '{input_glob}', hive_partitioning=true, union_by_name=true
        ) b
        inner join horse_pedigree hp
          on hp.ketto_toroku_bango = b.ketto_toroku_bango
        where hp.sire_id is not null
          and b.keibajo_code is not null
          and try_cast(b.kyori as integer) is not null
          and b.race_date is not null
        """
    )
    con.execute(
        "create index target_sire_cells_idx on target_sire_cells "
        "(sire_id, keibajo_code, surface_type, kyori, race_date)"
    )


def sire_history_focus_filter_sql(focused_target: bool) -> str:
    if focused_target:
        return "and exists (select 1 from target_sires ts where ts.sire_id = p.sire_id)"
    return ""


def target_sire_horse_filter_sql(con: duckdb.DuckDBPyConnection) -> str:
    """Build a constant horse-ID predicate for the raw remote history scan.

    ``horse_pedigree`` has already applied the cross-source master priority
    (JRA mirror, NAR mirror, then NAR native). Resolving the cohort here keeps
    that exact behavior while allowing the resulting literals to reach the
    Iceberg SE scan; a local semijoin through the compatibility view cannot be
    pushed below its field-size window.
    """
    rows = con.execute(
        """
        select distinct hp.ketto_toroku_bango
        from horse_pedigree hp
        inner join target_sires ts on ts.sire_id = hp.sire_id
        where hp.ketto_toroku_bango is not null
        order by hp.ketto_toroku_bango
        """
    ).fetchall()
    horse_ids = sorted({str(row[0]) for row in rows})
    if not horse_ids:
        return "false"
    literals = ", ".join(sql_literal(horse_id) for horse_id in horse_ids)
    return f"se.ketto_toroku_bango in ({literals})"


def target_history_upper_bound(con: duckdb.DuckDBPyConnection) -> str | None:
    """Return the latest target date as a literal remote-scan upper bound."""
    row = con.execute("select max(race_date) from target_sire_cells").fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def stage_sire_race_history(
    con: duckdb.DuckDBPyConnection,
    from_date: str,
    category: str,
    focused_target: bool = False,
    to_date: str | None = None,
) -> None:
    """Stage finished race rows joined to their sire, with venue / surface / dist.

    One row per (horse, race) carrying sire_id + keibajo_code + surface_type +
    kyori + race_date + finish_position, filtered to the requested category.
    """
    source_value, keibajo_predicate = _category_predicates(category, alias="se")
    target_filter = sire_history_focus_filter_sql(focused_target)
    horse_filter = target_sire_horse_filter_sql(con) if focused_target else "true"
    se_table = "jvd_se" if source_value == "jra" else "nvd_se"
    ra_table = "jvd_ra" if source_value == "jra" else "nvd_ra"
    from_date_literal = sql_literal(from_date)
    from_year_literal = sql_literal(from_date[:4])
    if to_date is None:
        upper_bound = ""
    else:
        upper_bound = (
            f"and se.kaisai_nen <= {sql_literal(to_date[:4])} "
            f"and se.kaisai_nen || se.kaisai_tsukihi < {sql_literal(to_date)}"
        )
    con.execute(
        f"""
        create or replace temp table sire_race_history as
        select
          p.sire_id,
          se.keibajo_code,
          {_surface_sql("ra.track_code")} as surface_type,
          try_cast(nullif(trim(ra.kyori), '') as integer) as kyori,
          se.kaisai_nen || se.kaisai_tsukihi as race_date,
          try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)
            as finish_position
        from pg.{se_table} se
        inner join pg.{ra_table} ra
          using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        inner join horse_pedigree p
          on p.ketto_toroku_bango = se.ketto_toroku_bango
        where se.kaisai_nen >= {from_year_literal}
          and se.kaisai_nen || se.kaisai_tsukihi >= {from_date_literal}
          {upper_bound}
          and p.sire_id is not null
          and try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)
            is not null
          and nullif(trim(se.ketto_toroku_bango), '') is not null
          and try_cast(nullif(trim(se.umaban), '') as integer) is not null
          and try_cast(nullif(trim(ra.kyori), '') as integer) is not null
          and {keibajo_predicate}
          and ({horse_filter})
          {target_filter}
        """
    )
    con.execute(
        "create index sire_race_history_idx on sire_race_history "
        "(sire_id, keibajo_code, surface_type, kyori, race_date)"
    )


def stage_svsd_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """Aggregate strictly-prior history for exact target cells only."""
    con.execute(
        """
        create or replace temp table sire_svsd_cumul as
        select
          t.sire_id, t.keibajo_code, t.surface_type, t.kyori, t.race_date,
          count(h.finish_position) as past_starts,
          count(*) filter (where h.finish_position = 1) as past_wins,
          count(*) filter (where h.finish_position <= 3) as past_places
        from target_sire_cells t
        left join sire_race_history h
          on h.sire_id = t.sire_id
          and h.keibajo_code = t.keibajo_code
          and h.surface_type = t.surface_type
          and h.kyori = t.kyori
          and h.race_date < t.race_date
        group by all
        """
    )
    con.execute(
        "create index sire_svsd_cumul_idx on sire_svsd_cumul "
        "(sire_id, keibajo_code, surface_type, kyori, race_date)"
    )


def stage_svs_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """Aggregate broader target cells without a second remote scan."""
    con.execute(
        """
        create or replace temp table sire_svs_cumul as
        select
          t.sire_id, t.keibajo_code, t.surface_type, t.race_date,
          count(h.finish_position) as past_starts,
          count(*) filter (where h.finish_position = 1) as past_wins,
          count(*) filter (where h.finish_position <= 3) as past_places
        from (
          select distinct sire_id, keibajo_code, surface_type, race_date
          from target_sire_cells
        ) t
        left join sire_race_history h
          on h.sire_id = t.sire_id
          and h.keibajo_code = t.keibajo_code
          and h.surface_type = t.surface_type
          and h.race_date < t.race_date
        group by all
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
    # Read each pedigree master once. The former scoped implementation issued
    # three separate priority-resolution passes (nine remote scans), which was
    # slower than transferring the compact mapping once through R2 Catalog.
    stage_horse_pedigree(con)
    # The output can only use sires present in this input parquet. Apply the
    # same semijoin for whole-day builds as focused single-race builds instead
    # of materializing every sire's NAR/JRA history since --from-date. This is
    # output-equivalent and keeps the final day-base layer within its bounded
    # Container deadline.
    stage_target_sires(con, input_glob)
    stage_target_sire_cells(con, input_glob)
    stage_sire_race_history(
        con,
        args.from_date,
        args.category,
        focused_target=True,
        to_date=target_history_upper_bound(con),
    )
    stage_svsd_cumul(con)
    stage_svs_cumul(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
