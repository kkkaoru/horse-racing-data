#!/usr/bin/env python3
"""Append JRA jockey/pedigree cell features for finish-position cell routing.

The 19 features mirror the leak-free candidate gate:

* jockey x venue x meeting-day empirical-Bayes rates/ranks
* grandsire / sire / keito cell empirical-Bayes rates/ranks

All history windows are race-grain and exclude the whole current race. This is
required because row-grain ``1 preceding`` windows leak when two runners in the
same race share a sire/grandsire/keito.
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import cast

import duckdb

from _catalog_attach import attach_source_catalog

from _resource_defaults import add_resource_args, apply_to_connection

DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
JRA = "('01','02','03','04','05','06','07','08','09','10')"
JOCKEY_K_CELL = 20.0
JOCKEY_K_FULL = 30.0
PED_K = 30.0
PED_K_KEITO = 40.0
BASE_WIN_RATE = 0.0707


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_jra_jockey_pedigree_cell_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pg-url", type=str, default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL))
    parser.add_argument(
        "--from-date",
        type=str,
        default="20080101",
        help="History lower bound. Default matches the adopted gate feature build.",
    )
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Focused production mode keibajo_code:race_bango. The input parquet "
            "is already race-scoped; this restricts history staging to target "
            "jockey/pedigree entities and emits rows for unfinished target races."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    attach_source_catalog(con, pg_url)


def _sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_in_values(values: list[object]) -> str:
    if not values:
        return "(null)"
    return "(" + ",".join(_sql_string(value) for value in values) + ")"


def _fetch_scalar_values(con: duckdb.DuckDBPyConnection, sql: str) -> list[object]:
    return [cast(object, row[0]) for row in con.execute(sql).fetchall()]


def _jockey_focus_filter(con: duckdb.DuckDBPyConnection, focused_target: bool) -> str:
    if not focused_target:
        return ""
    jockeys = _fetch_scalar_values(
        con,
        """
        select distinct kishu_code
        from jra_jockey_cell_target_rows
        where kishu_code is not null and kishu_code <> ''
        """,
    )
    return f"and se.kishu_code in {_sql_in_values(jockeys)}"


def _pedigree_focus_filter(con: duckdb.DuckDBPyConnection, focused_target: bool) -> str:
    if not focused_target:
        return ""
    sires = _sql_in_values(
        _fetch_scalar_values(
            con,
            """
            select distinct sire
            from jra_pedigree_cell_target_rows
            where sire is not null and sire <> ''
            """,
        )
    )
    gsires = _sql_in_values(
        _fetch_scalar_values(
            con,
            """
            select distinct gsire
            from jra_pedigree_cell_target_rows
            where gsire is not null and gsire <> ''
            """,
        )
    )
    keitos = _sql_in_values(
        _fetch_scalar_values(
            con,
            """
            select distinct keito_id
            from jra_pedigree_cell_target_rows
            where keito_id is not null
            """,
        )
    )
    return f"and (sire in {sires} or gsire in {gsires} or keito_id in {keitos})"


def stage_jockey_target_rows(
    con: duckdb.DuckDBPyConnection, input_glob: str, from_year: int
) -> None:
    con.execute(
        f"""
        create or replace temp table jra_jockey_cell_target_rows as
        select distinct
          se.kaisai_nen,
          se.kaisai_tsukihi,
          se.keibajo_code,
          lpad(cast(cast(se.race_bango as int) as varchar),2,'0') as race_bango,
          (se.kaisai_nen || se.kaisai_tsukihi || se.keibajo_code
            || lpad(cast(cast(se.race_bango as int) as varchar),2,'0')) as race_seq,
          se.ketto_toroku_bango,
          se.kishu_code,
          se.kaisai_nichime,
          ra.kyoso_joken_code,
          case
            when cast(ra.kyori as int) < 1400 then 0
            when cast(ra.kyori as int) < 1800 then 1
            when cast(ra.kyori as int) < 2200 then 2
            else 3 end as kyori_band,
          cast(null as double) as is_win,
          cast(null as double) as is_top3
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
        join pg.jvd_se se
          on b.kaisai_nen = se.kaisai_nen
         and b.kaisai_tsukihi = se.kaisai_tsukihi
         and b.keibajo_code = se.keibajo_code
         and lpad(cast(cast(b.race_bango as int) as varchar),2,'0')
           = lpad(cast(cast(se.race_bango as int) as varchar),2,'0')
         and b.ketto_toroku_bango = se.ketto_toroku_bango
        join pg.jvd_ra ra
          on se.kaisai_nen = ra.kaisai_nen and se.kaisai_tsukihi = ra.kaisai_tsukihi
         and se.keibajo_code = ra.keibajo_code and se.race_bango = ra.race_bango
        where se.keibajo_code in {JRA}
          and cast(se.kaisai_nen as int) >= {from_year}
          and se.kishu_code is not null and se.kishu_code <> ''
        """
    )


def stage_jockey_features(
    con: duckdb.DuckDBPyConnection,
    from_year: int,
    input_glob: str,
    focused_target: bool = False,
) -> None:
    target_cte = ""
    focus_filter = ""
    if focused_target:
        stage_jockey_target_rows(con, input_glob, from_year)
        focus_filter = _jockey_focus_filter(con, focused_target=True)
        target_cte = """
        ,
        target_base as (
          select *
          from jra_jockey_cell_target_rows t
          where not exists (
            select 1
            from history_base h
            where h.kaisai_nen = t.kaisai_nen
              and h.kaisai_tsukihi = t.kaisai_tsukihi
              and h.keibajo_code = t.keibajo_code
              and h.race_bango = t.race_bango
              and h.ketto_toroku_bango = t.ketto_toroku_bango
          )
        ),
        base as (
          select * from history_base
          union all
          select * from target_base
        )
        """
    else:
        target_cte = """
        ,
        base as (
          select * from history_base
        )
        """
    con.execute(
        f"""
        create or replace temp table jra_jockey_cell_features as
        with history_base as (
          select
            se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code,
            lpad(cast(cast(se.race_bango as int) as varchar),2,'0') as race_bango,
            (se.kaisai_nen || se.kaisai_tsukihi || se.keibajo_code
              || lpad(cast(cast(se.race_bango as int) as varchar),2,'0')) as race_seq,
            se.ketto_toroku_bango,
            se.kishu_code,
            se.kaisai_nichime,
            ra.kyoso_joken_code,
            case
              when cast(ra.kyori as int) < 1400 then 0
              when cast(ra.kyori as int) < 1800 then 1
              when cast(ra.kyori as int) < 2200 then 2
              else 3 end as kyori_band,
            (case when try_cast(se.kakutei_chakujun as int) = 1 then 1.0 else 0.0 end) as is_win,
            (case when try_cast(se.kakutei_chakujun as int) <= 3 then 1.0 else 0.0 end) as is_top3
          from pg.jvd_se se
          join pg.jvd_ra ra
            on se.kaisai_nen = ra.kaisai_nen and se.kaisai_tsukihi = ra.kaisai_tsukihi
           and se.keibajo_code = ra.keibajo_code and se.race_bango = ra.race_bango
          where se.keibajo_code in {JRA}
            and cast(se.kaisai_nen as int) >= {from_year}
            and try_cast(se.kakutei_chakujun as int) is not null
            and se.kishu_code is not null and se.kishu_code <> ''
            {focus_filter}
        )
        {target_cte},
        jo_race as (
          select race_seq, kishu_code, sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from base group by race_seq, kishu_code
        ),
        jo_prior as (
          select race_seq, kishu_code,
            sum(w) over jo as jo_w,
            sum(n) over jo as jo_n
          from jo_race
          window jo as (partition by kishu_code order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        jvn_race as (
          select race_seq, kishu_code, keibajo_code, kaisai_nichime,
            sum(coalesce(is_win, 0.0)) as w, sum(coalesce(is_top3, 0.0)) as t3, count(is_win) as n
          from base group by race_seq, kishu_code, keibajo_code, kaisai_nichime
        ),
        jvn_prior as (
          select race_seq, kishu_code, keibajo_code, kaisai_nichime,
            sum(w) over jvn as jvn_w,
            sum(t3) over jvn as jvn_t3,
            sum(n) over jvn as jvn_n
          from jvn_race
          window jvn as (
            partition by kishu_code, keibajo_code, kaisai_nichime
            order by race_seq rows between unbounded preceding and 1 preceding
          )
        ),
        jn_race as (
          select race_seq, kishu_code, kaisai_nichime,
            sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from base group by race_seq, kishu_code, kaisai_nichime
        ),
        jn_prior as (
          select race_seq, kishu_code, kaisai_nichime,
            sum(w) over jn as jn_w,
            sum(n) over jn as jn_n
          from jn_race
          window jn as (partition by kishu_code, kaisai_nichime order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        jf_race as (
          select race_seq, kishu_code, keibajo_code, kyoso_joken_code, kyori_band, kaisai_nichime,
            sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from base group by race_seq, kishu_code, keibajo_code, kyoso_joken_code, kyori_band, kaisai_nichime
        ),
        jf_prior as (
          select race_seq, kishu_code, keibajo_code, kyoso_joken_code, kyori_band, kaisai_nichime,
            sum(w) over jf as jf_w,
            sum(n) over jf as jf_n
          from jf_race
          window jf as (
            partition by kishu_code, keibajo_code, kyoso_joken_code, kyori_band, kaisai_nichime
            order by race_seq rows between unbounded preceding and 1 preceding
          )
        ),
        w as (
          select b.*,
            jo.jo_w, jo.jo_n,
            jvn.jvn_w, jvn.jvn_t3, jvn.jvn_n,
            jn.jn_w, jn.jn_n,
            jf.jf_w, jf.jf_n
          from base b
          left join jo_prior jo on b.race_seq = jo.race_seq and b.kishu_code = jo.kishu_code
          left join jvn_prior jvn on b.race_seq = jvn.race_seq and b.kishu_code = jvn.kishu_code
            and b.keibajo_code = jvn.keibajo_code and b.kaisai_nichime = jvn.kaisai_nichime
          left join jn_prior jn on b.race_seq = jn.race_seq and b.kishu_code = jn.kishu_code
            and b.kaisai_nichime = jn.kaisai_nichime
          left join jf_prior jf on b.race_seq = jf.race_seq and b.kishu_code = jf.kishu_code
            and b.keibajo_code = jf.keibajo_code and b.kyoso_joken_code = jf.kyoso_joken_code
            and b.kyori_band = jf.kyori_band and b.kaisai_nichime = jf.kaisai_nichime
        ),
        features as (
          select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
            (jvn_w + {JOCKEY_K_CELL} * (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end)) / (jvn_n + {JOCKEY_K_CELL}) as jk_venue_nichime_win_eb,
            (jvn_t3 + {JOCKEY_K_CELL}*3 * (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end)) / (jvn_n + {JOCKEY_K_CELL}) as jk_venue_nichime_top3_eb,
            (jn_w  + {JOCKEY_K_CELL} * (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end)) / (jn_n  + {JOCKEY_K_CELL}) as jk_nichime_win_eb,
            (jf_w  + {JOCKEY_K_FULL} * (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end)) / (jf_n  + {JOCKEY_K_FULL}) as jk_fullcell_win_eb,
            ((jvn_w + {JOCKEY_K_CELL} * (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end)) / (jvn_n + {JOCKEY_K_CELL}))
               - (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end) as jk_venue_nichime_edge,
            ((jf_w  + {JOCKEY_K_FULL} * (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end)) / (jf_n  + {JOCKEY_K_FULL}))
               - (case when jo_n>0 then jo_w/jo_n else {BASE_WIN_RATE} end) as jk_fullcell_edge,
            ln(1 + jvn_n) as jk_venue_nichime_logn
          from w
        )
        select *,
          case when jk_venue_nichime_win_eb is null then null else (
            rank() over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
              order by jk_venue_nichime_win_eb
            )
            + (count(*) over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, jk_venue_nichime_win_eb
            ) - 1) / 2.0
          ) end as jk_venue_nichime_win_rank_in_race,
          case when jk_venue_nichime_edge is null then null else (
            rank() over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
              order by jk_venue_nichime_edge
            )
            + (count(*) over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, jk_venue_nichime_edge
            ) - 1) / 2.0
          ) end as jk_venue_nichime_edge_rank_in_race
        from features
        where kaisai_nen >= '2013'
        """
    )
    con.execute(
        "create index jra_jockey_cell_features_idx on jra_jockey_cell_features "
        "(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_pedigree_target_rows(
    con: duckdb.DuckDBPyConnection, input_glob: str, from_year: int
) -> None:
    con.execute(
        f"""
        create or replace temp table jra_pedigree_cell_target_rows as
        select distinct
          se.kaisai_nen,
          se.kaisai_tsukihi,
          se.keibajo_code,
          lpad(cast(cast(se.race_bango as int) as varchar),2,'0') as race_bango,
          (se.kaisai_nen || se.kaisai_tsukihi || se.keibajo_code
            || lpad(cast(cast(se.race_bango as int) as varchar),2,'0')) as race_seq,
          se.ketto_toroku_bango,
          ra.kyoso_joken_code,
          case
            when cast(ra.kyori as int) < 1400 then 0
            when cast(ra.kyori as int) < 1800 then 1
            when cast(ra.kyori as int) < 2200 then 2
            else 3 end as kyori_band,
          case
            when ra.track_code between '10' and '22' then 't'
            when ra.track_code between '23' and '26' then 'd'
            else 'j' end as surface,
          trim(both '　' from trim(um.ketto_joho_01b)) as sire,
          trim(both '　' from trim(um.ketto_joho_03b)) as gsire,
          cast(null as double) as is_win,
          cast(null as double) as is_top3,
          bt.keito_id
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
        join pg.jvd_se se
          on b.kaisai_nen = se.kaisai_nen
         and b.kaisai_tsukihi = se.kaisai_tsukihi
         and b.keibajo_code = se.keibajo_code
         and lpad(cast(cast(b.race_bango as int) as varchar),2,'0')
           = lpad(cast(cast(se.race_bango as int) as varchar),2,'0')
         and b.ketto_toroku_bango = se.ketto_toroku_bango
        join pg.jvd_ra ra
          on se.kaisai_nen = ra.kaisai_nen and se.kaisai_tsukihi = ra.kaisai_tsukihi
         and se.keibajo_code = ra.keibajo_code and se.race_bango = ra.race_bango
        join pg.jvd_um um on se.ketto_toroku_bango = um.ketto_toroku_bango
        left join hnmap hn on trim(both '　' from trim(um.ketto_joho_01b)) = hn.nm
        left join pg.jvd_bt bt on hn.h = bt.hanshoku_toroku_bango
        where se.keibajo_code in {JRA}
          and cast(se.kaisai_nen as int) >= {from_year}
        """
    )


def stage_pedigree_features(
    con: duckdb.DuckDBPyConnection,
    from_year: int,
    input_glob: str,
    focused_target: bool = False,
) -> None:
    con.execute(
        "create or replace temp table hnmap as "
        "select trim(both '　' from trim(bamei)) nm, hanshoku_toroku_bango h from pg.jvd_hn"
    )
    target_cte = ""
    focus_filter = ""
    if focused_target:
        stage_pedigree_target_rows(con, input_glob, from_year)
        focus_filter = _pedigree_focus_filter(con, focused_target=True)
        target_cte = """
        ,
        target_base as (
          select *
          from jra_pedigree_cell_target_rows t
          where not exists (
            select 1
            from withkeito_history h
            where h.kaisai_nen = t.kaisai_nen
              and h.kaisai_tsukihi = t.kaisai_tsukihi
              and h.keibajo_code = t.keibajo_code
              and h.race_bango = t.race_bango
              and h.ketto_toroku_bango = t.ketto_toroku_bango
          )
        ),
        withkeito as (
          select * from withkeito_history
          union all
          select * from target_base
        )
        """
    else:
        target_cte = """
        ,
        withkeito as (
          select * from withkeito_history
        )
        """
    con.execute(
        f"""
        create or replace temp table jra_pedigree_cell_features as
        with history_base as (
          select
            se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code,
            lpad(cast(cast(se.race_bango as int) as varchar),2,'0') as race_bango,
            (se.kaisai_nen || se.kaisai_tsukihi || se.keibajo_code
              || lpad(cast(cast(se.race_bango as int) as varchar),2,'0')) as race_seq,
            se.ketto_toroku_bango,
            ra.kyoso_joken_code,
            case
              when cast(ra.kyori as int) < 1400 then 0
              when cast(ra.kyori as int) < 1800 then 1
              when cast(ra.kyori as int) < 2200 then 2
              else 3 end as kyori_band,
            case
              when ra.track_code between '10' and '22' then 't'
              when ra.track_code between '23' and '26' then 'd'
              else 'j' end as surface,
            trim(both '　' from trim(um.ketto_joho_01b)) as sire,
            trim(both '　' from trim(um.ketto_joho_03b)) as gsire,
            (case when try_cast(se.kakutei_chakujun as int) = 1 then 1.0 else 0.0 end) as is_win,
            (case when try_cast(se.kakutei_chakujun as int) <= 3 then 1.0 else 0.0 end) as is_top3
          from pg.jvd_se se
          join pg.jvd_ra ra
            on se.kaisai_nen = ra.kaisai_nen and se.kaisai_tsukihi = ra.kaisai_tsukihi
           and se.keibajo_code = ra.keibajo_code and se.race_bango = ra.race_bango
          join pg.jvd_um um on se.ketto_toroku_bango = um.ketto_toroku_bango
          where se.keibajo_code in {JRA}
            and cast(se.kaisai_nen as int) >= {from_year}
            and try_cast(se.kakutei_chakujun as int) is not null
        ),
        withkeito_history as (
          select b.*, bt.keito_id
          from history_base b
          left join hnmap hn on b.sire = hn.nm
          left join pg.jvd_bt bt on hn.h = bt.hanshoku_toroku_bango
          where true
            {focus_filter}
        )
        {target_cte}
        ,
        go_race as (
          select race_seq, gsire, sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from withkeito where gsire is not null and gsire <> ''
          group by race_seq, gsire
        ),
        go_prior as (
          select race_seq, gsire, sum(w) over go as go_w, sum(n) over go as go_n
          from go_race
          window go as (partition by gsire order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        gds_race as (
          select race_seq, gsire, kyori_band, surface,
            sum(coalesce(is_win, 0.0)) as w, sum(coalesce(is_top3, 0.0)) as t3, count(is_win) as n
          from withkeito where gsire is not null and gsire <> ''
          group by race_seq, gsire, kyori_band, surface
        ),
        gds_prior as (
          select race_seq, gsire, kyori_band, surface,
            sum(w) over gds as gds_w, sum(t3) over gds as gds_t3, sum(n) over gds as gds_n
          from gds_race
          window gds as (partition by gsire, kyori_band, surface order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        gv_race as (
          select race_seq, gsire, keibajo_code,
            sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from withkeito where gsire is not null and gsire <> ''
          group by race_seq, gsire, keibajo_code
        ),
        gv_prior as (
          select race_seq, gsire, keibajo_code, sum(w) over gv as gv_w, sum(n) over gv as gv_n
          from gv_race
          window gv as (partition by gsire, keibajo_code order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        so_race as (
          select race_seq, sire, sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from withkeito where sire is not null and sire <> ''
          group by race_seq, sire
        ),
        so_prior as (
          select race_seq, sire, sum(w) over so as so_w, sum(n) over so as so_n
          from so_race
          window so as (partition by sire order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        scs_race as (
          select race_seq, sire, kyoso_joken_code, surface,
            sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from withkeito where sire is not null and sire <> ''
          group by race_seq, sire, kyoso_joken_code, surface
        ),
        scs_prior as (
          select race_seq, sire, kyoso_joken_code, surface,
            sum(w) over scs as scs_w, sum(n) over scs as scs_n
          from scs_race
          window scs as (partition by sire, kyoso_joken_code, surface order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        ko_race as (
          select race_seq, keito_id, sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from withkeito where keito_id is not null
          group by race_seq, keito_id
        ),
        ko_prior as (
          select race_seq, keito_id, sum(w) over ko as ko_w, sum(n) over ko as ko_n
          from ko_race
          window ko as (partition by keito_id order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        kds_race as (
          select race_seq, keito_id, kyori_band, surface,
            sum(coalesce(is_win, 0.0)) as w, count(is_win) as n
          from withkeito where keito_id is not null
          group by race_seq, keito_id, kyori_band, surface
        ),
        kds_prior as (
          select race_seq, keito_id, kyori_band, surface,
            sum(w) over kds as kds_w, sum(n) over kds as kds_n
          from kds_race
          window kds as (partition by keito_id, kyori_band, surface order by race_seq rows between unbounded preceding and 1 preceding)
        ),
        w as (
          select b.*,
            go.go_w, go.go_n, gds.gds_w, gds.gds_t3, gds.gds_n, gv.gv_w, gv.gv_n,
            so.so_w, so.so_n, scs.scs_w, scs.scs_n, ko.ko_w, ko.ko_n, kds.kds_w, kds.kds_n
          from withkeito b
          left join go_prior go on b.race_seq = go.race_seq and b.gsire = go.gsire
          left join gds_prior gds on b.race_seq = gds.race_seq and b.gsire = gds.gsire
            and b.kyori_band = gds.kyori_band and b.surface = gds.surface
          left join gv_prior gv on b.race_seq = gv.race_seq and b.gsire = gv.gsire
            and b.keibajo_code = gv.keibajo_code
          left join so_prior so on b.race_seq = so.race_seq and b.sire = so.sire
          left join scs_prior scs on b.race_seq = scs.race_seq and b.sire = scs.sire
            and b.kyoso_joken_code = scs.kyoso_joken_code and b.surface = scs.surface
          left join ko_prior ko on b.race_seq = ko.race_seq and b.keito_id = ko.keito_id
          left join kds_prior kds on b.race_seq = kds.race_seq and b.keito_id = kds.keito_id
            and b.kyori_band = kds.kyori_band and b.surface = kds.surface
        ),
        features as (
          select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
            (gds_w + {PED_K} * (case when go_n>0 then go_w/go_n else {BASE_WIN_RATE} end)) / (gds_n + {PED_K}) as gsire_dist_surface_win_eb,
            (gds_t3 + {PED_K}*3 * (case when go_n>0 then go_w/go_n else {BASE_WIN_RATE} end)) / (gds_n + {PED_K}) as gsire_dist_surface_top3_eb,
            (gv_w  + {PED_K} * (case when go_n>0 then go_w/go_n else {BASE_WIN_RATE} end)) / (gv_n  + {PED_K}) as gsire_venue_win_eb,
            ((gds_w + {PED_K} * (case when go_n>0 then go_w/go_n else {BASE_WIN_RATE} end)) / (gds_n + {PED_K}))
               - (case when go_n>0 then go_w/go_n else {BASE_WIN_RATE} end) as gsire_dist_surface_edge,
            ln(1 + gds_n) as gsire_dist_surface_logn,
            (scs_w + {PED_K} * (case when so_n>0 then so_w/so_n else {BASE_WIN_RATE} end)) / (scs_n + {PED_K}) as sire_class_surface_win_eb,
            ((scs_w + {PED_K} * (case when so_n>0 then so_w/so_n else {BASE_WIN_RATE} end)) / (scs_n + {PED_K}))
               - (case when so_n>0 then so_w/so_n else {BASE_WIN_RATE} end) as sire_class_surface_edge,
            case when keito_id is null then null
                 else (kds_w + {PED_K_KEITO} * (case when ko_n>0 then ko_w/ko_n else {BASE_WIN_RATE} end)) / (kds_n + {PED_K_KEITO}) end as keito_dist_surface_win_eb
          from w
        )
        select *,
          case when gsire_dist_surface_win_eb is null then null else (
            rank() over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
              order by gsire_dist_surface_win_eb
            )
            + (count(*) over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, gsire_dist_surface_win_eb
            ) - 1) / 2.0
          ) end as gsire_dist_surface_win_rank_in_race,
          case when gsire_dist_surface_edge is null then null else (
            rank() over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
              order by gsire_dist_surface_edge
            )
            + (count(*) over (
              partition by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, gsire_dist_surface_edge
            ) - 1) / 2.0
          ) end as gsire_dist_surface_edge_rank_in_race
        from features
        where kaisai_nen >= '2013'
        """
    )
    con.execute(
        "create index jra_pedigree_cell_features_idx on jra_pedigree_cell_features "
        "(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
    ),
    j as (
      select
        kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        min(jk_venue_nichime_win_eb) as jk_venue_nichime_win_eb,
        min(jk_venue_nichime_top3_eb) as jk_venue_nichime_top3_eb,
        min(jk_nichime_win_eb) as jk_nichime_win_eb,
        min(jk_fullcell_win_eb) as jk_fullcell_win_eb,
        min(jk_venue_nichime_edge) as jk_venue_nichime_edge,
        min(jk_fullcell_edge) as jk_fullcell_edge,
        min(jk_venue_nichime_logn) as jk_venue_nichime_logn,
        min(jk_venue_nichime_win_rank_in_race) as jk_venue_nichime_win_rank_in_race,
        min(jk_venue_nichime_edge_rank_in_race) as jk_venue_nichime_edge_rank_in_race
      from jra_jockey_cell_features
      group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    ),
    p as (
      select
        kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        min(gsire_dist_surface_win_eb) as gsire_dist_surface_win_eb,
        min(gsire_dist_surface_top3_eb) as gsire_dist_surface_top3_eb,
        min(gsire_venue_win_eb) as gsire_venue_win_eb,
        min(gsire_dist_surface_edge) as gsire_dist_surface_edge,
        min(gsire_dist_surface_logn) as gsire_dist_surface_logn,
        min(sire_class_surface_win_eb) as sire_class_surface_win_eb,
        min(sire_class_surface_edge) as sire_class_surface_edge,
        min(keito_dist_surface_win_eb) as keito_dist_surface_win_eb,
        min(gsire_dist_surface_win_rank_in_race) as gsire_dist_surface_win_rank_in_race,
        min(gsire_dist_surface_edge_rank_in_race) as gsire_dist_surface_edge_rank_in_race
      from jra_pedigree_cell_features
      group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    select
      b.*,
      j.jk_venue_nichime_win_eb,
      j.jk_venue_nichime_top3_eb,
      j.jk_nichime_win_eb,
      j.jk_fullcell_win_eb,
      j.jk_venue_nichime_edge,
      j.jk_fullcell_edge,
      j.jk_venue_nichime_logn,
      j.jk_venue_nichime_win_rank_in_race,
      j.jk_venue_nichime_edge_rank_in_race,
      p.gsire_dist_surface_win_eb,
      p.gsire_dist_surface_top3_eb,
      p.gsire_venue_win_eb,
      p.gsire_dist_surface_edge,
      p.gsire_dist_surface_logn,
      p.sire_class_surface_win_eb,
      p.sire_class_surface_edge,
      p.keito_dist_surface_win_eb,
      p.gsire_dist_surface_win_rank_in_race,
      p.gsire_dist_surface_edge_rank_in_race
    from base b
    left join j
      on b.kaisai_nen = j.kaisai_nen
     and b.kaisai_tsukihi = j.kaisai_tsukihi
     and b.keibajo_code = j.keibajo_code
     and lpad(cast(cast(b.race_bango as int) as varchar),2,'0') = j.race_bango
     and b.ketto_toroku_bango = j.ketto_toroku_bango
    left join p
      on b.kaisai_nen = p.kaisai_nen
     and b.kaisai_tsukihi = p.kaisai_tsukihi
     and b.keibajo_code = p.keibajo_code
     and lpad(cast(cast(b.race_bango as int) as varchar),2,'0') = p.race_bango
     and b.ketto_toroku_bango = p.ketto_toroku_bango
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
    from_year = int(str(args.from_date)[:4])
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    focused_target = args.target_race is not None
    stage_jockey_features(con, from_year, input_glob, focused_target)
    stage_pedigree_features(con, from_year, input_glob, focused_target)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
