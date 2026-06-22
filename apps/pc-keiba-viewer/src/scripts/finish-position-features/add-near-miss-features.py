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
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection
from pedigree_staging import stage_horse_pedigree

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
RACE_PARTITION_BY = "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
DISTANCE_TOLERANCE_M = 200


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
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_race_history(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """過去レースの finish_position / time_sa / tansho_odds / ninkijun / kishumei を staging。"""
    con.execute(
        f"""
        create or replace temp table race_history as
        select
          source,
          race_date,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          ketto_toroku_bango,
          kishumei_ryakusho,
          finish_position,
          time_sa,
          tansho_odds,
          tansho_ninkijun,
          shusso_tosu,
          kyori,
          track_code,
          grade_code
        from pg.race_entry_corner_features
        where race_date >= '{from_date}'
          and finish_position is not null
        """
    )


def stage_horse_near_miss(con: duckdb.DuckDBPyConnection) -> None:
    """馬ごとの 2 着特化 stats を計算 (lookback only, 当該レース除外)。"""
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
            rows between unbounded preceding and 1 preceding
          ),
          horse_recent_5 as (
            partition by source, ketto_toroku_bango
            order by race_date
            rows between 5 preceding and 1 preceding
          )
        """
    )
    con.execute(
        "create index horse_near_miss_idx on horse_near_miss (source, ketto_toroku_bango, race_date)"
    )


def stage_horse_context(con: duckdb.DuckDBPyConnection) -> None:
    """Context-specific (same_keibajo / same_distance / same_track / jockey-horse-pair) 2 着率を
    self-join で計算 (lookback: past.race_date < curr.race_date)。
    """
    con.execute("create index race_history_idx_horse on race_history (source, ketto_toroku_bango, race_date)")
    con.execute(
        """
        create or replace temp table horse_context as
        select
          curr.source, curr.race_date, curr.ketto_toroku_bango,
          count(case when past.keibajo_code = curr.keibajo_code then 1 end) as same_keibajo_starts,
          sum(case when past.keibajo_code = curr.keibajo_code and past.finish_position = 2 then 1 else 0 end) as same_keibajo_p2,
          count(case when curr.kyori is not null and past.kyori is not null and abs(past.kyori - curr.kyori) <= 200 then 1 end) as same_distance_starts,
          sum(case when curr.kyori is not null and past.kyori is not null and abs(past.kyori - curr.kyori) <= 200 and past.finish_position = 2 then 1 else 0 end) as same_distance_p2,
          count(case when left(coalesce(past.track_code, ''), 1) = left(coalesce(curr.track_code, ''), 1) then 1 end) as same_track_starts,
          sum(case when left(coalesce(past.track_code, ''), 1) = left(coalesce(curr.track_code, ''), 1) and past.finish_position = 2 then 1 else 0 end) as same_track_p2,
          count(case when past.kishumei_ryakusho = curr.kishumei_ryakusho then 1 end) as pair_starts,
          sum(case when past.kishumei_ryakusho = curr.kishumei_ryakusho and past.finish_position = 2 then 1 else 0 end) as pair_p2
        from race_history curr
        left join race_history past
          on past.source = curr.source
          and past.ketto_toroku_bango = curr.ketto_toroku_bango
          and past.race_date < curr.race_date
        group by curr.source, curr.race_date, curr.ketto_toroku_bango
        """
    )
    con.execute(
        "create index horse_context_idx on horse_context (source, ketto_toroku_bango, race_date)"
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


def stage_horse_pedigree_context(con: duckdb.DuckDBPyConnection) -> None:
    """ASOF-join target × cumulative pedigree stats.

    Distance tolerance (±200m) is implemented by expanding each target row to all
    matching past kyori values (typically 1-5 discrete JRA distances within ±200m),
    then ASOF-joining the cumulative row per (parent_id, exact past_kyori) with
    target.race_date as the strict-greater inequality.

    Grade match is exact (single bucket per target), no expansion needed.
    """
    con.execute(
        """
        create or replace temp table pedigree_target as
        select
          h.source, h.race_date, h.ketto_toroku_bango,
          h.kyori, coalesce(h.grade_code, '') as grade_code,
          p.sire_id, p.damsire_id
        from race_history h
        left join horse_pedigree p using (ketto_toroku_bango)
        """
    )
    con.execute(
        f"""
        create or replace temp table sire_distance_stats as
        with target_expanded as (
          select t.source, t.race_date, t.ketto_toroku_bango, t.sire_id, t.kyori as t_kyori,
            sk.kyori as past_kyori
          from pedigree_target t
          join (select distinct sire_id, kyori from sire_kyori_cumul) sk
            on sk.sire_id = t.sire_id
            and abs(sk.kyori - t.kyori) <= {DISTANCE_TOLERANCE_M}
          where t.sire_id is not null and t.kyori is not null
        )
        select te.source, te.race_date, te.ketto_toroku_bango,
          sum(coalesce(s.cum_starts, 0)) as sire_distance_starts,
          sum(coalesce(s.cum_p2, 0)) as sire_distance_p2
        from target_expanded te
        asof left join sire_kyori_cumul s
          on te.sire_id = s.sire_id
          and te.past_kyori = s.kyori
          and te.race_date > s.race_date
        group by te.source, te.race_date, te.ketto_toroku_bango
        """
    )
    con.execute(
        """
        create or replace temp table sire_grade_stats as
        select
          t.source, t.race_date, t.ketto_toroku_bango,
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
          select t.source, t.race_date, t.ketto_toroku_bango, t.damsire_id, t.kyori as t_kyori,
            dk.kyori as past_kyori
          from pedigree_target t
          join (select distinct damsire_id, kyori from damsire_kyori_cumul) dk
            on dk.damsire_id = t.damsire_id
            and abs(dk.kyori - t.kyori) <= {DISTANCE_TOLERANCE_M}
          where t.damsire_id is not null and t.kyori is not null
        )
        select te.source, te.race_date, te.ketto_toroku_bango,
          sum(coalesce(d.cum_starts, 0)) as damsire_distance_starts,
          sum(coalesce(d.cum_p2, 0)) as damsire_distance_p2
        from target_expanded te
        asof left join damsire_kyori_cumul d
          on te.damsire_id = d.damsire_id
          and te.past_kyori = d.kyori
          and te.race_date > d.race_date
        group by te.source, te.race_date, te.ketto_toroku_bango
        """
    )
    con.execute(
        """
        create or replace temp table horse_pedigree_context as
        select
          coalesce(sd.source, sg.source, dd.source) as source,
          coalesce(sd.race_date, sg.race_date, dd.race_date) as race_date,
          coalesce(sd.ketto_toroku_bango, sg.ketto_toroku_bango, dd.ketto_toroku_bango) as ketto_toroku_bango,
          sd.sire_distance_starts, sd.sire_distance_p2,
          sg.sire_grade_starts, sg.sire_grade_p2,
          dd.damsire_distance_starts, dd.damsire_distance_p2
        from sire_distance_stats sd
        full outer join sire_grade_stats sg
          on sd.source = sg.source and sd.race_date = sg.race_date and sd.ketto_toroku_bango = sg.ketto_toroku_bango
        full outer join damsire_distance_stats dd
          on coalesce(sd.source, sg.source) = dd.source
          and coalesce(sd.race_date, sg.race_date) = dd.race_date
          and coalesce(sd.ketto_toroku_bango, sg.ketto_toroku_bango) = dd.ketto_toroku_bango
        """
    )
    con.execute(
        "create index horse_pedigree_context_idx on horse_pedigree_context (source, ketto_toroku_bango, race_date)"
    )


def stage_horse_distance_grade(con: duckdb.DuckDBPyConnection) -> None:
    """この馬の (kyori, grade) ペアの過去累積を pre-aggregate + ASOF で計算。
    Distance tolerance ±200m は target row を kyori 候補で expand して ASOF join。"""
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
    con.execute(
        f"""
        create or replace temp table horse_distance_grade as
        with target as (
          select source, race_date, ketto_toroku_bango, kyori,
            coalesce(grade_code, '') as grade_code
          from race_history
          where kyori is not null
        ),
        target_expanded as (
          select t.source, t.race_date, t.ketto_toroku_bango, t.kyori as t_kyori,
            t.grade_code, hk.kyori as past_kyori
          from target t
          join (select distinct source, ketto_toroku_bango, kyori, grade_code
                from horse_kyori_grade_cumul) hk
            on hk.source = t.source
            and hk.ketto_toroku_bango = t.ketto_toroku_bango
            and hk.grade_code = t.grade_code
            and abs(hk.kyori - t.kyori) <= {DISTANCE_TOLERANCE_M}
        )
        select te.source, te.race_date, te.ketto_toroku_bango,
          sum(coalesce(h.cum_starts, 0)) as dg_starts,
          sum(coalesce(h.cum_p2, 0)) as dg_p2
        from target_expanded te
        asof left join horse_kyori_grade_cumul h
          on te.source = h.source
          and te.ketto_toroku_bango = h.ketto_toroku_bango
          and te.grade_code = h.grade_code
          and te.past_kyori = h.kyori
          and te.race_date > h.race_date
        group by te.source, te.race_date, te.ketto_toroku_bango
        """
    )
    con.execute(
        "create index horse_distance_grade_idx on horse_distance_grade (source, ketto_toroku_bango, race_date)"
    )


def stage_jockey_near_miss(con: duckdb.DuckDBPyConnection) -> None:
    """騎手ごとの 2 着率を race_history (PG-staged) から計算。
    同一日に同騎手が複数騎乗する → date 単位 deduplicate して 1 行/日とする。
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
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index jockey_near_miss_idx on jockey_near_miss (source, kishumei_ryakusho, race_date)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_with_meta as (
      select b.*, rh.kishumei_ryakusho, rh.tansho_ninkijun, rh.shusso_tosu
      from base b
      left join race_history rh
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
        case when hc.same_keibajo_starts > 0
             then hc.same_keibajo_p2::double / hc.same_keibajo_starts
             else null end as same_keibajo_place2_rate,
        case when hc.same_distance_starts > 0
             then hc.same_distance_p2::double / hc.same_distance_starts
             else null end as same_distance_place2_rate,
        case when hc.same_track_starts > 0
             then hc.same_track_p2::double / hc.same_track_starts
             else null end as same_track_place2_rate,
        case when hc.pair_starts > 0
             then hc.pair_p2::double / hc.pair_starts
             else null end as jockey_horse_pair_place2_rate,
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
      left join horse_near_miss h
        on h.source = b.source
        and h.ketto_toroku_bango = b.ketto_toroku_bango
        and h.race_date = b.race_date
      left join horse_context hc
        on hc.source = b.source
        and hc.ketto_toroku_bango = b.ketto_toroku_bango
        and hc.race_date = b.race_date
      left join horse_pedigree_context hp
        on hp.source = b.source
        and hp.ketto_toroku_bango = b.ketto_toroku_bango
        and hp.race_date = b.race_date
      left join horse_distance_grade hdg
        on hdg.source = b.source
        and hdg.ketto_toroku_bango = b.ketto_toroku_bango
        and hdg.race_date = b.race_date
      left join jockey_near_miss j
        on j.source = b.source
        and j.kishumei_ryakusho = b.kishumei_ryakusho
        and j.race_date = b.race_date
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
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    stage_race_history(con, args.from_date)
    stage_horse_near_miss(con)
    stage_horse_context(con)
    stage_horse_pedigree(con)
    stage_pedigree_cumulatives(con)
    stage_horse_pedigree_context(con)
    stage_horse_distance_grade(con)
    stage_jockey_near_miss(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
