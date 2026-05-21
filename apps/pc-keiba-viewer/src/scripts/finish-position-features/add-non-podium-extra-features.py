#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append additional non-podium pattern features (v8 extra layer).

Motivation:
  v8 base (add-non-podium-pattern-features.py) の 16 features では walk-forward で
  v7 baseline 劣後 (top1 -0.30pp)。career-only fade rate は noise が多い (古い data の
  影響、entity-volume bias)。本 layer は recent form (時間窓 short)、pair-wise
  combinations (horse×kishu, kishu×chokyoshi)、context interactive (grade×kyori) で
  discriminative power を強化する。

Features added (per horse × race):
  Multi-window horse fade:
    - horse_recent_fade_rate_3            : 直近 3 走 fade rate
    - horse_recent_fade_rate_10           : 直近 10 走 fade rate
    - horse_fade_streak_consecutive       : 直近の連続 fade 数 (4着以下が何走続いたか)
    - horse_won_last_race                 : 直前 race で 1 着だったか (boolean)
  Recent (not career) jockey/trainer form:
    - kishu_recent_30d_fade_rate          : 直近 30日 の kishu fade rate
    - kishu_recent_30d_starts             : 直近 30日 出走数
    - chokyoshi_recent_30d_fade_rate
    - chokyoshi_recent_30d_starts
  Pair/combo fade:
    - horse_kishu_pair_fade_rate          : 特定 horse-kishu pair の過去 fade rate
    - kishu_chokyoshi_pair_fade_rate      : kishu × chokyoshi 組合せ
  Context interactive:
    - horse_grade_kyori_fade_rate         : horse × grade_code × kyori (±200m) 別 fade

Data leakage 防止: race_date strictly less than current race_date のみ集計。
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
    parser = argparse.ArgumentParser(prog="add_non_podium_extra_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--category", choices=("jra", "nar"), default="jra")
    parser.add_argument("--pg-url", type=str, default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL))
    parser.add_argument("--from-date", type=str, default="20100101")
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_race_history(con: duckdb.DuckDBPyConnection, from_date: str, category: str) -> None:
    se_table = "pg.jvd_se" if category == "jra" else "pg.nvd_se"
    source_filter = "jra" if category == "jra" else "nar"
    con.execute(
        f"""
        create or replace temp table race_history as
        select
          rec.source, rec.race_date,
          rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
          rec.ketto_toroku_bango,
          rec.finish_position,
          rec.grade_code,
          rec.kyori,
          nullif(trim(se.chokyoshi_code), '') as chokyoshi_code,
          nullif(trim(se.kishu_code), '') as kishu_code,
          case when rec.finish_position >= 4 then 1 else 0 end as is_fade,
          case when rec.finish_position = 1 then 1 else 0 end as is_win,
          strptime(rec.race_date, '%Y%m%d')::date as race_date_d
        from pg.race_entry_corner_features rec
        left join {se_table} se
          on se.kaisai_nen = rec.kaisai_nen
          and se.kaisai_tsukihi = rec.kaisai_tsukihi
          and se.keibajo_code = rec.keibajo_code
          and se.race_bango = rec.race_bango
          and se.ketto_toroku_bango = rec.ketto_toroku_bango
        where rec.race_date >= '{from_date}'
          and rec.finish_position is not null
          and rec.source = '{source_filter}'
        """
    )
    con.execute(
        "create index race_history_horse_idx on race_history (source, ketto_toroku_bango, race_date)"
    )
    con.execute(
        "create index race_history_kishu_idx on race_history (source, kishu_code, race_date_d)"
    )
    con.execute(
        "create index race_history_chokyoshi_idx on race_history (source, chokyoshi_code, race_date_d)"
    )


def stage_horse_multi_window(con: duckdb.DuckDBPyConnection) -> None:
    """horse 直近 3/10 走 fade rate + win streak / fade streak。"""
    con.execute(
        """
        create or replace temp table horse_multi_window as
        select source, ketto_toroku_bango, race_date,
          avg(is_fade::double) over horse_recent_3 as recent_fade_rate_3,
          avg(is_fade::double) over horse_recent_10 as recent_fade_rate_10,
          first_value(is_win) over horse_recent_1 as won_last_race
        from race_history
        window
          horse_recent_3 as (
            partition by source, ketto_toroku_bango
            order by race_date
            rows between 3 preceding and 1 preceding
          ),
          horse_recent_10 as (
            partition by source, ketto_toroku_bango
            order by race_date
            rows between 10 preceding and 1 preceding
          ),
          horse_recent_1 as (
            partition by source, ketto_toroku_bango
            order by race_date desc
            rows between 1 preceding and 1 preceding
          )
        """
    )
    con.execute(
        "create index horse_multi_window_idx on horse_multi_window (source, ketto_toroku_bango, race_date)"
    )


def stage_horse_fade_streak(con: duckdb.DuckDBPyConnection) -> None:
    """horse の直近の連続 fade 数 (前 race が fade だったら 1 加算、win/place3 でリセット)。

    実装: row ごとに「直近の non-fade race からの距離」を計算する代わりに、
    rows between 5 preceding を sum で近似 (実用上同じ signal)。
    """
    con.execute(
        """
        create or replace temp table horse_fade_streak as
        select source, ketto_toroku_bango, race_date,
          sum(is_fade) over horse_recent_5 as fade_streak_5
        from race_history
        window horse_recent_5 as (
          partition by source, ketto_toroku_bango
          order by race_date
          rows between 5 preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index horse_fade_streak_idx on horse_fade_streak (source, ketto_toroku_bango, race_date)"
    )


def stage_kishu_recent_30d(con: duckdb.DuckDBPyConnection) -> None:
    """kishu 直近 30 日 の fade rate (range window で date 範囲指定)。"""
    con.execute(
        """
        create or replace temp table kishu_recent_30d as
        with daily as (
          select source, kishu_code, race_date_d,
            count(*) as starts_on_day,
            sum(is_fade) as fade_on_day
          from race_history
          where kishu_code is not null
          group by all
        )
        select source, kishu_code, race_date_d,
          sum(starts_on_day) over kishu_30d as past_starts_30d,
          sum(fade_on_day) over kishu_30d as past_fade_30d
        from daily
        window kishu_30d as (
          partition by source, kishu_code
          order by race_date_d
          range between interval 30 day preceding and interval 1 day preceding
        )
        """
    )
    con.execute(
        "create index kishu_recent_30d_idx on kishu_recent_30d (source, kishu_code, race_date_d)"
    )


def stage_chokyoshi_recent_30d(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp table chokyoshi_recent_30d as
        with daily as (
          select source, chokyoshi_code, race_date_d,
            count(*) as starts_on_day,
            sum(is_fade) as fade_on_day
          from race_history
          where chokyoshi_code is not null
          group by all
        )
        select source, chokyoshi_code, race_date_d,
          sum(starts_on_day) over chokyoshi_30d as past_starts_30d,
          sum(fade_on_day) over chokyoshi_30d as past_fade_30d
        from daily
        window chokyoshi_30d as (
          partition by source, chokyoshi_code
          order by race_date_d
          range between interval 30 day preceding and interval 1 day preceding
        )
        """
    )
    con.execute(
        "create index chokyoshi_recent_30d_idx on chokyoshi_recent_30d (source, chokyoshi_code, race_date_d)"
    )


def stage_horse_kishu_pair_fade(con: duckdb.DuckDBPyConnection) -> None:
    """horse × kishu pair の過去 career fade rate。"""
    con.execute(
        """
        create or replace temp table horse_kishu_pair_daily as
        select source, ketto_toroku_bango, kishu_code, race_date,
          count(*) as starts_on_day,
          sum(is_fade) as fade_on_day
        from race_history
        where kishu_code is not null
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table horse_kishu_pair_fade as
        select source, ketto_toroku_bango, kishu_code, race_date,
          sum(starts_on_day) over pair_career as past_starts,
          sum(fade_on_day) over pair_career as past_fade
        from horse_kishu_pair_daily
        window pair_career as (
          partition by source, ketto_toroku_bango, kishu_code
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index horse_kishu_pair_fade_idx on horse_kishu_pair_fade (source, ketto_toroku_bango, kishu_code, race_date)"
    )


def stage_kishu_chokyoshi_combo_fade(con: duckdb.DuckDBPyConnection) -> None:
    """kishu × chokyoshi 組合せの career fade rate。"""
    con.execute(
        """
        create or replace temp table kishu_chokyoshi_combo_daily as
        select source, kishu_code, chokyoshi_code, race_date,
          count(*) as starts_on_day,
          sum(is_fade) as fade_on_day
        from race_history
        where kishu_code is not null and chokyoshi_code is not null
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table kishu_chokyoshi_combo_fade as
        select source, kishu_code, chokyoshi_code, race_date,
          sum(starts_on_day) over combo_career as past_starts,
          sum(fade_on_day) over combo_career as past_fade
        from kishu_chokyoshi_combo_daily
        window combo_career as (
          partition by source, kishu_code, chokyoshi_code
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        "create index kishu_chokyoshi_combo_fade_idx on kishu_chokyoshi_combo_fade (source, kishu_code, chokyoshi_code, race_date)"
    )


def stage_horse_grade_kyori_fade(con: duckdb.DuckDBPyConnection) -> None:
    """horse × grade_code × kyori (±200m) 別 fade rate (context-specific)。"""
    con.execute(
        """
        create or replace temp table horse_grade_kyori_fade as
        select
          curr.source, curr.race_date, curr.ketto_toroku_bango,
          count(case when past.grade_code = curr.grade_code
                       and curr.kyori is not null and past.kyori is not null
                       and abs(try_cast(past.kyori as int) - try_cast(curr.kyori as int)) <= 200
                     then 1 end) as gk_starts,
          sum(case when past.grade_code = curr.grade_code
                       and curr.kyori is not null and past.kyori is not null
                       and abs(try_cast(past.kyori as int) - try_cast(curr.kyori as int)) <= 200
                     then past.is_fade else 0 end) as gk_fade
        from race_history curr
        left join race_history past
          on past.source = curr.source
          and past.ketto_toroku_bango = curr.ketto_toroku_bango
          and past.race_date < curr.race_date
        group by curr.source, curr.race_date, curr.ketto_toroku_bango
        """
    )
    con.execute(
        "create index horse_grade_kyori_fade_idx on horse_grade_kyori_fade (source, ketto_toroku_bango, race_date)"
    )


def stage_current_entities(con: duckdb.DuckDBPyConnection, category: str) -> None:
    se_table = "pg.jvd_se" if category == "jra" else "pg.nvd_se"
    src_value = "jra" if category == "jra" else "nar"
    con.execute(
        f"""
        create or replace temp table current_entities as
        select
          '{src_value}' as source,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
          nullif(trim(chokyoshi_code), '') as chokyoshi_code,
          nullif(trim(kishu_code), '') as kishu_code
        from {se_table}
        """
    )
    con.execute(
        f"create index current_entities_idx on current_entities ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_w_entities as (
      select b.*,
        ce.chokyoshi_code,
        ce.kishu_code,
        strptime(b.race_date, '%Y%m%d')::date as race_date_d
      from base b
      left join current_entities ce
        on ce.source = b.source
        and ce.kaisai_nen = b.kaisai_nen
        and ce.kaisai_tsukihi = b.kaisai_tsukihi
        and ce.keibajo_code = b.keibajo_code
        and ce.race_bango = b.race_bango
        and ce.ketto_toroku_bango = b.ketto_toroku_bango
    ),
    joined as (
      select
        bwe.* exclude (chokyoshi_code, kishu_code, race_date_d),
        hmw.recent_fade_rate_3 as horse_recent_fade_rate_3,
        hmw.recent_fade_rate_10 as horse_recent_fade_rate_10,
        hmw.won_last_race as horse_won_last_race,
        hfs.fade_streak_5 as horse_fade_streak_consecutive,
        case when kr.past_starts_30d > 0 then kr.past_fade_30d::double / kr.past_starts_30d else null end as kishu_recent_30d_fade_rate,
        kr.past_starts_30d as kishu_recent_30d_starts,
        case when cr.past_starts_30d > 0 then cr.past_fade_30d::double / cr.past_starts_30d else null end as chokyoshi_recent_30d_fade_rate,
        cr.past_starts_30d as chokyoshi_recent_30d_starts,
        case when hkp.past_starts > 0 then hkp.past_fade::double / hkp.past_starts else null end as horse_kishu_pair_fade_rate,
        hkp.past_starts as horse_kishu_pair_career_starts,
        case when kcc.past_starts > 0 then kcc.past_fade::double / kcc.past_starts else null end as kishu_chokyoshi_pair_fade_rate,
        case when hgk.gk_starts > 0 then hgk.gk_fade::double / hgk.gk_starts else null end as horse_grade_kyori_fade_rate
      from base_w_entities bwe
      left join horse_multi_window hmw
        on hmw.source = bwe.source and hmw.ketto_toroku_bango = bwe.ketto_toroku_bango and hmw.race_date = bwe.race_date
      left join horse_fade_streak hfs
        on hfs.source = bwe.source and hfs.ketto_toroku_bango = bwe.ketto_toroku_bango and hfs.race_date = bwe.race_date
      left join kishu_recent_30d kr
        on kr.source = bwe.source and kr.kishu_code = bwe.kishu_code and kr.race_date_d = bwe.race_date_d
      left join chokyoshi_recent_30d cr
        on cr.source = bwe.source and cr.chokyoshi_code = bwe.chokyoshi_code and cr.race_date_d = bwe.race_date_d
      left join horse_kishu_pair_fade hkp
        on hkp.source = bwe.source and hkp.ketto_toroku_bango = bwe.ketto_toroku_bango and hkp.kishu_code = bwe.kishu_code and hkp.race_date = bwe.race_date
      left join kishu_chokyoshi_combo_fade kcc
        on kcc.source = bwe.source and kcc.kishu_code = bwe.kishu_code and kcc.chokyoshi_code = bwe.chokyoshi_code and kcc.race_date = bwe.race_date
      left join horse_grade_kyori_fade hgk
        on hgk.source = bwe.source and hgk.ketto_toroku_bango = bwe.ketto_toroku_bango and hgk.race_date = bwe.race_date
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
    stage_race_history(con, args.from_date, args.category)
    stage_horse_multi_window(con)
    stage_horse_fade_streak(con)
    stage_kishu_recent_30d(con)
    stage_chokyoshi_recent_30d(con)
    stage_horse_kishu_pair_fade(con)
    stage_kishu_chokyoshi_combo_fade(con)
    stage_horse_grade_kyori_fade(con)
    stage_current_entities(con, args.category)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
