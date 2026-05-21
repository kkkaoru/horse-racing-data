#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append non-podium (4+ finish) pattern features (v8 layer).

Motivation:
  既存 v7 features は「podium 候補」を強化する direction だったが、ranking model は
  「下位を確信を持って push down する」signal でも精度が向上する (相対比較なので)。
  本 layer は 4 dimension (horse / kishu / chokyoshi / banushi) で
  「fade pattern (= 4 着以下率)」を encode、podium 推論の対比を強化する。

Features added (per horse × race):
  Horse-level:
    - horse_fade_rate_career             : career での 4 着以下率
    - horse_recent_fade_rate_5           : 直近 5 走の 4 着以下率
    - horse_finish_pos_avg_5             : 直近 5 走の平均着順
    - horse_keibajo_fade_rate            : 同 keibajo での 4 着以下率
    - horse_kyori_fade_rate              : 同 距離 (±200m) での 4 着以下率
  Kishu (jockey) level:
    - kishu_fade_rate_career             : career 4 着以下率
    - kishu_keibajo_fade_rate            : 同 keibajo
    - kishu_grade_fade_rate              : 同 grade_code
  Chokyoshi (trainer) level:
    - chokyoshi_fade_rate_career         : career 4 着以下率
    - chokyoshi_keibajo_fade_rate        : 同 keibajo
    - chokyoshi_grade_fade_rate          : 同 grade_code
  Banushi (owner) level:
    - banushi_fade_rate_career           : owner career 4 着以下率
    - banushi_grade_fade_rate            : 同 grade
  Race-context:
    - field_strong_competitor_count      : current race の中で「低 fade rate」horse の数
    - horse_relative_fade_in_field       : self_fade_rate - field_avg_fade_rate

Data leakage 防止: race_date strictly less than current race_date のみ集計。

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-non-podium-pattern-features.py \\
    --input-dir tmp/feat-jra-v7-final \\
    --output-dir tmp/feat-jra-v8-non-podium \\
    --category jra
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
    parser = argparse.ArgumentParser(prog="add_non_podium_pattern_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--category",
        choices=("jra", "nar"),
        default="jra",
        help="jra → pg.jvd_se, nar → pg.nvd_se",
    )
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


def stage_race_history(con: duckdb.DuckDBPyConnection, from_date: str, category: str) -> None:
    """race_history with 4-dimension entity codes + finish_position + is_fade flag."""
    se_table = "pg.jvd_se" if category == "jra" else "pg.nvd_se"
    source_filter = "jra" if category == "jra" else "nar"
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
          rec.finish_position,
          rec.grade_code,
          rec.kyori,
          nullif(trim(se.chokyoshi_code), '') as chokyoshi_code,
          nullif(trim(se.kishu_code), '') as kishu_code,
          nullif(trim(se.banushi_code), '') as banushi_code,
          case when rec.finish_position >= 4 then 1 else 0 end as is_fade
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


def stage_horse_fade(con: duckdb.DuckDBPyConnection) -> None:
    """horse career + recent 5 fade rate + 直近 5 走 avg finish position。"""
    con.execute(
        """
        create or replace temp table horse_fade as
        select source, ketto_toroku_bango, race_date,
          sum(is_fade) over horse_career as past_fade_count,
          count(*) over horse_career as past_starts,
          avg(is_fade::double) over horse_recent_5 as recent_fade_rate_5,
          avg(finish_position::double) over horse_recent_5 as recent_avg_finish_5
        from race_history
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
        "create index horse_fade_idx on horse_fade (source, ketto_toroku_bango, race_date)"
    )


def stage_horse_context_fade(con: duckdb.DuckDBPyConnection) -> None:
    """horse の同 keibajo / 同 distance (±200m) 別 fade rate。"""
    con.execute(
        """
        create or replace temp table horse_context_fade as
        select
          curr.source, curr.race_date, curr.ketto_toroku_bango,
          count(case when past.keibajo_code = curr.keibajo_code then 1 end) as same_keibajo_starts,
          sum(case when past.keibajo_code = curr.keibajo_code then past.is_fade else 0 end) as same_keibajo_fade,
          count(case when curr.kyori is not null and past.kyori is not null
                       and abs(try_cast(past.kyori as int) - try_cast(curr.kyori as int)) <= 200 then 1 end) as same_kyori_starts,
          sum(case when curr.kyori is not null and past.kyori is not null
                       and abs(try_cast(past.kyori as int) - try_cast(curr.kyori as int)) <= 200
                       then past.is_fade else 0 end) as same_kyori_fade
        from race_history curr
        left join race_history past
          on past.source = curr.source
          and past.ketto_toroku_bango = curr.ketto_toroku_bango
          and past.race_date < curr.race_date
        group by curr.source, curr.race_date, curr.ketto_toroku_bango
        """
    )
    con.execute(
        "create index horse_context_fade_idx on horse_context_fade (source, ketto_toroku_bango, race_date)"
    )


def _stage_entity_career_fade(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    entity_col: str,
) -> None:
    """汎用 entity (kishu/chokyoshi/banushi) の career fade rate cumul。"""
    con.execute(
        f"""
        create or replace temp table {table_name}_daily as
        select source, {entity_col}, race_date,
          count(*) as starts_on_day,
          sum(is_fade) as fade_on_day
        from race_history
        where {entity_col} is not null
        group by all
        """
    )
    con.execute(
        f"""
        create or replace temp table {table_name} as
        select source, {entity_col}, race_date,
          sum(starts_on_day) over career as past_starts,
          sum(fade_on_day) over career as past_fade
        from {table_name}_daily
        window career as (
          partition by source, {entity_col}
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        f"create index {table_name}_idx on {table_name} (source, {entity_col}, race_date)"
    )


def _stage_entity_keibajo_fade(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    entity_col: str,
) -> None:
    """汎用 entity × keibajo_code 別 fade rate cumul。"""
    con.execute(
        f"""
        create or replace temp table {table_name}_daily as
        select source, {entity_col}, keibajo_code, race_date,
          count(*) as starts_on_day,
          sum(is_fade) as fade_on_day
        from race_history
        where {entity_col} is not null
        group by all
        """
    )
    con.execute(
        f"""
        create or replace temp table {table_name} as
        select source, {entity_col}, keibajo_code, race_date,
          sum(starts_on_day) over kj_career as past_starts,
          sum(fade_on_day) over kj_career as past_fade
        from {table_name}_daily
        window kj_career as (
          partition by source, {entity_col}, keibajo_code
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        f"create index {table_name}_idx on {table_name} (source, {entity_col}, keibajo_code, race_date)"
    )


def _stage_entity_grade_fade(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    entity_col: str,
) -> None:
    """汎用 entity × grade_code 別 fade rate cumul。"""
    con.execute(
        f"""
        create or replace temp table {table_name}_daily as
        select source, {entity_col}, grade_code, race_date,
          count(*) as starts_on_day,
          sum(is_fade) as fade_on_day
        from race_history
        where {entity_col} is not null and grade_code is not null
        group by all
        """
    )
    con.execute(
        f"""
        create or replace temp table {table_name} as
        select source, {entity_col}, grade_code, race_date,
          sum(starts_on_day) over gr_career as past_starts,
          sum(fade_on_day) over gr_career as past_fade
        from {table_name}_daily
        window gr_career as (
          partition by source, {entity_col}, grade_code
          order by race_date
          rows between unbounded preceding and 1 preceding
        )
        """
    )
    con.execute(
        f"create index {table_name}_idx on {table_name} (source, {entity_col}, grade_code, race_date)"
    )


def stage_kishu_fade(con: duckdb.DuckDBPyConnection) -> None:
    _stage_entity_career_fade(con, "kishu_career_fade", "kishu_code")
    _stage_entity_keibajo_fade(con, "kishu_keibajo_fade", "kishu_code")
    _stage_entity_grade_fade(con, "kishu_grade_fade", "kishu_code")


def stage_chokyoshi_fade(con: duckdb.DuckDBPyConnection) -> None:
    _stage_entity_career_fade(con, "chokyoshi_career_fade", "chokyoshi_code")
    _stage_entity_keibajo_fade(con, "chokyoshi_keibajo_fade", "chokyoshi_code")
    _stage_entity_grade_fade(con, "chokyoshi_grade_fade", "chokyoshi_code")


def stage_banushi_fade(con: duckdb.DuckDBPyConnection) -> None:
    _stage_entity_career_fade(con, "banushi_career_fade", "banushi_code")
    _stage_entity_grade_fade(con, "banushi_grade_fade", "banushi_code")


def stage_current_race_entities(con: duckdb.DuckDBPyConnection, category: str) -> None:
    """current race の horse × kishu/chokyoshi/banushi mapping。"""
    se_table = "pg.jvd_se" if category == "jra" else "pg.nvd_se"
    con.execute(
        f"""
        create or replace temp table current_race_entities as
        select
          '{ "jra" if category == "jra" else "nar" }' as source,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
          nullif(trim(chokyoshi_code), '') as chokyoshi_code,
          nullif(trim(kishu_code), '') as kishu_code,
          nullif(trim(banushi_code), '') as banushi_code
        from {se_table}
        """
    )
    con.execute(
        f"create index current_race_entities_idx on current_race_entities ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_with_entities as (
      select b.*,
        cre.chokyoshi_code,
        cre.kishu_code,
        cre.banushi_code
      from base b
      left join current_race_entities cre
        on cre.source = b.source
        and cre.kaisai_nen = b.kaisai_nen
        and cre.kaisai_tsukihi = b.kaisai_tsukihi
        and cre.keibajo_code = b.keibajo_code
        and cre.race_bango = b.race_bango
        and cre.ketto_toroku_bango = b.ketto_toroku_bango
    ),
    horse_layer as (
      select
        bwe.*,
        hf.recent_fade_rate_5 as horse_recent_fade_rate_5,
        hf.recent_avg_finish_5 as horse_recent_avg_finish_5,
        case when hf.past_starts > 0 then hf.past_fade_count::double / hf.past_starts else null end as horse_fade_rate_career,
        case when hcf.same_keibajo_starts > 0 then hcf.same_keibajo_fade::double / hcf.same_keibajo_starts else null end as horse_keibajo_fade_rate,
        case when hcf.same_kyori_starts > 0 then hcf.same_kyori_fade::double / hcf.same_kyori_starts else null end as horse_kyori_fade_rate
      from base_with_entities bwe
      left join horse_fade hf on hf.source = bwe.source and hf.ketto_toroku_bango = bwe.ketto_toroku_bango and hf.race_date = bwe.race_date
      left join horse_context_fade hcf on hcf.source = bwe.source and hcf.ketto_toroku_bango = bwe.ketto_toroku_bango and hcf.race_date = bwe.race_date
    ),
    kishu_layer as (
      select
        hl.*,
        case when kcf.past_starts > 0 then kcf.past_fade::double / kcf.past_starts else null end as kishu_fade_rate_career,
        case when kkf.past_starts > 0 then kkf.past_fade::double / kkf.past_starts else null end as kishu_keibajo_fade_rate,
        case when kgf.past_starts > 0 then kgf.past_fade::double / kgf.past_starts else null end as kishu_grade_fade_rate
      from horse_layer hl
      left join kishu_career_fade kcf on kcf.source = hl.source and kcf.kishu_code = hl.kishu_code and kcf.race_date = hl.race_date
      left join kishu_keibajo_fade kkf on kkf.source = hl.source and kkf.kishu_code = hl.kishu_code and kkf.keibajo_code = hl.keibajo_code and kkf.race_date = hl.race_date
      left join kishu_grade_fade kgf on kgf.source = hl.source and kgf.kishu_code = hl.kishu_code and kgf.grade_code = hl.grade_code and kgf.race_date = hl.race_date
    ),
    chokyoshi_layer as (
      select
        kl.*,
        case when ccf.past_starts > 0 then ccf.past_fade::double / ccf.past_starts else null end as chokyoshi_fade_rate_career,
        case when cck.past_starts > 0 then cck.past_fade::double / cck.past_starts else null end as chokyoshi_keibajo_fade_rate,
        case when ccg.past_starts > 0 then ccg.past_fade::double / ccg.past_starts else null end as chokyoshi_grade_fade_rate
      from kishu_layer kl
      left join chokyoshi_career_fade ccf on ccf.source = kl.source and ccf.chokyoshi_code = kl.chokyoshi_code and ccf.race_date = kl.race_date
      left join chokyoshi_keibajo_fade cck on cck.source = kl.source and cck.chokyoshi_code = kl.chokyoshi_code and cck.keibajo_code = kl.keibajo_code and cck.race_date = kl.race_date
      left join chokyoshi_grade_fade ccg on ccg.source = kl.source and ccg.chokyoshi_code = kl.chokyoshi_code and ccg.grade_code = kl.grade_code and ccg.race_date = kl.race_date
    ),
    banushi_layer as (
      select
        cl.* exclude (chokyoshi_code, kishu_code, banushi_code),
        case when bcf.past_starts > 0 then bcf.past_fade::double / bcf.past_starts else null end as banushi_fade_rate_career,
        case when bgf.past_starts > 0 then bgf.past_fade::double / bgf.past_starts else null end as banushi_grade_fade_rate
      from chokyoshi_layer cl
      left join banushi_career_fade bcf on bcf.source = cl.source and bcf.banushi_code = cl.banushi_code and bcf.race_date = cl.race_date
      left join banushi_grade_fade bgf on bgf.source = cl.source and bgf.banushi_code = cl.banushi_code and bgf.grade_code = cl.grade_code and bgf.race_date = cl.race_date
    ),
    race_aggregate as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        avg(horse_fade_rate_career) as field_avg_horse_fade_rate,
        sum(case when horse_fade_rate_career < 0.5 then 1 else 0 end) as field_strong_competitor_count
      from banushi_layer
      group by all
    ),
    joined as (
      select
        bl.*,
        ra.field_avg_horse_fade_rate,
        ra.field_strong_competitor_count,
        bl.horse_fade_rate_career - ra.field_avg_horse_fade_rate as horse_relative_fade_in_field
      from banushi_layer bl
      left join race_aggregate ra
        on ra.source = bl.source
        and ra.kaisai_nen = bl.kaisai_nen
        and ra.kaisai_tsukihi = bl.kaisai_tsukihi
        and ra.keibajo_code = bl.keibajo_code
        and ra.race_bango = bl.race_bango
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
    stage_horse_fade(con)
    stage_horse_context_fade(con)
    stage_kishu_fade(con)
    stage_chokyoshi_fade(con)
    stage_banushi_fade(con)
    stage_current_race_entities(con, args.category)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
