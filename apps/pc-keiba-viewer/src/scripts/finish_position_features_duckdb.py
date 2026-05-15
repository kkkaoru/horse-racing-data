#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""DuckDB-based feature builder that mirrors the TypeScript / PostgreSQL
build-finish-position-features stages but runs the heavy aggregation in
DuckDB and writes year-partitioned Parquet for the LightGBM training
pipeline.

Run with:
  uv run --with duckdb --with pyarrow python \
    src/scripts/finish_position_features_duckdb.py \
    --category jra --from-date 20160101 --to-date 20251231 \
    --output-dir tmp/finish-position-features-parquet
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from time import perf_counter
from typing import TypedDict

import duckdb

DEFAULT_OUTPUT_DIR = Path("tmp/finish-position-features-parquet")
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@localhost:5432/horse_racing"
DEFAULT_THREADS = 8
DEFAULT_MEMORY_LIMIT = "8GB"

RECENT_WINDOW_SIZE = 5
SAME_DISTANCE_TOLERANCE = 200
HISTORY_LOOKBACK_YYYYMMDD = 100000
JOCKEY_RECENT_DAYS = 60
TRACK_BIAS_WINDOW_DAYS = 5
FRONT_CORNER_THRESHOLD = 0.33
RIVAL_DISTANCE_THRESHOLD = 0.3
MAX_FIELD_SIZE = 18
DISTANCE_BAND_METERS = 400
PEDIGREE_MIN_RACES = 5
PEDIGREE_COMPOSITE_DIVISOR = 3
TREND_MIN_RACES = 3


class BuildArgs(TypedDict):
    category: str
    from_date: str
    output_dir: Path
    pg_url: str
    threads: int
    to_date: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_features_duckdb")
    parser.add_argument(
        "--category",
        choices=("all", "ban-ei", "jra", "nar"),
        default="jra",
    )
    parser.add_argument("--from-date", type=str, default="20160101")
    parser.add_argument("--to-date", type=str, default="20251231")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--pg-url", type=str, default=None)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--memory-limit", type=str, default=DEFAULT_MEMORY_LIMIT)
    return parser.parse_args(argv)


def resolve_pg_url(cli_value: str | None) -> str:
    if cli_value is not None and cli_value != "":
        return cli_value
    env_value = os.environ.get("DATABASE_URL_LOCAL") or os.environ.get("DATABASE_URL")
    if env_value is not None and env_value != "":
        return env_value
    return DEFAULT_PG_URL


def category_source_filter(category: str, alias: str) -> str:
    if category == "jra":
        return f"{alias}.source = 'jra'"
    if category == "nar":
        return f"{alias}.source = 'nar' and {alias}.keibajo_code <> '83'"
    if category == "ban-ei":
        return f"{alias}.source = 'nar' and {alias}.keibajo_code = '83'"
    return "true"


def category_expression(category: str) -> str:
    if category == "jra":
        return "'jra'"
    if category == "nar":
        return "'nar'"
    if category == "ban-ei":
        return "'ban-ei'"
    return "case when source='jra' then 'jra' when keibajo_code='83' then 'ban-ei' else 'nar' end"


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")


def stage_source_tables(con: duckdb.DuckDBPyConnection, from_date: str, to_date: str) -> None:
    history_start = str(int(from_date) - HISTORY_LOOKBACK_YYYYMMDD)
    con.execute(
        f"""
        create or replace temp table rec as
        select * from pg.race_entry_corner_features
        where race_date between '{history_start}' and '{to_date}'
        """
    )
    con.execute("create index rec_horse_date on rec (source, ketto_toroku_bango, race_date)")
    con.execute("create index rec_jockey_date on rec (source, kishumei_ryakusho, race_date)")
    con.execute("create index rec_trainer_date on rec (source, chokyoshimei_ryakusho, race_date)")
    con.execute("create index rec_keibajo_date on rec (source, keibajo_code, race_date)")
    con.execute(
        f"""
        create or replace temp table jra_se as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
               nullif(bataiju, '') as bataiju
        from pg.jvd_se
        where (kaisai_nen || kaisai_tsukihi) between '{history_start}' and '{to_date}'
        """
    )
    con.execute(
        f"""
        create or replace temp table nar_se as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
               nullif(bataiju, '') as bataiju
        from pg.nvd_se
        where (kaisai_nen || kaisai_tsukihi) between '{history_start}' and '{to_date}'
        """
    )
    con.execute("create or replace temp table jra_um as select * from pg.jvd_um")
    con.execute(
        "create or replace temp table nar_um as select * from pg.nvd_um"
    )
    con.execute(
        f"""
        create or replace temp table jra_ra as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, tenko_code
        from pg.jvd_ra
        where (kaisai_nen || kaisai_tsukihi) between '{from_date}' and '{to_date}'
        """
    )
    con.execute(
        f"""
        create or replace temp table nar_ra as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, tenko_code
        from pg.nvd_ra
        where (kaisai_nen || kaisai_tsukihi) between '{from_date}' and '{to_date}'
        """
    )


def build_target_table(con: duckdb.DuckDBPyConnection, category: str, from_date: str, to_date: str) -> None:
    filter_clause = category_source_filter(category, "rec")
    cat_expr = category_expression(category)
    con.execute(
        f"""
        create or replace temp table target as
        select
          rec.source,
          rec.race_date,
          rec.kaisai_nen,
          rec.kaisai_tsukihi,
          rec.keibajo_code,
          rec.race_bango,
          rec.ketto_toroku_bango,
          rec.umaban,
          {cat_expr} as category,
          rec.kyori,
          rec.track_code,
          rec.grade_code,
          rec.shusso_tosu,
          rec.finish_position,
          rec.finish_norm,
          rec.kishumei_ryakusho,
          rec.chokyoshimei_ryakusho,
          rec.kyoso_joken_code,
          rec.babajotai_code_shiba,
          rec.babajotai_code_dirt,
          'v1' as feature_schema_version,
          cast(substr(rec.race_date, 1, 4) as int) as race_year
        from rec
        where rec.race_date between '{from_date}' and '{to_date}'
          and {filter_clause}
          and rec.ketto_toroku_bango is not null
        """
    )


def horse_career_cte() -> str:
    return f"""
    horse_history as (
      select
        t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
        t.ketto_toroku_bango, t.race_date as target_race_date,
        t.keibajo_code as target_keibajo, t.kyori as target_kyori,
        t.track_code as target_track_code, t.grade_code as target_grade_code,
        h.race_date as history_race_date,
        h.finish_position,
        cast(h.time_sa as double) as time_sa,
        cast(h.kohan_3f as double) as kohan_3f,
        cast(h.corner4_norm as double) as corner4_norm,
        h.keibajo_code as history_keibajo,
        h.kyori as history_kyori,
        h.track_code as history_track_code,
        h.grade_code as history_grade_code,
        row_number() over (
          partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
          order by h.race_date desc
        ) as recent_rank
      from target t
      join rec h
        on h.source = t.source
        and h.ketto_toroku_bango = t.ketto_toroku_bango
        and h.race_date < t.race_date
        and cast(h.race_date as int) >= cast(t.race_date as int) - {HISTORY_LOOKBACK_YYYYMMDD}
      where h.finish_position is not null
    ),
    horse_career as (
      select
        source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        avg(time_sa) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as speed_index_avg_5,
        min(time_sa) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as speed_index_best_5,
        avg(kohan_3f) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as kohan3f_avg_5,
        avg(corner4_norm) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as corner_pass_avg_5,
        avg(case when finish_position = 1 then 1 else 0 end) as career_win_rate,
        avg(case when finish_position between 1 and 3 then 1 else 0 end) as career_place_rate,
        count(*) filter (where finish_position = 1) as career_top1_count,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as same_keibajo_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE}) as same_distance_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)) as same_track_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, '')) as same_grade_win_rate,
        strptime(max(target_race_date), '%Y%m%d')::date - strptime(max(history_race_date) filter (where recent_rank = 1), '%Y%m%d')::date as days_since_last_race,
        count(*) filter (where strptime(target_race_date, '%Y%m%d')::date - strptime(history_race_date, '%Y%m%d')::date <= 30) as consecutive_race_count
      from horse_history
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    """


def partner_history_cte(alias: str, partner_column: str, agg_alias: str) -> str:
    return f"""
    {alias} as (
      select
        t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        t.race_date as target_race_date,
        t.keibajo_code as target_keibajo, t.kyori as target_kyori,
        t.track_code as target_track_code, t.grade_code as target_grade_code,
        t.ketto_toroku_bango as target_horse,
        h.finish_position,
        h.race_date as history_race_date,
        h.keibajo_code as history_keibajo,
        h.kyori as history_kyori,
        h.track_code as history_track_code,
        h.grade_code as history_grade_code,
        h.ketto_toroku_bango as history_horse
      from target t
      join rec h
        on h.source = t.source
        and h.{partner_column} = t.{partner_column}
        and h.race_date < t.race_date
        and cast(h.race_date as int) >= cast(t.race_date as int) - {HISTORY_LOOKBACK_YYYYMMDD}
      where h.finish_position is not null and t.{partner_column} is not null
    ),
    {agg_alias} as (
      select
        source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        {{aggregations}}
      from {alias}
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    """


def jockey_cte() -> str:
    template = partner_history_cte("jockey_history", "kishumei_ryakusho", "jockey_career")
    aggregations = f"""
        avg(case when finish_position = 1 then 1 else 0 end) as jockey_career_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where strptime(history_race_date, '%Y%m%d')::date >= strptime(target_race_date, '%Y%m%d')::date - {JOCKEY_RECENT_DAYS}) as jockey_recent_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as jockey_keibajo_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE}) as jockey_distance_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)) as jockey_track_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, '')) as jockey_grade_win_rate,
        count(*) filter (where history_horse = target_horse) as jockey_horse_pair_count,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as jockey_horse_pair_win_rate
    """
    return template.replace("{aggregations}", aggregations)


def trainer_cte() -> str:
    template = partner_history_cte("trainer_history", "chokyoshimei_ryakusho", "trainer_career")
    aggregations = f"""
        avg(case when finish_position = 1 then 1 else 0 end) as trainer_career_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as trainer_keibajo_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE}) as trainer_distance_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as trainer_horse_win_rate
    """
    return template.replace("{aggregations}", aggregations)


def pedigree_cte(category: str) -> str:
    if category == "jra":
        um_table = "jra_um"
        source_filter = "rec.source = 'jra'"
    elif category in ("nar", "ban-ei"):
        um_table = "nar_um"
        source_filter = "rec.source = 'nar'"
        if category == "ban-ei":
            source_filter = "rec.source = 'nar' and rec.keibajo_code = '83'"
        else:
            source_filter = "rec.source = 'nar' and rec.keibajo_code <> '83'"
    else:
        um_table = "jra_um"
        source_filter = "true"
    return f"""
    sire_distance_stats as (
      select
        um.ketto_joho_01b as sire,
        cast(coalesce(rec.kyori, 0) as int) / {DISTANCE_BAND_METERS} as kyori_band,
        avg(case when rec.finish_position = 1 then 1 else 0 end) as sire_distance_win_rate_val,
        avg(rec.finish_norm) as sire_avg_finish_at_distance_val,
        count(*) as race_count
      from rec join {um_table} um using (ketto_toroku_bango)
      where {source_filter} and rec.finish_position is not null
        and um.ketto_joho_01b is not null and trim(um.ketto_joho_01b) <> ''
      group by 1, 2
    ),
    sire_track_stats as (
      select
        um.ketto_joho_01b as sire,
        left(coalesce(rec.track_code, ''), 1) as surface,
        avg(case when rec.finish_position = 1 then 1 else 0 end) as sire_track_win_rate_val,
        count(*) as race_count
      from rec join {um_table} um using (ketto_toroku_bango)
      where {source_filter} and rec.finish_position is not null
        and um.ketto_joho_01b is not null and trim(um.ketto_joho_01b) <> ''
      group by 1, 2
    ),
    damsire_distance_stats as (
      select
        um.ketto_joho_05b as damsire,
        cast(coalesce(rec.kyori, 0) as int) / {DISTANCE_BAND_METERS} as kyori_band,
        avg(case when rec.finish_position = 1 then 1 else 0 end) as dam_sire_distance_win_rate_val,
        count(*) as race_count
      from rec join {um_table} um using (ketto_toroku_bango)
      where {source_filter} and rec.finish_position is not null
        and um.ketto_joho_05b is not null and trim(um.ketto_joho_05b) <> ''
      group by 1, 2
    ),
    damsire_track_stats as (
      select
        um.ketto_joho_05b as damsire,
        left(coalesce(rec.track_code, ''), 1) as surface,
        avg(rec.finish_norm) as damsire_avg_finish_at_track_val,
        count(*) as race_count
      from rec join {um_table} um using (ketto_toroku_bango)
      where {source_filter} and rec.finish_position is not null
        and um.ketto_joho_05b is not null and trim(um.ketto_joho_05b) <> ''
      group by 1, 2
    ),
    target_pedigree as (
      select
        t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        cast(coalesce(t.kyori, 0) as int) / {DISTANCE_BAND_METERS} as kyori_band,
        left(coalesce(t.track_code, ''), 1) as surface,
        um.ketto_joho_01b as target_sire,
        um.ketto_joho_05b as target_damsire
      from target t
      left join {um_table} um using (ketto_toroku_bango)
    )
    """


def race_context_cte() -> str:
    return f"""
    race_horses as (
      select
        source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        speed_index_avg_5, speed_index_best_5, same_distance_win_rate
      from horse_career
    ),
    race_field_aggregates as (
      select
        source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        avg(speed_index_avg_5) as race_avg_speed,
        count(*) filter (where same_distance_win_rate > {RIVAL_DISTANCE_THRESHOLD}) as race_strong_count
      from race_horses
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    ),
    race_top3_speed as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             avg(speed_index_best_5) as race_top_speed
      from (
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          speed_index_best_5,
          row_number() over (
            partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
            order by speed_index_best_5 asc nulls last
          ) as rk
        from race_horses
        where speed_index_best_5 is not null
      ) where rk <= 3
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
    """


def track_bias_cte() -> str:
    return f"""
    track_bias as (
      select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        avg(case when h.finish_position = 1 and h.umaban * 2 <= h.shusso_tosu + 1 then 1 else 0 end) as track_bias_inside,
        avg(case when h.finish_position = 1 and cast(h.corner1_norm as double) <= {FRONT_CORNER_THRESHOLD} then 1 else 0 end) as track_bias_front
      from target t
      left join rec h
        on h.source = t.source and h.keibajo_code = t.keibajo_code
        and h.race_date < t.race_date
        and strptime(h.race_date, '%Y%m%d')::date >= strptime(t.race_date, '%Y%m%d')::date - {TRACK_BIAS_WINDOW_DAYS}
        and h.finish_position is not null
      group by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
    )
    """


def weight_cte() -> str:
    return f"""
    target_weight as (
      select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        t.race_date,
        coalesce(cast(j.bataiju as int), cast(n.bataiju as int)) as current_bataiju
      from target t
      left join jra_se j on t.source='jra' and j.kaisai_nen=t.kaisai_nen and j.kaisai_tsukihi=t.kaisai_tsukihi
        and j.keibajo_code=t.keibajo_code and j.race_bango=t.race_bango and j.ketto_toroku_bango=t.ketto_toroku_bango
      left join nar_se n on t.source='nar' and n.kaisai_nen=t.kaisai_nen and n.kaisai_tsukihi=t.kaisai_tsukihi
        and n.keibajo_code=t.keibajo_code and n.race_bango=t.race_bango and n.ketto_toroku_bango=t.ketto_toroku_bango
    ),
    weight_history as (
      select
        tw.source, tw.kaisai_nen, tw.kaisai_tsukihi, tw.keibajo_code, tw.race_bango, tw.ketto_toroku_bango,
        tw.current_bataiju,
        coalesce(cast(hj.bataiju as int), cast(hn.bataiju as int)) as history_bataiju,
        row_number() over (
          partition by tw.source, tw.kaisai_nen, tw.kaisai_tsukihi, tw.keibajo_code, tw.race_bango, tw.ketto_toroku_bango
          order by h.race_date desc
        ) as recent_rank
      from target_weight tw
      join rec h
        on h.source = tw.source and h.ketto_toroku_bango = tw.ketto_toroku_bango
        and h.race_date < tw.race_date
        and cast(h.race_date as int) >= cast(tw.race_date as int) - {HISTORY_LOOKBACK_YYYYMMDD}
      left join jra_se hj on h.source='jra' and hj.kaisai_nen=h.kaisai_nen and hj.kaisai_tsukihi=h.kaisai_tsukihi
        and hj.keibajo_code=h.keibajo_code and hj.race_bango=h.race_bango and hj.ketto_toroku_bango=h.ketto_toroku_bango
      left join nar_se hn on h.source='nar' and hn.kaisai_nen=h.kaisai_nen and hn.kaisai_tsukihi=h.kaisai_tsukihi
        and hn.keibajo_code=h.keibajo_code and hn.race_bango=h.race_bango and hn.ketto_toroku_bango=h.ketto_toroku_bango
      where h.finish_position is not null
    ),
    weight_agg as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        max(current_bataiju) as current_bataiju_kept,
        avg(history_bataiju) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as weight_avg_5
      from weight_history
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    """


def recent_form_cte() -> str:
    class_map = """
      case kyoso_joken_code
        when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
        when '701' then 4 when '703' then 5 when '999' then 6
        else null end
    """
    return f"""
    recent_form_history as (
      select
        t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        t.kyori as target_kyori,
        ({class_map.replace("kyoso_joken_code", "t.kyoso_joken_code")}) as target_class_level,
        cast(h.finish_norm as double) as finish_norm,
        cast(h.time_sa as double) as time_sa,
        cast(h.corner3_norm as double) as corner3_norm,
        h.kyori as history_kyori,
        ({class_map.replace("kyoso_joken_code", "h.kyoso_joken_code")}) as history_class_level,
        row_number() over (
          partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
          order by h.race_date desc
        ) as recent_rank
      from target t
      join rec h
        on h.source = t.source and h.ketto_toroku_bango = t.ketto_toroku_bango
        and h.race_date < t.race_date
        and cast(h.race_date as int) >= cast(t.race_date as int) - {HISTORY_LOOKBACK_YYYYMMDD}
      where h.finish_position is not null
    ),
    recent_form as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        max(finish_norm) filter (where recent_rank = 1) as last_race_finish_norm,
        max(time_sa) filter (where recent_rank = 1) as last_race_margin_to_winner,
        max(corner3_norm) filter (where recent_rank = 1) as last_race_corner_pass_norm,
        max(target_class_level) filter (where recent_rank = 1)
          - max(history_class_level) filter (where recent_rank = 1) as last_race_class_diff,
        max(history_kyori) filter (where recent_rank = 1)
          - max(target_kyori) filter (where recent_rank = 1) as last_race_distance_diff,
        case when count(*) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) >= {TREND_MIN_RACES}
             then regr_slope(finish_norm, cast(recent_rank as double)) filter (where recent_rank <= {RECENT_WINDOW_SIZE})
             else null end as finish_trend_5,
        avg(finish_norm) filter (where recent_rank <= 3) as last_3_avg_finish_norm
      from recent_form_history
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    """


def legacy_five_cte() -> str:
    return f"""
    legacy_horse_history as (
      select
        t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        cast(h.finish_norm as double) as finish_norm,
        row_number() over (
          partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
          order by h.race_date desc
        ) as recent_rank
      from target t
      join rec h
        on h.source = t.source and h.ketto_toroku_bango = t.ketto_toroku_bango
        and h.race_date < t.race_date
        and cast(h.race_date as int) >= cast(t.race_date as int) - {HISTORY_LOOKBACK_YYYYMMDD}
      where h.finish_position is not null
    ),
    legacy_horse_avg as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        avg(finish_norm) as avg_finish,
        avg(finish_norm) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as recent_finish
      from legacy_horse_history
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    ),
    legacy_target as (
      select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        cast(rec.tansho_ninkijun as int) as ninkijun,
        cast(rec.tansho_odds as double) as odds_value,
        cast(rec.shusso_tosu as int) as runner_count
      from target t
      join rec on rec.source = t.source and rec.kaisai_nen = t.kaisai_nen
        and rec.kaisai_tsukihi = t.kaisai_tsukihi and rec.keibajo_code = t.keibajo_code
        and rec.race_bango = t.race_bango and rec.ketto_toroku_bango = t.ketto_toroku_bango
    ),
    legacy_features as (
      select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        lha.avg_finish,
        lha.recent_finish,
        case when t.runner_count > 1 and t.ninkijun is not null
             then greatest(0::double, least(1::double, (t.ninkijun - 1)::double / nullif(t.runner_count - 1, 0)))
             else null end as popularity_score,
        case when t.odds_value is not null and t.odds_value > 0
             then greatest(0::double, least(1::double, ln(greatest(t.odds_value, 1::double)) / ln(300::double)))
             else null end as odds_score
      from legacy_target t
      left join legacy_horse_avg lha using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    )
    """


def assemble_final_query(category: str) -> str:
    return f"""
    with
    {horse_career_cte()},
    {jockey_cte()},
    {trainer_cte()},
    {pedigree_cte(category)},
    {race_context_cte()},
    {track_bias_cte()},
    {weight_cte()},
    {recent_form_cte()},
    {legacy_five_cte()},
    weather_lookup as (
      select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        coalesce(jr.tenko_code, nr.tenko_code) as tenko_code
      from target t
      left join jra_ra jr on t.source='jra' and jr.kaisai_nen=t.kaisai_nen and jr.kaisai_tsukihi=t.kaisai_tsukihi
        and jr.keibajo_code=t.keibajo_code and jr.race_bango=t.race_bango
      left join nar_ra nr on t.source='nar' and nr.kaisai_nen=t.kaisai_nen and nr.kaisai_tsukihi=t.kaisai_tsukihi
        and nr.keibajo_code=t.keibajo_code and nr.race_bango=t.race_bango
    )
    select
      t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
      t.ketto_toroku_bango, t.umaban, t.category, t.kyori, t.track_code, t.grade_code, t.shusso_tosu,
      t.finish_position, t.finish_norm,
      hc.speed_index_avg_5, hc.speed_index_best_5, hc.kohan3f_avg_5, hc.corner_pass_avg_5,
      hc.career_win_rate, hc.career_place_rate, hc.career_top1_count,
      hc.same_keibajo_win_rate, hc.same_distance_win_rate, hc.same_track_win_rate, hc.same_grade_win_rate,
      wa.weight_avg_5,
      cast(wa.current_bataiju_kept as double) - wa.weight_avg_5 as weight_diff_from_avg,
      hc.days_since_last_race, hc.consecutive_race_count,
      jc.jockey_career_win_rate, jc.jockey_recent_win_rate, jc.jockey_keibajo_win_rate,
      jc.jockey_distance_win_rate, jc.jockey_track_win_rate, jc.jockey_grade_win_rate,
      jc.jockey_horse_pair_count, jc.jockey_horse_pair_win_rate,
      tc.trainer_career_win_rate, tc.trainer_keibajo_win_rate, tc.trainer_distance_win_rate, tc.trainer_horse_win_rate,
      case when sds.race_count >= {PEDIGREE_MIN_RACES} then sds.sire_distance_win_rate_val else null end as sire_distance_win_rate,
      case when sts.race_count >= {PEDIGREE_MIN_RACES} then sts.sire_track_win_rate_val else null end as sire_track_win_rate,
      case when dsd.race_count >= {PEDIGREE_MIN_RACES} then dsd.dam_sire_distance_win_rate_val else null end as dam_sire_distance_win_rate,
      case when sds.race_count >= {PEDIGREE_MIN_RACES} then sds.sire_avg_finish_at_distance_val else null end as sire_avg_finish_at_distance,
      case when dst.race_count >= {PEDIGREE_MIN_RACES} then dst.damsire_avg_finish_at_track_val else null end as damsire_avg_finish_at_track,
      (
        coalesce(sds.sire_distance_win_rate_val, 0) +
        coalesce(dsd.dam_sire_distance_win_rate_val, 0) +
        coalesce(sts.sire_track_win_rate_val, 0)
      ) / {PEDIGREE_COMPOSITE_DIVISOR}::double as pedigree_score_for_race,
      rfa.race_avg_speed as field_strength_avg_speed,
      rts.race_top_speed as field_strength_top3_speed,
      greatest(0, rfa.race_strong_count - case when hc.same_distance_win_rate > {RIVAL_DISTANCE_THRESHOLD} then 1 else 0 end) as rival_count_at_distance,
      tb.track_bias_inside,
      tb.track_bias_front,
      case wl.tenko_code
        when '1' then 0::double when '2' then 0.3::double
        when '3' then 0.7::double when '4' then 0.7::double
        when '5' then 1.0::double when '6' then 1.0::double
        else null end as weather_normalized,
      case
        when left(coalesce(t.track_code, ''), 1) = '1' then
          case t.babajotai_code_shiba when '1' then 0::double when '2' then 0.3::double when '3' then 0.6::double when '4' then 1.0::double else null end
        else
          case t.babajotai_code_dirt when '1' then 0::double when '2' then 0.3::double when '3' then 0.6::double when '4' then 1.0::double else null end
      end as track_condition_normalized,
      least(1::double, greatest(0::double, coalesce(t.shusso_tosu, 0)::double / {MAX_FIELD_SIZE})) as field_size_normalized,
      case when trim(coalesce(t.grade_code, '')) in ('A', 'B', 'C', 'D', 'G', 'H') then 1 else 0 end::int as is_grade_race,
      rf.last_race_finish_norm, rf.last_race_margin_to_winner, rf.last_race_corner_pass_norm,
      rf.last_race_class_diff, rf.last_race_distance_diff, rf.finish_trend_5, rf.last_3_avg_finish_norm,
      lf.avg_finish, lf.recent_finish, lf.popularity_score, lf.odds_score,
      t.feature_schema_version,
      t.race_year,
      t.source || ':' || t.kaisai_nen || ':' || t.kaisai_tsukihi || ':' || t.keibajo_code || ':' || t.race_bango as race_id
    from target t
    left join horse_career hc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join jockey_career jc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join trainer_career tc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join target_pedigree tp using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join sire_distance_stats sds on sds.sire = tp.target_sire and sds.kyori_band = tp.kyori_band
    left join sire_track_stats sts on sts.sire = tp.target_sire and sts.surface = tp.surface
    left join damsire_distance_stats dsd on dsd.damsire = tp.target_damsire and dsd.kyori_band = tp.kyori_band
    left join damsire_track_stats dst on dst.damsire = tp.target_damsire and dst.surface = tp.surface
    left join race_field_aggregates rfa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join race_top3_speed rts using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join track_bias tb using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join weight_agg wa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join recent_form rf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join legacy_features lf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join weather_lookup wl using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    """


def write_parquet(
    con: duckdb.DuckDBPyConnection,
    final_query: str,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        copy ({final_query})
        to '{output_dir.as_posix()}'
        (format parquet, partition_by (race_year), overwrite_or_ignore true)
        """
    )


class BuildResult(TypedDict):
    elapsed_seconds: float
    output_dir: str
    rows_written: int


def count_output_rows(output_dir: Path) -> int:
    parquet_files = list(output_dir.glob("race_year=*/data_0.parquet"))
    if not parquet_files:
        return 0
    counter = duckdb.connect(":memory:")
    counter.execute(
        f"create view all_data as select * from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')"
    )
    result = counter.execute("select count(*) from all_data").fetchone()
    counter.close()
    return int(result[0]) if result is not None else 0


def run(args: argparse.Namespace) -> BuildResult:
    pg_url = resolve_pg_url(args.pg_url)
    started = perf_counter()
    con = duckdb.connect(":memory:")
    con.execute(f"set threads to {int(args.threads)}")
    con.execute(f"set memory_limit = '{args.memory_limit}'")
    install_and_attach_pg(con, pg_url)
    stage_source_tables(con, args.from_date, args.to_date)
    build_target_table(con, args.category, args.from_date, args.to_date)
    final_query = assemble_final_query(args.category)
    write_parquet(con, final_query, args.output_dir)
    rows = count_output_rows(args.output_dir)
    con.close()
    return {
        "elapsed_seconds": perf_counter() - started,
        "output_dir": args.output_dir.as_posix(),
        "rows_written": rows,
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
