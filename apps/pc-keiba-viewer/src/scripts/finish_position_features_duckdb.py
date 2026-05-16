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
import resource
import shutil
import sys
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import TypedDict, final

import duckdb

DEFAULT_OUTPUT_DIR = Path("tmp/finish-position-features-parquet")
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@localhost:5432/horse_racing"
DEFAULT_THREADS = 8
DEFAULT_MEMORY_LIMIT = "8GB"
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0
BYTES_PER_MB = 1024 * 1024

RECENT_WINDOW_SIZE = 5
SAME_DISTANCE_TOLERANCE = 200
HISTORY_LOOKBACK_YEARS = 10
CONSECUTIVE_RACE_WINDOW_DAYS = 30
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
    force_clean_output: bool
    from_date: str
    heartbeat_interval: float
    keep_existing_output: bool
    output_dir: Path
    pg_url: str
    skip_count: bool
    status_file: Path | None
    threads: int
    to_date: str


def non_negative_float(raw: str) -> float:
    value = float(raw)
    if value < 0:
        raise argparse.ArgumentTypeError(f"value must be >= 0, got {raw}")
    return value


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
    parser.add_argument("--status-file", type=Path, default=None)
    parser.add_argument(
        "--heartbeat-interval",
        type=non_negative_float,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    )
    parser.add_argument("--skip-count", action="store_true")
    parser.add_argument("--keep-existing-output", action="store_true")
    parser.add_argument("--force-clean-output", action="store_true")
    return parser.parse_args(argv)


def compute_history_start(from_date: str, years_back: int) -> str:
    year = int(from_date[:4])
    rest = from_date[4:]
    return f"{year - years_back:04d}{rest}"


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
    history_start = compute_history_start(from_date, HISTORY_LOOKBACK_YEARS)
    con.execute(
        f"""
        create or replace temp table rec as
        select
          source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu, finish_position, finish_norm,
          time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds
        from pg.race_entry_corner_features
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
    con.execute(
        """
        create or replace temp table jra_um as
        select ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b from pg.jvd_um
        """
    )
    con.execute(
        """
        create or replace temp table nar_um as
        select ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b from pg.nvd_um
        """
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
          rec.race_dt,
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
        max(target_race_dt) - max(history_race_dt) filter (where recent_rank = 1) as days_since_last_race,
        count(*) filter (where target_race_dt - history_race_dt <= {CONSECUTIVE_RACE_WINDOW_DAYS}) as consecutive_race_count
      from horse_history_base
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    """


def partner_history_cte(
    alias: str,
    partner_column: str,
    agg_alias: str,
    target_filter: str = "true",
) -> str:
    return f"""
    {alias} as (
      select
        t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        t.race_dt as target_race_dt,
        t.keibajo_code as target_keibajo, t.kyori as target_kyori,
        t.track_code as target_track_code, t.grade_code as target_grade_code,
        t.ketto_toroku_bango as target_horse,
        h.finish_position,
        h.race_dt as history_race_dt,
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
        and h.race_dt >= t.race_dt - interval '{HISTORY_LOOKBACK_YEARS} years'
      where h.finish_position is not null and t.{partner_column} is not null
        and ({target_filter})
    ),
    {agg_alias} as (
      select
        source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        {{aggregations}}
      from {alias}
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    """


def jockey_cte(target_filter: str = "true") -> str:
    template = partner_history_cte(
        "jockey_history", "kishumei_ryakusho", "jockey_career", target_filter
    )
    aggregations = f"""
        avg(case when finish_position = 1 then 1 else 0 end) as jockey_career_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_race_dt >= target_race_dt - {JOCKEY_RECENT_DAYS}) as jockey_recent_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as jockey_keibajo_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE}) as jockey_distance_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)) as jockey_track_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, '')) as jockey_grade_win_rate,
        count(*) filter (where history_horse = target_horse) as jockey_horse_pair_count,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as jockey_horse_pair_win_rate
    """
    return template.replace("{aggregations}", aggregations)


def trainer_cte(target_filter: str = "true") -> str:
    template = partner_history_cte(
        "trainer_history", "chokyoshimei_ryakusho", "trainer_career", target_filter
    )
    aggregations = f"""
        avg(case when finish_position = 1 then 1 else 0 end) as trainer_career_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as trainer_keibajo_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE}) as trainer_distance_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as trainer_horse_win_rate
    """
    return template.replace("{aggregations}", aggregations)


def pedigree_rec_um_subquery(category: str) -> str:
    if category == "jra":
        return (
            "select rec.*, um.ketto_joho_01b, um.ketto_joho_05b"
            " from rec join jra_um um using (ketto_toroku_bango)"
            " where rec.source = 'jra'"
        )
    if category == "nar":
        return (
            "select rec.*, um.ketto_joho_01b, um.ketto_joho_05b"
            " from rec join nar_um um using (ketto_toroku_bango)"
            " where rec.source = 'nar' and rec.keibajo_code <> '83'"
        )
    if category == "ban-ei":
        return (
            "select rec.*, um.ketto_joho_01b, um.ketto_joho_05b"
            " from rec join nar_um um using (ketto_toroku_bango)"
            " where rec.source = 'nar' and rec.keibajo_code = '83'"
        )
    return (
        "select rec.*, um.ketto_joho_01b, um.ketto_joho_05b"
        " from rec join jra_um um using (ketto_toroku_bango)"
        " where rec.source = 'jra'"
        " union all"
        " select rec.*, um.ketto_joho_01b, um.ketto_joho_05b"
        " from rec join nar_um um using (ketto_toroku_bango)"
        " where rec.source = 'nar'"
    )


class PedigreeStatSpec(TypedDict):
    table: str
    key_column: str
    key_alias: str
    bucket_expr: str
    bucket_alias: str
    monthly_metrics_select: str
    accum_metrics_select: str


PEDIGREE_STAT_SPECS: list[PedigreeStatSpec] = [
    {
        "table": "sire_distance_stats",
        "key_column": "ketto_joho_01b",
        "key_alias": "sire",
        "bucket_expr": f"cast(coalesce(kyori, 0) as int) / {DISTANCE_BAND_METERS}",
        "bucket_alias": "kyori_band",
        "monthly_metrics_select": (
            "sum(case when finish_position = 1 then 1 else 0 end) as win_count,"
            " sum(finish_norm) as finish_norm_sum"
        ),
        "accum_metrics_select": (
            "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as sire_distance_win_rate_val,"
            " sum(m.finish_norm_sum)::double / nullif(sum(m.race_count), 0)"
            " as sire_avg_finish_at_distance_val"
        ),
    },
    {
        "table": "sire_track_stats",
        "key_column": "ketto_joho_01b",
        "key_alias": "sire",
        "bucket_expr": "left(coalesce(track_code, ''), 1)",
        "bucket_alias": "surface",
        "monthly_metrics_select": "sum(case when finish_position = 1 then 1 else 0 end) as win_count",
        "accum_metrics_select": (
            "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as sire_track_win_rate_val"
        ),
    },
    {
        "table": "damsire_distance_stats",
        "key_column": "ketto_joho_05b",
        "key_alias": "damsire",
        "bucket_expr": f"cast(coalesce(kyori, 0) as int) / {DISTANCE_BAND_METERS}",
        "bucket_alias": "kyori_band",
        "monthly_metrics_select": "sum(case when finish_position = 1 then 1 else 0 end) as win_count",
        "accum_metrics_select": (
            "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as dam_sire_distance_win_rate_val"
        ),
    },
    {
        "table": "damsire_track_stats",
        "key_column": "ketto_joho_05b",
        "key_alias": "damsire",
        "bucket_expr": "left(coalesce(track_code, ''), 1)",
        "bucket_alias": "surface",
        "monthly_metrics_select": "sum(finish_norm) as finish_norm_sum",
        "accum_metrics_select": (
            "sum(m.finish_norm_sum)::double / nullif(sum(m.race_count), 0)"
            " as damsire_avg_finish_at_track_val"
        ),
    },
]


def pedigree_monthly_stat_sql(spec: PedigreeStatSpec, rec_um_subquery: str) -> str:
    return f"""
    create or replace temp table {spec["table"]} as
    with rec_um as ({rec_um_subquery}),
    monthly as (
      select
        cast(substr(race_date, 1, 4) as int) * 100 + cast(substr(race_date, 5, 2) as int) as race_year_month,
        {spec["key_column"]} as {spec["key_alias"]},
        {spec["bucket_expr"]} as {spec["bucket_alias"]},
        {spec["monthly_metrics_select"]},
        count(*) as race_count
      from rec_um
      where finish_position is not null
        and {spec["key_column"]} is not null and trim({spec["key_column"]}) <> ''
      group by 1, 2, 3
    )
    select
      tm.stats_year_month,
      m.{spec["key_alias"]},
      m.{spec["bucket_alias"]},
      {spec["accum_metrics_select"]},
      sum(m.race_count) as race_count
    from target_months tm
    join monthly m on m.race_year_month < tm.stats_year_month
    group by tm.stats_year_month, m.{spec["key_alias"]}, m.{spec["bucket_alias"]}
    """


def target_pedigree_sql() -> str:
    return f"""
    create or replace temp table target_pedigree as
    select
      t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
      cast(coalesce(t.kyori, 0) as int) / {DISTANCE_BAND_METERS} as kyori_band,
      left(coalesce(t.track_code, ''), 1) as surface,
      coalesce(j_um.ketto_joho_01b, n_um.ketto_joho_01b) as target_sire,
      coalesce(j_um.ketto_joho_05b, n_um.ketto_joho_05b) as target_damsire
    from target t
    left join jra_um j_um on t.source = 'jra' and j_um.ketto_toroku_bango = t.ketto_toroku_bango
    left join nar_um n_um on t.source = 'nar' and n_um.ketto_toroku_bango = t.ketto_toroku_bango
    """


def target_months_sql() -> str:
    return (
        "create or replace temp table target_months as"
        " select distinct cast(kaisai_nen as int) * 100"
        " + cast(substr(kaisai_tsukihi, 1, 2) as int) as stats_year_month"
        " from target order by 1"
    )


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


def track_bias_cte(target_filter: str = "true") -> str:
    return f"""
    track_bias as (
      select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        avg(case when h.finish_position = 1 and h.umaban * 2 <= h.shusso_tosu + 1 then 1 else 0 end) as track_bias_inside,
        avg(case when h.finish_position = 1 and cast(h.corner1_norm as double) <= {FRONT_CORNER_THRESHOLD} then 1 else 0 end) as track_bias_front
      from target t
      left join rec h
        on h.source = t.source and h.keibajo_code = t.keibajo_code
        and h.race_date < t.race_date
        and h.race_dt >= t.race_dt - {TRACK_BIAS_WINDOW_DAYS}
        and h.finish_position is not null
      where ({target_filter})
      group by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
    )
    """


def weight_cte(target_filter: str = "true") -> str:
    return f"""
    target_weight as (
      select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
        coalesce(cast(j.bataiju as int), cast(n.bataiju as int)) as current_bataiju
      from target t
      left join jra_se j on t.source='jra' and j.kaisai_nen=t.kaisai_nen and j.kaisai_tsukihi=t.kaisai_tsukihi
        and j.keibajo_code=t.keibajo_code and j.race_bango=t.race_bango and j.ketto_toroku_bango=t.ketto_toroku_bango
      left join nar_se n on t.source='nar' and n.kaisai_nen=t.kaisai_nen and n.kaisai_tsukihi=t.kaisai_tsukihi
        and n.keibajo_code=t.keibajo_code and n.race_bango=t.race_bango and n.ketto_toroku_bango=t.ketto_toroku_bango
      where ({target_filter})
    ),
    weight_history as (
      select
        b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango,
        tw.current_bataiju,
        coalesce(cast(hj.bataiju as int), cast(hn.bataiju as int)) as history_bataiju,
        b.recent_rank
      from horse_history_base b
      join target_weight tw using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
      left join jra_se hj on b.source='jra' and hj.kaisai_nen=b.history_kaisai_nen
        and hj.kaisai_tsukihi=b.history_kaisai_tsukihi and hj.keibajo_code=b.history_keibajo
        and hj.race_bango=b.history_race_bango and hj.ketto_toroku_bango=b.ketto_toroku_bango
      left join nar_se hn on b.source='nar' and hn.kaisai_nen=b.history_kaisai_nen
        and hn.kaisai_tsukihi=b.history_kaisai_tsukihi and hn.keibajo_code=b.history_keibajo
        and hn.race_bango=b.history_race_bango and hn.ketto_toroku_bango=b.ketto_toroku_bango
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
    return f"""
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
      from horse_history_base
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    """


def legacy_five_cte(target_filter: str = "true") -> str:
    return f"""
    legacy_horse_avg as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
        avg(finish_norm) as avg_finish,
        avg(finish_norm) filter (where recent_rank <= {RECENT_WINDOW_SIZE}) as recent_finish
      from horse_history_base
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
      where ({target_filter})
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


PEDIGREE_STAT_TABLES = tuple(spec["table"] for spec in PEDIGREE_STAT_SPECS)


def materialize_pedigree_stats(
    con: duckdb.DuckDBPyConnection,
    category: str,
) -> None:
    log_event("pedigree.target_pedigree", "start", 0.0)
    started = perf_counter()
    con.execute(target_pedigree_sql())
    log_event("pedigree.target_pedigree", "done", perf_counter() - started)
    log_event("pedigree.target_months", "start", 0.0)
    started = perf_counter()
    con.execute(target_months_sql())
    log_event("pedigree.target_months", "done", perf_counter() - started)
    rec_um_subquery = pedigree_rec_um_subquery(category)
    for spec in PEDIGREE_STAT_SPECS:
        stage = f"pedigree.{spec['table']}"
        log_event(stage, "start", 0.0)
        started = perf_counter()
        con.execute(pedigree_monthly_stat_sql(spec, rec_um_subquery))
        log_event(stage, "done", perf_counter() - started)


def materialize_race_context(con: duckdb.DuckDBPyConnection) -> None:
    cte_text = race_context_cte()
    materialize_temp_table(
        con,
        "race_field_aggregates",
        "race_field_aggregates",
        cte_text,
        "race_field_aggregates",
    )
    materialize_temp_table(con, "race_top3_speed", "race_top3_speed", cte_text, "race_top3_speed")


def materialize_legacy_features(con: duckdb.DuckDBPyConnection) -> None:
    cte_text = legacy_five_cte()
    materialize_temp_table(con, "legacy_features", "legacy_features", cte_text, "legacy_features")


def materialize_weather_lookup(con: duckdb.DuckDBPyConnection) -> None:
    log_event("weather.weather_lookup", "start", 0.0)
    started = perf_counter()
    con.execute(
        """
        create or replace temp table weather_lookup as
        select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
          coalesce(jr.tenko_code, nr.tenko_code) as tenko_code
        from target t
        left join jra_ra jr on t.source='jra' and jr.kaisai_nen=t.kaisai_nen and jr.kaisai_tsukihi=t.kaisai_tsukihi
          and jr.keibajo_code=t.keibajo_code and jr.race_bango=t.race_bango
        left join nar_ra nr on t.source='nar' and nr.kaisai_nen=t.kaisai_nen and nr.kaisai_tsukihi=t.kaisai_tsukihi
          and nr.keibajo_code=t.keibajo_code and nr.race_bango=t.race_bango
        """
    )
    log_event("weather.weather_lookup", "done", perf_counter() - started)


def assemble_final_select_from_temp_tables(category: str) -> str:
    return f"""
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
    left join sire_distance_stats sds on sds.sire = tp.target_sire and sds.kyori_band = tp.kyori_band and sds.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
    left join sire_track_stats sts on sts.sire = tp.target_sire and sts.surface = tp.surface and sts.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
    left join damsire_distance_stats dsd on dsd.damsire = tp.target_damsire and dsd.kyori_band = tp.kyori_band and dsd.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
    left join damsire_track_stats dst on dst.damsire = tp.target_damsire and dst.surface = tp.surface and dst.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
    left join race_field_aggregates rfa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join race_top3_speed rts using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join track_bias tb using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join weight_agg wa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join recent_form rf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join legacy_features lf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join weather_lookup wl using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    """


PARTITION_DIR_PATTERN = "race_year=*"


def directory_only_contains_partitions(output_dir: Path) -> bool:
    for entry in output_dir.iterdir():
        if entry.is_dir() and entry.match(PARTITION_DIR_PATTERN):
            continue
        return False
    return True


def prepare_output_dir(output_dir: Path, keep_existing: bool, force_clean: bool) -> None:
    if keep_existing or not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        return
    if not force_clean and not directory_only_contains_partitions(output_dir):
        raise ValueError(
            f"refusing to clean {output_dir}: contains entries outside the "
            f"'{PARTITION_DIR_PATTERN}' pattern; pass --force-clean-output to override"
        )
    shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def write_parquet(
    con: duckdb.DuckDBPyConnection,
    final_query: str,
    output_dir: Path,
    keep_existing: bool,
    force_clean: bool,
) -> None:
    prepare_output_dir(output_dir, keep_existing, force_clean)
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


def log_event(stage: str, status: str, elapsed_seconds: float, rows: int | None = None) -> None:
    payload: dict[str, object] = {
        "stage": stage,
        "status": status,
        "elapsed_seconds": round(elapsed_seconds, 2),
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }
    if rows is not None:
        payload["rows"] = rows
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def materialize_temp_table(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    temp_name: str,
    cte_text: str,
    final_cte: str,
) -> int:
    log_event(stage, "start", 0.0)
    started = perf_counter()
    sql = f"create or replace temp table {temp_name} as with {cte_text} select * from {final_cte}"
    con.execute(sql)
    row_result = con.execute(f"select count(*) from {temp_name}").fetchone()
    row_count = int(row_result[0]) if row_result is not None else 0
    elapsed = perf_counter() - started
    log_event(stage, "done", elapsed, row_count)
    return row_count


def write_status_atomic(path: Path | None, payload: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    tmp_path.replace(path)


def read_runtime_snapshot() -> tuple[float, float]:
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    cpu_total = rusage.ru_utime + rusage.ru_stime
    return cpu_total, perf_counter()


def compute_runtime_stats(
    prev_cpu_seconds: float,
    prev_wall_seconds: float,
) -> tuple[dict[str, float], float, float]:
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    cpu_total = rusage.ru_utime + rusage.ru_stime
    wall_now = perf_counter()
    is_macos = sys.platform == "darwin"
    rss_bytes = rusage.ru_maxrss if is_macos else rusage.ru_maxrss * 1024
    delta_wall = wall_now - prev_wall_seconds
    delta_cpu = cpu_total - prev_cpu_seconds
    cpu_percent = round(delta_cpu / delta_wall * 100, 1) if delta_wall > 0 else 0.0
    stats: dict[str, float] = {
        "rss_mb": round(rss_bytes / BYTES_PER_MB, 1),
        "cpu_percent": cpu_percent,
        "cpu_user_seconds": round(rusage.ru_utime, 1),
        "cpu_sys_seconds": round(rusage.ru_stime, 1),
    }
    return stats, cpu_total, wall_now


@final
class Heartbeat:
    def __init__(self, interval_seconds: float, status_path: Path | None) -> None:
        self.interval = interval_seconds
        self.status_path = status_path
        self.stage = "starting"
        self.substage = ""
        self.stage_started_at = perf_counter()
        self.overall_started_at = perf_counter()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._last_cpu, self._last_wall = read_runtime_snapshot()

    def start(self) -> None:
        if self.interval > 0:
            self._thread.start()

    def stop(self) -> None:
        if self.interval > 0:
            self._stop_event.set()
            self._thread.join(timeout=2.0)
        self._emit("stop")

    def set_stage(self, stage: str) -> None:
        self.stage = stage
        self.substage = ""
        self.stage_started_at = perf_counter()
        self._emit("stage_change")

    def set_substage(self, substage: str) -> None:
        self.substage = substage
        self._emit("substage_change")

    def _loop(self) -> None:
        while not self._stop_event.wait(self.interval):
            self._emit("tick")

    def _emit(self, kind: str) -> None:
        stats, self._last_cpu, self._last_wall = compute_runtime_stats(
            self._last_cpu, self._last_wall
        )
        payload: dict[str, object] = {
            "type": "heartbeat",
            "kind": kind,
            "stage": self.stage,
            "substage": self.substage,
            "stage_elapsed_seconds": round(perf_counter() - self.stage_started_at, 1),
            "overall_elapsed_seconds": round(perf_counter() - self.overall_started_at, 1),
            "timestamp": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        }
        payload.update(stats)
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        write_status_atomic(self.status_path, payload)


def get_target_years(con: duckdb.DuckDBPyConnection) -> list[int]:
    rows = con.execute("select distinct race_year from target order by race_year").fetchall()
    return [int(row[0]) for row in rows]


def materialize_temp_table_by_year(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    temp_name: str,
    cte_builder: Callable[[str], str],
    final_cte: str,
    years: list[int],
    heartbeat: Heartbeat,
) -> int:
    log_event(stage, "start", 0.0)
    overall_start = perf_counter()
    for idx, year in enumerate(years):
        heartbeat.set_substage(f"year={year}")
        year_start = perf_counter()
        filter_clause = f"t.kaisai_nen = '{year:04d}'"
        cte_text = cte_builder(filter_clause)
        if idx == 0:
            con.execute(
                f"create or replace temp table {temp_name} as with {cte_text} select * from {final_cte}"
            )
        else:
            con.execute(
                f"insert into {temp_name} with {cte_text} select * from {final_cte}"
            )
        log_event(f"{stage}.year{year}", "done", perf_counter() - year_start)
    row_result = con.execute(f"select count(*) from {temp_name}").fetchone()
    total_rows = int(row_result[0]) if row_result is not None else 0
    log_event(stage, "done", perf_counter() - overall_start, total_rows)
    return total_rows


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


HORSE_HISTORY_BASE_SELECT = """
    select
      t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
      t.race_dt as target_race_dt,
      t.keibajo_code as target_keibajo, t.kyori as target_kyori,
      t.track_code as target_track_code, t.grade_code as target_grade_code,
      (case t.kyoso_joken_code
        when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
        when '701' then 4 when '703' then 5 when '999' then 6
        else null end) as target_class_level,
      h.kaisai_nen as history_kaisai_nen,
      h.kaisai_tsukihi as history_kaisai_tsukihi,
      h.keibajo_code as history_keibajo,
      h.race_bango as history_race_bango,
      h.race_dt as history_race_dt,
      h.finish_position,
      cast(h.finish_norm as double) as finish_norm,
      cast(h.time_sa as double) as time_sa,
      cast(h.kohan_3f as double) as kohan_3f,
      cast(h.corner3_norm as double) as corner3_norm,
      cast(h.corner4_norm as double) as corner4_norm,
      h.kyori as history_kyori,
      h.track_code as history_track_code,
      h.grade_code as history_grade_code,
      (case h.kyoso_joken_code
        when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
        when '701' then 4 when '703' then 5 when '999' then 6
        else null end) as history_class_level,
      row_number() over (
        partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
        order by h.race_date desc
      ) as recent_rank
"""


def materialize_horse_history_base(con: duckdb.DuckDBPyConnection, target_filter: str) -> int:
    con.execute(
        f"""
        create or replace temp table horse_history_base as
        {HORSE_HISTORY_BASE_SELECT}
        from target t
        join rec h
          on h.source = t.source
          and h.ketto_toroku_bango = t.ketto_toroku_bango
          and h.race_date < t.race_date
          and h.race_dt >= t.race_dt - interval '{HISTORY_LOOKBACK_YEARS} years'
        where h.finish_position is not null and ({target_filter})
        """
    )
    row_result = con.execute("select count(*) from horse_history_base").fetchone()
    return int(row_result[0]) if row_result is not None else 0


class DerivedStageSpec(TypedDict):
    name: str
    cte_builder: Callable[[str], str]
    final_cte: str


def execute_derived_stage(
    con: duckdb.DuckDBPyConnection,
    spec: DerivedStageSpec,
    target_filter: str,
    is_first_year: bool,
) -> None:
    cte_text = spec["cte_builder"](target_filter)
    final_cte = spec["final_cte"]
    if is_first_year:
        con.execute(
            f"create or replace temp table {final_cte} as with {cte_text} select * from {final_cte}"
        )
    else:
        con.execute(
            f"insert into {final_cte} with {cte_text} select * from {final_cte}"
        )


def stage_horse_history_derived(
    con: duckdb.DuckDBPyConnection,
    years: list[int],
    heartbeat: Heartbeat,
) -> None:
    specs: list[DerivedStageSpec] = [
        {"name": "horse_career", "cte_builder": lambda _: horse_career_cte(), "final_cte": "horse_career"},
        {"name": "recent_form", "cte_builder": lambda _: recent_form_cte(), "final_cte": "recent_form"},
        {"name": "weight_agg", "cte_builder": weight_cte, "final_cte": "weight_agg"},
        {"name": "legacy_features", "cte_builder": legacy_five_cte, "final_cte": "legacy_features"},
    ]
    log_event("horse_history_derived", "start", 0.0)
    overall_start = perf_counter()
    for idx, year in enumerate(years):
        heartbeat.set_substage(f"year={year}")
        year_filter = f"t.kaisai_nen = '{year:04d}'"
        base_start = perf_counter()
        base_rows = materialize_horse_history_base(con, year_filter)
        log_event(f"horse_history_base.year{year}", "done", perf_counter() - base_start, base_rows)
        for spec in specs:
            stage_start = perf_counter()
            execute_derived_stage(con, spec, year_filter, idx == 0)
            log_event(f"{spec['name']}.year{year}", "done", perf_counter() - stage_start)
    for spec in specs:
        row_result = con.execute(f"select count(*) from {spec['final_cte']}").fetchone()
        rows = int(row_result[0]) if row_result is not None else 0
        log_event(f"{spec['name']}.total", "done", 0.0, rows)
    log_event("horse_history_derived", "done", perf_counter() - overall_start)


def configure_duckdb_session(con: duckdb.DuckDBPyConnection, threads: int, memory_limit: str) -> None:
    con.execute(f"set threads to {int(threads)}")
    con.execute(f"set memory_limit = '{memory_limit}'")
    try:
        con.execute("PRAGMA enable_object_cache=true")
    except duckdb.Error:
        pass
    try:
        con.execute("SET enable_progress_bar_print = false")
    except duckdb.Error:
        pass


def stage_source(con: duckdb.DuckDBPyConnection, pg_url: str, from_date: str, to_date: str) -> None:
    log_event("source.stage", "start", 0.0)
    started = perf_counter()
    install_and_attach_pg(con, pg_url)
    stage_source_tables(con, from_date, to_date)
    log_event("source.stage", "done", perf_counter() - started)


def stage_target(con: duckdb.DuckDBPyConnection, category: str, from_date: str, to_date: str) -> int:
    log_event("target.build", "start", 0.0)
    started = perf_counter()
    build_target_table(con, category, from_date, to_date)
    target_row_result = con.execute("select count(*) from target").fetchone()
    target_rows = int(target_row_result[0]) if target_row_result is not None else 0
    log_event("target.build", "done", perf_counter() - started, target_rows)
    return target_rows


def stage_partner_features(
    con: duckdb.DuckDBPyConnection,
    years: list[int],
    heartbeat: Heartbeat,
) -> None:
    heartbeat.set_stage("jockey_career")
    materialize_temp_table_by_year(
        con, "jockey_career", "jockey_career", jockey_cte, "jockey_career", years, heartbeat,
    )
    heartbeat.set_stage("trainer_career")
    materialize_temp_table_by_year(
        con, "trainer_career", "trainer_career", trainer_cte, "trainer_career", years, heartbeat,
    )


def stage_track_bias(
    con: duckdb.DuckDBPyConnection,
    years: list[int],
    heartbeat: Heartbeat,
) -> None:
    heartbeat.set_stage("track_bias")
    materialize_temp_table_by_year(
        con, "track_bias", "track_bias", track_bias_cte, "track_bias", years, heartbeat,
    )


def resolve_output_rows(args: argparse.Namespace, target_rows: int) -> int:
    if args.skip_count:
        return target_rows
    return count_output_rows(args.output_dir)


def stage_parquet_write(
    con: duckdb.DuckDBPyConnection,
    category: str,
    output_dir: Path,
    keep_existing: bool,
    force_clean: bool,
) -> None:
    log_event("parquet.write", "start", 0.0)
    started = perf_counter()
    write_parquet(
        con,
        assemble_final_select_from_temp_tables(category),
        output_dir,
        keep_existing,
        force_clean,
    )
    log_event("parquet.write", "done", perf_counter() - started)


def build_empty_result(output_dir: Path, elapsed: float) -> BuildResult:
    return {
        "elapsed_seconds": elapsed,
        "output_dir": output_dir.as_posix(),
        "rows_written": 0,
    }


def run(args: argparse.Namespace) -> BuildResult:
    pg_url = resolve_pg_url(args.pg_url)
    overall_started = perf_counter()
    log_event("run", "start", 0.0)
    heartbeat = Heartbeat(args.heartbeat_interval, args.status_file)
    heartbeat.start()
    con = duckdb.connect(":memory:")
    try:
        configure_duckdb_session(con, args.threads, args.memory_limit)
        heartbeat.set_stage("source.stage")
        stage_source(con, pg_url, args.from_date, args.to_date)
        heartbeat.set_stage("target.build")
        target_rows = stage_target(con, args.category, args.from_date, args.to_date)
        years = get_target_years(con)
        log_event("target.years", "done", 0.0, len(years))
        if not years:
            prepare_output_dir(args.output_dir, args.keep_existing_output, args.force_clean_output)
            log_event("run", "skip", perf_counter() - overall_started, 0)
            return build_empty_result(args.output_dir, perf_counter() - overall_started)
        heartbeat.set_stage("horse_history_derived")
        stage_horse_history_derived(con, years, heartbeat)
        stage_partner_features(con, years, heartbeat)
        heartbeat.set_stage("pedigree")
        materialize_pedigree_stats(con, args.category)
        heartbeat.set_stage("race_context")
        materialize_race_context(con)
        stage_track_bias(con, years, heartbeat)
        heartbeat.set_stage("weather_lookup")
        materialize_weather_lookup(con)
        heartbeat.set_stage("parquet.write")
        stage_parquet_write(
            con,
            args.category,
            args.output_dir,
            args.keep_existing_output,
            args.force_clean_output,
        )
        rows = resolve_output_rows(args, target_rows)
        elapsed = perf_counter() - overall_started
        heartbeat.set_stage("done")
        log_event("run", "done", elapsed, rows)
        return {
            "elapsed_seconds": elapsed,
            "output_dir": args.output_dir.as_posix(),
            "rows_written": rows,
        }
    finally:
        heartbeat.stop()
        con.close()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
