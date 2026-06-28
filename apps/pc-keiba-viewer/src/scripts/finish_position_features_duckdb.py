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
import hashlib
import json
import os
import resource
import shutil
import sys
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import TextIO, TypedDict, final

import duckdb

DEFAULT_OUTPUT_DIR = Path("tmp/finish-position-features-parquet")
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@localhost:5432/horse_racing"
sys.path.insert(0, str(Path(__file__).parent / "finish-position-features"))


def _load_resource_defaults() -> tuple[Callable[[], int], Callable[[], str]]:
    import importlib

    module = importlib.import_module("_resource_defaults")
    return module.default_threads, module.default_memory_limit


_default_threads, _default_memory_limit = _load_resource_defaults()
DEFAULT_THREADS: int = _default_threads()
DEFAULT_MEMORY_LIMIT: str = _default_memory_limit()
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0
BYTES_PER_MB = 1024 * 1024

RECENT_WINDOW_SIZE = 5
SAME_DISTANCE_TOLERANCE = 200
HISTORY_LOOKBACK_YEARS = 10
CONSECUTIVE_RACE_WINDOW_DAYS = 30
JOCKEY_RECENT_DAYS = 60
TRACK_BIAS_WINDOW_DAYS = 5
# Target years processed per pass in materialize_temp_table_by_year. >1 cuts
# full-rec rescans; kept small so the 10yr-lookback intermediate stays under 6GB.
PARTNER_CAREER_YEAR_BATCH_SIZE = 2
FRONT_CORNER_THRESHOLD = 0.33
RIVAL_DISTANCE_THRESHOLD = 0.3
MAX_FIELD_SIZE = 18
DISTANCE_BAND_METERS = 400
PEDIGREE_MIN_RACES = 5
PEDIGREE_COMPOSITE_DIVISOR = 3
TREND_MIN_RACES = 3
# regr_slope returns NaN with <2 points (zero variance in x); require >=2.
WEIGHT_TREND_MIN_RACES = 2
# Floor volatility (kg) so near-zero spread does not blow up the z-score.
WEIGHT_ZSCORE_MIN_VOLATILITY = 1.0
WEIGHT_ZSCORE_CLAMP = 5.0
RUNNING_STYLE_SENKOU_THRESHOLD = 0.30
RUNNING_STYLE_SASHI_THRESHOLD = 0.70
RUNNING_STYLE_CLASS_NIGE = 0
RUNNING_STYLE_CLASS_SENKOU = 1
RUNNING_STYLE_CLASS_SASHI = 2
RUNNING_STYLE_CLASS_OIKOMI = 3
KYORI_BAND_SPRINT_MAX = 1300
KYORI_BAND_MILE_MAX = 1700
KYORI_BAND_INTERMEDIATE_MAX = 2200
KYORI_BAND_SPRINT = 0
KYORI_BAND_MILE = 1
KYORI_BAND_INTERMEDIATE = 2
KYORI_BAND_LONG = 3
SEASON_SPRING_MAX_MONTH = 5
SEASON_SUMMER_MAX_MONTH = 8
SEASON_AUTUMN_MAX_MONTH = 11
SEASON_SPRING = 0
SEASON_SUMMER = 1
SEASON_AUTUMN = 2
SEASON_WINTER = 3
NEWCOMER_RACE_JOKEN_CODE = "000"
UMABAN_NORM_MIN_FIELD = 2

# Race-hour window (inclusive) used to aggregate the per-venue hourly weather
# into a single daily value that overlaps actual race times.
VENUE_WEATHER_RACE_HOUR_MIN = 9
VENUE_WEATHER_RACE_HOUR_MAX = 17

# Empirical training-set medians for popularity_score and odds_score (derived
# from feat-jra-v7-final / feat-nar-v7-baba training parquets, finish_position
# IS NOT NULL rows only).  Used as COALESCE fallback at inference time when
# realtime odds are not yet available (UPCOMING races), so the model receives
# the median rather than NULL.
POPULARITY_SCORE_MEDIAN_JRA: float = 0.5000
POPULARITY_SCORE_MEDIAN_NAR: float = 0.5000
ODDS_SCORE_MEDIAN_JRA: float = 0.5664
ODDS_SCORE_MEDIAN_NAR: float = 0.5048

# NAR per-class routing labels. The NAR JV feed reports kyoso_joken_code='000' for
# every race, so the actual class signal lives in the free-text meisho field
# (kyoso_joken_meisho, e.g. "「　　　Ｃ２　」"). nar_subclass derives a clean
# routing label via regex on meisho. NULL for JRA + Ban-ei rows.
NAR_SUBCLASS_OP = "OP"
NAR_SUBCLASS_NEW = "NEW"
NAR_SUBCLASS_MUKATSU = "MUKATSU"
NAR_SUBCLASS_A = "A"
NAR_SUBCLASS_B = "B"
NAR_SUBCLASS_C = "C"
NAR_SUBCLASS_2YO = "2YO"
NAR_SUBCLASS_3YO = "3YO"
NAR_SUBCLASS_OTHER = "other"
NAR_NAMED_CLASSES: tuple[str, ...] = (
    NAR_SUBCLASS_OP,
    NAR_SUBCLASS_NEW,
    NAR_SUBCLASS_MUKATSU,
    NAR_SUBCLASS_A,
    NAR_SUBCLASS_B,
    NAR_SUBCLASS_C,
    NAR_SUBCLASS_2YO,
    NAR_SUBCLASS_3YO,
    NAR_SUBCLASS_OTHER,
)

# JRA central-venue keibajo codes (01 札幌 .. 10 小倉). The JRA-VAN feed
# (jvd_se / jvd_ra) also distributes NAR-venue (keibajo 30-58, data_kubun 'A')
# and overseas (data_kubun 'B') races, all stamped source='jra' by the
# corner-feature table. A JRA-only model must restrict TARGET rows to these
# central venues so NAR / overseas races do not leak in as fake JRA targets.
JRA_KEIBAJO_CODES: tuple[str, ...] = (
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
)
JRA_KEIBAJO_CODES_SQL = "(" + ", ".join(f"'{code}'" for code in JRA_KEIBAJO_CODES) + ")"


DATE_FORMAT = "%Y%m%d"


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
    temp_dir: Path | None
    threads: int
    to_date: str


def non_negative_float(raw: str) -> float:
    value = float(raw)
    if value < 0:
        raise argparse.ArgumentTypeError(f"value must be >= 0, got {raw}")
    return value


def non_negative_int(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError(f"value must be >= 0, got {raw}")
    return value


def target_date_arg(raw: str) -> str:
    try:
        datetime.strptime(raw, DATE_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"--target-date must be YYYYMMDD, got {raw}"
        ) from error
    return raw


def target_race_arg(raw: str) -> tuple[str, str]:
    parts = raw.split(":")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise argparse.ArgumentTypeError(
            f"--target-race must be keibajo_code:race_bango, got {raw}"
        )
    return parts[0], parts[1]


def add_days(date_yyyymmdd: str, days: int) -> str:
    base = datetime.strptime(date_yyyymmdd, DATE_FORMAT).replace(tzinfo=timezone.utc)
    return (base + timedelta(days=days)).strftime(DATE_FORMAT)


def resolve_date_range(args: argparse.Namespace) -> tuple[str, str]:
    """Resolve the (from_date, to_date) target window.

    When ``--target-date`` is provided the target window becomes
    [target_date, target_date + days_ahead] so the build emits feature rows for
    that day's races (including UPCOMING races whose ``finish_position`` is still
    NULL). Historical aggregates are always computed from
    ``compute_history_start`` BEFORE ``from_date`` via the existing
    ``h.race_date < t.race_date`` joins, so no target-race outcome leaks in and
    the window is computable even before the target race has been run.
    Without ``--target-date`` the explicit ``--from-date`` / ``--to-date`` win.
    """
    if args.target_date is None:
        return args.from_date, args.to_date
    return args.target_date, add_days(args.target_date, args.days_ahead)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_features_duckdb")
    parser.add_argument(
        "--category",
        choices=("all", "ban-ei", "jra", "nar"),
        default="jra",
    )
    parser.add_argument("--from-date", type=str, default="20160101")
    parser.add_argument("--to-date", type=str, default="20251231")
    parser.add_argument(
        "--target-date",
        type=target_date_arg,
        default=None,
        help=(
            "YYYYMMDD; build feature rows for this date's races (incl. UPCOMING, "
            "finish_position NULL). Overrides --from-date / --to-date."
        ),
    )
    parser.add_argument(
        "--days-ahead",
        type=non_negative_int,
        default=0,
        help="Extra days after --target-date to include (default 0 = that day only).",
    )
    parser.add_argument(
        "--target-race",
        type=target_race_arg,
        default=None,
        help=(
            "keibajo_code:race_bango — build features for a single race only. "
            "Restricts the rec history scan to the target race's horses / jockeys "
            "/ trainers (paired with --target-date), cutting PG transfer from "
            "millions of rows to ~100K for the per-race CF Container path."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--pg-url", type=str, default=None)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--memory-limit", type=str, default=DEFAULT_MEMORY_LIMIT)
    parser.add_argument("--status-file", type=Path, default=None)
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help=(
            "Path to append structured JSON progress logs (log_event + "
            "heartbeat). Opened in append mode with line buffering so "
            "background builds (run via &) can be tailed in real time. "
            "Output is still written to stdout as well."
        ),
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=non_negative_float,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    )
    parser.add_argument("--skip-count", action="store_true")
    parser.add_argument("--keep-existing-output", action="store_true")
    parser.add_argument("--force-clean-output", action="store_true")
    parser.add_argument("--temp-dir", type=Path, default=None)
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume from the last checkpoint in --temp-dir/table_spill: stages "
            "whose spilled parquet is intact AND whose SQL fingerprint matches "
            "are restored as views instead of recomputed."
        ),
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Like --resume, but additionally cascade-invalidates downstream "
            "stages whenever an upstream stage must re-run, so changed SQL "
            "propagates correctly."
        ),
    )
    parser.add_argument(
        "--venue-weather-dir",
        type=Path,
        default=None,
        dest="venue_weather_dir",
        help=(
            "Directory containing venue_weather_YYYY.duckdb files "
            "(apps/venue-weather/data). When provided, per-venue hourly "
            "temperature / precipitation / wind from the build's years is "
            "aggregated over race hours (9-17) and LEFT JOINed into the "
            "weather features. Absent -> the venue weather columns are NULL."
        ),
    )
    parser.add_argument(
        "--allow-empty-targets",
        action="store_true",
        help=(
            "In --target-date mode, exit 0 with an empty output dir when the "
            "target window has no race rows. Lets the upcoming-prediction "
            "pipeline skip a category that has no races today without "
            "aborting the whole run."
        ),
    )
    parser.add_argument(
        "--realtime-odds",
        type=Path,
        default=None,
        dest="realtime_odds",
        help=(
            "Path to a parquet/CSV file with columns "
            "(keibajo_code TEXT, race_bango TEXT, umaban INT, "
            "tansho_odds_realtime DOUBLE, ninkijun_realtime INT). "
            "When provided the UPCOMING branch COALESCEs realtime odds "
            "over the nvd_se/jvd_se fallback so odds_score / popularity_score "
            "are populated for today's races. Absent → current NULL-fallback "
            "behaviour (backward-compatible)."
        ),
    )
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
        return f"{alias}.source = 'jra' and {alias}.keibajo_code in {JRA_KEIBAJO_CODES_SQL}"
    if category == "nar":
        return f"{alias}.source = 'nar' and ({alias}.keibajo_code is null or {alias}.keibajo_code <> '83')"
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
    for stmt in (
        "SET pg_experimental_filter_pushdown = true",
        "SET pg_use_binary_copy = true",
        "SET pg_use_ctid_scan = true",
        "SET pg_pages_per_task = 5000",
        "SET pg_connection_cache = true",
        "SET pg_connection_limit = 8",
    ):
        try:
            con.execute(stmt)
        except duckdb.Error:
            pass


def run_staged_sql(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    sql: str,
    row_count_table: str | None = None,
) -> None:
    log_event(stage, "start", 0.0)
    started = perf_counter()
    con.execute(sql)
    elapsed = perf_counter() - started
    rows: int | None = None
    if row_count_table is not None:
        result = con.execute(f"select count(*) from {row_count_table}").fetchone()
        if result is not None:
            rows = int(result[0])
    log_event(stage, "done", elapsed, rows=rows)


BAN_EI_KEIBAJO_CODE = "83"
CATEGORY_BAN_EI = "ban-ei"
CATEGORY_NAR = "nar"
CATEGORY_JRA = "jra"


def signed_zogen_sa_sql(fugo_expr: str, sa_expr: str) -> str:
    """SQL for the signed official-weight delta from the last race.

    ``sa_expr`` is the magnitude column (e.g. ``"002"``) and ``fugo_expr`` is the
    sign column (``"-"`` for a loss, ``"+"`` or blank for a gain). The ``"000"`` /
    ``"FFF"`` (case-insensitive) / blank sentinels mean "no data" and map to NULL,
    so the feature is never a misleading 0. The result is a signed integer:
    ``-magnitude`` when the sign column is ``"-"`` and ``+magnitude`` otherwise.
    """
    return (
        f"case when nullif(upper(trim({sa_expr})), '') is null"
        f" or upper(trim({sa_expr})) in ('000', 'FFF') then null"
        f" else try_cast(trim({sa_expr}) as int)"
        f" * case when trim({fugo_expr}) = '-' then -1 else 1 end end"
    )


def _rec_select_from_corner_features(history_start: str, to_date: str) -> str:
    """Build rec SELECT from race_entry_corner_features (no bataiju enrichment).

    bataiju lookup is done separately via stage_se_table / weight_cte to avoid
    a heavy PG-side double LEFT JOIN that would dominate stage time.
    """
    return f"""
    select
      source, race_date,
      strptime(race_date, '%Y%m%d')::date as race_dt,
      kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
      ketto_toroku_bango, umaban,
      kishumei_ryakusho, chokyoshimei_ryakusho,
      kyori, track_code, grade_code, kyoso_joken_code,
      coalesce(
        nullif(shusso_tosu, 0),
        count(*) over (
          partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        )
      ) as shusso_tosu,
      finish_position, finish_norm,
      time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
      babajotai_code_shiba, babajotai_code_dirt,
      tansho_ninkijun, tansho_odds,
      cast(null as int) as bataiju,
      try_cast(nullif(trim(seibetsu_code), '') as int) as seibetsu_code,
      try_cast(barei as int) as barei
    from pg.race_entry_corner_features
    where race_date between '{history_start}' and '{to_date}'
    """


def _rec_select_from_ban_ei(history_start: str, to_date: str) -> str:
    return f"""
    select
      'nar' as source,
      se.kaisai_nen || se.kaisai_tsukihi as race_date,
      strptime(se.kaisai_nen || se.kaisai_tsukihi, '%Y%m%d')::date as race_dt,
      se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango,
      se.ketto_toroku_bango,
      try_cast(nullif(trim(se.umaban), '') as int) as umaban,
      se.kishumei_ryakusho, se.chokyoshimei_ryakusho,
      try_cast(nullif(trim(ra.kyori), '') as int) as kyori,
      ra.track_code, ra.grade_code, ra.kyoso_joken_code,
      coalesce(
        nullif(try_cast(nullif(trim(ra.shusso_tosu), '') as int), 0),
        count(*) over (
          partition by se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango
        )
      ) as shusso_tosu,
      try_cast(nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as int) as finish_position,
      case
        when try_cast(nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as double) is not null
             and try_cast(nullif(trim(ra.shusso_tosu), '') as double) is not null
             and try_cast(nullif(trim(ra.shusso_tosu), '') as double) > 0
        then try_cast(nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as double)
             / try_cast(nullif(trim(ra.shusso_tosu), '') as double)
        else null
      end as finish_norm,
      try_cast(nullif(trim(se.time_sa), '') as double) as time_sa,
      try_cast(nullif(trim(se.kohan_3f), '') as double) as kohan_3f,
      cast(null as double) as corner1_norm,
      cast(null as double) as corner3_norm,
      cast(null as double) as corner4_norm,
      ra.babajotai_code_shiba, ra.babajotai_code_dirt,
      try_cast(nullif(trim(se.tansho_ninkijun), '') as int) as tansho_ninkijun,
      try_cast(nullif(trim(se.tansho_odds), '') as double) / 10 as tansho_odds,
      try_cast(nullif(trim(se.bataiju), '') as int) as bataiju,
      try_cast(nullif(trim(se.seibetsu_code), '') as int) as seibetsu_code,
      try_cast(nullif(trim(se.barei), '') as int) as barei
    from pg.nvd_se se

    join pg.nvd_ra ra
      on ra.kaisai_nen = se.kaisai_nen
      and ra.kaisai_tsukihi = se.kaisai_tsukihi
      and ra.keibajo_code = se.keibajo_code
      and ra.race_bango = se.race_bango
    where se.keibajo_code = '{BAN_EI_KEIBAJO_CODE}'
      and se.kaisai_nen between '{history_start[:4]}' and '{to_date[:4]}'
      and (se.kaisai_nen || se.kaisai_tsukihi) between '{history_start}' and '{to_date}'
      and se.ketto_toroku_bango is not null
    """


def _rec_select_from_se_ra(
    source: str,
    se_table: str,
    ra_table: str,
    target_from: str,
    target_to: str,
    keibajo_predicate: str,
) -> str:
    """Direct ``*_se`` x ``*_ra`` SELECT for the target window (UPCOMING mode).

    Mirrors the column shape of the corner-feature / ban-ei rec selects but reads
    straight from the source race tables, so today's races that have NOT yet been
    materialised into ``race_entry_corner_features`` (the derived table lags) are
    still emitted as target rows. Post-race-only signals (corner_* / time_sa /
    kohan_3f) are NULL for unrun races, matching how the model saw NULLs in
    training; finish_position is parsed when present so already-run races on the
    same day keep their outcome.

    When the ``realtime_odds_rt`` temp table is present (loaded by
    ``stage_realtime_odds_table`` before this query runs) the odds/rank columns
    COALESCE: realtime value first (direct multiplier, already in the units the
    formulas expect), then the nvd_se / jvd_se fallback (4-char /10 raw string).
    An empty ``realtime_odds_rt`` table (created by
    ``_drop_realtime_odds_table_if_exists``) produces the same result as NULL for
    every row, so the fallback path is always executed when no realtime data is
    available — preserving the existing behaviour exactly.
    """
    return f"""
    select
      '{source}' as source,
      se.kaisai_nen || se.kaisai_tsukihi as race_date,
      strptime(se.kaisai_nen || se.kaisai_tsukihi, '%Y%m%d')::date as race_dt,
      se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango,
      se.ketto_toroku_bango,
      try_cast(nullif(trim(se.umaban), '') as int) as umaban,
      se.kishumei_ryakusho, se.chokyoshimei_ryakusho,
      try_cast(nullif(trim(ra.kyori), '') as int) as kyori,
      ra.track_code, ra.grade_code, ra.kyoso_joken_code,
      coalesce(
        nullif(try_cast(nullif(trim(ra.shusso_tosu), '') as int), 0),
        count(*) over (
          partition by se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango
        )
      ) as shusso_tosu,
      try_cast(nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as int) as finish_position,
      case
        when try_cast(nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as double) is not null
             and try_cast(nullif(trim(ra.shusso_tosu), '') as double) is not null
             and try_cast(nullif(trim(ra.shusso_tosu), '') as double) > 0
        then try_cast(nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as double)
             / try_cast(nullif(trim(ra.shusso_tosu), '') as double)
        else null
      end as finish_norm,
      try_cast(nullif(trim(se.time_sa), '') as double) as time_sa,
      try_cast(nullif(trim(se.kohan_3f), '') as double) as kohan_3f,
      cast(null as double) as corner1_norm,
      cast(null as double) as corner3_norm,
      cast(null as double) as corner4_norm,
      ra.babajotai_code_shiba, ra.babajotai_code_dirt,
      coalesce(
        rt.ninkijun_realtime,
        try_cast(nullif(trim(se.tansho_ninkijun), '') as int)
      ) as tansho_ninkijun,
      coalesce(
        rt.tansho_odds_realtime,
        try_cast(nullif(trim(se.tansho_odds), '') as double) / 10
      ) as tansho_odds,
      coalesce(
        rt.bataiju_realtime,
        try_cast(nullif(trim(se.bataiju), '') as int)
      ) as bataiju,
      try_cast(nullif(trim(se.seibetsu_code), '') as int) as seibetsu_code,
      try_cast(nullif(trim(se.barei), '') as int) as barei
    from pg.{se_table} se
    join pg.{ra_table} ra
      on ra.kaisai_nen = se.kaisai_nen
      and ra.kaisai_tsukihi = se.kaisai_tsukihi
      and ra.keibajo_code = se.keibajo_code
      and ra.race_bango = se.race_bango
    left join {REALTIME_ODDS_TABLE} rt
      on rt.keibajo_code = se.keibajo_code
      and rt.race_bango = se.race_bango
      and rt.umaban = try_cast(nullif(trim(se.umaban), '') as int)
    where {keibajo_predicate}
      and se.kaisai_nen between '{target_from[:4]}' and '{target_to[:4]}'
      and (se.kaisai_nen || se.kaisai_tsukihi) between '{target_from}' and '{target_to}'
      and se.ketto_toroku_bango is not null
      and try_cast(nullif(trim(se.umaban), '') as int) is not null
    """


def upcoming_target_union_sql(category: str, target_from: str, target_to: str) -> str:
    """Direct source select(s) for the UPCOMING target window, by category."""
    if category == CATEGORY_JRA:
        return _rec_select_from_se_ra(
            "jra",
            "jvd_se",
            "jvd_ra",
            target_from,
            target_to,
            f"se.keibajo_code in {JRA_KEIBAJO_CODES_SQL}",
        )
    if category == CATEGORY_NAR:
        return _rec_select_from_se_ra(
            "nar",
            "nvd_se",
            "nvd_ra",
            target_from,
            target_to,
            f"(se.keibajo_code is null or se.keibajo_code <> '{BAN_EI_KEIBAJO_CODE}')",
        )
    if category == CATEGORY_BAN_EI:
        return _rec_select_from_se_ra(
            "nar",
            "nvd_se",
            "nvd_ra",
            target_from,
            target_to,
            f"se.keibajo_code = '{BAN_EI_KEIBAJO_CODE}'",
        )
    jra_sql = upcoming_target_union_sql(CATEGORY_JRA, target_from, target_to)
    nar_sql = upcoming_target_union_sql(CATEGORY_NAR, target_from, target_to)
    ban_ei_sql = upcoming_target_union_sql(CATEGORY_BAN_EI, target_from, target_to)
    return f"{jra_sql}\nunion all\n{nar_sql}\nunion all\n{ban_ei_sql}"


def build_rec_select_sql(
    category: str,
    history_start: str,
    to_date: str,
    upcoming_window: tuple[str, str] | None = None,
) -> str:
    if category == CATEGORY_BAN_EI:
        base = _rec_select_from_ban_ei(history_start, to_date)
    else:
        corner_sql = _rec_select_from_corner_features(history_start, to_date)
        ban_ei_sql = _rec_select_from_ban_ei(history_start, to_date)
        base = f"{corner_sql}\nunion all\n{ban_ei_sql}"
    if upcoming_window is None:
        return base
    target_from, target_to = upcoming_window
    upcoming_sql = upcoming_target_union_sql(category, target_from, target_to)
    # The direct source rows (priority 1) overlap the corner-feature / ban-ei
    # rows (priority 0) on the target window. Keep the corner-feature row when it
    # exists (it carries corner_* signals); fall back to the direct source row
    # for races not yet materialised into race_entry_corner_features.
    return f"""
    select * exclude (_rec_priority) from (
      select base_union.*, _rec_priority from (
        select *, 0 as _rec_priority from ({base})
        union all by name
        select *, 1 as _rec_priority from ({upcoming_sql})
      ) base_union
      qualify row_number() over (
        partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
        order by _rec_priority
      ) = 1
    )
    """


def _query_target_race_entity_filter(
    con: duckdb.DuckDBPyConnection,
    keibajo_code: str,
    race_bango: str,
    target_from: str,
    target_to: str,
) -> str:
    """WHERE-clause fragment restricting rec to the target race's entities.

    Queries the raw source table (jvd_se for JRA keibajo codes, nvd_se for NAR /
    Ban-ei) for the horses / jockeys / trainers entered in the target race, then
    returns ``and (horse in (...) or jockey in (...) or trainer in (...))``. The
    target race is UPCOMING, so it lives in the raw source table — the derived
    race_entry_corner_features lags and may not yet contain it. Returns "" when
    the race has no entries, leaving rec unfiltered.
    """
    se_table = "jvd_se" if keibajo_code in JRA_KEIBAJO_CODES else "nvd_se"
    rows = con.execute(
        f"""
        select distinct ketto_toroku_bango, kishumei_ryakusho, chokyoshimei_ryakusho
        from pg.{se_table}
        where keibajo_code = '{keibajo_code}'
          and race_bango = '{race_bango}'
          and (kaisai_nen || kaisai_tsukihi) between '{target_from}' and '{target_to}'
        """
    ).fetchall()
    horses = [f"'{r[0]}'" for r in rows if r[0]]
    jockeys = [f"'{r[1]}'" for r in rows if r[1]]
    trainers = [f"'{r[2]}'" for r in rows if r[2]]
    parts: list[str] = []
    if horses:
        parts.append(f"ketto_toroku_bango in ({', '.join(horses)})")
    if jockeys:
        parts.append(f"kishumei_ryakusho in ({', '.join(jockeys)})")
    if trainers:
        parts.append(f"chokyoshimei_ryakusho in ({', '.join(trainers)})")
    if not parts:
        return ""
    return " and (" + " or ".join(parts) + ")"


def stage_rec_table(
    con: duckdb.DuckDBPyConnection,
    history_start: str,
    to_date: str,
    category: str,
    upcoming_window: tuple[str, str] | None = None,
    target_race: tuple[str, str] | None = None,
) -> None:
    select_sql = build_rec_select_sql(category, history_start, to_date, upcoming_window)
    if target_race is not None and upcoming_window is not None:
        keibajo_code, race_bango = target_race
        target_from, target_to = upcoming_window
        entity_filter = _query_target_race_entity_filter(
            con, keibajo_code, race_bango, target_from, target_to
        )
        if entity_filter:
            select_sql = (
                f"select * from ({select_sql}) _rec_target where true{entity_filter}"
            )
    run_staged_sql(
        con,
        "source.rec",
        f"create or replace temp table rec as {select_sql}",
        row_count_table="rec",
    )
    log_event("source.rec.indexes", "start", 0.0)
    started = perf_counter()
    con.execute("create index rec_horse_date on rec (source, ketto_toroku_bango, race_date)")
    con.execute("create index rec_jockey_date on rec (source, kishumei_ryakusho, race_date)")
    con.execute("create index rec_trainer_date on rec (source, chokyoshimei_ryakusho, race_date)")
    con.execute("create index rec_keibajo_date on rec (source, keibajo_code, race_date)")
    log_event("source.rec.indexes", "done", perf_counter() - started)


def stage_se_table(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    table: str,
    pg_table: str,
    history_start: str,
    to_date: str,
    keibajo_filter: str | None = None,
) -> None:
    keibajo_clause = f"and keibajo_code = '{keibajo_filter}'" if keibajo_filter else ""
    history_year = history_start[:4]
    to_year = to_date[:4]
    run_staged_sql(
        con,
        stage,
        f"""
        create or replace temp table {table} as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
               try_cast(nullif(trim(bataiju), '') as int) as bataiju,
               cast(zogen_sa as varchar) as zogen_sa,
               cast(zogen_fugo as varchar) as zogen_fugo
        from pg.{pg_table}
        where kaisai_nen between '{history_year}' and '{to_year}'
          and (kaisai_nen || kaisai_tsukihi) between '{history_start}' and '{to_date}'
          {keibajo_clause}
        """,
        row_count_table=table,
    )
    log_event(f"{stage}.index", "start", 0.0)
    started = perf_counter()
    con.execute(
        f"create index {table}_jk_idx on {table} "
        f"(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )
    log_event(f"{stage}.index", "done", perf_counter() - started)


def stage_um_table(
    con: duckdb.DuckDBPyConnection, stage: str, table: str, pg_table: str
) -> None:
    run_staged_sql(
        con,
        stage,
        f"""
        create or replace temp table {table} as
        select ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b from pg.{pg_table}
        """,
        row_count_table=table,
    )


def stage_ra_table(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    table: str,
    pg_table: str,
    from_date: str,
    to_date: str,
    keibajo_filter: str | None = None,
) -> None:
    keibajo_clause = f"and keibajo_code = '{keibajo_filter}'" if keibajo_filter else ""
    from_year = from_date[:4]
    to_year = to_date[:4]
    run_staged_sql(
        con,
        stage,
        f"""
        create or replace temp table {table} as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, tenko_code,
               kyoso_joken_meisho
        from pg.{pg_table}
        where kaisai_nen between '{from_year}' and '{to_year}'
          and (kaisai_nen || kaisai_tsukihi) between '{from_date}' and '{to_date}'
          {keibajo_clause}
        """,
        row_count_table=table,
    )


REALTIME_ODDS_TABLE = "realtime_odds_rt"


def stage_realtime_odds_table(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    """Load a pre-fetched realtime-odds file into a DuckDB temp table.

    Accepts parquet or CSV (auto-detected by DuckDB from the extension).
    The file must have columns:
      keibajo_code TEXT, race_bango TEXT, umaban INT,
      tansho_odds_realtime DOUBLE, ninkijun_realtime INT,
      bataiju_realtime INT (nullable — present when weight was available at
      fetch time, NULL otherwise; COALESCE in _rec_select_from_se_ra falls back
      to the nvd_se / jvd_se bataiju field for NULL rows).

    The ``bataiju_realtime`` column is loaded with ``try_cast`` so parquet files
    written before this feature (which lack the column) still load cleanly via
    DuckDB's column-missing-as-null behaviour.

    Returns the row count (0 if the file is empty — the COALESCE falls back to
    the nvd_se / jvd_se value silently).
    """
    log_event("source.realtime_odds", "start", 0.0)
    started = perf_counter()
    suffix = path.suffix.lower()
    if suffix in (".parquet", ".pq"):
        read_expr = f"read_parquet('{path.as_posix()}')"
    else:
        read_expr = f"read_csv('{path.as_posix()}', auto_detect=true)"
    con.execute(
        f"""
        create or replace temp table {REALTIME_ODDS_TABLE} as
        select
          cast(keibajo_code as varchar) as keibajo_code,
          cast(race_bango as varchar) as race_bango,
          cast(umaban as int) as umaban,
          cast(tansho_odds_realtime as double) as tansho_odds_realtime,
          cast(ninkijun_realtime as int) as ninkijun_realtime,
          try_cast(bataiju_realtime as int) as bataiju_realtime
        from {read_expr}
        """
    )
    con.execute(
        f"create index {REALTIME_ODDS_TABLE}_idx "
        f"on {REALTIME_ODDS_TABLE} (keibajo_code, race_bango, umaban)"
    )
    row = con.execute(f"select count(*) from {REALTIME_ODDS_TABLE}").fetchone()
    rc = int(row[0]) if row is not None else 0
    log_event("source.realtime_odds", "done", perf_counter() - started, rc)
    return rc


def create_empty_realtime_odds_stub(con: duckdb.DuckDBPyConnection) -> None:
    """Create an empty realtime_odds_rt stub so COALESCE refs resolve safely.

    Called by ``stage_source_tables`` when no realtime-odds file is provided.
    The stub has the correct schema (zero rows) so the LEFT JOIN in
    ``_rec_select_from_se_ra`` compiles and returns NULL for every horse,
    preserving the existing nvd_se / jvd_se fallback path exactly.
    Includes ``bataiju_realtime`` so the COALESCE in the UPCOMING branch
    references a known column even when no realtime file was provided.
    """
    con.execute(
        f"""
        create or replace temp table {REALTIME_ODDS_TABLE} as
        select
          cast(null as varchar) as keibajo_code,
          cast(null as varchar) as race_bango,
          cast(null as int) as umaban,
          cast(null as double) as tansho_odds_realtime,
          cast(null as int) as ninkijun_realtime,
          cast(null as int) as bataiju_realtime
        where false
        """
    )


def _log_source_config(category: str, history_start: str, from_date: str, to_date: str) -> None:
    emit_log_line(
        json.dumps(
            {
                "stage": "source.config",
                "status": "info",
                "category": category,
                "history_start": history_start,
                "from_date": from_date,
                "to_date": to_date,
            },
            ensure_ascii=False,
        )
    )


def _stage_empty_jra_stubs(con: duckdb.DuckDBPyConnection) -> None:
    """For ban-ei builds, create empty stub tables for jra_se/jra_um/jra_ra.

    weight_cte still references jra_se / nar_se via LEFT JOIN, and
    pedigree_rec_um_subquery / weather_lookup reference jra_um / jra_ra for
    the JRA branch of union queries. Empty stubs let those queries run
    without PG round-trips.
    """
    run_staged_sql(
        con,
        "source.jra_se.skip",
        "create or replace temp table jra_se as "
        "select cast(null as varchar) as kaisai_nen, cast(null as varchar) as kaisai_tsukihi, "
        "cast(null as varchar) as keibajo_code, cast(null as varchar) as race_bango, "
        "cast(null as varchar) as ketto_toroku_bango, cast(null as int) as bataiju, "
        "cast(null as varchar) as zogen_sa, cast(null as varchar) as zogen_fugo "
        "where false",
        row_count_table="jra_se",
    )
    run_staged_sql(
        con,
        "source.jra_um.skip",
        "create or replace temp table jra_um as "
        "select cast(null as varchar) as ketto_toroku_bango, "
        "cast(null as varchar) as ketto_joho_01b, cast(null as varchar) as ketto_joho_05b "
        "where false",
        row_count_table="jra_um",
    )
    run_staged_sql(
        con,
        "source.jra_ra.skip",
        "create or replace temp table jra_ra as "
        "select cast(null as varchar) as kaisai_nen, cast(null as varchar) as kaisai_tsukihi, "
        "cast(null as varchar) as keibajo_code, cast(null as varchar) as race_bango, "
        "cast(null as varchar) as tenko_code, "
        "cast(null as varchar) as kyoso_joken_meisho "
        "where false",
        row_count_table="jra_ra",
    )


def stage_source_tables(
    con: duckdb.DuckDBPyConnection,
    from_date: str,
    to_date: str,
    category: str,
    upcoming_window: tuple[str, str] | None = None,
    realtime_odds_path: Path | None = None,
    target_race: tuple[str, str] | None = None,
) -> None:
    history_start = compute_history_start(from_date, HISTORY_LOOKBACK_YEARS)
    _log_source_config(category, history_start, from_date, to_date)
    # Realtime-odds table must exist before stage_rec_table builds the upcoming
    # SELECT (which LEFT JOINs against it). An empty stub is created when no
    # file is provided so the COALESCE references resolve safely without
    # conditional SQL generation.
    if realtime_odds_path is not None:
        stage_realtime_odds_table(con, realtime_odds_path)
    else:
        create_empty_realtime_odds_stub(con)
    stage_rec_table(con, history_start, to_date, category, upcoming_window, target_race)
    nar_keibajo_filter = BAN_EI_KEIBAJO_CODE if category == CATEGORY_BAN_EI else None
    stage_se_table(
        con, "source.nar_se", "nar_se", "nvd_se", history_start, to_date, nar_keibajo_filter
    )
    stage_um_table(con, "source.nar_um", "nar_um", "nvd_um")
    stage_um_table(con, "source.nar_nu", "nar_nu", "nvd_nu")
    stage_ra_table(
        con, "source.nar_ra", "nar_ra", "nvd_ra", from_date, to_date, nar_keibajo_filter
    )
    if category == CATEGORY_BAN_EI:
        _stage_empty_jra_stubs(con)
        return
    stage_se_table(con, "source.jra_se", "jra_se", "jvd_se", history_start, to_date)
    stage_um_table(con, "source.jra_um", "jra_um", "jvd_um")
    stage_ra_table(con, "source.jra_ra", "jra_ra", "jvd_ra", from_date, to_date)


def build_target_table(
    con: duckdb.DuckDBPyConnection,
    category: str,
    from_date: str,
    to_date: str,
    target_race: tuple[str, str] | None = None,
) -> None:
    filter_clause = category_source_filter(category, "rec")
    cat_expr = category_expression(category)
    race_filter = ""
    if target_race is not None:
        keibajo_code, race_bango = target_race
        race_filter = (
            f"\n          and rec.keibajo_code = '{keibajo_code}'"
            f"\n          and rec.race_bango = '{race_bango}'"
        )
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
          rec.corner1_norm as target_corner_1_norm,
          rec.corner3_norm as target_corner_3_norm,
          rec.corner4_norm as target_corner_4_norm,
          case
            when rec.corner1_norm is null then null
            when rec.corner1_norm = 0 then {RUNNING_STYLE_CLASS_NIGE}
            when rec.corner1_norm <= {RUNNING_STYLE_SENKOU_THRESHOLD} then {RUNNING_STYLE_CLASS_SENKOU}
            when rec.corner1_norm <= {RUNNING_STYLE_SASHI_THRESHOLD} then {RUNNING_STYLE_CLASS_SASHI}
            else {RUNNING_STYLE_CLASS_OIKOMI}
          end as target_running_style_class,
          cast(rec.tansho_odds as double) as tansho_odds,
          cast(rec.tansho_ninkijun as int) as tansho_ninkijun,
          rec.seibetsu_code,
          rec.barei,
          case when rec.kaisai_tsukihi is null or length(rec.kaisai_tsukihi) < 2 then null
               else cast(substr(rec.kaisai_tsukihi, 1, 2) as int) end as kaisai_month,
          'v1' as feature_schema_version,
          cast(substr(rec.race_date, 1, 4) as int) as race_year
        from rec
        where rec.race_date between '{from_date}' and '{to_date}'
          and {filter_clause}
          and rec.ketto_toroku_bango is not null{race_filter}
        """
    )
    con.execute(
        "create index target_horse_idx on target (source, ketto_toroku_bango, race_date)"
    )
    con.execute(
        "create index target_jockey_idx on target (source, kishumei_ryakusho)"
    )
    con.execute(
        "create index target_trainer_idx on target (source, chokyoshimei_ryakusho)"
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
        cast(h.corner1_norm as double) as corner1_norm,
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
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as jockey_horse_pair_win_rate,
        avg(case when corner1_norm = 0 then 1.0
                 when corner1_norm is null then null
                 else 0.0 end) as jockey_nige_rate,
        avg(case when corner1_norm is null then null
                 when corner1_norm > 0 and corner1_norm <= {RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0
                 else 0.0 end) as jockey_senkou_rate,
        avg(case when corner1_norm is null then null
                 when corner1_norm > {RUNNING_STYLE_SENKOU_THRESHOLD}
                  and corner1_norm <= {RUNNING_STYLE_SASHI_THRESHOLD} then 1.0
                 else 0.0 end) as jockey_sashi_rate,
        avg(case when corner1_norm is null then null
                 when corner1_norm > {RUNNING_STYLE_SASHI_THRESHOLD} then 1.0
                 else 0.0 end) as jockey_oikomi_rate,
        avg(corner1_norm) as jockey_corner_1_norm_avg,
        avg(corner1_norm) filter (where history_horse = target_horse) as jockey_horse_corner_1_norm_avg,
        avg(corner1_norm) filter (where history_race_dt >= target_race_dt - {JOCKEY_RECENT_DAYS})
          as jockey_recent_corner_1_norm_avg_90d,
        avg(case when corner1_norm = 0 then 1.0
                 when corner1_norm is null then null
                 else 0.0 end)
          filter (where history_race_dt >= target_race_dt - {JOCKEY_RECENT_DAYS})
          as jockey_recent_nige_rate_90d,
        avg(case when finish_position = 1 then 1 else 0 end)
          filter (where (cast(month(history_race_dt) as int) + 9) % 12 // 3 = (cast(month(target_race_dt) as int) + 9) % 12 // 3)
          as jockey_season_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end)
          filter (where (cast(month(history_race_dt) as int) + 9) % 12 // 3 = (cast(month(target_race_dt) as int) + 9) % 12 // 3 and history_keibajo = target_keibajo)
          as jockey_season_keibajo_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end)
          filter (where history_keibajo = target_keibajo and abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE})
          as jockey_keibajo_distance_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end)
          filter (where (cast(month(history_race_dt) as int) + 9) % 12 // 3 = (cast(month(target_race_dt) as int) + 9) % 12 // 3 and history_keibajo = target_keibajo and abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE})
          as jockey_season_keibajo_distance_win_rate,
        count(*)
          filter (where (cast(month(history_race_dt) as int) + 9) % 12 // 3 = (cast(month(target_race_dt) as int) + 9) % 12 // 3 and history_keibajo = target_keibajo and abs(history_kyori - target_kyori) <= {SAME_DISTANCE_TOLERANCE})
          as jockey_season_keibajo_distance_count
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
        avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as trainer_horse_win_rate,
        avg(case when corner1_norm = 0 then 1.0
                 when corner1_norm is null then null
                 else 0.0 end) as trainer_nige_rate,
        avg(case when corner1_norm is null then null
                 when corner1_norm > 0 and corner1_norm <= {RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0
                 else 0.0 end) as trainer_senkou_rate,
        avg(case when corner1_norm is null then null
                 when corner1_norm > {RUNNING_STYLE_SENKOU_THRESHOLD}
                  and corner1_norm <= {RUNNING_STYLE_SASHI_THRESHOLD} then 1.0
                 else 0.0 end) as trainer_sashi_rate,
        avg(case when corner1_norm is null then null
                 when corner1_norm > {RUNNING_STYLE_SASHI_THRESHOLD} then 1.0
                 else 0.0 end) as trainer_oikomi_rate,
        avg(corner1_norm) as trainer_corner_1_norm_avg,
        avg(case when finish_position = 1 then 1 else 0 end)
          filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, ''))
          as trainer_grade_win_rate,
        avg(case when finish_position = 1 then 1 else 0 end)
          filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, '') and left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1) and (cast(month(history_race_dt) as int) + 9) % 12 // 3 = (cast(month(target_race_dt) as int) + 9) % 12 // 3)
          as trainer_class_surface_season_win_rate,
        count(*)
          filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, '') and left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1) and (cast(month(history_race_dt) as int) + 9) % 12 // 3 = (cast(month(target_race_dt) as int) + 9) % 12 // 3)
          as trainer_class_surface_season_count
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
            "select rec.*,"
            " coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) as ketto_joho_01b,"
            " coalesce(um.ketto_joho_05b, nu.ketto_joho_05b) as ketto_joho_05b"
            " from rec"
            " left join nar_um um using (ketto_toroku_bango)"
            " left join nar_nu nu using (ketto_toroku_bango)"
            " where rec.source = 'nar' and (rec.keibajo_code is null or rec.keibajo_code <> '83')"
            " and coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) is not null"
        )
    if category == "ban-ei":
        return (
            "select rec.*,"
            " coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) as ketto_joho_01b,"
            " coalesce(um.ketto_joho_05b, nu.ketto_joho_05b) as ketto_joho_05b"
            " from rec"
            " left join nar_um um using (ketto_toroku_bango)"
            " left join nar_nu nu using (ketto_toroku_bango)"
            " where rec.source = 'nar' and rec.keibajo_code = '83'"
            " and coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) is not null"
        )
    return (
        "select rec.*, um.ketto_joho_01b, um.ketto_joho_05b"
        " from rec join jra_um um using (ketto_toroku_bango)"
        " where rec.source = 'jra'"
        " union all"
        " select rec.*,"
        " coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) as ketto_joho_01b,"
        " coalesce(um.ketto_joho_05b, nu.ketto_joho_05b) as ketto_joho_05b"
        " from rec"
        " left join nar_um um using (ketto_toroku_bango)"
        " left join nar_nu nu using (ketto_toroku_bango)"
        " where rec.source = 'nar'"
        " and coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) is not null"
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
            " sum(finish_norm) as finish_norm_sum,"
            " count(finish_norm) as finish_norm_count"
        ),
        "accum_metrics_select": (
            "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as sire_distance_win_rate_val,"
            " sum(m.finish_norm_sum)::double / nullif(sum(m.finish_norm_count), 0)"
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
        "monthly_metrics_select": (
            "sum(finish_norm) as finish_norm_sum,"
            " count(finish_norm) as finish_norm_count"
        ),
        "accum_metrics_select": (
            "sum(m.finish_norm_sum)::double / nullif(sum(m.finish_norm_count), 0)"
            " as damsire_avg_finish_at_track_val"
        ),
    },
    {
        "table": "sire_keibajo_stats",
        "key_column": "ketto_joho_01b",
        "key_alias": "sire",
        "bucket_expr": "keibajo_code",
        "bucket_alias": "keibajo_code",
        "monthly_metrics_select": "sum(case when finish_position = 1 then 1 else 0 end) as win_count",
        "accum_metrics_select": (
            "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as sire_keibajo_win_rate_val"
        ),
    },
    {
        "table": "damsire_keibajo_stats",
        "key_column": "ketto_joho_05b",
        "key_alias": "damsire",
        "bucket_expr": "keibajo_code",
        "bucket_alias": "keibajo_code",
        "monthly_metrics_select": "sum(case when finish_position = 1 then 1 else 0 end) as win_count",
        "accum_metrics_select": (
            "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as damsire_keibajo_win_rate_val"
        ),
    },
    {
        "table": "sire_running_style_stats",
        "key_column": "ketto_joho_01b",
        "key_alias": "sire",
        "bucket_expr": "0",
        "bucket_alias": "rs_bucket",
        "monthly_metrics_select": (
            "sum(case when corner1_norm = 0 then 1 else 0 end) as nige_count,"
            f" sum(case when corner1_norm > 0 and corner1_norm <= {RUNNING_STYLE_SENKOU_THRESHOLD} then 1 else 0 end) as senkou_count,"
            f" sum(case when corner1_norm > {RUNNING_STYLE_SENKOU_THRESHOLD} and corner1_norm <= {RUNNING_STYLE_SASHI_THRESHOLD} then 1 else 0 end) as sashi_count,"
            f" sum(case when corner1_norm > {RUNNING_STYLE_SASHI_THRESHOLD} then 1 else 0 end) as oikomi_count,"
            " sum(corner1_norm) as corner1_norm_sum,"
            " count(corner1_norm) as corner1_norm_count"
        ),
        "accum_metrics_select": (
            "sum(m.nige_count)::double / nullif(sum(m.corner1_norm_count), 0) as sire_nige_rate_val,"
            " sum(m.senkou_count)::double / nullif(sum(m.corner1_norm_count), 0) as sire_senkou_rate_val,"
            " sum(m.sashi_count)::double / nullif(sum(m.corner1_norm_count), 0) as sire_sashi_rate_val,"
            " sum(m.oikomi_count)::double / nullif(sum(m.corner1_norm_count), 0) as sire_oikomi_rate_val,"
            " sum(m.corner1_norm_sum)::double / nullif(sum(m.corner1_norm_count), 0) as sire_corner_1_norm_avg_val"
        ),
    },
]


def pedigree_rec_um_sql(category: str) -> str:
    subquery = pedigree_rec_um_subquery(category)
    return f"""
    create or replace temp table pedigree_rec_um as
    with src as ({subquery})
    select
      source, race_date,
      cast(substr(race_date, 1, 4) as int) * 100 + cast(substr(race_date, 5, 2) as int) as race_year_month,
      ketto_toroku_bango, kyori, track_code, finish_position, finish_norm,
      keibajo_code, ketto_joho_01b, ketto_joho_05b,
      cast(corner1_norm as double) as corner1_norm
    from src
    """


def pedigree_stat_base_columns(spec: PedigreeStatSpec) -> list[str]:
    aliases = [
        fragment.rsplit(" as ", 1)[1].strip()
        for fragment in spec["monthly_metrics_select"].split(",")
    ]
    return [*aliases, "race_count"]


def pedigree_stat_cumulative_select(spec: PedigreeStatSpec) -> str:
    return ",\n        ".join(
        f"sum(m.{col}) over w as cum_{col}" for col in pedigree_stat_base_columns(spec)
    )


def pedigree_stat_accum_from_cumulative(spec: PedigreeStatSpec) -> str:
    accum = spec["accum_metrics_select"]
    for col in pedigree_stat_base_columns(spec):
        accum = accum.replace(f"sum(m.{col})", f"c.cum_{col}")
    return accum


def pedigree_monthly_stat_sql(spec: PedigreeStatSpec) -> str:
    return f"""
    create or replace temp table {spec["table"]} as
    with monthly as (
      select
        race_year_month,
        {spec["key_column"]} as {spec["key_alias"]},
        {spec["bucket_expr"]} as {spec["bucket_alias"]},
        {spec["monthly_metrics_select"]},
        count(*) as race_count
      from pedigree_rec_um
      where finish_position is not null
        and {spec["key_column"]} is not null and trim({spec["key_column"]}) <> ''
      group by 1, 2, 3
    ),
    cumulative as (
      select
        race_year_month,
        {spec["key_alias"]},
        {spec["bucket_alias"]},
        {pedigree_stat_cumulative_select(spec)}
      from monthly m
      window w as (
        partition by {spec["key_alias"]}, {spec["bucket_alias"]}
        order by race_year_month
        rows between unbounded preceding and current row
      )
    ),
    stat_keys as (
      select distinct {spec["key_alias"]}, {spec["bucket_alias"]} from cumulative
    )
    select
      tm.stats_year_month,
      k.{spec["key_alias"]},
      k.{spec["bucket_alias"]},
      {pedigree_stat_accum_from_cumulative(spec)},
      c.cum_race_count as race_count
    from target_months tm
    cross join stat_keys k
    asof join cumulative c
      on c.{spec["key_alias"]} = k.{spec["key_alias"]}
      and c.{spec["bucket_alias"]} = k.{spec["bucket_alias"]}
      and c.race_year_month < tm.stats_year_month
    """


def target_pedigree_sql() -> str:
    return f"""
    create or replace temp table target_pedigree as
    select
      t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
      cast(coalesce(t.kyori, 0) as int) / {DISTANCE_BAND_METERS} as kyori_band,
      left(coalesce(t.track_code, ''), 1) as surface,
      t.keibajo_code as target_keibajo_code,
      0 as rs_bucket,
      coalesce(j_um.ketto_joho_01b, n_um.ketto_joho_01b, n_nu.ketto_joho_01b) as target_sire,
      coalesce(j_um.ketto_joho_05b, n_um.ketto_joho_05b, n_nu.ketto_joho_05b) as target_damsire
    from target t
    left join jra_um j_um on t.source = 'jra' and j_um.ketto_toroku_bango = t.ketto_toroku_bango
    left join nar_um n_um on t.source = 'nar' and n_um.ketto_toroku_bango = t.ketto_toroku_bango
    left join nar_nu n_nu on t.source = 'nar' and n_nu.ketto_toroku_bango = t.ketto_toroku_bango
    """


PEDIGREE_NATURAL_KEY: tuple[str, ...] = (
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
)

PEDIGREE_FEATURES_TABLE = "pedigree_features"


class PedigreeJoinSpec(TypedDict):
    table: str
    alias: str
    key_alias: str
    bucket_alias: str
    target_bucket: str
    val_columns: tuple[str, ...]


# How each stats table joins back onto target_pedigree (tp) and which value
# columns it contributes. `target_bucket` is the tp column matched against the
# stats table's bucket; the third join key is always stats_year_month derived
# from the race date. Drives both the per-table index creation and the single
# consolidated pedigree_features join, replacing the 8 separate LEFT JOINs that
# previously ran inside base_features_select_sql for every one of the 2.89M rows.
PEDIGREE_JOIN_SPECS: tuple[PedigreeJoinSpec, ...] = (
    {
        "table": "sire_distance_stats",
        "alias": "sds",
        "key_alias": "sire",
        "bucket_alias": "kyori_band",
        "target_bucket": "kyori_band",
        "val_columns": ("sire_distance_win_rate_val", "sire_avg_finish_at_distance_val"),
    },
    {
        "table": "sire_track_stats",
        "alias": "sts",
        "key_alias": "sire",
        "bucket_alias": "surface",
        "target_bucket": "surface",
        "val_columns": ("sire_track_win_rate_val",),
    },
    {
        "table": "damsire_distance_stats",
        "alias": "dsd",
        "key_alias": "damsire",
        "bucket_alias": "kyori_band",
        "target_bucket": "kyori_band",
        "val_columns": ("dam_sire_distance_win_rate_val",),
    },
    {
        "table": "damsire_track_stats",
        "alias": "dst",
        "key_alias": "damsire",
        "bucket_alias": "surface",
        "target_bucket": "surface",
        "val_columns": ("damsire_avg_finish_at_track_val",),
    },
    {
        "table": "sire_running_style_stats",
        "alias": "srs",
        "key_alias": "sire",
        "bucket_alias": "rs_bucket",
        "target_bucket": "rs_bucket",
        "val_columns": (
            "sire_nige_rate_val",
            "sire_senkou_rate_val",
            "sire_sashi_rate_val",
            "sire_oikomi_rate_val",
            "sire_corner_1_norm_avg_val",
        ),
    },
    {
        "table": "sire_keibajo_stats",
        "alias": "sks",
        "key_alias": "sire",
        "bucket_alias": "keibajo_code",
        "target_bucket": "target_keibajo_code",
        "val_columns": ("sire_keibajo_win_rate_val",),
    },
    {
        "table": "damsire_keibajo_stats",
        "alias": "dks",
        "key_alias": "damsire",
        "bucket_alias": "keibajo_code",
        "target_bucket": "target_keibajo_code",
        "val_columns": ("damsire_keibajo_win_rate_val",),
    },
)


def pedigree_target_key_column(spec: PedigreeJoinSpec) -> str:
    """tp column holding the lineage id this stats table keys on (sire/damsire)."""
    return "target_sire" if spec["key_alias"] == "sire" else "target_damsire"


def pedigree_stats_index_sql(spec: PedigreeStatSpec) -> str:
    """CREATE INDEX on a stats temp table's composite probe key.

    The consolidated pedigree_features join probes each stats table by
    (key, bucket, stats_year_month); indexing that triple lets DuckDB use an
    index scan instead of building a full hash table per stats relation."""
    table = spec["table"]
    return (
        f"create index if not exists idx_{table} on {table}"
        f" ({spec['key_alias']}, {spec['bucket_alias']}, stats_year_month)"
    )


def target_pedigree_index_sql() -> str:
    """CREATE INDEX on target_pedigree's natural key (the consolidation anchor)."""
    keys = ", ".join(PEDIGREE_NATURAL_KEY)
    return f"create index if not exists idx_target_pedigree on target_pedigree ({keys})"


def pedigree_features_sql() -> str:
    """Pre-join target_pedigree with all stats tables into one temp table.

    Keyed at the target-row grain (PEDIGREE_NATURAL_KEY), it exposes every stats
    `_val` column plus each table's race_count (aliased `<alias>_race_count` to
    avoid collisions). base_features_select_sql then does a single LEFT JOIN on
    this table instead of 8 — one hash probe per row rather than eight."""
    tp_keys = ", ".join(f"tp.{col}" for col in PEDIGREE_NATURAL_KEY)
    select_cols = [tp_keys]
    joins: list[str] = []
    for spec in PEDIGREE_JOIN_SPECS:
        alias = spec["alias"]
        for val in spec["val_columns"]:
            select_cols.append(f"{alias}.{val} as {val}")
        select_cols.append(f"{alias}.race_count as {alias}_race_count")
        target_key = pedigree_target_key_column(spec)
        joins.append(
            f"    left join {spec['table']} {alias}"
            f" on {alias}.{spec['key_alias']} = tp.{target_key}"
            f" and {alias}.{spec['bucket_alias']} = tp.{spec['target_bucket']}"
            f" and {alias}.stats_year_month ="
            " cast(tp.kaisai_nen as int) * 100 + cast(substr(tp.kaisai_tsukihi, 1, 2) as int)"
        )
    columns = ",\n      ".join(select_cols)
    join_text = "\n".join(joins)
    return f"""
    create or replace temp table {PEDIGREE_FEATURES_TABLE} as
    select
      {columns}
    from target_pedigree tp
{join_text}
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
        avg(case when h.finish_position = 1 and h.shusso_tosu is not null and h.umaban * 2 <= h.shusso_tosu + 1 then 1 else 0 end) as track_bias_inside,
        avg(case when h.finish_position = 1 and h.corner1_norm is not null and cast(h.corner1_norm as double) <= {FRONT_CORNER_THRESHOLD} then 1 else 0 end) as track_bias_front
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


def horse_running_style_history_cte(target_filter: str = "true") -> str:
    """Per-horse running-style history aggregates from horse_history_base.

    Pure aggregation on horse_history_base (no JOIN). target_filter unused
    because per-year filtering is applied by the base materialization.
    """
    del target_filter
    return f"""
    horse_running_style_history as (
      select b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango,
        avg(b.corner1_norm)
          filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
          as past_corner_1_norm_avg_5,
        avg(b.corner1_norm)
          filter (where b.recent_rank <= 3)
          as past_corner_1_norm_avg_3,
        avg(b.corner1_norm)
          filter (where b.recent_rank <= 10)
          as past_corner_1_norm_avg_10,
        avg(b.corner4_norm - b.corner1_norm)
          filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
          as past_corner_progression_avg_5,
        stddev_samp(b.corner1_norm)
          filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
          as past_corner_1_norm_std_5,
        min(b.corner1_norm)
          filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
          as past_corner_1_norm_best_5,
        max(b.corner1_norm)
          filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
          as past_corner_1_norm_worst_5,
        avg(case when b.corner1_norm = 0 then 1.0
                 when b.corner1_norm is null then null
                 else 0.0 end) as past_nige_rate_self,
        avg(case when b.corner1_norm is null then null
                 when b.corner1_norm > 0 and b.corner1_norm <= {RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0
                 else 0.0 end) as past_senkou_rate_self,
        avg(case when b.corner1_norm is null then null
                 when b.corner1_norm > {RUNNING_STYLE_SENKOU_THRESHOLD}
                  and b.corner1_norm <= {RUNNING_STYLE_SASHI_THRESHOLD} then 1.0
                 else 0.0 end) as past_sashi_rate_self,
        avg(case when b.corner1_norm is null then null
                 when b.corner1_norm > {RUNNING_STYLE_SASHI_THRESHOLD} then 1.0
                 else 0.0 end) as past_oikomi_rate_self,
        max(b.corner1_norm) filter (where b.recent_rank = 1) as last_race_corner_1_norm,
        max(b.corner4_norm - b.corner1_norm) filter (where b.recent_rank = 1)
          as last_race_corner_progression,
        avg(b.corner1_norm) filter (where abs(b.history_kyori - b.target_kyori) <= {SAME_DISTANCE_TOLERANCE})
          as horse_distance_corner_1_norm_avg,
        avg(b.corner1_norm) filter (where left(coalesce(b.history_track_code, ''), 1)
                                       = left(coalesce(b.target_track_code, ''), 1))
          as horse_track_corner_1_norm_avg,
        avg(b.corner1_norm) filter (where b.history_keibajo = b.target_keibajo)
          as horse_keibajo_corner_1_norm_avg,
        avg(b.corner1_norm) filter (where coalesce(b.history_grade_code, '')
                                       = coalesce(b.target_grade_code, ''))
          as horse_grade_corner_1_norm_avg,
        avg(case when b.finish_position = 1 then 1.0 else 0.0 end)
          filter (where b.corner1_norm = 0)
          as past_nige_win_rate_self,
        avg(case when b.finish_position = 1 then 1.0 else 0.0 end)
          filter (where b.corner1_norm > 0
                    and b.corner1_norm <= {RUNNING_STYLE_SENKOU_THRESHOLD})
          as past_senkou_win_rate_self,
        avg(case when b.finish_position = 1 then 1.0 else 0.0 end)
          filter (where b.corner1_norm > {RUNNING_STYLE_SENKOU_THRESHOLD}
                    and b.corner1_norm <= {RUNNING_STYLE_SASHI_THRESHOLD})
          as past_sashi_win_rate_self,
        avg(case when b.finish_position = 1 then 1.0 else 0.0 end)
          filter (where b.corner1_norm > {RUNNING_STYLE_SASHI_THRESHOLD})
          as past_oikomi_win_rate_self,
        quantile_cont(b.corner1_norm, 0.75)
          filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
          - quantile_cont(b.corner1_norm, 0.25)
            filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
          as past_corner_1_norm_iqr_5,
        count(*) filter (
          where b.finish_position = 1
            and trim(coalesce(b.history_grade_code, '')) in ('A', 'B', 'C')
        )::bigint as top1_count_in_grade_races,
        count(*) filter (
          where b.finish_position between 1 and 3
            and trim(coalesce(b.history_grade_code, '')) in ('A', 'B', 'C')
        )::bigint as place_count_in_grade_races,
        count(*) filter (where trim(coalesce(b.history_grade_code, '')) = 'A')::bigint
          as experience_in_g1_race,
        count(*) filter (where b.finish_position = 1
                           and b.recent_rank <= {RECENT_WINDOW_SIZE})::bigint
          as recent_win_count_5,
        count(*) filter (where b.finish_position between 1 and 3
                           and b.recent_rank <= {RECENT_WINDOW_SIZE})::bigint
          as recent_top3_count_5,
        avg(b.kohan_3f) filter (where b.recent_rank <= 3) as last_3_avg_kohan_3f,
        greatest(
          count(*) filter (where b.corner1_norm = 0 and b.recent_rank <= {RECENT_WINDOW_SIZE}),
          count(*) filter (where b.corner1_norm > 0 and b.corner1_norm <= {RUNNING_STYLE_SENKOU_THRESHOLD}
                                  and b.recent_rank <= {RECENT_WINDOW_SIZE}),
          count(*) filter (where b.corner1_norm > {RUNNING_STYLE_SENKOU_THRESHOLD}
                                  and b.corner1_norm <= {RUNNING_STYLE_SASHI_THRESHOLD}
                                  and b.recent_rank <= {RECENT_WINDOW_SIZE}),
          count(*) filter (where b.corner1_norm > {RUNNING_STYLE_SASHI_THRESHOLD}
                                  and b.recent_rank <= {RECENT_WINDOW_SIZE})
        )::double / nullif(
          count(*) filter (where b.corner1_norm is not null
                             and b.recent_rank <= {RECENT_WINDOW_SIZE}), 0
        ) as past_dominant_label_consistency_5
      from horse_history_base b
      group by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango
    )
    """


def weight_cte(target_filter: str = "true") -> str:
    """Aggregate bataiju from pre-baked horse_history_base.history_bataiju.

    history_bataiju is baked into horse_history_base at materialization time
    via LEFT JOIN se_lookup (single UNION ALL of jra_se/nar_se with source col).
    current_bataiju comes from target_current_bataiju (target × se_lookup).
    weight_cte itself is a pure aggregation — no JOIN to jra_se/nar_se. This
    eliminates the 36.7M × 2 LEFT JOIN probe explosion + weight_history
    intermediate that previously dominated build time (>17 min on NAR v1).

    target_filter is unused (per-year filtering is applied via per-year base).
    Kept in signature for PER_YEAR_SPECS compatibility.
    """
    del target_filter
    return f"""
    weight_agg as (
      select b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango,
        max(tcb.current_bataiju) as current_bataiju_kept,
        max(tcb.target_zogen_sa) as zogen_sa,
        avg(b.history_bataiju) filter (where b.recent_rank <= {RECENT_WINDOW_SIZE}) as weight_avg_5,
        case when count(b.history_bataiju) filter (where b.recent_rank <= {RECENT_WINDOW_SIZE}) >= {WEIGHT_TREND_MIN_RACES}
             then regr_slope(b.history_bataiju, (-b.recent_rank)::double) filter (where b.recent_rank <= {RECENT_WINDOW_SIZE})
             else null end as weight_trend_5,
        stddev_pop(b.history_bataiju) filter (where b.recent_rank <= {RECENT_WINDOW_SIZE}) as weight_volatility_5
      from horse_history_base b
      left join target_current_bataiju tcb
        using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
      group by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango
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


def legacy_five_cte(target_filter: str = "true", category: str = CATEGORY_JRA) -> str:
    # Select empirical training medians by category.  Ban-ei shares NAR medians
    # as both are NAR-feed races with similar odds distributions.
    popularity_median = (
        POPULARITY_SCORE_MEDIAN_JRA if category == CATEGORY_JRA else POPULARITY_SCORE_MEDIAN_NAR
    )
    odds_median = (
        ODDS_SCORE_MEDIAN_JRA if category == CATEGORY_JRA else ODDS_SCORE_MEDIAN_NAR
    )
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
        coalesce(
          case when t.runner_count > 1 and t.ninkijun is not null
               then greatest(0::double, least(1::double, (t.ninkijun - 1)::double / nullif(t.runner_count - 1, 0)))
               else null end,
          {popularity_median}::double
        ) as popularity_score,
        coalesce(
          case when t.odds_value is not null and t.odds_value > 0
               then greatest(0::double, least(1::double, ln(greatest(t.odds_value, 1::double)) / ln(300::double)))
               else null end,
          {odds_median}::double
        ) as odds_score
      from legacy_target t
      left join legacy_horse_avg lha using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    )
    """


PEDIGREE_STAT_TABLES = tuple(spec["table"] for spec in PEDIGREE_STAT_SPECS)


def materialize_pedigree_stats(
    con: duckdb.DuckDBPyConnection,
    category: str,
) -> None:
    run_staged_sql(con, "pedigree.target_pedigree", target_pedigree_sql())
    con.execute(target_pedigree_index_sql())
    run_staged_sql(con, "pedigree.target_months", target_months_sql())
    run_staged_sql(con, "pedigree.rec_um", pedigree_rec_um_sql(category))
    for spec in PEDIGREE_STAT_SPECS:
        run_staged_sql(con, f"pedigree.{spec['table']}", pedigree_monthly_stat_sql(spec))
        con.execute(pedigree_stats_index_sql(spec))
    run_staged_sql(
        con,
        f"pedigree.{PEDIGREE_FEATURES_TABLE}",
        pedigree_features_sql(),
        PEDIGREE_FEATURES_TABLE,
    )
    for spec in PEDIGREE_JOIN_SPECS:
        drop_view_or_table(con, spec["table"])
    drop_view_or_table(con, "target_pedigree")


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


def venue_weather_empty_agg_sql() -> str:
    """Empty venue_weather_agg with the schema the LEFT JOIN expects.

    Used when --venue-weather-dir is not provided so the join still resolves and
    the venue weather columns are simply NULL for every row."""
    return """
    create or replace temp table venue_weather_agg (
      keibajo_code varchar,
      weather_date date,
      weather_date_yyyymmdd varchar,
      venue_temperature double,
      venue_precipitation_total double,
      venue_wind_speed_max double,
      venue_wind_gusts_max double
    )
    """


def venue_weather_files_for_years(venue_weather_dir: Path, years: list[int]) -> list[tuple[int, Path]]:
    """Return (year, path) for each build year that has a venue_weather_YYYY.duckdb."""
    found: list[tuple[int, Path]] = []
    for year in years:
        candidate = venue_weather_dir / f"venue_weather_{year:04d}.duckdb"
        if candidate.exists():
            found.append((year, candidate))
    return found


def materialize_venue_weather(
    con: duckdb.DuckDBPyConnection,
    venue_weather_dir: Path | None,
    years: list[int],
) -> None:
    """Build venue_weather_agg: per (keibajo_code, weather_date) summary of the
    open-meteo hourly weather over race hours, ATTACHed from the per-year
    venue_weather_YYYY.duckdb files. When the dir / files are absent the table is
    created empty so the downstream LEFT JOIN still resolves to NULL columns."""
    log_event("weather.venue_weather", "start", 0.0)
    started = perf_counter()
    files = (
        venue_weather_files_for_years(venue_weather_dir, years)
        if venue_weather_dir is not None
        else []
    )
    if not files:
        con.execute(venue_weather_empty_agg_sql())
        log_event("weather.venue_weather", "done", perf_counter() - started, 0)
        return
    aliases: list[str] = []
    union_parts: list[str] = []
    for year, path in files:
        alias = f"vw_{year:04d}"
        con.execute(f"attach '{path.as_posix()}' as {alias} (read_only)")
        aliases.append(alias)
        union_parts.append(
            "select keibajo_code, weather_date, weather_hour, "
            f"temperature, precipitation, wind_speed, wind_gusts from {alias}.venue_weather"
        )
    con.execute(
        "create or replace temp table venue_weather_raw as " + " union all ".join(union_parts)
    )
    for alias in aliases:
        con.execute(f"detach {alias}")
    con.execute(
        f"""
        create or replace temp table venue_weather_agg as
        select keibajo_code, weather_date,
          strftime(weather_date, '%Y%m%d') as weather_date_yyyymmdd,
          avg(temperature) as venue_temperature,
          sum(precipitation) as venue_precipitation_total,
          max(wind_speed) as venue_wind_speed_max,
          max(wind_gusts) as venue_wind_gusts_max
        from venue_weather_raw
        where weather_hour between {VENUE_WEATHER_RACE_HOUR_MIN} and {VENUE_WEATHER_RACE_HOUR_MAX}
        group by keibajo_code, weather_date
        """
    )
    con.execute("drop table if exists venue_weather_raw")
    agg_row = con.execute("select count(*) from venue_weather_agg").fetchone()
    agg_rows = int(agg_row[0]) if agg_row is not None else 0
    log_event("weather.venue_weather", "done", perf_counter() - started, agg_rows)


def materialize_weather_lookup(con: duckdb.DuckDBPyConnection) -> None:
    log_event("weather.weather_lookup", "start", 0.0)
    started = perf_counter()
    con.execute(
        """
        create or replace temp table weather_lookup as
        select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
          coalesce(jr.tenko_code, nr.tenko_code) as tenko_code,
          nr.kyoso_joken_meisho as nar_kyoso_joken_meisho,
          vw.venue_temperature,
          vw.venue_precipitation_total,
          vw.venue_wind_speed_max,
          vw.venue_wind_gusts_max
        from target t
        left join jra_ra jr on t.source='jra' and jr.kaisai_nen=t.kaisai_nen and jr.kaisai_tsukihi=t.kaisai_tsukihi
          and jr.keibajo_code=t.keibajo_code and jr.race_bango=t.race_bango
        left join nar_ra nr on t.source='nar' and nr.kaisai_nen=t.kaisai_nen and nr.kaisai_tsukihi=t.kaisai_tsukihi
          and nr.keibajo_code=t.keibajo_code and nr.race_bango=t.race_bango
        left join venue_weather_agg vw on vw.keibajo_code = t.keibajo_code
          and vw.weather_date_yyyymmdd = t.kaisai_nen || t.kaisai_tsukihi
        """
    )
    log_event("weather.weather_lookup", "done", perf_counter() - started)


def nar_subclass_case_sql(
    source_col: str,
    keibajo_col: str,
    meisho_col: str,
) -> str:
    """SQL case expression that derives `nar_subclass` from kyoso_joken_meisho.

    Order matters: highest tier first because OP races may also contain "Ａ" tokens
    in the meisho text. NULL is returned for JRA + Ban-ei rows (only emit for
    source='nar' AND keibajo_code <> '83').

    DuckDB's `~` operator does NOT match double-byte Japanese characters reliably,
    so we use `regexp_matches()` which handles UTF-8 correctly.
    """
    return f"""
    case
      when {source_col} <> 'nar' or {keibajo_col} = '83' then null
      when regexp_matches({meisho_col}, 'ＯＰ') then '{NAR_SUBCLASS_OP}'
      when regexp_matches({meisho_col}, '新馬') then '{NAR_SUBCLASS_NEW}'
      when regexp_matches({meisho_col}, '未勝利|未出走') then '{NAR_SUBCLASS_MUKATSU}'
      when regexp_matches({meisho_col}, '２歳|2歳') then '{NAR_SUBCLASS_2YO}'
      when regexp_matches({meisho_col}, '３歳|3歳') then '{NAR_SUBCLASS_3YO}'
      when regexp_matches({meisho_col}, 'Ａ') then '{NAR_SUBCLASS_A}'
      when regexp_matches({meisho_col}, 'Ｂ') then '{NAR_SUBCLASS_B}'
      when regexp_matches({meisho_col}, 'Ｃ') then '{NAR_SUBCLASS_C}'
      else '{NAR_SUBCLASS_OTHER}'
    end
    """


def base_features_select_sql(category: str) -> str:
    nar_subclass_expr = nar_subclass_case_sql(
        "t.source", "t.keibajo_code", "wl.nar_kyoso_joken_meisho"
    )
    return f"""
    select
      t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
      t.ketto_toroku_bango, t.umaban, t.category, t.kyori, t.track_code, t.grade_code, t.shusso_tosu,
      t.finish_position, t.finish_norm,
      t.kyoso_joken_code as kyoso_joken_code,
      {nar_subclass_expr} as nar_subclass,
      t.target_corner_1_norm, t.target_corner_3_norm, t.target_corner_4_norm, t.target_running_style_class,
      hc.speed_index_avg_5, hc.speed_index_best_5, hc.kohan3f_avg_5, hc.corner_pass_avg_5,
      hc.career_win_rate, hc.career_place_rate, hc.career_top1_count,
      hc.same_keibajo_win_rate, hc.same_distance_win_rate, hc.same_track_win_rate, hc.same_grade_win_rate,
      wa.weight_avg_5,
      cast(wa.current_bataiju_kept as double) - wa.weight_avg_5 as weight_diff_from_avg,
      wa.weight_trend_5,
      wa.weight_volatility_5,
      least(greatest((cast(wa.current_bataiju_kept as double) - wa.weight_avg_5) / nullif(greatest(wa.weight_volatility_5, {WEIGHT_ZSCORE_MIN_VOLATILITY}), 0), -{WEIGHT_ZSCORE_CLAMP}), {WEIGHT_ZSCORE_CLAMP}) as weight_zscore,
      hc.days_since_last_race, hc.consecutive_race_count,
      jc.jockey_career_win_rate, jc.jockey_recent_win_rate, jc.jockey_keibajo_win_rate,
      jc.jockey_distance_win_rate, jc.jockey_track_win_rate, jc.jockey_grade_win_rate,
      jc.jockey_horse_pair_count, jc.jockey_horse_pair_win_rate,
      jc.jockey_nige_rate, jc.jockey_senkou_rate, jc.jockey_sashi_rate, jc.jockey_oikomi_rate,
      jc.jockey_corner_1_norm_avg, jc.jockey_horse_corner_1_norm_avg,
      jc.jockey_recent_corner_1_norm_avg_90d, jc.jockey_recent_nige_rate_90d,
      jc.jockey_season_win_rate, jc.jockey_season_keibajo_win_rate, jc.jockey_keibajo_distance_win_rate,
      jc.jockey_season_keibajo_distance_win_rate, jc.jockey_season_keibajo_distance_count,
      tc.trainer_career_win_rate, tc.trainer_keibajo_win_rate, tc.trainer_distance_win_rate, tc.trainer_horse_win_rate,
      tc.trainer_nige_rate, tc.trainer_senkou_rate, tc.trainer_sashi_rate, tc.trainer_oikomi_rate,
      tc.trainer_corner_1_norm_avg,
      tc.trainer_grade_win_rate, tc.trainer_class_surface_season_win_rate, tc.trainer_class_surface_season_count,
      case when pf.sds_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_distance_win_rate_val else null end as sire_distance_win_rate,
      case when pf.sts_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_track_win_rate_val else null end as sire_track_win_rate,
      case when pf.dsd_race_count >= {PEDIGREE_MIN_RACES} then pf.dam_sire_distance_win_rate_val else null end as dam_sire_distance_win_rate,
      case when pf.sds_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_avg_finish_at_distance_val else null end as sire_avg_finish_at_distance,
      case when pf.dst_race_count >= {PEDIGREE_MIN_RACES} then pf.damsire_avg_finish_at_track_val else null end as damsire_avg_finish_at_track,
      case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_nige_rate_val else null end as sire_nige_rate,
      case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_senkou_rate_val else null end as sire_senkou_rate,
      case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_sashi_rate_val else null end as sire_sashi_rate,
      case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_oikomi_rate_val else null end as sire_oikomi_rate,
      case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_corner_1_norm_avg_val else null end as sire_corner_1_norm_avg,
      case when pf.sks_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_keibajo_win_rate_val else null end as sire_keibajo_win_rate,
      case when pf.dks_race_count >= {PEDIGREE_MIN_RACES} then pf.damsire_keibajo_win_rate_val else null end as damsire_keibajo_win_rate,
      (
        coalesce(case when pf.sds_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_distance_win_rate_val else null end, 0) +
        coalesce(case when pf.dsd_race_count >= {PEDIGREE_MIN_RACES} then pf.dam_sire_distance_win_rate_val else null end, 0) +
        coalesce(case when pf.sts_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_track_win_rate_val else null end, 0) +
        coalesce(case when pf.sks_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_keibajo_win_rate_val else null end, 0) +
        coalesce(case when pf.dks_race_count >= {PEDIGREE_MIN_RACES} then pf.damsire_keibajo_win_rate_val else null end, 0)
      ) / nullif(
        (case when pf.sds_race_count >= {PEDIGREE_MIN_RACES} then 1 else 0 end) +
        (case when pf.dsd_race_count >= {PEDIGREE_MIN_RACES} then 1 else 0 end) +
        (case when pf.sts_race_count >= {PEDIGREE_MIN_RACES} then 1 else 0 end) +
        (case when pf.sks_race_count >= {PEDIGREE_MIN_RACES} then 1 else 0 end) +
        (case when pf.dks_race_count >= {PEDIGREE_MIN_RACES} then 1 else 0 end),
        0
      )::double as pedigree_score_for_race,
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
      wl.venue_temperature,
      wl.venue_precipitation_total,
      wl.venue_wind_speed_max,
      wl.venue_wind_gusts_max,
      coalesce(wl.venue_precipitation_total, 0) * coalesce(hc.speed_index_avg_5, 0) as rain_x_speed_decay,
      coalesce(wl.venue_wind_speed_max, 0) * coalesce(rsh.past_nige_rate_self, 0) as wind_x_front_runner,
      coalesce(case when pf.sks_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_keibajo_win_rate_val else null end, 0) * coalesce(hc.same_keibajo_win_rate, 0) as pedigree_venue_x_horse_venue,
      coalesce(case when pf.sds_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_distance_win_rate_val else null end, 0) * coalesce(hc.same_distance_win_rate, 0) as pedigree_distance_x_horse_distance,
      case
        when rsh.past_nige_rate_self is null then null
        else
          coalesce(rsh.past_nige_rate_self, 0) * coalesce(case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_nige_rate_val else null end, 0) +
          coalesce(rsh.past_senkou_rate_self, 0) * coalesce(case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_senkou_rate_val else null end, 0) +
          coalesce(rsh.past_sashi_rate_self, 0) * coalesce(case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_sashi_rate_val else null end, 0) +
          coalesce(rsh.past_oikomi_rate_self, 0) * coalesce(case when pf.srs_race_count >= {PEDIGREE_MIN_RACES} then pf.sire_oikomi_rate_val else null end, 0)
      end as sire_style_x_horse_style_match,
      coalesce(wl.venue_wind_speed_max, 0) * (coalesce(t.shusso_tosu, 0)::double / {MAX_FIELD_SIZE}::double) as wind_x_field_size,
      coalesce(wl.venue_precipitation_total, 0) * coalesce(
        case
          when left(coalesce(t.track_code, ''), 1) = '1' then
            case t.babajotai_code_shiba when '1' then 0::double when '2' then 0.3::double when '3' then 0.6::double when '4' then 1.0::double else null end
          else
            case t.babajotai_code_dirt when '1' then 0::double when '2' then 0.3::double when '3' then 0.6::double when '4' then 1.0::double else null end
        end, 0) as rain_x_track_condition,
      (20.0 - least(20.0, greatest(0.0, coalesce(wl.venue_temperature, 20.0)))) * coalesce(hc.speed_index_avg_5, 0) as cold_x_speed_effect,
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
      rsh.past_corner_1_norm_avg_5,
      rsh.past_corner_1_norm_avg_3,
      rsh.past_corner_1_norm_avg_10,
      rsh.past_corner_progression_avg_5,
      rsh.past_corner_1_norm_std_5,
      rsh.past_corner_1_norm_best_5,
      rsh.past_corner_1_norm_worst_5,
      rsh.past_nige_rate_self,
      rsh.past_senkou_rate_self,
      rsh.past_sashi_rate_self,
      rsh.past_oikomi_rate_self,
      rsh.last_race_corner_1_norm,
      rsh.last_race_corner_progression,
      rsh.horse_distance_corner_1_norm_avg,
      rsh.horse_track_corner_1_norm_avg,
      rsh.horse_keibajo_corner_1_norm_avg,
      rsh.horse_grade_corner_1_norm_avg,
      rsh.past_nige_win_rate_self,
      rsh.past_senkou_win_rate_self,
      rsh.past_sashi_win_rate_self,
      rsh.past_oikomi_win_rate_self,
      rsh.past_corner_1_norm_iqr_5,
      rsh.top1_count_in_grade_races,
      rsh.place_count_in_grade_races,
      rsh.experience_in_g1_race,
      rsh.recent_win_count_5,
      rsh.recent_top3_count_5,
      rsh.past_dominant_label_consistency_5,
      rsh.last_3_avg_kohan_3f,
      case
        when t.shusso_tosu is null or t.shusso_tosu < {UMABAN_NORM_MIN_FIELD} then null
        when t.umaban is null then null
        else least(1.0, greatest(0.0,
          (cast(t.umaban as double) - 1) / (cast(t.shusso_tosu as double) - 1)
        ))
      end as umaban_norm,
      case
        when trim(coalesce(t.kyoso_joken_code, '')) = '{NEWCOMER_RACE_JOKEN_CODE}' then 1
        else 0
      end as is_newcomer_race,
      case
        when t.kyori is null then null
        when t.kyori <= {KYORI_BAND_SPRINT_MAX} then {KYORI_BAND_SPRINT}
        when t.kyori <= {KYORI_BAND_MILE_MAX} then {KYORI_BAND_MILE}
        when t.kyori <= {KYORI_BAND_INTERMEDIATE_MAX} then {KYORI_BAND_INTERMEDIATE}
        else {KYORI_BAND_LONG}
      end as kyori_band,
      case
        when t.kaisai_tsukihi is null or length(t.kaisai_tsukihi) < 2 then null
        when cast(substr(t.kaisai_tsukihi, 1, 2) as int) < 3 then {SEASON_WINTER}
        when cast(substr(t.kaisai_tsukihi, 1, 2) as int) <= {SEASON_SPRING_MAX_MONTH} then {SEASON_SPRING}
        when cast(substr(t.kaisai_tsukihi, 1, 2) as int) <= {SEASON_SUMMER_MAX_MONTH} then {SEASON_SUMMER}
        when cast(substr(t.kaisai_tsukihi, 1, 2) as int) <= {SEASON_AUTUMN_MAX_MONTH} then {SEASON_AUTUMN}
        else {SEASON_WINTER}
      end as season_band,
      t.feature_schema_version,
      t.race_year,
      t.tansho_odds,
      t.tansho_ninkijun,
      t.seibetsu_code,
      t.barei,
      wa.zogen_sa as zogen_sa,
      t.kaisai_month,
      t.source || ':' || t.kaisai_nen || ':' || t.kaisai_tsukihi || ':' || t.keibajo_code || ':' || t.race_bango as race_id
    from target t
    left join horse_career hc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join jockey_career jc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join trainer_career tc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join {PEDIGREE_FEATURES_TABLE} pf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join race_field_aggregates rfa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join race_top3_speed rts using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join track_bias tb using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join weight_agg wa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join recent_form rf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join legacy_features lf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join weather_lookup wl using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    left join horse_running_style_history rsh using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    """


RACE_PARTITION_COLUMNS = "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"


def assemble_final_select_from_temp_tables(category: str) -> str:
    base = base_features_select_sql(category)
    return f"""
    with base_features as ({base})
    select
      b.*,
      rank() over race_by_speed_avg_asc as speed_index_avg_5_rank_in_race,
      rank() over race_by_speed_best_asc as speed_index_best_5_rank_in_race,
      rank() over race_by_jockey_recent_desc as jockey_recent_win_rate_rank_in_race,
      rank() over race_by_trainer_career_desc as trainer_career_win_rate_rank_in_race,
      rank() over race_by_pedigree_desc as pedigree_score_for_race_rank_in_race,
      rank() over race_by_same_distance_desc as same_distance_win_rate_rank_in_race,
      b.speed_index_avg_5 - avg(b.speed_index_avg_5) over race_partition
        as speed_index_avg_5_diff_from_race_avg,
      b.jockey_recent_win_rate - avg(b.jockey_recent_win_rate) over race_partition
        as jockey_recent_win_rate_diff_from_race_avg,
      b.pedigree_score_for_race - avg(b.pedigree_score_for_race) over race_partition
        as pedigree_score_diff_from_race_avg
    from base_features b
    window
      race_partition as (partition by {RACE_PARTITION_COLUMNS}),
      race_by_speed_avg_asc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.speed_index_avg_5 asc nulls last
      ),
      race_by_speed_best_asc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.speed_index_best_5 asc nulls last
      ),
      race_by_jockey_recent_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.jockey_recent_win_rate desc nulls last
      ),
      race_by_trainer_career_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.trainer_career_win_rate desc nulls last
      ),
      race_by_pedigree_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.pedigree_score_for_race desc nulls last
      ),
      race_by_same_distance_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.same_distance_win_rate desc nulls last
      )
    """


def _window_query_from_base_table(table_name: str) -> str:
    """Build the window-function-only SELECT reading from *table_name*.

    This produces the same columns as ``assemble_final_select_from_temp_tables``
    but assumes the 17-table JOIN has already been materialized into
    *table_name*, avoiding repeated evaluation in the per-year write loop.
    """
    return f"""
    select
      b.*,
      rank() over race_by_speed_avg_asc as speed_index_avg_5_rank_in_race,
      rank() over race_by_speed_best_asc as speed_index_best_5_rank_in_race,
      rank() over race_by_jockey_recent_desc as jockey_recent_win_rate_rank_in_race,
      rank() over race_by_trainer_career_desc as trainer_career_win_rate_rank_in_race,
      rank() over race_by_pedigree_desc as pedigree_score_for_race_rank_in_race,
      rank() over race_by_same_distance_desc as same_distance_win_rate_rank_in_race,
      b.speed_index_avg_5 - avg(b.speed_index_avg_5) over race_partition
        as speed_index_avg_5_diff_from_race_avg,
      b.jockey_recent_win_rate - avg(b.jockey_recent_win_rate) over race_partition
        as jockey_recent_win_rate_diff_from_race_avg,
      b.pedigree_score_for_race - avg(b.pedigree_score_for_race) over race_partition
        as pedigree_score_diff_from_race_avg
    from {table_name} b
    window
      race_partition as (partition by {RACE_PARTITION_COLUMNS}),
      race_by_speed_avg_asc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.speed_index_avg_5 asc nulls last
      ),
      race_by_speed_best_asc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.speed_index_best_5 asc nulls last
      ),
      race_by_jockey_recent_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.jockey_recent_win_rate desc nulls last
      ),
      race_by_trainer_career_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.trainer_career_win_rate desc nulls last
      ),
      race_by_pedigree_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.pedigree_score_for_race desc nulls last
      ),
      race_by_same_distance_desc as (
        partition by {RACE_PARTITION_COLUMNS} order by b.same_distance_win_rate desc nulls last
      )
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
    # Discover years from the ``target`` temp table (already present in ``con``)
    # so we can materialize one year at a time and avoid OOM on large datasets
    # (NAR: 2.89M rows).  The previous approach materialized *all* rows into a
    # staging table first, which required holding the full result set in memory.
    # ``final_query`` contains window functions partitioned per-race, so adding
    # an outer ``WHERE race_year = <year>`` is safe — it cannot change window
    # results because no race spans multiple years.
    year_rows = con.execute(
        "select distinct race_year from target order by race_year"
    ).fetchall()
    threads_row = con.execute("select current_setting('threads')").fetchone()
    original_threads = int(threads_row[0]) if threads_row is not None else DEFAULT_THREADS
    con.execute("set threads = 1")
    for row in year_rows:
        year = int(row[0])
        year_query = f"select * from ({final_query}) _fq where race_year = {year}"
        con.execute(f"create or replace temp table _parquet_staging as {year_query}")
        con.execute(
            f"""
            copy (select * from _parquet_staging)
            to '{output_dir.as_posix()}'
            (format parquet, partition_by (race_year), overwrite_or_ignore true)
            """
        )
        con.execute("drop table if exists _parquet_staging")
    con.execute(f"set threads = {original_threads}")


class BuildResult(TypedDict):
    elapsed_seconds: float
    output_dir: str
    rows_written: int


_log_file: TextIO | None = None


def set_log_file(path: Path | None) -> None:
    global _log_file
    if path is None:
        _log_file = None
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    _log_file = path.open("a", encoding="utf-8", buffering=1)


def close_log_file() -> None:
    global _log_file
    if _log_file is not None:
        _log_file.close()
        _log_file = None


def emit_log_line(line: str) -> None:
    print(line, flush=True)
    if _log_file is not None:
        _log_file.write(line + "\n")
        _log_file.flush()


def log_event(stage: str, status: str, elapsed_seconds: float, rows: int | None = None) -> None:
    payload: dict[str, object] = {
        "stage": stage,
        "status": status,
        "elapsed_seconds": round(elapsed_seconds, 2),
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }
    if rows is not None:
        payload["rows"] = rows
    emit_log_line(json.dumps(payload, ensure_ascii=False))


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
        emit_log_line(json.dumps(payload, ensure_ascii=False))
        write_status_atomic(self.status_path, payload)


def get_target_years(con: duckdb.DuckDBPyConnection) -> list[int]:
    rows = con.execute("select distinct race_year from target order by race_year").fetchall()
    return [int(row[0]) for row in rows]


def chunk_years(years: list[int], batch_size: int) -> list[list[int]]:
    """Split a sorted distinct-years list into consecutive chunks of at most batch_size.

    batch_size <= 0 is treated as 1 so a misconfiguration degrades to the original
    one-year-per-pass behaviour instead of an empty/zero-step range.
    """
    step = batch_size if batch_size > 0 else 1
    return [years[i : i + step] for i in range(0, len(years), step)]


def year_in_filter(year_chunk: list[int]) -> str:
    """SQL filter restricting target rows to the given years via an explicit IN list.

    IN (not BETWEEN) so years absent from the distinct-years list are never pulled in,
    which matters for gapped year lists and resumed/partial runs.
    """
    literals = ", ".join(f"'{year:04d}'" for year in year_chunk)
    return f"t.kaisai_nen in ({literals})"


def materialize_temp_table_by_year(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    temp_name: str,
    cte_builder: Callable[[str], str],
    final_cte: str,
    years: list[int],
    heartbeat: Heartbeat,
    batch_size: int = 1,
) -> int:
    log_event(stage, "start", 0.0)
    overall_start = perf_counter()
    for idx, year_chunk in enumerate(chunk_years(years, batch_size)):
        label = "-".join(f"{year:04d}" for year in year_chunk)
        heartbeat.set_substage(f"years={label}")
        chunk_start = perf_counter()
        filter_clause = year_in_filter(year_chunk)
        cte_text = cte_builder(filter_clause)
        if idx == 0:
            con.execute(
                f"create or replace temp table {temp_name} as with {cte_text} select * from {final_cte}"
            )
        else:
            con.execute(
                f"insert into {temp_name} with {cte_text} select * from {final_cte}"
            )
        log_event(f"{stage}.years{label}", "done", perf_counter() - chunk_start)
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
      cast(h.corner1_norm as double) as corner1_norm,
      cast(h.corner3_norm as double) as corner3_norm,
      cast(h.corner4_norm as double) as corner4_norm,
      h.kyori as history_kyori,
      h.track_code as history_track_code,
      h.grade_code as history_grade_code,
      (case h.kyoso_joken_code
        when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
        when '701' then 4 when '703' then 5 when '999' then 6
        else null end) as history_class_level,
      hs.bataiju as history_bataiju,
      row_number() over (
        partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
        order by h.race_date desc
      ) as recent_rank
"""

HORSE_HISTORY_BASE_FROM = """
        from target t
        join rec h
          on h.source = t.source
          and h.ketto_toroku_bango = t.ketto_toroku_bango
          and h.race_date < t.race_date
          and h.race_dt >= t.race_dt - interval '{years} years'
        left join se_lookup hs
          on hs.source = h.source
          and hs.kaisai_nen = h.kaisai_nen
          and hs.kaisai_tsukihi = h.kaisai_tsukihi
          and hs.keibajo_code = h.keibajo_code
          and hs.race_bango = h.race_bango
          and hs.ketto_toroku_bango = h.ketto_toroku_bango
"""


def materialize_horse_history_base(con: duckdb.DuckDBPyConnection, target_filter: str) -> int:
    from_clause = HORSE_HISTORY_BASE_FROM.format(years=HISTORY_LOOKBACK_YEARS)
    con.execute(
        f"""
        create or replace temp table horse_history_base as
        {HORSE_HISTORY_BASE_SELECT}
        {from_clause}
        where h.finish_position is not null and ({target_filter})
        """
    )
    con.execute(
        "create index horse_history_base_target_idx on horse_history_base "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
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


def materialize_se_lookup(con: duckdb.DuckDBPyConnection) -> int:
    """Single UNION ALL bataiju lookup over jra_se / nar_se with source col.

    Replaces the two-LEFT-JOIN pattern (jra_se + nar_se, source-conditional)
    with a single LEFT JOIN to a unified lookup table. Cuts probe count in
    half wherever bataiju is needed (horse_history_base + target_current_bataiju).
    """
    log_event("se_lookup.build", "start", 0.0)
    started = perf_counter()
    con.execute(
        f"""
        create or replace temp table se_lookup as
        select 'jra' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango,
               try_cast(nullif(trim(cast(bataiju as varchar)), '') as int) as bataiju,
               {signed_zogen_sa_sql("zogen_fugo", "zogen_sa")} as zogen_sa
        from jra_se where ketto_toroku_bango is not null
        union all
        select 'nar' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango,
               try_cast(nullif(trim(cast(bataiju as varchar)), '') as int) as bataiju,
               {signed_zogen_sa_sql("zogen_fugo", "zogen_sa")} as zogen_sa
        from nar_se where ketto_toroku_bango is not null
        """
    )
    con.execute(
        "create index se_lookup_idx on se_lookup "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )
    row = con.execute("select count(*) from se_lookup").fetchone()
    rc = int(row[0]) if row is not None else 0
    log_event("se_lookup.build", "done", perf_counter() - started, rc)
    return rc


def materialize_target_current_bataiju(con: duckdb.DuckDBPyConnection) -> int:
    """Pre-compute current_bataiju per target row via single LEFT JOIN to se_lookup."""
    log_event("target_current_bataiju.build", "start", 0.0)
    started = perf_counter()
    con.execute(
        """
        create or replace temp table target_current_bataiju as
        select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
               t.ketto_toroku_bango, s.bataiju as current_bataiju,
               s.zogen_sa as target_zogen_sa
        from target t
        left join se_lookup s
          using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
        """
    )
    row = con.execute("select count(*) from target_current_bataiju").fetchone()
    rc = int(row[0]) if row is not None else 0
    log_event("target_current_bataiju.build", "done", perf_counter() - started, rc)
    return rc


def build_per_year_specs(category: str) -> list[DerivedStageSpec]:
    return [
        {"name": "horse_career", "cte_builder": lambda _: horse_career_cte(), "final_cte": "horse_career"},
        {"name": "recent_form", "cte_builder": lambda _: recent_form_cte(), "final_cte": "recent_form"},
        {
            "name": "legacy_features",
            "cte_builder": lambda tf, cat=category: legacy_five_cte(tf, cat),
            "final_cte": "legacy_features",
        },
        {"name": "weight_agg", "cte_builder": weight_cte, "final_cte": "weight_agg"},
        {
            "name": "horse_running_style_history",
            "cte_builder": horse_running_style_history_cte,
            "final_cte": "horse_running_style_history",
        },
    ]


def stage_horse_history_derived(
    con: duckdb.DuckDBPyConnection,
    years: list[int],
    heartbeat: Heartbeat,
    category: str = CATEGORY_JRA,
) -> None:
    per_year_specs = build_per_year_specs(category)
    log_event("horse_history_derived", "start", 0.0)
    overall_start = perf_counter()
    heartbeat.set_substage("se_lookup")
    materialize_se_lookup(con)
    heartbeat.set_substage("target_current_bataiju")
    materialize_target_current_bataiju(con)
    for idx, year in enumerate(years):
        heartbeat.set_substage(f"year={year}")
        year_filter = f"t.kaisai_nen = '{year:04d}'"
        base_start = perf_counter()
        base_rows = materialize_horse_history_base(con, year_filter)
        log_event(f"horse_history_base.year{year}", "done", perf_counter() - base_start, base_rows)
        for spec in per_year_specs:
            stage_start = perf_counter()
            execute_derived_stage(con, spec, year_filter, idx == 0)
            log_event(f"{spec['name']}.year{year}", "done", perf_counter() - stage_start)
    for spec in per_year_specs:
        row_result = con.execute(f"select count(*) from {spec['final_cte']}").fetchone()
        rows = int(row_result[0]) if row_result is not None else 0
        log_event(f"{spec['name']}.total", "done", 0.0, rows)
    log_event("horse_history_derived", "done", perf_counter() - overall_start)


def configure_duckdb_session(
    con: duckdb.DuckDBPyConnection,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None = None,
) -> None:
    con.execute(f"set threads to {int(threads)}")
    con.execute(f"set memory_limit = '{memory_limit}'")
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"set temp_directory = '{temp_dir.as_posix()}'")
    for stmt in (
        "PRAGMA enable_object_cache=true",
        "SET enable_progress_bar_print = false",
        "SET preserve_insertion_order = false",
        "SET max_temp_directory_size = '200GB'",
    ):
        try:
            con.execute(stmt)
        except duckdb.Error:
            pass


def stage_source(
    con: duckdb.DuckDBPyConnection,
    pg_url: str,
    from_date: str,
    to_date: str,
    category: str,
    upcoming_window: tuple[str, str] | None = None,
    realtime_odds_path: Path | None = None,
    target_race: tuple[str, str] | None = None,
) -> None:
    log_event("source.stage", "start", 0.0)
    started = perf_counter()
    install_and_attach_pg(con, pg_url)
    stage_source_tables(
        con, from_date, to_date, category, upcoming_window, realtime_odds_path, target_race
    )
    log_event("source.stage", "done", perf_counter() - started)


def stage_target(
    con: duckdb.DuckDBPyConnection,
    category: str,
    from_date: str,
    to_date: str,
    target_race: tuple[str, str] | None = None,
) -> int:
    log_event("target.build", "start", 0.0)
    started = perf_counter()
    build_target_table(con, category, from_date, to_date, target_race)
    target_row_result = con.execute("select count(*) from target").fetchone()
    target_rows = int(target_row_result[0]) if target_row_result is not None else 0
    log_event("target.build", "done", perf_counter() - started, target_rows)
    shrink_se_tables_to_target_horses(con)
    return target_rows


def _shrink_se_table_to_target_horses(con: duckdb.DuckDBPyConnection, table: str) -> int:
    """Replace ``table`` with its target-horse subset, then index it.

    Works whether ``table`` is a temp table (fresh run) or a parquet-backed view
    (restored from a SOURCE checkpoint): the filtered rows are staged into a
    scratch temp table, the original object is dropped via ``drop_view_or_table``
    (``create or replace temp table`` cannot replace a view), and the scratch is
    renamed in. Returns the resulting row count."""
    scratch = f"_{table}_shrunk"
    con.execute(
        f"create or replace temp table {scratch} as "
        f"select * from {table} "
        "where ketto_toroku_bango in (select ketto_toroku_bango from _target_horse_ids)"
    )
    drop_view_or_table(con, table)
    con.execute(f"alter table {scratch} rename to {table}")
    con.execute(
        f"create index {table}_jk_idx on {table} "
        "(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )
    row = con.execute(f"select count(*) from {table}").fetchone()
    return int(row[0]) if row is not None else 0


def shrink_se_tables_to_target_horses(con: duckdb.DuckDBPyConnection) -> None:
    """Restrict jra_se / nar_se to bataiju lookup rows for target horses only.

    weight_cte is the sole consumer of jra_se / nar_se (bataiju lookup).
    Restricting them to ketto_toroku_bango that appear in target keeps full
    historical bataiju (transferred horses included via source rec history),
    while dramatically shrinking hash-join input size for large NAR/JRA builds.
    """
    log_event("target.shrink_se", "start", 0.0)
    started = perf_counter()
    con.execute(
        "create or replace temp table _target_horse_ids as "
        "select distinct ketto_toroku_bango from target"
    )
    horse_count_row = con.execute("select count(*) from _target_horse_ids").fetchone()
    horse_count = int(horse_count_row[0]) if horse_count_row is not None else 0
    jra_rows = _shrink_se_table_to_target_horses(con, "jra_se")
    nar_rows = _shrink_se_table_to_target_horses(con, "nar_se")
    elapsed = perf_counter() - started
    emit_log_line(
        json.dumps(
            {
                "stage": "target.shrink_se",
                "status": "done",
                "elapsed_seconds": round(elapsed, 2),
                "target_horses": horse_count,
                "jra_se_rows": jra_rows,
                "nar_se_rows": nar_rows,
            },
            ensure_ascii=False,
        )
    )


def stage_partner_features(
    con: duckdb.DuckDBPyConnection,
    years: list[int],
    heartbeat: Heartbeat,
) -> None:
    heartbeat.set_stage("jockey_career")
    materialize_temp_table_by_year(
        con, "jockey_career", "jockey_career", jockey_cte, "jockey_career", years, heartbeat,
        batch_size=PARTNER_CAREER_YEAR_BATCH_SIZE,
    )
    heartbeat.set_stage("trainer_career")
    materialize_temp_table_by_year(
        con, "trainer_career", "trainer_career", trainer_cte, "trainer_career", years, heartbeat,
        batch_size=PARTNER_CAREER_YEAR_BATCH_SIZE,
    )


def stage_track_bias(
    con: duckdb.DuckDBPyConnection,
    years: list[int],
    heartbeat: Heartbeat,
) -> None:
    # The 5-day lookback window is selective enough that a single pass fits in
    # memory, so unlike jockey/trainer career this stage skips the per-year loop.
    del years
    heartbeat.set_stage("track_bias")
    materialize_temp_table(
        con, "track_bias", "track_bias", track_bias_cte("true"), "track_bias",
    )


def resolve_output_rows(args: argparse.Namespace, target_rows: int) -> int:
    if args.skip_count:
        return target_rows
    return count_output_rows(args.output_dir)


# Reference list of every temp table that is spilled to parquet before the final
# JOIN. Spilling happens progressively (per stage) in run() so peak memory stays
# low; the per-stage groups below partition this list by the stage that produces
# them. `target`, `weight_agg`, `recent_form` and `legacy_features` are spilled
# last because `target` is read by almost every stage and the other three are
# only consumed by the final JOIN but created in the first stage.
SPILL_TABLES: tuple[str, ...] = (
    "target",
    "horse_career",
    "jockey_career",
    "trainer_career",
    "pedigree_features",
    "race_field_aggregates",
    "race_top3_speed",
    "track_bias",
    "weight_agg",
    "recent_form",
    "legacy_features",
    "weather_lookup",
    "horse_running_style_history",
)

# Per-stage spill groups: tables each stage produces that are only needed by the
# final JOIN (or by a spill-as-view downstream). Spilling these right after their
# stage keeps memory bounded. `horse_career` is also read by materialize_race_context
# but, once spilled, is served as a parquet-backed view so that read still works.
# `jockey_career` / `trainer_career` are produced by stage_partner_features (not
# stage_horse_history_derived), so they spill in the partner group.
SPILL_AFTER_HORSE_HISTORY: tuple[str, ...] = ("horse_career",)
SPILL_AFTER_PARTNER: tuple[str, ...] = (
    "jockey_career",
    "trainer_career",
    "horse_running_style_history",
)
SPILL_AFTER_PEDIGREE: tuple[str, ...] = ("pedigree_features",)
SPILL_AFTER_RACE_CONTEXT: tuple[str, ...] = (
    "race_field_aggregates",
    "race_top3_speed",
)
SPILL_AFTER_TRACK_BIAS: tuple[str, ...] = ("track_bias",)
SPILL_AFTER_WEATHER: tuple[str, ...] = ("weather_lookup",)
SPILL_BEFORE_PARQUET: tuple[str, ...] = (
    "target",
    "weight_agg",
    "recent_form",
    "legacy_features",
)


def spill_temp_tables_to_disk(
    con: duckdb.DuckDBPyConnection,
    temp_dir: Path | None,
    tables: tuple[str, ...] = SPILL_TABLES,
) -> None:
    """Free working memory by spilling the given temp tables to parquet and
    replacing them with views. The view names match the original table names so
    queries reading them (the final SELECT, downstream stages) are unchanged.

    Called progressively after each stage so peak memory stays low instead of
    accumulating every temp table until a single end-of-run spill."""
    spill_dir = Path(temp_dir.as_posix() if temp_dir is not None else "/tmp/duckdb-spill") / "table_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    for spill_table in tables:
        parquet_path = (spill_dir / f"{spill_table}.parquet").as_posix()
        con.execute(f"copy {spill_table} to '{parquet_path}' (format parquet)")
        con.execute(f"drop table {spill_table}")
        con.execute(
            f"create or replace view {spill_table} as select * from read_parquet('{parquet_path}')"
        )


# ---------------------------------------------------------------------------
# Resumable / incremental checkpointing
# ---------------------------------------------------------------------------
#
# After each pipeline stage, the tables it produces are already spilled to
# parquet in {temp_dir}/table_spill/ (see spill_temp_tables_to_disk). The
# checkpoint manifest records, per stage, which parquet files were written, how
# many rows each held and a SHA-256 of the SQL that produced them. A subsequent
# --resume / --incremental run can then skip a stage and re-attach its parquet
# as views, provided the SQL fingerprint still matches and every file is intact.

# Logical checkpoint stages, in dependency order. SOURCE is the root; the
# horse-history .. weather stages each depend only on SOURCE (plus TARGET);
# PARQUET_WRITE depends on every other stage.
CHECKPOINT_SOURCE = "source"
CHECKPOINT_TARGET = "target"
CHECKPOINT_HORSE_HISTORY = "horse_history_derived"
CHECKPOINT_PARTNER = "partner_features"
CHECKPOINT_PEDIGREE = "pedigree"
CHECKPOINT_RACE_CONTEXT = "race_context"
CHECKPOINT_TRACK_BIAS = "track_bias"
CHECKPOINT_WEATHER = "weather_lookup"
CHECKPOINT_PARQUET_WRITE = "parquet_write"

CHECKPOINT_STAGE_ORDER: tuple[str, ...] = (
    CHECKPOINT_SOURCE,
    CHECKPOINT_TARGET,
    CHECKPOINT_HORSE_HISTORY,
    CHECKPOINT_PARTNER,
    CHECKPOINT_PEDIGREE,
    CHECKPOINT_RACE_CONTEXT,
    CHECKPOINT_TRACK_BIAS,
    CHECKPOINT_WEATHER,
    CHECKPOINT_PARQUET_WRITE,
)

# Source tables spilled (and restored) for the SOURCE checkpoint. These are the
# raw PG-backed temp tables every downstream stage reads. `target` is spilled by
# the TARGET checkpoint, not here, because it is produced by a later stage.
SOURCE_SPILL_TABLES: tuple[str, ...] = (
    "rec",
    "nar_se",
    "nar_um",
    "nar_nu",
    "nar_ra",
    "jra_se",
    "jra_um",
    "jra_ra",
)

CHECKPOINT_VERSION = 1
CHECKPOINT_STATUS_DONE = "done"

# Tables each checkpoint stage OWNS — i.e. produces and is responsible for
# spilling + recording. Ownership is by producer, so a restored stage re-attaches
# exactly the parquet it wrote. In checkpoint mode these are spilled right after
# the producing stage (instead of the memory-tuned interleaved order used on a
# plain run) so each checkpoint is self-contained and independently restorable.
# `weight_agg` / `recent_form` / `legacy_features` are produced by
# stage_horse_history_derived; `horse_running_style_history` likewise. The plain
# (non-checkpoint) run spills them later for memory reasons, but for resume they
# belong to the stage that built them.
CHECKPOINT_STAGE_TABLES: dict[str, tuple[str, ...]] = {
    CHECKPOINT_SOURCE: SOURCE_SPILL_TABLES,
    CHECKPOINT_TARGET: ("target",),
    CHECKPOINT_HORSE_HISTORY: (
        "horse_career",
        "weight_agg",
        "recent_form",
        "legacy_features",
        "horse_running_style_history",
    ),
    CHECKPOINT_PARTNER: ("jockey_career", "trainer_career"),
    CHECKPOINT_PEDIGREE: ("pedigree_features",),
    CHECKPOINT_RACE_CONTEXT: ("race_field_aggregates", "race_top3_speed"),
    CHECKPOINT_TRACK_BIAS: ("track_bias",),
    CHECKPOINT_WEATHER: ("weather_lookup",),
}


@dataclass
class StageCheckpoint:
    status: str
    tables: list[str]
    row_counts: dict[str, int]
    query_hash: str
    timestamp: float


@dataclass
class CheckpointManifest:
    version: int = CHECKPOINT_VERSION
    category: str = ""
    from_date: str = ""
    to_date: str = ""
    stages: dict[str, StageCheckpoint] = field(default_factory=dict)

    @staticmethod
    def path(temp_dir: Path) -> Path:
        return temp_dir / "table_spill" / "checkpoint.json"

    @classmethod
    def load(cls, temp_dir: Path) -> CheckpointManifest | None:
        manifest_path = cls.path(temp_dir)
        if not manifest_path.exists():
            return None
        try:
            raw = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        if not isinstance(raw, dict):
            return None
        stages_raw = raw.get("stages", {})
        stages: dict[str, StageCheckpoint] = {}
        if isinstance(stages_raw, dict):
            for stage_name, payload in stages_raw.items():
                if isinstance(payload, dict):
                    stages[str(stage_name)] = StageCheckpoint(
                        status=str(payload.get("status", "")),
                        tables=[str(t) for t in payload.get("tables", [])],
                        row_counts={
                            str(k): int(v)
                            for k, v in dict(payload.get("row_counts", {})).items()
                        },
                        query_hash=str(payload.get("query_hash", "")),
                        timestamp=float(payload.get("timestamp", 0.0)),
                    )
        return cls(
            version=int(raw.get("version", CHECKPOINT_VERSION)),
            category=str(raw.get("category", "")),
            from_date=str(raw.get("from_date", "")),
            to_date=str(raw.get("to_date", "")),
            stages=stages,
        )

    def save(self, temp_dir: Path) -> None:
        manifest_path = self.path(temp_dir)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2))
        tmp_path.replace(manifest_path)

    def mark_done(
        self,
        stage_name: str,
        tables: list[str],
        row_counts: dict[str, int],
        query_hash: str,
        temp_dir: Path,
    ) -> None:
        self.stages[stage_name] = StageCheckpoint(
            status=CHECKPOINT_STATUS_DONE,
            tables=tables,
            row_counts=row_counts,
            query_hash=query_hash,
            timestamp=datetime.now(timezone.utc).timestamp(),
        )
        self.save(temp_dir)

    def invalidate(self, stage_name: str, temp_dir: Path) -> None:
        if stage_name in self.stages:
            del self.stages[stage_name]
            self.save(temp_dir)

    def is_stage_valid(self, stage_name: str, current_hash: str, spill_dir: Path) -> bool:
        checkpoint = self.stages.get(stage_name)
        if checkpoint is None:
            return False
        if checkpoint.status != CHECKPOINT_STATUS_DONE:
            return False
        if checkpoint.query_hash != current_hash:
            return False
        return all((spill_dir / table).exists() for table in checkpoint.tables)


def _stage_sql_fingerprint(
    stage_name: str,
    category: str,
    venue_weather_dir: Path | None,
) -> str:
    """Concatenate the SQL each stage would generate, as a change fingerprint.

    A stage's fingerprint changes whenever any SQL builder feeding it changes, so
    a checkpoint produced by older SQL is correctly invalidated on the next run.
    """
    if stage_name == CHECKPOINT_SOURCE:
        return build_rec_select_sql(category, "00000000", "99999999")
    if stage_name == CHECKPOINT_TARGET:
        return build_target_fingerprint(category)
    if stage_name == CHECKPOINT_HORSE_HISTORY:
        parts = [HORSE_HISTORY_BASE_SELECT, HORSE_HISTORY_BASE_FROM]
        parts.extend(spec["cte_builder"]("true") for spec in build_per_year_specs(category))
        return "\n".join(parts)
    if stage_name == CHECKPOINT_PARTNER:
        return jockey_cte() + trainer_cte()
    if stage_name == CHECKPOINT_PEDIGREE:
        parts = [
            target_pedigree_sql(),
            target_months_sql(),
            pedigree_rec_um_sql(category),
        ]
        parts.extend(pedigree_monthly_stat_sql(spec) for spec in PEDIGREE_STAT_SPECS)
        parts.append(pedigree_features_sql())
        return "\n".join(parts)
    if stage_name == CHECKPOINT_RACE_CONTEXT:
        return race_context_cte()
    if stage_name == CHECKPOINT_TRACK_BIAS:
        return track_bias_cte()
    if stage_name == CHECKPOINT_WEATHER:
        venue_indicator = "venue" if venue_weather_dir is not None else "no-venue"
        return f"weather_lookup::v1::{venue_indicator}"
    if stage_name == CHECKPOINT_PARQUET_WRITE:
        return assemble_final_select_from_temp_tables(category)
    return stage_name


def build_target_fingerprint(category: str) -> str:
    """SQL fingerprint for the TARGET stage (build_target_table parameters)."""
    return category_source_filter(category, "rec") + "||" + category_expression(category)


def compute_stage_hash(
    stage_name: str,
    category: str,
    from_date: str,
    to_date: str,
    years: list[int],
    extra: str = "",
) -> str:
    fingerprint = _stage_sql_fingerprint(
        stage_name,
        category,
        Path(extra) if extra != "" else None,
    )
    parts = [
        stage_name,
        category,
        from_date,
        to_date,
        ",".join(str(year) for year in years),
        extra,
        fingerprint,
    ]
    return hashlib.sha256(" ".join(parts).encode("utf-8")).hexdigest()


def restore_stage_from_spill(
    con: duckdb.DuckDBPyConnection,
    stage_name: str,
    checkpoint: StageCheckpoint,
    spill_dir: Path,
) -> bool:
    """Re-attach a stage's spilled parquet files as views.

    Returns False (and leaves any partially created views in place) if any
    parquet file is missing or unreadable, so the caller falls back to running
    the stage fresh.
    """
    log_event(f"checkpoint.restore.{stage_name}", "start", 0.0)
    started = perf_counter()
    for table in checkpoint.tables:
        parquet_path = spill_dir / table
        if not parquet_path.exists():
            log_event(f"checkpoint.restore.{stage_name}", "miss", perf_counter() - started)
            return False
        view_name = parquet_path.stem
        try:
            con.execute(
                f"create or replace view {view_name} as "
                f"select * from read_parquet('{parquet_path.as_posix()}')"
            )
        except duckdb.Error:
            log_event(f"checkpoint.restore.{stage_name}", "error", perf_counter() - started)
            return False
    log_event(f"checkpoint.restore.{stage_name}", "done", perf_counter() - started)
    return True


def spilled_table_files(spill_dir: Path, tables: tuple[str, ...]) -> list[str]:
    """Parquet filenames (e.g. ['target.parquet']) for the given spill tables."""
    return [f"{table}.parquet" for table in tables]


def spilled_row_counts(con: duckdb.DuckDBPyConnection, tables: tuple[str, ...]) -> dict[str, int]:
    """Row count of each (already-spilled, view-backed) table for the manifest."""
    counts: dict[str, int] = {}
    for table in tables:
        row = con.execute(f"select count(*) from {table}").fetchone()
        counts[f"{table}.parquet"] = int(row[0]) if row is not None else 0
    return counts


def drop_view_or_table(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Drop ``name`` whether it is a temp table (fresh run) or a view (restored).

    A table restored from a checkpoint is a parquet-backed view, so the plain
    ``drop table`` used on a fresh run would fail. DuckDB's ``drop view if
    exists`` / ``drop table if exists`` still raise a type-mismatch error when
    the object exists but is the other kind, so the catalog is consulted first
    and only the matching ``drop`` is issued."""
    rows = con.execute(
        "select table_type from information_schema.tables where table_name = ?",
        [name],
    ).fetchall()
    types = {str(row[0]) for row in rows}
    if "VIEW" in types:
        con.execute(f"drop view if exists {name}")
    if "BASE TABLE" in types or "LOCAL TEMPORARY" in types:
        con.execute(f"drop table if exists {name}")


def extract_years_from_target(con: duckdb.DuckDBPyConnection) -> list[int]:
    """Distinct race years from the (possibly restored-as-view) target relation."""
    return get_target_years(con)


def target_rows_from_target(con: duckdb.DuckDBPyConnection) -> int:
    """Row count of the target relation, whether it is a temp table or a view."""
    row = con.execute("select count(*) from target").fetchone()
    return int(row[0]) if row is not None else 0


@dataclass
class CheckpointController:
    """Drives per-stage skip / restore / record decisions for run().

    Holds the manifest, the spill directory and the build parameters needed to
    compute per-stage hashes. ``active`` is False when neither --resume nor
    --incremental was passed, in which case every method is a no-op and run()
    behaves exactly as before.
    """

    active: bool
    incremental: bool
    manifest: CheckpointManifest
    temp_dir: Path
    spill_dir: Path
    category: str
    from_date: str
    to_date: str
    venue_weather_extra: str

    def stage_hash(self, stage_name: str, years: list[int]) -> str:
        return compute_stage_hash(
            stage_name,
            self.category,
            self.from_date,
            self.to_date,
            years,
            self.venue_weather_extra,
        )

    def should_skip(self, stage_name: str, years: list[int]) -> bool:
        if not self.active:
            return False
        return self.manifest.is_stage_valid(
            stage_name, self.stage_hash(stage_name, years), self.spill_dir
        )

    def try_restore(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        years: list[int],
    ) -> bool:
        if not self.should_skip(stage_name, years):
            return False
        checkpoint = self.manifest.stages[stage_name]
        if restore_stage_from_spill(con, stage_name, checkpoint, self.spill_dir):
            return True
        self.manifest.invalidate(stage_name, self.temp_dir)
        return False

    def spill_and_record(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        years: list[int],
    ) -> None:
        """Spill the stage's owned tables to parquet, then record the checkpoint.

        No-op when checkpointing is inactive, so a plain run keeps its original
        memory-tuned spill ordering untouched.
        """
        if not self.active:
            return
        tables = CHECKPOINT_STAGE_TABLES[stage_name]
        spill_temp_tables_to_disk(con, self.temp_dir, tables)
        self.manifest.mark_done(
            stage_name,
            spilled_table_files(self.spill_dir, tables),
            spilled_row_counts(con, tables),
            self.stage_hash(stage_name, years),
            self.temp_dir,
        )

    def cascade_invalidate_from(self, stage_name: str) -> None:
        """Drop downstream checkpoints once a stage is known to re-run.

        Only meaningful in incremental mode: if an upstream stage's SQL changed
        it has been re-run, so every later stage's cached output is stale and
        must not be restored even if its own hash still matches.
        """
        if not self.incremental:
            return
        try:
            start = CHECKPOINT_STAGE_ORDER.index(stage_name)
        except ValueError:
            return
        for downstream in CHECKPOINT_STAGE_ORDER[start + 1 :]:
            self.manifest.invalidate(downstream, self.temp_dir)


def make_checkpoint_controller(args: argparse.Namespace) -> CheckpointController:
    active = bool(getattr(args, "resume", False) or getattr(args, "incremental", False))
    temp_dir = args.temp_dir if args.temp_dir is not None else Path("/tmp/duckdb-spill")
    spill_dir = temp_dir / "table_spill"
    loaded = CheckpointManifest.load(temp_dir) if active else None
    from_date, to_date = resolve_date_range(args)
    manifest = loaded if loaded is not None else CheckpointManifest()
    manifest.category = args.category
    manifest.from_date = from_date
    manifest.to_date = to_date
    venue_extra = (
        args.venue_weather_dir.as_posix() if args.venue_weather_dir is not None else ""
    )
    return CheckpointController(
        active=active,
        incremental=bool(getattr(args, "incremental", False)),
        manifest=manifest,
        temp_dir=temp_dir,
        spill_dir=spill_dir,
        category=args.category,
        from_date=from_date,
        to_date=to_date,
        venue_weather_extra=venue_extra,
    )


def stage_parquet_write(
    con: duckdb.DuckDBPyConnection,
    category: str,
    output_dir: Path,
    keep_existing: bool,
    force_clean: bool,
) -> None:
    log_event("parquet.write", "start", 0.0)
    started = perf_counter()
    # Pre-materialize the 17-table JOIN once so that write_parquet's
    # per-year loop only re-evaluates cheap window functions, not the
    # full JOIN for every year.
    base_sql = base_features_select_sql(category)
    con.execute(f"create or replace temp table _base_features_all as {base_sql}")
    window_query = _window_query_from_base_table("_base_features_all")
    write_parquet(con, window_query, output_dir, keep_existing, force_clean)
    con.execute("drop table if exists _base_features_all")
    log_event("parquet.write", "done", perf_counter() - started)


def build_empty_result(output_dir: Path, elapsed: float) -> BuildResult:
    return {
        "elapsed_seconds": elapsed,
        "output_dir": output_dir.as_posix(),
        "rows_written": 0,
    }


def resolve_upcoming_window(args: argparse.Namespace, from_date: str, to_date: str) -> tuple[str, str] | None:
    """Window used to also pull target rows straight from the source tables.

    Only active in ``--target-date`` mode; otherwise None so the historical
    build keeps reading targets from ``race_entry_corner_features`` unchanged.
    """
    if args.target_date is None:
        return None
    return from_date, to_date


def run_stage_source(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
    controller: CheckpointController,
    heartbeat: Heartbeat,
    pg_url: str,
    from_date: str,
    to_date: str,
    upcoming_window: tuple[str, str] | None,
) -> list[int]:
    """Run (or restore) the source + target stages and return target years.

    Source and target are coupled because ``years`` is derived from ``target``.
    On restore both are re-attached as views and ``years`` is read back off the
    restored ``target`` view (extract_years_from_target).
    """
    heartbeat.set_stage("source.stage")
    if controller.try_restore(con, CHECKPOINT_SOURCE, []) and controller.try_restore(
        con, CHECKPOINT_TARGET, []
    ):
        years = extract_years_from_target(con)
        log_event("checkpoint.source.restored", "done", 0.0, len(years))
        return years
    controller.cascade_invalidate_from(CHECKPOINT_SOURCE)
    stage_source(
        con,
        pg_url,
        from_date,
        to_date,
        args.category,
        upcoming_window,
        args.realtime_odds,
        args.target_race,
    )
    controller.spill_and_record(con, CHECKPOINT_SOURCE, [])
    heartbeat.set_stage("target.build")
    stage_target(con, args.category, from_date, to_date, args.target_race)
    years = get_target_years(con)
    # SOURCE / TARGET hashes are computed with years=[] because ``years`` is
    # derived FROM target — it is an output, not an input, so it must not feed
    # the hash that the restore-time check (which has no years yet) recomputes.
    controller.spill_and_record(con, CHECKPOINT_TARGET, [])
    return years


def run_stage_horse_history(
    con: duckdb.DuckDBPyConnection,
    controller: CheckpointController,
    heartbeat: Heartbeat,
    years: list[int],
    category: str,
    temp_dir: Path | None,
) -> None:
    heartbeat.set_stage("horse_history_derived")
    if controller.try_restore(con, CHECKPOINT_HORSE_HISTORY, years):
        return
    controller.cascade_invalidate_from(CHECKPOINT_HORSE_HISTORY)
    stage_horse_history_derived(con, years, heartbeat, category)
    for t in ("horse_history_base", "se_lookup", "target_current_bataiju", "jra_se", "nar_se"):
        drop_view_or_table(con, t)
    if controller.active:
        controller.spill_and_record(con, CHECKPOINT_HORSE_HISTORY, years)
    else:
        spill_temp_tables_to_disk(con, temp_dir, SPILL_AFTER_HORSE_HISTORY)


def run_stage_partner(
    con: duckdb.DuckDBPyConnection,
    controller: CheckpointController,
    heartbeat: Heartbeat,
    years: list[int],
    temp_dir: Path | None,
) -> None:
    if controller.try_restore(con, CHECKPOINT_PARTNER, years):
        return
    controller.cascade_invalidate_from(CHECKPOINT_PARTNER)
    stage_partner_features(con, years, heartbeat)
    if controller.active:
        controller.spill_and_record(con, CHECKPOINT_PARTNER, years)
    else:
        spill_temp_tables_to_disk(con, temp_dir, SPILL_AFTER_PARTNER)


def run_stage_pedigree(
    con: duckdb.DuckDBPyConnection,
    controller: CheckpointController,
    heartbeat: Heartbeat,
    years: list[int],
    category: str,
    temp_dir: Path | None,
) -> None:
    heartbeat.set_stage("pedigree")
    if controller.try_restore(con, CHECKPOINT_PEDIGREE, years):
        return
    controller.cascade_invalidate_from(CHECKPOINT_PEDIGREE)
    materialize_pedigree_stats(con, category)
    for t in ("pedigree_rec_um", "target_months", "jra_um", "nar_um"):
        drop_view_or_table(con, t)
    if controller.active:
        controller.spill_and_record(con, CHECKPOINT_PEDIGREE, years)
    else:
        spill_temp_tables_to_disk(con, temp_dir, SPILL_AFTER_PEDIGREE)


def run_stage_race_context(
    con: duckdb.DuckDBPyConnection,
    controller: CheckpointController,
    heartbeat: Heartbeat,
    years: list[int],
    temp_dir: Path | None,
) -> None:
    heartbeat.set_stage("race_context")
    if controller.try_restore(con, CHECKPOINT_RACE_CONTEXT, years):
        return
    controller.cascade_invalidate_from(CHECKPOINT_RACE_CONTEXT)
    materialize_race_context(con)
    if controller.active:
        controller.spill_and_record(con, CHECKPOINT_RACE_CONTEXT, years)
    else:
        spill_temp_tables_to_disk(con, temp_dir, SPILL_AFTER_RACE_CONTEXT)


def run_stage_track_bias(
    con: duckdb.DuckDBPyConnection,
    controller: CheckpointController,
    heartbeat: Heartbeat,
    years: list[int],
    temp_dir: Path | None,
) -> None:
    if controller.try_restore(con, CHECKPOINT_TRACK_BIAS, years):
        return
    controller.cascade_invalidate_from(CHECKPOINT_TRACK_BIAS)
    stage_track_bias(con, years, heartbeat)
    if controller.active:
        controller.spill_and_record(con, CHECKPOINT_TRACK_BIAS, years)
    else:
        spill_temp_tables_to_disk(con, temp_dir, SPILL_AFTER_TRACK_BIAS)


def run_stage_weather(
    con: duckdb.DuckDBPyConnection,
    controller: CheckpointController,
    heartbeat: Heartbeat,
    years: list[int],
    venue_weather_dir: Path | None,
    temp_dir: Path | None,
) -> None:
    heartbeat.set_stage("weather_lookup")
    if controller.try_restore(con, CHECKPOINT_WEATHER, years):
        for t in ("rec", "jra_ra", "nar_ra"):
            drop_view_or_table(con, t)
        return
    controller.cascade_invalidate_from(CHECKPOINT_WEATHER)
    materialize_venue_weather(con, venue_weather_dir, years)
    materialize_weather_lookup(con)
    con.execute("drop table if exists venue_weather_agg")
    for t in ("rec", "jra_ra", "nar_ra"):
        drop_view_or_table(con, t)
    if controller.active:
        controller.spill_and_record(con, CHECKPOINT_WEATHER, years)
    else:
        spill_temp_tables_to_disk(con, temp_dir, SPILL_AFTER_WEATHER)


def run(args: argparse.Namespace) -> BuildResult:
    pg_url = resolve_pg_url(args.pg_url)
    from_date, to_date = resolve_date_range(args)
    upcoming_window = resolve_upcoming_window(args, from_date, to_date)
    controller = make_checkpoint_controller(args)
    overall_started = perf_counter()
    set_log_file(args.log_file)
    log_event("run", "start", 0.0)
    heartbeat = Heartbeat(args.heartbeat_interval, args.status_file)
    heartbeat.start()
    con = duckdb.connect(":memory:")
    try:
        configure_duckdb_session(con, args.threads, args.memory_limit, args.temp_dir)
        years = run_stage_source(
            con, args, controller, heartbeat, pg_url, from_date, to_date, upcoming_window
        )
        log_event("target.years", "done", 0.0, len(years))
        if not years:
            prepare_output_dir(args.output_dir, args.keep_existing_output, args.force_clean_output)
            log_event("run", "skip", perf_counter() - overall_started, 0)
            return build_empty_result(args.output_dir, perf_counter() - overall_started)
        run_stage_horse_history(con, controller, heartbeat, years, args.category, args.temp_dir)
        run_stage_partner(con, controller, heartbeat, years, args.temp_dir)
        run_stage_pedigree(con, controller, heartbeat, years, args.category, args.temp_dir)
        run_stage_race_context(con, controller, heartbeat, years, args.temp_dir)
        run_stage_track_bias(con, controller, heartbeat, years, args.temp_dir)
        run_stage_weather(
            con, controller, heartbeat, years, args.venue_weather_dir, args.temp_dir
        )
        target_rows = target_rows_from_target(con)
        if not controller.active:
            heartbeat.set_stage("spill")
            spill_temp_tables_to_disk(con, args.temp_dir, SPILL_BEFORE_PARQUET)
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
        close_log_file()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
