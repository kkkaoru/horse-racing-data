"""Local PostgreSQL export for causal horse-performance time series."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Protocol, cast

DEFAULT_LOCAL_DATABASE_URL = "postgresql://horse_racing:horse_racing@localhost:15432/horse_racing"
DATABASE_URL_ENV = "DATABASE_URL_LOCAL"


class DuckDbModule(Protocol):
    """Dynamically imported DuckDB module."""

    connect: DuckDbConnect


class DuckDbConnect(Protocol):
    """Dynamically imported DuckDB constructor."""

    def __call__(self) -> DuckDbConnectionLike:
        """Create an in-memory connection."""
        ...


class DuckDbConnectionLike(Protocol):
    """Minimal DuckDB boundary needed by the exporter."""

    def execute(self, query: str, parameters: list[str] | None = None) -> object:
        """Execute one local statement."""
        ...

    def close(self) -> None:
        """Close the local connection."""
        ...


def duckdb_literal(value: str) -> str:
    """Quote one DuckDB string literal without exposing it to logs."""
    if "\x00" in value:
        raise ValueError("DuckDB literals must not contain NUL bytes")
    return "'" + value.replace("'", "''") + "'"


def encoded_race_time_seconds_sql(expression: str) -> str:
    """Decode a PC-KEIBA packed MSSd clock to elapsed seconds in DuckDB."""
    raw = f"try_cast(trim({expression}) as bigint)"
    seconds = f"(({raw} // 10) % 100)"
    return (
        "case "
        f"when {raw} > 0 and {seconds} < 60 "
        f"then (({raw} // 1000) * 600 + {seconds} * 10 + ({raw} % 10)) / 10.0 "
        "else null end"
    )


def history_select_sql(category: str, *, from_year: int = 2020, to_year: int = 2026) -> str:
    """Build the same causal history export for JRA or NAR source tables."""
    if category not in {"jra", "nar"}:
        raise ValueError("category must be jra or nar")
    if from_year > to_year:
        raise ValueError("from_year must not exceed to_year")
    prefix = category
    table_prefix = "jvd" if category == "jra" else "nvd"
    return rf"""
with se as (
  select
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    max(kakutei_chakujun) as kakutei_chakujun,
    max(soha_time) as soha_time,
    max(kohan_3f) as kohan_3f,
    max(corner_1) as corner_1,
    max(corner_2) as corner_2,
    max(corner_3) as corner_3,
    max(corner_4) as corner_4,
    max(time_sa) as time_sa,
    max(futan_juryo) as futan_juryo,
    max(bataiju) as bataiju,
    max(kishu_code) as kishu_code,
    max(chokyoshi_code) as chokyoshi_code,
    max(tansho_odds) as tansho_odds
  from pg.{table_prefix}_se
  where kaisai_nen between '{from_year}' and '{to_year}'
    and try_cast(trim(kakutei_chakujun) as integer) > 0
  group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
), ra as (
  select
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    max(kyori) as kyori,
    max(track_code) as track_code,
    max(babajotai_code_shiba) as babajotai_code_shiba,
    max(babajotai_code_dirt) as babajotai_code_dirt,
    max(kyoso_joken_code) as kyoso_joken_code
  from pg.{table_prefix}_ra
  group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
), raw as (
  select
    concat('{prefix}:', se.kaisai_nen, ':', se.kaisai_tsukihi, ':',
           lpad(se.keibajo_code, 2, '0'), ':', lpad(se.race_bango, 2, '0')) as race_id,
    concat(se.kaisai_nen, se.kaisai_tsukihi) as race_date,
    se.ketto_toroku_bango as horse_id,
    ra.keibajo_code as venue_code,
    try_cast(trim(se.kakutei_chakujun) as integer) as finish_position,
    {encoded_race_time_seconds_sql("se.soha_time")} as elapsed_seconds,
    try_cast(trim(se.kohan_3f) as integer) / 10.0 as final_3f_seconds,
    try_cast(trim(se.corner_1) as integer) as corner_1,
    try_cast(trim(se.corner_2) as integer) as corner_2,
    try_cast(trim(se.corner_3) as integer) as corner_3,
    try_cast(trim(se.corner_4) as integer) as corner_4,
    try_cast(replace(trim(se.time_sa), '+', '') as integer) / 10.0 as margin_seconds,
    try_cast(trim(se.futan_juryo) as integer) / 10.0 as carried_weight,
    try_cast(trim(se.bataiju) as integer) as body_weight,
    se.kishu_code as jockey_code,
    se.chokyoshi_code as trainer_code,
    try_cast(trim(se.tansho_odds) as integer) / 10.0 as decimal_odds,
    try_cast(trim(ra.kyori) as integer) as distance,
    ra.track_code,
    case when substr(ra.track_code, 1, 1) = '1'
         then ra.babajotai_code_shiba else ra.babajotai_code_dirt end as going_code,
    coalesce(nullif(trim(ra.kyoso_joken_code), ''), 'unknown') as class_code,
    count(*) over (
      partition by se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango
    ) as field_size
  from se
  join ra
    on ra.kaisai_nen = se.kaisai_nen
   and ra.kaisai_tsukihi = se.kaisai_tsukihi
   and ra.keibajo_code = se.keibajo_code
   and ra.race_bango = se.race_bango
  where se.kaisai_nen between '{from_year}' and '{to_year}'
    and try_cast(trim(se.kakutei_chakujun) as integer) > 0
), rated as (
  select raw.*,
    1.0 - (finish_position - 1.0) / greatest(field_size - 1.0, 1.0) as performance_rating,
    median(elapsed_seconds) over (partition by race_id) - elapsed_seconds as speed_figure,
    median(final_3f_seconds) over (partition by race_id) - final_3f_seconds as final_3f_rating,
    1.0 - (
      coalesce(corner_1, corner_2, corner_3, corner_4, finish_position) - 1.0
    ) / greatest(field_size - 1.0, 1.0) as pace_rating
  from raw
)
select * from rated
order by race_date, race_id, horse_id
""".strip()


HISTORY_SELECT_SQL = history_select_sql("nar")


def export_history_parquet(
    output_path: Path,
    *,
    database_url: str | None = None,
    connection: DuckDbConnectionLike | None = None,
    category: str = "nar",
    from_year: int = 2020,
    to_year: int = 2026,
) -> None:
    """Export immutable local horse histories without contacting cloud services."""
    url = database_url or os.environ.get(DATABASE_URL_ENV, DEFAULT_LOCAL_DATABASE_URL)
    own_connection = connection is None
    if connection is None:
        duckdb_module = cast("DuckDbModule", importlib.import_module("duckdb"))
        connect = duckdb_module.connect
        active = connect()
    else:
        active = connection
    try:
        active.execute("INSTALL postgres")
        active.execute("LOAD postgres")
        active.execute(f"ATTACH {duckdb_literal(url)} AS pg (TYPE POSTGRES, READ_ONLY)")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        active.execute(
            f"COPY ({history_select_sql(category, from_year=from_year, to_year=to_year)}) "
            f"TO {duckdb_literal(str(output_path))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        if own_connection:
            active.close()
