"""Attach the feature source to DuckDB under the legacy ``pg`` alias."""

from __future__ import annotations

import os
from typing import Protocol


class DuckDBConnectionLike(Protocol):
    def execute(self, query: str) -> object: ...


_R2_CATALOG_ENV_NAMES = (
    "R2_CATALOG_WAREHOUSE",
    "R2_CATALOG_URI",
    "R2_CATALOG_TOKEN",
)

_RAW_CATALOG_TABLES = (
    "jvd_bt",
    "jvd_hc",
    "jvd_hn",
    "jvd_ra",
    "jvd_se",
    "jvd_um",
    "jvd_wc",
    "netkeiba_training_workouts",
    "nvd_nu",
    "nvd_ra",
    "nvd_se",
    "nvd_um",
)

_RAW_RACE_ENTRY_VIEW_SQL = """
create view pg.race_entry_corner_features as
with raw_rows as (
  select 'jra' as source, se.*, ra.track_code, ra.grade_code,
    ra.kyoso_shubetsu_code, ra.juryo_shubetsu_code, ra.kyoso_joken_code,
    ra.babajotai_code_shiba, ra.babajotai_code_dirt, ra.kyori, ra.shusso_tosu
  from pg.jvd_se se
  inner join pg.jvd_ra ra using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
  union all by name
  select 'nar' as source, se.*, ra.track_code, ra.grade_code,
    ra.kyoso_shubetsu_code, ra.juryo_shubetsu_code, ra.kyoso_joken_code,
    ra.babajotai_code_shiba, ra.babajotai_code_dirt, ra.kyori, ra.shusso_tosu
  from pg.nvd_se se
  inner join pg.nvd_ra ra using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
), race_counts as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    count(*) as entry_count
  from raw_rows
  where nullif(trim(ketto_toroku_bango), '') is not null
    and try_cast(nullif(trim(umaban), '') as integer) is not null
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
), normalized as (
  select
    source,
    kaisai_nen || kaisai_tsukihi as race_date,
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    ketto_toroku_bango,
    try_cast(nullif(trim(umaban), '') as integer) as umaban,
    bamei, track_code, grade_code, kyoso_shubetsu_code, juryo_shubetsu_code,
    kyoso_joken_code, babajotai_code_shiba, babajotai_code_dirt,
    try_cast(nullif(trim(kyori), '') as integer) as kyori,
    coalesce(
      try_cast(nullif(trim(raw.shusso_tosu), '00') as integer),
      counts.entry_count
    ) as shusso_tosu,
    seibetsu_code,
    try_cast(nullif(trim(barei), '00') as integer) as barei,
    try_cast(nullif(trim(futan_juryo), '000') as double) / 10 as futan_juryo,
    kishumei_ryakusho, chokyoshimei_ryakusho, banushimei,
    try_cast(nullif(trim(kakutei_chakujun), '00') as integer) as finish_position,
    try_cast(nullif(trim(tansho_ninkijun), '00') as integer) as tansho_ninkijun,
    try_cast(nullif(trim(tansho_odds), '0000') as double) / 10 as tansho_odds,
    try_cast(nullif(trim(soha_time), '0000') as integer) as soha_time,
    try_cast(nullif(trim(time_sa), '0000') as double) / 10 as time_sa,
    try_cast(nullif(trim(kohan_3f), '000') as double) / 10 as kohan_3f,
    try_cast(nullif(trim(corner_1), '00') as double) as corner_1,
    try_cast(nullif(trim(corner_2), '00') as double) as corner_2,
    try_cast(nullif(trim(corner_3), '00') as double) as corner_3,
    try_cast(nullif(trim(corner_4), '00') as double) as corner_4
  from raw_rows raw
  inner join race_counts counts
    using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
  where nullif(trim(raw.ketto_toroku_bango), '') is not null
)
select
  *,
  case when shusso_tosu > 1 and finish_position is not null
    then (finish_position - 1) / (shusso_tosu - 1) else null end as finish_norm,
  case when shusso_tosu > 1 and corner_1 is not null
    then (corner_1 - 1) / (shusso_tosu - 1) else null end as corner1_norm,
  case when shusso_tosu > 1 and corner_2 is not null
    then (corner_2 - 1) / (shusso_tosu - 1) else null end as corner2_norm,
  case when shusso_tosu > 1 and corner_3 is not null
    then (corner_3 - 1) / (shusso_tosu - 1) else null end as corner3_norm,
  case when shusso_tosu > 1 and corner_4 is not null
    then (corner_4 - 1) / (shusso_tosu - 1) else null end as corner4_norm
from normalized
where umaban is not null and kyori is not null and shusso_tosu is not null
"""

_EMPTY_RUNNING_STYLE_PREDICTIONS_VIEW_SQL = """
create view pg.race_running_style_model_predictions as
select
  cast(null as varchar) as model_version,
  cast(null as varchar) as source,
  cast(null as varchar) as kaisai_nen,
  cast(null as varchar) as kaisai_tsukihi,
  cast(null as varchar) as keibajo_code,
  cast(null as varchar) as race_bango,
  cast(null as varchar) as ketto_toroku_bango,
  cast(null as double) as p_nige,
  cast(null as double) as p_senkou,
  cast(null as double) as p_sashi,
  cast(null as double) as p_oikomi,
  cast(null as integer) as predicted_class,
  cast(null as varchar) as cell_model_key,
  cast(null as varchar) as cell_variant_id
where false
"""


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def attach_source_catalog(con: DuckDBConnectionLike, source_url: str) -> None:
    """Attach PostgreSQL or an R2 Data Catalog as ``pg``."""
    if not source_url.startswith("r2-catalog://"):
        con.execute("install postgres")
        con.execute("load postgres")
        con.execute(f"attach '{source_url}' as pg (type postgres, read_only)")
        return

    catalog_env = {name: os.environ.get(name, "") for name in _R2_CATALOG_ENV_NAMES}
    missing = [name for name, value in catalog_env.items() if not value]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(f"R2 catalog source requires environment variables: {names}")

    warehouse = _sql_literal(catalog_env["R2_CATALOG_WAREHOUSE"])
    uri = _sql_literal(catalog_env["R2_CATALOG_URI"])
    token = _sql_literal(catalog_env["R2_CATALOG_TOKEN"])

    con.execute("install iceberg")
    con.execute("load iceberg")
    con.execute("install httpfs")
    con.execute("load httpfs")
    con.execute(
        "create or replace temporary secret finish_position_r2_catalog "
        f"(type iceberg, token {token})"
    )
    con.execute(
        f"attach {warehouse} as catalog_raw (type iceberg, endpoint {uri}, "
        "secret finish_position_r2_catalog, default_schema 'pc_keiba', read_only)"
    )
    con.execute("create schema pg")
    for table_name in _RAW_CATALOG_TABLES:
        con.execute(
            f"create view pg.{table_name} as select * from catalog_raw.{table_name}"
        )
    con.execute(_RAW_RACE_ENTRY_VIEW_SQL)
    con.execute(_EMPTY_RUNNING_STYLE_PREDICTIONS_VIEW_SQL)
