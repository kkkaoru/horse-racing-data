#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5,<1.6",
#   "pyarrow>=19",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///
"""Build the partition-pruned race entity history serving table."""

from __future__ import annotations

import argparse
import time
import uuid
from typing import Final

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from sync_r2_catalog import (
    ARROW_BATCH_SIZE,
    DEFAULT_MASTER_MAX_BYTES,
    DEFAULT_MASTER_MAX_ROWS,
    PG_ALIAS,
    Settings,
    TableSpec,
    build_iceberg_schema,
    collect_arrow_batches,
    create_catalog,
    emit,
    load_settings,
    require_primary_key_fields,
    sql_string,
    validate_local_pg_url,
)

TABLE_NAME: Final[str] = "race_entity_history_v1"
ENTITY_TYPES: Final[tuple[tuple[str, str, str], ...]] = (
    ("horse", "ketto_toroku_bango", "bamei"),
    ("jockey", "kishu_code", "kishumei_ryakusho"),
    ("trainer", "chokyoshi_code", "chokyoshimei_ryakusho"),
    ("owner", "banushi_code", "banushimei"),
)
RACE_COLUMNS: Final[tuple[str, ...]] = (
    "hasso_jikoku",
    "kyosomei_hondai",
    "kyoso_joken_meisho",
    "grade_code",
    "kyori",
    "track_code",
    "tenko_code",
    "babajotai_code_shiba",
    "babajotai_code_dirt",
    "shusso_tosu",
)
RUNNER_COLUMNS: Final[tuple[str, ...]] = (
    "ketto_toroku_bango",
    "bamei",
    "kishu_code",
    "kishumei_ryakusho",
    "chokyoshi_code",
    "chokyoshimei_ryakusho",
    "banushi_code",
    "banushimei",
    "kakutei_chakujun",
    "ijo_kubun_code",
    "tansho_ninkijun",
    "tansho_odds",
    "soha_time",
    "time_sa",
    "kohan_3f",
    "corner_1",
    "corner_2",
    "corner_3",
    "corner_4",
    "umaban",
    "wakuban",
    "futan_juryo",
    "bataiju",
    "zogen_fugo",
    "zogen_sa",
)
KEY_COLUMNS: Final[tuple[str, ...]] = (
    "entity_type",
    "source",
    "entity_id",
    "result_id",
)
MIN_HISTORY_YEAR: Final[str] = "1986"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true")
    mode.add_argument("--year", type=parse_year)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def parse_year(value: str) -> str:
    if (
        len(value) != 4
        or not value.isdigit()
        or not MIN_HISTORY_YEAR <= value <= "9998"
    ):
        raise argparse.ArgumentTypeError("year must use YYYY and be at least 1986")
    return value


def entity_values_sql() -> str:
    rows = ",\n      ".join(
        f"({sql_string(entity_type)}, se.{id_column}, se.{name_column})"
        for entity_type, id_column, name_column in ENTITY_TYPES
    )
    return f"VALUES\n      {rows}"


def projected_columns(alias: str, columns: tuple[str, ...]) -> str:
    return ",\n  ".join(f"{alias}.{column}" for column in columns)


def normalized_sql(expression: str) -> str:
    return f"trim(replace(coalesce({expression}, ''), chr(12288), ''))"


def source_select(
    source: str,
    race_table: str,
    runner_table: str,
    year: str,
) -> str:
    race_columns = projected_columns("ra", RACE_COLUMNS)
    runner_columns = projected_columns("se", RUNNER_COLUMNS)
    entity_id = normalized_sql("entity.entity_id")
    entity_name = normalized_sql("entity.entity_name")
    horse_number = normalized_sql("se.umaban")
    horse_id = normalized_sql("se.ketto_toroku_bango")
    finish_position = normalized_sql("se.kakutei_chakujun")
    abnormality = normalized_sql("se.ijo_kubun_code")
    completion_predicate = f"""AND (
    coalesce(nullif({finish_position}, ''), '00') <> '00'
    OR coalesce(nullif({abnormality}, ''), '0') <> '0'
  )"""
    return f"""SELECT
  entity.entity_type,
  {sql_string(source)} AS source,
  {entity_id} AS entity_id,
  nullif({entity_name}, '') AS entity_name,
  substr(md5({entity_id}), 1, 1) AS entity_bucket,
  se.kaisai_nen,
  se.kaisai_tsukihi,
  se.keibajo_code,
  se.race_bango,
  concat({sql_string(source)}, ':', se.kaisai_nen, se.kaisai_tsukihi, ':', se.keibajo_code, ':', se.race_bango, ':', {horse_number}, ':', {horse_id}) AS result_id,
  {race_columns},
  {runner_columns}
FROM {PG_ALIAS}.public.{runner_table} se
INNER JOIN {PG_ALIAS}.public.{race_table} ra
  ON ra.kaisai_nen = se.kaisai_nen
  AND ra.kaisai_tsukihi = se.kaisai_tsukihi
  AND ra.keibajo_code = se.keibajo_code
  AND ra.race_bango = se.race_bango
CROSS JOIN LATERAL (
  {entity_values_sql()}
) entity(entity_type, entity_id, entity_name)
WHERE se.kaisai_nen = {sql_string(year)}
  AND nullif({entity_id}, '') IS NOT NULL
  AND regexp_matches({entity_id}, '[^0]')
  {completion_predicate}"""


def entity_query(year: str) -> str:
    selects = (
        source_select("jra", "jvd_ra", "jvd_se", year),
        source_select("nar", "nvd_ra", "nvd_se", year),
    )
    return (
        "\nUNION ALL\n".join(selects)
        + "\nORDER BY entity_type, source, entity_bucket, entity_id, kaisai_nen DESC, kaisai_tsukihi DESC, hasso_jikoku DESC, result_id DESC"
    )


def history_query(year: str) -> str:
    return entity_query(year)


def connect_source(settings: Settings) -> duckdb.DuckDBPyConnection:
    validate_local_pg_url(settings.pg_url)
    connection = duckdb.connect()
    connection.execute("INSTALL postgres")
    connection.execute("LOAD postgres")
    connection.execute("SET memory_limit='6GB'")
    connection.execute("SET threads TO 4")
    connection.execute(
        f"ATTACH {sql_string(settings.pg_url)} AS {PG_ALIAS} (TYPE postgres, READ_ONLY)"
    )
    return connection


def source_years(connection: duckdb.DuckDBPyConnection) -> list[str]:
    rows = connection.execute(
        f"""SELECT DISTINCT kaisai_nen
FROM (
  SELECT kaisai_nen FROM {PG_ALIAS}.public.jvd_se
  UNION ALL
  SELECT kaisai_nen FROM {PG_ALIAS}.public.nvd_se
)
WHERE kaisai_nen >= {sql_string(MIN_HISTORY_YEAR)}
ORDER BY kaisai_nen"""
    ).fetchall()
    return [str(row[0]) for row in rows]


def extract_query(
    connection: duckdb.DuckDBPyConnection, year: str, query: str, label: str
) -> pa.Table:
    reader = connection.execute(query).to_arrow_reader(ARROW_BATCH_SIZE)
    data = collect_arrow_batches(
        reader,
        f"{TABLE_NAME}:{label}:{year}",
        max_rows=DEFAULT_MASTER_MAX_ROWS * 4,
        max_bytes=DEFAULT_MASTER_MAX_BYTES * 2,
    )
    return require_primary_key_fields(data, table_spec())


def extract_year(connection: duckdb.DuckDBPyConnection, year: str) -> pa.Table:
    return extract_query(connection, year, history_query(year), "history")


def partition_spec(data: pa.Table) -> PartitionSpec:
    schema = build_iceberg_schema(
        require_primary_key_fields(data, table_spec()), table_spec()
    )
    names = ("entity_type", "source", "entity_bucket", "kaisai_nen")
    return PartitionSpec(
        *(
            PartitionField(
                source_id=schema.find_field(name).field_id,
                field_id=1000 + index,
                transform=IdentityTransform(),
                name=name,
            )
            for index, name in enumerate(names)
        )
    )


def table_spec() -> TableSpec:
    return TableSpec(TABLE_NAME, KEY_COLUMNS)


def create_table(catalog: Catalog, identifier: str, data: pa.Table) -> Table:
    schema = build_iceberg_schema(
        require_primary_key_fields(data, table_spec()), table_spec()
    )
    return catalog.create_table(
        identifier,
        schema=schema,
        partition_spec=partition_spec(data),
        properties={
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
            "write.target-file-size-bytes": "67108864",
        },
    )


def build_full(
    connection: duckdb.DuckDBPyConnection, catalog: Catalog, settings: Settings
) -> int:
    years = source_years(connection)
    if not years:
        raise RuntimeError("entity history source contains no supported years")
    target_identifier = f"{settings.namespace}.{TABLE_NAME}"
    build_name = f"{TABLE_NAME}_build_{uuid.uuid4().hex[:8]}"
    build_identifier = f"{settings.namespace}.{build_name}"
    table = None
    total_rows = 0
    try:
        for year in years:
            started = time.perf_counter()
            data = extract_year(connection, year)
            if data.num_rows == 0:
                continue
            table = (
                create_table(catalog, build_identifier, data)
                if table is None
                else table
            )
            table.append(data)
            total_rows += data.num_rows
            emit(
                "entity_history_year_appended",
                year=year,
                rows=data.num_rows,
                elapsed_ms=round((time.perf_counter() - started) * 1000, 1),
            )
        if table is None:
            raise RuntimeError("entity history build produced no rows")
        if catalog.table_exists(target_identifier):
            catalog.drop_table(target_identifier)
        catalog.rename_table(build_identifier, target_identifier)
    except Exception:
        if catalog.table_exists(build_identifier):
            catalog.drop_table(build_identifier)
        raise
    emit("entity_history_full_complete", rows=total_rows, years=len(years))
    return total_rows


def refresh_year(
    connection: duckdb.DuckDBPyConnection,
    catalog: Catalog,
    settings: Settings,
    year: str,
) -> int:
    identifier = f"{settings.namespace}.{TABLE_NAME}"
    if not catalog.table_exists(identifier):
        raise RuntimeError("entity history table is missing; run --full first")
    data = extract_year(connection, year)
    table = catalog.load_table(identifier)
    table.overwrite(data, overwrite_filter=EqualTo("kaisai_nen", year))
    emit("entity_history_year_refreshed", year=year, rows=data.num_rows)
    return data.num_rows


def run(args: argparse.Namespace, settings: Settings) -> int:
    connection = connect_source(settings)
    try:
        if args.dry_run:
            years = source_years(connection) if args.full else [args.year]
            rows = sum(extract_year(connection, year).num_rows for year in years)
            emit("entity_history_dry_run", rows=rows, years=len(years))
            return 0
        catalog = create_catalog(settings)
        build_full(connection, catalog, settings) if args.full else refresh_year(
            connection, catalog, settings, args.year
        )
        return 0
    finally:
        connection.close()


def main() -> int:
    return run(parse_args(), load_settings())


if __name__ == "__main__":
    raise SystemExit(main())
