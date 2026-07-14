#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5,<1.6",
#   "pyarrow>=19",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///
"""Synchronize local PC-KEIBA PostgreSQL tables to an R2 Iceberg catalog."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.io.pyarrow import pyarrow_to_schema, schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.name_mapping import MappedField, NameMapping
from pyiceberg.transforms import IdentityTransform, TruncateTransform


DEFAULT_PG_URL: Final[str] = (
    "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
)
DEFAULT_ACCOUNT_ID: Final[str] = "78109ec18c7c85b194b19fb32e3bb149"
DEFAULT_BUCKET: Final[str] = "pc-keiba-r2-catalog"
DEFAULT_CATALOG_URI: Final[str] = (
    f"https://catalog.cloudflarestorage.com/{DEFAULT_ACCOUNT_ID}/{DEFAULT_BUCKET}"
)
DEFAULT_WAREHOUSE: Final[str] = f"{DEFAULT_ACCOUNT_ID}_{DEFAULT_BUCKET}"
DEFAULT_NAMESPACE: Final[str] = "pc_keiba"
PG_ALIAS: Final[str] = "source_pg"
MANIFEST_TABLE: Final[str] = "_sync_manifest"
ARROW_BATCH_SIZE: Final[int] = 100_000
DEFAULT_MASTER_MAX_ROWS: Final[int] = 2_000_000
DEFAULT_MASTER_MAX_BYTES: Final[int] = 1_073_741_824
PARTITION_MAX_ROWS: Final[int] = 2_000_000
PARTITION_MAX_BYTES: Final[int] = 536_870_912
FIVE_YEAR_MAX_ROWS: Final[int] = 5_000_000
FIVE_YEAR_MAX_BYTES: Final[int] = 2_147_483_648
R2_S3_CONNECT_TIMEOUT_SECONDS: Final[int] = 30
R2_S3_REQUEST_TIMEOUT_SECONDS: Final[int] = 120
IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_]*$")
LOCAL_PG_HOSTS: Final[frozenset[str]] = frozenset({"127.0.0.1", "::1", "localhost"})


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_key: tuple[str, ...]
    date_columns: tuple[str, ...] = ()

    @property
    def is_master(self) -> bool:
        return not self.date_columns


@dataclass(frozen=True, order=True)
class YearScope:
    start: str
    end_exclusive: str

    @property
    def label(self) -> str:
        return f"{self.start}-{int(self.end_exclusive) - 1:04d}"

    def contains(self, year: str) -> bool:
        return self.start <= year < self.end_exclusive


TABLE_SPECS: Final[dict[str, TableSpec]] = {
    spec.name: spec
    for spec in (
        TableSpec(
            "jvd_se",
            (
                "kaisai_nen",
                "kaisai_tsukihi",
                "keibajo_code",
                "race_bango",
                "umaban",
                "ketto_toroku_bango",
            ),
            ("kaisai_nen", "kaisai_tsukihi"),
        ),
        TableSpec(
            "jvd_ra",
            ("kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango"),
            ("kaisai_nen", "kaisai_tsukihi"),
        ),
        TableSpec("jvd_um", ("ketto_toroku_bango",)),
        TableSpec(
            "jvd_hc",
            ("tracen_kubun", "chokyo_nengappi", "chokyo_jikoku", "ketto_toroku_bango"),
            ("chokyo_nengappi",),
        ),
        TableSpec(
            "nvd_se",
            (
                "kaisai_nen",
                "kaisai_tsukihi",
                "keibajo_code",
                "race_bango",
                "ketto_toroku_bango",
            ),
            ("kaisai_nen", "kaisai_tsukihi"),
        ),
        TableSpec(
            "nvd_ra",
            ("kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango"),
            ("kaisai_nen", "kaisai_tsukihi"),
        ),
        TableSpec("nvd_um", ("ketto_toroku_bango",)),
        TableSpec("nvd_nu", ("ketto_toroku_bango",)),
        TableSpec("jvd_hn", ("hanshoku_toroku_bango",)),
        TableSpec("jvd_bt", ("hanshoku_toroku_bango",)),
    )
}


@dataclass(frozen=True)
class Settings:
    pg_url: str
    catalog_uri: str
    warehouse: str
    namespace: str
    token: str | None
    master_max_rows: int = DEFAULT_MASTER_MAX_ROWS
    master_max_bytes: int = DEFAULT_MASTER_MAX_BYTES


@dataclass(frozen=True)
class SyncResult:
    table_name: str
    status: str
    source_rows: int
    target_rows: int
    source_fingerprint: str
    target_fingerprint: str
    snapshot_id: int | None


def parse_date(value: str) -> str:
    if not re.fullmatch(r"\d{8}", value):
        raise argparse.ArgumentTypeError("date must use YYYYMMDD")
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid calendar date: {value}") from exc
    return value


def parse_tables(value: str | None) -> list[TableSpec]:
    if value is None:
        return list(TABLE_SPECS.values())
    names = [part.strip() for part in value.split(",")]
    if not names or any(not name for name in names):
        raise ValueError("--tables must be a non-empty CSV without empty entries")
    selected: list[TableSpec] = []
    seen: set[str] = set()
    for name in names:
        if not IDENTIFIER_RE.fullmatch(name) or name not in TABLE_SPECS:
            allowed = ",".join(TABLE_SPECS)
            raise ValueError(f"unsupported table {name!r}; allowed: {allowed}")
        if name not in seen:
            selected.append(TABLE_SPECS[name])
            seen.add(name)
    return selected


def load_settings(env: dict[str, str] | os._Environ[str] = os.environ) -> Settings:
    return Settings(
        pg_url=env.get("PC_KEIBA_SOURCE_DATABASE_URL") or DEFAULT_PG_URL,
        catalog_uri=env.get("R2_CATALOG_URI")
        or env.get("R2_CATALOG_URL")
        or DEFAULT_CATALOG_URI,
        warehouse=env.get("R2_CATALOG_WAREHOUSE")
        or env.get("R2_WAREHOUSE")
        or DEFAULT_WAREHOUSE,
        namespace=env.get("R2_CATALOG_NAMESPACE") or DEFAULT_NAMESPACE,
        token=env.get("R2_CATALOG_TOKEN")
        or env.get("CLOUDFLARE_DEBUG_TOKEN")
        or env.get("WRANGLER_R2_SQL_AUTH_TOKEN"),
        master_max_rows=parse_positive_limit(
            env.get("R2_SYNC_MASTER_MAX_ROWS"), DEFAULT_MASTER_MAX_ROWS
        ),
        master_max_bytes=parse_positive_limit(
            env.get("R2_SYNC_MASTER_MAX_BYTES"), DEFAULT_MASTER_MAX_BYTES
        ),
    )


def parse_positive_limit(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"invalid positive limit: {value!r}") from exc
    if parsed <= 0:
        raise ValueError(f"invalid positive limit: {value!r}")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--full", action="store_true", help="replace full selected tables"
    )
    mode.add_argument(
        "--date",
        type=parse_date,
        help="refresh the containing calendar-year partition for YYYYMMDD",
    )
    parser.add_argument("--tables", help="comma-separated allowlisted table names")
    parser.add_argument(
        "--dry-run", action="store_true", help="read and validate PG only"
    )
    return parser.parse_args(argv)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def validate_local_pg_url(pg_url: str) -> None:
    parsed = urlparse(pg_url)
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or parsed.hostname not in LOCAL_PG_HOSTS
    ):
        raise ValueError(
            "PC_KEIBA_SOURCE_DATABASE_URL must be a loopback PostgreSQL URL; "
            "remote PostgreSQL sources and runtime fallbacks are forbidden"
        )


def date_values(spec: TableSpec, target_date: str) -> tuple[str, ...]:
    if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
        return target_date[:4], target_date[4:]
    if spec.date_columns == ("chokyo_nengappi",):
        return (target_date,)
    raise ValueError(f"{spec.name} has unsupported date columns: {spec.date_columns}")


def next_year(year: str) -> str:
    if not re.fullmatch(r"\d{4}", year) or not 1 <= int(year) < 9999:
        raise ValueError(f"invalid year: {year!r}")
    return f"{int(year) + 1:04d}"


def five_year_scope(year: str) -> YearScope:
    next_year(year)
    start = int(year) - (int(year) % 5)
    if start < 1 or start > 9994:
        raise ValueError(f"year cannot form a five-year scope: {year!r}")
    return YearScope(f"{start:04d}", f"{start + 5:04d}")


def single_year_scope(year: str) -> YearScope:
    return YearScope(year, next_year(year))


def year_scopes(years: set[str] | list[str]) -> list[YearScope]:
    return sorted({five_year_scope(year) for year in years})


def source_query(
    spec: TableSpec,
    target_date: str | None,
    *,
    target_scope: YearScope | None = None,
) -> tuple[str, list[str]]:
    if target_date is not None and target_scope is not None:
        raise ValueError("target_date and target_scope are mutually exclusive")
    where = ""
    params: list[str] = []
    if target_date is not None:
        params = list(date_values(spec, target_date))
        predicates = [f'"{column}" = ?' for column in spec.date_columns]
        where = " WHERE " + " AND ".join(predicates)
    elif target_scope is not None:
        if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
            where = ' WHERE "kaisai_nen" >= ? AND "kaisai_nen" < ?'
            params = [target_scope.start, target_scope.end_exclusive]
        elif spec.date_columns == ("chokyo_nengappi",):
            where = ' WHERE "chokyo_nengappi" >= ? AND "chokyo_nengappi" < ?'
            params = [
                f"{target_scope.start}0101",
                f"{target_scope.end_exclusive}0101",
            ]
        else:
            raise ValueError(f"{spec.name}: cannot query non-date table by year")
    order_by = ", ".join(f'"{column}"' for column in spec.primary_key)
    sql = f'SELECT * FROM {PG_ALIAS}.public."{spec.name}"{where} ORDER BY {order_by}'
    return sql, params


def connect_source(pg_url: str) -> duckdb.DuckDBPyConnection:
    validate_local_pg_url(pg_url)
    connection = duckdb.connect()
    connection.execute("INSTALL postgres")
    connection.execute("LOAD postgres")
    connection.execute(
        f"ATTACH {sql_string(pg_url)} AS {PG_ALIAS} (TYPE postgres, READ_ONLY)"
    )
    return connection


def extract_source(
    connection: duckdb.DuckDBPyConnection,
    spec: TableSpec,
    target_date: str | None,
    *,
    target_scope: YearScope | None = None,
    max_rows: int,
    max_bytes: int,
) -> pa.Table:
    sql, params = source_query(spec, target_date, target_scope=target_scope)
    reader = connection.execute(sql, params).to_arrow_reader(ARROW_BATCH_SIZE)
    return collect_arrow_batches(
        reader,
        spec.name,
        max_rows=max_rows,
        max_bytes=max_bytes,
    )


def collect_arrow_batches(
    reader: Any,
    label: str,
    *,
    max_rows: int,
    max_bytes: int,
) -> pa.Table:
    batches: list[pa.RecordBatch] = []
    row_count = 0
    byte_count = 0
    for batch in reader:
        row_count += batch.num_rows
        byte_count += batch.nbytes
        if row_count > max_rows or byte_count > max_bytes:
            raise RuntimeError(
                f"{label}: Arrow safety limit exceeded before write: "
                f"rows={row_count}/{max_rows}, bytes={byte_count}/{max_bytes}"
            )
        batches.append(batch)
    return normalize_timestamptz_to_utc(
        pa.Table.from_batches(batches, schema=reader.schema)
    )


def normalize_timestamptz_to_utc(data: pa.Table) -> pa.Table:
    """Normalize timezone-aware Arrow timestamps to UTC without changing instants."""
    fields: list[pa.Field] = []
    changed = False
    for field in data.schema:
        field_type = field.type
        if pa.types.is_timestamp(field_type) and field_type.tz != "UTC":
            if field_type.tz is not None:
                field_type = pa.timestamp(field_type.unit, tz="UTC")
                changed = True
        fields.append(
            pa.field(
                field.name,
                field_type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
        )
    if not changed:
        return data
    return data.cast(pa.schema(fields, metadata=data.schema.metadata), safe=True)


def source_years(connection: duckdb.DuckDBPyConnection, spec: TableSpec) -> list[str]:
    if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
        year_expression = '"kaisai_nen"'
    elif spec.date_columns == ("chokyo_nengappi",):
        year_expression = 'substring("chokyo_nengappi", 1, 4)'
    else:
        raise ValueError(f"{spec.name}: cannot enumerate non-date table years")
    rows = connection.execute(
        f"SELECT DISTINCT {year_expression} AS sync_year "
        f'FROM {PG_ALIAS}.public."{spec.name}" ORDER BY sync_year'
    ).fetchall()
    years: list[str] = []
    for (value,) in rows:
        try:
            year = str(value)
            next_year(year)
            years.append(year)
        except ValueError as exc:
            raise ValueError(
                f"{spec.name}: invalid source partition year {value!r}"
            ) from exc
    return years


def validate_primary_key(data: pa.Table, spec: TableSpec) -> None:
    missing = [column for column in spec.primary_key if column not in data.column_names]
    if missing:
        raise ValueError(
            f"{spec.name}: missing primary-key columns: {','.join(missing)}"
        )
    null_columns = [column for column in spec.primary_key if data[column].null_count]
    if null_columns:
        raise ValueError(
            f"{spec.name}: NULL primary-key fields: {','.join(null_columns)}"
        )
    if data.num_rows:
        unique_rows = (
            data.select(spec.primary_key).group_by(list(spec.primary_key)).aggregate([])
        )
        if unique_rows.num_rows != data.num_rows:
            duplicates = data.num_rows - unique_rows.num_rows
            raise ValueError(f"{spec.name}: {duplicates} duplicate primary-key rows")


def require_primary_key_fields(data: pa.Table, spec: TableSpec) -> pa.Table:
    validate_primary_key(data, spec)
    fields = [
        pa.field(
            field.name,
            field.type,
            nullable=field.name not in spec.primary_key,
            metadata=field.metadata,
        )
        for field in data.schema
    ]
    return data.cast(pa.schema(fields, metadata=data.schema.metadata))


def arrow_fingerprint(data: pa.Table, primary_key: tuple[str, ...]) -> str:
    normalized = data.replace_schema_metadata(None).combine_chunks()
    if normalized.num_rows:
        indices = pc.sort_indices(
            normalized,
            sort_keys=[(column, "ascending") for column in primary_key],
        )
        normalized = normalized.take(indices).combine_chunks()
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, normalized.schema) as writer:
        writer.write_table(normalized)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def build_iceberg_schema(data: pa.Table, spec: TableSpec) -> Schema:
    name_mapping = NameMapping(
        [
            MappedField(field_id=index, names=[field.name])
            for index, field in enumerate(data.schema, start=1)
        ]
    )
    converted = pyarrow_to_schema(
        data.schema, name_mapping=name_mapping, format_version=2
    )
    identifier_ids = [converted.find_field(name).field_id for name in spec.primary_key]
    return Schema(*converted.fields, identifier_field_ids=identifier_ids)


def build_partition_spec(schema: Schema, spec: TableSpec) -> PartitionSpec:
    if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
        return PartitionSpec(
            PartitionField(
                source_id=schema.find_field("kaisai_nen").field_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="kaisai_nen",
            )
        )
    if spec.date_columns == ("chokyo_nengappi",):
        return PartitionSpec(
            PartitionField(
                source_id=schema.find_field("chokyo_nengappi").field_id,
                field_id=1000,
                transform=TruncateTransform(4),
                name="chokyo_nengappi_year",
            )
        )
    return PartitionSpec()


def iceberg_filter(spec: TableSpec, target_date: str) -> Any:
    expressions = [
        EqualTo(column, value)
        for column, value in zip(
            spec.date_columns, date_values(spec, target_date), strict=True
        )
    ]
    expression = expressions[0]
    for next_expression in expressions[1:]:
        expression = And(expression, next_expression)
    return expression


def iceberg_scope_filter(spec: TableSpec, target_scope: YearScope) -> Any:
    if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
        return And(
            GreaterThanOrEqual("kaisai_nen", target_scope.start),
            LessThan("kaisai_nen", target_scope.end_exclusive),
        )
    if spec.date_columns == ("chokyo_nengappi",):
        return And(
            GreaterThanOrEqual("chokyo_nengappi", f"{target_scope.start}0101"),
            LessThan("chokyo_nengappi", f"{target_scope.end_exclusive}0101"),
        )
    raise ValueError(f"{spec.name}: cannot filter non-date table by year scope")


def conform_to_table(data: pa.Table, table: Any, spec: TableSpec) -> pa.Table:
    expected = schema_to_pyarrow(table.schema())
    missing = set(expected.names) - set(data.column_names)
    extra = set(data.column_names) - set(expected.names)
    if missing or extra:
        raise ValueError(
            f"{spec.name}: schema columns differ; missing={sorted(missing)}, extra={sorted(extra)}"
        )
    try:
        conformed = data.select(expected.names).cast(expected)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        raise ValueError(f"{spec.name}: incompatible Iceberg schema: {exc}") from exc
    validate_primary_key(conformed, spec)
    return conformed


def validate_existing_table(table: Any, spec: TableSpec, *, full: bool) -> None:
    schema = table.schema()
    actual_identifiers = {
        schema.find_field(field_id).name for field_id in schema.identifier_field_ids
    }
    expected_identifiers = set(spec.primary_key)
    if actual_identifiers != expected_identifiers:
        raise ValueError(
            f"{spec.name}: identifier fields differ; "
            f"expected={sorted(expected_identifiers)}, actual={sorted(actual_identifiers)}"
        )
    if not full:
        fields = table.spec().fields
        valid_partition_spec = False
        if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
            valid_partition_spec = (
                len(fields) == 1
                and fields[0].name == "kaisai_nen"
                and isinstance(fields[0].transform, IdentityTransform)
            )
        elif spec.date_columns == ("chokyo_nengappi",):
            valid_partition_spec = (
                len(fields) == 1
                and fields[0].name == "chokyo_nengappi_year"
                and isinstance(fields[0].transform, TruncateTransform)
                and fields[0].transform.width == 4
            )
        if not valid_partition_spec:
            raise ValueError(
                f"{spec.name}: unexpected Iceberg partition spec: "
                f"{tuple((field.name, str(field.transform)) for field in fields)}"
            )


def ensure_table(catalog: Any, identifier: str, data: pa.Table, spec: TableSpec) -> Any:
    if catalog.table_exists(identifier):
        table = catalog.load_table(identifier)
        table.refresh()
        return table
    schema = build_iceberg_schema(data, spec)
    return catalog.create_table(
        identifier,
        schema=schema,
        partition_spec=build_partition_spec(schema, spec),
        properties={
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
        },
    )


def read_target(
    table: Any,
    spec: TableSpec,
    target_date: str | None,
    *,
    target_scope: YearScope | None = None,
    max_rows: int,
    max_bytes: int,
) -> pa.Table:
    if target_date is not None and target_scope is not None:
        raise ValueError("target_date and target_scope are mutually exclusive")
    if target_date is not None:
        scan = table.scan(row_filter=iceberg_filter(spec, target_date))
    elif target_scope is not None:
        scan = table.scan(row_filter=iceberg_scope_filter(spec, target_scope))
    else:
        scan = table.scan()
    return collect_arrow_batches(
        scan.to_arrow_batch_reader(),
        f"{spec.name} target",
        max_rows=max_rows,
        max_bytes=max_bytes,
    )


def current_snapshot_id(table: Any) -> int | None:
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot is not None else None


def snapshot_properties(
    run_id: str,
    spec: TableSpec,
    mode: str,
    target_date: str | None,
    fingerprint: str,
    row_count: int,
    *,
    target_scope: YearScope | None = None,
) -> dict[str, str]:
    properties = {
        "sync.run-id": run_id,
        "sync.source": "local-postgresql",
        "sync.table": spec.name,
        "sync.mode": mode,
        "sync.row-count": str(row_count),
        "sync.fingerprint.sha256": fingerprint,
    }
    if target_date is not None:
        properties["sync.partition-date"] = target_date
    if target_scope is not None:
        properties["sync.partition-year-scope"] = target_scope.label
    return properties


def validate_scope_data(
    data: pa.Table, spec: TableSpec, target_scope: YearScope
) -> None:
    if not data.num_rows:
        return
    if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
        values = data["kaisai_nen"]
        lower = target_scope.start
        upper = target_scope.end_exclusive
    elif spec.date_columns == ("chokyo_nengappi",):
        values = data["chokyo_nengappi"]
        lower = f"{target_scope.start}0101"
        upper = f"{target_scope.end_exclusive}0101"
    else:
        raise ValueError(f"{spec.name}: cannot validate a master table year scope")
    minimum = pc.min(values).as_py()
    maximum = pc.max(values).as_py()
    if minimum < lower or maximum >= upper:
        raise ValueError(
            f"{spec.name}: source rows escaped year scope {target_scope.label}"
        )


def sync_table(
    catalog: Any,
    namespace: str,
    spec: TableSpec,
    source_data: pa.Table,
    *,
    full: bool,
    target_date: str | None,
    run_id: str,
    target_scope: YearScope | None = None,
    max_rows: int = PARTITION_MAX_ROWS,
    max_bytes: int = PARTITION_MAX_BYTES,
) -> SyncResult:
    if target_date is not None:
        if target_scope is None:
            raise ValueError("date sync requires a containing year scope")
        if not target_scope.contains(target_date[:4]):
            raise ValueError(
                f"date {target_date} is outside scope {target_scope.label}"
            )
    source_data = require_primary_key_fields(source_data, spec)
    identifier = f"{namespace}.{spec.name}"
    table = ensure_table(catalog, identifier, source_data, spec)
    validate_existing_table(table, spec, full=full)
    source_data = conform_to_table(source_data, table, spec)
    validate_primary_key(source_data, spec)
    if target_scope is not None:
        validate_scope_data(source_data, spec, target_scope)
    source_fingerprint = arrow_fingerprint(source_data, spec.primary_key)
    properties = snapshot_properties(
        run_id,
        spec,
        "full" if full else "date" if target_date is not None else "scope",
        target_date,
        source_fingerprint,
        source_data.num_rows,
        target_scope=target_scope,
    )
    if full:
        table.overwrite(source_data, snapshot_properties=properties)
    elif not source_data.num_rows:
        delete_filter = (
            iceberg_scope_filter(spec, target_scope)
            if target_scope is not None
            else iceberg_filter(spec, target_date or "")
        )
        table.delete(delete_filter, snapshot_properties=properties)
    elif target_scope is not None and (
        target_date is None or spec.date_columns == ("chokyo_nengappi",)
    ):
        table.overwrite(
            source_data,
            overwrite_filter=iceberg_scope_filter(spec, target_scope),
            snapshot_properties=properties,
        )
    else:
        table.dynamic_partition_overwrite(source_data, snapshot_properties=properties)
    table.refresh()

    after = conform_to_table(
        read_target(
            table,
            spec,
            None if target_scope is not None else target_date,
            target_scope=target_scope,
            max_rows=max_rows,
            max_bytes=max_bytes,
        ),
        table,
        spec,
    )
    target_fingerprint = arrow_fingerprint(after, spec.primary_key)
    if after.num_rows != source_data.num_rows:
        raise RuntimeError(
            f"{spec.name}: row-count verification failed: "
            f"source={source_data.num_rows}, target={after.num_rows}"
        )
    if target_fingerprint != source_fingerprint:
        raise RuntimeError(f"{spec.name}: fingerprint verification failed")
    validate_primary_key(after, spec)
    return SyncResult(
        table_name=spec.name,
        status="replaced",
        source_rows=source_data.num_rows,
        target_rows=after.num_rows,
        source_fingerprint=source_fingerprint,
        target_fingerprint=target_fingerprint,
        snapshot_id=current_snapshot_id(table),
    )


def manifest_arrow(
    result: SyncResult,
    spec: TableSpec,
    *,
    run_id: str,
    mode: str,
    target_date: str | None,
) -> pa.Table:
    return pa.table(
        {
            "manifest_id": pa.array([str(uuid.uuid4())], pa.string()),
            "run_id": pa.array([run_id], pa.string()),
            "table_name": pa.array([result.table_name], pa.string()),
            "mode": pa.array([mode], pa.string()),
            "partition_date": pa.array([target_date], pa.string()),
            "source_rows": pa.array([result.source_rows], pa.int64()),
            "target_rows": pa.array([result.target_rows], pa.int64()),
            "primary_key": pa.array([",".join(spec.primary_key)], pa.string()),
            "source_fingerprint": pa.array([result.source_fingerprint], pa.string()),
            "target_fingerprint": pa.array([result.target_fingerprint], pa.string()),
            "snapshot_id": pa.array([result.snapshot_id], pa.int64()),
            "status": pa.array([result.status], pa.string()),
            "committed_at": pa.array(
                [datetime.now(timezone.utc)], pa.timestamp("us", tz="UTC")
            ),
        }
    )


def append_manifest(
    catalog: Any,
    namespace: str,
    result: SyncResult,
    spec: TableSpec,
    *,
    run_id: str,
    mode: str,
    target_date: str | None,
) -> None:
    data = manifest_arrow(
        result,
        spec,
        run_id=run_id,
        mode=mode,
        target_date=target_date,
    )
    manifest_spec = TableSpec(MANIFEST_TABLE, ("manifest_id",))
    data = require_primary_key_fields(data, manifest_spec)
    table = ensure_table(catalog, f"{namespace}.{MANIFEST_TABLE}", data, manifest_spec)
    validate_existing_table(table, manifest_spec, full=True)
    data = conform_to_table(data, table, manifest_spec)
    table.append(
        data,
        snapshot_properties={
            "sync.run-id": run_id,
            "sync.manifest-for": result.table_name,
        },
    )


def target_years(catalog: Any, namespace: str, spec: TableSpec) -> set[str]:
    identifier = f"{namespace}.{spec.name}"
    if not catalog.table_exists(identifier):
        return set()
    table = catalog.load_table(identifier)
    table.refresh()
    validate_existing_table(table, spec, full=False)
    years: set[str] = set()
    for row in table.inspect.partitions().to_pylist():
        partition = row["partition"]
        if spec.date_columns == ("kaisai_nen", "kaisai_tsukihi"):
            value = str(partition["kaisai_nen"])
        else:
            value = str(partition["chokyo_nengappi_year"])
        try:
            next_year(value)
            years.add(value)
        except ValueError as exc:
            raise ValueError(
                f"{spec.name}: invalid target partition year {value!r}"
            ) from exc
    return years


def create_catalog(settings: Settings) -> Any:
    if not settings.token:
        raise ValueError(
            "R2_CATALOG_TOKEN or CLOUDFLARE_DEBUG_TOKEN is required unless --dry-run is set"
        )
    return load_catalog(
        "r2_pc_keiba",
        type="rest",
        uri=settings.catalog_uri,
        warehouse=settings.warehouse,
        token=settings.token,
        **{
            "s3.connect-timeout": str(R2_S3_CONNECT_TIMEOUT_SECONDS),
            "s3.request-timeout": str(R2_S3_REQUEST_TIMEOUT_SECONDS),
        },
    )


def emit(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def redact_error(exc: BaseException, settings: Settings) -> str:
    message = str(exc)
    secrets = [settings.token, settings.pg_url]
    for secret in secrets:
        if secret:
            message = message.replace(secret, "[redacted]")
    message = re.sub(
        r"postgres(?:ql)?://[^\s'\"]+",
        "postgresql://[redacted]",
        message,
        flags=re.IGNORECASE,
    )
    return message


def run(args: argparse.Namespace, settings: Settings) -> int:
    selected = parse_tables(args.tables)
    target_date = args.date
    if target_date is not None:
        selected_date_tables = [spec for spec in selected if not spec.is_master]
        skipped = [spec.name for spec in selected if spec.is_master]
        if skipped:
            emit("skip_master_requires_full", tables=skipped)
        selected = selected_date_tables
        if not selected:
            raise ValueError(
                "date mode selected no date-keyed tables; masters require --full"
            )

    connection = connect_source(settings.pg_url)
    try:
        catalog = None if args.dry_run else create_catalog(settings)
        if catalog is not None:
            catalog.create_namespace_if_not_exists(settings.namespace)
        run_id = str(uuid.uuid4())

        def process_data(
            spec: TableSpec,
            partition_date: str | None,
            *,
            partition_scope: YearScope | None = None,
            full_write: bool,
            manifest_mode: str,
            max_rows: int,
            max_bytes: int,
        ) -> None:
            data = extract_source(
                connection,
                spec,
                None if partition_scope is not None else partition_date,
                target_scope=partition_scope,
                max_rows=max_rows,
                max_bytes=max_bytes,
            )
            data = require_primary_key_fields(data, spec)
            fingerprint = arrow_fingerprint(data, spec.primary_key)
            if args.dry_run:
                emit(
                    "dry_run",
                    table=spec.name,
                    mode=manifest_mode,
                    date=partition_date,
                    year_scope=partition_scope.label if partition_scope else None,
                    rows=data.num_rows,
                    fingerprint=fingerprint,
                    primary_key=list(spec.primary_key),
                    partitions=list(spec.date_columns),
                )
                return
            result = sync_table(
                catalog,
                settings.namespace,
                spec,
                data,
                full=full_write,
                target_date=partition_date,
                run_id=run_id,
                target_scope=partition_scope,
                max_rows=max_rows,
                max_bytes=max_bytes,
            )
            append_manifest(
                catalog,
                settings.namespace,
                result,
                spec,
                run_id=run_id,
                mode=manifest_mode,
                target_date=partition_date
                or (partition_scope.label if partition_scope else None),
            )
            emit(
                "sync_complete",
                table=result.table_name,
                status=result.status,
                mode=manifest_mode,
                date=partition_date,
                year_scope=partition_scope.label if partition_scope else None,
                rows=result.target_rows,
                fingerprint=result.target_fingerprint,
                snapshot_id=result.snapshot_id,
            )

        for spec in selected:
            if spec.is_master:
                process_data(
                    spec,
                    None,
                    full_write=True,
                    manifest_mode="full",
                    max_rows=settings.master_max_rows,
                    max_bytes=settings.master_max_bytes,
                )
                continue
            if target_date is not None:
                process_data(
                    spec,
                    target_date,
                    partition_scope=single_year_scope(target_date[:4]),
                    full_write=False,
                    manifest_mode="date",
                    max_rows=PARTITION_MAX_ROWS,
                    max_bytes=PARTITION_MAX_BYTES,
                )
                continue
            scopes = year_scopes(source_years(connection, spec))
            emit("year_scope_plan", table=spec.name, scopes=len(scopes))
            for scope in scopes:
                process_data(
                    spec,
                    None,
                    partition_scope=scope,
                    full_write=False,
                    manifest_mode="full-5year",
                    max_rows=FIVE_YEAR_MAX_ROWS,
                    max_bytes=FIVE_YEAR_MAX_BYTES,
                )
            if catalog is not None:
                target_scopes = set(
                    year_scopes(target_years(catalog, settings.namespace, spec))
                )
                for stale_scope in sorted(target_scopes - set(scopes)):
                    process_data(
                        spec,
                        None,
                        partition_scope=stale_scope,
                        full_write=False,
                        manifest_mode="full-stale-5year-delete",
                        max_rows=FIVE_YEAR_MAX_ROWS,
                        max_bytes=FIVE_YEAR_MAX_BYTES,
                    )
        return 0
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    try:
        return run(parse_args(argv), settings)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        emit(
            "sync_failed",
            error_type=type(exc).__name__,
            error=redact_error(exc, settings),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
