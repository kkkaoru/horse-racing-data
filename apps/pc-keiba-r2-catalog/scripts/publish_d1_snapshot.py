#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Publish bounded, lossless D1 snapshot batches into an actual R2 Iceberg table."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Protocol

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import BooleanExpression, EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, StringType


@dataclass(frozen=True)
class SnapshotBatch:
    snapshot_id: str
    database_name: str
    table_name: str
    rows: tuple[tuple[str, str], ...]
    batch_id: str


class SnapshotScan(Protocol):
    def to_arrow(self) -> pa.Table: ...


class SnapshotTable(Protocol):
    def overwrite(
        self,
        df: pa.Table,
        *,
        overwrite_filter: BooleanExpression,
        snapshot_properties: dict[str, str],
    ) -> None: ...

    def scan(
        self,
        *,
        row_filter: BooleanExpression,
        selected_fields: tuple[str, ...],
    ) -> SnapshotScan: ...


MAX_BATCH_BYTES: Final[int] = 64 * 1024 * 1024
MAX_BATCH_ROWS: Final[int] = 50_000
COLUMNS: Final[tuple[str, ...]] = (
    "snapshot_id",
    "database_name",
    "table_name",
    "batch_id",
    "row_key",
    "payload",
)
SCHEMA: Final[Schema] = Schema(
    *(
        NestedField(field_id=index, name=name, field_type=StringType(), required=True)
        for index, name in enumerate(COLUMNS, start=1)
    )
)
ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [pa.field(name, pa.string(), nullable=False) for name in COLUMNS]
)
PARTITION: Final[PartitionSpec] = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=IdentityTransform(), name="snapshot_id"
    ),
    PartitionField(
        source_id=2, field_id=1001, transform=IdentityTransform(), name="database_name"
    ),
    PartitionField(
        source_id=3, field_id=1002, transform=IdentityTransform(), name="table_name"
    ),
)


def text_field(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Missing snapshot identity or payload")
    return value


def row_sort_key(row: tuple[str, str]) -> int:
    return int(row[0])


def fingerprint(
    *, identity: tuple[str, str, str], rows: tuple[tuple[str, str], ...]
) -> str:
    digest = hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode("utf-8"))
    for row in sorted(rows, key=row_sort_key):
        digest.update(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def load_batch(path: Path) -> SnapshotBatch:
    if path.stat().st_size > MAX_BATCH_BYTES:
        raise ValueError("Snapshot batch exceeds byte limit")
    with path.open(encoding="utf-8") as stream:
        document: object = json.load(stream)
    if not isinstance(document, dict):
        raise TypeError("Invalid snapshot batch")
    identity = (
        text_field(document.get("snapshot_id")),
        text_field(document.get("database_name")),
        text_field(document.get("table_name")),
    )
    raw_rows: object = document.get("rows")
    if not isinstance(raw_rows, list) or not 1 <= len(raw_rows) <= MAX_BATCH_ROWS:
        raise ValueError("Invalid snapshot row count")
    rows = tuple(parse_row(row) for row in raw_rows)
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("Duplicate snapshot row identifiers")
    return SnapshotBatch(*identity, rows, fingerprint(identity=identity, rows=rows))


def parse_row(value: object) -> tuple[str, str]:
    if not isinstance(value, dict):
        raise TypeError("Invalid snapshot row")
    key = text_field(value.get("row_key"))
    if str(int(key)) != key or not -(2**63) <= int(key) < 2**63:
        raise ValueError("Invalid SQLite row identifier")
    payload = text_field(value.get("payload"))
    parsed: object = json.loads(payload)
    if not isinstance(parsed, dict):
        raise TypeError("Invalid source-typed snapshot payload")
    return key, payload


def arrow_batch(batch: SnapshotBatch) -> pa.Table:
    return pa.Table.from_pylist(
        [
            dict(
                zip(
                    COLUMNS,
                    (
                        batch.snapshot_id,
                        batch.database_name,
                        batch.table_name,
                        batch.batch_id,
                        key,
                        payload,
                    ),
                    strict=True,
                )
            )
            for key, payload in batch.rows
        ],
        schema=ARROW_SCHEMA,
    )


def publish_batch(table: SnapshotTable, batch: SnapshotBatch) -> str:
    predicate = EqualTo(term="batch_id", value=batch.batch_id)
    # Replace this content-addressed batch atomically: a lost acknowledgement cannot duplicate rows.
    table.overwrite(
        arrow_batch(batch),
        overwrite_filter=predicate,
        snapshot_properties={"migration.batch-id": batch.batch_id},
    )
    restored = table.scan(
        row_filter=predicate, selected_fields=("row_key", "payload")
    ).to_arrow()
    rows = tuple(parse_row(row) for row in restored.to_pylist())
    expected = fingerprint(
        identity=(batch.snapshot_id, batch.database_name, batch.table_name), rows=rows
    )
    if len(rows) != len(batch.rows) or expected != batch.batch_id:
        raise ValueError("Catalog snapshot round-trip reconciliation failed")
    return expected


def run(path: Path) -> None:
    batch = load_batch(path)
    account = text_field(os.environ.get("R2_ACCOUNT_ID"))
    token = text_field(os.environ.get("CLOUDFLARE_DEBUG_TOKEN"))
    catalog = load_catalog(
        "d1_snapshot_migration",
        type="rest",
        uri=f"https://catalog.cloudflarestorage.com/{account}/pc-keiba-r2-catalog",
        warehouse=f"{account}_pc-keiba-r2-catalog",
        token=token,
        **{"s3.connect-timeout": "30", "s3.request-timeout": "120"},
    )
    table = catalog.create_table_if_not_exists(
        "pc_keiba.d1_snapshot_rows",
        schema=SCHEMA,
        partition_spec=PARTITION,
        properties={"format-version": "2", "write.parquet.compression-codec": "zstd"},
    )
    digest = publish_batch(table, batch)
    print(
        json.dumps({"batch_id": digest, "rows": len(batch.rows), "reconciled": True}),
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch",
        required=True,
        help="Bounded snapshot batch JSON produced by the D1 extractor",
    )
    run(Path(text_field(parser.parse_args().batch)))


if __name__ == "__main__":
    main()
