# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Validate immutable TS capture artifacts and publish verified native Iceberg events.

The caller must hold an exclusive database writer lock and retain the
exact pending artifact until acknowledgement. Dirty keys are not source-row images.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, Protocol

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, BooleanExpression, EqualTo, In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType


@dataclass(frozen=True)
class CaptureEvent:
    capture_id: str
    table_name: str
    schema_hash: str
    sequence: int
    operation: str
    captured_at: str
    before_key: str | None
    after_key: str | None


@dataclass(frozen=True)
class CaptureBatch:
    database_name: str
    database_id: str
    events: tuple[CaptureEvent, ...]
    batch_id: str


class CaptureScan(Protocol):
    def to_arrow(self) -> pa.Table: ...


class CaptureTable(Protocol):
    def refresh(self) -> object: ...
    def append(self, df: pa.Table, *, snapshot_properties: dict[str, str]) -> None: ...
    def scan(
        self, *, row_filter: BooleanExpression, selected_fields: tuple[str, ...]
    ) -> CaptureScan: ...


MAX_BYTES: Final[int] = 1048576
MAX_ROWS: Final[int] = 1000
MIN_INT: Final[int] = -(2**63)
MAX_INT: Final[int] = 2**63 - 1
HASH: Final[re.Pattern[str]] = re.compile(r"[a-f0-9]{64}")
NAME: Final[re.Pattern[str]] = re.compile(r"[a-zA-Z0-9_-]{1,64}")
UUID: Final[re.Pattern[str]] = re.compile(
    r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"
)
EVENT_FIELDS: Final[frozenset[str]] = frozenset(
    (
        "captureId",
        "table",
        "schemaHash",
        "sequence",
        "operation",
        "capturedAt",
        "beforeKey",
        "afterKey",
    )
)
DOCUMENT_FIELDS: Final[frozenset[str]] = frozenset(
    ("formatVersion", "databaseName", "databaseId", "events", "batchId")
)
COLUMNS: Final[tuple[str, ...]] = (
    "database_id",
    "database_name",
    "capture_id",
    "table_name",
    "schema_hash",
    "sequence",
    "batch_id",
    "operation",
    "before_key",
    "after_key",
    "captured_at",
)
NULLABLE: Final[frozenset[str]] = frozenset(("before_key", "after_key"))
SCHEMA: Final[Schema] = Schema(
    *(
        NestedField(
            field_id=index,
            name=name,
            field_type=LongType() if name == "sequence" else StringType(),
            required=name not in NULLABLE,
        )
        for index, name in enumerate(COLUMNS, start=1)
    )
)
ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field(
            name,
            pa.int64() if name == "sequence" else pa.string(),
            nullable=name in NULLABLE,
        )
        for name in COLUMNS
    ]
)
PARTITION: Final[PartitionSpec] = PartitionSpec(
    *(
        PartitionField(
            source_id=source_id,
            field_id=1000 + index,
            transform=IdentityTransform(),
            name=name,
        )
        for index, (source_id, name) in enumerate(
            ((1, "database_id"), (3, "capture_id"), (4, "table_name"))
        )
    )
)


def text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("Expected capture text")
    return value


def integer(value: object) -> int:
    raw = text(value)
    if len(raw) > 20 or re.fullmatch(r"(?:0|-?[1-9]\d*)", raw) is None:
        raise ValueError("Invalid capture integer")
    number = int(raw)
    if not MIN_INT <= number <= MAX_INT or str(number) != raw:
        raise ValueError("Capture integer out of range")
    return number


def nullable_key(value: object) -> str | None:
    if value is None:
        return None
    integer(value)
    return text(value)


def parse_event(value: object) -> CaptureEvent:
    if not isinstance(value, dict) or set(value) != EVENT_FIELDS:
        raise ValueError("Invalid capture event fields")
    event = CaptureEvent(
        text(value["captureId"]),
        text(value["table"]),
        text(value["schemaHash"]),
        integer(value["sequence"]),
        text(value["operation"]),
        text(value["capturedAt"]),
        nullable_key(value["beforeKey"]),
        nullable_key(value["afterKey"]),
    )
    if (
        NAME.fullmatch(event.capture_id) is None
        or not 1 <= len(event.table_name.encode("utf-16-le")) // 2 <= 256
        or HASH.fullmatch(event.schema_hash) is None
        or event.sequence <= 0
    ):
        raise ValueError("Invalid capture event identity")
    valid_keys = {
        "insert": event.before_key is None and event.after_key is not None,
        "update": event.before_key is not None and event.after_key is not None,
        "delete": event.before_key is not None and event.after_key is None,
        "touch": event.before_key is not None and event.before_key == event.after_key,
    }
    if not valid_keys.get(event.operation, False):
        raise ValueError("Invalid capture operation keys")
    if (
        len(event.captured_at) != 24
        or datetime.fromisoformat(event.captured_at)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
        != event.captured_at
    ):
        raise ValueError("Invalid capture timestamp")
    return event


def event_document(event: CaptureEvent) -> dict[str, object]:
    # Exactly matches the TS reader's property order; do not sort keys.
    return {
        "captureId": event.capture_id,
        "table": event.table_name,
        "schemaHash": event.schema_hash,
        "sequence": str(event.sequence),
        "operation": event.operation,
        "capturedAt": event.captured_at,
        "beforeKey": event.before_key,
        "afterKey": event.after_key,
    }


def fingerprint(batch: CaptureBatch) -> str:
    content = {
        "formatVersion": 1,
        "databaseName": batch.database_name,
        "databaseId": batch.database_id,
        "events": [event_document(event) for event in batch.events],
    }
    return hashlib.sha256(
        json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def validate_batch(batch: CaptureBatch) -> None:
    if not 1 <= len(batch.events) <= MAX_ROWS:
        raise ValueError("Invalid capture event count")
    if (
        NAME.fullmatch(batch.database_name) is None
        or UUID.fullmatch(batch.database_id) is None
        or HASH.fullmatch(batch.batch_id) is None
    ):
        raise ValueError("Invalid capture database or batch identity")
    for event in batch.events:
        parse_event(event_document(event))
    if any(
        left.sequence >= right.sequence
        for left, right in zip(batch.events, batch.events[1:])
    ):
        raise ValueError("Capture events are not strictly ordered")
    if fingerprint(batch) != batch.batch_id:
        raise ValueError("Capture artifact fingerprint mismatch")


def load_batch(path: Path) -> CaptureBatch:
    with path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("Capture artifact exceeds byte limit")
    document: object = json.loads(raw)
    if (
        not isinstance(document, dict)
        or set(document) != DOCUMENT_FIELDS
        or type(document["formatVersion"]) is not int
        or document["formatVersion"] != 1
    ):
        raise ValueError("Invalid capture artifact format")
    rows: object = document["events"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ROWS:
        raise ValueError("Invalid capture event count")
    batch = CaptureBatch(
        text(document["databaseName"]),
        text(document["databaseId"]),
        tuple(parse_event(row) for row in rows),
        text(document["batchId"]),
    )
    validate_batch(batch)
    return batch


def native_row(*, batch: CaptureBatch, event: CaptureEvent) -> dict[str, object]:
    return dict(
        zip(
            COLUMNS,
            (
                batch.database_id,
                batch.database_name,
                event.capture_id,
                event.table_name,
                event.schema_hash,
                event.sequence,
                batch.batch_id,
                event.operation,
                event.before_key,
                event.after_key,
                event.captured_at,
            ),
            strict=True,
        )
    )


def comparable(row: dict[str, object]) -> dict[str, object]:
    # An overlapping retry can have a different batch ID, but not different event data.
    return {key: value for key, value in row.items() if key != "batch_id"}


def scan_existing(
    *,
    table: CaptureTable,
    predicate: BooleanExpression,
    expected: dict[int, dict[str, object]],
) -> set[int]:
    restored = table.scan(row_filter=predicate, selected_fields=COLUMNS).to_arrow()
    if restored.num_rows > len(expected):
        raise ValueError("Duplicate Catalog capture identities")
    seen: set[int] = set()
    for row in restored.to_pylist():
        sequence: object = row.get("sequence")
        if type(sequence) is not int or sequence in seen or sequence not in expected:
            raise ValueError("Unexpected Catalog capture identity")
        if comparable(row) != comparable(expected[sequence]):
            raise ValueError("Conflicting Catalog capture event")
        seen.add(sequence)
    return seen


def publish_batch(table: CaptureTable, batch: CaptureBatch) -> str:
    validate_batch(batch)
    expected = {
        event.sequence: native_row(batch=batch, event=event) for event in batch.events
    }
    predicate = And(
        EqualTo(term="database_id", value=batch.database_id),
        In(term="sequence", literals=tuple(expected)),
    )
    table.refresh()
    seen = scan_existing(table=table, predicate=predicate, expected=expected)
    missing = [row for sequence, row in expected.items() if sequence not in seen]
    if missing:
        table.append(
            pa.Table.from_pylist(missing, schema=ARROW_SCHEMA),
            snapshot_properties={"migration.batch-id": batch.batch_id},
        )
    table.refresh()
    if scan_existing(table=table, predicate=predicate, expected=expected) != set(
        expected
    ):
        raise ValueError("Catalog capture readback is incomplete")
    return batch.batch_id


def run(path: Path) -> None:
    batch = load_batch(path)
    account = text(os.environ.get("R2_ACCOUNT_ID"))
    token = text(os.environ.get("CLOUDFLARE_DEBUG_TOKEN"))
    if re.fullmatch(r"[a-f0-9]{32}", account) is None or not token.strip():
        raise ValueError("Invalid capture publication credentials")
    catalog = load_catalog(
        "d1_capture_publication",
        type="rest",
        uri=f"https://catalog.cloudflarestorage.com/{account}/pc-keiba-r2-catalog",
        warehouse=f"{account}_pc-keiba-r2-catalog",
        token=token,
        **{"s3.connect-timeout": "30", "s3.request-timeout": "120"},
    )
    table = catalog.create_table_if_not_exists(
        "pc_keiba.d1_capture_events",
        schema=SCHEMA,
        partition_spec=PARTITION,
        properties={"format-version": "2", "write.parquet.compression-codec": "zstd"},
    )
    digest = publish_batch(table, batch)
    print(
        json.dumps(
            {
                "batchId": digest,
                "eventCount": len(batch.events),
                "lastSequence": str(batch.events[-1].sequence),
                "reconciled": True,
            }
        ),
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True)
    run(Path(text(parser.parse_args().batch)))


if __name__ == "__main__":
    main()
