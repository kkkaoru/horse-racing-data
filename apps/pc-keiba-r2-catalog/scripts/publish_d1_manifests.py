# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Publish queryable baseline schema/count manifests, including empty D1 tables.

A manifest certifies copied batch counts, NOT a consistent live-source snapshot or
permission to promote readers. Run serially; the baseline copier may run alongside.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, BooleanExpression, EqualTo

from publish_d1_snapshot import (
    PARTITION,
    SCHEMA,
    SnapshotBatch,
    fingerprint,
    publish_batch,
    text_field,
)


@dataclass(frozen=True)
class BaselineManifest:
    batch: SnapshotBatch
    copied_rows: int


def load_document(path: Path) -> dict[str, object]:
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("Manifest input exceeds byte limit")
    with path.open(encoding="utf-8") as stream:
        value: object = json.load(stream)
    if not isinstance(value, dict):
        raise TypeError("Invalid manifest input document")
    return value


def load_manifest(path: Path) -> BaselineManifest | None:
    state = load_document(path)
    if state.get("phase") == "copying":
        return None
    if state.get("phase") != "copied" or state.get("pending") is not None:
        raise ValueError("Manifest checkpoint is not complete")
    identity = (
        text_field(state.get("snapshotId")),
        text_field(state.get("databaseName")),
        text_field(state.get("tableName")),
    )
    if identity != (path.parents[2].name, path.parents[1].name, path.parent.name):
        raise ValueError("Manifest identity does not match its directory")
    copied_rows = state.get("copiedRows")
    if (
        isinstance(copied_rows, bool)
        or not isinstance(copied_rows, int)
        or copied_rows < 0
    ):
        raise ValueError("Invalid manifest copied row count")
    cursor = state.get("afterRowId")
    if (cursor is None) != (copied_rows == 0):
        raise ValueError("Manifest cursor and count disagree")
    if cursor is not None and (
        not isinstance(cursor, str)
        or str(int(cursor)) != cursor
        or not -(2**63) <= int(cursor) < 2**63
    ):
        raise ValueError("Invalid manifest source cursor")
    schema = load_document(path.parent / "schema.json")
    columns = schema.get("schema")
    if not isinstance(columns, list) or not columns:
        raise ValueError("Missing manifest source schema")
    signature = hashlib.sha256(
        json.dumps(columns, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if signature != schema.get("schemaSignature") or signature != state.get(
        "schemaSignature"
    ):
        raise ValueError("Manifest schema signature mismatch")
    payload = json.dumps(
        {
            "kind": "d1-baseline-manifest-v1",
            "source_schema": columns,
            "source_schema_signature": signature,
            "copied_rows": copied_rows,
            "after_row_id": cursor,
            "verification": "batch_readback_and_catalog_count",
            "promoted": False,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    rows = (("0", payload),)
    return BaselineManifest(
        SnapshotBatch(*identity, rows, fingerprint(identity=identity, rows=rows)),
        copied_rows,
    )


def partition_filter(batch: SnapshotBatch) -> BooleanExpression:
    return And(
        left=EqualTo(term="snapshot_id", value=batch.snapshot_id),
        right=And(
            left=EqualTo(term="database_name", value=batch.database_name),
            right=EqualTo(term="table_name", value=batch.table_name),
        ),
    )


def run(root: Path) -> None:
    if not root.is_dir():
        raise FileNotFoundError("Snapshot directory does not exist")
    account = text_field(os.environ.get("R2_ACCOUNT_ID"))
    token = text_field(os.environ.get("CLOUDFLARE_DEBUG_TOKEN"))
    catalog = load_catalog(
        "d1_baseline_manifests",
        type="rest",
        uri=f"https://catalog.cloudflarestorage.com/{account}/pc-keiba-r2-catalog",
        warehouse=f"{account}_pc-keiba-r2-catalog",
        token=token,
        **{"s3.connect-timeout": "30", "s3.request-timeout": "120"},
    )
    manifests = catalog.create_table_if_not_exists(
        "pc_keiba.d1_snapshot_manifests",
        schema=SCHEMA,
        partition_spec=PARTITION,
        properties={"format-version": "2", "write.parquet.compression-codec": "zstd"},
    )
    for path in sorted(root.glob("*/*/checkpoint.json")):
        manifest = load_manifest(path)
        if manifest is None:
            continue
        # Reload source metadata after the completed checkpoint; never trust an old snapshot.
        count = (
            catalog.load_table("pc_keiba.d1_snapshot_rows")
            .scan(row_filter=partition_filter(manifest.batch))
            .count()
        )
        if count != manifest.copied_rows:
            raise ValueError(
                "Catalog partition count does not match completed checkpoint"
            )
        publish_batch(manifests, manifest.batch)
        print(
            json.dumps(
                {
                    "database": manifest.batch.database_name,
                    "table": manifest.batch.table_name,
                    "rows": count,
                    "manifest_reconciled": True,
                    "promoted": False,
                }
            ),
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-directory", required=True)
    run(Path(parser.parse_args().snapshot_directory))


if __name__ == "__main__":
    main()
