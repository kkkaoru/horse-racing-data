#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb>=1.5,<1.6", "pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Append explicitly scoped local history to an existing Catalog without replacing rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from contextlib import closing
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

import sync_r2_catalog as sync

TABLE_NAME: str = "oversea_person_race_history"
SCOPE_PATTERN: re.Pattern[str] = re.compile(r"(owner|trainer|jockey):([A-Za-z0-9]+)")


def load_scopes(path: Path) -> frozenset[tuple[str, str]]:
    document: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("scopes"), list):
        raise TypeError("A scope manifest with a scopes array is required")
    scopes: set[tuple[str, str]] = set()
    for entry in document["scopes"]:
        key = entry.get("key") if isinstance(entry, dict) else None
        match = SCOPE_PATTERN.fullmatch(key) if isinstance(key, str) else None
        if match is None:
            raise ValueError("Invalid source history scope")
        scopes.add((match.group(1), match.group(2)))
    if not scopes:
        raise ValueError("At least one explicit source scope is required")
    return frozenset(scopes)


def select_delta(
    *, source: pa.Table, before: pa.Table, scopes: frozenset[tuple[str, str]]
) -> pa.Table:
    spec = sync.TABLE_SPECS[TABLE_NAME]
    sync.validate_primary_key(source, spec)
    sync.validate_primary_key(before, spec)
    source_rows = source.to_pylist()
    incoming = {row["history_id"]: row for row in source_rows}
    existing = {row["history_id"]: row for row in before.to_pylist()}
    if any(incoming.get(key) != row for key, row in existing.items()):
        raise RuntimeError(
            "Cannot preserve every existing Catalog row from the local authority"
        )
    selected = [
        row["history_id"] not in existing
        and row["source"] == "netkeiba"
        and (row["person_kind"], row["source_person_id"]) in scopes
        for row in source_rows
    ]
    return source.filter(pa.array(selected, type=pa.bool_()))


def save_json(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False)
    path.chmod(0o600)


def append_scopes(
    *, manifest: Path, output: Path, settings: sync.Settings
) -> dict[str, object]:
    scopes = load_scopes(manifest)
    if urlparse(settings.pg_url).hostname not in sync.LOCAL_PG_HOSTS:
        raise ValueError("Only explicit local PostgreSQL authority is permitted")
    output.mkdir(mode=0o700)
    spec = sync.TABLE_SPECS[TABLE_NAME]
    with closing(sync.connect_source(settings.pg_url)) as connection:
        catalog = sync.create_catalog(settings)
        table = sync.load_existing_table(
            catalog, f"{settings.namespace}.{TABLE_NAME}", cache={}
        )
        if table is None:
            raise RuntimeError("An existing production Catalog table is required")
        sync.validate_existing_table(table, spec, full=True)
        raw = sync.extract_source(
            connection,
            spec,
            None,
            max_rows=settings.master_max_rows,
            max_bytes=settings.master_max_bytes,
        )
        source = sync.conform_to_table(
            sync.require_primary_key_fields(raw, spec), table, spec
        )
        before = sync.conform_to_table(
            sync.read_target(
                table,
                spec,
                None,
                max_rows=settings.master_max_rows,
                max_bytes=settings.master_max_bytes,
            ),
            table,
            spec,
        )
        delta = select_delta(source=source, before=before, scopes=scopes)
        expected_rows = pa.concat_tables([before, delta])
        expected = sync.arrow_fingerprint(expected_rows, spec.primary_key)
        snapshot = sync.current_snapshot_id(table)
        if snapshot is None:
            raise RuntimeError("An existing pinned Catalog snapshot is required")
        pq.write_table(before, output / "before.parquet")
        pq.write_table(delta, output / "delta.parquet")
        (output / "before.parquet").chmod(0o600)
        (output / "delta.parquet").chmod(0o600)
        save_json(
            output / "pending.json",
            {
                "oldSnapshot": str(snapshot),
                "oldRows": before.num_rows,
                "deltaRows": delta.num_rows,
                "expectedFingerprint": expected,
                "deltaSha256": hashlib.sha256(
                    (output / "delta.parquet").read_bytes()
                ).hexdigest(),
                "scopeManifestSha256": hashlib.sha256(
                    manifest.read_bytes()
                ).hexdigest(),
                "localSourceOnly": True,
                "appendOnly": True,
            },
        )
        table.refresh()
        if sync.current_snapshot_id(table) != snapshot:
            raise RuntimeError("Catalog changed before append; inspect without replay")
        if delta.num_rows:
            table.append(
                delta,
                snapshot_properties={
                    "operation-origin": "overseas-history-scoped-append",
                    "source-fingerprint": expected,
                },
            )
        table.refresh()
        after = sync.conform_to_table(
            sync.read_target(
                table,
                spec,
                None,
                max_rows=settings.master_max_rows,
                max_bytes=settings.master_max_bytes,
            ),
            table,
            spec,
        )
        sync.validate_primary_key(after, spec)
        if (
            after.num_rows != expected_rows.num_rows
            or sync.arrow_fingerprint(after, spec.primary_key) != expected
        ):
            raise RuntimeError("Full Catalog readback mismatch; inspect without replay")
        if delta.num_rows:
            sync.record_table_fingerprint(
                table,
                sync.fingerprint_key(None, None, full=True),
                sync.stored_fingerprint_value(expected),
            )
        result: dict[str, object] = {
            "table": TABLE_NAME,
            "catalogUri": settings.catalog_uri,
            "warehouse": settings.warehouse,
            "namespace": settings.namespace,
            "oldRows": before.num_rows,
            "appendedRows": delta.num_rows,
            "verifiedRows": after.num_rows,
            "oldRowsUnchanged": True,
            "fingerprint": expected,
            "snapshotId": str(sync.current_snapshot_id(table)),
            "localSourceOnly": True,
            "appendOnly": True,
            "requestedScopesOnly": True,
            "scopeCount": len(scopes),
        }
        save_json(output / "receipt.json", result)
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--confirm-append", action="store_true", required=True)
    args = parser.parse_args(argv)
    result = append_scopes(
        manifest=args.manifest, output=args.output, settings=sync.load_settings()
    )
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
