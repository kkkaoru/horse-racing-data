#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "aiohttp>=3.13,<4",
#   "duckdb>=1.5,<1.6",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///
"""Publish an atomic API index over Catalog-managed entity Parquet files."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Final, TypedDict
from urllib.parse import quote

import aiohttp
from pyiceberg.table import Table
from sync_r2_catalog import create_catalog, load_settings

DEFAULT_BUCKET: Final[str] = "pc-keiba-r2-catalog"
DEFAULT_OUTPUT: Final[Path] = Path("tmp/entity-catalog-manifest.json")
MANIFEST_KEY: Final[str] = "entity-catalog-serving-v1/manifest.json"
MANIFEST_VERSION: Final[int] = 1
MAX_MANIFEST_BYTES: Final[int] = 2 * 1024 * 1024
HISTORY_TABLE: Final[str] = "race_entity_history_v1"
RAW_TABLES: Final[tuple[str, ...]] = ("jvd_se", "nvd_se", "jvd_ra", "nvd_ra")


class CatalogFile(TypedDict):
    file_path: str
    file_size_in_bytes: int
    partition: Mapping[str, object]


class TableManifest(TypedDict):
    dataPrefix: str
    partitions: dict[str, list[list[str | int]]]
    snapshotId: str


class ServingManifest(TypedDict):
    history: TableManifest
    raw: dict[str, TableManifest]
    version: int


PartitionKey = Callable[[Mapping[str, object]], str]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--skip-upload", action="store_true")
    return parser.parse_args(argv)


def required_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    raise RuntimeError(f"one of {', '.join(names)} is required")


def object_url(account_id: str, bucket: str, key: str) -> str:
    return (
        "https://api.cloudflare.com/client/v4/accounts/"
        f"{quote(account_id, safe='')}/r2/buckets/{quote(bucket, safe='')}/objects/"
        f"{quote(key, safe='/')}"
    )


def history_partition_key(partition: Mapping[str, object]) -> str:
    names = ("entity_type", "source", "entity_bucket", "kaisai_nen")
    values = [partition.get(name) for name in names]
    if not all(isinstance(value, str) and value for value in values):
        raise ValueError("history Catalog partition is malformed")
    return "/".join(str(value) for value in values)


def year_partition_key(partition: Mapping[str, object]) -> str:
    year = partition.get("kaisai_nen")
    if not isinstance(year, str) or len(year) != 4 or not year.isdigit():
        raise ValueError("raw Catalog year partition is malformed")
    return year


def split_data_key(file_path: str, bucket: str) -> tuple[str, str]:
    prefix = f"s3://{bucket}/"
    if not file_path.startswith(prefix):
        raise ValueError("Catalog data file is outside the configured bucket")
    key = file_path.removeprefix(prefix)
    marker = "/data/"
    marker_index = key.find(marker)
    if marker_index < 0:
        raise ValueError("Catalog data file path has no data directory")
    split_index = marker_index + len(marker)
    return key[:split_index], key[split_index:]


def catalog_file_sort_key(file: list[str | int]) -> str:
    return str(file[0])


def build_table_manifest(
    rows: Sequence[CatalogFile],
    *,
    bucket: str,
    partition_key: PartitionKey,
    snapshot_id: int,
) -> TableManifest:
    if not rows:
        raise ValueError("Catalog table contains no data files")
    partitions: dict[str, list[list[str | int]]] = {}
    data_prefix: str | None = None
    for row in rows:
        prefix, relative_key = split_data_key(row["file_path"], bucket)
        if data_prefix is None:
            data_prefix = prefix
        elif prefix != data_prefix:
            raise ValueError("Catalog table spans multiple data prefixes")
        key = partition_key(row["partition"])
        partitions.setdefault(key, []).append([relative_key, row["file_size_in_bytes"]])
    if data_prefix is None:
        raise ValueError("Catalog table data prefix is unavailable")
    sorted_partitions = {
        key: sorted(files, key=catalog_file_sort_key)
        for key, files in sorted(partitions.items())
    }
    return {
        "dataPrefix": data_prefix,
        "partitions": sorted_partitions,
        "snapshotId": str(snapshot_id),
    }


def inspect_table(
    table: Table, bucket: str, partition_key: PartitionKey
) -> TableManifest:
    snapshot_id = table.metadata.current_snapshot_id
    if snapshot_id is None:
        raise RuntimeError("Catalog table has no current snapshot")
    rows = table.inspect.files().select(
        ["file_path", "file_size_in_bytes", "partition"]
    )
    return build_table_manifest(
        rows.to_pylist(),
        bucket=bucket,
        partition_key=partition_key,
        snapshot_id=snapshot_id,
    )


def build_manifest(bucket: str) -> ServingManifest:
    settings = load_settings()
    catalog = create_catalog(settings)
    history = inspect_table(
        catalog.load_table(f"{settings.namespace}.{HISTORY_TABLE}"),
        bucket,
        history_partition_key,
    )
    raw = {
        name: inspect_table(
            catalog.load_table(f"{settings.namespace}.{name}"),
            bucket,
            year_partition_key,
        )
        for name in RAW_TABLES
    }
    return {"history": history, "raw": raw, "version": MANIFEST_VERSION}


def encode_manifest(manifest: ServingManifest) -> bytes:
    body = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode()
    if len(body) > MAX_MANIFEST_BYTES:
        raise RuntimeError("Catalog serving manifest exceeds the Worker read limit")
    return body


async def upload_manifest(bucket: str, body: bytes) -> None:
    account_id = required_env("R2_ACCOUNT_ID", "CLOUDFLARE_ACCOUNT_ID")
    token = required_env("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_DEBUG_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=60)
    async with (
        aiohttp.ClientSession(timeout=timeout) as session,
        session.put(
            object_url(account_id, bucket, MANIFEST_KEY), data=body, headers=headers
        ) as response,
    ):
        response_text = await response.text()
        if response.status != 200:
            raise RuntimeError(
                f"manifest upload failed ({response.status}): {response_text[:500]}"
            )


def run(args: argparse.Namespace) -> int:
    body = encode_manifest(build_manifest(args.bucket))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(body)
    if not args.skip_upload:
        asyncio.run(upload_manifest(args.bucket, body))
    print(
        json.dumps(
            {
                "bucket": args.bucket,
                "bytes": len(body),
                "event": "entity_catalog_manifest_published"
                if not args.skip_upload
                else "entity_catalog_manifest_built",
                "key": MANIFEST_KEY,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return len(body)


def main() -> int:
    run(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
