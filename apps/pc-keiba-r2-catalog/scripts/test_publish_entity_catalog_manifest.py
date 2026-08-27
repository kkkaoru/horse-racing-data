#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "aiohttp>=3.13,<4",
#   "duckdb>=1.5,<1.6",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///
"""Tests for the direct-Catalog serving manifest publisher."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from publish_entity_catalog_manifest import (
    CatalogFile,
    TableManifest,
    build_table_manifest,
    encode_manifest,
    history_partition_key,
    object_url,
    parse_args,
    required_env,
    run,
    split_data_key,
    year_partition_key,
)


class PublishEntityCatalogManifestTest(unittest.TestCase):
    def test_split_data_key_keeps_catalog_owned_path(self) -> None:
        self.assertEqual(
            split_data_key(
                "s3://bucket/__r2_data_catalog/table/data/entity_type=horse/file.parquet",
                "bucket",
            ),
            (
                "__r2_data_catalog/table/data/",
                "entity_type=horse/file.parquet",
            ),
        )

    def test_split_data_key_rejects_another_bucket(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside the configured bucket"):
            split_data_key("s3://other/table/data/file.parquet", "bucket")

    def test_partition_keys_are_bounded_and_deterministic(self) -> None:
        self.assertEqual(
            history_partition_key(
                {
                    "entity_type": "jockey",
                    "source": "nar",
                    "entity_bucket": "a",
                    "kaisai_nen": "2026",
                }
            ),
            "jockey/nar/a/2026",
        )
        self.assertEqual(year_partition_key({"kaisai_nen": "2026"}), "2026")

    def test_partition_keys_reject_malformed_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "history Catalog partition"):
            history_partition_key({"entity_type": "horse"})
        with self.assertRaisesRegex(ValueError, "raw Catalog year"):
            year_partition_key({"kaisai_nen": "26"})

    def test_build_table_manifest_compacts_and_sorts_files(self) -> None:
        rows: list[CatalogFile] = [
            {
                "file_path": "s3://bucket/root/data/p/z.parquet",
                "file_size_in_bytes": 20,
                "partition": {"kaisai_nen": "2026"},
            },
            {
                "file_path": "s3://bucket/root/data/p/a.parquet",
                "file_size_in_bytes": 10,
                "partition": {"kaisai_nen": "2026"},
            },
        ]
        self.assertEqual(
            build_table_manifest(
                rows,
                bucket="bucket",
                partition_key=year_partition_key,
                snapshot_id=123,
            ),
            {
                "dataPrefix": "root/data/",
                "partitions": {"2026": [["p/a.parquet", 10], ["p/z.parquet", 20]]},
                "snapshotId": "123",
            },
        )

    def test_encode_manifest_is_compact_json(self) -> None:
        table: TableManifest = {
            "dataPrefix": "root/data/",
            "partitions": {"2026": [["file.parquet", 10]]},
            "snapshotId": "123",
        }
        body = encode_manifest(
            {
                "history": table,
                "raw": {"jvd_se": table},
                "version": 1,
            }
        )
        self.assertEqual(json.loads(body), json.loads(body.decode()))
        self.assertNotIn(b" ", body)

    def test_object_url_escapes_bucket_and_key(self) -> None:
        self.assertEqual(
            object_url("account/id", "bucket name", "path/file.json"),
            "https://api.cloudflare.com/client/v4/accounts/account%2Fid/r2/buckets/bucket%20name/objects/path/file.json",
        )

    def test_required_env_uses_first_available_name(self) -> None:
        with patch.dict(os.environ, {"SECOND_TOKEN": "token"}, clear=True):
            self.assertEqual(required_env("FIRST_TOKEN", "SECOND_TOKEN"), "token")
        with (
            patch.dict(os.environ, {}, clear=True),
            self.assertRaisesRegex(RuntimeError, "FIRST_TOKEN, SECOND_TOKEN"),
        ):
            required_env("FIRST_TOKEN", "SECOND_TOKEN")

    def test_parse_args_defaults_to_atomic_manifest(self) -> None:
        args = parse_args(["--skip-upload"])
        self.assertEqual(args.output, Path("tmp/entity-catalog-manifest.json"))
        self.assertTrue(args.skip_upload)

    def test_run_writes_without_upload_when_requested(self) -> None:
        table: TableManifest = {
            "dataPrefix": "root/data/",
            "partitions": {"2026": [["file.parquet", 10]]},
            "snapshotId": "123",
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "manifest.json"
            args = argparse.Namespace(bucket="bucket", output=output, skip_upload=True)
            with patch(
                "publish_entity_catalog_manifest.build_manifest",
                return_value={"history": table, "raw": {}, "version": 1},
            ):
                size = run(args)
            self.assertEqual(size, output.stat().st_size)
            self.assertEqual(
                json.loads(output.read_text()),
                {"history": table, "raw": {}, "version": 1},
            )


if __name__ == "__main__":
    unittest.main()
