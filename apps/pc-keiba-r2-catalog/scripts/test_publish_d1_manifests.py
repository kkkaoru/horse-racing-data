# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Tests for queryable baseline manifests and their non-promotion guarantees."""

from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

import publish_d1_manifests as subject

COLUMNS: list[dict[str, object]] = [
    {"name": "v", "type": "TEXT", "notNull": 0, "primaryKey": 0, "defaultValue": None}
]
SIGNATURE = hashlib.sha256(
    json.dumps(COLUMNS, separators=(",", ":")).encode()
).hexdigest()
SCHEMA: dict[str, object] = {"schema": COLUMNS, "schemaSignature": SIGNATURE}
STATE: dict[str, object] = {
    "snapshotId": "s",
    "databaseName": "db",
    "tableName": "t",
    "phase": "copied",
    "pending": None,
    "copiedRows": 0,
    "afterRowId": None,
    "schemaSignature": SIGNATURE,
}
PATH = Path("/s/db/t/checkpoint.json")


class ManifestTests(unittest.TestCase):
    def manifest(self) -> subject.BaselineManifest:
        with patch.object(subject, "load_document", side_effect=[STATE, SCHEMA]):
            result = subject.load_manifest(PATH)
        if result is None:
            raise AssertionError("Expected a completed manifest")
        return result

    def test_empty_schema_is_queryable_without_promotion(self) -> None:
        manifest = self.manifest()
        payload = json.loads(manifest.batch.rows[0][1])
        self.assertEqual(payload["copied_rows"], 0)
        self.assertEqual(payload["source_schema"], COLUMNS)
        self.assertFalse(payload["promoted"])
        self.assertIn("snapshot_id", str(subject.partition_filter(manifest.batch)))

    def test_nonempty_manifest(self) -> None:
        with patch.object(
            subject,
            "load_document",
            side_effect=[
                {**STATE, "copiedRows": 1, "afterRowId": "-9223372036854775808"},
                SCHEMA,
            ],
        ):
            result = subject.load_manifest(PATH)
        self.assertIsNotNone(result)

    def test_skips_inflight_state(self) -> None:
        with patch.object(subject, "load_document", return_value={"phase": "copying"}):
            self.assertIsNone(subject.load_manifest(PATH))

    def test_rejects_invalid_checkpoints(self) -> None:
        for fields in [
            {"phase": "unknown"},
            {"pending": {}},
            {"snapshotId": "other"},
            {"copiedRows": True},
            {"copiedRows": "1"},
            {"copiedRows": -1},
            {"copiedRows": 1},
            {"copiedRows": 1, "afterRowId": 1},
            {"copiedRows": 1, "afterRowId": "01"},
            {"copiedRows": 1, "afterRowId": "9223372036854775808"},
        ]:
            with (
                self.subTest(fields=fields),
                patch.object(
                    subject, "load_document", return_value={**STATE, **fields}
                ),
                self.assertRaises(ValueError),
            ):
                subject.load_manifest(PATH)

    def test_rejects_invalid_schemas(self) -> None:
        for schema in [{}, {"schema": []}, {**SCHEMA, "schemaSignature": "wrong"}]:
            with (
                self.subTest(schema=schema),
                patch.object(subject, "load_document", side_effect=[STATE, schema]),
                self.assertRaises(ValueError),
            ):
                subject.load_manifest(PATH)
        with (
            patch.object(
                subject,
                "load_document",
                side_effect=[{**STATE, "schemaSignature": "wrong"}, SCHEMA],
            ),
            self.assertRaises(ValueError),
        ):
            subject.load_manifest(PATH)

    def test_reads_bounded_json(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            path.write_text("{}", encoding="utf-8")
            self.assertEqual(subject.load_document(path), {})
            path.write_text("[]", encoding="utf-8")
            with self.assertRaises(TypeError):
                subject.load_document(path)
            with (
                patch.object(
                    Path, "stat", return_value=SimpleNamespace(st_size=1024 * 1024 + 1)
                ),
                self.assertRaises(ValueError),
            ):
                subject.load_document(path)

    def test_publishes_only_after_catalog_count_matches(self) -> None:
        catalog = Mock()
        catalog.load_table.return_value.scan.return_value.count.return_value = 0
        manifest = self.manifest()
        with (
            patch.object(Path, "is_dir", return_value=True),
            patch.object(Path, "glob", return_value=[PATH, PATH]),
            patch.object(subject, "load_manifest", side_effect=[None, manifest]),
            patch.object(subject, "load_catalog", return_value=catalog),
            patch.object(subject, "publish_batch") as publish,
            patch.dict(
                subject.os.environ,
                {"R2_ACCOUNT_ID": "account", "CLOUDFLARE_DEBUG_TOKEN": "example"},
            ),
            patch("builtins.print"),
        ):
            subject.run(Path("/s"))
        publish.assert_called_once_with(
            catalog.create_table_if_not_exists.return_value, manifest.batch
        )

    def test_rejects_catalog_count_mismatch(self) -> None:
        catalog = Mock()
        catalog.load_table.return_value.scan.return_value.count.return_value = 1
        with (
            patch.object(Path, "is_dir", return_value=True),
            patch.object(Path, "glob", return_value=[PATH]),
            patch.object(subject, "load_manifest", return_value=self.manifest()),
            patch.object(subject, "load_catalog", return_value=catalog),
            patch.object(subject, "publish_batch") as publish,
            patch.dict(
                subject.os.environ,
                {"R2_ACCOUNT_ID": "account", "CLOUDFLARE_DEBUG_TOKEN": "example"},
            ),
            self.assertRaisesRegex(ValueError, "partition count"),
        ):
            subject.run(Path("/s"))
        publish.assert_not_called()

    def test_rejects_missing_directory(self) -> None:
        with (
            patch.object(Path, "is_dir", return_value=False),
            self.assertRaises(FileNotFoundError),
        ):
            subject.run(Path("/missing"))

    def test_cli(self) -> None:
        with (
            patch("sys.argv", ["publisher", "--snapshot-directory", "/s"]),
            patch.object(subject, "run") as run,
        ):
            subject.main()
        run.assert_called_once_with(Path("/s"))


if __name__ == "__main__":
    unittest.main()
