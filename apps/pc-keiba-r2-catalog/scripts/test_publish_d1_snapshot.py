#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Tests for lossless D1-to-Iceberg batch publication."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, mock_open, patch

import pyarrow as pa
from pyiceberg.expressions import BooleanExpression

import publish_d1_snapshot as subject


class FakeScan:
    def __init__(self, data: pa.Table) -> None:
        self.data = data

    def to_arrow(self) -> pa.Table:
        return self.data


class FakeTable:
    def __init__(self, *, corrupt: bool = False) -> None:
        self.data = pa.Table.from_pylist([], schema=subject.ARROW_SCHEMA)
        self.corrupt = corrupt
        self.writes = 0

    def overwrite(
        self,
        df: pa.Table,
        *,
        overwrite_filter: BooleanExpression,
        snapshot_properties: dict[str, str],
    ) -> None:
        self.data = df.slice(0, 0) if self.corrupt else df
        self.writes += 1

    def scan(
        self, *, row_filter: BooleanExpression, selected_fields: tuple[str, ...]
    ) -> FakeScan:
        return FakeScan(self.data.select(selected_fields))


class SnapshotPublicationTests(unittest.TestCase):
    def test_loads_lossless_source_typed_rows(self) -> None:
        document = {
            "snapshot_id": "s1",
            "database_name": "db",
            "table_name": "events",
            "rows": [
                {
                    "row_key": "9223372036854775807",
                    "payload": '{"v":{"type":"integer","value":"9223372036854775807"}}',
                }
            ],
        }
        with (
            patch.object(Path, "stat", return_value=SimpleNamespace(st_size=100)),
            patch.object(Path, "open", mock_open(read_data=json.dumps(document))),
        ):
            batch = subject.load_batch(Path("/batch"))
        self.assertEqual(
            batch.rows,
            (
                (
                    "9223372036854775807",
                    '{"v":{"type":"integer","value":"9223372036854775807"}}',
                ),
            ),
        )
        self.assertRegex(batch.batch_id, r"^[a-f0-9]{64}$")

    def test_rejects_oversized_file_before_read(self) -> None:
        with (
            patch.object(
                Path,
                "stat",
                return_value=SimpleNamespace(st_size=subject.MAX_BATCH_BYTES + 1),
            ),
            self.assertRaisesRegex(ValueError, "byte limit"),
        ):
            subject.load_batch(Path("/batch"))

    def test_rejects_non_object_document(self) -> None:
        with (
            patch.object(Path, "stat", return_value=SimpleNamespace(st_size=2)),
            patch.object(Path, "open", mock_open(read_data="[]")),
            self.assertRaisesRegex(TypeError, "Invalid snapshot batch"),
        ):
            subject.load_batch(Path("/batch"))

    def test_rejects_empty_identity(self) -> None:
        with self.assertRaises(ValueError):
            subject.text_field(1)
        with self.assertRaises(ValueError):
            subject.text_field("")

    def test_rejects_empty_row_batch(self) -> None:
        document = {
            "snapshot_id": "s1",
            "database_name": "db",
            "table_name": "events",
            "rows": [],
        }
        with (
            patch.object(Path, "stat", return_value=SimpleNamespace(st_size=100)),
            patch.object(Path, "open", mock_open(read_data=json.dumps(document))),
            self.assertRaisesRegex(ValueError, "row count"),
        ):
            subject.load_batch(Path("/batch"))

    def test_rejects_missing_row_list(self) -> None:
        document = {
            "snapshot_id": "s1",
            "database_name": "db",
            "table_name": "events",
            "rows": None,
        }
        with (
            patch.object(Path, "stat", return_value=SimpleNamespace(st_size=100)),
            patch.object(Path, "open", mock_open(read_data=json.dumps(document))),
            self.assertRaisesRegex(ValueError, "row count"),
        ):
            subject.load_batch(Path("/batch"))

    def test_rejects_duplicate_row_keys(self) -> None:
        document = {
            "snapshot_id": "s1",
            "database_name": "db",
            "table_name": "events",
            "rows": [
                {"row_key": "1", "payload": "{}"},
                {"row_key": "1", "payload": "{}"},
            ],
        }
        with (
            patch.object(Path, "stat", return_value=SimpleNamespace(st_size=100)),
            patch.object(Path, "open", mock_open(read_data=json.dumps(document))),
            self.assertRaisesRegex(ValueError, "Duplicate"),
        ):
            subject.load_batch(Path("/batch"))

    def test_rejects_invalid_row_shapes(self) -> None:
        with self.assertRaisesRegex(TypeError, "Invalid snapshot row"):
            subject.parse_row(None)
        with self.assertRaises(ValueError):
            subject.parse_row({"row_key": "01", "payload": "{}"})
        with self.assertRaises(ValueError):
            subject.parse_row({"row_key": "9223372036854775808", "payload": "{}"})
        with self.assertRaises(TypeError):
            subject.parse_row({"row_key": "1", "payload": "[]"})

    def test_batch_fingerprint_is_order_independent_and_identity_scoped(self) -> None:
        first = subject.fingerprint(
            identity=("s1", "db", "table"), rows=(("2", "{}"), ("1", "{}"))
        )
        reordered = subject.fingerprint(
            identity=("s1", "db", "table"), rows=(("1", "{}"), ("2", "{}"))
        )
        other = subject.fingerprint(
            identity=("s2", "db", "table"), rows=(("1", "{}"), ("2", "{}"))
        )
        self.assertEqual(len({first, reordered}), 1)
        self.assertEqual(len({first, other}), 2)

    def test_reconciles_actual_arrow_round_trip_and_idempotent_replay(self) -> None:
        rows = (("1", '{"v":{"type":"blob","value":"00FF"}}'),)
        batch = subject.SnapshotBatch(
            "s1",
            "db",
            "events",
            rows,
            subject.fingerprint(identity=("s1", "db", "events"), rows=rows),
        )
        table = FakeTable()
        self.assertRegex(subject.publish_batch(table, batch), r"^[a-f0-9]{64}$")
        self.assertRegex(subject.publish_batch(table, batch), r"^[a-f0-9]{64}$")
        self.assertEqual(table.data.num_rows, 1)
        self.assertEqual(
            table.data.column_names,
            [
                "snapshot_id",
                "database_name",
                "table_name",
                "batch_id",
                "row_key",
                "payload",
            ],
        )
        self.assertEqual(table.writes, 2)

    def test_reconciliation_fails_for_missing_catalog_rows(self) -> None:
        rows = (("1", "{}"),)
        batch = subject.SnapshotBatch(
            "s1",
            "db",
            "events",
            rows,
            subject.fingerprint(identity=("s1", "db", "events"), rows=rows),
        )
        with self.assertRaisesRegex(ValueError, "reconciliation failed"):
            subject.publish_batch(FakeTable(corrupt=True), batch)

    def test_reconciliation_fails_for_incorrect_expected_digest(self) -> None:
        batch = subject.SnapshotBatch("s1", "db", "events", (("1", "{}"),), "incorrect")
        with self.assertRaisesRegex(ValueError, "reconciliation failed"):
            subject.publish_batch(FakeTable(), batch)

    def test_creates_a_partitioned_v2_iceberg_table_and_publishes(self) -> None:
        rows = (("1", "{}"),)
        batch = subject.SnapshotBatch(
            "s1",
            "db",
            "events",
            rows,
            subject.fingerprint(identity=("s1", "db", "events"), rows=rows),
        )
        catalog = Mock()
        catalog.create_table_if_not_exists.return_value = FakeTable()
        with (
            patch.object(subject, "load_batch", return_value=batch),
            patch.dict(
                subject.os.environ,
                {"R2_ACCOUNT_ID": "account", "CLOUDFLARE_DEBUG_TOKEN": "token"},
            ),
            patch.object(subject, "load_catalog", return_value=catalog),
            patch("builtins.print"),
        ):
            subject.run(Path("/batch"))
        self.assertEqual(
            catalog.create_table_if_not_exists.call_args.args[0],
            "pc_keiba.d1_snapshot_rows",
        )
        self.assertEqual(
            catalog.create_table_if_not_exists.call_args.kwargs["properties"],
            {"format-version": "2", "write.parquet.compression-codec": "zstd"},
        )

    def test_cli_requires_an_explicit_batch_path(self) -> None:
        with (
            patch("sys.argv", ["publish", "--batch", "/batch"]),
            patch.object(subject, "run") as run,
        ):
            subject.main()
        self.assertEqual(str(run.call_args.args[0]), "/batch")


if __name__ == "__main__":
    unittest.main()
