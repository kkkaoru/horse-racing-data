# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Isolated capture artifact and native Arrow publication tests."""

from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Final
from unittest.mock import Mock, mock_open, patch

import publish_d1_capture as subject
import pyarrow as pa


class Scan:
    def __init__(self, data: pa.Table) -> None:
        self.data = data

    def to_arrow(self) -> pa.Table:
        return self.data


EVENT: Final[subject.CaptureEvent] = subject.CaptureEvent(
    "test",
    "items",
    "0" * 64,
    1,
    "insert",
    "2026-09-16T00:00:00.000Z",
    None,
    "9007199254740993",
)
BATCH: Final[subject.CaptureBatch] = subject.CaptureBatch(
    "sample-db",
    "00000000-0000-0000-0000-000000000001",
    (EVENT,),
    "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
)
EMPTY: Final[pa.Table] = pa.Table.from_pylist([], schema=subject.ARROW_SCHEMA)
DOCUMENT: Final[dict[str, object]] = {
    "formatVersion": 1,
    "databaseName": "sample-db",
    "databaseId": "00000000-0000-0000-0000-000000000001",
    "events": [subject.event_document(EVENT)],
    "batchId": "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
}


class CaptureTests(unittest.TestCase):
    def test_cli_publishes_only_to_dedicated_event_table(self) -> None:
        native = pa.Table.from_pylist(
            [subject.native_row(batch=BATCH, event=EVENT)], schema=subject.ARROW_SCHEMA
        )
        table = Mock(spec=subject.CaptureTable)
        table.scan.side_effect = [Scan(EMPTY), Scan(native)]
        catalog = Mock()
        catalog.create_table_if_not_exists.return_value = table
        with (
            patch.object(
                Path, "open", mock_open(read_data=json.dumps(DOCUMENT).encode())
            ),
            patch.dict(
                subject.os.environ,
                {
                    "R2_ACCOUNT_ID": "0" * 32,
                    "CLOUDFLARE_DEBUG_TOKEN": "private-test-token",
                },
                clear=True,
            ),
            patch.object(subject, "load_catalog", return_value=catalog),
            patch("builtins.print") as output,
        ):
            subject.run(Path("/batch"))
        self.assertEqual(
            catalog.create_table_if_not_exists.call_args.args[0],
            "pc_keiba.d1_capture_events",
        )
        self.assertEqual(
            json.loads(output.call_args.args[0]),
            {
                "batchId": "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
                "eventCount": 1,
                "lastSequence": "1",
                "reconciled": True,
            },
        )
        self.assertNotIn("private-test-token", output.call_args.args[0])

    def test_cli_rejects_missing_or_invalid_credentials_before_network(self) -> None:
        with (
            patch.object(subject, "load_batch", return_value=BATCH),
            patch.dict(subject.os.environ, {}, clear=True),
            self.assertRaises(TypeError),
        ):
            subject.run(Path("/batch"))
        with (
            patch.object(subject, "load_batch", return_value=BATCH),
            patch.dict(
                subject.os.environ,
                {"R2_ACCOUNT_ID": "bad", "CLOUDFLARE_DEBUG_TOKEN": "x"},
                clear=True,
            ),
            self.assertRaisesRegex(ValueError, "credentials"),
        ):
            subject.run(Path("/batch"))
        with (
            patch.object(subject, "load_batch", return_value=BATCH),
            patch.dict(
                subject.os.environ,
                {"R2_ACCOUNT_ID": "0" * 32, "CLOUDFLARE_DEBUG_TOKEN": " "},
                clear=True,
            ),
            self.assertRaisesRegex(ValueError, "credentials"),
        ):
            subject.run(Path("/batch"))

    def test_main_forwards_required_artifact_path(self) -> None:
        with (
            patch("sys.argv", ["publish_d1_capture.py", "--batch", "/batch"]),
            patch.object(subject, "run") as run,
        ):
            subject.main()
        run.assert_called_once_with(Path("/batch"))

    def test_loads_artifact_with_exact_typescript_digest(self) -> None:
        with patch.object(
            Path, "open", mock_open(read_data=json.dumps(DOCUMENT).encode())
        ):
            batch = subject.load_batch(Path("/batch"))
        self.assertEqual(
            batch.batch_id,
            "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
        )
        self.assertEqual(batch.events[0].after_key, "9007199254740993")
        self.assertEqual(
            subject.fingerprint(batch),
            "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
        )

    def test_bounded_file_read(self) -> None:
        with (
            patch.object(
                Path, "open", mock_open(read_data=b"x" * (subject.MAX_BYTES + 1))
            ),
            self.assertRaisesRegex(ValueError, "byte limit"),
        ):
            subject.load_batch(Path("/batch"))

    def test_invalid_document(self) -> None:
        with (
            patch.object(Path, "open", mock_open(read_data=b"[]")),
            self.assertRaisesRegex(ValueError, "format"),
        ):
            subject.load_batch(Path("/batch"))
        with (
            patch.object(
                Path,
                "open",
                mock_open(
                    read_data=json.dumps({**DOCUMENT, "formatVersion": True}).encode()
                ),
            ),
            self.assertRaisesRegex(ValueError, "format"),
        ):
            subject.load_batch(Path("/batch"))
        with (
            patch.object(
                Path,
                "open",
                mock_open(
                    read_data=json.dumps({**DOCUMENT, "formatVersion": 2}).encode()
                ),
            ),
            self.assertRaisesRegex(ValueError, "format"),
        ):
            subject.load_batch(Path("/batch"))
        with (
            patch.object(
                Path,
                "open",
                mock_open(read_data=json.dumps({**DOCUMENT, "events": None}).encode()),
            ),
            self.assertRaisesRegex(ValueError, "event count"),
        ):
            subject.load_batch(Path("/batch"))
        with (
            patch.object(
                Path,
                "open",
                mock_open(read_data=json.dumps({**DOCUMENT, "events": []}).encode()),
            ),
            self.assertRaisesRegex(ValueError, "event count"),
        ):
            subject.load_batch(Path("/batch"))

    def test_identity_order_and_digest_rejections(self) -> None:
        with self.assertRaisesRegex(ValueError, "event count"):
            subject.validate_batch(replace(BATCH, events=()))
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.validate_batch(replace(BATCH, database_name="bad/name"))
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.validate_batch(replace(BATCH, database_id="bad"))
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.validate_batch(replace(BATCH, batch_id="bad"))
        with self.assertRaisesRegex(ValueError, "ordered"):
            subject.validate_batch(replace(BATCH, events=(EVENT, EVENT)))
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            subject.validate_batch(replace(BATCH, batch_id="1" * 64))

    def test_exact_integer_validation(self) -> None:
        self.assertEqual(subject.integer("9223372036854775807"), 9223372036854775807)
        self.assertEqual(subject.integer("-9223372036854775808"), -9223372036854775808)
        with self.assertRaises(TypeError):
            subject.integer(1)
        with self.assertRaises(ValueError):
            subject.integer("123456789012345678901")
        with self.assertRaises(ValueError):
            subject.integer("01")
        with self.assertRaises(ValueError):
            subject.integer("9223372036854775808")
        with self.assertRaises(ValueError):
            subject.integer("١")
        with self.assertRaises(ValueError):
            subject.integer("1١")

    def test_event_validation(self) -> None:
        with self.assertRaisesRegex(ValueError, "fields"):
            subject.parse_event({})
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.parse_event(
                {**subject.event_document(EVENT), "captureId": "bad/name"}
            )
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.parse_event({**subject.event_document(EVENT), "table": ""})
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.parse_event({**subject.event_document(EVENT), "table": "x" * 257})
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.parse_event({**subject.event_document(EVENT), "table": "🚀" * 129})
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.parse_event({**subject.event_document(EVENT), "schemaHash": "bad"})
        with self.assertRaisesRegex(ValueError, "identity"):
            subject.parse_event({**subject.event_document(EVENT), "sequence": "0"})
        with self.assertRaisesRegex(ValueError, "operation keys"):
            subject.parse_event(
                {**subject.event_document(EVENT), "operation": "invalid"}
            )
        with self.assertRaisesRegex(ValueError, "operation keys"):
            subject.parse_event({**subject.event_document(EVENT), "operation": "touch"})
        with self.assertRaisesRegex(ValueError, "timestamp"):
            subject.parse_event({**subject.event_document(EVENT), "capturedAt": "bad"})
        with self.assertRaisesRegex(ValueError, "timestamp"):
            subject.parse_event(
                {
                    **subject.event_document(EVENT),
                    "capturedAt": "2026-09-16 00:00:00.000Z",
                }
            )
        self.assertEqual(
            subject.parse_event(
                {
                    **subject.event_document(EVENT),
                    "operation": "update",
                    "beforeKey": "2",
                }
            ).operation,
            "update",
        )
        self.assertEqual(
            subject.parse_event(
                {
                    **subject.event_document(EVENT),
                    "operation": "delete",
                    "beforeKey": "2",
                    "afterKey": None,
                }
            ).operation,
            "delete",
        )
        self.assertEqual(
            subject.parse_event(
                {
                    **subject.event_document(EVENT),
                    "operation": "touch",
                    "beforeKey": "2",
                    "afterKey": "2",
                }
            ).before_key,
            "2",
        )

    def test_append_and_native_readback(self) -> None:
        native = pa.Table.from_pylist(
            [subject.native_row(batch=BATCH, event=EVENT)], schema=subject.ARROW_SCHEMA
        )
        table = Mock(spec=subject.CaptureTable)
        table.scan.side_effect = [Scan(EMPTY), Scan(native)]
        self.assertEqual(
            subject.publish_batch(table, BATCH),
            "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
        )
        self.assertEqual(table.append.call_count, 1)
        self.assertEqual(table.refresh.call_count, 2)
        self.assertEqual(
            table.append.call_args.args[0].column("sequence").to_pylist(), [1]
        )
        self.assertEqual(
            table.append.call_args.args[0].column("after_key").to_pylist(),
            ["9007199254740993"],
        )

    def test_native_sequence_keeps_int64_precision(self) -> None:
        event = replace(EVENT, sequence=9007199254740993)
        draft = replace(BATCH, events=(event,))
        batch = replace(draft, batch_id=subject.fingerprint(draft))
        native = pa.Table.from_pylist(
            [subject.native_row(batch=batch, event=event)], schema=subject.ARROW_SCHEMA
        )
        table = Mock(spec=subject.CaptureTable)
        table.scan.side_effect = [Scan(EMPTY), Scan(native)]
        subject.publish_batch(table, batch)
        self.assertEqual(
            table.append.call_args.args[0].column("sequence").to_pylist(),
            [9007199254740993],
        )
        self.assertEqual(
            str(table.append.call_args.args[0].column("sequence").type), "int64"
        )

    def test_uncertain_acknowledgement_requires_fresh_reconciliation_not_blind_retry(
        self,
    ) -> None:
        native = pa.Table.from_pylist(
            [subject.native_row(batch=BATCH, event=EVENT)], schema=subject.ARROW_SCHEMA
        )
        table = Mock(spec=subject.CaptureTable)
        table.scan.side_effect = [Scan(EMPTY), Scan(native), Scan(native)]
        table.append.side_effect = RuntimeError("lost ack")
        with self.assertRaisesRegex(RuntimeError, "lost ack"):
            subject.publish_batch(table, BATCH)
        self.assertEqual(
            subject.publish_batch(table, BATCH),
            "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
        )
        self.assertEqual(table.append.call_count, 1)

    def test_overlapping_batch_keeps_existing_event_and_appends_only_missing(
        self,
    ) -> None:
        second = replace(EVENT, sequence=2)
        draft = replace(BATCH, events=(EVENT, second))
        batch = replace(draft, batch_id=subject.fingerprint(draft))
        old_row = subject.native_row(batch=BATCH, event=EVENT)
        new_row = subject.native_row(batch=batch, event=second)
        table = Mock(spec=subject.CaptureTable)
        table.scan.side_effect = [
            Scan(pa.Table.from_pylist([old_row], schema=subject.ARROW_SCHEMA)),
            Scan(pa.Table.from_pylist([old_row, new_row], schema=subject.ARROW_SCHEMA)),
        ]
        subject.publish_batch(table, batch)
        self.assertEqual(
            table.append.call_args.args[0].column("sequence").to_pylist(), [2]
        )

    def test_incomplete_readback_never_acknowledged(self) -> None:
        table = Mock(spec=subject.CaptureTable)
        table.scan.return_value = Scan(EMPTY)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            subject.publish_batch(table, BATCH)

    def test_conflicts_and_duplicates_never_overwrite_events(self) -> None:
        row = subject.native_row(batch=BATCH, event=EVENT)
        table = Mock(spec=subject.CaptureTable)
        table.scan.return_value = Scan(
            pa.Table.from_pylist(
                [{**row, "after_key": "wrong"}], schema=subject.ARROW_SCHEMA
            )
        )
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            subject.publish_batch(table, BATCH)
        table.scan.return_value = Scan(
            pa.Table.from_pylist([row, row], schema=subject.ARROW_SCHEMA)
        )
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            subject.publish_batch(table, BATCH)
        table.scan.return_value = Scan(
            pa.Table.from_pylist([{**row, "sequence": 2}], schema=subject.ARROW_SCHEMA)
        )
        with self.assertRaisesRegex(ValueError, "Unexpected"):
            subject.publish_batch(table, BATCH)
        table.scan.return_value = Scan(
            pa.Table.from_pylist(
                [{**row, "sequence": None}], schema=subject.ARROW_SCHEMA
            )
        )
        with self.assertRaisesRegex(ValueError, "Unexpected"):
            subject.publish_batch(table, BATCH)
        self.assertEqual(table.append.call_count, 0)

    def test_duplicate_identity_within_expected_count(self) -> None:
        draft = replace(BATCH, events=(EVENT, replace(EVENT, sequence=2)))
        batch = replace(draft, batch_id=subject.fingerprint(draft))
        row = subject.native_row(batch=BATCH, event=EVENT)
        table = Mock(spec=subject.CaptureTable)
        table.scan.return_value = Scan(
            pa.Table.from_pylist([row, row], schema=subject.ARROW_SCHEMA)
        )
        with self.assertRaisesRegex(ValueError, "Unexpected"):
            subject.publish_batch(table, batch)


if __name__ == "__main__":
    unittest.main()
