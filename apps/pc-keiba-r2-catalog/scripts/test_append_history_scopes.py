#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb>=1.5,<1.6", "pyarrow>=19", "pyiceberg[pyarrow,s3fs]>=0.11,<0.12"]
# ///
"""Offline verification of insert-only history Catalog publication."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import final, override
from unittest.mock import MagicMock, patch

import pyarrow as pa

import append_history_scopes as subject
import sync_r2_catalog as sync


@final
class AppendHistoryTests(unittest.TestCase):
    @override
    def setUp(self) -> None:
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.manifest = Path(self.temp) / "scopes.json"
        self.manifest.write_text(
            '{"scopes":[{"key":"owner:415800"}]}', encoding="utf-8"
        )
        self.output = Path(self.temp) / "new"
        self.settings = sync.load_settings({})
        self.source = pa.table(
            {
                "history_id": [1, 2, 3, 4],
                "source": ["netkeiba", "netkeiba", "netkeiba", "other"],
                "person_kind": ["owner", "owner", "trainer", "owner"],
                "source_person_id": ["415800", "415800", "01038", "415800"],
                "value": ["old", "new", "unrequested", "unrequested-source"],
            }
        )
        self.source = sync.require_primary_key_fields(
            self.source, sync.TABLE_SPECS[subject.TABLE_NAME]
        )
        self.before = self.source.slice(0, 1)
        self.after = self.source.slice(0, 2)
        self.connection = MagicMock()
        self.table = MagicMock()
        self.enterContext(
            patch.object(sync, "connect_source", return_value=self.connection)
        )
        self.enterContext(patch.object(sync, "create_catalog"))
        self.load_table = self.enterContext(
            patch.object(sync, "load_existing_table", return_value=self.table)
        )
        self.enterContext(patch.object(sync, "validate_existing_table"))
        self.enterContext(
            patch.object(sync, "extract_source", return_value=self.source)
        )
        self.enterContext(
            patch.object(
                sync,
                "conform_to_table",
                side_effect=lambda data, table, spec: data,
            )
        )
        self.read_target = self.enterContext(
            patch.object(sync, "read_target", side_effect=[self.before, self.after])
        )
        self.snapshot = self.enterContext(
            patch.object(sync, "current_snapshot_id", return_value=42)
        )
        self.record = self.enterContext(patch.object(sync, "record_table_fingerprint"))

    def test_scope_manifest_validates_and_deduplicates(self) -> None:
        self.manifest.write_text(
            '{"scopes":[{"key":"owner:415800"},{"key":"owner:415800"},{"key":"trainer:a02fe"}]}',
            encoding="utf-8",
        )
        self.assertEqual(
            subject.load_scopes(self.manifest),
            frozenset({("owner", "415800"), ("trainer", "a02fe")}),
        )

    def test_invalid_manifest_shape(self) -> None:
        self.manifest.write_text("[]", encoding="utf-8")
        with self.assertRaisesRegex(TypeError, "scopes array"):
            subject.load_scopes(self.manifest)

    def test_invalid_scope_list(self) -> None:
        self.manifest.write_text('{"scopes":null}', encoding="utf-8")
        with self.assertRaisesRegex(TypeError, "scopes array"):
            subject.load_scopes(self.manifest)

    def test_empty_manifest(self) -> None:
        self.manifest.write_text('{"scopes":[]}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "At least one"):
            subject.load_scopes(self.manifest)

    def test_non_object_scope(self) -> None:
        self.manifest.write_text('{"scopes":[null]}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Invalid source"):
            subject.load_scopes(self.manifest)

    def test_unsupported_scope(self) -> None:
        self.manifest.write_text('{"scopes":[{"key":"horse:abc"}]}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Invalid source"):
            subject.load_scopes(self.manifest)

    def test_missing_existing_row_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "preserve every existing"):
            subject.select_delta(
                source=self.source.slice(1),
                before=self.before,
                scopes=frozenset({("owner", "415800")}),
            )

    def test_changed_existing_value_rejected(self) -> None:
        changed = self.before.set_column(4, "value", pa.array(["different"]))
        with self.assertRaisesRegex(RuntimeError, "preserve every existing"):
            subject.select_delta(
                source=self.source,
                before=changed,
                scopes=frozenset({("owner", "415800")}),
            )

    def test_duplicate_primary_key_rejected(self) -> None:
        with self.assertRaises(ValueError):
            subject.select_delta(
                source=pa.concat_tables([self.before, self.before]),
                before=self.before,
                scopes=frozenset({("owner", "415800")}),
            )

    def test_only_requested_new_rows_append_and_read_back(self) -> None:
        result = subject.append_scopes(
            manifest=self.manifest, output=self.output, settings=self.settings
        )
        self.assertEqual(result["appendedRows"], 1)
        self.assertEqual(result["namespace"], "pc_keiba")
        self.assertEqual(
            result["warehouse"], "78109ec18c7c85b194b19fb32e3bb149_pc-keiba-r2-catalog"
        )
        self.assertEqual(result["verifiedRows"], 2)
        self.assertEqual(result["oldRowsUnchanged"], True)
        self.assertEqual(
            self.table.append.call_args.args[0].column("history_id").to_pylist(), [2]
        )
        self.assertTrue((self.output / "pending.json").exists())
        self.assertEqual(
            json.loads((self.output / "receipt.json").read_text(encoding="utf-8"))[
                "oldRows"
            ],
            1,
        )
        self.assertEqual((self.output / "receipt.json").stat().st_mode & 0o777, 0o600)
        self.connection.close.assert_called_once()
        self.record.assert_called_once()

    def test_existing_directory_refuses_replay(self) -> None:
        self.output.mkdir()
        with self.assertRaises(FileExistsError):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=self.settings
            )
        self.table.append.assert_not_called()

    def test_nonlocal_authority_rejected(self) -> None:
        remote = sync.Settings(
            "postgresql://host/db", "uri", "warehouse", "namespace", None
        )
        with self.assertRaisesRegex(ValueError, "local PostgreSQL"):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=remote
            )
        self.table.append.assert_not_called()

    def test_missing_table_rejected_and_connection_closed(self) -> None:
        self.load_table.return_value = None
        with self.assertRaisesRegex(RuntimeError, "existing production"):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=self.settings
            )
        self.connection.close.assert_called_once()

    def test_missing_snapshot_rejected(self) -> None:
        self.snapshot.return_value = None
        with self.assertRaisesRegex(RuntimeError, "pinned Catalog"):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=self.settings
            )
        self.table.append.assert_not_called()

    def test_concurrent_snapshot_change_never_writes(self) -> None:
        self.snapshot.side_effect = [42, 43]
        with self.assertRaisesRegex(RuntimeError, "changed before append"):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=self.settings
            )
        self.table.append.assert_not_called()
        self.assertTrue((self.output / "pending.json").exists())
        self.assertFalse((self.output / "receipt.json").exists())

    def test_readback_wrong_count_keeps_pending_without_receipt(self) -> None:
        self.read_target.side_effect = [self.before, self.before]
        with self.assertRaisesRegex(RuntimeError, "readback mismatch"):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=self.settings
            )
        self.assertFalse((self.output / "receipt.json").exists())
        self.record.assert_not_called()

    def test_readback_changed_value_rejected(self) -> None:
        self.read_target.side_effect = [
            self.before,
            self.after.set_column(4, "value", pa.array(["old", "corrupt"])),
        ]
        with self.assertRaisesRegex(RuntimeError, "readback mismatch"):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=self.settings
            )
        self.record.assert_not_called()

    def test_uncertain_append_never_retries(self) -> None:
        self.table.append.side_effect = TimeoutError("uncertain write")
        with self.assertRaises(TimeoutError):
            subject.append_scopes(
                manifest=self.manifest, output=self.output, settings=self.settings
            )
        self.table.append.assert_called_once()
        self.assertTrue((self.output / "pending.json").exists())
        self.assertFalse((self.output / "receipt.json").exists())
        self.connection.close.assert_called_once()

    def test_no_missing_rows_causes_no_catalog_writes(self) -> None:
        self.read_target.side_effect = [self.after, self.after]
        result = subject.append_scopes(
            manifest=self.manifest, output=self.output, settings=self.settings
        )
        self.assertEqual(result["appendedRows"], 0)
        self.table.append.assert_not_called()
        self.record.assert_not_called()

    def test_json_evidence_cannot_be_overwritten(self) -> None:
        with self.assertRaises(FileExistsError):
            subject.save_json(self.manifest, {})

    def test_cli_requires_confirmation(self) -> None:
        with self.assertRaises(SystemExit), patch("sys.stderr"):
            subject.main(["manifest", "output"])
        self.table.append.assert_not_called()

    def test_cli_delegates_and_prints_receipt(self) -> None:
        with (
            patch.object(
                subject, "append_scopes", return_value={"appendedRows": 1}
            ) as append,
            patch("builtins.print") as printed,
        ):
            self.assertEqual(
                subject.main(
                    [str(self.manifest), str(self.output), "--confirm-append"]
                ),
                0,
            )
        append.assert_called_once()
        printed.assert_called_once_with('{"appendedRows": 1}')


if __name__ == "__main__":
    unittest.main()
