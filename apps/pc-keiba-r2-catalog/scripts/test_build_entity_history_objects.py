#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5,<1.6",
#   "pyarrow>=19",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///
"""Tests for the low-latency entity-history object builder."""

from __future__ import annotations

import argparse
import gzip
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from build_entity_history_objects import (
    grouped_rows,
    parse_year,
    read_generations,
    write_generations,
    write_year_objects,
)


def row(**overrides: object) -> dict[str, object]:
    return {
        "entity_type": "horse",
        "source": "jra",
        "entity_bucket": "a",
        "entity_id": "2023100001",
        "kaisai_nen": "2026",
        "kaisai_tsukihi": "0827",
        **overrides,
    }


def read_gzip_json(path: Path) -> object:
    with gzip.open(path, "rb") as source:
        return json.loads(source.read())


class EntityHistoryObjectBuilderTest(unittest.TestCase):
    def test_parse_year_rejects_invalid_values(self) -> None:
        self.assertEqual(parse_year("1986"), "1986")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_year("1985")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_year("26")

    def test_grouped_rows_partitions_history_and_target_days(self) -> None:
        history, targets = grouped_rows(
            [
                row(),
                row(entity_type="owner", entity_bucket="f", entity_id="18003"),
                row(source="nar", entity_bucket="b", kaisai_tsukihi="0828"),
            ],
            "2026",
        )
        self.assertEqual(len(history[("horse", "jra", "a", "1")]), 1)
        self.assertEqual(len(history[("owner", "jra", "f", "3")]), 1)
        self.assertEqual(len(targets[("jra", "0827")]), 2)
        self.assertEqual(len(targets[("nar", "0828")]), 1)

    def test_grouped_rows_rejects_malformed_partitions(self) -> None:
        with self.assertRaisesRegex(ValueError, "bucket"):
            grouped_rows([row(entity_bucket="z")], "2026")
        with self.assertRaisesRegex(ValueError, "year"):
            grouped_rows([row(kaisai_nen="2025")], "2026")

    def test_write_year_objects_creates_compressed_envelopes(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory)
            history_count, target_count = write_year_objects(
                output,
                "2026",
                "generation1",
                [
                    row(),
                    row(
                        entity_type="jockey",
                        entity_id="12345",
                        entity_bucket="f",
                    ),
                ],
            )
            self.assertEqual(history_count, 2)
            self.assertEqual(target_count, 1)
            history = read_gzip_json(
                output / "data/2026/generation1/history/horse/jra/a-1.json.gz"
            )
            target = read_gzip_json(
                output / "data/2026/generation1/target/jra/0827.json.gz"
            )
            self.assertEqual(history, {"rows": [row()], "version": 1})
            self.assertIsInstance(target, dict)
            if not isinstance(target, dict):
                self.fail("target envelope must be a dictionary")
            self.assertEqual(len(target["rows"]), 2)

    def test_generation_manifest_round_trip(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory)
            self.assertEqual(read_generations(output), {})
            write_generations(output, {"2025": "old", "2026": "new"})
            self.assertEqual(read_generations(output), {"2025": "old", "2026": "new"})
            (output / "generations.json").write_text("[]")
            with self.assertRaisesRegex(ValueError, "malformed"):
                read_generations(output)


if __name__ == "__main__":
    unittest.main()
