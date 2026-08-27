#!/usr/bin/env python3
"""Tests for range-readable entity-history pack generation."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pack_entity_history_objects import pack, pack_members


class EntityHistoryPackTest(unittest.TestCase):
    def test_pack_members_records_stable_offsets(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            (source / "a.json.gz").write_bytes(b"abc")
            (source / "b.json.gz").write_bytes(b"defg")
            index = pack_members(source, root / "members.pack")
            self.assertEqual(index, {"a": [0, 3], "b": [3, 4]})
            self.assertEqual((root / "members.pack").read_bytes(), b"abcdefg")

    def test_pack_writes_three_objects_per_generation(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory)
            generation = "generation"
            source = output / "data/2026" / generation
            (source / "history/horse/jra").mkdir(parents=True)
            (source / "target/jra").mkdir(parents=True)
            (source / "history/horse/jra/a-1.json.gz").write_bytes(b"history")
            (source / "target/jra/0827.json.gz").write_bytes(b"target")
            (output / "generations.json").write_text(
                json.dumps({"version": 1, "years": {"2026": generation}})
            )
            self.assertEqual(pack(output), 3)
            index = json.loads(
                (output / f"packed/2026/{generation}/index.json").read_text()
            )
            self.assertEqual(index["history"], {"horse/jra/a-1": [0, 7]})
            self.assertEqual(index["target"], {"jra/0827": [0, 6]})

    def test_incremental_pack_preserves_other_years(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "packed/2025/old").mkdir(parents=True)
            (output / "packed/2025/old/index.json").write_text("old")
            source = output / "data/2026/new"
            (source / "history").mkdir(parents=True)
            (source / "target").mkdir(parents=True)
            (output / "generations.json").write_text(
                json.dumps({"version": 1, "years": {"2025": "old", "2026": "new"}})
            )
            self.assertEqual(pack(output, {"2026"}), 3)
            self.assertEqual((output / "packed/2025/old/index.json").read_text(), "old")
            self.assertTrue((output / "packed/2026/new/index.json").is_file())
            with self.assertRaisesRegex(ValueError, "2024"):
                pack(output, {"2024"})


if __name__ == "__main__":
    unittest.main()
