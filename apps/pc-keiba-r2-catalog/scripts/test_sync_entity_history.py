#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5,<1.6",
#   "pyarrow>=19",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///
"""Tests for the entity history serving-table synchronizer."""

from __future__ import annotations

import argparse
import unittest

import pyarrow as pa
import sync_entity_history as subject
from pyiceberg.transforms import IdentityTransform


class EntityHistorySyncTest(unittest.TestCase):
    def test_parse_year_accepts_supported_year(self) -> None:
        self.assertEqual(subject.parse_year("2026"), "2026")

    def test_parse_year_rejects_old_year(self) -> None:
        with self.assertRaisesRegex(argparse.ArgumentTypeError, "at least 1986"):
            subject.parse_year("1985")

    def test_history_query_expands_all_entities_and_sources(self) -> None:
        query = subject.history_query("2026")
        self.assertIn("('horse', se.ketto_toroku_bango, se.bamei)", query)
        self.assertIn("('jockey', se.kishu_code, se.kishumei_ryakusho)", query)
        self.assertIn("('trainer', se.chokyoshi_code, se.chokyoshimei_ryakusho)", query)
        self.assertIn("('owner', se.banushi_code, se.banushimei)", query)
        self.assertIn("source_pg.public.jvd_se", query)
        self.assertIn("source_pg.public.nvd_se", query)
        self.assertIn("se.kaisai_nen = '2026'", query)
        self.assertIn(
            "substr(md5(trim(replace(coalesce(entity.entity_id, ''), chr(12288), ''))), 1, 1)",
            query,
        )

    def test_target_query_includes_unfinished_runners(self) -> None:
        history_query = subject.history_query("2026")
        target_query = subject.target_query("2026")
        self.assertIn("'0') <> '0'", history_query)
        self.assertNotIn("'0') <> '0'", target_query)
        self.assertIn("source_pg.public.jvd_se", target_query)
        self.assertIn("source_pg.public.nvd_se", target_query)

    def test_table_spec_has_stable_primary_key(self) -> None:
        self.assertEqual(
            subject.table_spec().primary_key,
            ("entity_type", "source", "entity_id", "result_id"),
        )

    def test_partition_spec_prunes_entity_source_bucket_and_year(self) -> None:
        data = pa.table(
            {
                "entity_type": ["jockey"],
                "source": ["nar"],
                "entity_id": ["21379"],
                "result_id": ["nar:20260827:50:04:07:2022103916"],
                "entity_bucket": ["c"],
                "kaisai_nen": ["2026"],
            }
        )
        spec = subject.partition_spec(data)
        self.assertEqual(
            tuple(field.name for field in spec.fields),
            ("entity_type", "source", "entity_bucket", "kaisai_nen"),
        )
        self.assertIsInstance(spec.fields[0].transform, IdentityTransform)
        self.assertIsInstance(spec.fields[1].transform, IdentityTransform)
        self.assertIsInstance(spec.fields[2].transform, IdentityTransform)
        self.assertIsInstance(spec.fields[3].transform, IdentityTransform)


if __name__ == "__main__":
    unittest.main()
