#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5,<1.6",
#   "pyarrow>=19",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
#   "psycopg[binary]>=3.2",
# ]
# ///
"""Tests for the running-style R2 Catalog backfill."""

from __future__ import annotations

import datetime as dt
import unittest

import backfill_running_style_catalog as subject
import duckdb
import pyarrow as pa


def prediction_row(
    model_version: str, horse: str, p_nige: float, generated_at: dt.datetime | None
) -> dict[str, object]:
    return {
        "model_version": model_version,
        "source": "jra",
        "kaisai_nen": "2026",
        "kaisai_tsukihi": "1018",
        "keibajo_code": "05",
        "race_bango": "11",
        "ketto_toroku_bango": horse,
        "umaban": 1,
        "p_nige": p_nige,
        "p_senkou": 0.2,
        "p_sashi": 0.2,
        "p_oikomi": 0.2,
        "predicted_class": 0,
        "predicted_label": "nige",
        "cell_model_key": None,
        "cell_variant_id": None,
        "predicted_corner_front_score": None,
        "predicted_corner_rank": None,
        "prediction_generated_at": generated_at,
    }


class BackfillRunningStyleCatalogTest(unittest.TestCase):
    def test_combined_rows_keep_the_newest_prediction_per_key(self) -> None:
        older = dt.datetime(2026, 10, 18, 9, 0, tzinfo=dt.UTC)
        newer = dt.datetime(2026, 10, 18, 10, 0, tzinfo=dt.UTC)
        con = duckdb.connect()
        con.execute("create schema local_db")
        con.register(
            "local_rows",
            pa.Table.from_pylist(
                [
                    prediction_row("v3", "A", 0.1, older),
                    prediction_row("v1", "A", 0.3, older),
                ]
            ),
        )
        con.execute("create view local_db.public_rows as select * from local_rows")
        con.register(
            "neon_rows",
            pa.Table.from_pylist(
                [
                    prediction_row("v3", "A", 0.9, newer),
                    prediction_row("v3", "B", 0.5, None),
                ]
            ),
        )
        sql = subject.combined_rows_sql("local_db", "neon_rows").replace(
            "local_db.public.race_running_style_model_predictions",
            "local_db.public_rows",
        )
        rows = con.execute(
            f"select model_version, ketto_toroku_bango, p_nige from ({sql}) order by 1, 2"
        ).fetchall()
        self.assertEqual(rows, [("v1", "A", 0.3), ("v3", "A", 0.9), ("v3", "B", 0.5)])

    def test_combined_rows_prefer_the_neon_row_on_equal_timestamps(self) -> None:
        generated_at = dt.datetime(2026, 10, 18, 9, 0, tzinfo=dt.UTC)
        con = duckdb.connect()
        con.execute("create schema local_db")
        con.register(
            "local_rows",
            pa.Table.from_pylist([prediction_row("v3", "A", 0.1, generated_at)]),
        )
        con.execute("create view local_db.public_rows as select * from local_rows")
        con.register(
            "neon_rows",
            pa.Table.from_pylist([prediction_row("v3", "A", 0.9, generated_at)]),
        )
        sql = subject.combined_rows_sql("local_db", "neon_rows").replace(
            "local_db.public.race_running_style_model_predictions",
            "local_db.public_rows",
        )
        self.assertEqual(
            con.execute(f"select p_nige from ({sql})").fetchall(), [(0.9,)]
        )


if __name__ == "__main__":
    unittest.main()
