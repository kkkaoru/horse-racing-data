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
"""Backfill the R2 Catalog ``race_running_style_model_predictions`` Iceberg table.

Running-style predictions become an R2 Catalog table so training and other readers
no longer need Neon. History (2024-2025 walk-forward model versions) is read from the
local PostgreSQL training DB; production rows from the latest local year on come from Neon, which is kept as
the backup copy. Rows are de-duplicated on the table's natural key
(model_version, source, race, horse), keeping the newest prediction_generated_at, and
the whole table is replaced in one Iceberg commit.

Usage:
  uv run scripts/backfill_running_style_catalog.py --dry-run
  uv run scripts/backfill_running_style_catalog.py
Env: CLOUDFLARE_DEBUG_TOKEN (or R2_CATALOG_TOKEN), NEON_PRIMARY_URL, optional
PC_KEIBA_SOURCE_DATABASE_URL for the local DB.
"""

from __future__ import annotations

import argparse
import os
from typing import Final

import duckdb
import psycopg
import pyarrow as pa
from sync_r2_catalog import create_catalog, load_settings

TABLE: Final[str] = "race_running_style_model_predictions"
KEY_COLUMNS: Final[tuple[str, ...]] = (
    "model_version",
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
)
COLUMNS: Final[tuple[str, ...]] = (
    *KEY_COLUMNS,
    "umaban",
    "p_nige",
    "p_senkou",
    "p_sashi",
    "p_oikomi",
    "predicted_class",
    "predicted_label",
    "cell_model_key",
    "cell_variant_id",
    "predicted_corner_front_score",
    "predicted_corner_rank",
    "prediction_generated_at",
)


def combined_rows_sql(local_alias: str, neon_view: str) -> str:
    """Local history plus Neon production rows, newest prediction per natural key."""
    columns = ", ".join(COLUMNS)
    keys = ", ".join(KEY_COLUMNS)
    return f"""
    with combined as (
      select {columns}, 1 as source_rank from {local_alias}.public.{TABLE}
      union all
      select {columns}, 0 as source_rank from {neon_view}
    )
    select {columns}
    from combined
    qualify row_number() over (
      partition by {keys}
      order by prediction_generated_at desc nulls last, source_rank
    ) = 1
    """


def fetch_neon_rows(neon_url: str, from_year: str) -> pa.Table:
    """Neon rows from ``from_year`` on (the 1.7M-row history already matches local).

    Neon's pooler drops DuckDB's binary COPY, so the rows are fetched over psycopg.
    """
    with psycopg.connect(neon_url, connect_timeout=30) as conn, conn.cursor() as cur:
        cur.execute(
            f"select {', '.join(COLUMNS)} from {TABLE} where kaisai_nen >= %s",
            (from_year,),
        )
        rows = cur.fetchall()
    return pa.Table.from_pylist([dict(zip(COLUMNS, row, strict=True)) for row in rows])


def load_rows(local_url: str, neon_url: str) -> pa.Table:
    con = duckdb.connect()
    con.execute("install postgres; load postgres")
    con.execute(f"attach '{local_url}' as local_pg (type postgres, read_only)")
    max_year = con.execute(
        f"select max(kaisai_nen) from local_pg.public.{TABLE}"
    ).fetchone()
    if max_year is None or max_year[0] is None:
        raise SystemExit(f"local {TABLE} is empty; nothing to backfill history from")
    con.register("neon_rows", fetch_neon_rows(neon_url, str(max_year[0])))
    return con.execute(combined_rows_sql("local_pg", "neon_rows")).to_arrow_table()


def summarize(rows: pa.Table) -> list[tuple[object, ...]]:
    con = duckdb.connect()
    con.register("rows_view", rows)
    return con.execute(
        "select source, kaisai_nen, model_version, count(*) from rows_view group by all order by 1, 2, 3"
    ).fetchall()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="backfill_running_style_catalog")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    settings = load_settings()
    neon_url = os.environ.get("NEON_PRIMARY_URL")
    if not neon_url:
        raise SystemExit(
            "NEON_PRIMARY_URL is required (Neon is the 2026+ backup source)"
        )
    rows = load_rows(settings.pg_url, neon_url)
    for line in summarize(rows):
        print(line)
    print("total rows", rows.num_rows)
    if args.dry_run:
        return 0
    table = create_catalog(settings).load_table((settings.namespace, TABLE))
    table.overwrite(rows.select(list(COLUMNS)).cast(table.schema().as_arrow()))
    print("committed snapshot", table.current_snapshot().snapshot_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
