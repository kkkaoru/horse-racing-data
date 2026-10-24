#!/usr/bin/env python3
"""Rebuild the local PostgreSQL ``race_entry_corner_features`` table from raw SE/RA.

Production (R2 Catalog) never stores this table: ``_catalog_attach`` derives it as
a DuckDB view over raw ``jvd_se``/``jvd_ra``/``nvd_se``/``nvd_ra``. The local
table was instead built by ``build-corner-feature-table.ts`` with its own rules
(no counted field-size fallback, so ~30k rows keep a NULL ``shusso_tosu`` and
NULL norms; NUMERIC instead of DOUBLE columns), so local training read inputs
that serving never sees.

This script materialises the production view's SELECT verbatim and adds one
correction: ``jvd_se`` also carries rows for JRA-registered horses at NAR
venues (keibajo 30-65/83, ~19.6k rows in 2025). Each one duplicates the same
runner's ``nvd_se`` row but is labelled ``source = 'jra'``; they are dropped.
The local-only ``feature_vector`` (corner prediction kNN) and ``updated_at``
columns and every existing index on the target table are rebuilt.

Run (one-month sample into a scratch table; the full rebuild omits the dates
and the target):
  uv run python src/scripts/rebuild_race_entry_corner_features_local.py \\
    --from-date 20250601 --to-date 20250630 \\
    --target-table race_entry_corner_features_sample
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from time import perf_counter
from typing import Final, Protocol

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent / "finish-position-features"))

DEFAULT_PG_URL: Final[str] = (
    "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
)
DEFAULT_TARGET_TABLE: Final[str] = "race_entry_corner_features"
VIEW_PREFIX: Final[str] = "\ncreate view pg.race_entry_corner_features as\n"
# NAR venue codes are 30-65 and 83; overseas codes are alphanumeric and sort above.
NAR_VENUE_MIN: Final[str] = "30"
NAR_VENUE_MAX: Final[str] = "99"
# umaban is part of the key: production keeps overseas races whose foreign
# runners all share ketto_toroku_bango '0000000000' (5,376 rows locally), which
# build-corner-feature-table.ts dropped via its numeric-venue filter. Its
# 6-column ON CONFLICT upsert therefore no longer applies to the rebuilt table.
PRIMARY_KEY: Final[str] = (
    "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, "
    "ketto_toroku_bango, umaban"
)
IDENTIFIER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_]*$")
DATE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d{8}$")
# Same 8-dim corner-similarity vector build-corner-feature-table.ts stores.
FEATURE_VECTOR_SQL: Final[str] = """array[
      least(1, greatest(0, coalesce(kyori, 0)::numeric / 3600)),
      least(1, greatest(0, coalesce(shusso_tosu, 0)::numeric / 18)),
      least(1, greatest(0, coalesce(umaban, 0)::numeric / greatest(coalesce(shusso_tosu, 1), 1))),
      least(1, greatest(0, coalesce(tansho_ninkijun, shusso_tosu, 0)::numeric / greatest(coalesce(shusso_tosu, 1), 1))),
      least(1, greatest(0, ln(greatest(coalesce(tansho_odds, 1), 1)::numeric) / ln(300))),
      case when left(coalesce(track_code, ''), 1) = '1' then 0 else 1 end,
      least(1, greatest(0, coalesce(case when keibajo_code ~ '^[0-9]+$' then nullif(keibajo_code, '')::numeric else null end, 0) / 99)),
      least(1, greatest(0, coalesce(case when race_bango ~ '^[0-9]+$' then nullif(race_bango, '')::numeric else null end, 0) / 12))
    ]::vector(8)"""


class Connection(Protocol):
    def execute(self, query: str) -> Connection: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="rebuild_race_entry_corner_features_local")
    parser.add_argument("--pg-url", default=DEFAULT_PG_URL)
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    args = parser.parse_args(argv)
    if not IDENTIFIER_PATTERN.match(args.target_table):
        parser.error("--target-table must be a lower-case SQL identifier")
    for name in ("from_date", "to_date"):
        value = getattr(args, name)
        if value is not None and not DATE_PATTERN.match(value):
            parser.error(f"--{name.replace('_', '-')} must be YYYYMMDD")
    return args


def production_select_sql() -> str:
    """The production view body, i.e. the exact SELECT serving reads."""
    from _catalog_attach import _RAW_RACE_ENTRY_VIEW_SQL

    if not _RAW_RACE_ENTRY_VIEW_SQL.startswith(VIEW_PREFIX):
        raise RuntimeError("production race_entry_corner_features view SQL changed shape")
    return _RAW_RACE_ENTRY_VIEW_SQL.removeprefix(VIEW_PREFIX)


def rebuild_select_sql(from_date: str | None, to_date: str | None) -> str:
    """Production rows minus the NAR-venue rows mislabelled ``jra``.

    The filters apply outside the view so its per-race field-size count is
    computed over the same rows as in production (a race is never split).
    """
    predicates = [
        f"not (rec.source = 'jra' and rec.keibajo_code between '{NAR_VENUE_MIN}' "
        f"and '{NAR_VENUE_MAX}')"
    ]
    if from_date is not None:
        predicates.append(f"rec.race_date >= '{from_date}'")
    if to_date is not None:
        predicates.append(f"rec.race_date <= '{to_date}'")
    return (
        f"select * from ({production_select_sql()}) rec\nwhere "
        + "\n  and ".join(predicates)
    )


def postgres_execute_sql(statement: str) -> str:
    """Wrap PostgreSQL-native SQL for DuckDB's ``postgres_execute``."""
    return "call postgres_execute('pg', '" + statement.replace("'", "''") + "')"


def existing_index_definitions(con: Connection, target_table: str) -> list[str]:
    """Secondary index DDL of the current target (empty when it does not exist)."""
    query = (
        "select indexdef from pg_indexes where schemaname = 'public' "
        f"and tablename = '{target_table}' and indexname <> '{target_table}_pkey' "
        "order by indexname"
    )
    rows = con.execute(
        "select indexdef from postgres_query('pg', '" + query.replace("'", "''") + "')"
    ).fetchall()
    return [str(row[0]) for row in rows]


def swap_sql(target_table: str, index_definitions: Sequence[str]) -> str:
    """PostgreSQL statements replacing the target with the staged rows.

    Sent as one simple-query string, which PostgreSQL runs as a single
    transaction: any failure (e.g. a dependent view) leaves the target intact.
    """
    staging = f"{target_table}__staging"
    rebuilt = f"{target_table}__rebuilt"
    statements = [
        f"drop table if exists {rebuilt}",
        f"create table {rebuilt} as select s.*, {FEATURE_VECTOR_SQL} as feature_vector, "
        f"now() as updated_at from {staging} s",
        f"drop table if exists {target_table}",
        f"alter table {rebuilt} rename to {target_table}",
        f"alter table {target_table} add primary key ({PRIMARY_KEY})",
        *index_definitions,
        f"drop table {staging}",
    ]
    return ";\n".join(statements)


def rebuild(
    con: Connection,
    *,
    target_table: str,
    from_date: str | None,
    to_date: str | None,
) -> int:
    """Stage the production rows, swap them in and return the new row count."""
    staging = f"{target_table}__staging"
    con.execute(f"drop table if exists pg.public.{staging}")
    con.execute(
        f"create table pg.public.{staging} as {rebuild_select_sql(from_date, to_date)}"
    )
    index_definitions = existing_index_definitions(con, target_table)
    con.execute(postgres_execute_sql(swap_sql(target_table, index_definitions)))
    rows = con.execute(
        "select count from postgres_query('pg', "
        f"'select count(*) as count from {target_table}')"
    ).fetchall()
    return int(str(rows[0][0]))


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    started = perf_counter()
    con = duckdb.connect()
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{args.pg_url}' as pg (type postgres)")
    count = rebuild(
        con,
        target_table=args.target_table,
        from_date=args.from_date,
        to_date=args.to_date,
    )
    con.close()
    print(
        f"{args.target_table}={count} rows in {perf_counter() - started:.1f}s",
        flush=True,
    )


if __name__ == "__main__":
    main()
