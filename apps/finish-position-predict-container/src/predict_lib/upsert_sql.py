"""Build chunked, idempotent UPSERT SQL for the predictions table.

Mirrors ``apps/pc-keiba-viewer/src/scripts/finish-position-features/import-predictions-sql.ts``:
same table, primary key, insert columns and ``ON CONFLICT DO UPDATE`` set. We
write parameterised statements (psycopg3 ``%s`` placeholders) so values are
bound by the driver and never string-interpolated. Chunking keeps each
statement well under the 16-minute single-query wall and bounds the parameter
count per round trip.

NOTE on placeholders: psycopg3 client-side binding recognises ``%s`` (and
``%(name)s``) only. ``$n`` is libpq-native (asyncpg / postgres directly) and
psycopg3 raises ``ProgrammingError: the query has 0 placeholders but N
parameters were passed`` if you ship ``$n``-bearing SQL with positional params.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

PREDICTIONS_TABLE: Final[str] = "race_finish_position_model_predictions"

PRIMARY_KEY_COLUMNS: Final[tuple[str, ...]] = (
    "model_version",
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
)

INSERT_COLUMNS: Final[tuple[str, ...]] = (
    *PRIMARY_KEY_COLUMNS,
    "umaban",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
)

UPDATABLE_COLUMNS: Final[tuple[str, ...]] = (
    "umaban",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
)

DEFAULT_CHUNK_SIZE: Final[int] = 500


def _placeholder_row(_row_index: int) -> str:
    """Build the ``(%s, %s, ...)`` psycopg3 placeholder tuple for one row.

    ``row_index`` is retained in the signature for chunking-loop callers, but
    psycopg3 uses positional ``%s`` (NOT numbered ``$n``) so the offset is
    unused — every row reuses the same per-column placeholder string.
    """
    placeholders = ("%s" for _ in range(len(INSERT_COLUMNS)))
    return "(" + ", ".join(placeholders) + ")"


def build_upsert_sql(row_count: int) -> str:
    """Return a parameterised multi-row UPSERT statement for ``row_count`` rows.

    Raises ``ValueError`` for a non-positive ``row_count`` so callers never emit
    an empty ``VALUES`` clause.
    """
    if row_count <= 0:
        message = f"row_count must be positive, got {row_count}"
        raise ValueError(message)
    values_clause = ",\n      ".join(_placeholder_row(index) for index in range(row_count))
    update_assignments = ",\n      ".join(
        f"{column} = excluded.{column}" for column in UPDATABLE_COLUMNS
    )
    insert_columns = ", ".join(INSERT_COLUMNS)
    conflict_columns = ", ".join(PRIMARY_KEY_COLUMNS)
    return (
        f"insert into {PREDICTIONS_TABLE} ({insert_columns})\n"
        f"    values\n      {values_clause}\n"
        f"    on conflict ({conflict_columns})\n"
        f"    do update set\n      {update_assignments},\n"
        "      prediction_generated_at = now()"
    )


def chunk_rows(
    rows: Sequence[Sequence[object]],
    chunk_size: int,
) -> list[list[Sequence[object]]]:
    """Split ``rows`` into chunks of at most ``chunk_size``.

    Raises ``ValueError`` for a non-positive ``chunk_size``.
    """
    if chunk_size <= 0:
        message = f"chunk_size must be positive, got {chunk_size}"
        raise ValueError(message)
    return [list(rows[start : start + chunk_size]) for start in range(0, len(rows), chunk_size)]


def flatten_params(rows: Sequence[Sequence[object]]) -> list[object]:
    """Flatten per-row value tuples into a single positional-parameter list."""
    return [value for row in rows for value in row]
