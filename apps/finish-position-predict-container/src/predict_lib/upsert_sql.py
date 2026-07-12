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

# Race-level subgroup metadata columns persisted alongside each prediction. This
# is the subset of ``subgroup.SUBGROUP_DIMENSIONS`` that the predictions table
# stores; ``venue`` is intentionally omitted because the race's keibajo is
# already a primary-key column (``keibajo_code``), so a separate ``venue`` column
# would be redundant. The order here MUST match the viewer TS mirror
# ``import-predictions-sql.ts`` INSERT_COLUMNS and the values appended by
# ``upcoming.build_prediction_rows``. These are the same value for every horse in
# a race and are NOT part of the primary key — INSERT + UPDATABLE only.
PREDICTION_SUBGROUP_COLUMNS: Final[tuple[str, ...]] = (
    "distance_band",
    "field_size_band",
    "season_band",
    "class_code",
    "surface",
)

# MASTER-INVENTORY finding #12: the served prediction row never persisted the
# feature values it was scored from, blocking any later serve-time-value
# audit. These four are read from the horse's own scored entry (the same
# feature row already computed for the model, not a new computation) --
# see ``upcoming.build_prediction_rows``'s ``entries`` parameter. Nullable,
# additive, and normal-update semantics (unlike ``first_served_at``): a
# re-score legitimately reflects a new set of odds/weight inputs, so these
# SHOULD be overwritten on every ON CONFLICT DO UPDATE, same as the other
# prediction-output columns below.
PREDICTION_AUDIT_COLUMNS: Final[tuple[str, ...]] = (
    "odds_score",
    "tansho_odds",
    "futan_juryo",
    "weight_diff_from_avg",
)

INSERT_COLUMNS: Final[tuple[str, ...]] = (
    *PRIMARY_KEY_COLUMNS,
    "umaban",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
    *PREDICTION_AUDIT_COLUMNS,
    *PREDICTION_SUBGROUP_COLUMNS,
)

UPDATABLE_COLUMNS: Final[tuple[str, ...]] = (
    "umaban",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
    *PREDICTION_AUDIT_COLUMNS,
    *PREDICTION_SUBGROUP_COLUMNS,
)

# MASTER-INVENTORY finding #13: two columns track prediction timing, and they
# MUST stay asymmetric. ``prediction_generated_at`` is forced to ``now()`` in
# every ``ON CONFLICT DO UPDATE`` below (see ``build_upsert_sql``) -- correct,
# it tracks "most recently (re)generated". ``first_served_at`` is deliberately
# absent from BOTH ``INSERT_COLUMNS`` and ``UPDATABLE_COLUMNS`` above: it is
# populated once by the column's own ``DEFAULT now()`` (see
# ``apps/pc-keiba-viewer/.../import-predictions-sql.ts``'s
# ``buildSetFirstServedAtDefaultSql``) on a row's original INSERT, and a
# same-key re-score/backfill's ON CONFLICT DO UPDATE never touches it again --
# so it stays a true "first ever served" timestamp. The column was added to
# the live table via a two-step no-rewrite ALTER (``buildAddFirstServedAtColumnSql``
# then ``buildSetFirstServedAtDefaultSql``) with no backfill, so rows written
# before that migration have ``first_served_at IS NULL`` -- an honest "unknown"
# rather than a fabricated timestamp; callers reading this column must handle
# that NULL case. Do NOT add ``first_served_at`` to either tuple above; doing
# so would silently reintroduce the exact bug this finding fixed.

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
