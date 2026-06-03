"""Build the per-run audit record + INSERT SQL.

The audit row records ``{run_date, status, races_predicted, duration, error}``
for each cron-triggered container run. Insert-only: there is never a DELETE /
TRUNCATE on this table (project ``feedback_no_data_delete`` rule). The cron
Worker owns the canonical audit in D1; this Python-side mirror is optional and
lets the container also persist its own result when run standalone.
"""

from __future__ import annotations

from typing import Final, Literal, NamedTuple

AUDIT_TABLE: Final[str] = "finish_position_cron_executions"

AuditStatus = Literal["success", "partial", "error"]

AUDIT_COLUMNS: Final[tuple[str, ...]] = (
    "run_date",
    "status",
    "races_predicted",
    "duration_ms",
    "error",
)


class AuditRecord(NamedTuple):
    """A single audit row describing one container prediction run."""

    run_date: str
    status: AuditStatus
    races_predicted: int
    duration_ms: int
    error: str | None


def build_audit_record(
    run_date: str,
    status: AuditStatus,
    races_predicted: int,
    duration_ms: int,
    error: str | None,
) -> AuditRecord:
    """Construct an ``AuditRecord``, rejecting negative counters."""
    if races_predicted < 0:
        message = f"races_predicted must be non-negative, got {races_predicted}"
        raise ValueError(message)
    if duration_ms < 0:
        message = f"duration_ms must be non-negative, got {duration_ms}"
        raise ValueError(message)
    return AuditRecord(
        run_date=run_date,
        status=status,
        races_predicted=races_predicted,
        duration_ms=duration_ms,
        error=error,
    )


def build_audit_table_ddl() -> str:
    """Return the ``CREATE TABLE IF NOT EXISTS`` for the audit table."""
    return (
        f"create table if not exists {AUDIT_TABLE} (\n"
        "      id bigint generated always as identity primary key,\n"
        "      run_date text not null,\n"
        "      status text not null,\n"
        "      races_predicted integer not null,\n"
        "      duration_ms integer not null,\n"
        "      error text,\n"
        "      recorded_at timestamptz not null default now()\n"
        "    )"
    )


def build_audit_insert_sql() -> str:
    """Return the parameterised single-row INSERT for the audit table.

    Uses psycopg3 ``%s`` placeholders (NOT libpq ``$n``): psycopg3 client-side
    binding only recognises ``%s`` / ``%(name)s`` and raises
    ``ProgrammingError: the query has 0 placeholders but N parameters were
    passed`` if you ship ``$n`` text and pass parameters.
    """
    columns = ", ".join(AUDIT_COLUMNS)
    placeholders = ", ".join("%s" for _ in range(len(AUDIT_COLUMNS)))
    return f"insert into {AUDIT_TABLE} ({columns}) values ({placeholders})"


def audit_params(record: AuditRecord) -> list[object]:
    """Flatten an ``AuditRecord`` into positional INSERT parameters."""
    return [
        record.run_date,
        record.status,
        record.races_predicted,
        record.duration_ms,
        record.error,
    ]
