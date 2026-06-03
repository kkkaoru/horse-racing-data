"""Collapse duplicate primary keys within a prediction batch.

The NAR walk-forward JSONL contains multiple runners with
``ketto_toroku_bango == "0000000000"`` in the same race, which collide on the
``race_finish_position_model_predictions`` primary key. Postgres rejects
``ON CONFLICT DO UPDATE`` when the same target row appears twice in one
statement, so we must dedupe (last-wins, matching UPSERT semantics) before
flushing. Mirrors the TypeScript ``dedupeBatch`` fix (commit 18ae921).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

PrimaryKeyFields = tuple[str, str]


def primary_key(record: Mapping[str, object]) -> PrimaryKeyFields:
    """Return the (race_id, ketto_toroku_bango) collision key for a record."""
    return (str(record["race_id"]), str(record["ketto_toroku_bango"]))


def dedupe_batch(
    batch: Sequence[Mapping[str, object]],
) -> list[Mapping[str, object]]:
    """Return ``batch`` with duplicate primary keys collapsed (last-wins).

    Insertion order of the first occurrence is preserved so output ordering is
    deterministic, while the value kept is the last seen (UPSERT semantics).
    """
    by_key: dict[PrimaryKeyFields, Mapping[str, object]] = {}
    for record in batch:
        by_key[primary_key(record)] = record
    return list(by_key.values())
