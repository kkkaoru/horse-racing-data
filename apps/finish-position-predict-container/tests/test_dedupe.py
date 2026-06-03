"""Tests for batch primary-key dedupe (NAR zero-ketto collision)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.dedupe import dedupe_batch, primary_key


def test_primary_key_tuple() -> None:
    record = {"race_id": "nar:2026:0523:54:11", "ketto_toroku_bango": "0000000000", "umaban": 3}
    assert primary_key(record) == ("nar:2026:0523:54:11", "0000000000")


def test_dedupe_keeps_unique_rows() -> None:
    batch = [
        {"race_id": "jra:2024:0101:45:08", "ketto_toroku_bango": "111", "predicted_rank": 1},
        {"race_id": "jra:2024:0101:45:08", "ketto_toroku_bango": "222", "predicted_rank": 2},
    ]
    assert len(dedupe_batch(batch)) == 2


def test_dedupe_collapses_zero_ketto_collision_last_wins() -> None:
    batch = [
        {"race_id": "nar:2026:0523:54:11", "ketto_toroku_bango": "0000000000", "predicted_rank": 5},
        {"race_id": "nar:2026:0523:54:11", "ketto_toroku_bango": "0000000000", "predicted_rank": 9},
    ]
    deduped = dedupe_batch(batch)
    assert len(deduped) == 1
    assert deduped[0]["predicted_rank"] == 9


def test_dedupe_empty_batch() -> None:
    assert dedupe_batch([]) == []
