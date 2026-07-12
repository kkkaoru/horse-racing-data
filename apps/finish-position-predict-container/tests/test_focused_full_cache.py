"""Tests for the bounded in-process focused-full cache store
(``predict_lib.focused_full_cache``)."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.focused_full_cache import FocusedFullCachePayload, FocusedFullCacheStore

_PAYLOAD_A = FocusedFullCachePayload(
    parquet_base64="YQ==",
    parquet_key="feat-cache/jra/20260712/05/09/features.parquet",
    per_race_parquets=[{"parquetBase64": "cGE=", "parquetKey": "per-race-a"}],
)
_PAYLOAD_B = FocusedFullCachePayload(
    parquet_base64="Yg==",
    parquet_key="feat-cache/nar/20260712/44/02/features.parquet",
    per_race_parquets=None,
)


def _make_clock(start: float = 0.0) -> tuple[list[float], Callable[[], float]]:
    """Return ``(box, time_fn)`` -- box[0] is the injectable clock's current value."""
    box = [start]

    def _time_fn() -> float:
        return box[0]

    return box, _time_fn


def test_put_then_pop_returns_payload() -> None:
    store = FocusedFullCacheStore()
    store.put("jra:20260712:05:09", _PAYLOAD_A)
    assert store.pop("jra:20260712:05:09") == _PAYLOAD_A


def test_pop_missing_key_returns_none() -> None:
    store = FocusedFullCacheStore()
    assert store.pop("no-such-key") is None


def test_pop_consumes_entry() -> None:
    store = FocusedFullCacheStore()
    store.put("jra:20260712:05:09", _PAYLOAD_A)
    assert store.pop("jra:20260712:05:09") == _PAYLOAD_A
    assert store.pop("jra:20260712:05:09") is None


def test_two_races_do_not_cross_contaminate() -> None:
    """Regression test for the cache-poisoning shape flagged during design --
    two different race keys must never share or overwrite each other's
    payload."""
    store = FocusedFullCacheStore()
    store.put("jra:20260712:05:09", _PAYLOAD_A)
    store.put("nar:20260712:44:02", _PAYLOAD_B)
    assert store.pop("jra:20260712:05:09") == _PAYLOAD_A
    assert store.pop("nar:20260712:44:02") == _PAYLOAD_B


def test_put_overwrites_existing_key() -> None:
    store = FocusedFullCacheStore()
    store.put("jra:20260712:05:09", _PAYLOAD_A)
    store.put("jra:20260712:05:09", _PAYLOAD_B)
    assert store.pop("jra:20260712:05:09") == _PAYLOAD_B


def test_ttl_expiry_on_pop() -> None:
    box, time_fn = _make_clock(start=0.0)
    store = FocusedFullCacheStore(ttl_seconds=100.0, time_fn=time_fn)
    store.put("jra:20260712:05:09", _PAYLOAD_A)
    box[0] = 200.0  # past the 100s TTL
    assert store.pop("jra:20260712:05:09") is None


def test_entry_survives_within_ttl() -> None:
    box, time_fn = _make_clock(start=0.0)
    store = FocusedFullCacheStore(ttl_seconds=100.0, time_fn=time_fn)
    store.put("jra:20260712:05:09", _PAYLOAD_A)
    box[0] = 50.0  # still within the 100s TTL
    assert store.pop("jra:20260712:05:09") == _PAYLOAD_A


def test_ttl_expiry_evicted_on_subsequent_put() -> None:
    """An expired entry is purged the next time ``put`` runs its own eviction
    pass, not only when explicitly popped."""
    box, time_fn = _make_clock(start=0.0)
    store = FocusedFullCacheStore(ttl_seconds=100.0, time_fn=time_fn)
    store.put("jra:20260712:05:09", _PAYLOAD_A)
    box[0] = 200.0
    store.put("nar:20260712:44:02", _PAYLOAD_B)
    assert store.pop("jra:20260712:05:09") is None
    assert store.pop("nar:20260712:44:02") == _PAYLOAD_B


def test_capacity_eviction_drops_oldest_entry() -> None:
    box, time_fn = _make_clock(start=0.0)
    store = FocusedFullCacheStore(max_entries=2, time_fn=time_fn)
    store.put("race:1", _PAYLOAD_A)
    box[0] = 1.0
    store.put("race:2", _PAYLOAD_A)
    box[0] = 2.0
    store.put("race:3", _PAYLOAD_A)

    assert store.pop("race:1") is None, "oldest entry must be evicted over capacity"
    assert store.pop("race:2") == _PAYLOAD_A
    assert store.pop("race:3") == _PAYLOAD_A
