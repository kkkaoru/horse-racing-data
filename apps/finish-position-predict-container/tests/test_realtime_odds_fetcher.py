"""Tests for the pure helper functions in ``realtime_odds_fetcher.py``.

``realtime_odds_fetcher.py`` is excluded from the ``--cov=predict_lib`` gate
(it is a cov-excluded top-level module with live HTTP I/O). These tests verify
the pure helper logic — race-key construction, URL encoding, row extraction,
Protocol injection pattern — without any network calls.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import cast, final

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from realtime_odds_fetcher import (
    HOT_WORKER_BASE_URL,
    RealtimeOddsFetcher,
    build_race_key,
    encode_race_key,
    extract_rows,
    fetch_odds_for_race,
    fetch_realtime_odds_parquet,
)

# ruff wants uppercase-before-lowercase; reorder alphabetically
# HOT_WORKER_BASE_URL < RealtimeOddsFetcher < build_race_key < encode_race_key
# < extract_rows < fetch_odds_for_race < fetch_realtime_odds_parquet
# ← already correct; if ruff complains the issue is first-letter casing


# ---------------------------------------------------------------------------
# Race-key construction
# ---------------------------------------------------------------------------


def testbuild_race_key_nar_format() -> None:
    assert build_race_key("nar", "20260610", "44", "01") == "nar:2026:0610:44:01"


def testbuild_race_key_jra_format() -> None:
    assert build_race_key("jra", "20260607", "05", "11") == "jra:2026:0607:05:11"


def testbuild_race_key_ban_ei_uses_nar_source() -> None:
    # Ban-ei races are stored as source=nar in D1; pipeline maps ban-ei→nar.
    assert build_race_key("nar", "20260610", "83", "01") == "nar:2026:0610:83:01"


# ---------------------------------------------------------------------------
# URL encoding — colons must be percent-encoded
# ---------------------------------------------------------------------------


def testencode_race_key_encodes_colons() -> None:
    encoded = encode_race_key("nar:2026:0610:44:01")
    assert ":" not in encoded
    assert encoded == "nar%3A2026%3A0610%3A44%3A01"


def testencode_race_key_round_trips_via_urllib() -> None:
    import urllib.parse

    raw = "jra:2026:0607:05:11"
    encoded = encode_race_key(raw)
    assert urllib.parse.unquote(encoded) == raw


# ---------------------------------------------------------------------------
# extract_rows — JSON response parsing
# ---------------------------------------------------------------------------


def testextract_rows_returns_correct_tuples() -> None:
    response: dict[str, object] = {
        "fetchedAt": "2026-06-10T14:30:00+09:00",
        "latest": {
            "tansho": [
                {"combination": "1", "odds": 7.3, "rank": 1},
                {"combination": "2", "odds": 12.5, "rank": 2},
            ]
        },
    }
    rows = extract_rows("44", "01", response)
    assert rows == [
        ("44", "01", 1, 7.3, 1),
        ("44", "01", 2, 12.5, 2),
    ]


def testextract_rows_empty_when_latest_absent() -> None:
    response: dict[str, object] = {"fetchedAt": None, "latest": None}
    assert extract_rows("44", "01", response) == []


def testextract_rows_empty_when_tansho_absent() -> None:
    response: dict[str, object] = {"latest": {}}
    assert extract_rows("44", "01", response) == []


def testextract_rows_empty_when_tansho_empty_list() -> None:
    response: dict[str, object] = {"latest": {"tansho": []}}
    assert extract_rows("44", "01", response) == []


def testextract_rows_skips_entries_with_zero_or_negative_odds() -> None:
    response: dict[str, object] = {
        "latest": {
            "tansho": [
                {"combination": "1", "odds": 0.0, "rank": 1},
                {"combination": "2", "odds": -1.0, "rank": 2},
                {"combination": "3", "odds": 5.0, "rank": 3},
            ]
        }
    }
    rows = extract_rows("44", "01", response)
    assert len(rows) == 1
    assert rows[0] == ("44", "01", 3, 5.0, 3)


def testextract_rows_skips_entries_with_missing_fields() -> None:
    response: dict[str, object] = {
        "latest": {
            "tansho": [
                {"combination": "1", "rank": 1},  # missing odds
                {"combination": "2", "odds": 5.0},  # missing rank
                {"odds": 5.0, "rank": 2},  # missing combination
                {"combination": "4", "odds": 3.0, "rank": 4},  # valid
            ]
        }
    }
    rows = extract_rows("44", "01", response)
    assert len(rows) == 1
    assert rows[0][2] == 4  # umaban=4


def testextract_rows_skips_non_dict_entries() -> None:
    response: dict[str, object] = {
        "latest": {"tansho": [None, "bad", {"combination": "1", "odds": 7.3, "rank": 1}]}
    }
    rows = extract_rows("44", "01", response)
    assert len(rows) == 1


def testextract_rows_propagates_keibajo_and_race_bango() -> None:
    response: dict[str, object] = {
        "latest": {"tansho": [{"combination": "5", "odds": 4.2, "rank": 3}]}
    }
    rows = extract_rows("30", "07", response)
    assert rows == [("30", "07", 5, 4.2, 3)]


# ---------------------------------------------------------------------------
# fetch_odds_for_race — Protocol injection (no network)
# ---------------------------------------------------------------------------


@final
class _StubFetcher:
    """Stub that returns a canned response for one race key."""

    _responses: dict[str, dict[str, object]]

    def __init__(self, responses: dict[str, dict[str, object]]) -> None:
        self._responses = responses
        self.calls: list[str] = []

    def fetch(self, url: str, timeout: float) -> dict[str, object]:
        self.calls.append(url)
        if url in self._responses:
            return self._responses[url]
        raise OSError(f"no stub for {url}")


def testfetch_odds_for_race_calls_correct_url() -> None:
    encoded_key = "nar%3A2026%3A0610%3A44%3A01"
    expected_url = f"{HOT_WORKER_BASE_URL}/{encoded_key}"
    stub = _StubFetcher({expected_url: {"latest": {"tansho": []}}})
    fetch_odds_for_race(stub, "nar", "20260610", "44", "01")
    assert stub.calls == [expected_url]


def testfetch_odds_for_race_returns_rows_on_success() -> None:
    encoded_key = "nar%3A2026%3A0610%3A44%3A01"
    url = f"{HOT_WORKER_BASE_URL}/{encoded_key}"
    stub = _StubFetcher(
        {url: {"latest": {"tansho": [{"combination": "1", "odds": 7.3, "rank": 1}]}}}
    )
    rows = fetch_odds_for_race(stub, "nar", "20260610", "44", "01")
    assert rows == [("44", "01", 1, 7.3, 1)]


def testfetch_odds_for_race_returns_empty_on_http_error() -> None:
    class _ErrorFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise OSError("connection refused")

    rows = fetch_odds_for_race(_ErrorFetcher(), "nar", "20260610", "44", "01")
    assert rows == []


def testfetch_odds_for_race_returns_empty_on_timeout() -> None:
    class _TimeoutFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise TimeoutError("timed out")

    rows = fetch_odds_for_race(_TimeoutFetcher(), "nar", "20260610", "44", "01")
    assert rows == []


def testfetch_odds_for_race_returns_empty_on_json_error() -> None:
    import json

    class _BadJsonFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise json.JSONDecodeError("bad json", "", 0)

    rows = fetch_odds_for_race(_BadJsonFetcher(), "nar", "20260610", "44", "01")
    assert rows == []


# ---------------------------------------------------------------------------
# fetch_realtime_odds_parquet — integration of fetch + write (stubbed fetcher)
# ---------------------------------------------------------------------------


def test_fetch_realtime_odds_parquet_returns_none_when_race_keys_none(
    tmp_path: Path,
) -> None:
    result = fetch_realtime_odds_parquet("nar", "20260610", tmp_path, race_keys=None)
    assert result is None


def test_fetch_realtime_odds_parquet_returns_none_when_all_fetches_empty(
    tmp_path: Path,
) -> None:
    stub = _StubFetcher({})  # all requests raise OSError → empty rows

    result = fetch_realtime_odds_parquet(
        "nar",
        "20260610",
        tmp_path,
        race_keys=[("44", "01"), ("44", "02")],
        fetcher=stub,
    )
    assert result is None


def test_fetch_realtime_odds_parquet_writes_parquet_on_success(
    tmp_path: Path,
) -> None:
    import pandas as pd

    def _make_url(keibajo: str, race: str) -> str:
        key = f"nar:2026:0610:{keibajo}:{race}"
        import urllib.parse

        encoded = urllib.parse.quote(key, safe="")
        return f"{HOT_WORKER_BASE_URL}/{encoded}"

    responses: dict[str, dict[str, object]] = {
        _make_url("44", "01"): {
            "latest": {
                "tansho": [
                    {"combination": "1", "odds": 7.3, "rank": 1},
                    {"combination": "2", "odds": 12.5, "rank": 2},
                ]
            }
        },
        _make_url("44", "02"): {
            "latest": {"tansho": cast(list[object], [])},
        },  # no odds for race 02
    }
    stub = _StubFetcher(responses)

    result = fetch_realtime_odds_parquet(
        "nar",
        "20260610",
        tmp_path,
        race_keys=[("44", "01"), ("44", "02")],
        fetcher=stub,
    )

    assert result is not None
    assert result.exists()
    df = pd.read_parquet(result)
    assert len(df) == 2
    assert set(df.columns) == {
        "keibajo_code",
        "race_bango",
        "umaban",
        "tansho_odds_realtime",
        "ninkijun_realtime",
    }
    assert list(df.sort_values("umaban")["tansho_odds_realtime"]) == [7.3, 12.5]


def test_fetch_realtime_odds_parquet_uses_nar_source_for_ban_ei(
    tmp_path: Path,
) -> None:
    """Ban-ei odds are in D1 under source=nar; race keys use nar: prefix."""
    calls: list[str] = []

    class _CaptureFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            calls.append(url)
            return {"latest": {"tansho": []}}

    fetch_realtime_odds_parquet(
        "ban-ei",
        "20260610",
        tmp_path,
        race_keys=[("83", "01")],
        fetcher=_CaptureFetcher(),
    )
    assert len(calls) == 1
    # The URL must use "nar" prefix for Ban-ei.
    assert "nar" in calls[0]
    assert "ban" not in calls[0]


def test_realtime_odds_fetcher_protocol_is_satisfied_by_stub() -> None:
    """Confirm _StubFetcher satisfies the Protocol (runtime check)."""
    stub = _StubFetcher({})
    assert isinstance(stub, RealtimeOddsFetcher)
