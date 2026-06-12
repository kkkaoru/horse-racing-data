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
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from realtime_odds_fetcher import (
    HOT_WORKER_BASE_URL,
    WEIGHT_WORKER_BASE_URL,
    RealtimeOddsFetcher,
    build_race_key,
    encode_race_key,
    extract_rows,
    extract_sanrenpuku_p3,
    extract_weight_map,
    fetch_odds_and_sanrenpuku_for_race,
    fetch_odds_for_race,
    fetch_realtime_odds_parquet,
    fetch_weight_for_race,
    fetch_with_retry,
    merge_weight_into_rows,
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
        "bataiju_realtime",
        "exotic_sanrenpuku_p3_realtime",
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


# ---------------------------------------------------------------------------
# extract_weight_map — JSON response parsing for horse weight
# ---------------------------------------------------------------------------


def testextract_weight_map_returns_correct_map() -> None:
    response: dict[str, object] = {
        "fetchedAt": "2026-06-10T14:00:00+09:00",
        "horses": [
            {"horseNumber": 1, "horseName": "TestHorse", "weight": 447,
             "changeSign": "+", "changeAmount": 2},
            {"horseNumber": 2, "horseName": "OtherHorse", "weight": 500,
             "changeSign": "0", "changeAmount": 0},
        ],
    }
    result = extract_weight_map(response)
    assert result == {1: 447, 2: 500}


def testextract_weight_map_returns_empty_when_horses_absent() -> None:
    response: dict[str, object] = {"fetchedAt": "2026-06-10T14:00:00+09:00"}
    assert extract_weight_map(response) == {}


def testextract_weight_map_returns_empty_when_horses_not_list() -> None:
    response: dict[str, object] = {"horses": "bad"}
    assert extract_weight_map(response) == {}


def testextract_weight_map_skips_entries_with_missing_fields() -> None:
    response: dict[str, object] = {
        "horses": [
            {"horseName": "NoNumber", "weight": 400},  # missing horseNumber
            {"horseNumber": 2, "horseName": "NoWeight"},  # missing weight
            {"horseNumber": 3, "horseName": "Valid", "weight": 450},
        ]
    }
    result = extract_weight_map(response)
    assert result == {3: 450}


def testextract_weight_map_skips_non_dict_entries() -> None:
    response: dict[str, object] = {
        "horses": [None, "bad", {"horseNumber": 1, "weight": 450}]
    }
    result = extract_weight_map(response)
    assert result == {1: 450}


def testextract_weight_map_skips_zero_and_negative_weight() -> None:
    response: dict[str, object] = {
        "horses": [
            {"horseNumber": 1, "weight": 0},
            {"horseNumber": 2, "weight": -1},
            {"horseNumber": 3, "weight": 450},
        ]
    }
    result = extract_weight_map(response)
    assert result == {3: 450}


def testextract_weight_map_handles_string_number_fields() -> None:
    response: dict[str, object] = {
        "horses": [
            {"horseNumber": "1", "weight": "447"},
        ]
    }
    result = extract_weight_map(response)
    assert result == {1: 447}


def testextract_weight_map_skips_unconvertible_fields() -> None:
    response: dict[str, object] = {
        "horses": [
            {"horseNumber": "bad", "weight": 450},
            {"horseNumber": 2, "weight": "bad"},
            {"horseNumber": 3, "weight": 460},
        ]
    }
    result = extract_weight_map(response)
    assert result == {3: 460}


# ---------------------------------------------------------------------------
# fetch_weight_for_race — Protocol injection (no network)
# ---------------------------------------------------------------------------


def testfetch_weight_for_race_calls_correct_url() -> None:
    encoded_key = "nar%3A2026%3A0610%3A44%3A01"
    expected_url = f"{WEIGHT_WORKER_BASE_URL}/{encoded_key}"
    stub = _StubFetcher(
        {expected_url: {"horses": [{"horseNumber": 1, "weight": 447}]}}
    )
    result = fetch_weight_for_race(stub, "nar", "20260610", "44", "01")
    assert stub.calls == [expected_url]
    assert result == {1: 447}


def testfetch_weight_for_race_returns_empty_on_error() -> None:
    class _ErrorFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise OSError("connection refused")

    result = fetch_weight_for_race(_ErrorFetcher(), "nar", "20260610", "44", "01")
    assert result == {}


def testfetch_weight_for_race_returns_empty_when_no_horses() -> None:
    encoded_key = "nar%3A2026%3A0610%3A44%3A02"
    url = f"{WEIGHT_WORKER_BASE_URL}/{encoded_key}"
    stub = _StubFetcher({url: {}})  # response has no "horses" key
    result = fetch_weight_for_race(stub, "nar", "20260610", "44", "02")
    assert result == {}


# ---------------------------------------------------------------------------
# merge_weight_into_rows
# ---------------------------------------------------------------------------


def testmerge_weight_into_rows_merges_by_umaban() -> None:
    odds_rows = [
        ("44", "01", 1, 7.3, 1),
        ("44", "01", 2, 12.5, 2),
    ]
    weight_map = {1: 447, 2: 500}
    result = merge_weight_into_rows(odds_rows, weight_map)
    assert result == [
        ("44", "01", 1, 7.3, 1, 447),
        ("44", "01", 2, 12.5, 2, 500),
    ]


def testmerge_weight_into_rows_uses_none_for_missing_umaban() -> None:
    odds_rows = [
        ("44", "01", 1, 7.3, 1),
        ("44", "01", 2, 12.5, 2),
    ]
    weight_map = {1: 447}  # umaban=2 not in weight map
    result = merge_weight_into_rows(odds_rows, weight_map)
    assert result == [
        ("44", "01", 1, 7.3, 1, 447),
        ("44", "01", 2, 12.5, 2, None),
    ]


def testmerge_weight_into_rows_empty_weight_map_gives_all_none() -> None:
    odds_rows = [("44", "01", 1, 7.3, 1)]
    result = merge_weight_into_rows(odds_rows, {})
    assert result == [("44", "01", 1, 7.3, 1, None)]


def testmerge_weight_into_rows_empty_odds_rows_returns_empty() -> None:
    result = merge_weight_into_rows([], {1: 447})
    assert result == []


# ---------------------------------------------------------------------------
# fetch_realtime_odds_parquet — bataiju_realtime in written parquet
# ---------------------------------------------------------------------------


def test_fetch_realtime_odds_parquet_includes_bataiju_realtime_when_weight_available(
    tmp_path: Path,
) -> None:
    """When weight fetch succeeds bataiju_realtime is populated in the parquet."""
    import pandas as pd

    def _make_odds_url(keibajo: str, race: str) -> str:
        import urllib.parse
        key = f"nar:2026:0610:{keibajo}:{race}"
        encoded = urllib.parse.quote(key, safe="")
        return f"{HOT_WORKER_BASE_URL}/{encoded}"

    def _make_weight_url(keibajo: str, race: str) -> str:
        import urllib.parse
        key = f"nar:2026:0610:{keibajo}:{race}"
        encoded = urllib.parse.quote(key, safe="")
        return f"{WEIGHT_WORKER_BASE_URL}/{encoded}"

    responses: dict[str, dict[str, object]] = {
        _make_odds_url("44", "01"): {
            "latest": {
                "tansho": [
                    {"combination": "1", "odds": 7.3, "rank": 1},
                ]
            }
        },
        _make_weight_url("44", "01"): {
            "horses": [{"horseNumber": 1, "weight": 447}]
        },
    }
    stub = _StubFetcher(responses)

    result = fetch_realtime_odds_parquet(
        "nar",
        "20260610",
        tmp_path,
        race_keys=[("44", "01")],
        fetcher=stub,
    )

    assert result is not None
    df = pd.read_parquet(result)
    assert len(df) == 1
    assert df.iloc[0]["bataiju_realtime"] == 447


def test_fetch_realtime_odds_parquet_bataiju_is_none_when_weight_fetch_fails(
    tmp_path: Path,
) -> None:
    """When weight fetch fails bataiju_realtime is None (not a crash)."""
    import pandas as pd

    def _make_odds_url(keibajo: str, race: str) -> str:
        import urllib.parse
        key = f"nar:2026:0610:{keibajo}:{race}"
        encoded = urllib.parse.quote(key, safe="")
        return f"{HOT_WORKER_BASE_URL}/{encoded}"

    # Only odds URL is in the stub; weight URL raises OSError.
    responses: dict[str, dict[str, object]] = {
        _make_odds_url("44", "03"): {
            "latest": {
                "tansho": [
                    {"combination": "1", "odds": 5.0, "rank": 1},
                ]
            }
        },
    }
    stub = _StubFetcher(responses)

    result = fetch_realtime_odds_parquet(
        "nar",
        "20260610",
        tmp_path,
        race_keys=[("44", "03")],
        fetcher=stub,
    )

    assert result is not None
    df = pd.read_parquet(result)
    assert len(df) == 1
    assert df.iloc[0]["bataiju_realtime"] is None or pd.isna(df.iloc[0]["bataiju_realtime"])


# ---------------------------------------------------------------------------
# fetch_with_retry — exponential backoff retry helper
# ---------------------------------------------------------------------------


def testfetch_with_retry_succeeds_on_first_attempt_no_sleep() -> None:
    """When the first fetch succeeds, no sleep is called."""
    expected: dict[str, object] = {"latest": {"tansho": []}}

    @final
    class _OnceFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            return expected

    with patch("realtime_odds_fetcher.time.sleep") as mock_sleep:
        result = fetch_with_retry(_OnceFetcher(), "http://example.com", 5.0)
    assert result is expected
    mock_sleep.assert_not_called()


def testfetch_with_retry_retries_on_first_failure_and_succeeds() -> None:
    """First attempt raises TimeoutError; second attempt succeeds."""
    expected: dict[str, object] = {
        "latest": {"tansho": [{"combination": "1", "odds": 7.3, "rank": 1}]}
    }
    calls: list[int] = []

    @final
    class _TransientFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            calls.append(1)
            if len(calls) == 1:
                raise TimeoutError("timed out")
            return expected

    with patch("realtime_odds_fetcher.time.sleep") as mock_sleep:
        result = fetch_with_retry(
            _TransientFetcher(), "http://example.com", 5.0, max_retries=2, backoff_base=0.5
        )
    assert result is expected
    assert len(calls) == 2
    mock_sleep.assert_called_once_with(0.5)


def testfetch_with_retry_retries_twice_then_succeeds() -> None:
    """First two attempts raise OSError; third attempt succeeds."""
    expected: dict[str, object] = {"ok": True}
    calls: list[int] = []

    @final
    class _TwoFailFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            calls.append(1)
            if len(calls) <= 2:
                raise OSError("connection refused")
            return expected

    with patch("realtime_odds_fetcher.time.sleep") as mock_sleep:
        result = fetch_with_retry(
            _TwoFailFetcher(), "http://example.com", 5.0, max_retries=2, backoff_base=0.5
        )
    assert result is expected
    assert len(calls) == 3
    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(0.5)
    mock_sleep.assert_any_call(1.0)


def testfetch_with_retry_raises_after_all_retries_exhausted() -> None:
    """All attempts fail: the final exception is propagated to the caller."""

    @final
    class _AlwaysFailFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise TimeoutError("always times out")

    with patch("realtime_odds_fetcher.time.sleep"):
        try:
            fetch_with_retry(
                _AlwaysFailFetcher(), "http://example.com", 5.0, max_retries=2, backoff_base=0.5
            )
            raised = False
        except TimeoutError:
            raised = True
    assert raised


def testfetch_with_retry_zero_retries_raises_immediately() -> None:
    """With max_retries=0, the single attempt failure propagates immediately."""

    @final
    class _FailFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise OSError("fail")

    with patch("realtime_odds_fetcher.time.sleep") as mock_sleep:
        try:
            fetch_with_retry(
                _FailFetcher(), "http://example.com", 5.0, max_retries=0, backoff_base=0.5
            )
            raised = False
        except OSError:
            raised = True
    assert raised
    mock_sleep.assert_not_called()


def test_fetch_odds_for_race_retries_on_transient_failure() -> None:
    """fetch_odds_for_race retries on transient failure then returns rows."""
    calls: list[int] = []

    @final
    class _TransientOddsFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            calls.append(1)
            if len(calls) == 1:
                raise TimeoutError("first attempt timeout")
            return {"latest": {"tansho": [{"combination": "1", "odds": 7.3, "rank": 1}]}}

    with patch("realtime_odds_fetcher.time.sleep"):
        rows = fetch_odds_for_race(_TransientOddsFetcher(), "nar", "20260610", "44", "01")
    assert rows == [("44", "01", 1, 7.3, 1)]
    assert len(calls) == 2


def test_fetch_weight_for_race_retries_on_transient_failure() -> None:
    """fetch_weight_for_race retries on transient failure then returns weight map."""
    calls: list[int] = []

    @final
    class _TransientWeightFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            calls.append(1)
            if len(calls) == 1:
                raise TimeoutError("first attempt timeout")
            return {"horses": [{"horseNumber": 1, "weight": 447}]}

    with patch("realtime_odds_fetcher.time.sleep"):
        result = fetch_weight_for_race(_TransientWeightFetcher(), "nar", "20260610", "44", "01")
    assert result == {1: 447}
    assert len(calls) == 2


def test_fetch_odds_for_race_returns_empty_after_all_retries_fail() -> None:
    """fetch_odds_for_race returns empty list when all retry attempts fail."""
    mock_sleep = MagicMock()

    @final
    class _AlwaysFailOddsFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise TimeoutError("always fails")

    with patch("realtime_odds_fetcher.time.sleep", mock_sleep):
        rows = fetch_odds_for_race(_AlwaysFailOddsFetcher(), "nar", "20260610", "44", "01")
    assert rows == []
    assert mock_sleep.call_count == 2  # 2 retries means 2 sleeps before final raise


# ---------------------------------------------------------------------------
# extract_sanrenpuku_p3 — JSON response parsing
# ---------------------------------------------------------------------------


def testextract_sanrenpuku_p3_returns_normalized_map() -> None:
    """Basic case: valid sanrenpuku list returns normalized per-horse map."""
    response: dict[str, object] = {
        "latest": {
            "tansho": [],
            "3renpuku": [
                {"combination": "1-2-3", "odds": 10.0},
                {"combination": "1-2-4", "odds": 20.0},
            ],
        }
    }
    result = extract_sanrenpuku_p3(response)
    # Horse 1 appears in both: inv_prob = 1/10 + 1/20 = 0.15
    # Horse 2 appears in both: inv_prob = 1/10 + 1/20 = 0.15
    # Horse 3 appears once: inv_prob = 1/10 = 0.10
    # Horse 4 appears once: inv_prob = 1/20 = 0.05
    # total = 0.15 + 0.15 + 0.10 + 0.05 = 0.45
    assert set(result.keys()) == {1, 2, 3, 4}
    total = sum(result.values())
    assert abs(total - 1.0) < 1e-9


def testextract_sanrenpuku_p3_empty_when_sanrenpuku_absent() -> None:
    response: dict[str, object] = {"latest": {"tansho": []}}
    assert extract_sanrenpuku_p3(response) == {}


def testextract_sanrenpuku_p3_empty_when_latest_absent() -> None:
    response: dict[str, object] = {}
    assert extract_sanrenpuku_p3(response) == {}


def testextract_sanrenpuku_p3_empty_when_latest_not_dict() -> None:
    response: dict[str, object] = {"latest": None}
    assert extract_sanrenpuku_p3(response) == {}


def testextract_sanrenpuku_p3_empty_when_sanrenpuku_empty_list() -> None:
    response: dict[str, object] = {"latest": {"3renpuku": []}}
    assert extract_sanrenpuku_p3(response) == {}


def testextract_sanrenpuku_p3_skips_malformed_combination() -> None:
    """Entries with wrong number of dash-separated parts are skipped."""
    response: dict[str, object] = {
        "latest": {
            "3renpuku": [
                {"combination": "1-2", "odds": 10.0},        # only 2 parts
                {"combination": "1-2-3-4", "odds": 10.0},    # 4 parts
                {"combination": "1-2-3", "odds": 5.0},       # valid
            ]
        }
    }
    result = extract_sanrenpuku_p3(response)
    # Only the last entry is valid
    assert set(result.keys()) == {1, 2, 3}
    assert abs(sum(result.values()) - 1.0) < 1e-9


def testextract_sanrenpuku_p3_skips_non_numeric_horses() -> None:
    """Entries where horse numbers can't be parsed as int are skipped."""
    response: dict[str, object] = {
        "latest": {
            "3renpuku": [
                {"combination": "1-X-3", "odds": 10.0},    # bad horse
                {"combination": "1-2-3", "odds": 8.0},     # valid
            ]
        }
    }
    result = extract_sanrenpuku_p3(response)
    assert set(result.keys()) == {1, 2, 3}


def testextract_sanrenpuku_p3_skips_zero_or_negative_odds() -> None:
    response: dict[str, object] = {
        "latest": {
            "3renpuku": [
                {"combination": "1-2-3", "odds": 0.0},
                {"combination": "1-2-4", "odds": -5.0},
                {"combination": "2-3-4", "odds": 10.0},
            ]
        }
    }
    result = extract_sanrenpuku_p3(response)
    assert set(result.keys()) == {2, 3, 4}


def testextract_sanrenpuku_p3_skips_non_dict_entries() -> None:
    response: dict[str, object] = {
        "latest": {
            "3renpuku": [
                None,
                "bad",
                {"combination": "1-2-3", "odds": 10.0},
            ]
        }
    }
    result = extract_sanrenpuku_p3(response)
    assert set(result.keys()) == {1, 2, 3}


def testextract_sanrenpuku_p3_skips_entries_missing_fields() -> None:
    response: dict[str, object] = {
        "latest": {
            "3renpuku": [
                {"combination": "1-2-3"},             # missing odds
                {"odds": 10.0},                        # missing combination
                {"combination": "4-5-6", "odds": 8.0},  # valid
            ]
        }
    }
    result = extract_sanrenpuku_p3(response)
    assert set(result.keys()) == {4, 5, 6}


def testextract_sanrenpuku_p3_normalization_sums_to_one() -> None:
    """Values must sum to exactly 1.0 after normalization."""
    response: dict[str, object] = {
        "latest": {
            "3renpuku": [
                {"combination": "1-2-3", "odds": 5.0},
                {"combination": "1-2-4", "odds": 10.0},
                {"combination": "1-3-4", "odds": 15.0},
                {"combination": "2-3-4", "odds": 25.0},
            ]
        }
    }
    result = extract_sanrenpuku_p3(response)
    assert abs(sum(result.values()) - 1.0) < 1e-9


def testextract_sanrenpuku_p3_skips_bad_odds_type() -> None:
    """Non-numeric odds values are skipped."""
    response: dict[str, object] = {
        "latest": {
            "3renpuku": [
                {"combination": "1-2-3", "odds": "bad"},
                {"combination": "4-5-6", "odds": 12.0},
            ]
        }
    }
    result = extract_sanrenpuku_p3(response)
    assert set(result.keys()) == {4, 5, 6}


# ---------------------------------------------------------------------------
# fetch_odds_and_sanrenpuku_for_race — combined fetch (no network)
# ---------------------------------------------------------------------------


def testfetch_odds_and_sanrenpuku_returns_both_on_success() -> None:
    """When the response has both tansho and sanrenpuku, returns rows + map."""
    encoded_key = "nar%3A2026%3A0610%3A44%3A01"
    url = f"{HOT_WORKER_BASE_URL}/{encoded_key}"
    stub = _StubFetcher(
        {
            url: {
                "latest": {
                    "tansho": [{"combination": "1", "odds": 7.3, "rank": 1}],
                    "3renpuku": [
                        {"combination": "1-2-3", "odds": 25.0},
                    ],
                }
            }
        }
    )
    rows, sanrenpuku_map = fetch_odds_and_sanrenpuku_for_race(
        stub, "nar", "20260610", "44", "01"
    )
    assert rows == [("44", "01", 1, 7.3, 1)]
    assert set(sanrenpuku_map.keys()) == {1, 2, 3}
    assert abs(sum(sanrenpuku_map.values()) - 1.0) < 1e-9


def testfetch_odds_and_sanrenpuku_returns_empty_both_on_network_error() -> None:
    """On HTTP error, returns ([], {})."""

    class _ErrorFetcher:
        def fetch(self, url: str, timeout: float) -> dict[str, object]:
            raise OSError("network error")

    rows, sanrenpuku_map = fetch_odds_and_sanrenpuku_for_race(
        _ErrorFetcher(), "nar", "20260610", "44", "01"
    )
    assert rows == []
    assert sanrenpuku_map == {}


def testfetch_odds_and_sanrenpuku_returns_empty_map_when_sanrenpuku_absent() -> None:
    """When response has tansho but no sanrenpuku, rows are returned, map is empty."""
    encoded_key = "nar%3A2026%3A0610%3A44%3A02"
    url = f"{HOT_WORKER_BASE_URL}/{encoded_key}"
    stub = _StubFetcher(
        {
            url: {
                "latest": {
                    "tansho": [{"combination": "1", "odds": 5.0, "rank": 1}],
                }
            }
        }
    )
    rows, sanrenpuku_map = fetch_odds_and_sanrenpuku_for_race(
        stub, "nar", "20260610", "44", "02"
    )
    assert rows == [("44", "02", 1, 5.0, 1)]
    assert sanrenpuku_map == {}


def testfetch_odds_and_sanrenpuku_calls_hot_worker_url() -> None:
    """Verifies the correct URL is called."""
    encoded_key = "nar%3A2026%3A0610%3A44%3A05"
    expected_url = f"{HOT_WORKER_BASE_URL}/{encoded_key}"
    stub = _StubFetcher({expected_url: {"latest": {"tansho": [], "3renpuku": []}}})
    fetch_odds_and_sanrenpuku_for_race(stub, "nar", "20260610", "44", "05")
    assert stub.calls == [expected_url]


# ---------------------------------------------------------------------------
# fetch_realtime_odds_parquet — exotic_sanrenpuku_p3_realtime column
# ---------------------------------------------------------------------------


def test_fetch_realtime_odds_parquet_has_exotic_sanrenpuku_column(
    tmp_path: Path,
) -> None:
    """The written parquet always has exotic_sanrenpuku_p3_realtime column."""
    import pandas as pd

    def _make_url(keibajo: str, race: str) -> str:
        import urllib.parse
        key = f"nar:2026:0610:{keibajo}:{race}"
        encoded = urllib.parse.quote(key, safe="")
        return f"{HOT_WORKER_BASE_URL}/{encoded}"

    responses: dict[str, dict[str, object]] = {
        _make_url("44", "01"): {
            "latest": {
                "tansho": [
                    {"combination": "1", "odds": 7.3, "rank": 1},
                    {"combination": "2", "odds": 12.5, "rank": 2},
                ],
                "3renpuku": [
                    {"combination": "1-2-3", "odds": 25.0},
                    {"combination": "1-2-4", "odds": 18.0},
                ],
            }
        },
    }
    stub = _StubFetcher(responses)

    result = fetch_realtime_odds_parquet(
        "nar",
        "20260610",
        tmp_path,
        race_keys=[("44", "01")],
        fetcher=stub,
    )

    assert result is not None
    df = pd.read_parquet(result)
    assert "exotic_sanrenpuku_p3_realtime" in df.columns


def test_fetch_realtime_odds_parquet_exotic_column_none_when_no_sanrenpuku(
    tmp_path: Path,
) -> None:
    """When sanrenpuku data is absent, exotic_sanrenpuku_p3_realtime is all None."""
    import pandas as pd

    def _make_url(keibajo: str, race: str) -> str:
        import urllib.parse
        key = f"nar:2026:0610:{keibajo}:{race}"
        encoded = urllib.parse.quote(key, safe="")
        return f"{HOT_WORKER_BASE_URL}/{encoded}"

    responses: dict[str, dict[str, object]] = {
        _make_url("44", "06"): {
            "latest": {
                "tansho": [
                    {"combination": "1", "odds": 5.0, "rank": 1},
                ],
                # no sanrenpuku key
            }
        },
    }
    stub = _StubFetcher(responses)

    result = fetch_realtime_odds_parquet(
        "nar",
        "20260610",
        tmp_path,
        race_keys=[("44", "06")],
        fetcher=stub,
    )

    assert result is not None
    df = pd.read_parquet(result)
    assert "exotic_sanrenpuku_p3_realtime" in df.columns
    assert bool(df["exotic_sanrenpuku_p3_realtime"].isna().all())


def test_fetch_realtime_odds_parquet_exotic_column_populated_for_known_horses(
    tmp_path: Path,
) -> None:
    """Horses in sanrenpuku get non-None values; unknown horses get None."""
    import pandas as pd

    def _make_url(keibajo: str, race: str) -> str:
        import urllib.parse
        key = f"nar:2026:0610:{keibajo}:{race}"
        encoded = urllib.parse.quote(key, safe="")
        return f"{HOT_WORKER_BASE_URL}/{encoded}"

    responses: dict[str, dict[str, object]] = {
        _make_url("44", "07"): {
            "latest": {
                "tansho": [
                    {"combination": "1", "odds": 5.0, "rank": 1},
                    {"combination": "2", "odds": 10.0, "rank": 2},
                ],
                "3renpuku": [
                    {"combination": "1-2-3", "odds": 20.0},
                ],
            }
        },
    }
    stub = _StubFetcher(responses)

    result = fetch_realtime_odds_parquet(
        "nar",
        "20260610",
        tmp_path,
        race_keys=[("44", "07")],
        fetcher=stub,
    )

    assert result is not None
    df = pd.read_parquet(result)
    df_sorted = df.sort_values("umaban").reset_index(drop=True)
    # Horse 1 and 2 are in tansho; sanrenpuku covers 1,2,3 but only 1 and 2 are in tansho
    assert not pd.isna(df_sorted.iloc[0]["exotic_sanrenpuku_p3_realtime"])  # horse 1
    assert not pd.isna(df_sorted.iloc[1]["exotic_sanrenpuku_p3_realtime"])  # horse 2
    # Values sum (they're fractions of the total; horses 1 and 2 each get a share)
    p3_h1 = df_sorted.iloc[0]["exotic_sanrenpuku_p3_realtime"]
    p3_h2 = df_sorted.iloc[1]["exotic_sanrenpuku_p3_realtime"]
    assert p3_h1 > 0.0
    assert p3_h2 > 0.0
