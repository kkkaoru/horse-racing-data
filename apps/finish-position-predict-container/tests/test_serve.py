"""Unit tests for ``predict_lib.serve``.

Covers: parse_predict_params (incl. mode param), parse_request_path,
mask_error_message, build_progress_line, build_result_line,
build_r2_feat_cache_key, iter_predict_chunks (mode=full, mode=rescore,
rescore-fallback, threaded keepalive), CacheMissError.

``_predict_category`` is mocked via the ``predict_fn`` / ``rescore_fn``
arguments so no real Neon / DuckDB / ML I/O is performed.  Coverage is
measured under ``--cov=predict_lib`` (``pyproject.toml``).

Threading keepalive design note
---------------------------------
``iter_predict_chunks`` runs the prediction callable in a background thread
and yields progress lines at ``progress_interval_s`` intervals while the
thread is alive.  Tests that exercise this behaviour inject:
  - ``sleep_fn=_noop_sleep`` to avoid real 1-second sleeps (existing tests).
  - A ``threading.Event``-controlled predict_fn + a clock that advances on
    each ``sleep_fn`` call to drive the keepalive loop deterministically.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast

import pytest

from predict_lib import serve as serve_module
from predict_lib.debug_log import (
    drain_debug_progress,
    record_debug_progress,
    record_operational_progress,
)
from predict_lib.focused_full_cache import FocusedFullCachePayload
from predict_lib.serve import (
    FOCUSED_FULL_ACCEPTED_STATUS,
    FOCUSED_FULL_ALREADY_COMPLETE_STATUS,
    FOCUSED_FULL_BUSY_STATUS,
    FOCUSED_FULL_SLOT_BUSY,
    FOCUSED_FULL_SLOT_CLAIMED,
    FOCUSED_FULL_SLOT_IN_FLIGHT_SELF,
    PREWARM_EMPTY_STATUS,
    RESCORE_ATTESTATION_FUTURE_SKEW_MS,
    RESCORE_ATTESTATION_MAX_ENTRIES,
    RESCORE_ATTESTATION_TTL_MS,
    CacheMissError,
    CacheValidationError,
    FocusedFullSlotState,
    MarketSignalFoundationAttestation,
    PredictCategoryFn,
    PredictParams,
    PrewarmParams,
    R2Config,
    RescoreCacheAttestation,
    SleepFn,
    TimeFn,
    WeightSnapshotGeneration,
    activate_scoped_rescore_cache_miss_fallback,
    build_focused_full_cache_response_body,
    build_focused_full_race_key,
    build_focused_full_status_response_body,
    build_prewarm_cache_key,
    build_prewarm_result_line,
    build_prewarm_status_response_body,
    build_progress_line,
    build_r2_day_base_key,
    build_r2_feat_cache_key,
    build_r2_per_race_feat_cache_key,
    build_r2_running_style_foundation_key,
    build_result_line,
    current_market_signal_foundation_attestation,
    get_prewarm_run_state,
    has_single_race_scope,
    is_focused_full_request,
    is_scoped_rescore_cache_miss_fallback,
    iter_predict_chunks,
    iter_prewarm_chunks,
    mark_focused_full_progress,
    mask_error_message,
    parse_day_base_cache_identity,
    parse_focused_full_cache_query,
    parse_predict_params,
    parse_prewarm_params,
    parse_request_path,
    run_prewarm_in_background,
)

_RELEASE_PREWARM_SLOT_ATTR = "_release_prewarm_slot"
_release_prewarm_slot = cast(
    Callable[[str], None],
    getattr(serve_module, _RELEASE_PREWARM_SLOT_ATTR),
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _noop_sleep(_: float) -> None:
    """No-op sleep injected into tests that do not need wall-clock delays."""


def _make_time_fn(increments: list[float]) -> Callable[[], float]:
    """Return a monotonic-clock stub that returns successive values."""
    calls = iter(increments)
    current = [0.0]

    def _tick() -> float:
        try:
            current[0] = next(calls)
        except StopIteration:
            current[0] += 1.0
        return current[0]

    return _tick


def _mock_predict_ok(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
    card_max_race_bango: int | None = None,
) -> int:
    return 42


# ---------------------------------------------------------------------------
# parse_request_path
# ---------------------------------------------------------------------------


def test_parse_request_path_with_query() -> None:
    path, qs = parse_request_path("/predict?category=jra&runDate=20260619&daysAhead=0")
    assert path == "/predict"
    assert "category=jra" in qs


def test_parse_request_path_no_query() -> None:
    path, qs = parse_request_path("/ping")
    assert path == "/ping"
    assert qs == ""


def test_parse_request_path_empty_query_string() -> None:
    path, qs = parse_request_path("/predict?")
    assert path == "/predict"
    assert qs == ""


# ---------------------------------------------------------------------------
# parse_predict_params — success
# ---------------------------------------------------------------------------


def test_parse_predict_params_jra_success() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&daysAhead=0")
    assert isinstance(result, PredictParams)
    assert result.category == "jra"
    assert result.run_date == "20260619"
    assert result.days_ahead == 0


def test_parse_predict_params_nar_success() -> None:
    result = parse_predict_params("category=nar&runDate=20260619&daysAhead=2")
    assert isinstance(result, PredictParams)
    assert result.category == "nar"
    assert result.days_ahead == 2


def test_parse_predict_params_banei_success() -> None:
    result = parse_predict_params("category=ban-ei&runDate=20260619")
    assert isinstance(result, PredictParams)
    assert result.category == "ban-ei"
    assert result.days_ahead == 0  # default


def test_parse_predict_params_debug_flag_enabled() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&debug=1")
    assert isinstance(result, PredictParams)
    assert result.debug_logs is True


def test_parse_predict_params_debug_flag_default_false() -> None:
    result = parse_predict_params("category=jra&runDate=20260619")
    assert isinstance(result, PredictParams)
    assert result.debug_logs is False


def test_parse_predict_params_debug_flag_zero_is_off() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&debug=0")
    assert isinstance(result, PredictParams)
    assert result.debug_logs is False


def test_parse_predict_params_debug_flag_false_is_off() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&debug=false")
    assert isinstance(result, PredictParams)
    assert result.debug_logs is False


def test_parse_predict_params_debug_flag_garbage_is_off() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&debug=maybe")
    assert isinstance(result, PredictParams)
    assert result.debug_logs is False


def test_parse_predict_params_debug_flag_true_is_on() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&debug=true")
    assert isinstance(result, PredictParams)
    assert result.debug_logs is True


def test_parse_predict_params_force_flag_enabled() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&force=1")
    assert isinstance(result, PredictParams)
    assert result.force is True


def test_parse_predict_params_force_flag_default_false() -> None:
    result = parse_predict_params("category=jra&runDate=20260619")
    assert isinstance(result, PredictParams)
    assert result.force is False


def _market_signal_attestation_query(**overrides: str) -> str:
    values = {
        "category": "jra",
        "runDate": "20260824",
        "mode": "full",
        "keibajoCode": "01",
        "raceBango": "03",
        "marketSignalFoundationKey": (
            "feat-racechain-market-signal/catalog-v1/jra/20260824/01/03/foundation.json"
        ),
        "marketSignalFoundationEtag": "artifact-etag",
        "marketSignalFoundationVersion": "",
        "marketSignalOddsSnapshotHash": "a" * 64,
        "marketSignalBaseGenerationId": "b" * 64,
    }
    values.update(overrides)
    return "&".join(f"{name}={value}" for name, value in values.items())


def test_parse_predict_params_accepts_complete_market_signal_attestation() -> None:
    result = parse_predict_params(_market_signal_attestation_query())
    assert isinstance(result, PredictParams)
    assert result.market_signal_foundation_attestation == MarketSignalFoundationAttestation(
        base_generation_id="b" * 64,
        etag="artifact-etag",
        key="feat-racechain-market-signal/catalog-v1/jra/20260824/01/03/foundation.json",
        odds_snapshot_hash="a" * 64,
        version="",
    )


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            _market_signal_attestation_query(marketSignalFoundationEtag=""),
            "Etag must be non-empty",
        ),
        (
            _market_signal_attestation_query(marketSignalOddsSnapshotHash="A" * 64),
            "OddsSnapshotHash must be a lowercase SHA-256",
        ),
        (
            _market_signal_attestation_query(marketSignalBaseGenerationId="short"),
            "BaseGenerationId must be a lowercase SHA-256",
        ),
        (
            _market_signal_attestation_query(category="nar"),
            "requires a focused JRA mode=full request",
        ),
        (
            _market_signal_attestation_query(mode="rescore"),
            "requires a focused JRA mode=full request",
        ),
        (
            _market_signal_attestation_query(marketSignalFoundationVersion="missing").replace(
                "&marketSignalFoundationEtag=artifact-etag", ""
            ),
            "requires all five parameters",
        ),
    ],
)
def test_parse_predict_params_rejects_invalid_market_signal_attestation(
    query: str, expected: str
) -> None:
    result = parse_predict_params(query)
    assert isinstance(result, str)
    assert expected in result


def test_predict_execution_binds_and_resets_market_signal_attestation() -> None:
    attestation = MarketSignalFoundationAttestation(
        base_generation_id="b" * 64,
        etag="artifact-etag",
        key="artifact-key",
        odds_snapshot_hash="a" * 64,
        version="artifact-version",
    )
    observed: list[MarketSignalFoundationAttestation | None] = []

    def predict(
        _category: str,
        _run_date: str,
        _days_ahead: int,
        _keibajo_code: str | None,
        _race_bango: str | None,
        _card_max_race_bango: int | None,
    ) -> int:
        observed.append(current_market_signal_foundation_attestation())
        return 1

    params = PredictParams(
        category="jra",
        run_date="20260824",
        days_ahead=0,
        mode="full",
        keibajo_code="01",
        race_bango="03",
        market_signal_foundation_attestation=attestation,
    )
    list(iter_predict_chunks(params, predict, sleep_fn=_noop_sleep))
    assert observed == [attestation]
    assert current_market_signal_foundation_attestation() is None


def _attested_query(**overrides: str) -> str:
    values = {
        "category": "jra",
        "runDate": "20260823",
        "mode": "rescore",
        "keibajoCode": "07",
        "raceBango": "03",
        "entrySetHash": "a" * 64,
        "entryCount": "16",
        "featureCacheEtag": "etag-123",
        "featureCacheVersion": "",
        "attestationIssuedAtMs": "2000000",
        "weightSnapshotCount": "2",
        "weightSnapshotFetchedAt": "2026-08-23T14%3A30%3A00%2B09%3A00",
        "weightSnapshotHash": "b" * 64,
        "raceStartAtJst": "2026-08-23T15%3A30%3A00%2B09%3A00",
    }
    values.update(overrides)
    return "&".join(f"{key}={value}" for key, value in values.items())


def test_parse_predict_params_accepts_complete_rescore_attestation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(serve_module.time, "time_ns", lambda: 2_000_000 * 1_000_000)

    result = parse_predict_params(_attested_query())

    assert isinstance(result, PredictParams)
    assert result.rescore_cache_attestation == RescoreCacheAttestation(
        entry_set_hash="a" * 64,
        entry_count=16,
        feature_cache_etag="etag-123",
        feature_cache_version="",
        issued_at_ms=2_000_000,
    )


def test_parse_predict_params_attestation_absent_preserves_canary_compatibility() -> None:
    result = parse_predict_params("category=jra&runDate=20260823&mode=rescore")
    assert isinstance(result, PredictParams)
    assert result.rescore_cache_attestation is None


def test_parse_predict_params_accepts_exact_weight_snapshot_generation() -> None:
    result = parse_predict_params(
        "category=jra&runDate=20260823&mode=rescore&keibajoCode=07&raceBango=03"
        "&raceStartAtJst=2026-08-23T15%3A30%3A00%2B09%3A00"
        "&weightSnapshotCount=2&weightSnapshotFetchedAt=2026-08-23T14%3A30%3A00%2B09%3A00"
        "&weightSnapshotHash=" + "b" * 64
    )

    assert isinstance(result, PredictParams)
    assert result.weight_snapshot_generation == WeightSnapshotGeneration(
        count=2,
        fetched_at="2026-08-23T14:30:00+09:00",
        snapshot_hash="b" * 64,
    )


def test_parse_predict_params_accepts_attested_active_and_canceled_runner_sets() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260824&mode=rescore&keibajoCode=35&raceBango=03"
        "&raceStartAtJst=2026-08-24T12%3A55%3A00%2B09%3A00"
        "&weightSnapshotCount=2&weightSnapshotFetchedAt=2026-08-24T12%3A03%3A41%2B09%3A00"
        "&weightSnapshotHash="
        + "b"
        * 64
        + "&activeHorseNumbers=%5B1%2C3%5D&excludedHorseNumbers=%5B2%5D"
        "&entrySnapshotFetchedAt=2026-08-24T12%3A03%3A41%2B09%3A00"
        "&entrySnapshotHash=00574dee8f89ae6c93ded1fc8187aa58e1a9d460db315e00c1d782296997e5d7"
    )

    assert isinstance(result, PredictParams)
    assert result.weight_snapshot_generation == WeightSnapshotGeneration(
        count=2,
        fetched_at="2026-08-24T12:03:41+09:00",
        snapshot_hash="b" * 64,
        active_horse_numbers=(1, 3),
        excluded_horse_numbers=(2,),
        entry_snapshot_fetched_at="2026-08-24T12:03:41+09:00",
        entry_snapshot_hash="00574dee8f89ae6c93ded1fc8187aa58e1a9d460db315e00c1d782296997e5d7",
    )


def test_parse_predict_params_rejects_entry_snapshot_hash_mismatch() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260824&mode=rescore&keibajoCode=35&raceBango=03"
        "&raceStartAtJst=2026-08-24T12%3A55%3A00%2B09%3A00"
        "&weightSnapshotCount=2&weightSnapshotFetchedAt=2026-08-24T12%3A03%3A41%2B09%3A00"
        "&weightSnapshotHash="
        + "b"
        * 64
        + "&activeHorseNumbers=%5B1%2C3%5D&excludedHorseNumbers=%5B2%5D"
        "&entrySnapshotFetchedAt=2026-08-24T12%3A03%3A41%2B09%3A00"
        "&entrySnapshotHash=" + "c" * 64
    )

    assert result == "entry snapshot generation hash mismatch"


@pytest.mark.parametrize(
    ("suffix", "expected"),
    [
        ("", "requires all weight snapshot generation parameters"),
        ("&weightSnapshotCount=0", "requires all weight snapshot generation parameters"),
        (
            "&weightSnapshotCount=1&weightSnapshotFetchedAt=not-a-date"
            "&weightSnapshotHash=" + "b" * 64,
            "invalid weightSnapshotFetchedAt",
        ),
        (
            "&weightSnapshotCount=1&weightSnapshotFetchedAt=2026-08-23T14%3A30%3A00%2B09%3A00"
            "&weightSnapshotHash=" + "B" * 64,
            "invalid weightSnapshotHash",
        ),
    ],
)
def test_parse_predict_params_rejects_invalid_weight_snapshot_generation(
    suffix: str,
    expected: str,
) -> None:
    result = parse_predict_params(
        "category=jra&runDate=20260823&mode=rescore&keibajoCode=07&raceBango=03"
        "&raceStartAtJst=2026-08-23T15%3A30%3A00%2B09%3A00" + suffix
    )

    assert isinstance(result, str)
    assert expected in result


@pytest.mark.parametrize(
    ("race_start_query", "expected"),
    [
        ("", "requires raceStartAtJst"),
        ("&raceStartAtJst=invalid", "invalid raceStartAtJst"),
        ("&raceStartAtJst=2026-08-23T15%3A30%3A00", "invalid raceStartAtJst"),
    ],
)
def test_parse_predict_params_rejects_missing_or_invalid_race_start(
    race_start_query: str,
    expected: str,
) -> None:
    result = parse_predict_params(
        "category=jra&runDate=20260823&mode=rescore&keibajoCode=07&raceBango=03" + race_start_query
    )

    assert isinstance(result, str)
    assert expected in result


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "category=jra&runDate=20260823&mode=rescore&keibajoCode=07&raceBango=03"
            "&raceStartAtJst=2026-08-23T15%3A30%3A00%2B09%3A00"
            "&weightSnapshotCount=2&weightSnapshotFetchedAt=2026-08-23T14%3A30%3A00%2B09%3A00"
            "&weightSnapshotHash=" + "b" * 64 + "&entrySetHash=" + "a" * 64,
            "must include all",
        ),
        (_attested_query(mode="full"), "requires mode=rescore"),
        (_attested_query(keibajoCode=""), "requires an exact race scope"),
        (_attested_query(entrySetHash="A" * 64), "invalid entrySetHash"),
        (_attested_query(entryCount="bad"), "invalid entryCount"),
        (_attested_query(entryCount="0"), "invalid entryCount"),
        (
            _attested_query(entryCount=str(RESCORE_ATTESTATION_MAX_ENTRIES + 1)),
            "invalid entryCount",
        ),
        (_attested_query(featureCacheEtag=""), "invalid featureCacheEtag"),
        (_attested_query(featureCacheVersion="%20"), "invalid featureCacheVersion"),
        (_attested_query(attestationIssuedAtMs="bad"), "invalid attestationIssuedAtMs"),
        (_attested_query(attestationIssuedAtMs="-1"), "invalid attestationIssuedAtMs"),
    ],
)
def test_parse_predict_params_rejects_invalid_attestation(
    query: str,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(serve_module.time, "time_ns", lambda: 2_000_000 * 1_000_000)
    result = parse_predict_params(query)
    assert isinstance(result, str)
    assert expected in result


@pytest.mark.parametrize(
    ("issued_at_ms", "expected"),
    [
        (
            2_000_000 + RESCORE_ATTESTATION_FUTURE_SKEW_MS + 1,
            "too far in the future",
        ),
        (2_000_000 - RESCORE_ATTESTATION_TTL_MS - 1, "expired"),
    ],
)
def test_parse_predict_params_rejects_attestation_outside_time_window(
    issued_at_ms: int,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(serve_module.time, "time_ns", lambda: 2_000_000 * 1_000_000)
    result = parse_predict_params(_attested_query(attestationIssuedAtMs=str(issued_at_ms)))
    assert isinstance(result, str)
    assert expected in result


def test_iter_predict_chunks_sets_debug_env_during_predict() -> None:
    seen: list[str | None] = []
    os.environ["PREDICT_DEBUG_LOGS"] = "previous"

    def _predict_debug(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        seen.append(os.environ.get("PREDICT_DEBUG_LOGS"))
        return 1

    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        debug_logs=True,
    )
    chunks = list(iter_predict_chunks(params, _predict_debug, sleep_fn=_noop_sleep))
    assert json.loads(chunks[-1].decode())["status"] == "success"
    assert seen == ["1"]
    assert os.environ["PREDICT_DEBUG_LOGS"] == "previous"
    os.environ.pop("PREDICT_DEBUG_LOGS", None)


def test_build_result_line_omits_step_when_not_provided() -> None:
    line = build_result_line("jra", "20260619", 10, status="success")
    parsed = json.loads(line.decode())
    assert "step" not in parsed


def test_build_result_line_includes_step_when_provided() -> None:
    line = build_result_line(
        "jra",
        "20260619",
        10,
        status="success",
        step="step=racechain-layer index=1/1 status=done",
    )
    parsed = json.loads(line.decode())
    assert parsed["step"] == "step=racechain-layer index=1/1 status=done"


def test_iter_predict_chunks_debug_off_omits_racechain_and_daybase_hit_progress() -> None:
    drain_debug_progress()

    def _predict_with_tokens(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        record_debug_progress("step=daybase-hit source=local category=jra target_date=20260619")
        record_debug_progress("step=racechain-layer index=1/1 status=start")
        return 1

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, debug_logs=False)
    chunks = list(iter_predict_chunks(params, _predict_with_tokens, sleep_fn=_noop_sleep))
    joined = b"".join(chunks).decode()
    assert "racechain-layer" not in joined
    assert "daybase-hit" not in joined
    assert "daybase-miss" not in joined
    assert "daybase-base" not in joined
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert "step" not in last


def test_iter_predict_chunks_debug_on_daybase_present_emits_racechain_not_rebuild() -> None:
    drain_debug_progress()

    def _predict_hit(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        record_debug_progress(
            "step=daybase-hit source=local category=jra target_date=20260619 reason=watermark-match"
        )
        record_debug_progress(
            "step=racechain-layer index=1/1 status=start category=jra script=x.py "
            "target_race=83:03 elapsed_seconds=0.000"
        )
        return 1

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, debug_logs=True)
    chunks = list(iter_predict_chunks(params, _predict_hit, sleep_fn=_noop_sleep))
    joined = b"".join(chunks).decode()
    assert "step=daybase-hit" in joined
    assert "step=racechain-layer" in joined
    assert "step=daybase-base" not in joined
    assert "step=daybase-miss" not in joined
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["step"] == (
        "step=racechain-layer index=1/1 status=start category=jra script=x.py "
        "target_race=83:03 elapsed_seconds=0.000"
    )


def test_iter_predict_chunks_debug_on_daybase_missing_emits_miss_rebuild_token() -> None:
    drain_debug_progress()

    def _predict_miss(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        record_debug_progress(
            "step=daybase-miss category=jra target_date=20260619 reason=no-local-cache"
        )
        record_debug_progress(
            "step=daybase-base index=0 status=start category=jra target_date=20260619"
        )
        return 1

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, debug_logs=True)
    chunks = list(iter_predict_chunks(params, _predict_miss, sleep_fn=_noop_sleep))
    joined = b"".join(chunks).decode()
    assert "step=daybase-miss" in joined
    assert "step=daybase-base" in joined
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["step"] == (
        "step=daybase-base index=0 status=start category=jra target_date=20260619"
    )


def test_iter_predict_chunks_debug_on_without_pipeline_steps_omits_result_step() -> None:
    drain_debug_progress()
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, debug_logs=True)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    joined = b"".join(chunks).decode()
    assert last["status"] == "success"
    assert "step" not in last
    assert "racechain-layer" not in joined
    assert "daybase-hit" not in joined


def test_iter_predict_chunks_debug_on_error_includes_last_step() -> None:
    drain_debug_progress()

    def _predict_fail(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        record_debug_progress(
            "step=daybase-miss category=jra target_date=20260619 reason=no-local-cache"
        )
        raise RuntimeError("rebuild failed")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, debug_logs=True)
    chunks = list(iter_predict_chunks(params, _predict_fail, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    joined = b"".join(chunks).decode()
    assert last["status"] == "error"
    assert "step=daybase-miss" in joined
    assert last["step"] == (
        "step=daybase-miss category=jra target_date=20260619 reason=no-local-cache"
    )


def test_iter_predict_chunks_debug_on_emits_daybase_hit_during_keepalive() -> None:
    drain_debug_progress()
    started = threading.Event()
    unblock = threading.Event()
    sleeps = [0]

    def _predict_hold(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        record_debug_progress(
            "step=daybase-hit source=local category=jra target_date=20260619 reason=watermark-match"
        )
        started.set()
        unblock.wait(timeout=2.0)
        record_debug_progress(
            "step=racechain-layer index=1/1 status=done category=jra script=x.py "
            "target_race=83:03 elapsed_seconds=0.100"
        )
        return 1

    def _sleep(_: float) -> None:
        sleeps[0] += 1
        if sleeps[0] >= 2:
            unblock.set()

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, debug_logs=True)
    chunks = list(iter_predict_chunks(params, _predict_hold, sleep_fn=_sleep))
    joined = b"".join(chunks).decode()
    last = json.loads(chunks[-1].decode())
    assert started.wait(timeout=2.0)
    assert "step=daybase-hit" in joined
    assert "step=racechain-layer" in joined
    assert "step=daybase-base" not in joined
    assert last["status"] == "success"
    assert last["step"] == (
        "step=racechain-layer index=1/1 status=done category=jra script=x.py "
        "target_race=83:03 elapsed_seconds=0.100"
    )


def test_iter_predict_chunks_emits_result_ndjson_without_debug(
    capsys: pytest.CaptureFixture[str],
) -> None:
    os.environ.pop("PREDICT_DEBUG_LOGS", None)

    def _predict_ok(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        return 3

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _predict_ok, sleep_fn=_noop_sleep))
    parsed = json.loads(chunks[-1].decode())
    assert parsed["type"] == "result"
    assert parsed["status"] == "success"
    assert parsed["racesPredicted"] == 3
    assert parsed["category"] == "jra"
    captured = capsys.readouterr()
    assert captured.err == ""


def test_iter_prewarm_chunks_emits_result_ndjson_without_debug(
    capsys: pytest.CaptureFixture[str],
) -> None:
    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            sleep_fn=_noop_sleep,
        )
    )
    parsed = json.loads(chunks[-1].decode())
    assert parsed["type"] == "result"
    assert parsed["status"] == "success"
    assert parsed["category"] == "jra"
    assert parsed["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"
    captured = capsys.readouterr()
    assert captured.err == ""


def test_parse_predict_params_days_ahead_missing_defaults_to_zero() -> None:
    result = parse_predict_params("category=jra&runDate=20260619")
    assert isinstance(result, PredictParams)
    assert result.days_ahead == 0


# ---------------------------------------------------------------------------
# parse_predict_params — validation errors
# ---------------------------------------------------------------------------


def test_parse_predict_params_missing_category() -> None:
    result = parse_predict_params("runDate=20260619&daysAhead=0")
    assert isinstance(result, str)
    assert "category" in result


def test_parse_predict_params_invalid_category() -> None:
    result = parse_predict_params("category=invalid&runDate=20260619")
    assert isinstance(result, str)
    assert "invalid" in result


def test_parse_predict_params_missing_run_date() -> None:
    result = parse_predict_params("category=jra&daysAhead=0")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_predict_params_invalid_run_date_non_digits() -> None:
    result = parse_predict_params("category=jra&runDate=2026-06-19")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_predict_params_invalid_run_date_too_short() -> None:
    result = parse_predict_params("category=jra&runDate=2026061")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_predict_params_invalid_run_date_too_long() -> None:
    result = parse_predict_params("category=jra&runDate=202606190")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_predict_params_days_ahead_non_integer() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&daysAhead=abc")
    assert isinstance(result, str)
    assert "daysAhead" in result


def test_parse_predict_params_days_ahead_negative() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&daysAhead=-1")
    assert isinstance(result, str)
    assert "daysAhead" in result


def test_parse_predict_params_empty_query_string() -> None:
    result = parse_predict_params("")
    assert isinstance(result, str)
    assert "category" in result


# ---------------------------------------------------------------------------
# mask_error_message
# ---------------------------------------------------------------------------


def test_mask_error_message_postgresql_with_password() -> None:
    raw = "connect failed: postgresql://user:secret@neon.tech/db"
    masked = mask_error_message(raw)
    assert "secret" not in masked
    assert "[REDACTED]" in masked
    assert "neon.tech/db" in masked


def test_mask_error_message_postgresql_no_password() -> None:
    raw = "connect failed: postgresql://user@neon.tech/db"
    masked = mask_error_message(raw)
    assert "[REDACTED]" in masked
    assert "neon.tech/db" in masked


def test_mask_error_message_postgres_scheme() -> None:
    raw = "postgres://user:pw@host/db"
    masked = mask_error_message(raw)
    assert "[REDACTED]" in masked
    assert "pw" not in masked


def test_mask_error_message_no_credentials() -> None:
    msg = "some error without a URL"
    assert mask_error_message(msg) == msg


def test_mask_error_message_empty_string() -> None:
    assert mask_error_message("") == ""


# ---------------------------------------------------------------------------
# build_progress_line
# ---------------------------------------------------------------------------


def test_build_progress_line_returns_bytes() -> None:
    line = build_progress_line("starting", 0.0)
    assert isinstance(line, bytes)


def test_build_progress_line_ends_with_newline() -> None:
    line = build_progress_line("feature-build", 12.3)
    assert line.endswith(b"\n")


def test_build_progress_line_valid_json() -> None:
    line = build_progress_line("predict", 42.7)
    parsed = json.loads(line.decode())
    assert parsed["type"] == "progress"
    assert parsed["stage"] == "predict"
    assert parsed["elapsed_s"] == pytest.approx(42.7, abs=0.1)


def test_build_progress_line_elapsed_rounded_to_one_decimal() -> None:
    line = build_progress_line("x", 12.345)
    parsed = json.loads(line.decode())
    # round(12.345, 1) == 12.3 in Python
    assert parsed["elapsed_s"] == pytest.approx(12.3, abs=0.05)


# ---------------------------------------------------------------------------
# build_result_line
# ---------------------------------------------------------------------------


def test_build_result_line_success() -> None:
    line = build_result_line("jra", "20260619", 10, status="success")
    parsed = json.loads(line.decode())
    assert parsed["type"] == "result"
    assert parsed["category"] == "jra"
    assert parsed["runDate"] == "20260619"
    assert parsed["racesPredicted"] == 10
    assert parsed["status"] == "success"
    assert "error" not in parsed


def test_build_result_line_error_with_message() -> None:
    line = build_result_line("nar", "20260619", 3, status="error", error="RuntimeError: oops")
    parsed = json.loads(line.decode())
    assert parsed["status"] == "error"
    assert "error" in parsed
    assert parsed["racesPredicted"] == 3


def test_build_result_line_error_masks_credentials() -> None:
    raw_error = "connect failed: postgresql://user:secret@neon.tech/db"
    line = build_result_line("jra", "20260619", 0, status="error", error=raw_error)
    assert b"secret" not in line
    assert b"[REDACTED]" in line


def test_build_result_line_ends_with_newline() -> None:
    line = build_result_line("ban-ei", "20260619", 5, status="success")
    assert line.endswith(b"\n")


def test_build_result_line_valid_json() -> None:
    line = build_result_line("jra", "20260619", 0, status="success")
    parsed = json.loads(line.decode())
    assert isinstance(parsed, dict)


# ---------------------------------------------------------------------------
# iter_predict_chunks — success path
# ---------------------------------------------------------------------------


def test_iter_predict_chunks_success_yields_progress_then_result() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))

    # At minimum: starting + predict + result lines
    assert len(chunks) >= 3

    parsed = [json.loads(c.decode()) for c in chunks]
    types = [p["type"] for p in parsed]
    assert types[0] == "progress"
    assert types[-1] == "result"
    assert "result" in types


def test_iter_predict_chunks_result_is_success() -> None:
    params = PredictParams(category="nar", run_date="20260619", days_ahead=1)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["racesPredicted"] == 42
    assert last["category"] == "nar"
    assert last["runDate"] == "20260619"


def test_iter_predict_chunks_first_chunk_is_starting_progress() -> None:
    params = PredictParams(category="ban-ei", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    first = json.loads(chunks[0].decode())
    assert first["type"] == "progress"
    assert first["stage"] == "starting"


def test_iter_predict_chunks_predict_progress_emitted() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = [p.get("stage") for p in parsed if p.get("type") == "progress"]
    assert "predict" in stages


# ---------------------------------------------------------------------------
# iter_predict_chunks — exception path
# ---------------------------------------------------------------------------


def _mock_predict_raises(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
    card_max_race_bango: int | None = None,
) -> int:
    raise RuntimeError("feature build failed: postgresql://user:pw@host/db")


def test_iter_predict_chunks_exception_yields_error_result() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "error"
    assert "error" in last


def test_iter_predict_chunks_exception_races_predicted_is_zero() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["racesPredicted"] == 0


def test_iter_predict_chunks_exception_masks_credentials() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises, sleep_fn=_noop_sleep))
    last_bytes = chunks[-1]
    assert b"pw" not in last_bytes
    assert b"[REDACTED]" in last_bytes


def test_iter_predict_chunks_exception_error_includes_exception_type() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert "RuntimeError" in last["error"]


def test_iter_predict_chunks_never_raises() -> None:
    """The generator must not propagate any exception from predict_fn."""

    def _explode(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        raise ValueError("unexpected kaboom")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    # If this raises, the test fails — the generator must catch all exceptions.
    chunks = list(iter_predict_chunks(params, _explode, sleep_fn=_noop_sleep))
    assert len(chunks) >= 1


# ---------------------------------------------------------------------------
# iter_predict_chunks — interval-based progress gating (pre-thread checks)
# ---------------------------------------------------------------------------


def test_iter_predict_chunks_feature_build_progress_when_interval_elapsed() -> None:
    """The feature-build progress line fires when interval elapses at that checkpoint.

    time_fn() call order inside iter_predict_chunks (fast mock, no loop iterations):
      1  started=                          (0.0)
      2  _elapsed() in "starting" yield    (0.0)
      3  last_progress=                    (0.0)
      4  now= (feature-build gate)         (20.0)  <- 20 - 0 >= 10 -> fires
      5  _elapsed() in "feature-build"     (20.0)
      (last_progress = 20.0)
      6  _elapsed() in "predict" yield     (20.0)
      7  last_progress=                    (20.0)
      ... keepalive loop (0 iterations for instant mock) ...
      8+ post-pipeline check
    """
    # Provide enough values; after index 7 the stub increments by 1.0 each call.
    time_fn = _make_time_fn([0.0, 0.0, 0.0, 20.0, 20.0, 20.0, 20.0, 20.0, 25.0, 30.0])
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            time_fn=time_fn,
            sleep_fn=_noop_sleep,
            progress_interval_s=10.0,
        )
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "feature-build" in stages


def test_iter_predict_chunks_no_extra_progress_within_interval() -> None:
    """When all calls complete before the interval, no extra progress is emitted."""
    # All time_fn calls return 0.0 so interval never elapses.
    time_fn = _make_time_fn([0.0] * 30)
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            time_fn=time_fn,
            sleep_fn=_noop_sleep,
            progress_interval_s=10.0,
        )
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "starting" in stages
    assert "predict" in stages
    assert "feature-build" not in stages


def test_iter_predict_chunks_post_pipeline_progress_when_interval_elapsed() -> None:
    """After predict_fn returns, a completion progress is emitted if interval elapsed.

    time_fn() call order (no feature-build gate, fast mock, 0 loop iterations):
      1 started=         (0.0)
      2 _elapsed() "starting"              (0.0)
      3 last_progress=                     (0.0)
      4 now= (feature-build gate)          (0.0) -> gate does not fire
      5 _elapsed() "predict"               (0.0)
      6 last_progress= (reset)             (0.0)
      ... loop: 0 iterations for instant mock ...
      7 now (post-pipeline check)          (100.0) -> 100-0 >= 10 -> fires
    """
    call_count = [0]

    def _slow_time() -> float:
        call_count[0] += 1
        if call_count[0] <= 6:
            return 0.0
        return 100.0

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            time_fn=_slow_time,
            sleep_fn=_noop_sleep,
            progress_interval_s=10.0,
        )
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "complete" in stages


# ---------------------------------------------------------------------------
# parse_predict_params — mode parameter
# ---------------------------------------------------------------------------


def test_parse_predict_params_mode_full_explicit() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&mode=full")
    assert isinstance(result, PredictParams)
    assert result.mode == "full"


def test_parse_predict_params_mode_rescore() -> None:
    result = parse_predict_params("category=nar&runDate=20260619&mode=rescore")
    assert isinstance(result, PredictParams)
    assert result.mode == "rescore"


def test_parse_predict_params_mode_default_is_full() -> None:
    result = parse_predict_params("category=jra&runDate=20260619")
    assert isinstance(result, PredictParams)
    assert result.mode == "full"


def test_parse_predict_params_mode_invalid() -> None:
    result = parse_predict_params("category=jra&runDate=20260619&mode=turbo")
    assert isinstance(result, str)
    assert "mode" in result
    assert "turbo" in result


def test_predict_params_mode_stored_correctly() -> None:
    params = PredictParams(category="ban-ei", run_date="20260619", days_ahead=2, mode="rescore")
    assert params.mode == "rescore"
    assert params.category == "ban-ei"


# ---------------------------------------------------------------------------
# parse_predict_params — race-scope (keibajoCode / raceBango) parameters
# ---------------------------------------------------------------------------


def test_parse_predict_params_keibajo_code_parsed() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260619&mode=rescore&keibajoCode=44&raceBango=01"
        "&raceStartAtJst=2026-06-19T15%3A30%3A00%2B09%3A00"
        "&weightSnapshotCount=1&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00"
        "&weightSnapshotHash=" + "b" * 64
    )
    assert isinstance(result, PredictParams)
    assert result.keibajo_code == "44"


def test_parse_predict_params_race_bango_parsed() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260619&mode=rescore&keibajoCode=44&raceBango=01"
        "&raceStartAtJst=2026-06-19T15%3A30%3A00%2B09%3A00"
        "&weightSnapshotCount=1&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00"
        "&weightSnapshotHash=" + "b" * 64
    )
    assert isinstance(result, PredictParams)
    assert result.race_bango == "01"


def test_parse_predict_params_scope_absent_is_none() -> None:
    result = parse_predict_params("category=nar&runDate=20260619")
    assert isinstance(result, PredictParams)
    assert result.keibajo_code is None
    assert result.race_bango is None


def test_parse_predict_params_keibajo_code_blank_is_none() -> None:
    result = parse_predict_params("category=nar&runDate=20260619&keibajoCode=")
    assert isinstance(result, PredictParams)
    assert result.keibajo_code is None


def test_parse_predict_params_race_bango_blank_is_none() -> None:
    result = parse_predict_params("category=nar&runDate=20260619&raceBango=")
    assert isinstance(result, PredictParams)
    assert result.race_bango is None


def test_parse_predict_params_keibajo_code_only() -> None:
    result = parse_predict_params("category=nar&runDate=20260619&keibajoCode=30")
    assert isinstance(result, PredictParams)
    assert result.keibajo_code == "30"
    assert result.race_bango is None


def test_parse_predict_params_scope_whitespace_is_none() -> None:
    result = parse_predict_params("category=nar&runDate=20260619&raceBango=%20%20")
    assert isinstance(result, PredictParams)
    assert result.race_bango is None


def test_parse_predict_params_card_max_race_bango_absent_is_none() -> None:
    result = parse_predict_params("category=nar&runDate=20260619&keibajoCode=54&raceBango=10")
    assert isinstance(result, PredictParams)
    assert result.card_max_race_bango is None


def test_parse_predict_params_card_max_race_bango_parsed() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260619&keibajoCode=54&raceBango=10&cardMaxRaceBango=10"
    )
    assert isinstance(result, PredictParams)
    assert result.card_max_race_bango == 10


def test_parse_predict_params_card_max_race_bango_non_integer() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260619&keibajoCode=54&raceBango=10&cardMaxRaceBango=abc"
    )
    assert isinstance(result, str)
    assert "cardMaxRaceBango" in result


def test_parse_predict_params_card_max_race_bango_negative() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260619&keibajoCode=54&raceBango=10&cardMaxRaceBango=-1"
    )
    assert isinstance(result, str)
    assert "cardMaxRaceBango" in result


def test_predict_params_scope_stored_correctly() -> None:
    params = PredictParams(
        category="nar",
        run_date="20260619",
        days_ahead=0,
        mode="rescore",
        keibajo_code="44",
        race_bango="01",
    )
    assert params.keibajo_code == "44"
    assert params.race_bango == "01"


# ---------------------------------------------------------------------------
# build_r2_feat_cache_key
# ---------------------------------------------------------------------------


def test_build_r2_feat_cache_key_format() -> None:
    key = build_r2_feat_cache_key("jra", "20260619")
    assert key == "feat-cache/catalog-v1/jra/20260619/features.parquet"


def test_build_r2_feat_cache_key_nar() -> None:
    key = build_r2_feat_cache_key("nar", "20260101")
    assert key == "feat-cache/catalog-v1/nar/20260101/features.parquet"


def test_build_r2_feat_cache_key_banei() -> None:
    key = build_r2_feat_cache_key("ban-ei", "20260619")
    assert key == "feat-cache/catalog-v1/ban-ei/20260619/features.parquet"


def test_build_r2_feat_cache_key_deterministic() -> None:
    """Same inputs must produce the same key (idempotent cache update)."""
    key1 = build_r2_feat_cache_key("jra", "20260619")
    key2 = build_r2_feat_cache_key("jra", "20260619")
    assert key1 == key2


# ---------------------------------------------------------------------------
# build_r2_per_race_feat_cache_key
# ---------------------------------------------------------------------------


def test_build_r2_per_race_feat_cache_key_format() -> None:
    key = build_r2_per_race_feat_cache_key("jra", "20260619", "05", "09")
    assert key == "feat-cache/catalog-v1/jra/20260619/05/09/features.parquet"


def test_build_r2_per_race_feat_cache_key_nar() -> None:
    key = build_r2_per_race_feat_cache_key("nar", "20260101", "30", "11")
    assert key == "feat-cache/catalog-v1/nar/20260101/30/11/features.parquet"


def test_build_r2_per_race_feat_cache_key_banei() -> None:
    key = build_r2_per_race_feat_cache_key("ban-ei", "20260619", "83", "01")
    assert key == "feat-cache/catalog-v1/ban-ei/20260619/83/01/features.parquet"


def test_build_r2_per_race_feat_cache_key_deterministic() -> None:
    """Same inputs must produce the same per-race key (idempotent cache update)."""
    key1 = build_r2_per_race_feat_cache_key("jra", "20260619", "05", "09")
    key2 = build_r2_per_race_feat_cache_key("jra", "20260619", "05", "09")
    assert key1 == key2


def test_build_r2_per_race_feat_cache_key_zero_pads_unpadded_codes() -> None:
    """Unpadded Worker codes must land on the same key as zero-padded race_id parts."""
    assert build_r2_per_race_feat_cache_key("jra", "20260619", "5", "9") == (
        "feat-cache/catalog-v1/jra/20260619/05/09/features.parquet"
    )


# ---------------------------------------------------------------------------
# build_r2_day_base_key
# ---------------------------------------------------------------------------


def test_build_prewarm_cache_key_is_category_and_date() -> None:
    assert build_prewarm_cache_key("ban-ei", "20260816") == "prewarm:ban-ei:20260816"


def test_parse_day_base_cache_identity_reads_category_and_date() -> None:
    assert parse_day_base_cache_identity(
        "feat-daybase/catalog-v1/jra/20260712/features.parquet"
    ) == ("jra", "20260712")


def test_parse_day_base_cache_identity_reads_banei_category() -> None:
    assert parse_day_base_cache_identity(
        "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet"
    ) == ("ban-ei", "20260816")


def test_parse_day_base_cache_identity_rejects_short_key() -> None:
    with pytest.raises(RuntimeError, match="invalid day-base parquet key"):
        parse_day_base_cache_identity("feat-daybase/catalog-v1/jra")


def test_build_r2_day_base_key_format() -> None:
    key = build_r2_day_base_key("jra", "20260712")
    assert key == "feat-daybase/catalog-v1/jra/20260712/features.parquet"


def test_build_r2_running_style_foundation_key_format() -> None:
    assert build_r2_running_style_foundation_key("jra", "20260822") == (
        "feat-running-style-base/catalog-v1/jra/20260822/features.parquet"
    )


def test_build_r2_day_base_key_nar() -> None:
    key = build_r2_day_base_key("nar", "20260101")
    assert key == "feat-daybase/catalog-v1/nar/20260101/features.parquet"


def test_build_r2_day_base_key_banei() -> None:
    key = build_r2_day_base_key("ban-ei", "20260712")
    assert key == "feat-daybase/catalog-v1/ban-ei/20260712/features.parquet"


def test_build_r2_day_base_key_deterministic() -> None:
    key1 = build_r2_day_base_key("jra", "20260712")
    key2 = build_r2_day_base_key("jra", "20260712")
    assert key1 == key2


def test_build_r2_day_base_key_distinct_namespace_from_feat_cache() -> None:
    """The day-base key must never collide with the full-pipeline feat-cache key
    for the same category+date -- a prior design explicitly rejected merging
    cache granularities (see the R2_DAY_BASE_PREFIX docstring)."""
    day_base_key = build_r2_day_base_key("jra", "20260712")
    feat_cache_key = build_r2_feat_cache_key("jra", "20260712")
    assert day_base_key != feat_cache_key
    assert day_base_key.startswith("feat-daybase/")
    assert feat_cache_key.startswith("feat-cache/")


# ---------------------------------------------------------------------------
# R2Config dataclass
# ---------------------------------------------------------------------------


def test_r2_config_fields() -> None:
    cfg = R2Config(
        account_id="abc123",
        access_key_id="KEY",
        secret_access_key="SECRET",
        bucket="my-bucket",
    )
    assert cfg.account_id == "abc123"
    assert cfg.access_key_id == "KEY"
    assert cfg.secret_access_key == "SECRET"
    assert cfg.bucket == "my-bucket"


def test_r2_config_is_frozen() -> None:
    import dataclasses

    cfg = R2Config(
        account_id="abc",
        access_key_id="k",
        secret_access_key="s",
        bucket="b",
    )
    # Retrieve the field name dynamically so ruff B010 (constant-attr setattr)
    # is not triggered while still exercising the frozen-dataclass runtime guard.
    field_name: str = dataclasses.fields(cfg)[0].name
    with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
        # Calling setattr with a *variable* field name avoids ruff B010 (which
        # only flags constant-literal attribute names) while still triggering
        # the frozen-dataclass __setattr__ guard at runtime.
        setattr(cfg, field_name, "other")


# ---------------------------------------------------------------------------
# CacheMissError
# ---------------------------------------------------------------------------


def test_cache_miss_error_is_exception() -> None:
    err = CacheMissError("no cache found")
    assert isinstance(err, Exception)
    assert "no cache found" in str(err)


def test_cache_miss_error_can_be_raised_and_caught() -> None:
    with pytest.raises(CacheMissError, match="miss"):
        raise CacheMissError("cache miss for jra/20260619")


# ---------------------------------------------------------------------------
# iter_predict_chunks — mode=full (default, no rescore_fn)
# ---------------------------------------------------------------------------


def test_iter_predict_chunks_mode_full_default_calls_predict_fn() -> None:
    called = [False]

    def _fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        called[0] = True
        return 5

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(iter_predict_chunks(params, _fn, sleep_fn=_noop_sleep))
    assert called[0]
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 5


# ---------------------------------------------------------------------------
# iter_predict_chunks — mode=rescore with rescore_fn (success)
# ---------------------------------------------------------------------------


def _mock_rescore_ok(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
    card_max_race_bango: int | None = None,
) -> int:
    return 7


def test_iter_predict_chunks_mode_rescore_calls_rescore_fn() -> None:
    """When mode=rescore and rescore_fn succeeds, predict_fn is NOT called."""
    full_called = [False]

    def _full_fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        full_called[0] = True
        return 99  # should not be reached

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(params, _full_fn, rescore_fn=_mock_rescore_ok, sleep_fn=_noop_sleep)
    )
    assert not full_called[0], "predict_fn (full) must not be called when rescore succeeds"
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 7


def test_iter_predict_chunks_mode_rescore_result_has_correct_fields() -> None:
    params = PredictParams(category="nar", run_date="20260619", days_ahead=1, mode="rescore")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, rescore_fn=_mock_rescore_ok, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["category"] == "nar"
    assert last["runDate"] == "20260619"
    assert last["status"] == "success"


# ---------------------------------------------------------------------------
# iter_predict_chunks — mode=rescore CacheMissError fallback to full
# ---------------------------------------------------------------------------


def _mock_rescore_cache_miss(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
    card_max_race_bango: int | None = None,
) -> int:
    raise CacheMissError(f"no cache for {category}/{run_date}")


def test_iter_predict_chunks_rescore_cache_miss_falls_back_to_full() -> None:
    """CacheMissError from rescore_fn must trigger fallback to predict_fn."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, rescore_fn=_mock_rescore_cache_miss, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 42  # from _mock_predict_ok


def test_iter_predict_chunks_rescore_cache_miss_emits_fallback_progress() -> None:
    """A rescore-fallback-to-full progress line must be emitted on CacheMissError."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, rescore_fn=_mock_rescore_cache_miss, sleep_fn=_noop_sleep
        )
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "rescore-fallback-to-full" in stages


def test_iter_predict_chunks_attested_rescore_cache_miss_fails_closed() -> None:
    """A Worker-attested production miss must return to Queue without rebuilding."""
    full_calls: list[str] = []

    def _full_fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        del run_date, days_ahead, keibajo_code, race_bango, card_max_race_bango
        full_calls.append(category)
        return 1

    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="rescore",
        keibajo_code="05",
        race_bango="09",
        rescore_cache_attestation=RescoreCacheAttestation(
            entry_set_hash="a" * 64,
            entry_count=8,
            feature_cache_etag="etag-1",
            feature_cache_version="version-1",
            issued_at_ms=0,
        ),
    )
    chunks = list(
        iter_predict_chunks(
            params, _full_fn, rescore_fn=_mock_rescore_cache_miss, sleep_fn=_noop_sleep
        )
    )
    parsed = [json.loads(chunk.decode()) for chunk in chunks]

    assert full_calls == []
    assert all(line.get("stage") != "rescore-fallback-to-full" for line in parsed)
    assert parsed[-1]["status"] == "error"
    assert "CacheMissError" in parsed[-1]["error"]


def test_iter_predict_chunks_rescore_no_rescore_fn_falls_back_to_full() -> None:
    """When mode=rescore but rescore_fn=None, must fall back to predict_fn."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(params, _mock_predict_ok, rescore_fn=None, sleep_fn=_noop_sleep)
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 42


def test_iter_predict_chunks_rescore_no_fn_emits_fallback_progress() -> None:
    """When mode=rescore with no rescore_fn, fallback progress must be emitted."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep)
    )  # no rescore_fn
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "rescore-fallback-to-full" in stages


def test_iter_predict_chunks_rescore_non_cache_miss_propagates_as_error() -> None:
    """Non-CacheMissError exceptions from rescore_fn must yield an error result."""

    def _rescore_runtime_err(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        raise RuntimeError("unexpected DB error")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, rescore_fn=_rescore_runtime_err, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert "RuntimeError" in last["error"]


def test_iter_predict_chunks_attestation_failure_does_not_call_full_fn() -> None:
    full_calls: list[str] = []

    def _full_fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        del run_date, days_ahead, keibajo_code, race_bango, card_max_race_bango
        full_calls.append(category)
        return 1

    def _rejected_rescore(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        del category, run_date, days_ahead, keibajo_code, race_bango, card_max_race_bango
        raise CacheValidationError("rescore cache attestation rejected: identity-mismatch")

    params = PredictParams("jra", "20260823", 0, mode="rescore", keibajo_code="07", race_bango="03")
    chunks = list(
        iter_predict_chunks(params, _full_fn, rescore_fn=_rejected_rescore, sleep_fn=_noop_sleep)
    )
    parsed = [json.loads(chunk.decode()) for chunk in chunks]

    assert full_calls == []
    assert all(line.get("stage") != "rescore-fallback-to-full" for line in parsed)
    assert parsed[-1]["status"] == "error"
    assert "CacheValidationError" in parsed[-1]["error"]


def test_iter_predict_chunks_scoped_rescore_cache_miss_sets_fallback_flag() -> None:
    """Scoped CacheMiss fallback must mark the predict_fn call as scoped recovery."""
    seen: list[bool] = []

    def _full_fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        seen.append(is_scoped_rescore_cache_miss_fallback())
        return 1

    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="rescore",
        keibajo_code="05",
        race_bango="09",
    )
    chunks = list(
        iter_predict_chunks(
            params, _full_fn, rescore_fn=_mock_rescore_cache_miss, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert seen == [True]
    assert is_scoped_rescore_cache_miss_fallback() is False


def test_iter_predict_chunks_unscoped_rescore_cache_miss_does_not_set_fallback_flag() -> None:
    """Whole-day CacheMiss fallback must keep day-base split available."""
    seen: list[bool] = []

    def _full_fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        seen.append(is_scoped_rescore_cache_miss_fallback())
        return 1

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    list(
        iter_predict_chunks(
            params, _full_fn, rescore_fn=_mock_rescore_cache_miss, sleep_fn=_noop_sleep
        )
    )
    assert seen == [False]
    assert is_scoped_rescore_cache_miss_fallback() is False


def test_activate_scoped_rescore_cache_miss_fallback_resets_after_block() -> None:
    assert is_scoped_rescore_cache_miss_fallback() is False
    with activate_scoped_rescore_cache_miss_fallback():
        assert is_scoped_rescore_cache_miss_fallback() is True
    assert is_scoped_rescore_cache_miss_fallback() is False


def test_iter_predict_chunks_rescore_non_cache_miss_does_not_call_full_fn() -> None:
    """Non-CacheMissError from rescore_fn must NOT fall back to predict_fn."""
    full_called = [False]

    def _full_fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        full_called[0] = True
        return 99

    def _rescore_runtime_err(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        raise RuntimeError("unexpected")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    list(
        iter_predict_chunks(params, _full_fn, rescore_fn=_rescore_runtime_err, sleep_fn=_noop_sleep)
    )
    assert not full_called[0]


# ---------------------------------------------------------------------------
# threading behaviour: verify via iter_predict_chunks (no private imports)
# ---------------------------------------------------------------------------


def test_threaded_predict_fn_result_is_returned_in_success_line() -> None:
    """The return value of predict_fn run in a thread must appear in the result line."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["racesPredicted"] == 42


def test_threaded_predict_fn_exception_surfaces_as_error_result() -> None:
    """An exception raised by predict_fn in its thread must yield an error result line."""

    def _raise(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        raise ValueError("thread boom")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _raise, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert "ValueError" in last["error"]


def test_threaded_predict_fn_does_not_block_generator_indefinitely() -> None:
    """Verify the generator terminates (background thread does not block main thread forever)."""
    done = threading.Event()
    done.set()  # immediately unblocked

    def _unblocked(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        done.wait()
        return 7

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _unblocked, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 7


# ---------------------------------------------------------------------------
# iter_predict_chunks — threaded keepalive: progress lines during long predict
# ---------------------------------------------------------------------------


def _make_blocking_predict(
    done_event: threading.Event, return_value: int = 55
) -> PredictCategoryFn:
    """Return a predict_fn that blocks until *done_event* is set."""

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        done_event.wait()
        return return_value

    return _predict


def _make_advancing_clock(
    step: float,
    sleep_event: threading.Event | None = None,
) -> tuple[TimeFn, SleepFn, list[float]]:
    """Return a (time_fn, sleep_fn, history) triple for deterministic keepalive tests.

    Each call to *sleep_fn* advances the clock by *step* seconds and optionally
    sets *sleep_event* so the test can synchronise with the generator's poll loop.
    *history* records every value returned by *time_fn* for assertion.
    """
    clock = [0.0]
    history: list[float] = []

    def _time() -> float:
        val = clock[0]
        history.append(val)
        return val

    def _sleep(_: float) -> None:
        clock[0] += step
        if sleep_event is not None:
            sleep_event.set()

    return _time, _sleep, history


def test_iter_predict_chunks_keepalive_emits_progress_during_blocking_predict() -> None:
    """Progress lines must be yielded DURING a long-running predict_fn call.

    Mechanism: a clock that advances by 15 s on each sleep_fn call (> the 10 s
    interval), so two sleep calls produce two keepalive progress yields before
    the predict completes.

    The test unblocks the predict_fn after the generator has yielded at least
    two keepalive progress lines (checked by consuming the generator item by item
    from a background thread so the generator is not suspended indefinitely).
    """
    done = threading.Event()
    predict_fn = _make_blocking_predict(done, return_value=88)

    # Clock advances 15 s per sleep call — well above the 10 s interval.
    time_fn, sleep_fn, _ = _make_advancing_clock(step=15.0)

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)

    # Collect chunks in a background thread, unblocking predict after 2 keepalives.
    collected: list[bytes] = []
    progress_count = [0]
    generator_done = threading.Event()

    def _consume() -> None:
        gen = iter_predict_chunks(
            params,
            predict_fn,
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            progress_interval_s=10.0,
        )
        for chunk in gen:
            collected.append(chunk)
            parsed = json.loads(chunk.decode())
            if parsed.get("type") == "progress" and parsed.get("stage") == "predict":
                progress_count[0] += 1
                # Unblock the predict_fn after 2 keepalive lines so the generator
                # terminates rather than looping forever.
                if progress_count[0] >= 2:
                    done.set()
        generator_done.set()

    consumer = threading.Thread(target=_consume, daemon=True)
    consumer.start()
    generator_done.wait(timeout=10.0)
    assert generator_done.is_set(), "generator did not complete within 10 s"

    parsed_all = [json.loads(c.decode()) for c in collected]
    progress_stages = [p.get("stage") for p in parsed_all if p.get("type") == "progress"]

    # At least 2 keepalive "predict" progress lines emitted during the blocking call.
    assert progress_stages.count("predict") >= 2, (
        f"expected >=2 keepalive 'predict' lines, got stages={progress_stages}"
    )
    # Final line is a success result.
    last = parsed_all[-1]
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["racesPredicted"] == 88


# ---------------------------------------------------------------------------
# build_result_line — parquet proxy fields
# ---------------------------------------------------------------------------


def test_build_result_line_with_parquet_fields() -> None:
    """When parquet_base64 and parquet_key are provided, they appear in the result."""
    line = build_result_line(
        "nar",
        "20260619",
        8,
        status="success",
        parquet_base64="dGVzdA==",
        parquet_key="feat-cache/nar/20260619/features.parquet",
    )
    parsed = json.loads(line.decode())
    assert parsed["parquetBase64"] == "dGVzdA=="
    assert parsed["parquetKey"] == "feat-cache/nar/20260619/features.parquet"


def test_build_result_line_without_parquet_fields() -> None:
    """When parquet fields are absent, the result line must not include them."""
    line = build_result_line("jra", "20260619", 5, status="success")
    parsed = json.loads(line.decode())
    assert "parquetBase64" not in parsed
    assert "parquetKey" not in parsed


def test_build_result_line_parquet_key_only_excluded() -> None:
    """When only parquet_key is set (no base64), the field must still be absent."""
    key = "feat-cache/jra/20260619/features.parquet"
    line = build_result_line("jra", "20260619", 5, status="success", parquet_key=key)
    parsed = json.loads(line.decode())
    assert "parquetBase64" not in parsed


def test_build_result_line_with_per_race_parquets() -> None:
    """When per_race_parquets is provided, it appears in the result as perRaceParquets."""
    per_race = [
        {
            "parquetBase64": "dGVzdA==",
            "parquetKey": "feat-cache/jra/20260619/05/09/features.parquet",
        },
        {
            "parquetBase64": "Zm9vYg==",
            "parquetKey": "feat-cache/jra/20260619/05/10/features.parquet",
        },
    ]
    line = build_result_line("jra", "20260619", 2, status="success", per_race_parquets=per_race)
    parsed = json.loads(line.decode())
    assert parsed["perRaceParquets"] == per_race


def test_build_result_line_per_race_parquets_empty_list_included() -> None:
    """An explicit empty list is still included (distinguishes 'split produced 0' from None)."""
    line = build_result_line("nar", "20260619", 0, status="success", per_race_parquets=[])
    parsed = json.loads(line.decode())
    assert parsed["perRaceParquets"] == []


def test_build_result_line_without_per_race_parquets() -> None:
    """When per_race_parquets is absent, the result line must not include the field."""
    line = build_result_line("jra", "20260619", 5, status="success")
    parsed = json.loads(line.decode())
    assert "perRaceParquets" not in parsed


# ---------------------------------------------------------------------------
# iter_predict_chunks — parquet_payload_fn injection
# ---------------------------------------------------------------------------


def test_iter_predict_chunks_full_mode_calls_parquet_payload_fn() -> None:
    """On mode=full success, parquet_payload_fn must be called and embedded in result."""
    called = [False]

    def _parquet_payload() -> tuple[str, str] | None:
        called[0] = True
        return "dGVzdA==", "feat-cache/nar/20260619/features.parquet"

    params = PredictParams(category="nar", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, parquet_payload_fn=_parquet_payload, sleep_fn=_noop_sleep
        )
    )
    assert called[0]
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last.get("parquetBase64") == "dGVzdA=="
    assert last.get("parquetKey") == "feat-cache/nar/20260619/features.parquet"


def test_iter_predict_chunks_rescore_mode_does_not_call_parquet_payload_fn() -> None:
    """On mode=rescore, parquet_payload_fn must NOT be called (only called for full)."""
    called = [False]

    def _parquet_payload() -> tuple[str, str] | None:
        called[0] = True
        return "dGVzdA==", "key"

    params = PredictParams(category="nar", run_date="20260619", days_ahead=0, mode="rescore")
    list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            rescore_fn=_mock_rescore_ok,
            parquet_payload_fn=_parquet_payload,
            sleep_fn=_noop_sleep,
        )
    )
    assert not called[0]


def test_iter_predict_chunks_parquet_payload_fn_error_swallowed() -> None:
    """An exception from parquet_payload_fn must not block the success result."""

    def _failing_payload() -> tuple[str, str] | None:
        raise RuntimeError("disk read failed")

    params = PredictParams(category="nar", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, parquet_payload_fn=_failing_payload, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert "parquetBase64" not in last


def test_iter_predict_chunks_parquet_payload_fn_none_result() -> None:
    """When parquet_payload_fn returns None, no parquet fields appear in result."""

    def _no_parquet() -> tuple[str, str] | None:
        return None

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, parquet_payload_fn=_no_parquet, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert "parquetBase64" not in last
    assert "parquetKey" not in last


def test_iter_predict_chunks_no_parquet_payload_fn_no_fields() -> None:
    """When parquet_payload_fn is not provided (None), result has no parquet fields."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert "parquetBase64" not in last


# ---------------------------------------------------------------------------
# iter_predict_chunks — per_race_parquet_payload_fn injection
# ---------------------------------------------------------------------------


def test_iter_predict_chunks_full_mode_calls_per_race_payload_fn() -> None:
    """On mode=full success, per_race_parquet_payload_fn must be called and embedded."""
    called = [False]
    per_race = [
        {
            "parquetBase64": "dGVzdA==",
            "parquetKey": "feat-cache/nar/20260619/30/11/features.parquet",
        }
    ]

    def _per_race_payload() -> list[dict[str, str]] | None:
        called[0] = True
        return per_race

    params = PredictParams(category="nar", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            per_race_parquet_payload_fn=_per_race_payload,
            sleep_fn=_noop_sleep,
        )
    )
    assert called[0]
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last.get("perRaceParquets") == per_race


def test_iter_predict_chunks_scoped_rescore_cache_miss_skips_per_race_payload() -> None:
    """Scoped CacheMiss fallback must not embed perRaceParquets (dead JRA pedigree)."""
    called = [False]

    def _per_race_payload() -> list[dict[str, str]] | None:
        called[0] = True
        return [
            {
                "parquetBase64": "ZGVnZW5lcmF0ZQ==",
                "parquetKey": "feat-cache/catalog-v1/jra/20260619/05/01/features.parquet",
            }
        ]

    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="rescore",
        keibajo_code="05",
        race_bango="01",
    )
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            rescore_fn=_mock_rescore_cache_miss,
            per_race_parquet_payload_fn=_per_race_payload,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert called[0] is False
    assert "perRaceParquets" not in last


def test_iter_predict_chunks_rescore_mode_calls_per_race_payload_fn() -> None:
    """On mode=rescore, per_race_parquet_payload_fn must be called (both modes)."""
    called = [False]

    def _per_race_payload() -> list[dict[str, str]] | None:
        called[0] = True
        return []

    params = PredictParams(category="nar", run_date="20260619", days_ahead=0, mode="rescore")
    list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            rescore_fn=_mock_rescore_ok,
            per_race_parquet_payload_fn=_per_race_payload,
            sleep_fn=_noop_sleep,
        )
    )
    assert called[0]


def test_iter_predict_chunks_rescore_mode_per_race_payload_embedded_in_result() -> None:
    """On mode=rescore success, per_race_parquet_payload_fn result is embedded."""
    per_race = [
        {
            "parquetBase64": "cmVzY29yZQ==",
            "parquetKey": "feat-cache/jra/20260619/05/01/features.parquet",
        }
    ]

    def _per_race_payload() -> list[dict[str, str]] | None:
        return per_race

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            rescore_fn=_mock_rescore_ok,
            per_race_parquet_payload_fn=_per_race_payload,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last.get("perRaceParquets") == per_race


def test_iter_predict_chunks_per_race_payload_fn_error_swallowed() -> None:
    """An exception from per_race_parquet_payload_fn must not block the success result."""

    def _failing_payload() -> list[dict[str, str]] | None:
        raise RuntimeError("duckdb split failed")

    params = PredictParams(category="nar", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            per_race_parquet_payload_fn=_failing_payload,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert "perRaceParquets" not in last


def test_iter_predict_chunks_per_race_payload_fn_none_result() -> None:
    """When per_race_parquet_payload_fn returns None, no perRaceParquets field appears."""

    def _no_per_race() -> list[dict[str, str]] | None:
        return None

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, per_race_parquet_payload_fn=_no_per_race, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert "perRaceParquets" not in last


def test_iter_predict_chunks_per_race_payload_fn_empty_list_embedded() -> None:
    """An empty per-race list is still embedded (distinguishes 0 races from None)."""

    def _empty_per_race() -> list[dict[str, str]] | None:
        return []

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            per_race_parquet_payload_fn=_empty_per_race,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["perRaceParquets"] == []


def test_iter_predict_chunks_no_per_race_payload_fn_no_field() -> None:
    """When per_race_parquet_payload_fn is not provided (None), no perRaceParquets field."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert "perRaceParquets" not in last


def test_iter_predict_chunks_result_after_keepalive_has_correct_races_predicted() -> None:
    """racesPredicted in the result line must reflect what predict_fn returned."""
    done = threading.Event()
    predict_fn = _make_blocking_predict(done, return_value=123)
    time_fn, sleep_fn, _ = _make_advancing_clock(step=15.0)
    params = PredictParams(category="nar", run_date="20260619", days_ahead=0)

    collected: list[bytes] = []
    progress_count = [0]
    generator_done = threading.Event()

    def _consume() -> None:
        gen = iter_predict_chunks(
            params,
            predict_fn,
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            progress_interval_s=10.0,
        )
        for chunk in gen:
            collected.append(chunk)
            parsed = json.loads(chunk.decode())
            if parsed.get("type") == "progress" and parsed.get("stage") == "predict":
                progress_count[0] += 1
                if progress_count[0] >= 1:
                    done.set()
        generator_done.set()

    consumer = threading.Thread(target=_consume, daemon=True)
    consumer.start()
    generator_done.wait(timeout=10.0)

    last = json.loads(collected[-1].decode())
    assert last["racesPredicted"] == 123
    assert last["status"] == "success"


def test_iter_predict_chunks_keepalive_exception_in_thread_yields_error_result() -> None:
    """An exception in the background predict thread must yield an error result.

    Uses an Event-controlled fn that raises after being unblocked.
    """
    done = threading.Event()

    def _predict_raise(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        done.wait()
        raise RuntimeError("thread crash: postgresql://user:pw@host/db")

    time_fn, sleep_fn, _ = _make_advancing_clock(step=15.0)
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)

    collected: list[bytes] = []
    progress_count = [0]
    generator_done = threading.Event()

    def _consume() -> None:
        gen = iter_predict_chunks(
            params,
            _predict_raise,
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            progress_interval_s=10.0,
        )
        for chunk in gen:
            collected.append(chunk)
            parsed = json.loads(chunk.decode())
            if parsed.get("type") == "progress" and parsed.get("stage") == "predict":
                progress_count[0] += 1
                if progress_count[0] >= 1:
                    done.set()
        generator_done.set()

    consumer = threading.Thread(target=_consume, daemon=True)
    consumer.start()
    generator_done.wait(timeout=10.0)
    assert generator_done.is_set()

    last = json.loads(collected[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "error"
    assert "RuntimeError" in last["error"]
    # Credentials must be masked in the threaded error path too.
    assert b"pw" not in collected[-1]
    assert b"[REDACTED]" in collected[-1]


def test_iter_predict_chunks_keepalive_rescore_cache_miss_fallback_with_progress() -> None:
    """CacheMissError from rescore_fn in thread triggers fallback + keepalive in fallback phase."""
    rescore_done = threading.Event()
    predict_done = threading.Event()

    def _rescore_miss(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        rescore_done.wait()
        raise CacheMissError("no cache")

    def _predict_full(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        predict_done.wait()
        return 77

    time_fn, sleep_fn, _ = _make_advancing_clock(step=15.0)
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")

    collected: list[bytes] = []
    rescore_progress_seen = [0]
    fallback_seen = [False]
    fallback_predict_progress_seen = [0]
    generator_done = threading.Event()

    def _consume() -> None:
        gen = iter_predict_chunks(
            params,
            _predict_full,
            rescore_fn=_rescore_miss,
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            progress_interval_s=10.0,
        )
        for chunk in gen:
            collected.append(chunk)
            parsed = json.loads(chunk.decode())
            if parsed.get("type") == "progress":
                stage = parsed.get("stage")
                if stage == "predict" and not fallback_seen[0]:
                    rescore_progress_seen[0] += 1
                    if rescore_progress_seen[0] >= 1:
                        rescore_done.set()
                elif stage == "rescore-fallback-to-full":
                    fallback_seen[0] = True
                elif stage == "predict" and fallback_seen[0]:
                    fallback_predict_progress_seen[0] += 1
                    if fallback_predict_progress_seen[0] >= 1:
                        predict_done.set()
        generator_done.set()

    consumer = threading.Thread(target=_consume, daemon=True)
    consumer.start()
    generator_done.wait(timeout=15.0)
    assert generator_done.is_set()

    parsed_all = [json.loads(c.decode()) for c in collected]
    stages = [p.get("stage") for p in parsed_all if p.get("type") == "progress"]
    assert "rescore-fallback-to-full" in stages

    last = parsed_all[-1]
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["racesPredicted"] == 77


def test_iter_predict_chunks_keepalive_fallback_exception_yields_error() -> None:
    """When the fallback predict_fn raises after CacheMissError, yield an error result."""
    done = threading.Event()

    def _rescore_miss(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        raise CacheMissError("miss")

    def _predict_raise(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        done.wait()
        raise RuntimeError("fallback exploded")

    time_fn, sleep_fn, _ = _make_advancing_clock(step=15.0)
    params = PredictParams(category="nar", run_date="20260619", days_ahead=0, mode="rescore")

    collected: list[bytes] = []
    fallback_predict_seen = [0]
    generator_done = threading.Event()

    def _consume() -> None:
        gen = iter_predict_chunks(
            params,
            _predict_raise,
            rescore_fn=_rescore_miss,
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            progress_interval_s=10.0,
        )
        for chunk in gen:
            collected.append(chunk)
            parsed = json.loads(chunk.decode())
            if parsed.get("type") == "progress" and parsed.get("stage") == "predict":
                fallback_predict_seen[0] += 1
                if fallback_predict_seen[0] >= 1:
                    done.set()
        generator_done.set()

    consumer = threading.Thread(target=_consume, daemon=True)
    consumer.start()
    generator_done.wait(timeout=10.0)
    assert generator_done.is_set()

    last = json.loads(collected[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "error"
    assert "RuntimeError" in last["error"]


# ---------------------------------------------------------------------------
# has_single_race_scope / is_focused_full_request
# ---------------------------------------------------------------------------


def test_has_single_race_scope_true_when_both_fields_set() -> None:
    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="rescore",
        keibajo_code="05",
        race_bango="09",
    )
    assert has_single_race_scope(params) is True


def test_has_single_race_scope_false_when_either_field_missing() -> None:
    only_keibajo = PredictParams(
        category="jra", run_date="20260619", days_ahead=0, keibajo_code="05"
    )
    only_bango = PredictParams(category="jra", run_date="20260619", days_ahead=0, race_bango="09")
    neither = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    assert has_single_race_scope(only_keibajo) is False
    assert has_single_race_scope(only_bango) is False
    assert has_single_race_scope(neither) is False


def test_is_focused_full_request_true_for_full_with_both_scope_fields() -> None:
    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="full",
        keibajo_code="05",
        race_bango="09",
    )
    assert is_focused_full_request(params) is True


def test_is_focused_full_request_false_for_rescore_with_both_scope_fields() -> None:
    """mode=rescore is never treated as a focused-full guarded request,
    even when both race-scope fields are present."""
    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="rescore",
        keibajo_code="05",
        race_bango="09",
    )
    assert is_focused_full_request(params) is False


def test_is_focused_full_request_false_for_full_with_no_scope() -> None:
    """A day/category batch request (mode=full, no scope) is not focused-full."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    assert is_focused_full_request(params) is False


def test_is_focused_full_request_false_for_full_with_only_keibajo_code() -> None:
    params = PredictParams(
        category="jra", run_date="20260619", days_ahead=0, mode="full", keibajo_code="05"
    )
    assert is_focused_full_request(params) is False


def test_is_focused_full_request_false_for_full_with_only_race_bango() -> None:
    params = PredictParams(
        category="jra", run_date="20260619", days_ahead=0, mode="full", race_bango="09"
    )
    assert is_focused_full_request(params) is False


# ---------------------------------------------------------------------------
# build_focused_full_race_key
# ---------------------------------------------------------------------------


def testbuild_focused_full_race_key_format() -> None:
    params = PredictParams(
        category="nar",
        run_date="20260619",
        days_ahead=0,
        mode="full",
        keibajo_code="30",
        race_bango="11",
    )
    assert build_focused_full_race_key(params) == "nar:20260619:30:11"


def testbuild_focused_full_race_key_differs_per_race() -> None:
    """Two different races in the same category/date must yield distinct keys
    (identity token only -- the guard slot itself is single-process-wide)."""
    base = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="full",
        keibajo_code="05",
        race_bango="01",
    )
    other = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="full",
        keibajo_code="05",
        race_bango="02",
    )
    assert build_focused_full_race_key(base) != build_focused_full_race_key(other)


# ---------------------------------------------------------------------------
# iter_predict_chunks — focused per-race full guarded keepalive dispatch
# ---------------------------------------------------------------------------


def _make_focused_full_params(
    keibajo_code: str = "05", race_bango: str = "09", force: bool = False
) -> PredictParams:
    return PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="full",
        keibajo_code=keibajo_code,
        race_bango=race_bango,
        force=force,
    )


def test_iter_predict_chunks_focused_full_claimed_returns_accepted_and_detaches() -> None:
    """A claimed focused per-race full request starts the pipeline and returns accepted."""
    invoked = threading.Event()
    release = threading.Event()

    def _slow_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        invoked.set()
        release.wait(timeout=2.0)
        return 1

    params = _make_focused_full_params()
    chunks = list(
        iter_predict_chunks(
            params,
            _slow_predict,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=lambda _key: None,
            sleep_fn=_noop_sleep,
        )
    )

    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0
    assert last["category"] == "jra"
    assert last["runDate"] == "20260619"
    assert invoked.wait(timeout=2.0), "predict_fn was never invoked"
    focused_lock = vars(serve_module)["_FOCUSED_FULL_LOCK"]
    detached_threads = vars(serve_module)["_DETACHED_FOCUSED_FULL_THREADS"]
    with focused_lock:
        detached = list(detached_threads)
    assert detached and all(not thread.daemon for thread in detached)
    release.set()


def test_focused_full_status_records_running_progress_and_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    started = threading.Event()
    release = threading.Event()

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        started.set()
        release.wait(timeout=2.0)
        return 1

    params = _make_focused_full_params(keibajo_code="05", race_bango="11")
    chunks = list(iter_predict_chunks(params, _predict, focused_full_timeout_seconds=60.0))
    assert json.loads(chunks[-1].decode())["status"] == "accepted"
    assert started.wait(timeout=2.0)

    running = json.loads(build_focused_full_status_response_body(params))
    assert running["raceKey"] == "jra:20260619:05:11"
    assert running["status"] == "running"
    assert running["startedAtMs"] == running["lastProgressAtMs"]
    assert running["deadlineAtMs"] - running["startedAtMs"] == 60000
    assert running["finishedAtMs"] is None
    assert running["error"] is None

    mark_focused_full_progress("jra:20260619:05:11", wall_time_fn=lambda: 1234567.0)
    progressed = json.loads(build_focused_full_status_response_body(params))
    assert progressed["startedAtMs"] == running["startedAtMs"]
    assert progressed["lastProgressAtMs"] == 1234567000
    assert progressed["deadlineAtMs"] == running["deadlineAtMs"]

    release.set()
    deadline = time.monotonic() + 2.0
    terminal = json.loads(build_focused_full_status_response_body(params))
    while terminal["status"] == "running" and time.monotonic() < deadline:
        terminal = json.loads(build_focused_full_status_response_body(params))
    assert terminal["status"] == "success"
    assert terminal["finishedAtMs"] is not None
    assert terminal["error"] is None
    lifecycle = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if '"raceKey":"jra:20260619:05:11"' in line
    ]
    assert [row["status"] for row in lifecycle] == ["accepted", "success"]
    assert lifecycle[0] == {
        "event": "focused-full-lifecycle",
        "raceKey": "jra:20260619:05:11",
        "category": "jra",
        "runDate": "20260619",
        "venue": "05",
        "race": "11",
        "status": "accepted",
        "elapsedMs": 0,
        "error": None,
    }
    assert lifecycle[1]["elapsedMs"] >= 0
    assert lifecycle[1]["error"] is None


def test_focused_full_status_records_masked_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    failed = threading.Event()

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        failed.set()
        raise RuntimeError("postgresql://user:secret@host/db failed")

    params = _make_focused_full_params(keibajo_code="05", race_bango="12")
    chunks = list(iter_predict_chunks(params, _predict))
    assert json.loads(chunks[-1].decode())["status"] == "accepted"
    assert failed.wait(timeout=2.0)
    deadline = time.monotonic() + 2.0
    terminal = json.loads(build_focused_full_status_response_body(params))
    while terminal["status"] == "running" and time.monotonic() < deadline:
        terminal = json.loads(build_focused_full_status_response_body(params))
    assert terminal["status"] == "error"
    assert terminal["error"] == "RuntimeError: postgresql://[REDACTED]@host/db failed"
    assert terminal["finishedAtMs"] is not None
    captured = capsys.readouterr().err
    assert "user:secret" not in captured
    lifecycle = [
        json.loads(line)
        for line in captured.splitlines()
        if '"raceKey":"jra:20260619:05:12"' in line
    ]
    assert [row["status"] for row in lifecycle] == ["accepted", "error"]
    assert lifecycle[1]["error"] == terminal["error"]
    assert lifecycle[1]["elapsedMs"] >= 0


def test_iter_predict_chunks_debug_on_focused_full_holds_and_emits_racechain() -> None:
    drain_debug_progress()
    released: list[str] = []

    def _predict_hit(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        record_debug_progress(
            "step=daybase-hit source=r2 category=nar target_date=20260619 reason=watermark-match"
        )
        record_debug_progress(
            "step=racechain-layer index=1/1 status=start category=nar script=x.py "
            "target_race=83:03 elapsed_seconds=0.000"
        )
        return 1

    params = PredictParams(
        category="nar",
        run_date="20260619",
        days_ahead=0,
        mode="full",
        keibajo_code="83",
        race_bango="03",
        debug_logs=True,
    )
    chunks = list(
        iter_predict_chunks(
            params,
            _predict_hit,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=lambda key: released.append(key),
            sleep_fn=_noop_sleep,
        )
    )
    joined = b"".join(chunks).decode()
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["racesPredicted"] == 1
    assert "step=daybase-hit" in joined
    assert "step=racechain-layer" in joined
    assert "step=daybase-base" not in joined
    assert last["step"] == (
        "step=racechain-layer index=1/1 status=start category=nar script=x.py "
        "target_race=83:03 elapsed_seconds=0.000"
    )
    assert released == ["nar:20260619:83:03"]


def test_iter_predict_chunks_focused_full_claim_busy_skips_launch() -> None:
    """When the single-process guard reports the slot busy (held by a DIFFERENT
    race), predict_fn must never be invoked and the response must report
    status='busy' so the Worker queue consumer re-enqueues a fresh copy."""
    call_count = [0]

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        call_count[0] += 1
        return 1

    params = _make_focused_full_params(keibajo_code="83", race_bango="01")
    chunks = list(
        iter_predict_chunks(
            params,
            _predict,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_BUSY,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_BUSY_STATUS
    assert last["racesPredicted"] == 0
    assert call_count[0] == 0, "predict_fn must not be launched when the slot is busy"


def test_iter_predict_chunks_focused_full_release_called_once_with_race_key() -> None:
    """After the detached pipeline completes, the release fn must receive the race key."""
    release_calls: list[str] = []
    released = threading.Event()

    def _claim(_key: str) -> FocusedFullSlotState:
        return FOCUSED_FULL_SLOT_CLAIMED

    def _release(key: str) -> None:
        release_calls.append(key)
        released.set()

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        return 9

    params = _make_focused_full_params(keibajo_code="30", race_bango="02")
    expected_key = build_focused_full_race_key(params)

    chunks = list(
        iter_predict_chunks(
            params,
            _predict,
            focused_full_claim_fn=_claim,
            focused_full_release_fn=_release,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0

    assert released.wait(timeout=2.0), "release_fn was never called"
    assert release_calls == [expected_key]


def test_iter_predict_chunks_focused_full_release_called_even_on_predict_error() -> None:
    """The release fn must run even when predict_fn raises -- the slot must not
    be left permanently claimed after a failed run."""
    released = threading.Event()

    def _release(key: str) -> None:
        released.set()

    def _predict_raises(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        raise RuntimeError("focused pipeline failed")

    params = _make_focused_full_params(keibajo_code="44", race_bango="04")
    chunks = list(
        iter_predict_chunks(
            params,
            _predict_raises,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=_release,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0
    assert released.wait(timeout=2.0), "release_fn must run even after predict_fn raises"


def test_iter_predict_chunks_focused_full_cache_populate_called_before_release() -> None:
    """A successful claimed run calls focused_full_cache_populate_fn with the
    request's own params, and does so BEFORE release_fn -- see
    FocusedFullCachePopulateFn's docstring for why ordering matters (no other
    pipeline run can start and overwrite shared state while the slot is
    still held)."""
    order: list[str] = []
    populated_with: list[PredictParams] = []
    released = threading.Event()

    def _populate(params: PredictParams) -> None:
        order.append("populate")
        populated_with.append(params)

    def _release(_key: str) -> None:
        order.append("release")
        released.set()

    params = _make_focused_full_params(keibajo_code="55", race_bango="03")
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=_release,
            focused_full_cache_populate_fn=_populate,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS

    assert released.wait(timeout=2.0), "release_fn was never called"
    assert order == ["populate", "release"]
    assert populated_with == [params]


def test_iter_predict_chunks_focused_full_cache_populate_not_called_on_predict_error() -> None:
    """A failed pipeline run must never populate the cache -- there is no
    successful parquet to cache, and populating one anyway would seed a
    stale/wrong object under this race's key."""
    populate_calls: list[PredictParams] = []
    released = threading.Event()

    def _populate(params: PredictParams) -> None:
        populate_calls.append(params)

    def _release(_key: str) -> None:
        released.set()

    def _predict_raises(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        raise RuntimeError("focused pipeline failed")

    params = _make_focused_full_params(keibajo_code="56", race_bango="04")
    chunks = list(
        iter_predict_chunks(
            params,
            _predict_raises,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=_release,
            focused_full_cache_populate_fn=_populate,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS

    assert released.wait(timeout=2.0), "release_fn must still run after predict_fn raises"
    assert populate_calls == []


def test_iter_predict_chunks_focused_full_cache_populate_error_marks_run_failed() -> None:
    """A prediction without its exact-race cache is not terminal success."""
    released = threading.Event()

    def _populate_raises(_params: PredictParams) -> None:
        raise RuntimeError("cache populate blew up")

    def _release(_key: str) -> None:
        released.set()

    params = _make_focused_full_params(keibajo_code="57", race_bango="05")
    chunks = list(
        iter_predict_chunks(
            params,
            _mock_predict_ok,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=_release,
            focused_full_cache_populate_fn=_populate_raises,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0

    assert released.wait(timeout=2.0), "release_fn must still run after populate_fn raises"
    terminal = json.loads(build_focused_full_status_response_body(params))
    assert terminal["status"] == "error"
    assert "cache populate blew up" in terminal["error"]


def test_iter_predict_chunks_focused_full_default_guard_single_slot_enforced() -> None:
    """End-to-end exercise of the REAL module-level single-process guard (no
    focused_full_claim_fn / focused_full_release_fn injected -- i.e. the
    default ``_claim_focused_full_slot`` / ``_release_focused_full_slot``):

    1. A focused-full request for race A starts its detached pipeline
       (predict_fn actually runs) -- the claim succeeds.
    2. While race A's pipeline is still in flight, a focused-full request for
       a DIFFERENT race B in the same category is rejected by the guard --
       predict_fn for B is never invoked and the response reports status='busy'
       -- exercising the "slot held by a different race" branch through the
       public API only (no private-module access).
    3. Once race A's pipeline finishes and releases the slot, a follow-up
       request (race C) can claim it again and return accepted --
       confirming the guard neither wedges permanently nor leaks state for
       later tests.

    This single test is intentionally self-contained (it both claims and
    fully drains the shared process-wide slot before returning) so it has no
    ordering dependency on any other test in this module.
    """
    a_started = threading.Event()
    a_release = threading.Event()
    a_done = threading.Event()
    b_called = threading.Event()
    c_called = threading.Event()
    short_sleep = threading.Event()
    chunks_a: list[bytes] = []
    errors_a: list[BaseException] = []

    def _predict_a(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        a_started.set()
        a_release.wait(timeout=5.0)
        a_done.set()
        return 1

    def _predict_b(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        b_called.set()
        return 2

    def _predict_c(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        c_called.set()
        return 3

    params_a = _make_focused_full_params(keibajo_code="05", race_bango="01")
    params_b = _make_focused_full_params(keibajo_code="05", race_bango="02")
    params_c = _make_focused_full_params(keibajo_code="05", race_bango="03")

    def _sleep_briefly(_: float) -> None:
        short_sleep.wait(timeout=0.001)

    def _consume_a() -> None:
        try:
            chunks_a.extend(iter_predict_chunks(params_a, _predict_a, sleep_fn=_sleep_briefly))
        except BaseException as exc:
            errors_a.append(exc)

    thread_a = threading.Thread(target=_consume_a)
    thread_a.start()

    try:
        assert a_started.wait(timeout=2.0), "race A predict_fn never started"

        chunks_b = list(iter_predict_chunks(params_b, _predict_b, sleep_fn=_noop_sleep))
        assert json.loads(chunks_b[-1].decode())["status"] == FOCUSED_FULL_BUSY_STATUS
        assert not b_called.wait(timeout=0.2), (
            "race B predict_fn must not run while race A's pipeline is still in flight"
        )
    finally:
        # Let race A finish, which releases the slot from the detached worker path.
        a_release.set()
        thread_a.join(timeout=2.0)

    assert not thread_a.is_alive(), "race A accepted response did not finish"
    assert errors_a == []
    assert a_done.wait(timeout=0.1), "race A predict_fn did not finish"
    last_a = json.loads(chunks_a[-1].decode())
    assert last_a["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last_a["racesPredicted"] == 0

    retry_deadline = time.monotonic() + 2.0
    last_c: dict[str, object] = {"status": FOCUSED_FULL_BUSY_STATUS}
    while last_c["status"] == FOCUSED_FULL_BUSY_STATUS and time.monotonic() < retry_deadline:
        chunks_c = list(iter_predict_chunks(params_c, _predict_c, sleep_fn=_noop_sleep))
        last_c = json.loads(chunks_c[-1].decode())
        if last_c["status"] == FOCUSED_FULL_BUSY_STATUS:
            short_sleep.wait(timeout=0.001)
    assert last_c["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last_c["racesPredicted"] == 0
    assert c_called.wait(timeout=2.0), "race C was never able to claim the slot"


def test_iter_predict_chunks_focused_full_default_guard_in_flight_self_no_relaunch() -> None:
    """End-to-end exercise of the REAL module-level guard's "in-flight-self"
    branch (no focused_full_claim_fn / focused_full_release_fn injected -- i.e.
    the default ``_claim_focused_full_slot`` / ``_release_focused_full_slot``):

    1. A focused-full request for race A starts its detached pipeline
       (predict_fn actually runs) -- the claim succeeds.
    2. While race A's pipeline is still in flight, a redelivery of the SAME
       race A (identical race key) with a DIFFERENT predict_fn must NOT launch
       a second pipeline -- the real guard returns "in-flight-self" -- yet the
       response must still report status='accepted' (the caller keeps polling).
    3. Once race A's pipeline finishes and releases the slot, a follow-up
       request (race D) can claim it again and return accepted -- leaving
       the shared process-wide slot clean for other tests.

    Covers the real guard's "in-flight-self" branch through the public API
    only (no private-module access); the "claimed" branch is covered by race A
    launching and the "busy" branch by the sibling default-guard test.
    """
    a_started = threading.Event()
    a_release = threading.Event()
    a_done = threading.Event()
    a2_called = threading.Event()
    d_called = threading.Event()
    short_sleep = threading.Event()
    chunks_a: list[bytes] = []
    errors_a: list[BaseException] = []

    def _predict_a(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        a_started.set()
        a_release.wait(timeout=5.0)
        a_done.set()
        return 1

    def _predict_a2(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        a2_called.set()
        return 2

    def _predict_d(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        d_called.set()
        return 4

    params_a = _make_focused_full_params(keibajo_code="06", race_bango="01")
    params_d = _make_focused_full_params(keibajo_code="06", race_bango="02")

    def _sleep_briefly(_: float) -> None:
        short_sleep.wait(timeout=0.001)

    def _consume_a() -> None:
        try:
            chunks_a.extend(iter_predict_chunks(params_a, _predict_a, sleep_fn=_sleep_briefly))
        except BaseException as exc:
            errors_a.append(exc)

    thread_a = threading.Thread(target=_consume_a)
    thread_a.start()

    try:
        assert a_started.wait(timeout=2.0), "race A predict_fn never started"

        # Redelivery of the SAME race A (same race key) while A's pipeline is in
        # flight: the real guard must report in-flight-self -> no second launch,
        # status still 'accepted'.
        chunks_a2 = list(iter_predict_chunks(params_a, _predict_a2, sleep_fn=_noop_sleep))
        assert json.loads(chunks_a2[-1].decode())["status"] == FOCUSED_FULL_ACCEPTED_STATUS
        assert not a2_called.wait(timeout=0.2), (
            "a redelivery of the same in-flight race must not launch a second pipeline"
        )
    finally:
        # Let race A finish, which releases the slot from the detached worker path.
        a_release.set()
        thread_a.join(timeout=2.0)

    assert not thread_a.is_alive(), "race A accepted response did not finish"
    assert errors_a == []
    assert a_done.wait(timeout=0.1), "race A predict_fn did not finish"
    last_a = json.loads(chunks_a[-1].decode())
    assert last_a["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last_a["racesPredicted"] == 0

    retry_deadline = time.monotonic() + 2.0
    last_d: dict[str, object] = {"status": FOCUSED_FULL_BUSY_STATUS}
    while last_d["status"] == FOCUSED_FULL_BUSY_STATUS and time.monotonic() < retry_deadline:
        chunks_d = list(iter_predict_chunks(params_d, _predict_d, sleep_fn=_noop_sleep))
        last_d = json.loads(chunks_d[-1].decode())
        if last_d["status"] == FOCUSED_FULL_BUSY_STATUS:
            short_sleep.wait(timeout=0.001)
    assert last_d["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last_d["racesPredicted"] == 0
    assert d_called.wait(timeout=2.0), "race D was never able to claim the slot"


def test_iter_predict_chunks_pipeline_exec_lock_serializes_batch_against_focused_full() -> None:
    """Real (non-injected) module-level guards: a focused-full request's
    detached pipeline execution must fully finish before a CONCURRENT batch
    (day-level, non-focused) ``mode=full`` request's predict_fn is allowed to
    run -- even though the focused-full slot bookkeeping
    (``_FOCUSED_FULL_IN_FLIGHT``) never tracks batch requests at all.

    This is ``_PIPELINE_EXEC_LOCK``'s job: every ``PredictCategoryFn``
    invocation funnels through ``_run_predict_fn``, so the two executions can
    never overlap regardless of which path did (or did not) claim the
    focused-full slot. Guards against a regression where
    ``ThreadingHTTPServer`` lets a batch and a focused-full request's
    handlers run on separate threads: without this lock both predict_fn
    bodies could run at the same time and corrupt the SAME category-scoped
    ``pipeline_runner.WORK_DIR`` directories (the batch path writes them
    directly; the focused-full path writes them from its detached thread).
    """
    a_started = threading.Event()
    a_release = threading.Event()
    a_done = threading.Event()
    batch_started = threading.Event()
    batch_done = threading.Event()
    short_sleep = threading.Event()
    chunks_a: list[bytes] = []
    chunks_batch: list[bytes] = []
    errors_a: list[BaseException] = []
    errors_batch: list[BaseException] = []
    order: list[str] = []

    def _predict_a(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        order.append("a-start")
        a_started.set()
        a_release.wait(timeout=5.0)
        order.append("a-end")
        a_done.set()
        return 1

    def _predict_batch(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        order.append("batch-start")
        batch_started.set()
        order.append("batch-end")
        batch_done.set()
        return 9

    params_a = _make_focused_full_params(keibajo_code="07", race_bango="01")
    params_batch = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")

    def _sleep_briefly(_: float) -> None:
        short_sleep.wait(timeout=0.001)

    def _consume_a() -> None:
        try:
            chunks_a.extend(iter_predict_chunks(params_a, _predict_a, sleep_fn=_sleep_briefly))
        except BaseException as exc:
            errors_a.append(exc)

    def _consume_batch() -> None:
        try:
            chunks_batch.extend(
                iter_predict_chunks(params_batch, _predict_batch, sleep_fn=_sleep_briefly)
            )
        except BaseException as exc:
            errors_batch.append(exc)

    thread_a = threading.Thread(target=_consume_a)
    thread_batch = threading.Thread(target=_consume_batch)
    thread_a.start()
    try:
        assert a_started.wait(timeout=2.0), "race A predict_fn never started"

        thread_batch.start()
        try:
            assert not batch_started.wait(timeout=0.3), (
                "batch predict_fn must not start while a focused-full pipeline "
                "holds _PIPELINE_EXEC_LOCK -- it must wait its turn"
            )
        finally:
            a_release.set()
            thread_batch.join(timeout=2.0)
    finally:
        a_release.set()
        thread_a.join(timeout=2.0)

    assert not thread_a.is_alive(), "race A accepted response did not finish"
    assert not thread_batch.is_alive(), "batch request did not finish"
    assert errors_a == []
    assert errors_batch == []
    assert a_done.wait(timeout=0.1), "race A predict_fn did not finish"
    assert batch_done.wait(timeout=0.1), "batch predict_fn did not finish"
    assert order == ["a-start", "a-end", "batch-start", "batch-end"], (
        f"executions overlapped or ran out of order: {order}"
    )
    last_a = json.loads(chunks_a[-1].decode())
    assert last_a["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    last_batch = json.loads(chunks_batch[-1].decode())
    assert last_batch["status"] == "success"
    assert last_batch["racesPredicted"] == 9


def test_iter_predict_chunks_focused_full_does_not_affect_rescore_mode() -> None:
    """mode=rescore with race scope must keep the original blocking behaviour
    (never routed through the focused-full guarded branch)."""
    params = PredictParams(
        category="nar",
        run_date="20260619",
        days_ahead=0,
        mode="rescore",
        keibajo_code="30",
        race_bango="11",
    )
    chunks = list(
        iter_predict_chunks(
            params, _mock_predict_ok, rescore_fn=_mock_rescore_ok, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 7


def test_iter_predict_chunks_focused_full_does_not_affect_batch_full_mode() -> None:
    """mode=full without race scope (day/category batch) must keep the original
    blocking behaviour (never routed through the focused-full branch)."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 42


def test_iter_predict_chunks_focused_full_in_flight_self_skips_second_launch() -> None:
    """When the single-process guard reports the slot already held by THIS SAME
    race key (a redelivery of a race whose own pipeline is still running),
    predict_fn must not be launched again -- yet the response must still report
    status='accepted' (the caller keeps polling for the in-flight pipeline)."""
    call_count = [0]

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        call_count[0] += 1
        return 1

    params = _make_focused_full_params(keibajo_code="13", race_bango="05")
    chunks = list(
        iter_predict_chunks(
            params,
            _predict,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_IN_FLIGHT_SELF,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0
    assert call_count[0] == 0, "predict_fn must not be launched for an in-flight-self redelivery"


def test_iter_predict_chunks_focused_full_already_complete_skips_launch_and_releases() -> None:
    """When the completion fn reports the race already has complete predictions
    in Neon, predict_fn must not be launched, the slot must be released with the
    claimed race key, and the response must report status='already-complete'."""
    call_count = [0]
    release_calls: list[str] = []

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        call_count[0] += 1
        return 1

    def _release(key: str) -> None:
        release_calls.append(key)

    params = _make_focused_full_params(keibajo_code="20", race_bango="06")
    expected_key = build_focused_full_race_key(params)
    chunks = list(
        iter_predict_chunks(
            params,
            _predict,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=_release,
            focused_full_completion_fn=lambda _p: True,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ALREADY_COMPLETE_STATUS
    assert last["racesPredicted"] == 0
    assert call_count[0] == 0, "predict_fn must not be launched when already complete"
    assert release_calls == [expected_key]


def test_iter_predict_chunks_focused_full_force_bypasses_already_complete() -> None:
    """Defect H: when params.force is True, the pipeline must still launch even
    though completion_fn reports the race already complete -- force must
    reach the container's own completion check, not just the Worker's."""
    invoked = threading.Event()

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        invoked.set()
        return 1

    params = _make_focused_full_params(keibajo_code="23", race_bango="09", force=True)
    chunks = list(
        iter_predict_chunks(
            params,
            _predict,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=lambda _key: None,
            focused_full_completion_fn=lambda _p: True,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0
    assert invoked.wait(timeout=2.0), "predict_fn was never invoked when force bypasses completion"


def test_iter_predict_chunks_focused_full_completion_false_runs_pipeline() -> None:
    """When the completion fn reports the race is NOT yet complete, the pipeline
    must still launch and return accepted."""
    invoked = threading.Event()

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        invoked.set()
        return 1

    params = _make_focused_full_params(keibajo_code="21", race_bango="07")
    chunks = list(
        iter_predict_chunks(
            params,
            _predict,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=lambda _key: None,
            focused_full_completion_fn=lambda _p: False,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0
    assert invoked.wait(timeout=2.0), (
        "predict_fn was never invoked when completion fn returns False"
    )


def test_iter_predict_chunks_focused_full_completion_raises_runs_pipeline() -> None:
    """A raising completion fn must be treated as 'not complete' -- the pipeline
    still launches rather than the request silently doing nothing."""
    invoked = threading.Event()

    def _predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        invoked.set()
        return 1

    def _raising_completion(_params: PredictParams) -> bool:
        raise RuntimeError("completion check failed")

    params = _make_focused_full_params(keibajo_code="22", race_bango="08")
    chunks = list(
        iter_predict_chunks(
            params,
            _predict,
            focused_full_claim_fn=lambda _key: FOCUSED_FULL_SLOT_CLAIMED,
            focused_full_release_fn=lambda _key: None,
            focused_full_completion_fn=_raising_completion,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == FOCUSED_FULL_ACCEPTED_STATUS
    assert last["racesPredicted"] == 0
    assert invoked.wait(timeout=2.0), "predict_fn was never invoked when completion fn raises"


# ---------------------------------------------------------------------------
# parse_prewarm_params — success
# ---------------------------------------------------------------------------


def test_parse_prewarm_params_debug_flag_enabled() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&debug=1")
    assert isinstance(result, PrewarmParams)
    assert result.debug_logs is True


def test_parse_prewarm_params_debug_flag_default_false() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712")
    assert isinstance(result, PrewarmParams)
    assert result.debug_logs is False


def test_parse_prewarm_params_debug_flag_invalid_is_off() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&debug=nope")
    assert isinstance(result, PrewarmParams)
    assert result.debug_logs is False


def test_parse_prewarm_params_force_enabled() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&force=1")
    assert isinstance(result, PrewarmParams)
    assert result.force is True


def test_parse_prewarm_params_force_disabled() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&force=0")
    assert isinstance(result, PrewarmParams)
    assert result.force is False


def test_parse_prewarm_params_force_invalid() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&force=true")
    assert result == "invalid force: 'true'; must be 0 or 1"


def test_parse_prewarm_params_rebuild_enabled() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&rebuild=1")
    assert isinstance(result, PrewarmParams)
    assert result.force is False
    assert result.rebuild is True


def test_parse_prewarm_params_rebuild_invalid() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&rebuild=yes")
    assert result == "invalid rebuild: 'yes'; must be 0 or 1"


def test_parse_prewarm_params_rejects_force_with_rebuild() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&force=1&rebuild=1")
    assert result == "force and rebuild cannot both be enabled"


def test_parse_prewarm_params_jra_success() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&daysAhead=0")
    assert isinstance(result, PrewarmParams)
    assert result.category == "jra"
    assert result.run_date == "20260712"
    assert result.days_ahead == 0


def test_parse_prewarm_params_nar_success() -> None:
    result = parse_prewarm_params("category=nar&runDate=20260712&daysAhead=2")
    assert isinstance(result, PrewarmParams)
    assert result.category == "nar"
    assert result.days_ahead == 2


def test_parse_prewarm_params_banei_success() -> None:
    result = parse_prewarm_params("category=ban-ei&runDate=20260712")
    assert isinstance(result, PrewarmParams)
    assert result.category == "ban-ei"
    assert result.days_ahead == 0  # default


def test_parse_prewarm_params_days_ahead_missing_defaults_to_zero() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712")
    assert isinstance(result, PrewarmParams)
    assert result.days_ahead == 0


# ---------------------------------------------------------------------------
# parse_prewarm_params — validation errors
# ---------------------------------------------------------------------------


def test_parse_prewarm_params_missing_category() -> None:
    result = parse_prewarm_params("runDate=20260712&daysAhead=0")
    assert isinstance(result, str)
    assert "category" in result


def test_parse_prewarm_params_invalid_category() -> None:
    result = parse_prewarm_params("category=invalid&runDate=20260712")
    assert isinstance(result, str)
    assert "invalid" in result


def test_parse_prewarm_params_missing_run_date() -> None:
    result = parse_prewarm_params("category=jra&daysAhead=0")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_prewarm_params_invalid_run_date_non_digits() -> None:
    result = parse_prewarm_params("category=jra&runDate=2026-07-12")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_prewarm_params_invalid_run_date_too_short() -> None:
    result = parse_prewarm_params("category=jra&runDate=2026071")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_prewarm_params_invalid_run_date_too_long() -> None:
    result = parse_prewarm_params("category=jra&runDate=202607120")
    assert isinstance(result, str)
    assert "runDate" in result


def test_parse_prewarm_params_days_ahead_non_integer() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&daysAhead=abc")
    assert isinstance(result, str)
    assert "daysAhead" in result


def test_parse_prewarm_params_days_ahead_negative() -> None:
    result = parse_prewarm_params("category=jra&runDate=20260712&daysAhead=-1")
    assert isinstance(result, str)
    assert "daysAhead" in result


def test_parse_prewarm_params_empty_query_string() -> None:
    result = parse_prewarm_params("")
    assert isinstance(result, str)
    assert "category" in result


# ---------------------------------------------------------------------------
# parse_focused_full_cache_query
# ---------------------------------------------------------------------------


def test_parse_focused_full_cache_query_success() -> None:
    result = parse_focused_full_cache_query(
        "category=jra&runDate=20260712&keibajoCode=05&raceBango=09"
    )
    assert isinstance(result, PredictParams)
    assert result.category == "jra"
    assert result.run_date == "20260712"
    assert result.keibajo_code == "05"
    assert result.race_bango == "09"


def test_parse_focused_full_cache_query_missing_category() -> None:
    result = parse_focused_full_cache_query("runDate=20260712&keibajoCode=05&raceBango=09")
    assert result == "missing required parameter: category"


def test_parse_focused_full_cache_query_invalid_category() -> None:
    result = parse_focused_full_cache_query(
        "category=nope&runDate=20260712&keibajoCode=05&raceBango=09"
    )
    assert isinstance(result, str)
    assert "invalid category" in result


def test_parse_focused_full_cache_query_missing_run_date() -> None:
    result = parse_focused_full_cache_query("category=jra&keibajoCode=05&raceBango=09")
    assert result == "missing required parameter: runDate"


def test_parse_focused_full_cache_query_invalid_run_date() -> None:
    result = parse_focused_full_cache_query(
        "category=jra&runDate=notadate&keibajoCode=05&raceBango=09"
    )
    assert isinstance(result, str)
    assert "invalid runDate" in result


def test_parse_focused_full_cache_query_missing_keibajo_code() -> None:
    result = parse_focused_full_cache_query("category=jra&runDate=20260712&raceBango=09")
    assert result == "missing required parameter: keibajoCode"


def test_parse_focused_full_cache_query_missing_race_bango() -> None:
    result = parse_focused_full_cache_query("category=jra&runDate=20260712&keibajoCode=05")
    assert result == "missing required parameter: raceBango"


# ---------------------------------------------------------------------------
# build_focused_full_cache_response_body
# ---------------------------------------------------------------------------


def test_build_focused_full_cache_response_body_not_found() -> None:
    body = build_focused_full_cache_response_body(None)
    assert json.loads(body.decode()) == {"found": False}


def test_build_focused_full_cache_response_body_found() -> None:
    payload = FocusedFullCachePayload(
        parquet_base64="YmFzZTY0",
        parquet_key="feat-cache/jra/20260712/05/09/features.parquet",
        per_race_parquets=[{"parquetBase64": "cGVy", "parquetKey": "per-race-key"}],
    )
    body = build_focused_full_cache_response_body(payload)
    assert json.loads(body.decode()) == {
        "found": True,
        "parquetBase64": "YmFzZTY0",
        "parquetKey": "feat-cache/jra/20260712/05/09/features.parquet",
        "perRaceParquets": [{"parquetBase64": "cGVy", "parquetKey": "per-race-key"}],
    }


def test_build_focused_full_cache_response_body_includes_daybase_watermark() -> None:
    payload = FocusedFullCachePayload(
        parquet_base64="YmFzZTY0",
        parquet_key="feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        per_race_parquets=None,
        daybase_watermark={
            "maxDataSakuseiNengappi": "20260816",
            "rowCount": 80,
            "rsPredictedAtMax": "2026-08-16T00:00:00",
            "rsRowCount": 4,
        },
    )
    body = build_focused_full_cache_response_body(payload)
    assert json.loads(body.decode()) == {
        "found": True,
        "parquetBase64": "YmFzZTY0",
        "parquetKey": "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        "perRaceParquets": None,
        "daybaseWatermark": {
            "maxDataSakuseiNengappi": "20260816",
            "rowCount": 80,
            "rsPredictedAtMax": "2026-08-16T00:00:00",
            "rsRowCount": 4,
        },
    }


def test_build_focused_full_cache_response_body_includes_watermark_error() -> None:
    payload = FocusedFullCachePayload(
        parquet_base64="YmFzZTY0",
        parquet_key="feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        per_race_parquets=None,
        daybase_watermark=None,
        watermark_error="watermark count is 0",
    )
    body = build_focused_full_cache_response_body(payload)
    assert json.loads(body.decode()) == {
        "found": True,
        "parquetBase64": "YmFzZTY0",
        "parquetKey": "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        "perRaceParquets": None,
        "watermarkError": "watermark count is 0",
    }


# ---------------------------------------------------------------------------
# build_prewarm_result_line
# ---------------------------------------------------------------------------


def test_build_prewarm_result_line_success_no_races_predicted_field() -> None:
    line = build_prewarm_result_line("jra", "20260712", status="success")
    parsed = json.loads(line.decode())
    assert parsed["type"] == "result"
    assert parsed["category"] == "jra"
    assert parsed["runDate"] == "20260712"
    assert parsed["status"] == "success"
    assert "racesPredicted" not in parsed


def test_build_prewarm_result_line_with_parquet_fields() -> None:
    line = build_prewarm_result_line(
        "nar",
        "20260712",
        status="success",
        parquet_base64="dGVzdA==",
        parquet_key="feat-daybase/nar/20260712/features.parquet",
    )
    parsed = json.loads(line.decode())
    assert parsed["parquetBase64"] == "dGVzdA=="
    assert parsed["parquetKey"] == "feat-daybase/nar/20260712/features.parquet"


def test_build_prewarm_result_line_without_parquet_fields() -> None:
    line = build_prewarm_result_line("jra", "20260712", status="success")
    parsed = json.loads(line.decode())
    assert "parquetBase64" not in parsed
    assert "parquetKey" not in parsed


def test_build_prewarm_result_line_with_daybase_watermark() -> None:
    line = build_prewarm_result_line(
        "jra",
        "20260712",
        status="success",
        daybase_watermark={"maxDataSakuseiNengappi": "20260712", "rowCount": 946},
    )
    parsed = json.loads(line.decode())
    assert parsed["daybaseWatermark"] == {"maxDataSakuseiNengappi": "20260712", "rowCount": 946}


def test_build_prewarm_result_line_without_daybase_watermark() -> None:
    line = build_prewarm_result_line("jra", "20260712", status="success")
    parsed = json.loads(line.decode())
    assert "daybaseWatermark" not in parsed


def test_build_prewarm_result_line_includes_generation() -> None:
    line = build_prewarm_result_line("jra", "20260712", status="accepted", generation=3)

    assert json.loads(line.decode()) == {
        "type": "result",
        "category": "jra",
        "runDate": "20260712",
        "status": "accepted",
        "generation": 3,
    }


def test_build_prewarm_result_line_empty_status() -> None:
    line = build_prewarm_result_line("jra", "20260712", status=PREWARM_EMPTY_STATUS)
    parsed = json.loads(line.decode())
    assert parsed["status"] == "empty"


def test_build_prewarm_result_line_error_masks_credentials() -> None:
    line = build_prewarm_result_line(
        "jra",
        "20260712",
        status="error",
        error="RuntimeError: postgresql://user:secret@host/db timed out",
    )
    parsed = json.loads(line.decode())
    assert "secret" not in parsed["error"]
    assert "[REDACTED]" in parsed["error"]


# ---------------------------------------------------------------------------
# iter_prewarm_chunks
# ---------------------------------------------------------------------------


def _mock_build_ok(category: str, run_date: str, days_ahead: int) -> Path:
    return Path(f"/tmp/daybase-{category}-{run_date}")


def _mock_build_empty(category: str, run_date: str, days_ahead: int) -> None:
    return None


def test_iter_prewarm_chunks_sets_debug_env_during_build() -> None:
    seen: list[str | None] = []
    os.environ["PREDICT_DEBUG_LOGS"] = "previous"

    def _build_debug(category: str, run_date: str, days_ahead: int) -> Path:
        seen.append(os.environ.get("PREDICT_DEBUG_LOGS"))
        return Path("/tmp/daybase-jra-20260712")

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0, debug_logs=True)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _build_debug,
            parquet_payload_fn=_mock_payload,
            sleep_fn=_noop_sleep,
        )
    )
    assert json.loads(chunks[-1].decode())["status"] == "success"
    assert seen == ["1"]
    assert os.environ["PREDICT_DEBUG_LOGS"] == "previous"
    os.environ.pop("PREDICT_DEBUG_LOGS", None)


def _mock_build_raises(category: str, run_date: str, days_ahead: int) -> Path:
    raise RuntimeError("day-base build boom")


def _mock_payload(
    category: str, run_date: str, day_base_dir: Path
) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
    return (
        "dGVzdA==",
        "feat-daybase/catalog-v1/jra/20260712/features.parquet",
        {
            "maxDataSakuseiNengappi": "20260712",
            "rowCount": 946,
            "rsPredictedAtMax": "2026-07-18T09:00:00",
            "rsRowCount": 12,
        },
        None,
    )


def test_iter_prewarm_chunks_success_yields_progress_then_result() -> None:
    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_mock_payload, sleep_fn=_noop_sleep
        )
    )
    assert len(chunks) >= 2
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = [p.get("stage") for p in parsed if p.get("type") == "progress"]
    assert "starting" in stages
    last = parsed[-1]
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["category"] == "jra"
    assert last["runDate"] == "20260712"
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"


def test_iter_prewarm_chunks_streams_operational_daybase_timing_without_debug() -> None:
    params = PrewarmParams(category="nar", run_date="20260826", days_ahead=0)

    def _build(_category: str, _run_date: str, _days_ahead: int) -> None:
        record_operational_progress(
            "step=daybase-base index=0 status=done category=nar elapsed_seconds=2.500"
        )
        record_operational_progress(
            "step=daybase-layer index=1/8 status=done category=nar "
            "script=add-race-internal-features.py elapsed_seconds=0.100"
        )

    chunks = list(iter_prewarm_chunks(params, _build, sleep_fn=_noop_sleep))
    stages = [
        parsed["stage"]
        for parsed in (json.loads(chunk) for chunk in chunks)
        if parsed.get("type") == "progress" and str(parsed.get("stage", "")).startswith("step=")
    ]
    assert stages == [
        "step=daybase-base index=0 status=done category=nar elapsed_seconds=2.500",
        (
            "step=daybase-layer index=1/8 status=done category=nar "
            "script=add-race-internal-features.py elapsed_seconds=0.100"
        ),
    ]


def test_iter_prewarm_chunks_empty_build_yields_empty_status() -> None:
    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(iter_prewarm_chunks(params, _mock_build_empty, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == PREWARM_EMPTY_STATUS


def test_iter_prewarm_chunks_exception_yields_error_result() -> None:
    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(iter_prewarm_chunks(params, _mock_build_raises, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert "day-base build boom" in last["error"]


def test_iter_prewarm_chunks_exception_masks_credentials() -> None:
    def _raises_with_url(category: str, run_date: str, days_ahead: int) -> Path:
        raise RuntimeError("postgresql://user:secret@host/db unreachable")

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(iter_prewarm_chunks(params, _raises_with_url, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert "secret" not in last["error"]
    assert "[REDACTED]" in last["error"]


def test_iter_prewarm_chunks_never_raises() -> None:
    def _explode(category: str, run_date: str, days_ahead: int) -> Path:
        raise ValueError("kaboom")

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(iter_prewarm_chunks(params, _explode, sleep_fn=_noop_sleep))
    assert len(chunks) >= 1


def test_iter_prewarm_chunks_forwards_category_run_date_days_ahead() -> None:
    seen: list[tuple[str, str, int]] = []

    def _capture(category: str, run_date: str, days_ahead: int) -> Path:
        seen.append((category, run_date, days_ahead))
        return Path("/tmp/daybase")

    params = PrewarmParams(category="nar", run_date="20260712", days_ahead=3)
    list(
        iter_prewarm_chunks(
            params, _capture, parquet_payload_fn=_mock_payload, sleep_fn=_noop_sleep
        )
    )
    assert seen == [("nar", "20260712", 3)]


def test_iter_prewarm_chunks_calls_parquet_payload_fn_with_day_base_dir() -> None:
    captured: list[tuple[str, str, Path]] = []

    def _payload(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        captured.append((category, run_date, day_base_dir))
        return (
            "dGVzdA==",
            "feat-daybase/jra/20260712/features.parquet",
            {
                "maxDataSakuseiNengappi": "20260712",
                "rowCount": 946,
                "rsPredictedAtMax": "2026-07-18T09:00:00",
                "rsRowCount": 12,
            },
            None,
        )

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_payload, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["parquetBase64"] == "dGVzdA=="
    assert last["parquetKey"] == "feat-daybase/jra/20260712/features.parquet"
    assert last["daybaseWatermark"] == {
        "maxDataSakuseiNengappi": "20260712",
        "rowCount": 946,
        "rsPredictedAtMax": "2026-07-18T09:00:00",
        "rsRowCount": 12,
    }
    assert len(captured) == 1
    assert captured[0][0] == "jra"
    assert captured[0][1] == "20260712"
    assert captured[0][2] == Path("/tmp/daybase-jra-20260712")


def test_iter_prewarm_chunks_forwards_daybase_watermark_when_present() -> None:
    def _payload(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        return (
            "dGVzdA==",
            "feat-daybase/jra/20260712/features.parquet",
            {"maxDataSakuseiNengappi": "20260712", "rowCount": 946},
            None,
        )

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_payload, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["daybaseWatermark"] == {"maxDataSakuseiNengappi": "20260712", "rowCount": 946}


def test_iter_prewarm_chunks_missing_watermark_is_error() -> None:
    committed: list[tuple[str, str, Mapping[str, str | int] | None, str | None]] = []

    def _no_watermark(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        return "dGVzdA==", "feat-daybase/catalog-v1/jra/20260712/features.parquet", None, None

    def _commit(
        parquet_key: str,
        parquet_b64: str,
        watermark: Mapping[str, str | int] | None,
        watermark_error: str | None = None,
    ) -> None:
        committed.append((parquet_key, parquet_b64, watermark, watermark_error))

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_no_watermark,
            commit_fn=_commit,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "prewarm day-base watermark missing after day-base build"
    assert "parquetBase64" not in last
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"
    assert committed == [
        ("feat-daybase/catalog-v1/jra/20260712/features.parquet", "dGVzdA==", None, None)
    ]


def test_iter_prewarm_chunks_zero_count_reason_is_error() -> None:
    committed: list[tuple[str, Mapping[str, str | int] | None, str | None]] = []

    def _zero_count(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        return (
            "dGVzdA==",
            "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
            None,
            "watermark count is 0",
        )

    def _commit(
        parquet_key: str,
        parquet_b64: str,
        watermark: Mapping[str, str | int] | None,
        watermark_error: str | None = None,
    ) -> None:
        committed.append((parquet_key, watermark, watermark_error))

    params = PrewarmParams(category="ban-ei", run_date="20260816", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_zero_count,
            commit_fn=_commit,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "watermark count is 0"
    assert "parquetBase64" not in last
    assert last["parquetKey"] == "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet"
    assert committed == [
        ("feat-daybase/catalog-v1/ban-ei/20260816/features.parquet", None, "watermark count is 0")
    ]


def test_iter_prewarm_chunks_query_failed_reason_is_error() -> None:
    def _query_failed(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        return (
            "dGVzdA==",
            "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
            None,
            "watermark query failed: attach failed",
        )

    params = PrewarmParams(category="ban-ei", run_date="20260816", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_query_failed, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "watermark query failed: attach failed"
    assert "parquetBase64" not in last


def test_iter_prewarm_chunks_parquet_payload_fn_none_result() -> None:
    def _no_parquet(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        return None

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_no_parquet, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "prewarm parquet payload missing after day-base build"
    assert "parquetBase64" not in last


def test_iter_prewarm_chunks_parquet_payload_fn_error_is_reported() -> None:
    def _failing_payload(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        raise RuntimeError("disk read failed")

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_failing_payload, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "RuntimeError: disk read failed"
    assert "parquetBase64" not in last


def test_iter_prewarm_chunks_no_parquet_payload_fn_is_error() -> None:
    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(iter_prewarm_chunks(params, _mock_build_ok, sleep_fn=_noop_sleep))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "prewarm parquet payload missing after day-base build"
    assert "parquetBase64" not in last
    assert "parquetKey" not in last


def test_iter_prewarm_chunks_blank_parquet_key_is_error() -> None:
    def _blank_key(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        return "dGVzdA==", "   ", None, None

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_blank_key, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "prewarm parquet key missing after day-base build"
    assert "parquetBase64" not in last
    assert "parquetKey" not in last


def test_iter_prewarm_chunks_commit_fn_failure_is_error() -> None:
    def _failing_commit(
        parquet_key: str,
        parquet_b64: str,
        watermark: Mapping[str, str | int] | None,
        watermark_error: str | None = None,
    ) -> None:
        raise RuntimeError("put failed")

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            commit_fn=_failing_commit,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "RuntimeError: put failed"
    assert "parquetBase64" not in last


def test_iter_prewarm_chunks_commit_fn_success_keeps_key() -> None:
    committed: list[tuple[str, str]] = []

    def _commit(
        parquet_key: str,
        parquet_b64: str,
        watermark: Mapping[str, str | int] | None,
        watermark_error: str | None = None,
    ) -> None:
        committed.append((parquet_key, parquet_b64))

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            commit_fn=_commit,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"
    assert committed == [("feat-daybase/catalog-v1/jra/20260712/features.parquet", "dGVzdA==")]


def test_iter_prewarm_chunks_existing_object_skips_build() -> None:
    built: list[bool] = []

    def _build(category: str, run_date: str, days_ahead: int) -> Path:
        built.append(True)
        return Path("/tmp/daybase")

    def _existing(category: str, run_date: str) -> str | None:
        return "feat-daybase/catalog-v1/jra/20260712/features.parquet"

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(params, _build, existing_object_fn=_existing, sleep_fn=_noop_sleep)
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"
    assert built == []


def test_iter_prewarm_chunks_force_skips_existing_object_lookup_and_builds() -> None:
    built: list[bool] = []
    existing_lookups: list[bool] = []

    def _build(category: str, run_date: str, days_ahead: int) -> Path:
        built.append(True)
        return Path("/tmp/daybase")

    def _existing(category: str, run_date: str) -> str | None:
        existing_lookups.append(True)
        return "feat-daybase/catalog-v1/jra/20260712/features.parquet"

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0, force=True)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _build,
            parquet_payload_fn=_mock_payload,
            existing_object_fn=_existing,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert built == [True]
    assert existing_lookups == []


def test_iter_prewarm_chunks_rebuild_skips_existing_object_lookup_and_builds() -> None:
    built: list[bool] = []
    existing_lookups: list[bool] = []

    def _build(category: str, run_date: str, days_ahead: int) -> Path:
        built.append(True)
        return Path("/tmp/daybase")

    def _existing(category: str, run_date: str) -> str | None:
        existing_lookups.append(True)
        return "feat-daybase/catalog-v1/nar/20260712/features.parquet"

    params = PrewarmParams(category="nar", run_date="20260712", days_ahead=0, rebuild=True)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _build,
            parquet_payload_fn=_mock_payload,
            existing_object_fn=_existing,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert built == [True]
    assert existing_lookups == []


def test_existing_fresh_object_replaces_old_error_status_with_success() -> None:
    def _launch_failure(fn: Callable[[], None]) -> None:
        raise RuntimeError("thread unavailable")

    def _existing(category: str, run_date: str) -> str | None:
        return "feat-daybase/catalog-v1/jra/20260907/features.parquet"

    params = PrewarmParams(category="jra", run_date="20260907", days_ahead=0)
    failed = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_launch_failure,
            sleep_fn=_noop_sleep,
        )
    )
    fresh = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            existing_object_fn=_existing,
            sleep_fn=_noop_sleep,
        )
    )

    assert json.loads(failed[-1].decode())["status"] == "error"
    assert json.loads(failed[-1].decode())["generation"] == 1
    assert json.loads(fresh[-1].decode()) == {
        "type": "result",
        "category": "jra",
        "runDate": "20260907",
        "status": "success",
        "parquetKey": "feat-daybase/catalog-v1/jra/20260907/features.parquet",
        "generation": 2,
    }
    assert json.loads(build_prewarm_status_response_body(params).decode())["status"] == "success"
    assert json.loads(build_prewarm_status_response_body(params).decode())["generation"] == 2


def test_iter_prewarm_chunks_background_returns_accepted_with_key() -> None:
    launched: list[bool] = []

    def _background(fn: Callable[[], None]) -> None:
        launched.append(True)

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_background,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "accepted"
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"
    assert launched == [True]


def test_iter_prewarm_chunks_in_flight_second_call_is_accepted() -> None:
    def _background(fn: Callable[[], None]) -> None:
        return None

    params = PrewarmParams(category="nar", run_date="20260712", days_ahead=0)
    try:
        first = list(
            iter_prewarm_chunks(
                params,
                _mock_build_ok,
                parquet_payload_fn=_mock_payload,
                background_fn=_background,
                sleep_fn=_noop_sleep,
            )
        )
        second = list(
            iter_prewarm_chunks(
                params,
                _mock_build_ok,
                parquet_payload_fn=_mock_payload,
                background_fn=_background,
                sleep_fn=_noop_sleep,
            )
        )
        assert json.loads(first[-1].decode())["status"] == "accepted"
        assert json.loads(second[-1].decode())["status"] == "accepted"
        assert json.loads(second[-1].decode())["parquetKey"] == (
            "feat-daybase/catalog-v1/nar/20260712/features.parquet"
        )
    finally:
        _release_prewarm_slot("nar:20260712")


def test_background_prewarm_invalidates_only_new_generation() -> None:
    invalidated: list[tuple[str, str]] = []
    launched: list[Callable[[], None]] = []

    def _invalidate(category: str, run_date: str) -> None:
        invalidated.append((category, run_date))

    def _background(fn: Callable[[], None]) -> None:
        launched.append(fn)

    params = PrewarmParams(category="ban-ei", run_date="20260901", days_ahead=0)
    try:
        first = list(
            iter_prewarm_chunks(
                params,
                _mock_build_ok,
                parquet_payload_fn=_mock_payload,
                background_fn=_background,
                invalidate_fn=_invalidate,
                sleep_fn=_noop_sleep,
            )
        )
        duplicate = list(
            iter_prewarm_chunks(
                params,
                _mock_build_ok,
                parquet_payload_fn=_mock_payload,
                background_fn=_background,
                invalidate_fn=_invalidate,
                sleep_fn=_noop_sleep,
            )
        )

        assert invalidated == [("ban-ei", "20260901")]
        assert len(launched) == 1
        assert json.loads(first[-1].decode())["generation"] == 1
        assert json.loads(duplicate[-1].decode())["generation"] == 1
    finally:
        _release_prewarm_slot("ban-ei:20260901")


def test_background_prewarm_new_completed_flight_increments_generation() -> None:
    invalidated: list[tuple[str, str]] = []

    def _invalidate(category: str, run_date: str) -> None:
        invalidated.append((category, run_date))

    def _background(fn: Callable[[], None]) -> None:
        fn()

    params = PrewarmParams(category="nar", run_date="20260903", days_ahead=0)
    first = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_background,
            invalidate_fn=_invalidate,
            sleep_fn=_noop_sleep,
        )
    )
    second = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_background,
            invalidate_fn=_invalidate,
            sleep_fn=_noop_sleep,
        )
    )

    assert json.loads(first[-1].decode())["generation"] == 1
    assert json.loads(second[-1].decode())["generation"] == 2
    assert invalidated == [("nar", "20260903"), ("nar", "20260903")]


def test_background_prewarm_invalidation_failure_releases_flight() -> None:
    def _invalidate(category: str, run_date: str) -> None:
        raise RuntimeError("store unavailable")

    def _background(fn: Callable[[], None]) -> None:
        raise AssertionError("build must not launch after invalidation failure")

    params = PrewarmParams(category="jra", run_date="20260904", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_background,
            invalidate_fn=_invalidate,
            sleep_fn=_noop_sleep,
        )
    )

    assert json.loads(chunks[-1].decode()) == {
        "type": "result",
        "category": "jra",
        "runDate": "20260904",
        "status": "error",
        "error": "RuntimeError: store unavailable",
        "parquetKey": "feat-daybase/catalog-v1/jra/20260904/features.parquet",
        "generation": 1,
    }
    state = get_prewarm_run_state("jra", "20260904")
    assert state is not None
    assert state.status == "error"


def test_background_prewarm_launch_failure_releases_flight() -> None:
    def _background(fn: Callable[[], None]) -> None:
        raise RuntimeError("thread unavailable")

    params = PrewarmParams(category="ban-ei", run_date="20260905", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_background,
            sleep_fn=_noop_sleep,
        )
    )

    assert json.loads(chunks[-1].decode())["status"] == "error"
    assert json.loads(chunks[-1].decode())["error"] == "RuntimeError: thread unavailable"
    state = get_prewarm_run_state("ban-ei", "20260905")
    assert state is not None
    assert state.status == "error"


def test_prewarm_status_tracks_generation_without_debug() -> None:
    launched: list[Callable[[], None]] = []

    def _background(fn: Callable[[], None]) -> None:
        launched.append(fn)

    params = PrewarmParams(category="jra", run_date="20260902", days_ahead=0)
    accepted = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_background,
            sleep_fn=_noop_sleep,
        )
    )

    assert json.loads(accepted[-1].decode())["generation"] == 1
    state = get_prewarm_run_state("jra", "20260902")
    assert state is not None
    running = json.loads(build_prewarm_status_response_body(params).decode())
    assert running["flightKey"] == "jra:20260902"
    assert running["generation"] == 1
    assert running["status"] == "running"
    assert running["startedAtMs"] == state.started_at_ms
    assert running["finishedAtMs"] is None
    assert running["error"] is None

    launched[0]()
    finished = json.loads(build_prewarm_status_response_body(params).decode())
    assert finished["flightKey"] == "jra:20260902"
    assert finished["generation"] == 1
    assert finished["status"] == "success"
    assert isinstance(finished["finishedAtMs"], int)
    assert finished["error"] is None


def test_prewarm_lifecycle_logs_without_debug(capsys: pytest.CaptureFixture[str]) -> None:
    def _background(fn: Callable[[], None]) -> None:
        fn()

    params = PrewarmParams(category="jra", run_date="20260906", days_ahead=0)
    list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            background_fn=_background,
            sleep_fn=_noop_sleep,
        )
    )

    assert capsys.readouterr().err == (
        "[prewarm-status] category=jra runDate=20260906 generation=1 status=running\n"
        "[prewarm-status] category=jra runDate=20260906 generation=1 status=success\n"
    )


def test_prewarm_status_missing_generation() -> None:
    params = PrewarmParams(category="nar", run_date="20991231", days_ahead=0)

    assert json.loads(build_prewarm_status_response_body(params).decode()) == {
        "flightKey": "nar:20991231",
        "generation": 0,
        "status": "missing",
        "startedAtMs": None,
        "finishedAtMs": None,
        "error": None,
    }


def test_iter_prewarm_chunks_background_runs_commit() -> None:
    committed: list[str] = []

    def _commit(
        parquet_key: str,
        parquet_b64: str,
        watermark: Mapping[str, str | int] | None,
        watermark_error: str | None = None,
    ) -> None:
        committed.append(parquet_key)

    def _background(fn: Callable[[], None]) -> None:
        fn()

    params = PrewarmParams(category="jra", run_date="20260713", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            commit_fn=_commit,
            background_fn=_background,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "accepted"
    assert committed == ["feat-daybase/catalog-v1/jra/20260712/features.parquet"]


def test_iter_prewarm_chunks_existing_object_none_falls_through_to_build() -> None:
    def _existing(category: str, run_date: str) -> str | None:
        return None

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            existing_object_fn=_existing,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"


def test_release_prewarm_slot_unknown_key_is_noop() -> None:
    _release_prewarm_slot("missing:20990101")


def test_iter_prewarm_chunks_existing_object_exception_falls_through_to_build() -> None:
    def _existing(category: str, run_date: str) -> str | None:
        raise RuntimeError("head failed")

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            existing_object_fn=_existing,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"


def test_iter_prewarm_chunks_blank_existing_object_falls_through_to_build() -> None:
    def _existing(category: str, run_date: str) -> str | None:
        return "   "

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            existing_object_fn=_existing,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["parquetKey"] == "feat-daybase/catalog-v1/jra/20260712/features.parquet"


def test_iter_prewarm_chunks_empty_parquet_bytes_is_error() -> None:
    def _empty_bytes(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        return "", "feat-daybase/catalog-v1/jra/20260712/features.parquet", None, None

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params, _mock_build_ok, parquet_payload_fn=_empty_bytes, sleep_fn=_noop_sleep
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert last["error"] == "prewarm parquet key missing after day-base build"


def test_iter_prewarm_chunks_background_empty_build_does_not_commit() -> None:
    committed: list[bool] = []

    def _commit(
        parquet_key: str,
        parquet_b64: str,
        watermark: Mapping[str, str | int] | None,
        watermark_error: str | None = None,
    ) -> None:
        committed.append(True)

    def _background(fn: Callable[[], None]) -> None:
        fn()

    params = PrewarmParams(category="jra", run_date="20260714", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_empty,
            parquet_payload_fn=_mock_payload,
            commit_fn=_commit,
            background_fn=_background,
            sleep_fn=_noop_sleep,
        )
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "accepted"
    assert committed == []


def test_run_prewarm_in_background_executes_and_survives_error() -> None:
    ran = threading.Event()

    def _fn() -> None:
        ran.set()
        raise RuntimeError("boom")

    run_prewarm_in_background(_fn)
    assert ran.wait(timeout=2.0) is True


def test_iter_prewarm_chunks_empty_build_skips_parquet_payload_fn() -> None:
    called: list[bool] = []

    def _payload(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        called.append(True)
        return "x", "y", None, None

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    list(
        iter_prewarm_chunks(
            params, _mock_build_empty, parquet_payload_fn=_payload, sleep_fn=_noop_sleep
        )
    )
    assert called == []


def test_iter_prewarm_chunks_error_skips_parquet_payload_fn() -> None:
    called: list[bool] = []

    def _payload(
        category: str, run_date: str, day_base_dir: Path
    ) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
        called.append(True)
        return "x", "y", None, None

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    list(
        iter_prewarm_chunks(
            params, _mock_build_raises, parquet_payload_fn=_payload, sleep_fn=_noop_sleep
        )
    )
    assert called == []


def test_iter_prewarm_chunks_keepalive_emits_progress_during_blocking_build() -> None:
    """Progress lines must be yielded DURING a long-running build_fn call.

    Mirrors ``test_iter_predict_chunks_keepalive_emits_progress_during_blocking_predict``:
    a clock that advances by 15 s on each sleep_fn call (> the 10 s interval),
    so two sleep calls produce two keepalive progress yields before the build
    completes.
    """
    done = threading.Event()

    def _blocking_build(category: str, run_date: str, days_ahead: int) -> Path:
        done.wait()
        return Path("/tmp/daybase")

    time_fn, sleep_fn, _ = _make_advancing_clock(step=15.0)
    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)

    collected: list[bytes] = []
    progress_count = [0]
    generator_done = threading.Event()

    def _consume() -> None:
        gen = iter_prewarm_chunks(
            params,
            _blocking_build,
            parquet_payload_fn=_mock_payload,
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            progress_interval_s=10.0,
        )
        for chunk in gen:
            collected.append(chunk)
            parsed = json.loads(chunk.decode())
            if parsed.get("type") == "progress" and parsed.get("stage") == "day-base-build":
                progress_count[0] += 1
                if progress_count[0] >= 2:
                    done.set()
        generator_done.set()

    consumer = threading.Thread(target=_consume, daemon=True)
    consumer.start()
    generator_done.wait(timeout=10.0)
    assert generator_done.is_set(), "generator did not complete within 10 s"

    parsed_all = [json.loads(c.decode()) for c in collected]
    progress_stages = [p.get("stage") for p in parsed_all if p.get("type") == "progress"]
    assert progress_stages.count("day-base-build") >= 2

    last = parsed_all[-1]
    assert last["type"] == "result"
    assert last["status"] == "success"


def test_iter_prewarm_chunks_post_build_progress_when_interval_elapsed() -> None:
    """After build_fn returns, a completion progress is emitted if interval elapsed.

    time_fn() call order (fast mock, 0 keepalive-loop iterations):
      1 started=                             (0.0)
      2 _elapsed() "starting"                (0.0)
      3 last_progress=                       (0.0)
      4 _elapsed() "day-base-build"          (0.0)
      5 last_progress= (reset)               (0.0)
      ... _iter_keepalive loop: 0 iterations for instant mock ...
      6 now (post-build check)               (100.0) -> 100-0 >= 10 -> fires
    """
    call_count = [0]

    def _slow_time() -> float:
        call_count[0] += 1
        if call_count[0] <= 5:
            return 0.0
        return 100.0

    params = PrewarmParams(category="jra", run_date="20260712", days_ahead=0)
    chunks = list(
        iter_prewarm_chunks(
            params,
            _mock_build_ok,
            parquet_payload_fn=_mock_payload,
            time_fn=_slow_time,
            sleep_fn=_noop_sleep,
            progress_interval_s=10.0,
        )
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "complete" in stages
