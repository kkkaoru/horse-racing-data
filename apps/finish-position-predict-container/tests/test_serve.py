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
import threading
from collections.abc import Callable

import pytest

from predict_lib.serve import (
    CacheMissError,
    PredictCategoryFn,
    PredictParams,
    R2Config,
    SleepFn,
    TimeFn,
    build_progress_line,
    build_r2_feat_cache_key,
    build_r2_per_race_feat_cache_key,
    build_result_line,
    iter_predict_chunks,
    mask_error_message,
    parse_predict_params,
    parse_request_path,
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
    )
    assert isinstance(result, PredictParams)
    assert result.keibajo_code == "44"


def test_parse_predict_params_race_bango_parsed() -> None:
    result = parse_predict_params(
        "category=nar&runDate=20260619&mode=rescore&keibajoCode=44&raceBango=01"
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
    assert key == "feat-cache/jra/20260619/features.parquet"


def test_build_r2_feat_cache_key_nar() -> None:
    key = build_r2_feat_cache_key("nar", "20260101")
    assert key == "feat-cache/nar/20260101/features.parquet"


def test_build_r2_feat_cache_key_banei() -> None:
    key = build_r2_feat_cache_key("ban-ei", "20260619")
    assert key == "feat-cache/ban-ei/20260619/features.parquet"


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
    assert key == "feat-cache/jra/20260619/05/09/features.parquet"


def test_build_r2_per_race_feat_cache_key_nar() -> None:
    key = build_r2_per_race_feat_cache_key("nar", "20260101", "30", "11")
    assert key == "feat-cache/nar/20260101/30/11/features.parquet"


def test_build_r2_per_race_feat_cache_key_banei() -> None:
    key = build_r2_per_race_feat_cache_key("ban-ei", "20260619", "83", "01")
    assert key == "feat-cache/ban-ei/20260619/83/01/features.parquet"


def test_build_r2_per_race_feat_cache_key_deterministic() -> None:
    """Same inputs must produce the same per-race key (idempotent cache update)."""
    key1 = build_r2_per_race_feat_cache_key("jra", "20260619", "05", "09")
    key2 = build_r2_per_race_feat_cache_key("jra", "20260619", "05", "09")
    assert key1 == key2


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


def test_iter_predict_chunks_rescore_non_cache_miss_does_not_call_full_fn() -> None:
    """Non-CacheMissError from rescore_fn must NOT fall back to predict_fn."""
    full_called = [False]

    def _full_fn(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
    ) -> int:
        full_called[0] = True
        return 99

    def _rescore_runtime_err(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
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
    ) -> int:
        rescore_done.wait()
        raise CacheMissError("no cache")

    def _predict_full(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
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
    ) -> int:
        raise CacheMissError("miss")

    def _predict_raise(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
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
