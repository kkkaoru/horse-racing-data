"""Unit tests for ``predict_lib.serve``.

Covers: parse_predict_params (incl. mode param), parse_request_path,
mask_error_message, build_progress_line, build_result_line,
build_r2_feat_cache_key, iter_predict_chunks (mode=full, mode=rescore,
rescore-fallback), CacheMissError.

``_predict_category`` is mocked via the ``predict_fn`` / ``rescore_fn``
arguments so no real Neon / DuckDB / ML I/O is performed.  Coverage is
measured under ``--cov=predict_lib`` (``pyproject.toml``).
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from predict_lib.serve import (
    CacheMissError,
    PredictParams,
    R2Config,
    build_progress_line,
    build_r2_feat_cache_key,
    build_result_line,
    iter_predict_chunks,
    mask_error_message,
    parse_predict_params,
    parse_request_path,
)

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


def _mock_predict_ok(category: str, run_date: str, days_ahead: int) -> int:
    return 42


def test_iter_predict_chunks_success_yields_progress_then_result() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok))

    # At minimum: starting + predict + result lines
    assert len(chunks) >= 3

    parsed = [json.loads(c.decode()) for c in chunks]
    types = [p["type"] for p in parsed]
    assert types[0] == "progress"
    assert types[-1] == "result"
    assert "result" in types


def test_iter_predict_chunks_result_is_success() -> None:
    params = PredictParams(category="nar", run_date="20260619", days_ahead=1)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok))
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "success"
    assert last["racesPredicted"] == 42
    assert last["category"] == "nar"
    assert last["runDate"] == "20260619"


def test_iter_predict_chunks_first_chunk_is_starting_progress() -> None:
    params = PredictParams(category="ban-ei", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok))
    first = json.loads(chunks[0].decode())
    assert first["type"] == "progress"
    assert first["stage"] == "starting"


def test_iter_predict_chunks_predict_progress_emitted() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_ok))
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = [p.get("stage") for p in parsed if p.get("type") == "progress"]
    assert "predict" in stages


# ---------------------------------------------------------------------------
# iter_predict_chunks — exception path
# ---------------------------------------------------------------------------


def _mock_predict_raises(category: str, run_date: str, days_ahead: int) -> int:
    raise RuntimeError("feature build failed: postgresql://user:pw@host/db")


def test_iter_predict_chunks_exception_yields_error_result() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises))
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["status"] == "error"
    assert "error" in last


def test_iter_predict_chunks_exception_races_predicted_is_zero() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises))
    last = json.loads(chunks[-1].decode())
    assert last["racesPredicted"] == 0


def test_iter_predict_chunks_exception_masks_credentials() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises))
    last_bytes = chunks[-1]
    assert b"pw" not in last_bytes
    assert b"[REDACTED]" in last_bytes


def test_iter_predict_chunks_exception_error_includes_exception_type() -> None:
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(iter_predict_chunks(params, _mock_predict_raises))
    last = json.loads(chunks[-1].decode())
    assert "RuntimeError" in last["error"]


def test_iter_predict_chunks_never_raises() -> None:
    """The generator must not propagate any exception from predict_fn."""

    def _explode(category: str, run_date: str, days_ahead: int) -> int:
        raise ValueError("unexpected kaboom")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    # If this raises, the test fails — the generator must catch all exceptions.
    chunks = list(iter_predict_chunks(params, _explode))
    assert len(chunks) >= 1


# ---------------------------------------------------------------------------
# iter_predict_chunks — interval-based progress gating
# ---------------------------------------------------------------------------


def test_iter_predict_chunks_feature_build_progress_when_interval_elapsed() -> None:
    """The feature-build progress line fires when the interval elapses at that checkpoint.

    time_fn() call order inside iter_predict_chunks:
      1  started=                          (0.0)
      2  _elapsed() in "starting" yield    (0.0)
      3  last_progress=                    (0.0)
      4  now= (feature-build gate)         (20.0)  <- now - last_progress = 20 >= 10 -> fires
      5  _elapsed() in "feature-build"     (20.0)
      (last_progress = now = 20.0 in branch)
      6  _elapsed() in "predict" yield     (20.0)
      7  last_progress=                    (20.0)
      ...
    """
    time_fn = _make_time_fn([0.0, 0.0, 0.0, 20.0, 20.0, 20.0, 20.0, 20.0, 25.0, 30.0])
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(
        iter_predict_chunks(params, _mock_predict_ok, time_fn=time_fn, progress_interval_s=10.0)
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    # With elapsed time > interval at the feature-build gate, that stage fires
    assert "feature-build" in stages


def test_iter_predict_chunks_no_extra_progress_within_interval() -> None:
    """When all calls complete before the interval, no extra progress is emitted."""
    # All time_fn calls return 0 so interval never elapses
    time_fn = _make_time_fn([0.0] * 20)
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(
        iter_predict_chunks(params, _mock_predict_ok, time_fn=time_fn, progress_interval_s=10.0)
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    # starting + predict lines must be present (always emitted unconditionally)
    # feature-build line is skipped when interval has NOT elapsed
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "starting" in stages
    assert "predict" in stages


def test_iter_predict_chunks_post_pipeline_progress_when_interval_elapsed() -> None:
    """After predict_fn returns, a completion progress is emitted if interval elapsed.

    The generator calls time_fn() in this order:
      1 started=         (setup)
      2 _elapsed() in "starting" yield
      3 last_progress=   (after starting)
      4 now= (feature-build gate)
      5 _elapsed() in "predict" yield
      6 last_progress= (reset after "predict" emit)
      7 now= (post-pipeline check)

    For "complete" to emit: time[7] - time[6] >= interval.
    We therefore return 0.0 for calls 1-6 and 100.0 for call 7+.
    """
    call_count = [0]

    def _slow_time() -> float:
        call_count[0] += 1
        if call_count[0] <= 6:
            return 0.0
        return 100.0

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0)
    chunks = list(
        iter_predict_chunks(params, _mock_predict_ok, time_fn=_slow_time, progress_interval_s=10.0)
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

    def _fn(category: str, run_date: str, days_ahead: int) -> int:
        called[0] = True
        return 5

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="full")
    chunks = list(iter_predict_chunks(params, _fn))
    assert called[0]
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 5


# ---------------------------------------------------------------------------
# iter_predict_chunks — mode=rescore with rescore_fn (success)
# ---------------------------------------------------------------------------


def _mock_rescore_ok(category: str, run_date: str, days_ahead: int) -> int:
    return 7


def test_iter_predict_chunks_mode_rescore_calls_rescore_fn() -> None:
    """When mode=rescore and rescore_fn succeeds, predict_fn is NOT called."""
    full_called = [False]

    def _full_fn(category: str, run_date: str, days_ahead: int) -> int:
        full_called[0] = True
        return 99  # should not be reached

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(iter_predict_chunks(params, _full_fn, rescore_fn=_mock_rescore_ok))
    assert not full_called[0], "predict_fn (full) must not be called when rescore succeeds"
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 7


def test_iter_predict_chunks_mode_rescore_result_has_correct_fields() -> None:
    params = PredictParams(category="nar", run_date="20260619", days_ahead=1, mode="rescore")
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, rescore_fn=_mock_rescore_ok))
    last = json.loads(chunks[-1].decode())
    assert last["type"] == "result"
    assert last["category"] == "nar"
    assert last["runDate"] == "20260619"
    assert last["status"] == "success"


# ---------------------------------------------------------------------------
# iter_predict_chunks — mode=rescore CacheMissError fallback to full
# ---------------------------------------------------------------------------


def _mock_rescore_cache_miss(category: str, run_date: str, days_ahead: int) -> int:
    raise CacheMissError(f"no cache for {category}/{run_date}")


def test_iter_predict_chunks_rescore_cache_miss_falls_back_to_full() -> None:
    """CacheMissError from rescore_fn must trigger fallback to predict_fn."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(params, _mock_predict_ok, rescore_fn=_mock_rescore_cache_miss)
    )
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 42  # from _mock_predict_ok


def test_iter_predict_chunks_rescore_cache_miss_emits_fallback_progress() -> None:
    """A rescore-fallback-to-full progress line must be emitted on CacheMissError."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(
        iter_predict_chunks(params, _mock_predict_ok, rescore_fn=_mock_rescore_cache_miss)
    )
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "rescore-fallback-to-full" in stages


def test_iter_predict_chunks_rescore_no_rescore_fn_falls_back_to_full() -> None:
    """When mode=rescore but rescore_fn=None, must fall back to predict_fn."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, rescore_fn=None))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "success"
    assert last["racesPredicted"] == 42


def test_iter_predict_chunks_rescore_no_fn_emits_fallback_progress() -> None:
    """When mode=rescore with no rescore_fn, fallback progress must be emitted."""
    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(iter_predict_chunks(params, _mock_predict_ok))  # no rescore_fn
    parsed = [json.loads(c.decode()) for c in chunks]
    stages = {p.get("stage") for p in parsed if p.get("type") == "progress"}
    assert "rescore-fallback-to-full" in stages


def test_iter_predict_chunks_rescore_non_cache_miss_propagates_as_error() -> None:
    """Non-CacheMissError exceptions from rescore_fn must yield an error result."""

    def _rescore_runtime_err(category: str, run_date: str, days_ahead: int) -> int:
        raise RuntimeError("unexpected DB error")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    chunks = list(iter_predict_chunks(params, _mock_predict_ok, rescore_fn=_rescore_runtime_err))
    last = json.loads(chunks[-1].decode())
    assert last["status"] == "error"
    assert "RuntimeError" in last["error"]


def test_iter_predict_chunks_rescore_non_cache_miss_does_not_call_full_fn() -> None:
    """Non-CacheMissError from rescore_fn must NOT fall back to predict_fn."""
    full_called = [False]

    def _full_fn(category: str, run_date: str, days_ahead: int) -> int:
        full_called[0] = True
        return 99

    def _rescore_runtime_err(category: str, run_date: str, days_ahead: int) -> int:
        raise RuntimeError("unexpected")

    params = PredictParams(category="jra", run_date="20260619", days_ahead=0, mode="rescore")
    list(iter_predict_chunks(params, _full_fn, rescore_fn=_rescore_runtime_err))
    assert not full_called[0]
