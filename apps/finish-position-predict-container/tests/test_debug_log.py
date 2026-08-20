"""Tests for the process-wide container debug-log helper."""

from __future__ import annotations

import os

import pytest

from predict_lib.debug_log import (
    debug_log,
    debug_logs_enabled,
    debug_logs_scope,
    drain_debug_progress,
    parse_debug_flag,
    query_debug_enabled,
    record_debug_progress,
)


def test_parse_debug_flag_missing_is_off() -> None:
    assert parse_debug_flag(None) is False


def test_parse_debug_flag_empty_is_off() -> None:
    assert parse_debug_flag("") is False


def test_parse_debug_flag_zero_is_off() -> None:
    assert parse_debug_flag("0") is False


def test_parse_debug_flag_false_is_off() -> None:
    assert parse_debug_flag("false") is False


def test_parse_debug_flag_garbage_is_off() -> None:
    assert parse_debug_flag("maybe") is False


def test_parse_debug_flag_one_is_on() -> None:
    assert parse_debug_flag("1") is True


def test_parse_debug_flag_true_is_on() -> None:
    assert parse_debug_flag("true") is True


def test_parse_debug_flag_yes_is_on() -> None:
    assert parse_debug_flag("yes") is True


def test_parse_debug_flag_on_is_on() -> None:
    assert parse_debug_flag("on") is True


def test_parse_debug_flag_debug_token_is_on() -> None:
    assert parse_debug_flag("debug") is True


def test_parse_debug_flag_true_is_case_insensitive() -> None:
    assert parse_debug_flag("TRUE") is True


def test_query_debug_enabled_missing_is_off() -> None:
    assert query_debug_enabled("category=jra&runDate=20260712") is False


def test_query_debug_enabled_empty_query_is_off() -> None:
    assert query_debug_enabled("") is False


def test_query_debug_enabled_invalid_value_is_off() -> None:
    assert query_debug_enabled("debug=nope") is False


def test_query_debug_enabled_one_is_on() -> None:
    assert query_debug_enabled("category=jra&debug=1") is True


def test_debug_log_silent_when_env_missing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    debug_log("[day-base] HIT local category=jra target_date=20260712 reason=watermark-match")
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_debug_log_silent_when_env_zero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "0")
    debug_log("[day-base] MISS category=jra target_date=20260712 reason=watermark count is 0")
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_debug_log_emits_when_env_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    debug_log("[day-base] HIT local category=jra target_date=20260712 reason=watermark-match")
    captured = capsys.readouterr()
    assert captured.err == (
        "[day-base] HIT local category=jra target_date=20260712 reason=watermark-match\n"
    )
    assert captured.out == ""


def test_debug_logs_enabled_false_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    assert debug_logs_enabled() is False


def test_debug_logs_scope_enables_then_restores_missing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    with debug_logs_scope(True):
        assert os.environ["PREDICT_DEBUG_LOGS"] == "1"
        debug_log("[pipeline] step=base index=0 status=start")
    assert "PREDICT_DEBUG_LOGS" not in os.environ
    captured = capsys.readouterr()
    assert captured.err == "[pipeline] step=base index=0 status=start\n"


def test_debug_logs_scope_restores_previous_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "previous")
    with debug_logs_scope(False):
        assert os.environ["PREDICT_DEBUG_LOGS"] == "0"
    assert os.environ["PREDICT_DEBUG_LOGS"] == "previous"


def test_record_debug_progress_silent_when_debug_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    drain_debug_progress()
    record_debug_progress("step=racechain-layer index=1/1 status=start")
    record_debug_progress("step=daybase-hit source=local category=jra target_date=20260712")
    assert drain_debug_progress() == []


def test_record_debug_progress_queues_when_debug_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    record_debug_progress(
        "step=daybase-hit source=local category=jra target_date=20260712 reason=watermark-match"
    )
    record_debug_progress(
        "step=racechain-layer index=1/1 status=start category=jra script=x.py "
        "target_race=83:03 elapsed_seconds=0.000"
    )
    assert drain_debug_progress() == [
        "step=daybase-hit source=local category=jra target_date=20260712 reason=watermark-match",
        "step=racechain-layer index=1/1 status=start category=jra script=x.py "
        "target_race=83:03 elapsed_seconds=0.000",
    ]
    assert drain_debug_progress() == []


def test_record_debug_progress_miss_token_when_debug_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    record_debug_progress(
        "step=daybase-miss category=jra target_date=20260712 reason=no-local-cache"
    )
    assert drain_debug_progress() == [
        "step=daybase-miss category=jra target_date=20260712 reason=no-local-cache"
    ]
