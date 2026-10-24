"""Rescore hook that re-runs the baba layer once the race-day going is known."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

import pipeline_runner
import predict_upcoming

_REFRESH_ATTR = "_refresh_baba_for_current_going"
_refresh = cast(
    "Callable[[str, str, str, Path, tuple[str, str]], bool]",
    getattr(predict_upcoming, _REFRESH_ATTR),
)


def test_rescore_refresh_forwards_the_single_race_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[dict[str, object]] = []

    def fake_refresh(**kwargs: object) -> bool:
        calls.append(kwargs)
        return True

    monkeypatch.setattr(pipeline_runner, "refresh_baba_for_current_going", fake_refresh)

    assert _refresh("jra", "20261025", "r2-catalog://source", tmp_path, ("08", "11")) is True
    assert calls == [
        {
            "category": "jra",
            "target_date": "20261025",
            "target_race": "08:11",
            "database_url": "r2-catalog://source",
            "final_dir": tmp_path,
        }
    ]


def test_rescore_refresh_failure_keeps_cached_features(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def failing_refresh(**_kwargs: object) -> bool:
        raise RuntimeError("baba layer failed")

    monkeypatch.setattr(pipeline_runner, "refresh_baba_for_current_going", failing_refresh)

    assert _refresh("nar", "20261025", "r2-catalog://pc-keiba", tmp_path, ("46", "01")) is False
