"""Shared pytest fixtures for the prediction container tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

import predict_upcoming

REAL_FETCH_MARKET_DEVIATION_POOLS = predict_upcoming.fetch_market_deviation_pools
REPO_BASELINE_PATH = (
    Path(__file__).resolve().parents[2]
    / "pc-keiba-viewer/finish-position/lookups/exotic-market-deviation-baseline.json"
)


@pytest.fixture(autouse=True)
def no_live_market_deviation_fetch(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep every test hermetic: no test may reach the live hot-worker odds API."""
    monkeypatch.setattr(predict_upcoming, "fetch_market_deviation_pools", lambda _races: {})
    monkeypatch.setenv("MARKET_DEVIATION_BASELINE_PATH", str(REPO_BASELINE_PATH))
    yield
