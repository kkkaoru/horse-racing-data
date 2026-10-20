from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from learning import feature_selection_policy

PREDICT_CONTAINER_SRC = Path(__file__).resolve().parents[2] / "finish-position-predict-container" / "src"


def test_container_market_feature_policy_matches_the_viewer_source_of_truth(
    monkeypatch: pytest.MonkeyPatch,
):
    """The container's win-odds ban is a copy; it must stay identical to this one."""
    monkeypatch.syspath_prepend(str(PREDICT_CONTAINER_SRC))
    container_policy = importlib.import_module("predict_lib.market_feature_policy")
    assert container_policy.BANNED_MARKET_PATTERN.pattern == feature_selection_policy.BANNED_MARKET_PATTERN.pattern
    assert container_policy.MARKET_DEVIATION_FEATURES == feature_selection_policy.MARKET_DEVIATION_FEATURES
