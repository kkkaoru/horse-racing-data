"""Tests for the serving twin of the umaren / sanrenpuku deviation features."""

from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path
from types import ModuleType
from typing import final

import pytest

from predict_lib.market_deviation import (
    Baseline,
    RaceOddsPools,
    attach_market_deviation,
    baseline_path,
    compute_race_features,
    load_baseline,
    market_ranks,
    pools_from_latest,
    rank_residuals,
    shares_from_combinations,
)

BASELINE = Baseline(
    umaren={"3:1.0": 0.4, "3:2.0": 0.0, "3:3.0": -0.6},
    sanrenpuku={"3:1.0": 0.1, "3:2.0": 0.0, "3:3.0": -0.1},
)
OFFLINE_LAYER = (
    Path(__file__).resolve().parents[2]
    / "pc-keiba-viewer/src/scripts/finish-position-features/add_exotic_market_deviation_features.py"
)


def _load_offline_layer(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.syspath_prepend(str(OFFLINE_LAYER.parent))
    spec = importlib.util.spec_from_file_location("offline_market_deviation", OFFLINE_LAYER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "offline_market_deviation", module)
    spec.loader.exec_module(module)
    return module


def test_shares_from_combinations_skips_malformed_entries() -> None:
    entries = [
        {"combination": "1-2", "odds": 2.0},
        {"combination": "1-3", "odds": 4.0},
        {"combination": "1-2-3", "odds": 4.0},
        {"combination": "a-2", "odds": 4.0},
        {"combination": "2-3", "odds": 0},
        {"combination": "2-3", "odds": True},
        {"combination": "2-3", "odds": "5"},
        "bad",
    ]
    shares = shares_from_combinations(entries, 2)
    assert {h: round(v, 6) for h, v in shares.items()} == {1: 0.5, 2: 0.333333, 3: 0.166667}


def test_shares_from_combinations_rejects_non_list() -> None:
    assert shares_from_combinations(None, 2) == {}


def test_shares_from_combinations_all_invalid_returns_empty() -> None:
    assert shares_from_combinations([{"combination": "1-2", "odds": -1.0}], 2) == {}


def test_pools_from_latest_reads_both_pools() -> None:
    pools = pools_from_latest(
        {
            "umaren": [{"combination": "1-2", "odds": 2.0}],
            "3renpuku": [{"combination": "1-2-3", "odds": 3.0}],
            "tansho": [{"combination": "1", "odds": 1.5}],
        }
    )
    assert pools.umaren == {1: 0.5, 2: 0.5}
    assert {h: round(v, 6) for h, v in pools.sanrenpuku.items()} == {
        1: 0.333333,
        2: 0.333333,
        3: 0.333333,
    }


def test_pools_from_latest_non_mapping_is_empty() -> None:
    assert pools_from_latest(None) == RaceOddsPools(umaren={}, sanrenpuku={})


def test_baseline_path_uses_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MARKET_DEVIATION_BASELINE_PATH", "/tmp/b.json")
    assert baseline_path() == Path("/tmp/b.json")


def test_baseline_path_defaults_to_baked_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MARKET_DEVIATION_BASELINE_PATH")
    assert baseline_path() == Path("/app/lookups/exotic-market-deviation-baseline.json")


def test_load_baseline_reads_tracked_lookup_for_every_category() -> None:
    assert len(load_baseline(baseline_path(), "jra").umaren) == 161
    assert len(load_baseline(baseline_path(), "nar").sanrenpuku) == 130
    assert len(load_baseline(baseline_path(), "ban-ei").umaren) == 45


def test_market_ranks_average_ties() -> None:
    assert market_ranks({1: 0.25, 2: 0.5, 3: 0.25}) == {1: 2.5, 2: 1.0, 3: 2.5}


def test_rank_residuals_skip_cells_missing_from_baseline() -> None:
    residuals = rank_residuals({1: 0.5, 2: 0.3, 3: 0.2}, {"3:1.0": 0.4, "3:2.0": 0.0})
    assert {h: round(v, 6) for h, v in residuals.items()} == {1: 0.005465, 2: -0.105361}


def test_compute_race_features_needs_three_common_horses() -> None:
    pools = RaceOddsPools(umaren={1: 0.5, 2: 0.5}, sanrenpuku={1: 0.3, 2: 0.3, 3: 0.4})
    assert compute_race_features(pools, BASELINE) == {}


def test_compute_race_features_values() -> None:
    pools = RaceOddsPools(
        umaren={1: 0.5, 2: 0.3, 3: 0.2, 9: 0.1}, sanrenpuku={1: 0.4, 2: 0.35, 3: 0.25}
    )
    features = compute_race_features(pools, BASELINE)
    assert round(features[1]["exotic_umaren_p2_rank_residual"], 6) == 0.005465
    assert round(features[3]["exotic_sanrenpuku_p3_rank_residual"], 6) == -0.187682


def test_compute_race_features_omits_cells_missing_from_baseline() -> None:
    pools = RaceOddsPools(umaren={1: 0.5, 2: 0.3, 3: 0.2}, sanrenpuku={1: 0.4, 2: 0.35, 3: 0.25})
    features = compute_race_features(pools, Baseline(umaren={}, sanrenpuku={"3:1.0": 0.0}))
    assert features[2] == {}
    assert list(features[1]) == ["exotic_sanrenpuku_p3_rank_residual"]


def test_serving_matches_offline_layer_on_same_pools(monkeypatch: pytest.MonkeyPatch) -> None:
    offline = _load_offline_layer(monkeypatch)
    umaren = {1: 0.31, 2: 0.22, 3: 0.17, 4: 0.12, 5: 0.1, 6: 0.08}
    sanrenpuku = {1: 0.24, 2: 0.21, 3: 0.18, 4: 0.15, 5: 0.12, 6: 0.1}
    serving = compute_race_features(
        RaceOddsPools(umaren=umaren, sanrenpuku=sanrenpuku), load_baseline(baseline_path(), "jra")
    )
    training = offline.compute_race_features(
        offline.RaceOdds(umaren=umaren, sanrenpuku=sanrenpuku),
        offline.load_baseline(offline.DEFAULT_BASELINE_PATH, "jra"),
    )
    assert serving == training


def test_attach_market_deviation_sets_values_and_nan_for_unpriced() -> None:
    races = {
        "jra:2026:1018:05:11": [
            {"umaban": 1, "feat": 1.0},
            {"umaban": 2, "feat": 2.0},
            {"umaban": 3, "feat": 3.0},
            {"umaban": None, "feat": 4.0},
        ],
        "jra:2026:1018:05:12": [{"umaban": 1, "feat": 5.0}],
    }
    pools = {
        "jra:2026:1018:05:11": RaceOddsPools(
            umaren={1: 0.5, 2: 0.3, 3: 0.2}, sanrenpuku={1: 0.4, 2: 0.35, 3: 0.25}
        )
    }
    updated = attach_market_deviation(races, pools, BASELINE)
    first = updated["jra:2026:1018:05:11"]
    assert first[0]["feat"] == 1.0
    priced = first[0]["exotic_umaren_p2_rank_residual"]
    unpriced_horse = first[3]["exotic_umaren_p2_rank_residual"]
    unpriced_race = updated["jra:2026:1018:05:12"][0]["exotic_sanrenpuku_p3_rank_residual"]
    assert isinstance(priced, float)
    assert isinstance(unpriced_horse, float)
    assert isinstance(unpriced_race, float)
    assert round(priced, 6) == 0.005465
    assert math.isnan(unpriced_horse)
    assert math.isnan(unpriced_race)


def test_attach_market_deviation_treats_nan_umaban_as_unpriced() -> None:
    races = {"jra:2026:1018:05:11": [{"umaban": math.nan}]}
    pools = {"jra:2026:1018:05:11": RaceOddsPools(umaren={1: 1.0}, sanrenpuku={1: 1.0})}
    updated = attach_market_deviation(races, pools, BASELINE)
    value = updated["jra:2026:1018:05:11"][0]["exotic_sanrenpuku_p3_rank_residual"]
    assert isinstance(value, float)
    assert math.isnan(value)


@pytest.mark.parametrize("umaban", [True, "1"])
def test_attach_market_deviation_rejects_non_numeric_umaban(umaban: object) -> None:
    races = {"jra:2026:1018:05:11": [{"umaban": umaban}]}
    updated = attach_market_deviation(races, {}, BASELINE)
    value = updated["jra:2026:1018:05:11"][0]["exotic_umaren_p2_rank_residual"]
    assert isinstance(value, float)
    assert math.isnan(value)


@final
class _LatestStub:
    def __init__(self, responses: dict[str, dict[str, object]]) -> None:
        self._responses = responses

    def fetch(self, url: str, timeout: float) -> dict[str, object]:
        if url in self._responses:
            return self._responses[url]
        raise OSError("no stub")


def test_fetch_market_deviation_pools_with_keeps_only_published_races() -> None:
    import predict_upcoming
    from realtime_odds_fetcher import HOT_WORKER_BASE_URL

    stub = _LatestStub(
        {
            f"{HOT_WORKER_BASE_URL}/jra%3A2026%3A1018%3A05%3A11": {
                "latest": {
                    "umaren": [{"combination": "1-2", "odds": 2.0}],
                    "3renpuku": [{"combination": "1-2-3", "odds": 3.0}],
                }
            }
        }
    )
    pools = predict_upcoming.fetch_market_deviation_pools_with(
        {"jra:2026:1018:05:11": [], "jra:2026:1018:05:12": []}, stub
    )
    assert list(pools) == ["jra:2026:1018:05:11"]
    assert pools["jra:2026:1018:05:11"].umaren == {1: 0.5, 2: 0.5}


def test_fetch_market_deviation_pools_uses_http_fetcher(monkeypatch: pytest.MonkeyPatch) -> None:
    from conftest import REAL_FETCH_MARKET_DEVIATION_POOLS

    import predict_upcoming
    import realtime_odds_fetcher

    monkeypatch.setattr(realtime_odds_fetcher, "HttpRealtimeOddsFetcher", lambda: _LatestStub({}))
    monkeypatch.setattr(
        predict_upcoming, "fetch_market_deviation_pools", REAL_FETCH_MARKET_DEVIATION_POOLS
    )
    assert predict_upcoming.fetch_market_deviation_pools({"jra:2026:1018:05:11": []}) == {}
