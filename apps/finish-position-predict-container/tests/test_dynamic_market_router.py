"""Tests for the disabled-by-default dynamic market shadow scorer."""

from __future__ import annotations

import math

import pytest

from predict_lib.dynamic_market_router import (
    SELECTED_SHADOW_CONFIG,
    DynamicMarketRouterConfig,
    is_surface_expert_context,
    resolve_market_weight,
    route_dynamic_market_scores,
)

_ENABLED = DynamicMarketRouterConfig(
    enabled=True,
    minimum_market_weight=0.95,
    activation_threshold=0.40,
    gamma=0.50,
    high_upset_context_threshold=0.55,
)


def test_selected_shadow_config_is_disabled_and_fixed_to_loop_43() -> None:
    assert (
        DynamicMarketRouterConfig(
            enabled=False,
            minimum_market_weight=0.95,
            activation_threshold=0.40,
            gamma=0.50,
            high_upset_context_threshold=0.55,
        )
        == SELECTED_SHADOW_CONFIG
    )


@pytest.mark.parametrize(
    ("keyword", "value", "message"),
    [
        ("minimum_market_weight", -0.1, "minimum_market_weight"),
        ("minimum_market_weight", 1.1, "minimum_market_weight"),
        ("activation_threshold", -0.1, "activation_threshold"),
        ("activation_threshold", 1.0, "activation_threshold"),
        ("gamma", 0.0, "gamma"),
        ("high_upset_context_threshold", -0.1, "high_upset_context_threshold"),
        ("high_upset_context_threshold", 1.1, "high_upset_context_threshold"),
    ],
)
def test_config_rejects_out_of_range_values(keyword: str, value: float, message: str) -> None:
    values = {
        "enabled": True,
        "minimum_market_weight": 0.95,
        "activation_threshold": 0.40,
        "gamma": 0.50,
        "high_upset_context_threshold": 0.55,
    }
    values[keyword] = value

    with pytest.raises(ValueError, match=message):
        DynamicMarketRouterConfig(
            enabled=bool(values["enabled"]),
            minimum_market_weight=float(values["minimum_market_weight"]),
            activation_threshold=float(values["activation_threshold"]),
            gamma=float(values["gamma"]),
            high_upset_context_threshold=float(values["high_upset_context_threshold"]),
        )


def test_market_weight_is_one_at_or_below_activation_threshold() -> None:
    assert resolve_market_weight(0.40, _ENABLED) == 1.0


def test_market_weight_reaches_configured_minimum_at_probability_one() -> None:
    assert resolve_market_weight(1.0, _ENABLED) == pytest.approx(0.95)


@pytest.mark.parametrize("probability", [-0.01, 1.01, math.inf, math.nan])
def test_market_weight_rejects_invalid_probability(probability: float) -> None:
    with pytest.raises(ValueError, match="upset_probability"):
        resolve_market_weight(probability, _ENABLED)


@pytest.mark.parametrize(
    ("surface", "distance_band"),
    [
        ("turf", "intermediate"),
        ("dirt", "sprint"),
        ("obstacle", "extended"),
    ],
)
def test_base_surface_contexts_are_active(surface: str, distance_band: str) -> None:
    assert is_surface_expert_context(
        surface=surface,
        distance_band=distance_band,
        upset_probability=0.10,
        high_upset_context_threshold=0.55,
    )


@pytest.mark.parametrize("distance_band", ["mile", "long"])
def test_high_upset_turf_contexts_are_active(distance_band: str) -> None:
    assert is_surface_expert_context(
        surface="turf",
        distance_band=distance_band,
        upset_probability=0.55,
        high_upset_context_threshold=0.55,
    )


def test_high_upset_context_requires_probability_surface_and_distance() -> None:
    assert not is_surface_expert_context(
        surface="turf",
        distance_band="mile",
        upset_probability=0.54,
        high_upset_context_threshold=0.55,
    )
    assert not is_surface_expert_context(
        surface="dirt",
        distance_band="mile",
        upset_probability=0.80,
        high_upset_context_threshold=0.55,
    )
    assert not is_surface_expert_context(
        surface="turf",
        distance_band="sprint",
        upset_probability=0.80,
        high_upset_context_threshold=0.55,
    )


def test_disabled_router_preserves_champion_scores() -> None:
    decision = route_dynamic_market_scores(
        champion_scores=[3.0, 1.0],
        surface_market_scores=[1.0, 3.0],
        surface_market_free_scores=[3.0, 1.0],
        upset_probability=0.80,
        surface="turf",
        distance_band="mile",
    )

    assert decision.active is False
    assert decision.market_weight == 1.0
    assert decision.reason == "disabled"
    assert decision.routed_scores == (3.0, 1.0)


def test_enabled_router_preserves_champion_outside_selected_context() -> None:
    decision = route_dynamic_market_scores(
        champion_scores=[3.0, 1.0],
        surface_market_scores=[1.0, 3.0],
        surface_market_free_scores=[3.0, 1.0],
        upset_probability=0.20,
        surface="turf",
        distance_band="sprint",
        config=_ENABLED,
    )

    assert decision.active is False
    assert decision.market_weight == 1.0
    assert decision.reason == "outside-context"
    assert decision.routed_scores == (3.0, 1.0)


def test_enabled_router_blends_surface_expert_zscores() -> None:
    decision = route_dynamic_market_scores(
        champion_scores=[9.0, 1.0],
        surface_market_scores=[1.0, 3.0],
        surface_market_free_scores=[4.0, 2.0],
        upset_probability=1.0,
        surface="turf",
        distance_band="intermediate",
        config=_ENABLED,
    )

    assert decision.active is True
    assert decision.market_weight == pytest.approx(0.95)
    assert decision.reason == "surface-dynamic-blend"
    assert decision.routed_scores == pytest.approx((-0.9, 0.9))


def test_router_rejects_empty_scores() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        route_dynamic_market_scores(
            champion_scores=[],
            surface_market_scores=[],
            surface_market_free_scores=[],
            upset_probability=0.5,
            surface="turf",
            distance_band="mile",
            config=_ENABLED,
        )


def test_router_rejects_mismatched_score_lengths() -> None:
    with pytest.raises(ValueError, match="equal lengths"):
        route_dynamic_market_scores(
            champion_scores=[1.0, 2.0],
            surface_market_scores=[1.0],
            surface_market_free_scores=[1.0, 2.0],
            upset_probability=0.5,
            surface="turf",
            distance_band="mile",
            config=_ENABLED,
        )
