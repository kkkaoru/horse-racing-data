"""Pure shadow scorer for popularity-dependent JRA surface routing.

The scorer is deliberately not wired into the production serving path. It
encodes the fixed loop-43 rule so future races can be shadow-scored without
changing the served champion. The caller supplies the forward-only upset
probability and all three expert score arrays.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from .subgroup import (
    DISTANCE_BAND_EXTENDED,
    DISTANCE_BAND_INTERMEDIATE,
    DISTANCE_BAND_LONG,
    DISTANCE_BAND_MILE,
    DISTANCE_BAND_SPRINT,
    SURFACE_DIRT,
    SURFACE_OBSTACLE,
    SURFACE_TURF,
)
from .transformer_scorer import within_race_zscore


@dataclass(frozen=True, slots=True)
class DynamicMarketRouterConfig:
    enabled: bool
    minimum_market_weight: float
    activation_threshold: float
    gamma: float
    high_upset_context_threshold: float

    def __post_init__(self) -> None:
        if not 0.0 <= self.minimum_market_weight <= 1.0:
            raise ValueError("minimum_market_weight must be in [0, 1]")
        if not 0.0 <= self.activation_threshold < 1.0:
            raise ValueError("activation_threshold must be in [0, 1)")
        if self.gamma <= 0.0:
            raise ValueError("gamma must be positive")
        if not 0.0 <= self.high_upset_context_threshold <= 1.0:
            raise ValueError("high_upset_context_threshold must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class DynamicMarketRouteDecision:
    active: bool
    market_weight: float
    reason: str
    routed_scores: tuple[float, ...]


SELECTED_SHADOW_CONFIG: Final[DynamicMarketRouterConfig] = DynamicMarketRouterConfig(
    enabled=False,
    minimum_market_weight=0.95,
    activation_threshold=0.40,
    gamma=0.50,
    high_upset_context_threshold=0.55,
)
"""Retrospective loop-43 parameters; disabled keeps production unchanged."""


def resolve_market_weight(upset_probability: float, config: DynamicMarketRouterConfig) -> float:
    """Return the market-aware expert weight for one race."""
    if not math.isfinite(upset_probability) or not 0.0 <= upset_probability <= 1.0:
        raise ValueError("upset_probability must be finite and in [0, 1]")
    activation = (
        max(
            0.0,
            min(
                1.0,
                (upset_probability - config.activation_threshold)
                / (1.0 - config.activation_threshold),
            ),
        )
        ** config.gamma
    )
    return 1.0 - (1.0 - config.minimum_market_weight) * activation


def is_surface_expert_context(
    *,
    surface: str | None,
    distance_band: str | None,
    upset_probability: float,
    high_upset_context_threshold: float,
) -> bool:
    """Return whether loop 43 uses the surface-specialist score family."""
    if surface == SURFACE_TURF and distance_band == DISTANCE_BAND_INTERMEDIATE:
        return True
    if surface == SURFACE_DIRT and distance_band == DISTANCE_BAND_SPRINT:
        return True
    if surface == SURFACE_OBSTACLE and distance_band == DISTANCE_BAND_EXTENDED:
        return True
    return (
        upset_probability >= high_upset_context_threshold
        and surface == SURFACE_TURF
        and distance_band in (DISTANCE_BAND_MILE, DISTANCE_BAND_LONG)
    )


def route_dynamic_market_scores(
    *,
    champion_scores: Sequence[float],
    surface_market_scores: Sequence[float],
    surface_market_free_scores: Sequence[float],
    upset_probability: float,
    surface: str | None,
    distance_band: str | None,
    config: DynamicMarketRouterConfig = SELECTED_SHADOW_CONFIG,
) -> DynamicMarketRouteDecision:
    """Blend experts for shadow evaluation while preserving default serving."""
    count = len(champion_scores)
    if count == 0:
        raise ValueError("score arrays must not be empty")
    if len(surface_market_scores) != count or len(surface_market_free_scores) != count:
        raise ValueError("expert score arrays must have equal lengths")
    market_weight = resolve_market_weight(upset_probability, config)
    if not config.enabled:
        return DynamicMarketRouteDecision(
            active=False,
            market_weight=1.0,
            reason="disabled",
            routed_scores=tuple(float(score) for score in champion_scores),
        )
    if not is_surface_expert_context(
        surface=surface,
        distance_band=distance_band,
        upset_probability=upset_probability,
        high_upset_context_threshold=config.high_upset_context_threshold,
    ):
        return DynamicMarketRouteDecision(
            active=False,
            market_weight=1.0,
            reason="outside-context",
            routed_scores=tuple(float(score) for score in champion_scores),
        )
    market_z = within_race_zscore(surface_market_scores)
    market_free_z = within_race_zscore(surface_market_free_scores)
    routed = tuple(
        market_weight * market_score + (1.0 - market_weight) * market_free_score
        for market_score, market_free_score in zip(market_z, market_free_z, strict=True)
    )
    return DynamicMarketRouteDecision(
        active=True,
        market_weight=market_weight,
        reason="surface-dynamic-blend",
        routed_scores=routed,
    )
