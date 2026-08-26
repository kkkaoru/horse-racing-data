"""Lightweight production artifact versions for the dynamic-market shadow path."""

from __future__ import annotations

from typing import Final

SHADOW_ROUTER_VERSION: Final[str] = "jra-dynamic-market-shadow-loop43-2026"
SHADOW_SURFACES: Final[tuple[str, ...]] = ("turf", "dirt", "obstacle")
JOINT_ALTERNATE_MODEL_VERSION: Final[str] = "jra-joint-group-dro-alternate-top5-v1"
JOINT_SPECIALIST_MODEL_VERSION: Final[str] = "jra-cb-high-payout-specialist235-2026-v1"


def surface_expert_version(surface: str, *, market_free: bool) -> str:
    """Return the immutable artifact version for one shadow surface."""
    if surface not in SHADOW_SURFACES:
        raise ValueError(f"unsupported shadow surface: {surface!r}")
    role = "stage1" if market_free else "champion"
    return f"{SHADOW_ROUTER_VERSION}-{surface}-{role}"


def classifier_version() -> str:
    """Return the immutable upset-classifier artifact version."""
    return f"{SHADOW_ROUTER_VERSION}-upset-classifier"


def selected_artifact_versions() -> tuple[str, ...]:
    """Return every model version required by the enabled shadow runtime."""
    experts = tuple(
        surface_expert_version(surface, market_free=market_free)
        for surface in SHADOW_SURFACES
        for market_free in (False, True)
    )
    return (
        *experts,
        classifier_version(),
        JOINT_ALTERNATE_MODEL_VERSION,
        JOINT_SPECIALIST_MODEL_VERSION,
    )
