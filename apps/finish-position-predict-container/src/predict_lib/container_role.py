"""Typed runtime role contract for resource-specific prediction containers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from enum import StrEnum
from typing import Final

PREDICT_CONTAINER_ROLE_ENV: Final[str] = "PREDICT_CONTAINER_ROLE"
DAY_BASE_REQUIRED_CODE: Final[str] = "DAY_BASE_REQUIRED"


class PredictContainerRole(StrEnum):
    """Supported prediction-container execution roles."""

    LEGACY = "legacy"
    RACE_CHAIN = "race-chain"


class DayBaseRequiredError(RuntimeError):
    """Signal that a race-only container requires rerouting to a day-base producer."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"{DAY_BASE_REQUIRED_CODE}: {reason}")
        self.code: str = DAY_BASE_REQUIRED_CODE


def predict_container_role(
    environ: Mapping[str, str] | None = None,
) -> PredictContainerRole:
    """Return the typed role, preserving legacy behavior for absent/unknown values."""
    source = os.environ if environ is None else environ
    if source.get(PREDICT_CONTAINER_ROLE_ENV) == PredictContainerRole.RACE_CHAIN:
        return PredictContainerRole.RACE_CHAIN
    return PredictContainerRole.LEGACY
