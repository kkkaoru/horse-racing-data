"""Typed runtime role contract for resource-specific prediction containers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

PREDICT_CONTAINER_ROLE_ENV: Final[str] = "PREDICT_CONTAINER_ROLE"
DAY_BASE_REQUIRED_CODE: Final[str] = "DAY_BASE_REQUIRED"
RESCORE_ONLY_ERROR: Final[str] = "RESCORE_ONLY_REQUIRED: single-race attested rescore is required"


class PredictContainerRole(StrEnum):
    """Supported prediction-container execution roles."""

    LEGACY = "legacy"
    RACE_CHAIN = "race-chain"
    RESCORE = "rescore"


@dataclass(frozen=True)
class RescoreRoleRequest:
    mode: str
    single_race: bool
    attested: bool
    rescore_available: bool


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
    try:
        return PredictContainerRole(source.get(PREDICT_CONTAINER_ROLE_ENV))
    except ValueError:
        return PredictContainerRole.LEGACY


def rescore_role_allows(
    request: RescoreRoleRequest,
    environ: Mapping[str, str] | None = None,
) -> bool:
    """A small role may never reach the unattested rescore-to-full fallback."""
    return predict_container_role(environ) != PredictContainerRole.RESCORE or (
        request.mode == "rescore"
        and request.single_race
        and request.attested
        and request.rescore_available
    )
