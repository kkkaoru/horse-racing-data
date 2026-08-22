"""Tests for the resource-specific prediction-container role contract."""

from __future__ import annotations

import pytest

from predict_lib.container_role import (
    DAY_BASE_REQUIRED_CODE,
    DayBaseRequiredError,
    PredictContainerRole,
    predict_container_role,
)


def test_predict_container_role_defaults_to_legacy() -> None:
    assert predict_container_role({}) is PredictContainerRole.LEGACY


def test_predict_container_role_reads_race_chain() -> None:
    assert predict_container_role({"PREDICT_CONTAINER_ROLE": "race-chain"}) is (
        PredictContainerRole.RACE_CHAIN
    )


def test_predict_container_role_treats_unknown_value_as_legacy() -> None:
    assert predict_container_role({"PREDICT_CONTAINER_ROLE": "day-base"}) is (
        PredictContainerRole.LEGACY
    )


def test_predict_container_role_reads_process_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PREDICT_CONTAINER_ROLE", "race-chain")
    assert predict_container_role() is PredictContainerRole.RACE_CHAIN


def test_day_base_required_error_exposes_machine_code() -> None:
    error = DayBaseRequiredError("r2 object missing")
    assert error.code == DAY_BASE_REQUIRED_CODE
    assert str(error) == "DAY_BASE_REQUIRED: r2 object missing"
