from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pytest

import timesfm_finish_position.forecasting as subject
from timesfm_finish_position.forecasting import (
    ScratchAutoregressiveForecaster,
    TimesFm3Forecaster,
    resolve_timesfm_device,
)


@dataclass(frozen=True)
class FakeOutput:
    forecast: np.ndarray | None


class FakeEvaluator:
    def __init__(self, config: object) -> None:
        self.config = config

    def predict_batch(
        self,
        *,
        contexts: list[np.ndarray],
        horizon: int,
        return_quantiles: bool,
        use_symmetric_averaging: bool,
    ) -> list[FakeOutput]:
        del return_quantiles, use_symmetric_averaging
        return [FakeOutput(np.ones((context.shape[0], horizon))) for context in contexts]


class MissingForecastEvaluator(FakeEvaluator):
    def predict_batch(
        self,
        *,
        contexts: list[np.ndarray],
        horizon: int,
        return_quantiles: bool,
        use_symmetric_averaging: bool,
    ) -> list[FakeOutput]:
        del contexts, horizon, return_quantiles, use_symmetric_averaging
        return [FakeOutput(None)]


class WrongShapeEvaluator(FakeEvaluator):
    def predict_batch(
        self,
        *,
        contexts: list[np.ndarray],
        horizon: int,
        return_quantiles: bool,
        use_symmetric_averaging: bool,
    ) -> list[FakeOutput]:
        del contexts, horizon, return_quantiles, use_symmetric_averaging
        return [FakeOutput(np.ones((1, 1)))]


class OmittedEvaluator(FakeEvaluator):
    def predict_batch(
        self,
        *,
        contexts: list[np.ndarray],
        horizon: int,
        return_quantiles: bool,
        use_symmetric_averaging: bool,
    ) -> list[FakeOutput]:
        del contexts, horizon, return_quantiles, use_symmetric_averaging
        return []


def test_timesfm_forecaster_uses_official_multivariate_evaluator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "TimesFM3Evaluator", FakeEvaluator)
    forecaster = TimesFm3Forecaster("checkpoint", 2, "mps")
    result = forecaster.predict((np.zeros((21, 32)),), horizon=3)
    assert forecaster.backend == "pytorch-mps"
    assert result[0].shape == (21, 3)
    assert result[0].tolist()[0] == [1.0, 1.0, 1.0]


def test_timesfm_forecaster_returns_empty_without_contexts() -> None:
    forecaster = TimesFm3Forecaster("checkpoint", 2, "cpu")
    assert forecaster.predict((), horizon=1) == ()


def test_timesfm_forecaster_rejects_nonpositive_horizon() -> None:
    forecaster = TimesFm3Forecaster("checkpoint", 2, "cpu")
    with pytest.raises(ValueError, match="horizon must be positive"):
        forecaster.predict((np.zeros((21, 32)),), horizon=0)


def test_timesfm_forecaster_rejects_missing_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "TimesFM3Evaluator", MissingForecastEvaluator)
    forecaster = TimesFm3Forecaster("checkpoint", 2, "cpu")
    with pytest.raises(RuntimeError, match="returned no point forecast"):
        forecaster.predict((np.zeros((21, 32)),), horizon=1)


def test_timesfm_forecaster_rejects_wrong_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "TimesFM3Evaluator", WrongShapeEvaluator)
    forecaster = TimesFm3Forecaster("checkpoint", 2, "cpu")
    with pytest.raises(RuntimeError, match="unexpected TimesFM forecast shape"):
        forecaster.predict((np.zeros((21, 32)),), horizon=2)


def test_timesfm_forecaster_rejects_omitted_cell(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "TimesFM3Evaluator", OmittedEvaluator)
    forecaster = TimesFm3Forecaster("checkpoint", 2, "cpu")
    with pytest.raises(RuntimeError, match="omitted a cell forecast"):
        forecaster.predict((np.zeros((21, 32)),), horizon=1)


def test_resolve_timesfm_device_honors_requested_value() -> None:
    assert resolve_timesfm_device("cpu") == "cpu"


def test_resolve_timesfm_device_selects_mps_on_apple_silicon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(subject.platform, "machine", lambda: "arm64")
    assert resolve_timesfm_device(None) == "mps"


def test_scratch_forecaster_uses_constant_for_thin_history() -> None:
    forecaster = ScratchAutoregressiveForecaster(lags=16)
    result = forecaster.predict((np.ones((21, 8)),), horizon=2)
    assert result[0].shape == (21, 2)
    assert result[0][0].tolist() == [0.0, 0.0]


def test_scratch_forecaster_fits_numpy_ar_off_mac(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject.platform, "system", lambda: "Linux")
    context = np.tile(np.linspace(-1.0, 1.0, 64), (21, 1))
    forecaster = ScratchAutoregressiveForecaster(lags=4)
    result = forecaster.predict((context,), horizon=3)
    assert forecaster.backend == "numpy"
    assert result[0].shape == (21, 3)
    assert np.isfinite(result[0]).all()


def test_scratch_forecaster_fits_mlx_on_apple_silicon(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(subject.platform, "machine", lambda: "arm64")
    context = np.tile(np.linspace(-1.0, 1.0, 64), (21, 1))
    forecaster = ScratchAutoregressiveForecaster(lags=4)
    result = forecaster.predict((context,), horizon=2)
    assert forecaster.backend == "mlx"
    assert result[0].shape == (21, 2)
    assert np.isfinite(result[0]).all()


def test_scratch_forecaster_validates_lags_and_horizon() -> None:
    invalid_lags = ScratchAutoregressiveForecaster(lags=0)
    with pytest.raises(ValueError, match="lags must be positive"):
        invalid_lags.predict((np.zeros((21, 32)),), horizon=1)
    valid = ScratchAutoregressiveForecaster(lags=4)
    with pytest.raises(ValueError, match="horizon must be positive"):
        valid.predict((np.zeros((21, 32)),), horizon=0)
    assert valid.predict((), horizon=1) == ()
