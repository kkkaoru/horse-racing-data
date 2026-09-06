from __future__ import annotations

from collections.abc import Sequence
from types import SimpleNamespace

import numpy as np
import pytest

from timesfm_finish_position.chronos_forecasting import Chronos2Forecaster


class FakeTensor:
    def __init__(self, values: np.ndarray) -> None:
        self.values = values

    def detach(self) -> FakeTensor:
        return self

    def cpu(self) -> FakeTensor:
        return self

    def numpy(self) -> object:
        return self.values


class FakePipeline:
    def __init__(self, *, quantiles: bool = False, omit: bool = False) -> None:
        self.quantiles = quantiles
        self.omit = omit

    def predict(
        self,
        inputs: Sequence[np.ndarray],
        *,
        prediction_length: int,
        batch_size: int,
    ) -> list[FakeTensor]:
        assert batch_size == 8
        outputs = []
        for context in inputs:
            point = np.repeat(context[-1], prediction_length).astype(np.float64)
            values = np.vstack((point - 1.0, point, point + 1.0)) if self.quantiles else point
            outputs.append(FakeTensor(values))
        return outputs[:-1] if self.omit else outputs


def _forecaster(pipeline: FakePipeline) -> Chronos2Forecaster:
    return Chronos2Forecaster(batch_size=8, device="cpu", pipeline=pipeline)


def test_chronos_forecaster_flattens_and_restores_variates() -> None:
    forecaster = _forecaster(FakePipeline(quantiles=True))
    contexts = (
        np.asarray([[1.0, 2.0], [10.0, 20.0]], dtype=np.float64),
        np.asarray([[3.0, 4.0]], dtype=np.float64),
    )
    outputs = forecaster.predict(contexts, horizon=2)
    assert forecaster.backend == "pytorch-cpu"
    assert outputs[0].tolist() == [[2.0, 2.0], [20.0, 20.0]]
    assert outputs[1].tolist() == [[4.0, 4.0]]


def test_chronos_dynamic_device_and_pipeline_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("platform.system", lambda: "Darwin")
    monkeypatch.setattr("platform.machine", lambda: "arm64")
    assert Chronos2Forecaster().backend == "pytorch-mps"

    class FakeCuda:
        @staticmethod
        def is_available() -> bool:
            return False

    class FakeChronosClass:
        @classmethod
        def from_pretrained(cls, checkpoint: str, **kwargs: object) -> FakePipeline:
            assert checkpoint == "checkpoint"
            assert kwargs == {"device_map": "cpu"}
            return FakePipeline()

    def fake_import(name: str) -> object:
        if name == "torch":
            return SimpleNamespace(cuda=FakeCuda())
        assert name == "chronos"
        return SimpleNamespace(Chronos2Pipeline=FakeChronosClass)

    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("importlib.import_module", fake_import)
    forecaster = Chronos2Forecaster(checkpoint="checkpoint", batch_size=8)
    assert forecaster.backend == "pytorch-cpu"
    assert forecaster.predict((np.asarray([[1.0, 2.0]]),), horizon=1)[0].tolist() == [[2.0]]


def test_chronos_forecaster_validates_horizon_outputs_and_shapes() -> None:
    context = (np.asarray([[1.0, 2.0]], dtype=np.float64),)
    forecaster = _forecaster(FakePipeline())
    assert forecaster.predict((), horizon=1) == ()
    with pytest.raises(ValueError, match="horizon must be positive"):
        forecaster.predict(context, horizon=0)
    with pytest.raises(RuntimeError, match="omitted a variate"):
        _forecaster(FakePipeline(omit=True)).predict(context, horizon=1)
    assert Chronos2Forecaster.point_forecast(FakeTensor(np.asarray(3.0)), 1).tolist() == [3.0]
    assert Chronos2Forecaster.point_forecast(FakeTensor(np.arange(5.0)), 1).tolist() == [2.0]
    nested = FakeTensor(np.asarray([[[1.0, 2.0]]]))
    assert Chronos2Forecaster.point_forecast(nested, 2).tolist() == [1.0, 2.0]
    with pytest.raises(RuntimeError, match="unexpected Chronos-2 forecast shape"):
        Chronos2Forecaster.point_forecast(FakeTensor(np.zeros((2, 2, 2))), 2)
