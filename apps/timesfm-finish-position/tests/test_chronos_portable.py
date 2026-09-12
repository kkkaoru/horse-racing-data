"""CPU boundary rejects corruption/leakage and preserves sparse runner identity."""

import hashlib
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from timesfm_finish_position.chronos_forecasting import Chronos2Forecaster
from timesfm_finish_position.chronos_portable import (
    HistoryPoint,
    PortableArtifact,
    PortableChronosRuntime,
    RunnerHistory,
)
from timesfm_finish_position.domain import FloatArray


class FakeCpu(Chronos2Forecaster):
    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        assert horizon == 1
        assert len(contexts) == 1
        assert contexts[0].shape == (1, 128)
        return (np.array([[0.7]]),)


@pytest.fixture
def artifact(tmp_path: Path) -> PortableArtifact:
    weights = tmp_path / "model.safetensors"
    weights.write_bytes(b"test-model")
    config = tmp_path / "config.json"
    config.write_text('{"chronos_config": {}}', encoding="utf-8")
    return PortableArtifact(
        tmp_path,
        hashlib.sha256(weights.read_bytes()).hexdigest(),
        hashlib.sha256(config.read_bytes()).hexdigest(),
    )


def test_keyed_forecasts_and_sparse_fallback(artifact: PortableArtifact) -> None:
    runtime = PortableChronosRuntime(artifact, forecaster=FakeCpu(device="cpu"))
    result = runtime.predict(
        [
            RunnerHistory(
                "r",
                "a",
                date(2023, 1, 3),
                (HistoryPoint(date(2023, 1, 1), 0.2), HistoryPoint(date(2023, 1, 2), 0.5)),
            ),
            RunnerHistory("r", "b", date(2023, 1, 3), ()),
        ]
    )
    assert result[0].forecast == 0.7
    assert result[0].horse_id == "a"
    assert result[0].history_count == 2
    assert result[0].production_eligible is False
    assert result[1].horse_id == "b"
    assert result[1].forecast is None
    assert runtime.predict([]) == ()


def test_reject_corrupted_model(artifact: PortableArtifact) -> None:
    (artifact.directory / "model.safetensors").write_bytes(b"bad")
    with pytest.raises(ValueError, match="digest mismatch"):
        PortableChronosRuntime(artifact)


def test_reject_wrong_cpu_device(artifact: PortableArtifact) -> None:
    with pytest.raises(ValueError, match="must use CPU"):
        PortableChronosRuntime(artifact, forecaster=FakeCpu(device="mps"))


def test_history_guards(artifact: PortableArtifact) -> None:
    runtime = PortableChronosRuntime(artifact, forecaster=FakeCpu(device="cpu"))
    runner = RunnerHistory("r", "a", date(2023, 1, 1), ())
    with pytest.raises(ValueError, match="Duplicate runner"):
        runtime.predict([runner, runner])
    with pytest.raises(ValueError, match="identifiers"):
        runtime.predict([RunnerHistory("", "a", date(2023, 1, 1), ())])
    with pytest.raises(ValueError, match="Same-day"):
        runtime.predict(
            [RunnerHistory("r", "a", date(2023, 1, 1), (HistoryPoint(date(2023, 1, 1), 0.5),))]
        )
    with pytest.raises(ValueError, match="unique chronological"):
        runtime.predict(
            [
                RunnerHistory(
                    "r",
                    "a",
                    date(2023, 1, 3),
                    (HistoryPoint(date(2023, 1, 2), 0.2), HistoryPoint(date(2023, 1, 1), 0.5)),
                )
            ]
        )
    with pytest.raises(ValueError, match="finite"):
        runtime.predict(
            [
                RunnerHistory(
                    "r", "a", date(2023, 1, 3), (HistoryPoint(date(2023, 1, 1), float("nan")),)
                )
            ]
        )
