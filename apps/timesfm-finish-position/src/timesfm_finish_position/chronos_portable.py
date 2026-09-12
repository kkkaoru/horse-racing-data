"""MLX-free, digest-verified CPU temporal feature boundary for container use."""

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np

from timesfm_finish_position.chronos_forecasting import Chronos2Forecaster


@dataclass(frozen=True)
class PortableArtifact:
    directory: Path
    model_sha256: str
    config_sha256: str
    context_length: int = 128
    minimum_history: int = 2

    def verify(self) -> None:
        if self.context_length < 1 or self.minimum_history < 1:
            raise ValueError("Artifact history limits must be positive")
        _verify_digest(self.directory / "model.safetensors", self.model_sha256)
        _verify_digest(self.directory / "config.json", self.config_sha256)
        config = json.loads((self.directory / "config.json").read_text(encoding="utf-8"))
        if not isinstance(config, dict) or "chronos_config" not in config or "auto_map" in config:
            raise ValueError("Expected standard local Chronos-2 config without remote code")


@dataclass(frozen=True)
class HistoryPoint:
    race_date: date
    value: float


@dataclass(frozen=True)
class RunnerHistory:
    race_id: str
    horse_id: str
    evaluation_date: date
    history: tuple[HistoryPoint, ...]


@dataclass(frozen=True)
class TemporalFeature:
    race_id: str
    horse_id: str
    forecast: float | None
    history_count: int
    model_sha256: str
    production_eligible: bool = False


def _verify_digest(path: Path, expected: str) -> None:
    if len(expected) != 64 or any(character not in "0123456789abcdef" for character in expected):
        raise ValueError("Expected lowercase SHA-256 digest")
    if path.is_symlink():
        raise ValueError("Portable artifact files must not be symlinks")
    with path.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != expected:
        raise ValueError(f"Artifact digest mismatch: {path.name}")


def _context(runner: RunnerHistory, artifact: PortableArtifact) -> np.ndarray | None:
    if not runner.race_id or not runner.horse_id:
        raise ValueError("Exact race and horse identifiers are required")
    dates = [point.race_date for point in runner.history]
    if dates != sorted(dates) or len(set(dates)) != len(dates):
        raise ValueError("Runner history must have unique chronological dates")
    if any(point.race_date >= runner.evaluation_date for point in runner.history):
        raise ValueError("Same-day or future outcome in runner history")
    if any(not math.isfinite(point.value) for point in runner.history):
        raise ValueError("History values must be finite")
    if len(runner.history) < artifact.minimum_history:
        return None
    # Match training's fixed left padding, including position encodings.
    context = np.full((1, artifact.context_length), np.nan, dtype=np.float64)
    values = [point.value for point in runner.history[-artifact.context_length :]]
    context[0, -len(values) :] = values
    return context


class PortableChronosRuntime:
    """Return keyed research features, never mutate incumbent scores or heatmaps.

    A production orchestrator must apply its separately attested routing and
    accuracy gates. This boundary deliberately cannot mark a model eligible.
    """

    def __init__(
        self, artifact: PortableArtifact, *, forecaster: Chronos2Forecaster | None = None
    ) -> None:
        artifact.verify()
        self.artifact = artifact
        self.forecaster = forecaster or Chronos2Forecaster(
            checkpoint=str(artifact.directory.resolve()), device="cpu", batch_size=64
        )
        if self.forecaster.device != "cpu":
            raise ValueError("Portable container inference must use CPU")

    def predict(self, runners: Sequence[RunnerHistory]) -> tuple[TemporalFeature, ...]:
        keys = [(runner.race_id, runner.horse_id) for runner in runners]
        if len(set(keys)) != len(keys):
            raise ValueError("Duplicate runner identity")
        contexts = [_context(runner, self.artifact) for runner in runners]
        known = [context for context in contexts if context is not None]
        forecasts = self.forecaster.predict(known, horizon=1) if known else ()
        if len(forecasts) != len(known):
            raise ValueError("CPU runtime omitted runner forecasts")
        result: list[TemporalFeature] = []
        offset = 0
        for runner, context in zip(runners, contexts, strict=True):
            point: float | None = None
            if context is not None:
                forecast = forecasts[offset]
                if forecast.shape != (1, 1) or not np.isfinite(forecast).all():
                    raise ValueError("Invalid CPU forecast shape or value")
                point = float(forecast[0, 0])
                offset += 1
            result.append(
                TemporalFeature(
                    runner.race_id,
                    runner.horse_id,
                    point,
                    len(runner.history),
                    self.artifact.model_sha256,
                )
            )
        return tuple(result)
