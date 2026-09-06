"""Chronos-2 adapter for the common multivariate temporal-forecaster contract."""

from __future__ import annotations

import importlib
import platform
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol, cast

import numpy as np

from .domain import FloatArray


class TensorLike(Protocol):
    """Minimal predicted tensor boundary."""

    def detach(self) -> TensorLike:
        """Detach from autograd."""
        ...

    def cpu(self) -> TensorLike:
        """Move to CPU."""
        ...

    def numpy(self) -> object:
        """Return array values."""
        ...


class ChronosPipelineLike(Protocol):
    """Minimal Chronos-2 inference boundary."""

    def predict(
        self,
        inputs: Sequence[FloatArray],
        *,
        prediction_length: int,
        batch_size: int,
    ) -> Sequence[TensorLike]:
        """Forecast univariate contexts."""
        ...


class ClassMethodDescriptor(Protocol):
    """Descriptor boundary for a dynamically imported classmethod."""

    def __get__(self, instance: None, owner: object) -> FromPretrained:
        """Bind the classmethod to its owner."""
        ...


class FromPretrained(Protocol):
    """Dynamic Chronos pipeline loader."""

    def __call__(self, checkpoint: str, **kwargs: object) -> ChronosPipelineLike:
        """Load one frozen checkpoint."""
        ...


@dataclass
class Chronos2Forecaster:
    """Flatten multivariate contexts into batched Chronos-2 univariate calls."""

    checkpoint: str = "amazon/chronos-2"
    batch_size: int = 256
    device: str | None = None
    pipeline: ChronosPipelineLike | None = field(default=None, repr=False)

    @property
    def backend(self) -> str:
        """Return auditable runtime routing."""
        return f"pytorch-{self._resolved_device()}"

    def _resolved_device(self) -> str:
        if self.device is not None:
            return self.device
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            return "mps"
        torch_module = importlib.import_module("torch")
        cuda = vars(torch_module)["cuda"]
        return "cuda" if bool(cuda.is_available()) else "cpu"

    def _load_pipeline(self) -> ChronosPipelineLike:
        if self.pipeline is None:
            module = importlib.import_module("chronos")
            pipeline_class = vars(module)["Chronos2Pipeline"]
            descriptor = cast("ClassMethodDescriptor", vars(pipeline_class)["from_pretrained"])
            loader = descriptor.__get__(None, pipeline_class)
            self.pipeline = loader(self.checkpoint, device_map=self._resolved_device())
        return self.pipeline

    @staticmethod
    def point_forecast(output: TensorLike, horizon: int) -> FloatArray:
        values = np.asarray(output.detach().cpu().numpy(), dtype=np.float64)
        while values.ndim > 2 and values.shape[0] == 1:
            values = values[0]
        if values.ndim == 2 and values.shape[1] == horizon:
            return values[values.shape[0] // 2]
        if values.ndim == 1 and values.shape == (horizon,):
            return values
        if values.ndim == 1 and horizon == 1:
            return np.asarray([values[len(values) // 2]], dtype=np.float64)
        if values.ndim == 0 and horizon == 1:
            return np.asarray([float(values)], dtype=np.float64)
        raise RuntimeError(f"unexpected Chronos-2 forecast shape: {values.shape}")

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        """Forecast each variate independently and restore multivariate shape."""
        if horizon < 1:
            raise ValueError("horizon must be positive")
        if not contexts:
            return ()
        variates = [context.shape[0] for context in contexts]
        flattened = [context[row] for context in contexts for row in range(context.shape[0])]
        outputs = self._load_pipeline().predict(
            flattened, prediction_length=horizon, batch_size=self.batch_size
        )
        if len(outputs) != len(flattened):
            raise RuntimeError("Chronos-2 omitted a variate forecast")
        points = [self.point_forecast(output, horizon) for output in outputs]
        restored: list[FloatArray] = []
        offset = 0
        for count in variates:
            restored.append(np.asarray(points[offset : offset + count], dtype=np.float64))
            offset += count
        return tuple(restored)
