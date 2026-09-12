"""Static-shape Chronos-2 training primitives for measured MLX experiments."""

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import mlx.core as mx
import mlx.nn as nn
import mlx.optimizers as optim
from chronos2_mlx.model import Chronos2MLXModel
from mlx.utils import tree_flatten, tree_unflatten


@dataclass(frozen=True)
class StepConfig:
    """Optimizer-step settings; batch dimension includes accumulation steps."""

    learning_rate: float = 1e-4
    weight_decay: float = 0.01
    warmup_steps: int = 100
    total_steps: int = 1000
    accumulation_steps: int = 1
    grad_clip: float = 1.0
    compile_mode: Literal["off", "gradient", "full"] = "off"
    scheduler: Literal["constant", "cosine"] = "constant"

    def __post_init__(self) -> None:
        if self.learning_rate <= 0 or not math.isfinite(self.learning_rate):
            raise ValueError("learning_rate must be positive and finite")
        if self.total_steps < 1 or self.accumulation_steps < 1 or self.warmup_steps < 0:
            raise ValueError("Invalid step counts")
        if self.weight_decay < 0 or self.grad_clip < 0:
            raise ValueError("weight_decay and grad_clip must be nonnegative")
        if self.compile_mode not in ("off", "gradient", "full"):
            raise ValueError("Unknown compile mode")
        if self.scheduler not in ("constant", "cosine"):
            raise ValueError("Unknown scheduler")


def learning_rate_schedule(config: StepConfig) -> Callable[[mx.array], mx.array]:
    """Linear warmup counts optimizer updates, not microbatches."""

    def schedule(step: mx.array) -> mx.array:
        warmup = mx.minimum((step + 1) / max(1, config.warmup_steps), 1.0)
        if config.scheduler == "constant":
            return config.learning_rate * warmup
        progress = mx.clip(
            (step - config.warmup_steps) / max(1, config.total_steps - config.warmup_steps),
            0.0,
            1.0,
        )
        return config.learning_rate * warmup * (1.0 + mx.cos(math.pi * progress)) / 2.0

    return schedule


def forecast_loss(model: Chronos2MLXModel, context: mx.array, target: mx.array) -> mx.array:
    """Normalized pinball loss with FP32 accumulation and explicit missing targets."""
    horizon = target.shape[1]
    patches = math.ceil(horizon / model.config.output_patch_size)
    hidden, loc_scale, _, _ = model.encode(context=context, num_output_patches=patches)
    predictions = model.output_patch_embedding(hidden[:, -patches:])
    predictions = (
        predictions.reshape(
            context.shape[0], patches, model.config.num_quantiles, model.config.output_patch_size
        )
        .transpose(0, 2, 1, 3)
        .reshape(context.shape[0], model.config.num_quantiles, -1)
    )
    predictions = predictions[:, :, :horizon].astype(mx.float32)
    normalized, _ = model.instance_norm(target.astype(mx.float32), loc_scale)
    valid = mx.isfinite(target)
    normalized = mx.where(valid, normalized, 0.0)
    error = normalized[:, None, :] - predictions
    quantiles = model.quantiles.astype(mx.float32)[None, :, None]
    loss = 2.0 * mx.maximum(quantiles * error, (quantiles - 1.0) * error)
    return mx.where(valid[:, None, :], loss, 0.0).mean(axis=-1).sum(axis=-1).mean()


class ChronosTrainStep:
    """Own state capture so compiled optimizer updates cannot silently be lost."""

    def __init__(self, model: Chronos2MLXModel, config: StepConfig) -> None:
        self.model = model
        self.config = config
        # Keep trainable master parameters and AdamW moments in FP32 from step zero.
        # Frozen BF16 base weights remain BF16; no moments are allocated for them.
        master_parameters: list[tuple[str, mx.array]] = []
        for name, value in tree_flatten(model.trainable_parameters()):
            if not isinstance(value, mx.array):
                raise TypeError(f"Trainable parameter {name} is not an MLX array")
            master_parameters.append((name, value.astype(mx.float32)))
        model.update(tree_unflatten(master_parameters))
        self.optimizer = optim.AdamW(
            learning_rate=learning_rate_schedule(config), weight_decay=config.weight_decay
        )
        self.optimizer.init(model.trainable_parameters())
        self.raw_gradient = nn.value_and_grad(model, forecast_loss)
        self.gradient = (
            mx.compile(self._gradient, inputs=model.state, outputs=model.state)
            if config.compile_mode == "gradient"
            else self._gradient
        )
        state = [model.state, self.optimizer.state]
        self.step = (
            mx.compile(self._step, inputs=state, outputs=state)
            if config.compile_mode == "full"
            else self._step
        )

    def _gradient(self, context: mx.array, target: mx.array) -> tuple[mx.array, object]:
        return self.raw_gradient(self.model, context, target)

    def _step(self, contexts: mx.array, targets: mx.array) -> mx.array:
        accumulated: dict[str, mx.array] = {}
        total_loss = mx.array(0.0, dtype=mx.float32)
        for index in range(self.config.accumulation_steps):
            loss, gradients = self.gradient(contexts[index], targets[index])
            flat = dict(tree_flatten(gradients))
            accumulated = {
                key: accumulated.get(key, mx.zeros_like(value)).astype(mx.float32)
                + value.astype(mx.float32)
                for key, value in flat.items()
            }
            total_loss = total_loss + loss.astype(mx.float32)
        averaged = tree_unflatten(
            [(key, value / self.config.accumulation_steps) for key, value in accumulated.items()]
        )
        if self.config.grad_clip > 0:
            averaged, _ = optim.clip_grad_norm(averaged, self.config.grad_clip)
        self.optimizer.update(self.model, averaged)
        return total_loss / self.config.accumulation_steps

    def __call__(self, contexts: mx.array, targets: mx.array) -> mx.array:
        if contexts.ndim != 3 or targets.ndim != 3:
            raise ValueError("Expected [accumulation, microbatch, sequence] arrays")
        if contexts.shape[:2] != targets.shape[:2]:
            raise ValueError("Context and target batch dimensions must match")
        if contexts.shape[0] != self.config.accumulation_steps:
            raise ValueError("Accumulation dimension does not match configuration")
        loss = self.step(contexts, targets)
        mx.eval(loss, self.model.state, self.optimizer.state)
        return loss
