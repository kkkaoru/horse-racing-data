"""Training-state, schedule and missing-target regression tests."""

from typing import TypedDict

import mlx.core as mx
import numpy as np
import pytest
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel

from timesfm_finish_position.chronos_mlx_training import (
    ChronosTrainStep,
    StepConfig,
    forecast_loss,
    learning_rate_schedule,
)


class InvalidStepArguments(TypedDict, total=False):
    learning_rate: float
    total_steps: int
    accumulation_steps: int
    warmup_steps: int
    grad_clip: float
    weight_decay: float


@pytest.fixture
def model() -> Chronos2MLXModel:
    mx.random.seed(3)
    result = Chronos2MLXModel(
        Chronos2MLXConfig(d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2)
    )
    result.freeze()
    result.output_patch_embedding.unfreeze()
    return result


@pytest.mark.parametrize("compile_mode", ["off", "gradient", "full"])
def test_step_updates_head(model: Chronos2MLXModel, compile_mode: str) -> None:
    config = StepConfig(warmup_steps=0)
    if compile_mode == "gradient":
        config = StepConfig(warmup_steps=0, compile_mode="gradient")
    if compile_mode == "full":
        config = StepConfig(warmup_steps=0, compile_mode="full")
    step = ChronosTrainStep(model, config)
    before = np.array(model.output_patch_embedding.output_layer.weight)
    base = np.array(model.shared.weight)
    loss = step(mx.ones((1, 2, 32)), mx.ones((1, 2, 1)))
    assert bool(mx.isfinite(loss))
    assert not np.array_equal(before, np.array(model.output_patch_embedding.output_layer.weight))
    np.testing.assert_array_equal(np.array(model.shared.weight), base)


def test_bf16_frozen_base_and_fp32_trainable_state(model: Chronos2MLXModel) -> None:
    model.set_dtype(mx.bfloat16)
    step = ChronosTrainStep(model, StepConfig(warmup_steps=0))
    assert model.shared.weight.dtype == mx.bfloat16
    assert model.output_patch_embedding.output_layer.weight.dtype == mx.float32
    assert step.optimizer.state["shared"] == {}
    moments = step.optimizer.state["output_patch_embedding"]["output_layer"]["weight"]
    assert moments["m"].dtype == mx.float32
    assert moments["v"].dtype == mx.float32
    step(mx.ones((1, 2, 32)), mx.ones((1, 2, 1)))
    assert model.shared.weight.dtype == mx.bfloat16
    assert model.output_patch_embedding.output_layer.weight.dtype == mx.float32


def test_schedule() -> None:
    schedule = learning_rate_schedule(StepConfig(learning_rate=1.0, warmup_steps=2))
    np.testing.assert_allclose(np.array(schedule(mx.array([0, 1, 2]))), [0.5, 1.0, 1.0])
    cosine = learning_rate_schedule(
        StepConfig(learning_rate=1.0, warmup_steps=2, total_steps=4, scheduler="cosine")
    )
    np.testing.assert_allclose(np.array(cosine(mx.array([0, 2, 4]))), [0.5, 1.0, 0.0])


@pytest.mark.parametrize(
    "config",
    [
        {"learning_rate": 0.0},
        {"total_steps": 0},
        {"accumulation_steps": 0},
        {"warmup_steps": -1},
        {"grad_clip": -1.0},
        {"weight_decay": -1.0},
    ],
)
def test_invalid_config(config: InvalidStepArguments) -> None:
    with pytest.raises(ValueError):
        StepConfig(**config)


@pytest.mark.parametrize("compile_mode", ["gradient", "full"])
def test_compiled_updates_match_eager(model: Chronos2MLXModel, compile_mode: str) -> None:
    reference = Chronos2MLXModel(model.config)
    reference.update(model.parameters())
    reference.freeze()
    reference.output_patch_embedding.unfreeze()
    candidate_config = StepConfig(compile_mode="gradient", warmup_steps=0)
    if compile_mode == "full":
        candidate_config = StepConfig(compile_mode="full", warmup_steps=0)
    eager = ChronosTrainStep(reference, StepConfig(warmup_steps=0))
    candidate = ChronosTrainStep(model, candidate_config)
    context = mx.random.normal((1, 4, 32))
    target = mx.random.normal((1, 4, 1))
    np.testing.assert_allclose(float(candidate(context, target)), float(eager(context, target)))
    np.testing.assert_allclose(
        float(candidate(context, target)), float(eager(context, target)), rtol=1e-5
    )
    np.testing.assert_allclose(
        np.array(model.output_patch_embedding.output_layer.weight),
        np.array(reference.output_patch_embedding.output_layer.weight),
        atol=1e-6,
    )


def test_effective_batch_update_parity(model: Chronos2MLXModel) -> None:
    reference = Chronos2MLXModel(model.config)
    reference.update(model.parameters())
    reference.freeze()
    reference.output_patch_embedding.unfreeze()
    eager = ChronosTrainStep(reference, StepConfig(warmup_steps=0))
    accumulated = ChronosTrainStep(model, StepConfig(warmup_steps=0, accumulation_steps=2))
    context = mx.random.normal((1, 4, 32))
    target = mx.random.normal((1, 4, 1))
    np.testing.assert_allclose(
        float(accumulated(context.reshape(2, 2, 32), target.reshape(2, 2, 1))),
        float(eager(context, target)),
        rtol=1e-5,
    )
    np.testing.assert_allclose(
        np.array(model.output_patch_embedding.output_layer.weight),
        np.array(reference.output_patch_embedding.output_layer.weight),
        atol=1e-6,
    )


def test_missing_target_loss(model: Chronos2MLXModel) -> None:
    loss = forecast_loss(model, mx.ones((2, 32)), mx.full((2, 1), float("nan")))
    assert float(loss) == 0.0


def test_accumulation_and_shape_validation(model: Chronos2MLXModel) -> None:
    step = ChronosTrainStep(model, StepConfig(accumulation_steps=2, grad_clip=0))
    assert bool(mx.isfinite(step(mx.ones((2, 2, 32)), mx.ones((2, 2, 1)))))
    with pytest.raises(ValueError, match="Expected"):
        step(mx.ones((2, 32)), mx.ones((2, 1)))
    with pytest.raises(ValueError, match="batch dimensions"):
        step(mx.ones((2, 2, 32)), mx.ones((2, 3, 1)))
    with pytest.raises(ValueError, match="Accumulation dimension"):
        step(mx.ones((1, 2, 32)), mx.ones((1, 2, 1)))
