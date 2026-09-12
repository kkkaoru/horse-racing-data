"""Repeatable synthetic runtime benchmark, never an accuracy-selection gate."""

import argparse
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import mlx.core as mx
import numpy as np
from chronos2_mlx.pipeline import Chronos2MLXPipeline
from mlx.utils import tree_flatten

from timesfm_finish_position.chronos_mlx_kernels import enable_fused_attention
from timesfm_finish_position.chronos_mlx_training import ChronosTrainStep, StepConfig, forecast_loss


@dataclass(frozen=True)
class BenchmarkConfig:
    dtype: Literal["float32", "bfloat16"] = "float32"
    compile_mode: Literal["off", "gradient", "full"] = "off"
    fused: bool = False
    batch_size: int = 8
    context_length: int = 128
    steps: int = 100
    seed: int = 42

    def __post_init__(self) -> None:
        if self.steps < 4 or self.batch_size < 1 or self.context_length < 16:
            raise ValueError("Require steps >=4, batch >=1 and context >=16")


def benchmark(config: BenchmarkConfig) -> dict[str, object]:
    """Measure warm updates separately from load/first-step and retain predictions."""
    if os.environ.get("MLX_ENABLE_TF32") != "0":
        raise ValueError("Launch with MLX_ENABLE_TF32=0 before importing MLX")
    mx.random.seed(config.seed)
    rng = np.random.default_rng(config.seed)
    contexts = mx.array(
        rng.normal(size=(1, config.batch_size, config.context_length)).astype(np.float32)
    )
    targets = mx.array(rng.normal(size=(1, config.batch_size, 1)).astype(np.float32))
    validation = mx.array(
        rng.normal(size=(config.batch_size, config.context_length)).astype(np.float32)
    )
    validation_target = mx.array(rng.normal(size=(config.batch_size, 1)).astype(np.float32))
    start = time.perf_counter()
    pipeline = Chronos2MLXPipeline.from_pretrained("amazon/chronos-2", dtype=config.dtype)
    load_seconds = time.perf_counter() - start
    model = pipeline.model
    if config.fused:
        enable_fused_attention(model)
    model.freeze()
    model.output_patch_embedding.unfreeze()
    step = ChronosTrainStep(
        model,
        StepConfig(
            learning_rate=1e-5,
            warmup_steps=0,
            total_steps=config.steps,
            compile_mode=config.compile_mode,
        ),
    )
    mx.eval(model.state, step.optimizer.state, contexts, targets)
    mx.reset_peak_memory()
    initial_loss = float(forecast_loss(model, validation, validation_target))
    times: list[float] = []
    losses: list[float] = []
    for _ in range(config.steps):
        start = time.perf_counter()
        loss = step(contexts, targets)
        times.append(time.perf_counter() - start)
        losses.append(float(loss))
    warm_median = float(np.median(times[2:]))
    return {
        "config": asdict(config),
        "synthetic": True,
        "production_eligible": False,
        "tf32": False,
        "mode": "output-head-only",
        "trainable_dtype": "float32",
        "load_seconds": load_seconds,
        "first_step_seconds": times[0],
        "warm_step_seconds": warm_median,
        "warm_samples_per_second": config.batch_size / warm_median,
        "peak_bytes": mx.get_peak_memory(),
        "initial_validation_loss": initial_loss,
        "final_validation_loss": float(forecast_loss(model, validation, validation_target)),
        "train_losses": losses,
        "step_seconds": times,
        "trainable_parameters": sum(
            value.size
            for _, value in tree_flatten(model.trainable_parameters())
            if isinstance(value, mx.array)
        ),
        "prediction": np.array(model(validation).astype(mx.float32)).tolist(),
        "gpu_utilization": None,
        "gpu_utilization_reason": "No privileged hardware sampler enabled",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dtype", choices=["float32", "bfloat16"], default="float32")
    parser.add_argument("--compile-mode", choices=["off", "gradient", "full"], default="off")
    parser.add_argument("--fused", action="store_true")
    parser.add_argument("--steps", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--context-length", type=int, default=128)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = benchmark(
        BenchmarkConfig(
            dtype=args.dtype,
            compile_mode=args.compile_mode,
            fused=args.fused,
            steps=args.steps,
            batch_size=args.batch_size,
            context_length=args.context_length,
        )
    )
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)


if __name__ == "__main__":
    main()
