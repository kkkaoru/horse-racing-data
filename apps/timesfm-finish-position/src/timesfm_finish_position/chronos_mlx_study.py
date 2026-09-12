"""Bounded, reproducible head-only study using strict PIT horse histories."""

import argparse
import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Literal

import mlx.core as mx
import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.parquet as pq
from chronos2_mlx.adapters import LoRAConfig, LoRALinear, apply_lora
from chronos2_mlx.model import Chronos2MLXModel
from chronos2_mlx.pipeline import Chronos2MLXPipeline

from timesfm_finish_position.chronos_domains import TrainingDomain, domain_contexts, domain_labels
from timesfm_finish_position.chronos_mlx_checkpoint import restore_checkpoint, save_checkpoint
from timesfm_finish_position.chronos_mlx_data import (
    HorseWindows,
    StaticWindowBatches,
    WindowConfig,
    build_windows,
    filter_window_targets,
)
from timesfm_finish_position.chronos_mlx_export import export_pretrained
from timesfm_finish_position.chronos_mlx_kernels import enable_finite_attention_masks
from timesfm_finish_position.chronos_mlx_loading import load_local_pipeline
from timesfm_finish_position.chronos_mlx_training import ChronosTrainStep, StepConfig


@dataclass(frozen=True)
class StudyConfig:
    steps: int = 1000
    batch_size: int = 32
    context_length: int = 128
    minimum_history: int = 2
    seed: int = 42
    mode: Literal["head", "lora"] = "head"
    learning_rate: float = 1e-5
    weight_decay: float = 0.01
    warmup_steps: int = 100
    lora_rank: int = 8
    lora_alpha: float = 16.0
    training_start: str = "20200101"
    training_end: str = "20221231"
    development_start: str = "20230101"
    development_end: str = "20231231"
    target_race_prefix: str | None = None
    training_domain: TrainingDomain | None = None

    def __post_init__(self) -> None:
        values = (
            self.training_start,
            self.training_end,
            self.development_start,
            self.development_end,
        )
        if any(len(value) != 8 or not value.isdecimal() for value in values):
            raise ValueError("Study dates must use YYYYMMDD")
        first, last, dev_first, dev_last = (date.fromisoformat(value) for value in values)
        if not first <= last < dev_first <= dev_last:
            raise ValueError("Training and development dates must be separated")


def configure_adaptation(
    model: Chronos2MLXModel,
    mode: Literal["head", "lora"],
    *,
    lora_rank: int = 8,
    lora_alpha: float = 16.0,
) -> None:
    enable_finite_attention_masks(model)
    model.freeze()
    if mode == "head":
        model.output_patch_embedding.unfreeze()
        return
    paths = apply_lora(model, LoRAConfig(rank=lora_rank, alpha=lora_alpha))
    if not paths:
        raise ValueError("No LoRA projections injected")
    for _, module in model.named_modules():
        if isinstance(module, LoRALinear):
            module.unfreeze()
            module.base.freeze()


def predict_windows(
    pipeline: Chronos2MLXPipeline, windows: HorseWindows
) -> npt.NDArray[np.float32]:
    predictions: list[npt.NDArray[np.float32]] = []
    for start in range(0, len(windows.contexts), 64):
        forecast = pipeline.predict(
            mx.array(windows.contexts[start : start + 64]), prediction_length=1
        )
        values = np.array(forecast.astype(mx.float32))[:, :, 0]
        if not np.all(np.isfinite(values)):
            raise ValueError("Nonfinite forecast: stop before training or export")
        predictions.append(values)
    if not predictions:
        raise ValueError("No development windows")
    return np.concatenate(predictions)


def _scatter_predictions(
    *,
    indices: npt.NDArray[np.int64],
    windows: HorseWindows,
    predictions: npt.NDArray[np.float32],
    total_rows: int,
) -> npt.NDArray[np.float32]:
    result = np.full(total_rows, np.nan, dtype=np.float32)
    result[windows.source_indices] = predictions[:, predictions.shape[1] // 2]
    return result[indices]


def run_study(
    *,
    history: Path,
    source_config: Path,
    output: Path,
    config: StudyConfig,
    resume: Path | None = None,
    checkpoint_every: int = 250,
) -> dict[str, object]:
    if os.environ.get("MLX_ENABLE_TF32") != "0":
        raise ValueError("Use the repository MLX launcher with TF32 disabled")
    if config.steps < 1 or checkpoint_every < 1:
        raise ValueError("Training steps and checkpoint interval must be positive")
    output.mkdir(parents=True, exist_ok=False)
    with history.open("rb") as stream:
        data_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    table = pq.read_table(history)
    if config.training_domain is not None:
        venues = np.asarray(table["venue_code"].to_pylist(), dtype=np.str_)
        table = table.filter(pa.array(domain_contexts(venues, config.training_domain)))
    horse_ids = np.asarray(table["horse_id"].to_pylist(), dtype=np.str_)
    dates = np.asarray(table["race_date"].to_pylist(), dtype=np.str_)
    values = np.asarray(table["performance_rating"].to_pylist(), dtype=np.float32)
    train = build_windows(
        horse_ids=horse_ids,
        dates=dates,
        values=values,
        config=WindowConfig(
            config.training_start,
            config.training_end,
            config.context_length,
            config.minimum_history,
        ),
    )
    development = build_windows(
        horse_ids=horse_ids,
        dates=dates,
        values=values,
        config=WindowConfig(
            config.development_start,
            config.development_end,
            config.context_length,
            config.minimum_history,
        ),
    )
    target_rows = np.ones(len(table), dtype=np.bool_)
    if config.training_domain is not None:
        target_rows = domain_labels(
            np.asarray(table["race_id"].to_pylist(), dtype=np.str_),
            np.asarray(table["venue_code"].to_pylist(), dtype=np.str_),
            config.training_domain,
        )
    if config.target_race_prefix is not None:
        target_rows &= np.char.startswith(
            np.asarray(table["race_id"].to_pylist(), dtype=np.str_), config.target_race_prefix
        )
    train = filter_window_targets(train, target_rows)
    development = filter_window_targets(development, target_rows)
    sampler = StaticWindowBatches(train, batch_size=config.batch_size, seed=config.seed)
    mx.random.seed(config.seed)
    pipeline = load_local_pipeline(source_config, dtype="bfloat16")
    configure_adaptation(
        pipeline.model, config.mode, lora_rank=config.lora_rank, lora_alpha=config.lora_alpha
    )
    step = ChronosTrainStep(
        pipeline.model,
        StepConfig(
            learning_rate=config.learning_rate,
            weight_decay=config.weight_decay,
            total_steps=config.steps,
            warmup_steps=config.warmup_steps,
            compile_mode="full",
        ),
    )
    baseline = predict_windows(pipeline, development)
    np.save(output / "baseline-quantiles.npy", baseline)
    print(
        json.dumps(
            {
                "train_windows": len(train.targets),
                "development_windows": len(development.targets),
                "baseline_finite": True,
            }
        ),
        flush=True,
    )
    identity = data_hash + ":" + json.dumps(asdict(config), sort_keys=True) + ":performance"
    start_step = (
        0 if resume is None else restore_checkpoint(step, source=resume, dataset_id=identity)
    )
    if not 0 <= start_step <= config.steps:
        raise ValueError("Checkpoint step outside study schedule")
    losses: list[float] = []
    for index in range(start_step, config.steps):
        context, target = sampler.at_step(index)
        loss = float(step(mx.array(context), mx.array(target)))
        if not np.isfinite(loss):
            raise ValueError("Nonfinite training loss")
        losses.append(loss)
        if (index + 1) % 100 == 0:
            print(json.dumps({"step": index + 1, "loss": loss}), flush=True)
        if (index + 1) % checkpoint_every == 0 and index + 1 < config.steps:
            save_checkpoint(
                step,
                output=output / "checkpoints" / f"step-{index + 1:06d}",
                dataset_id=identity,
            )
    save_checkpoint(step, output=output / "checkpoint", dataset_id=identity)
    final = predict_windows(pipeline, development)
    np.save(output / "final-quantiles.npy", final)
    np.save(output / "development-source-indices.npy", development.source_indices)
    metadata = export_pretrained(
        pipeline.model,
        source_config=source_config,
        output_dir=output / "portable",
        training_mode=config.mode,
    )
    indices = np.flatnonzero(
        (dates >= config.development_start) & (dates <= config.development_end) & target_rows
    )
    predictions = table.take(pa.array(indices))
    predictions = predictions.append_column(
        "chronos_zero_shot",
        pa.array(
            _scatter_predictions(
                indices=indices, windows=development, predictions=baseline, total_rows=len(table)
            ),
            from_pandas=True,
        ),
    )
    predictions = predictions.append_column(
        f"chronos_{config.mode}",
        pa.array(
            _scatter_predictions(
                indices=indices, windows=development, predictions=final, total_rows=len(table)
            ),
            from_pandas=True,
        ),
    )
    pq.write_table(predictions, output / "development-predictions.parquet", compression="zstd")
    report: dict[str, object] = {
        "config": asdict(config),
        "data_sha256": data_hash,
        "dataset_identity": identity,
        "train_windows": len(train.targets),
        "development_windows": len(development.targets),
        "development_all_rows": len(indices),
        "baseline_mae": float(
            np.mean(np.abs(baseline[:, baseline.shape[1] // 2] - development.targets[:, 0]))
        ),
        "final_mae": float(
            np.mean(np.abs(final[:, final.shape[1] // 2] - development.targets[:, 0]))
        ),
        "train_losses": losses,
        "resumed_step": start_step,
        "portable": metadata,
        "production_eligible": False,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--source-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--steps", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--context-length", type=int, default=128)
    parser.add_argument("--minimum-history", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--learning-rate", type=float, default=1e-5)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--warmup-steps", type=int, default=100)
    parser.add_argument("--lora-rank", type=int, default=8)
    parser.add_argument("--lora-alpha", type=float, default=16.0)
    parser.add_argument("--training-start", default="20200101")
    parser.add_argument("--training-end", default="20221231")
    parser.add_argument("--development-start", default="20230101")
    parser.add_argument("--development-end", default="20231231")
    parser.add_argument("--target-race-prefix")
    parser.add_argument("--training-domain", choices=["jra", "nar", "banei"])
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--checkpoint-every", type=int, default=250)
    parser.add_argument("--mode", choices=["head", "lora"], default="head")
    args = parser.parse_args()
    run_study(
        history=args.history,
        source_config=args.source_config,
        output=args.output,
        config=StudyConfig(
            steps=args.steps,
            mode=args.mode,
            batch_size=args.batch_size,
            context_length=args.context_length,
            minimum_history=args.minimum_history,
            seed=args.seed,
            learning_rate=args.learning_rate,
            weight_decay=args.weight_decay,
            warmup_steps=args.warmup_steps,
            lora_rank=args.lora_rank,
            lora_alpha=args.lora_alpha,
            training_start=args.training_start,
            training_end=args.training_end,
            development_start=args.development_start,
            development_end=args.development_end,
            target_race_prefix=args.target_race_prefix,
            training_domain=args.training_domain,
        ),
        resume=args.resume,
        checkpoint_every=args.checkpoint_every,
    )


if __name__ == "__main__":
    main()
