"""Atomic MLX checkpoints with optimizer, RNG and sampler-step identity."""

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path

import mlx.core as mx
from mlx.utils import tree_flatten, tree_unflatten

from timesfm_finish_position.chronos_mlx_training import ChronosTrainStep


def _random_state() -> list[mx.array]:
    state = mx.random.state
    if not isinstance(state, Iterable):
        raise TypeError("Expected iterable MLX random state")
    arrays: list[mx.array] = []
    for value in state:
        if not isinstance(value, mx.array):
            raise TypeError("Expected MLX random state arrays")
        arrays.append(value)
    return arrays


def save_checkpoint(step: ChronosTrainStep, *, output: Path, dataset_id: str) -> None:
    """Save full model state; no dependency on a mutable base-model cache."""
    if not dataset_id:
        raise ValueError("dataset_id is required")
    if output.exists():
        raise FileExistsError(output)
    partial = output.with_name(output.name + ".partial")
    partial.mkdir(parents=True, exist_ok=False)
    arrays = dict(tree_flatten(step.model.parameters(), prefix=".model"))
    arrays.update(dict(tree_flatten(step.optimizer.state, prefix=".optimizer")))
    arrays.update({f"rng.{index}": value for index, value in enumerate(_random_state())})
    weights = partial / "checkpoint.safetensors"
    mx.save_safetensors(str(weights), arrays)
    with weights.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    metadata = {
        "config": asdict(step.config),
        "dataset_id": dataset_id,
        "next_step": int(step.optimizer.state["step"]),
        "trainable_names": sorted(
            name for name, _ in tree_flatten(step.model.trainable_parameters())
        ),
        "sha256": digest,
    }
    (partial / "checkpoint.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    partial.rename(output)


def restore_checkpoint(step: ChronosTrainStep, *, source: Path, dataset_id: str) -> int:
    """Fail before state mutation on incompatible configuration/data/architecture."""
    metadata = json.loads((source / "checkpoint.json").read_text(encoding="utf-8"))
    if metadata["config"] != asdict(step.config) or metadata["dataset_id"] != dataset_id:
        raise ValueError("Checkpoint configuration or dataset identity mismatch")
    expected_names = sorted(name for name, _ in tree_flatten(step.model.trainable_parameters()))
    if metadata["trainable_names"] != expected_names:
        raise ValueError("Checkpoint trainable parameter mismatch")
    weights = source / "checkpoint.safetensors"
    with weights.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != metadata["sha256"]:
        raise ValueError("Checkpoint checksum mismatch")
    arrays = mx.load(str(weights))
    if not isinstance(arrays, dict):
        raise ValueError("Checkpoint must contain named tensors")
    model = {
        key.removeprefix("model."): value
        for key, value in arrays.items()
        if key.startswith("model.")
    }
    expected = dict(tree_flatten(step.model.parameters()))
    if model.keys() != expected.keys() or any(
        model[key].shape != expected[key].shape for key in model
    ):
        raise ValueError("Checkpoint model shape or parameter mismatch")
    optimizer = {
        key.removeprefix("optimizer."): value
        for key, value in arrays.items()
        if key.startswith("optimizer.")
    }
    next_step = int(optimizer["step"])
    if next_step != metadata["next_step"]:
        raise ValueError("Checkpoint step mismatch")
    rng = [arrays[f"rng.{index}"] for index in range(len(_random_state()))]
    step.model.update(tree_unflatten(model))
    # Preserve the captured dictionary identity used by mx.compile.
    step.optimizer.state.clear()
    step.optimizer.state.update(tree_unflatten(optimizer))
    for current, saved in zip(_random_state(), rng, strict=True):
        current[:] = saved
    mx.eval(step.model.state, step.optimizer.state, mx.random.state)
    return next_step
