"""Checkpoint restart must reproduce updates, RNG and dataset guards."""

import json
from pathlib import Path

import mlx.core as mx
import numpy as np
import pytest
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel

from timesfm_finish_position.chronos_mlx_checkpoint import restore_checkpoint, save_checkpoint
from timesfm_finish_position.chronos_mlx_training import ChronosTrainStep, StepConfig


@pytest.fixture
def step() -> ChronosTrainStep:
    mx.random.seed(6)
    model = Chronos2MLXModel(
        Chronos2MLXConfig(d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2)
    )
    model.freeze()
    model.output_patch_embedding.unfreeze()
    return ChronosTrainStep(model, StepConfig(compile_mode="full"))


def test_resume_update_and_rng(step: ChronosTrainStep, tmp_path: Path) -> None:
    context = mx.ones((1, 2, 32))
    target = mx.ones((1, 2, 1))
    step(context, target)
    output = tmp_path / "step1"
    save_checkpoint(step, output=output, dataset_id="immutable-data")
    expected_random = np.array(mx.random.normal((4,)))
    expected_loss = float(step(context, target))
    expected_weight = np.array(step.model.output_patch_embedding.output_layer.weight)
    assert restore_checkpoint(step, source=output, dataset_id="immutable-data") == 1
    np.testing.assert_array_equal(np.array(mx.random.normal((4,))), expected_random)
    assert float(step(context, target)) == pytest.approx(expected_loss)
    np.testing.assert_array_equal(
        np.array(step.model.output_patch_embedding.output_layer.weight), expected_weight
    )
    assert not (tmp_path / "step1.partial").exists()


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("trainable_names", [], "trainable parameter mismatch"),
        ("next_step", 99, "step mismatch"),
    ],
)
def test_metadata_incompatibility(
    step: ChronosTrainStep, tmp_path: Path, field: str, value: object, message: str
) -> None:
    output = tmp_path / "checkpoint"
    save_checkpoint(step, output=output, dataset_id="data")
    path = output / "checkpoint.json"
    metadata = json.loads(path.read_text(encoding="utf-8"))
    metadata[field] = value
    path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        restore_checkpoint(step, source=output, dataset_id="data")


def test_model_shape_incompatibility(step: ChronosTrainStep, tmp_path: Path) -> None:
    output = tmp_path / "checkpoint"
    save_checkpoint(step, output=output, dataset_id="data")
    step.model.shared.weight = mx.ones((3, 32))
    with pytest.raises(ValueError, match="model shape"):
        restore_checkpoint(step, source=output, dataset_id="data")


def test_checkpoint_guards(step: ChronosTrainStep, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="dataset_id"):
        save_checkpoint(step, output=tmp_path / "missing", dataset_id="")
    output = tmp_path / "saved"
    save_checkpoint(step, output=output, dataset_id="data")
    with pytest.raises(FileExistsError):
        save_checkpoint(step, output=output, dataset_id="data")
    with pytest.raises(ValueError, match="dataset identity"):
        restore_checkpoint(step, source=output, dataset_id="other")
    with (output / "checkpoint.safetensors").open("ab") as stream:
        stream.write(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        restore_checkpoint(step, source=output, dataset_id="data")
