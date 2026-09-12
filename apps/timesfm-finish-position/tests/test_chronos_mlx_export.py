"""Portable weight mapping and non-mutating adapter fusion tests."""

import json
from pathlib import Path

import mlx.core as mx
import mlx.nn as nn
import numpy as np
import pytest
from chronos2_mlx.adapters import LoRAConfig, LoRALinear, apply_lora
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel

from timesfm_finish_position.chronos_mlx_export import (
    export_pretrained,
    portable_weights,
    standard_weight_name,
)


@pytest.fixture
def model() -> Chronos2MLXModel:
    mx.random.seed(5)
    return Chronos2MLXModel(
        Chronos2MLXConfig(d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2)
    )


def test_weight_mapping() -> None:
    assert (
        standard_weight_name("encoder.block.0.layer.2.wi.weight")
        == "encoder.block.0.layer.2.mlp.wi.weight"
    )
    assert (
        standard_weight_name("encoder.block.0.layer.2.wo.weight")
        == "encoder.block.0.layer.2.mlp.wo.weight"
    )
    assert standard_weight_name("shared.weight") == "shared.weight"


def test_adapter_fusion_does_not_mutate(model: Chronos2MLXModel) -> None:
    apply_lora(model, LoRAConfig(rank=2))
    adapter = dict(model.named_modules())["encoder.block.0.layer.0.self_attention.q"]
    assert isinstance(adapter, LoRALinear)
    assert isinstance(adapter.base, nn.Linear)
    adapter.lora_b = mx.ones_like(adapter.lora_b) * 0.02
    original = np.array(adapter.base.weight)
    weights = portable_weights(model)
    expected = adapter.base.weight + adapter.scaling * (adapter.lora_b @ adapter.lora_a)
    np.testing.assert_allclose(
        np.array(weights["encoder.block.0.layer.0.self_attention.q.weight"]), np.array(expected)
    )
    np.testing.assert_array_equal(np.array(adapter.base.weight), original)
    assert not any("lora_" in name or ".base." in name for name in weights)


def test_quantized_fusion(model: Chronos2MLXModel) -> None:
    def quantizable_linear(path: str, module: nn.Module) -> bool:
        return bool(path) and isinstance(module, nn.Linear) and module.weight.shape[-1] % 32 == 0

    nn.quantize(model, group_size=32, bits=4, class_predicate=quantizable_linear)
    apply_lora(model, LoRAConfig(rank=2))
    adapter = dict(model.named_modules())["encoder.block.0.layer.0.self_attention.q"]
    assert isinstance(adapter, LoRALinear)
    assert isinstance(adapter.base, nn.QuantizedLinear)
    original = np.array(adapter.base.weight)
    weights = portable_weights(model)
    assert weights["encoder.block.0.layer.0.self_attention.q.weight"].shape == (32, 32)
    assert weights["encoder.block.0.layer.0.self_attention.k.weight"].dtype == mx.float32
    assert not any(name.endswith(".scales") for name in weights)
    np.testing.assert_array_equal(np.array(adapter.base.weight), original)


def test_export_files_and_no_overwrite(model: Chronos2MLXModel, tmp_path: Path) -> None:
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"chronos_config": {}}), encoding="utf-8")
    output = tmp_path / "export"
    result = export_pretrained(model, source_config=config, output_dir=output, training_mode="head")
    assert result["production_eligible"] is False
    assert result["parity_status"] == "pending"
    assert (output / "model.safetensors").is_file()
    assert (output / "export_metadata.json").is_file()
    with pytest.raises(FileExistsError):
        export_pretrained(model, source_config=config, output_dir=output, training_mode="head")


def test_invalid_config(model: Chronos2MLXModel, tmp_path: Path) -> None:
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="standard Chronos-2"):
        export_pretrained(
            model, source_config=config, output_dir=tmp_path / "export", training_mode="head"
        )
