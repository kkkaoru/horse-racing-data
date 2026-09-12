"""Non-mutating export from MLX into standard Chronos-2 weight names."""

import hashlib
import json
from pathlib import Path

import mlx.core as mx
import mlx.nn as nn
from chronos2_mlx.adapters import LoRALinear
from chronos2_mlx.model import Chronos2MLXModel
from mlx.utils import tree_flatten


def standard_weight_name(name: str) -> str:
    """The MLX encoder omits the standard feed-forward `mlp` namespace."""
    return name.replace(".layer.2.wi.", ".layer.2.mlp.wi.").replace(
        ".layer.2.wo.", ".layer.2.mlp.wo."
    )


def _linear_weights(module: nn.Module) -> dict[str, mx.array]:
    if isinstance(module, nn.QuantizedLinear):
        weight = mx.dequantize(
            module.weight,
            module.scales,
            module.biases,
            group_size=module.group_size,
            bits=module.bits,
        ).astype(mx.float32)
        result = {"weight": weight}
        if "bias" in module:
            result["bias"] = module.bias.astype(mx.float32)
        return result
    if isinstance(module, nn.Linear):
        result = {"weight": module.weight.astype(mx.float32)}
        if "bias" in module:
            result["bias"] = module.bias.astype(mx.float32)
        return result
    raise ValueError("Portable LoRA export requires a Linear or QuantizedLinear base")


def portable_weights(model: Chronos2MLXModel) -> dict[str, mx.array]:
    """Dequantize and fuse in FP32 without modifying the training model."""
    weights = dict(tree_flatten(model.parameters()))
    adapters = [
        (path, module) for path, module in model.named_modules() if isinstance(module, LoRALinear)
    ]
    for path, adapter in adapters:
        weights = {
            name: value for name, value in weights.items() if not name.startswith(path + ".")
        }
        base = _linear_weights(adapter.base)
        base["weight"] = base["weight"] + adapter.scaling * (
            adapter.lora_b.astype(mx.float32) @ adapter.lora_a.astype(mx.float32)
        )
        weights.update({f"{path}.{name}": value for name, value in base.items()})
    for path, module in model.named_modules():
        if isinstance(module, nn.QuantizedLinear) and not any(
            path.startswith(adapter_path + ".") for adapter_path, _ in adapters
        ):
            weights = {
                name: value for name, value in weights.items() if not name.startswith(path + ".")
            }
            weights.update(
                {f"{path}.{name}": value for name, value in _linear_weights(module).items()}
            )
    return {standard_weight_name(name): value.astype(mx.float32) for name, value in weights.items()}


def export_pretrained(
    model: Chronos2MLXModel,
    *,
    source_config: Path,
    output_dir: Path,
    training_mode: str,
) -> dict[str, str | bool]:
    """Write a new portable artifact directory; never overwrite existing artifacts."""
    config = json.loads(source_config.read_text(encoding="utf-8"))
    if not isinstance(config, dict) or "chronos_config" not in config:
        raise ValueError("Expected standard Chronos-2 config with chronos_config")
    weights = portable_weights(model)
    if any(".base." in key or "lora_" in key or key.endswith(".scales") for key in weights):
        raise ValueError("MLX adapter or quantization tensors remain in portable export")
    output_dir.mkdir(parents=True, exist_ok=False)
    config.pop("quantization", None)
    config.pop("quantization_config", None)
    (output_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    model_path = output_dir / "model.safetensors"
    mx.save_safetensors(str(model_path), weights, metadata={"format": "pt"})
    with model_path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    metadata: dict[str, str | bool] = {
        "source": "chronos2-mlx",
        "training_mode": training_mode,
        "export_dtype": "float32",
        "lora_fused": True,
        "mlx_quantization_removed": True,
        "model_sha256": digest,
        "production_eligible": False,
        "parity_status": "pending",
    }
    (output_dir / "export_metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )
    return metadata
