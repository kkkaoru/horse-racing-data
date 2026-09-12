"""Load an explicit local standard checkpoint without resolving mutable Hub refs."""

import json
from pathlib import Path
from typing import Literal

import mlx.core as mx
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel
from chronos2_mlx.pipeline import Chronos2MLXPipeline
from chronos2_mlx.weights import load_chronos2_weights, verify_weights_loaded
from safetensors.numpy import load_file


def load_local_pipeline(
    source_config: Path, *, dtype: Literal["float32", "bfloat16"]
) -> Chronos2MLXPipeline:
    document = json.loads(source_config.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("chronos_config"), dict):
        raise ValueError("Expected standard Chronos-2 configuration")
    # Architecture dimensions live at the top level in official checkpoints.
    merged = {**document, **document["chronos_config"]}
    config = Chronos2MLXConfig.from_hf_config({"chronos_config": merged})
    config.vocab_size = 2 if config.use_reg_token else 1
    model = Chronos2MLXModel(config)
    weights = load_file(str(source_config.parent / "model.safetensors"))
    used = load_chronos2_weights(model, weights)
    verify_weights_loaded(model, weights, used)
    if dtype == "bfloat16":
        model.set_dtype(mx.bfloat16)
    mx.eval(model.parameters())
    return Chronos2MLXPipeline(model, config)
