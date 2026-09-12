"""Local checkpoint loading preserves architecture and never needs Hub lookup."""

import json
from dataclasses import asdict
from pathlib import Path
from typing import Literal

import mlx.core as mx
import numpy as np
import pytest
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel

from timesfm_finish_position.chronos_mlx_export import portable_weights
from timesfm_finish_position.chronos_mlx_loading import load_local_pipeline


@pytest.mark.parametrize("dtype", ["float32", "bfloat16"])
def test_load_explicit_checkpoint(tmp_path: Path, dtype: Literal["float32", "bfloat16"]) -> None:
    config = Chronos2MLXConfig(
        d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2
    )
    model = Chronos2MLXModel(config)
    document = asdict(config)
    document["chronos_config"] = {"use_reg_token": True}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    mx.save_safetensors(str(tmp_path / "model.safetensors"), portable_weights(model))
    pipeline = load_local_pipeline(path, dtype=dtype)
    assert pipeline.config.d_model == 32
    assert pipeline.config.num_layers == 1
    if dtype == "bfloat16":
        model.set_dtype(mx.bfloat16)
    inputs = mx.ones((1, 32))
    np.testing.assert_array_equal(
        np.array(pipeline.model(inputs).astype(mx.float32)),
        np.array(model(inputs).astype(mx.float32)),
    )


def test_reject_missing_standard_config(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="standard Chronos-2"):
        load_local_pipeline(path, dtype="float32")
