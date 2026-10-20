#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingTypeStubs=false
"""Export trained Race Set Transformers to the numpy serving contract.

The deployed scorer (finish-position-predict-container predict_lib.transformer_scorer)
reads ``norm.json`` + ``weights_s{i}.npz`` and has no categorical embeddings. Training
keeps race-meta embeddings, but when the race-meta columns are absent every lookup
hits the padding row, a constant vector folded exactly into numeric_projection.bias.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

import mlx.core as mx
import mlx.utils
import numpy as np

from finish_position_transformer.dataset import MAX_RUNNERS, NormalizationStats
from finish_position_transformer.model import UMABAN_VOCAB_SIZE, ModelConfig, RaceSetTransformer

LAYER_NORM_EPS = 1e-5
RACE_EMBEDDING_PREFIX = "race_categorical_embeddings."
HORSE_EMBEDDING_PREFIX = "categorical_embeddings."
# np.savez's stub types **kwds against its own keyword params (allow_pickle: bool);
# the arrays-by-name call is what np.load reads back as npz keys.
_save_npz = cast(Callable[..., None], np.savez)


def serving_weights(model: RaceSetTransformer) -> dict[str, np.ndarray]:
    weights = {
        name: np.asarray(cast(mx.array, value), dtype=np.float32)
        for name, value in mlx.utils.tree_flatten(model.parameters())
    }
    if any(name.startswith(HORSE_EMBEDDING_PREFIX) for name in weights):
        raise ValueError("per-horse categorical embeddings are not part of the serving contract")
    bias_shift = np.zeros_like(weights["numeric_projection.bias"])
    for name in [n for n in weights if n.startswith(RACE_EMBEDDING_PREFIX)]:
        bias_shift += weights.pop(name)[0]  # padding/unknown row: the only row served
    weights["numeric_projection.bias"] = weights["numeric_projection.bias"] + bias_shift
    return weights


def write_serving_artifact(
    out_dir: Path,
    models: Sequence[RaceSetTransformer],
    stats: NormalizationStats,
    config: ModelConfig,
) -> dict[str, object]:
    """Write weights_s{1..n}.npz + norm.json; return the norm manifest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    seed_files = [f"weights_s{index}.npz" for index in range(1, len(models) + 1)]
    for file_name, model in zip(seed_files, models, strict=True):
        _save_npz(out_dir / file_name, **serving_weights(model))
    norm: dict[str, object] = {
        "feature_order": stats["numeric_columns"],
        "mean": stats["numeric_mean"],
        "std": stats["numeric_std"],
        "arch": {
            "dim": config["embedding_dim"],
            "num_layers": config["num_layers"],
            "num_heads": config["num_heads"],
            "eps": LAYER_NORM_EPS,
            "umaban_vocab_size": UMABAN_VOCAB_SIZE,
            "max_runners": MAX_RUNNERS,
            "activation": "relu",
            "race_meta": False,
        },
        "seed_files": seed_files,
    }
    (out_dir / "norm.json").write_text(json.dumps(norm, ensure_ascii=False, indent=1), encoding="utf-8")
    return norm


__all__ = ("serving_weights", "write_serving_artifact")
