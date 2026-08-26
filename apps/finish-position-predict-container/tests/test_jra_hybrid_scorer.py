from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import cast

import numpy as np
import pytest

from predict_lib import jra_hybrid_scorer as scorer


def _weights(seed: int = 1) -> dict[str, np.ndarray]:
    generator = np.random.default_rng(seed)
    dimensions = 4
    weights: dict[str, np.ndarray] = {
        "continuous_projection.weight": generator.normal(size=(dimensions, 2)).astype(np.float32),
        "continuous_projection.bias": generator.normal(size=dimensions).astype(np.float32),
        "missing_projection.weight": generator.normal(size=(dimensions, 2)).astype(np.float32),
        "continuous_scale": np.asarray([1.0], dtype=np.float32),
        "missing_scale": np.asarray([0.8], dtype=np.float32),
        "bin_scale": np.asarray([1.1], dtype=np.float32),
        "categorical_scale": np.asarray([0.9], dtype=np.float32),
        "umaban_embedding.weight": generator.normal(size=(4, dimensions)).astype(np.float32),
        "bin_embeddings.0.weight": generator.normal(size=(5, dimensions)).astype(np.float32),
        "bin_embeddings.1.weight": generator.normal(size=(5, dimensions)).astype(np.float32),
        "categorical_embeddings.0.weight": generator.normal(size=(3, dimensions)).astype(
            np.float32
        ),
        "categorical_embeddings.1.weight": generator.normal(size=(3, dimensions)).astype(
            np.float32
        ),
        "race_categorical_embeddings.0.weight": generator.normal(
            size=(3, dimensions)
        ).astype(np.float32),
    }
    for prefix in ("input_norm", "encoder.layers.0.ln1", "encoder.layers.0.ln2", "encoder.ln"):
        weights[f"{prefix}.weight"] = np.ones(dimensions, dtype=np.float32)
        weights[f"{prefix}.bias"] = np.zeros(dimensions, dtype=np.float32)
    attention = "encoder.layers.0.attention"
    for projection in ("query_proj", "key_proj", "value_proj", "out_proj"):
        weights[f"{attention}.{projection}.weight"] = generator.normal(
            scale=0.2, size=(dimensions, dimensions)
        ).astype(np.float32)
    weights["encoder.layers.0.linear1.weight"] = generator.normal(
        scale=0.2, size=(16, dimensions)
    ).astype(np.float32)
    weights["encoder.layers.0.linear1.bias"] = np.zeros(16, dtype=np.float32)
    weights["encoder.layers.0.linear2.weight"] = generator.normal(
        scale=0.2, size=(dimensions, 16)
    ).astype(np.float32)
    weights["encoder.layers.0.linear2.bias"] = np.zeros(dimensions, dtype=np.float32)
    for head in range(5):
        weights[f"expert_heads.{head}.input.weight"] = generator.normal(
            scale=0.2, size=(dimensions, dimensions)
        ).astype(np.float32)
        weights[f"expert_heads.{head}.input.bias"] = np.zeros(dimensions, dtype=np.float32)
        weights[f"expert_heads.{head}.output.weight"] = generator.normal(
            scale=0.2, size=(1, dimensions)
        ).astype(np.float32)
        weights[f"expert_heads.{head}.output.bias"] = np.zeros(1, dtype=np.float32)
    return weights


def _manifest() -> dict[str, object]:
    return {
        "stats": {
            "feature_names": ["f1", "f2"],
            "mean": [10.0, 20.0],
            "std": [2.0, 4.0],
            "bin_edges": [[9.0, 11.0], [18.0, 22.0]],
            "categorical_columns": ["cat1", "cat2"],
            "categorical_vocab": {"cat1": ["A", "B"], "cat2": ["X", "Y"]},
            "race_categorical_columns": ["venue"],
            "race_categorical_vocab": {"venue": ["01", "02"]},
        },
        "architecture": {
            "layers": 1,
            "attention_heads": 2,
            "max_runners": 3,
            "served_head_index": 4,
        },
        "route": {"companion_weight": 0.24, "seed_weights": [0.1, 0.1, 0.8]},
        "seeds": [
            {"weight_file": "weights_seed1.npz"},
            {"weight_file": "weights_seed2.npz"},
            {"weight_file": "weights_seed3.npz"},
        ],
    }


def _artifact(tmp_path: Path) -> Path:
    (tmp_path / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
    save_npz = cast(Callable[..., None], np.savez)
    for index in range(1, 4):
        save_npz(tmp_path / f"weights_seed{index}.npz", **_weights(index))
    return tmp_path


def _entries() -> list[dict[str, object]]:
    return [
        {"f1": 12.0, "f2": None, "cat1": "A", "cat2": "Y", "venue": "02", "umaban": 1},
        {"f1": "8", "f2": 24.0, "cat1": "unknown", "cat2": "X", "venue": "02", "umaban": 9},
    ]


def test_load_encode_and_companion_scores(tmp_path: Path) -> None:
    loaded = scorer.JraHybridScorer.load(_artifact(tmp_path))
    assert loaded.feature_order == ("f1", "f2")
    assert loaded.companion_weight == pytest.approx(0.24)
    arrays = loaded.encode_entries([_entries()])
    continuous, bins, categorical, race_categorical, umaban, mask = arrays
    assert continuous[0, 0].tolist() == [1.0, 0.0]
    assert continuous[0, 1].tolist() == [-1.0, 1.0]
    assert bins[0, :2].tolist() == [[3, 0], [1, 3]]
    assert categorical[0, :2].tolist() == [[1, 2], [0, 1]]
    assert race_categorical.tolist() == [[2]]
    assert umaban.tolist() == [[1, 0, 0]]
    assert mask.tolist() == [[True, True, False]]
    scores = loaded.companion_scores(_entries())
    assert len(scores) == 2
    assert all(np.isfinite(scores))


def test_encode_handles_empty_unknown_nonfinite_and_runner_limit(tmp_path: Path) -> None:
    loaded = scorer.JraHybridScorer.load(_artifact(tmp_path))
    entries = [
        {"f1": float("nan"), "f2": object(), "cat1": None, "cat2": "", "umaban": True},
        {"f1": False, "f2": "bad", "cat1": "B", "cat2": "X", "umaban": 2},
        {"f1": 10, "f2": 20, "cat1": "A", "cat2": "Y", "umaban": 3},
        {"f1": 11, "f2": 21, "cat1": "A", "cat2": "Y", "umaban": 1},
    ]
    arrays = loaded.encode_entries([entries, []])
    assert arrays[5].sum() == 3
    assert arrays[4][0].tolist() == [1, 2, 3]
    assert arrays[1][0, 0].tolist() == [0, 0]
    assert arrays[2][0, 0].tolist() == [0, 0]
    assert arrays[3][1].tolist() == [0]


def test_missing_feature_keys(tmp_path: Path) -> None:
    loaded = scorer.JraHybridScorer.load(_artifact(tmp_path))
    assert loaded.missing_feature_keys([]) == set()
    assert loaded.missing_feature_keys(_entries()) == set()
    missing = loaded.missing_feature_keys([{"f1": 1.0}])
    assert missing == {"f2", "cat1", "cat2", "venue", "umaban"}


def test_forward_heads_is_deterministic_and_respects_shape() -> None:
    continuous = np.asarray([[[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]], dtype=np.float32)
    bins = np.asarray([[[1, 0], [2, 3], [0, 0]]], dtype=np.int64)
    categorical = np.asarray([[[1, 2], [2, 1], [0, 0]]], dtype=np.int64)
    race_categorical = np.asarray([[1]], dtype=np.int64)
    umaban = np.asarray([[1, 2, 0]], dtype=np.int64)
    mask = np.asarray([[True, True, False]])
    first = scorer.forward_heads(
        continuous,
        bins,
        categorical,
        race_categorical,
        umaban,
        mask,
        _weights(),
        layers=1,
        attention_heads=2,
    )
    second = scorer.forward_heads(
        continuous,
        bins,
        categorical,
        race_categorical,
        umaban,
        mask,
        _weights(),
        layers=1,
        attention_heads=2,
    )
    assert first.shape == (1, 3, 5)
    np.testing.assert_array_equal(first, second)


def test_zscore_and_fusion_guards() -> None:
    assert scorer.within_race_zscore([]).tolist() == []
    assert scorer.within_race_zscore([2.0, 2.0]).tolist() == [0.0, 0.0]
    fused = scorer.fuse_jra_hybrid_scores([3.0, 1.0], [1.0, 3.0], companion_weight=0.25)
    assert fused == pytest.approx([0.5, -0.5])
    with pytest.raises(ValueError, match="lengths differ"):
        scorer.fuse_jra_hybrid_scores([1.0], [1.0, 2.0], companion_weight=0.2)


def test_load_rejects_missing_weight_file(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        scorer.JraHybridScorer.load(tmp_path)
