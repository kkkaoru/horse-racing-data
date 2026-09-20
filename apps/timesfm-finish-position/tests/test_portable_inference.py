from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pytest

from timesfm_finish_position.model_interface import ModelDataset
from timesfm_finish_position.neural_models import (
    NeuralModelConfig,
    NeuralModelKind,
    PortableNeuralModel,
)
from timesfm_finish_position.neural_preprocessing import NeuralFeatureConfig
from timesfm_finish_position.portable_inference import (
    evaluate_backend_parity,
    predict_portable_mlp,
)


def _dataset() -> ModelDataset:
    return ModelDataset(
        race_ids=np.asarray(["r1"] * 4 + ["r2"] * 4, dtype=np.str_),
        horse_ids=np.asarray([f"h{index}" for index in range(8)], dtype=np.str_),
        race_dates=np.asarray(["20240101"] * 4 + ["20240102"] * 4, dtype=np.str_),
        numeric=np.asarray([[3.0], [2.0], [1.0], [0.0]] * 2, dtype=np.float64),
        numeric_names=("speed",),
        categorical={},
        decimal_odds=np.asarray([2.0, 3.0, 4.0, 5.0] * 2, dtype=np.float64),
        finish_positions=np.asarray([1, 2, 3, 4] * 2, dtype=np.int64),
    )


def test_numpy_portable_mlp_matches_torch_and_preserves_rank(tmp_path: Path) -> None:
    data = _dataset()
    model = PortableNeuralModel(
        NeuralModelConfig(
            kind=NeuralModelKind.PLAIN_MLP,
            model_version="portable-v1",
            hidden_dimensions=(8,),
            epochs=1,
            batch_size=8,
            training_device="cpu",
        ),
        NeuralFeatureConfig(),
        feature_version="fixture-v1",
    )
    model.fit(data)
    model.save(tmp_path)
    torch_scores = model.predict(data).prediction
    numpy_scores = predict_portable_mlp(tmp_path, data, backend="numpy")
    parity = evaluate_backend_parity(torch_scores, numpy_scores, data.race_ids)
    assert parity.passed is True
    assert parity.max_abs_error <= 1e-5


@pytest.mark.skipif(
    importlib.util.find_spec("mlx") is None or importlib.util.find_spec("mlx.core") is None,
    reason="MLX is Mac-only",
)
def test_mlx_portable_mlp_matches_numpy(tmp_path: Path) -> None:
    data = _dataset()
    model = PortableNeuralModel(
        NeuralModelConfig(
            kind=NeuralModelKind.PLAIN_MLP,
            model_version="mlx-v1",
            hidden_dimensions=(8,),
            epochs=1,
            batch_size=8,
            training_device="cpu",
        ),
        NeuralFeatureConfig(),
        feature_version="fixture-v1",
    )
    model.fit(data)
    model.save(tmp_path)
    numpy_scores = predict_portable_mlp(tmp_path, data, backend="numpy")
    mlx_scores = predict_portable_mlp(tmp_path, data, backend="mlx")
    parity = evaluate_backend_parity(numpy_scores, mlx_scores, data.race_ids)
    assert parity.ranking_equal is True
    assert parity.passed is (parity.max_abs_error <= parity.tolerance)


def test_categorical_embedding_portable_inference_matches_torch(tmp_path: Path) -> None:
    base = _dataset()
    data = ModelDataset(
        race_ids=base.race_ids,
        horse_ids=base.horse_ids,
        race_dates=base.race_dates,
        numeric=base.numeric,
        numeric_names=base.numeric_names,
        categorical={"jockey": np.asarray(["j1", "j2", "j1", "j2"] * 2)},
        decimal_odds=base.decimal_odds,
        finish_positions=base.finish_positions,
    )
    model = PortableNeuralModel(
        NeuralModelConfig(
            kind=NeuralModelKind.EMBEDDING_MLP,
            model_version="categorical-v1",
            hidden_dimensions=(8,),
            epochs=1,
            batch_size=8,
            training_device="cpu",
        ),
        NeuralFeatureConfig(categorical_names=("jockey",)),
        feature_version="fixture-v1",
    )
    model.fit(data)
    model.save(tmp_path)
    torch_scores = model.predict(data).prediction
    numpy_scores = predict_portable_mlp(tmp_path, data, backend="numpy")
    assert evaluate_backend_parity(torch_scores, numpy_scores, data.race_ids).passed is True


def test_portable_inference_rejects_invalid_backend_and_parity_contract(tmp_path: Path) -> None:
    data = _dataset()
    with pytest.raises(ValueError, match="unsupported portable"):
        predict_portable_mlp(tmp_path, data, backend="metal")
    with pytest.raises(ValueError, match="must align"):
        evaluate_backend_parity(np.asarray([1.0]), np.asarray([]), data.race_ids)
    with pytest.raises(ValueError, match="must be positive"):
        evaluate_backend_parity(
            np.asarray([1.0]), np.asarray([1.0]), np.asarray(["r1"]), tolerance=0
        )
    parity = evaluate_backend_parity(
        np.asarray([2.0, 1.0]),
        np.asarray([1.0, 2.0]),
        np.asarray(["r1", "r1"]),
    )
    assert parity.ranking_equal is False
    assert parity.passed is False
