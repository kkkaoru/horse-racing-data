from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest
import torch

import timesfm_finish_position.neural_models as subject
from timesfm_finish_position.model_interface import ModelDataset
from timesfm_finish_position.neural_models import (
    LoMETabLinear,
    NeuralModelConfig,
    NeuralModelKind,
    PortableNeuralModel,
    model_config_json,
)
from timesfm_finish_position.neural_preprocessing import (
    NeuralFeatureConfig,
    NumericEncoding,
)


def _cpu_device(_preference: str) -> str:
    return "cpu"


def _dataset() -> ModelDataset:
    return ModelDataset(
        race_ids=np.asarray(["r1"] * 4 + ["r2"] * 4, dtype=np.str_),
        horse_ids=np.asarray([f"h{index}" for index in range(8)], dtype=np.str_),
        race_dates=np.asarray(["20240101"] * 4 + ["20240102"] * 4, dtype=np.str_),
        numeric=np.asarray(
            [[3.0, 0.0], [2.0, 1.0], [1.0, 2.0], [0.0, 3.0]] * 2,
            dtype=np.float64,
        ),
        numeric_names=("speed", "form"),
        categorical={"jockey_id": np.asarray(["j1", "j2", "j3", "j4"] * 2, dtype=np.str_)},
        decimal_odds=np.asarray([2.0, 3.0, 4.0, 5.0] * 2, dtype=np.float64),
        finish_positions=np.asarray([1, 2, 3, 4] * 2, dtype=np.int64),
    )


def test_training_device_resolution_honors_portable_override() -> None:
    assert subject.resolve_torch_device("cpu") == "cpu"
    with pytest.raises(ValueError, match="unsupported torch"):
        subject.resolve_torch_device("tpu")


def test_lometab_linear_uses_identity_residual_rank_adapter() -> None:
    layer = LoMETabLinear(
        2,
        1,
        ensemble_size=2,
        adapter_rank=1,
        initialization_scale=0.1,
    )
    with torch.no_grad():
        layer.weight.copy_(torch.tensor([[2.0, 3.0]]))
        layer.adapter_output.copy_(torch.tensor([[[1.0]], [[0.0]]]))
        layer.adapter_input.copy_(torch.tensor([[[0.5], [1.0]], [[2.0], [2.0]]]))
        layer.bias.zero_()
    output = layer(torch.tensor([[1.0, 2.0]]))
    assert output.detach().numpy()[0, :, 0].tolist() == pytest.approx([15.0, 8.0])


def test_plain_mlp_accepts_a_numeric_only_dataset() -> None:
    dataset = replace(_dataset(), categorical={})
    model = PortableNeuralModel(
        NeuralModelConfig(
            kind=NeuralModelKind.PLAIN_MLP,
            model_version="plain-numeric",
            epochs=1,
            batch_size=8,
            training_device="cpu",
        ),
        NeuralFeatureConfig(),
        feature_version="fixture-v1",
    )
    model.fit(dataset)
    assert model.predict(dataset).probability.shape == (8,)


def test_plain_mlp_fit_predict_and_portable_round_trip(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(subject, "resolve_torch_device", _cpu_device)
    config = NeuralModelConfig(
        kind=NeuralModelKind.PLAIN_MLP,
        model_version="plain-v1",
        hidden_dimensions=(8,),
        dropout=0.0,
        epochs=2,
        batch_size=4,
    )
    model = PortableNeuralModel(config, NeuralFeatureConfig(), feature_version="fixture-v1")
    dataset = _dataset()
    model.fit(dataset)
    prediction = model.predict(dataset)
    assert prediction.model_name == "plain-mlp"
    assert prediction.probability[:4].sum() == pytest.approx(1.0)
    assert prediction.probability[4:].sum() == pytest.approx(1.0)
    model.save(tmp_path)
    restored = PortableNeuralModel.load(tmp_path)
    restored_prediction = restored.predict(dataset)
    assert restored_prediction.prediction.tolist() == pytest.approx(prediction.prediction.tolist())
    assert restored.training_metadata["backend"] == "torch-cpu"


def test_embedding_mlp_consumes_train_only_categories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "resolve_torch_device", _cpu_device)
    config = NeuralModelConfig(
        kind=NeuralModelKind.EMBEDDING_MLP,
        model_version="embedding-v1",
        hidden_dimensions=(8,),
        dropout=0.0,
        epochs=1,
        batch_size=8,
    )
    feature_config = NeuralFeatureConfig(
        numeric_encoding=NumericEncoding.PERIODIC,
        periodic_frequencies=2,
        categorical_names=("jockey_id",),
    )
    model = PortableNeuralModel(config, feature_config, feature_version="fixture-v1")
    model.fit(_dataset())
    prediction = model.predict(_dataset())
    assert prediction.prediction.shape == (8,)
    assert model.preprocessor is not None
    assert model.preprocessor.categorical_cardinalities == (5,)


def test_lometab_fit_reports_member_ensemble_and_validates_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "resolve_torch_device", _cpu_device)
    config = NeuralModelConfig(
        kind=NeuralModelKind.LOMETAB,
        model_version="lometab-v1",
        hidden_dimensions=(8,),
        dropout=0.0,
        epochs=1,
        batch_size=8,
        ensemble_size=3,
        adapter_rank=2,
        initialization_scale=0.2,
    )
    model = PortableNeuralModel(config, NeuralFeatureConfig(), feature_version="fixture-v1")
    model.fit(_dataset())
    prediction = model.predict(_dataset())
    assert prediction.prediction.shape == (8,)
    assert model.predict_members(_dataset()).shape == (8, 3)
    assert '"adapter_rank":2' in model_config_json(config)
    with pytest.raises(ValueError, match="plain MLP"):
        PortableNeuralModel(
            NeuralModelConfig(
                kind=NeuralModelKind.PLAIN_MLP,
                model_version="bad-v1",
            ),
            NeuralFeatureConfig(categorical_names=("jockey_id",)),
            feature_version="fixture-v1",
        )
