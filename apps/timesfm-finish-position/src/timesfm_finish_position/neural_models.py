"""Portable PyTorch MLP and LoMETab experiment models."""

from __future__ import annotations

import json
import platform
from dataclasses import asdict, dataclass
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Self

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from .model_interface import (
    ModelDataset,
    ModelPrediction,
    PortableArtifactManifest,
    read_manifest,
    write_manifest,
)
from .neural_preprocessing import (
    NeuralFeatureConfig,
    NeuralPreprocessor,
    NumericEncoding,
    fit_neural_preprocessor,
)
from .tabular_evaluation import scores_to_probabilities

DEFAULT_SEED = 20260904
MIN_EMBEDDING_DIMENSION = 4
MAX_EMBEDDING_DIMENSION = 32


class NeuralModelKind(StrEnum):
    """Supported portable neural architectures."""

    PLAIN_MLP = "plain-mlp"
    EMBEDDING_MLP = "embedding-mlp"
    LOMETAB = "lometab"


@dataclass(frozen=True)
class NeuralModelConfig:
    """Training and architecture settings saved in every artifact."""

    kind: NeuralModelKind
    model_version: str
    hidden_dimensions: tuple[int, ...] = (128, 64)
    dropout: float = 0.1
    learning_rate: float = 0.002
    weight_decay: float = 0.0003
    epochs: int = 5
    batch_size: int = 4096
    ensemble_size: int = 8
    adapter_rank: int = 4
    initialization_scale: float = 0.3
    random_seed: int = DEFAULT_SEED
    training_device: str = "auto"


def resolve_torch_device(preference: str = "auto") -> str:
    """Resolve an explicit portable device or the accelerated experiment default."""
    if preference not in {"auto", "cpu", "mps", "cuda"}:
        raise ValueError(f"unsupported torch training device: {preference}")
    if preference != "auto":
        if preference == "mps" and not torch.backends.mps.is_available():
            raise RuntimeError("MPS training was requested but is unavailable")
        if preference == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA training was requested but is unavailable")
        return preference
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


def _embedding_dimension(cardinality: int) -> int:
    return min(
        MAX_EMBEDDING_DIMENSION, max(MIN_EMBEDDING_DIMENSION, int(np.ceil(np.sqrt(cardinality))))
    )


class _FeatureEncoder(nn.Module):
    """Shared categorical embedding layer used by MLP and LoMETab."""

    def __init__(self, numeric_features: int, cardinalities: tuple[int, ...]) -> None:
        super().__init__()
        self.numeric_features = numeric_features
        embedding_dimensions = tuple(_embedding_dimension(value) for value in cardinalities)
        self.embeddings = nn.ModuleList(
            nn.Embedding(cardinality, dimension)
            for cardinality, dimension in zip(cardinalities, embedding_dimensions, strict=True)
        )
        self.output_features = numeric_features + sum(embedding_dimensions)

    def forward(self, numeric: torch.Tensor, categorical: torch.Tensor) -> torch.Tensor:
        if not self.embeddings:
            return numeric
        encoded = tuple(
            embedding(categorical[:, index]) for index, embedding in enumerate(self.embeddings)
        )
        return torch.cat((numeric, *encoded), dim=1)


class _MlpNetwork(nn.Module):
    """Plain feed-forward network over portable encoded inputs."""

    def __init__(
        self,
        numeric_features: int,
        cardinalities: tuple[int, ...],
        hidden_dimensions: tuple[int, ...],
        dropout: float,
    ) -> None:
        super().__init__()
        self.encoder = _FeatureEncoder(numeric_features, cardinalities)
        dimensions = (self.encoder.output_features, *hidden_dimensions)
        layers: list[nn.Module] = []
        for input_dimension, output_dimension in pairwise(dimensions):
            layers.extend(
                (
                    nn.Linear(input_dimension, output_dimension),
                    nn.ReLU(),
                    nn.Dropout(dropout),
                )
            )
        self.backbone = nn.Sequential(*layers)
        self.head = nn.Linear(dimensions[-1], 1)

    def forward(self, numeric: torch.Tensor, categorical: torch.Tensor) -> torch.Tensor:
        return self.head(self.backbone(self.encoder(numeric, categorical))).squeeze(-1)


class LoMETabLinear(nn.Module):
    """Identity-residual rank-r multiplicative implicit-ensemble layer."""

    def __init__(
        self,
        input_features: int,
        output_features: int,
        *,
        ensemble_size: int,
        adapter_rank: int,
        initialization_scale: float,
    ) -> None:
        super().__init__()
        self.ensemble_size = ensemble_size
        self.weight = nn.Parameter(torch.empty(output_features, input_features))
        self.adapter_output = nn.Parameter(
            torch.empty(ensemble_size, output_features, adapter_rank)
        )
        self.adapter_input = nn.Parameter(torch.empty(ensemble_size, input_features, adapter_rank))
        self.bias = nn.Parameter(torch.zeros(ensemble_size, output_features))
        nn.init.kaiming_uniform_(self.weight, a=np.sqrt(5.0))
        nn.init.normal_(self.adapter_output, std=initialization_scale)
        nn.init.normal_(self.adapter_input, std=initialization_scale)

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        if values.ndim == 2:
            values = values[:, None, :].expand(-1, self.ensemble_size, -1)
        residual = torch.einsum("kor,kir->koi", self.adapter_output, self.adapter_input)
        effective_weight = self.weight[None, :, :] * (1.0 + residual)
        return torch.einsum("bki,koi->bko", values, effective_weight) + self.bias[None, :, :]


class _LoMETabNetwork(nn.Module):
    """LoMETab backbone with member-wise low-rank adapters and heads."""

    def __init__(
        self,
        numeric_features: int,
        cardinalities: tuple[int, ...],
        config: NeuralModelConfig,
    ) -> None:
        super().__init__()
        self.encoder = _FeatureEncoder(numeric_features, cardinalities)
        dimensions = (self.encoder.output_features, *config.hidden_dimensions, 1)
        self.layers = nn.ModuleList(
            LoMETabLinear(
                input_dimension,
                output_dimension,
                ensemble_size=config.ensemble_size,
                adapter_rank=config.adapter_rank,
                initialization_scale=config.initialization_scale,
            )
            for input_dimension, output_dimension in pairwise(dimensions)
        )
        self.dropout = nn.Dropout(config.dropout)

    def forward(self, numeric: torch.Tensor, categorical: torch.Tensor) -> torch.Tensor:
        values = self.encoder(numeric, categorical)
        for layer in self.layers[:-1]:
            values = self.dropout(torch.relu(layer(values)))
        return self.layers[-1](values).squeeze(-1)


def _validate_model_config(config: NeuralModelConfig) -> None:
    if not config.hidden_dimensions or any(value < 1 for value in config.hidden_dimensions):
        raise ValueError("hidden dimensions must be positive")
    if not 0.0 <= config.dropout < 1.0:
        raise ValueError("dropout must be between zero and one")
    positive_values = (
        config.learning_rate,
        config.weight_decay,
        config.epochs,
        config.batch_size,
        config.ensemble_size,
        config.adapter_rank,
        config.initialization_scale,
    )
    if any(value <= 0 for value in positive_values):
        raise ValueError("neural model training settings must be positive")


def _build_network(config: NeuralModelConfig, preprocessor: NeuralPreprocessor) -> nn.Module:
    cardinalities = preprocessor.categorical_cardinalities
    if config.kind == NeuralModelKind.LOMETAB:
        return _LoMETabNetwork(
            preprocessor.numeric_output_features,
            cardinalities,
            config,
        )
    return _MlpNetwork(
        preprocessor.numeric_output_features,
        cardinalities,
        config.hidden_dimensions,
        config.dropout,
    )


def _preprocessor_payload(preprocessor: NeuralPreprocessor) -> dict[str, object]:
    return {
        "numeric_encoding": preprocessor.config.numeric_encoding.value,
        "piecewise_bins": preprocessor.config.piecewise_bins,
        "periodic_frequencies": preprocessor.config.periodic_frequencies,
        "categorical_names": list(preprocessor.config.categorical_names),
        "categorical_vocabularies": {
            name: list(values) for name, values in preprocessor.categorical_vocabularies.items()
        },
    }


def _required_string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise ValueError(f"artifact field {name} must be a string")
    return value


def _required_integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int):
        raise ValueError(f"artifact field {name} must be an integer")
    return value


def _required_float(payload: dict[str, object], name: str) -> float:
    value = payload.get(name)
    if not isinstance(value, (int, float)):
        raise ValueError(f"artifact field {name} must be numeric")
    return float(value)


def _required_sequence(payload: dict[str, object], name: str) -> tuple[object, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise ValueError(f"artifact field {name} must be a list")
    return tuple(value)


def _config_from_payload(payload: dict[str, object]) -> NeuralModelConfig:
    hidden_values = _required_sequence(payload, "hidden_dimensions")
    if not all(isinstance(value, int) for value in hidden_values):
        raise ValueError("artifact hidden dimensions must be integers")
    return NeuralModelConfig(
        kind=NeuralModelKind(_required_string(payload, "kind")),
        model_version=_required_string(payload, "model_version"),
        hidden_dimensions=tuple(value for value in hidden_values if isinstance(value, int)),
        dropout=_required_float(payload, "dropout"),
        learning_rate=_required_float(payload, "learning_rate"),
        weight_decay=_required_float(payload, "weight_decay"),
        epochs=_required_integer(payload, "epochs"),
        batch_size=_required_integer(payload, "batch_size"),
        ensemble_size=_required_integer(payload, "ensemble_size"),
        adapter_rank=_required_integer(payload, "adapter_rank"),
        initialization_scale=_required_float(payload, "initialization_scale"),
        random_seed=_required_integer(payload, "random_seed"),
        training_device=_required_string(payload, "training_device"),
    )


def _feature_config_from_payload(payload: dict[str, object]) -> NeuralFeatureConfig:
    category_values = _required_sequence(payload, "categorical_names")
    if not all(isinstance(value, str) for value in category_values):
        raise ValueError("artifact categorical names must be strings")
    return NeuralFeatureConfig(
        numeric_encoding=NumericEncoding(_required_string(payload, "numeric_encoding")),
        piecewise_bins=_required_integer(payload, "piecewise_bins"),
        periodic_frequencies=_required_integer(payload, "periodic_frequencies"),
        categorical_names=tuple(value for value in category_values if isinstance(value, str)),
    )


def _vocabularies_from_payload(payload: dict[str, object]) -> dict[str, tuple[str, ...]]:
    raw = payload.get("categorical_vocabularies")
    if not isinstance(raw, dict):
        raise ValueError("artifact categorical vocabularies must be an object")
    vocabularies: dict[str, tuple[str, ...]] = {}
    for name, values in raw.items():
        if not isinstance(name, str) or not isinstance(values, list):
            raise ValueError("artifact categorical vocabulary is malformed")
        if not all(isinstance(value, str) for value in values):
            raise ValueError("artifact categorical vocabulary values must be strings")
        vocabularies[name] = tuple(value for value in values if isinstance(value, str))
    return vocabularies


class PortableNeuralModel:
    """Common fit/predict/save/load implementation for MLP and LoMETab."""

    def __init__(
        self,
        config: NeuralModelConfig,
        feature_config: NeuralFeatureConfig,
        *,
        feature_version: str,
    ) -> None:
        _validate_model_config(config)
        if config.kind == NeuralModelKind.PLAIN_MLP and feature_config.categorical_names:
            raise ValueError("plain MLP must not use categorical embeddings")
        self.config = config
        self.feature_config = feature_config
        self.feature_version = feature_version
        self.preprocessor: NeuralPreprocessor | None = None
        self.network: nn.Module | None = None
        self.training_metadata: dict[str, object] = {}

    def _prepared_inputs(self, data: ModelDataset) -> tuple[torch.Tensor, torch.Tensor]:
        if self.preprocessor is None:
            raise RuntimeError("neural model is not fitted")
        numeric = torch.from_numpy(self.preprocessor.transform_numeric(data.numeric))
        categorical_values = (
            self.preprocessor.transform_categorical(data.categorical)
            if self.preprocessor.config.categorical_names
            else np.empty((data.rows, 0), dtype=np.int64)
        )
        categorical = torch.from_numpy(categorical_values)
        return numeric, categorical

    def fit(self, train_data: ModelDataset) -> None:
        """Fit a deterministic member-wise binary winner model."""
        train_data.validate(require_labels=True)
        if train_data.finish_positions is None:
            raise RuntimeError("validated supervised labels are unavailable")
        torch.manual_seed(self.config.random_seed)
        self.preprocessor = fit_neural_preprocessor(
            train_data.numeric,
            train_data.categorical,
            self.feature_config,
        )
        self.network = _build_network(self.config, self.preprocessor)
        device = resolve_torch_device(self.config.training_device)
        self.network.to(device)
        numeric, categorical = self._prepared_inputs(train_data)
        labels = torch.from_numpy((train_data.finish_positions == 1).astype(np.float32))
        loader = DataLoader(
            TensorDataset(numeric, categorical, labels),
            batch_size=self.config.batch_size,
            shuffle=True,
            generator=torch.Generator().manual_seed(self.config.random_seed),
        )
        optimizer = torch.optim.AdamW(
            self.network.parameters(),
            lr=self.config.learning_rate,
            weight_decay=self.config.weight_decay,
        )
        positive_weight = torch.tensor(
            [(len(labels) - float(labels.sum())) / float(labels.sum())], device=device
        )
        loss_function = nn.BCEWithLogitsLoss(pos_weight=positive_weight)
        self.network.train()
        final_loss = 0.0
        for _epoch in range(self.config.epochs):
            epoch_loss = 0.0
            for batch_numeric, batch_categorical, batch_labels in loader:
                optimizer.zero_grad(set_to_none=True)
                logits = self.network(batch_numeric.to(device), batch_categorical.to(device))
                targets = batch_labels.to(device)
                if logits.ndim == 2:
                    targets = targets[:, None].expand_as(logits)
                loss = loss_function(logits, targets)
                loss.backward()
                optimizer.step()
                epoch_loss += float(loss.detach().cpu())
            final_loss = epoch_loss / len(loader)
        self.network.to("cpu")
        self.training_metadata = {
            "backend": f"torch-{device}",
            "rows": train_data.rows,
            "final_loss": final_loss,
        }

    def _predict_batches(self, data: ModelDataset) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        if self.network is None:
            raise RuntimeError("neural model is not fitted")
        numeric, categorical = self._prepared_inputs(data)
        loader = DataLoader(
            TensorDataset(numeric, categorical),
            batch_size=self.config.batch_size,
            shuffle=False,
        )
        scores: list[np.ndarray] = []
        raw_probabilities: list[np.ndarray] = []
        member_probabilities: list[np.ndarray] = []
        self.network.eval()
        with torch.inference_mode():
            for batch_numeric, batch_categorical in loader:
                logits = self.network(batch_numeric, batch_categorical)
                probabilities = torch.sigmoid(logits)
                if logits.ndim == 1:
                    probabilities = probabilities[:, None]
                member_probabilities.append(probabilities.numpy().astype(np.float64))
                mean_logits = logits.mean(dim=1) if logits.ndim == 2 else logits
                scores.append(mean_logits.numpy().astype(np.float64))
                raw_probabilities.append(probabilities.mean(dim=1).numpy().astype(np.float64))
        return (
            np.concatenate(scores),
            np.concatenate(raw_probabilities),
            np.concatenate(member_probabilities),
        )

    def predict_members(self, validation_data: ModelDataset) -> np.ndarray:
        """Return per-member probabilities for implicit-ensemble diversity audits."""
        validation_data.validate(require_labels=False)
        return self._predict_batches(validation_data)[2]

    def predict(self, validation_data: ModelDataset) -> ModelPrediction:
        """Predict and race-normalize winner probabilities on CPU."""
        validation_data.validate(require_labels=False)
        scores, raw_probabilities, _member_probabilities = self._predict_batches(validation_data)
        probability = scores_to_probabilities(
            np.log(np.clip(raw_probabilities, 1e-7, 1.0)),
            validation_data.race_ids,
            temperature=1.0,
        )
        return ModelPrediction(
            race_ids=validation_data.race_ids,
            horse_ids=validation_data.horse_ids,
            prediction=scores,
            probability=probability,
            model_name=self.config.kind.value,
            model_version=self.config.model_version,
        )

    def save(self, path: Path) -> None:
        """Save JSON metadata and backend-neutral NumPy weights."""
        if self.preprocessor is None or self.network is None:
            raise RuntimeError("neural model is not fitted")
        manifest = PortableArtifactManifest(
            model_name=self.config.kind.value,
            model_version=self.config.model_version,
            feature_version=self.feature_version,
            random_seed=self.config.random_seed,
            model_config=asdict(self.config),
            feature_config=_preprocessor_payload(self.preprocessor),
            training_metadata=self.training_metadata,
        )
        write_manifest(path, manifest)
        np.savez(
            path / "preprocessor.npz",
            medians=self.preprocessor.medians,
            means=self.preprocessor.means,
            scales=self.preprocessor.scales,
            piecewise_edges=self.preprocessor.piecewise_edges,
        )
        state = {
            name: parameter.detach().cpu().numpy()
            for name, parameter in self.network.state_dict().items()
        }
        np.savez(path / "weights.npz", **state)

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load a NumPy artifact into the portable CPU implementation."""
        manifest = read_manifest(path)
        model_payload = dict(manifest.model_config)
        feature_payload = dict(manifest.feature_config)
        config = _config_from_payload(model_payload)
        feature_config = _feature_config_from_payload(feature_payload)
        instance = cls(config, feature_config, feature_version=manifest.feature_version)
        with np.load(path / "preprocessor.npz") as arrays:
            vocabularies = _vocabularies_from_payload(feature_payload)
            instance.preprocessor = NeuralPreprocessor(
                feature_config,
                arrays["medians"].astype(np.float64),
                arrays["means"].astype(np.float64),
                arrays["scales"].astype(np.float64),
                arrays["piecewise_edges"].astype(np.float64),
                vocabularies,
            )
        instance.network = _build_network(config, instance.preprocessor)
        with np.load(path / "weights.npz") as arrays:
            state = {
                name: torch.from_numpy(arrays[name].copy())
                for name in instance.network.state_dict()
            }
        instance.network.load_state_dict(state)
        instance.network.eval()
        instance.training_metadata = dict(manifest.training_metadata)
        return instance


def model_config_json(config: NeuralModelConfig) -> str:
    """Return a stable configuration fingerprint input."""
    return json.dumps(asdict(config), sort_keys=True, separators=(",", ":"), default=str)
