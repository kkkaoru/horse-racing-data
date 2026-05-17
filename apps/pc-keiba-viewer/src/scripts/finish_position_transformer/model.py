#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingTypeStubs=false, reportUnannotatedClassAttribute=false, reportImplicitOverride=false, reportUnknownParameterType=false
"""MLX Race Set Transformer: per-horse scoring with attention across the race.

Inputs are dictionaries of mx.array produced from the dataset module:
  numeric_features: [batch, MAX_RUNNERS, num_numeric]
  categorical_indices: [batch, MAX_RUNNERS, num_categorical]
  umaban: [batch, MAX_RUNNERS]
  mask: [batch, MAX_RUNNERS] bool, True for real horses
Output is a dict of three per-horse score arrays.
"""
from __future__ import annotations

from typing import TypedDict, final

import mlx.core as mx
import mlx.nn as nn

from finish_position_transformer.dataset import MAX_RUNNERS

DEFAULT_EMBEDDING_DIM = 64
DEFAULT_NUM_LAYERS = 2
DEFAULT_NUM_HEADS = 4
DEFAULT_FFN_DIM_MULTIPLIER = 4
DEFAULT_DROPOUT = 0.1
UMABAN_VOCAB_SIZE = MAX_RUNNERS + 1
ATTENTION_NEG_INF = -1e9


class ModelConfig(TypedDict):
    embedding_dim: int
    num_layers: int
    num_heads: int
    ffn_dim: int
    dropout: float
    num_numeric_features: int
    categorical_vocab_sizes: list[int]


class ModelOutput(TypedDict):
    top1_logit: mx.array
    top3_logit: mx.array
    rank_score: mx.array


def default_model_config(
    num_numeric_features: int, categorical_vocab_sizes: list[int]
) -> ModelConfig:
    return {
        "embedding_dim": DEFAULT_EMBEDDING_DIM,
        "num_layers": DEFAULT_NUM_LAYERS,
        "num_heads": DEFAULT_NUM_HEADS,
        "ffn_dim": DEFAULT_EMBEDDING_DIM * DEFAULT_FFN_DIM_MULTIPLIER,
        "dropout": DEFAULT_DROPOUT,
        "num_numeric_features": num_numeric_features,
        "categorical_vocab_sizes": categorical_vocab_sizes,
    }


def build_padding_mask(mask: mx.array, num_heads: int) -> mx.array:
    keep = mask.astype(mx.float32)
    additive = (1.0 - keep) * ATTENTION_NEG_INF
    return additive[:, None, None, :]


@final
class RaceSetTransformer(nn.Module):
    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        dims = config["embedding_dim"]
        self.config = config
        self.numeric_projection = nn.Linear(config["num_numeric_features"], dims)
        self.categorical_embeddings = [
            nn.Embedding(num_embeddings=vocab, dims=dims)
            for vocab in config["categorical_vocab_sizes"]
        ]
        self.umaban_embedding = nn.Embedding(num_embeddings=UMABAN_VOCAB_SIZE, dims=dims)
        self.input_layer_norm = nn.LayerNorm(dims=dims)
        self.encoder = nn.TransformerEncoder(
            num_layers=config["num_layers"],
            dims=dims,
            num_heads=config["num_heads"],
            mlp_dims=config["ffn_dim"],
            dropout=config["dropout"],
            norm_first=True,
        )
        self.top1_head = nn.Linear(dims, 1)
        self.top3_head = nn.Linear(dims, 1)
        self.rank_head = nn.Linear(dims, 1)

    def embed(
        self,
        numeric_features: mx.array,
        categorical_indices: mx.array,
        umaban: mx.array,
    ) -> mx.array:
        x = self.numeric_projection(numeric_features)
        for col_idx, embedding in enumerate(self.categorical_embeddings):
            x = x + embedding(categorical_indices[:, :, col_idx])
        x = x + self.umaban_embedding(umaban)
        return self.input_layer_norm(x)

    def __call__(
        self,
        numeric_features: mx.array,
        categorical_indices: mx.array,
        umaban: mx.array,
        mask: mx.array,
    ) -> ModelOutput:
        embedded = self.embed(numeric_features, categorical_indices, umaban)
        attention_mask = build_padding_mask(mask, self.config["num_heads"])
        encoded = self.encoder(embedded, attention_mask)
        top1_logit = self.top1_head(encoded).squeeze(-1)
        top3_logit = self.top3_head(encoded).squeeze(-1)
        rank_score = self.rank_head(encoded).squeeze(-1)
        return {
            "top1_logit": top1_logit,
            "top3_logit": top3_logit,
            "rank_score": rank_score,
        }


__all__ = (
    "DEFAULT_EMBEDDING_DIM",
    "DEFAULT_NUM_LAYERS",
    "DEFAULT_NUM_HEADS",
    "DEFAULT_FFN_DIM_MULTIPLIER",
    "DEFAULT_DROPOUT",
    "UMABAN_VOCAB_SIZE",
    "ModelConfig",
    "ModelOutput",
    "RaceSetTransformer",
    "build_padding_mask",
    "default_model_config",
)
