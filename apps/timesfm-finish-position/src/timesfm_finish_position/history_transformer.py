"""MLX Transformer encoder for irregular per-horse race histories."""

from __future__ import annotations

from dataclasses import dataclass

import mlx.core as mx
import mlx.nn as nn

ATTENTION_NEGATIVE_INFINITY = -1e9


@dataclass(frozen=True)
class HistoryTransformerConfig:
    """Small local history encoder configuration."""

    input_features: int
    max_history: int = 10
    dimensions: int = 64
    layers: int = 2
    heads: int = 4
    feed_forward_dimensions: int = 128
    dropout: float = 0.1


class HorseHistoryTransformer(nn.Module):
    """Encode ten prior starts plus a target-gap query token."""

    def __init__(self, config: HistoryTransformerConfig) -> None:
        super().__init__()
        self.config = config
        self.input_projection = nn.Linear(config.input_features, config.dimensions)
        self.query_projection = nn.Linear(1, config.dimensions)
        self.position_embedding = mx.zeros((config.max_history + 1, config.dimensions))
        self.encoder = nn.TransformerEncoder(
            num_layers=config.layers,
            dims=config.dimensions,
            num_heads=config.heads,
            mlp_dims=config.feed_forward_dimensions,
            dropout=config.dropout,
            norm_first=True,
        )
        self.output_norm = nn.LayerNorm(config.dimensions)
        self.performance_head = nn.Linear(config.dimensions, 1)
        self.uncertainty_head = nn.Linear(config.dimensions, 1)

    def __call__(
        self, histories: mx.array, mask: mx.array, target_days_since_last: mx.array
    ) -> tuple[mx.array, mx.array, mx.array]:
        """Return embedding, next-performance estimate, and positive uncertainty."""
        if histories.ndim != 3:
            raise ValueError("histories must have shape (batch, history, features)")
        if histories.shape[1:] != (self.config.max_history, self.config.input_features):
            raise ValueError("history tensor shape does not match model configuration")
        if mask.shape != histories.shape[:2]:
            raise ValueError("history mask must align with history tensor")
        if target_days_since_last.shape != (histories.shape[0],):
            raise ValueError("target gap must have one value per horse")
        embedded = self.input_projection(histories)
        query = self.query_projection(target_days_since_last[:, None])[:, None, :]
        tokens = mx.concatenate([embedded, query], axis=1) + self.position_embedding[None, :, :]
        token_mask = mx.concatenate(
            [mask, mx.ones((histories.shape[0], 1), dtype=mx.bool_)], axis=1
        )
        additive_mask = (1.0 - token_mask.astype(mx.float32))[:, None, None, :]
        encoded = self.encoder(tokens, additive_mask * ATTENTION_NEGATIVE_INFINITY)
        horse_embedding = self.output_norm(encoded[:, -1, :])
        performance = self.performance_head(horse_embedding).squeeze(-1)
        uncertainty = nn.softplus(self.uncertainty_head(horse_embedding).squeeze(-1))
        return horse_embedding, performance, uncertainty
