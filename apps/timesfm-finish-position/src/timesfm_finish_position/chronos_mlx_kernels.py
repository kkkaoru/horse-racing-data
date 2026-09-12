"""Opt-in Chronos-2 attention kernels preserving unscaled attention semantics."""

import mlx.core as mx
from chronos2_mlx.attention import MultiHeadAttention
from chronos2_mlx.layers import Chronos2Encoder
from chronos2_mlx.model import Chronos2MLXModel
from chronos2_mlx.rope import apply_rope
from mlx.utils import tree_unflatten


class FiniteMaskEncoder(Chronos2Encoder):
    """Keep masked padding finite when FP32 minimum overflows the BF16 range."""

    @staticmethod
    def _make_time_mask(attention_mask: mx.array, dtype: mx.Dtype) -> mx.array:
        mask = Chronos2Encoder._make_time_mask(attention_mask, mx.float32)
        return mx.maximum(mask, mx.finfo(dtype).min).astype(dtype)

    @staticmethod
    def _make_group_time_mask(
        group_ids: mx.array, attention_mask: mx.array, dtype: mx.Dtype
    ) -> mx.array:
        mask = Chronos2Encoder._make_group_time_mask(group_ids, attention_mask, mx.float32)
        return mx.maximum(mask, mx.finfo(dtype).min).astype(dtype)


def enable_finite_attention_masks(model: Chronos2MLXModel) -> None:
    """Install before adapter injection/freezing; retain all pretrained parameters."""
    encoder = FiniteMaskEncoder(model.config)
    encoder.update(model.encoder.parameters())
    model.encoder = encoder


class FusedChronosAttention(MultiHeadAttention):
    """Use Metal SDPA without changing Chronos-2 projection names or RoPE."""

    def __call__(
        self,
        hidden_states: mx.array,
        mask: mx.array | None = None,
        position_ids: mx.array | None = None,
    ) -> mx.array:
        sequence_length = hidden_states.shape[1]
        query = self._shape(self.q(hidden_states), sequence_length)
        key = self._shape(self.k(hidden_states), sequence_length)
        value = self._shape(self.v(hidden_states), sequence_length)
        if self.use_rope:
            if position_ids is None:
                raise ValueError("RoPE attention requires position_ids")
            query, key = apply_rope(query, key, position_ids, theta=self.rope_theta)
        output = mx.fast.scaled_dot_product_attention(query, key, value, scale=1.0, mask=mask)
        return self.o(self._unshape(output, sequence_length))


def enable_fused_attention(model: Chronos2MLXModel) -> int:
    """Replace attention modules using MLX's nested module-tree contract."""
    replacements = [
        (name, fused_attention_copy(module))
        for name, module in model.named_modules()
        if isinstance(module, MultiHeadAttention)
    ]
    model.update_modules(tree_unflatten(replacements))
    return len(replacements)


def fused_attention_copy(attention: MultiHeadAttention) -> FusedChronosAttention:
    """Copy projections into a replacement; call before freezing/adapter injection."""
    replacement = FusedChronosAttention(
        d_model=attention.q.weight.shape[1],
        num_heads=attention.num_heads,
        d_kv=attention.d_kv,
        use_rope=attention.use_rope,
        rope_theta=attention.rope_theta,
    )
    replacement.update(attention.parameters())
    return replacement
