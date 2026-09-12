"""Numerical and gradient checks for opt-in Chronos attention acceleration."""

import mlx.core as mx
import numpy as np
import pytest
from chronos2_mlx.attention import MultiHeadAttention
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel

from timesfm_finish_position.chronos_mlx_kernels import (
    enable_finite_attention_masks,
    enable_fused_attention,
    fused_attention_copy,
)


@pytest.mark.parametrize("dtype", [mx.float32, mx.bfloat16])
@pytest.mark.parametrize("use_rope", [False, True])
def test_fused_attention_output_and_input_gradient(dtype: mx.Dtype, use_rope: bool) -> None:
    mx.random.seed(11)
    original = MultiHeadAttention(32, 2, 16, use_rope=use_rope)
    original.set_dtype(dtype)
    fused = fused_attention_copy(original)
    inputs = mx.random.normal((2, 8, 32)).astype(dtype)
    positions = mx.broadcast_to(mx.arange(8)[None, :], (2, 8))
    mask = mx.where(mx.arange(8)[None, None, None, :] < 6, 0.0, -1e9).astype(dtype)
    expected = original(inputs, mask=mask, position_ids=positions)
    actual = fused(inputs, mask=mask, position_ids=positions)

    def original_loss(x: mx.array) -> mx.array:
        return original(x, mask=mask, position_ids=positions).astype(mx.float32).square().mean()

    def fused_loss(x: mx.array) -> mx.array:
        return fused(x, mask=mask, position_ids=positions).astype(mx.float32).square().mean()

    expected_grad = mx.grad(original_loss)(inputs)
    actual_grad = mx.grad(fused_loss)(inputs)
    tolerance = 0.015 if dtype == mx.bfloat16 else 1e-5
    np.testing.assert_allclose(
        np.array(actual.astype(mx.float32)),
        np.array(expected.astype(mx.float32)),
        atol=tolerance,
        rtol=tolerance,
    )
    np.testing.assert_allclose(
        np.array(actual_grad.astype(mx.float32)),
        np.array(expected_grad.astype(mx.float32)),
        atol=tolerance,
        rtol=tolerance,
    )


def test_rope_rejects_missing_positions() -> None:
    original = MultiHeadAttention(32, 2, 16, use_rope=True)
    fused = fused_attention_copy(original)
    with pytest.raises(ValueError, match="RoPE attention requires position_ids"):
        fused(mx.ones((1, 4, 32)))


def test_bf16_padding_mask_remains_finite() -> None:
    model = Chronos2MLXModel(
        Chronos2MLXConfig(d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2)
    )
    model.set_dtype(mx.bfloat16)
    enable_finite_attention_masks(model)
    context = mx.concatenate([mx.full((2, 112), float("nan")), mx.ones((2, 16))], axis=1)
    assert bool(mx.all(mx.isfinite(model(context))))


def test_full_model_replacement_preserves_predictions() -> None:
    mx.random.seed(4)
    model = Chronos2MLXModel(
        Chronos2MLXConfig(d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2)
    )
    inputs = mx.random.normal((2, 32))
    original = np.array(model(inputs))
    assert enable_fused_attention(model) == 2
    np.testing.assert_allclose(np.array(model(inputs)), original, atol=1e-5)


def test_unmasked_attention() -> None:
    mx.random.seed(9)
    original = MultiHeadAttention(32, 2, 16)
    fused = fused_attention_copy(original)
    inputs = mx.random.normal((1, 4, 32))
    np.testing.assert_allclose(np.array(fused(inputs)), np.array(original(inputs)), atol=1e-6)
