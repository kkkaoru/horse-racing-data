from __future__ import annotations

import mlx.core as mx
import pytest

from timesfm_finish_position.history_transformer import (
    HistoryTransformerConfig,
    HorseHistoryTransformer,
)


def test_horse_history_transformer_outputs_embedding_prediction_and_uncertainty() -> None:
    config = HistoryTransformerConfig(input_features=13, max_history=10, dimensions=16, heads=4)
    model = HorseHistoryTransformer(config)
    embedding, performance, uncertainty = model(
        mx.zeros((2, 10, 13)),
        mx.array(
            [
                [False, False, False, False, False, True, True, True, True, True],
                [False, False, False, False, False, False, False, False, True, True],
            ]
        ),
        mx.array([14.0, 90.0]),
    )
    mx.eval(embedding, performance, uncertainty)
    assert embedding.shape == (2, 16)
    assert performance.shape == (2,)
    assert uncertainty.shape == (2,)
    assert bool(mx.all(uncertainty > 0.0).item()) is True


def test_horse_history_transformer_rejects_invalid_shapes() -> None:
    model = HorseHistoryTransformer(
        HistoryTransformerConfig(input_features=3, max_history=2, dimensions=8, heads=2)
    )
    with pytest.raises(ValueError, match="histories must have shape"):
        model(mx.zeros((2, 3)), mx.ones((2, 2), dtype=mx.bool_), mx.zeros((2,)))
    with pytest.raises(ValueError, match="does not match model configuration"):
        model(mx.zeros((2, 3, 3)), mx.ones((2, 3), dtype=mx.bool_), mx.zeros((2,)))
    with pytest.raises(ValueError, match="mask must align"):
        model(mx.zeros((2, 2, 3)), mx.ones((2, 1), dtype=mx.bool_), mx.zeros((2,)))
    with pytest.raises(ValueError, match="target gap must have one value"):
        model(mx.zeros((2, 2, 3)), mx.ones((2, 2), dtype=mx.bool_), mx.zeros((1,)))
