from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.neural_preprocessing import (
    NeuralFeatureConfig,
    NumericEncoding,
    fit_neural_preprocessor,
)

NUMERIC = np.asarray([[0.0, np.nan], [1.0, 2.0], [2.0, 4.0]], dtype=np.float64)
CATEGORICAL = {
    "jockey": np.asarray(["j1", "j2", "j1"], dtype=np.str_),
    "trainer": np.asarray(["t1", "t1", "t2"], dtype=np.str_),
}


def test_normalized_preprocessor_is_fit_only_on_training_values() -> None:
    preprocessor = fit_neural_preprocessor(
        NUMERIC,
        CATEGORICAL,
        NeuralFeatureConfig(categorical_names=("jockey", "trainer")),
    )
    transformed = preprocessor.transform_numeric(np.asarray([[3.0, np.nan]], dtype=np.float64))
    assert preprocessor.numeric_output_features == 2
    assert transformed[0].tolist() == pytest.approx([2.4494898, 0.0])
    categories = preprocessor.transform_categorical(
        {
            "jockey": np.asarray(["j2", "new"], dtype=np.str_),
            "trainer": np.asarray(["t2", ""], dtype=np.str_),
        }
    )
    assert categories.tolist() == [[2, 2], [0, 0]]
    assert preprocessor.categorical_cardinalities == (3, 3)


def test_piecewise_linear_embedding_has_fixed_width_and_bounds() -> None:
    preprocessor = fit_neural_preprocessor(
        NUMERIC,
        {},
        NeuralFeatureConfig(
            numeric_encoding=NumericEncoding.PIECEWISE_LINEAR,
            piecewise_bins=2,
        ),
    )
    transformed = preprocessor.transform_numeric(
        np.asarray([[-1.0, 3.0], [3.0, 3.0]], dtype=np.float64)
    )
    assert preprocessor.numeric_output_features == 4
    assert transformed.shape == (2, 4)
    assert transformed[0].tolist() == pytest.approx([0.0, 0.0, 1.0, 0.0])
    assert transformed[1].tolist() == pytest.approx([1.0, 1.0, 1.0, 0.0])


def test_periodic_embedding_matches_portable_formula() -> None:
    preprocessor = fit_neural_preprocessor(
        NUMERIC,
        {},
        NeuralFeatureConfig(
            numeric_encoding=NumericEncoding.PERIODIC,
            periodic_frequencies=1,
        ),
    )
    transformed = preprocessor.transform_numeric(np.asarray([[1.0, 3.0]], dtype=np.float64))
    assert preprocessor.numeric_output_features == 4
    assert transformed[0].tolist() == pytest.approx([0.0, 1.0, 0.0, 1.0], abs=1e-6)


def test_preprocessor_rejects_invalid_or_missing_features() -> None:
    with pytest.raises(ValueError, match="nonempty matrix"):
        fit_neural_preprocessor(np.empty((0, 2)), CATEGORICAL, NeuralFeatureConfig())
    with pytest.raises(ValueError, match="dimensions are invalid"):
        fit_neural_preprocessor(
            NUMERIC,
            CATEGORICAL,
            NeuralFeatureConfig(piecewise_bins=1),
        )
    with pytest.raises(ValueError, match="feature is missing"):
        fit_neural_preprocessor(
            NUMERIC,
            CATEGORICAL,
            NeuralFeatureConfig(categorical_names=("owner",)),
        )
