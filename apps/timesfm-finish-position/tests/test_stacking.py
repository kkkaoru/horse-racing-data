from __future__ import annotations

import numpy as np
import pytest

import timesfm_finish_position.stacking as subject
from timesfm_finish_position.stacking import fit_logistic_stacker, stacking_backend


def test_fit_logistic_stacker_uses_mlx_and_normalizes_per_race() -> None:
    features = np.asarray(
        [[3.0, 2.0], [2.0, 1.0], [1.0, 0.0], [0.0, 1.0], [1.0, 2.0], [2.0, 3.0]],
        dtype=np.float64,
    )
    labels = np.asarray([1.0, 0.0, 0.0, 0.0, 0.0, 1.0], dtype=np.float64)
    races = np.asarray(["r1", "r1", "r1", "r2", "r2", "r2"], dtype=np.str_)
    model = fit_logistic_stacker(features, labels, l2=1.0)
    probabilities, logits = model.predict(features, races)
    assert model.backend == "mlx"
    assert stacking_backend() == "mlx"
    assert probabilities[:3].sum() == pytest.approx(1.0)
    assert probabilities[3:].sum() == pytest.approx(1.0)
    assert logits.shape == (6,)


def test_fit_logistic_stacker_uses_numpy_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject.platform, "system", lambda: "Linux")
    features = np.asarray([[2.0], [1.0], [0.0], [3.0]], dtype=np.float64)
    labels = np.asarray([1.0, 0.0, 0.0, 1.0], dtype=np.float64)
    model = fit_logistic_stacker(features, labels, l2=0.5)
    probabilities, logits = model.predict(features, np.asarray(["r1", "r1", "r2", "r2"]))
    assert model.backend == "numpy"
    assert probabilities.tolist() == pytest.approx([0.5, 0.5, 0.5, 0.5], abs=0.5)
    assert logits.shape == (4,)


def test_logistic_stacker_rejects_invalid_training_and_prediction_shapes() -> None:
    with pytest.raises(ValueError, match="must align"):
        fit_logistic_stacker(np.zeros((2, 1)), np.zeros(1))
    with pytest.raises(ValueError, match="must not be empty"):
        fit_logistic_stacker(np.zeros((0, 1)), np.zeros(0))
    with pytest.raises(ValueError, match="labels must be binary"):
        fit_logistic_stacker(np.zeros((2, 1)), np.asarray([0.0, 2.0]))
    with pytest.raises(ValueError, match="l2 must be positive"):
        fit_logistic_stacker(np.zeros((2, 1)), np.asarray([0.0, 1.0]), l2=0.0)
    model = fit_logistic_stacker(
        np.asarray([[0.0], [1.0]], dtype=np.float64),
        np.asarray([0.0, 1.0], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="feature shape does not match"):
        model.predict(np.zeros((2, 2)), np.asarray(["r1", "r1"]))
