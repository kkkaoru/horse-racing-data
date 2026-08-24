"""Load a CatBoost JSON model and adapt it to ``BoosterLike``.

I/O wrapper around the native CatBoost runtime — not unit-tested (it needs the
compiled library + a real model.json), exercised at deploy time per DEPLOY.md.
Kept tiny so ``predict_upcoming`` can stay free of native imports until needed.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, cast

from predict_lib.dynamic_market_shadow import ProbabilityModel
from predict_lib.scorer import BoosterLike

CATBOOST_MODEL_FORMAT: str = "json"
RAW_FORMULA_VAL: str = "RawFormulaVal"


class CatBoostModelLike(Protocol):
    def predict(
        self, data: Sequence[Sequence[float]], prediction_type: str
    ) -> Sequence[float]: ...


class CatBoostBooster:
    """Adapt a loaded CatBoost model to the scorer's ``predict`` signature."""

    _model: CatBoostModelLike

    def __init__(self, model: CatBoostModelLike) -> None:
        self._model = model

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        raw = self._model.predict(matrix, prediction_type=RAW_FORMULA_VAL)
        return [float(score) for score in raw]

    @property
    def feature_names_(self) -> Sequence[str]:
        """Expose the native training order for artifact-contract validation."""
        raw = getattr(self._model, "feature_names_", ())
        if not isinstance(raw, (list, tuple)) or not all(
            isinstance(name, str) for name in raw
        ):
            return ()
        return tuple(raw)


def load_catboost_booster(model_path: str) -> BoosterLike:
    """Load ``model.json`` (CatBoost save_model format='json') into a booster."""
    from catboost import CatBoost

    model = CatBoost()
    model.load_model(model_path, format=CATBOOST_MODEL_FORMAT)
    return CatBoostBooster(model)


class CatBoostClassifierLike(Protocol):
    def predict_proba(
        self, data: Sequence[Sequence[float]]
    ) -> Sequence[Sequence[float]]: ...


class CatBoostProbabilityModel:
    """Adapt a CatBoost binary classifier to the shadow probability protocol."""

    _model: CatBoostClassifierLike

    def __init__(self, model: CatBoostClassifierLike) -> None:
        self._model = model

    def predict_proba(
        self, matrix: Sequence[Sequence[float]]
    ) -> Sequence[Sequence[float]]:
        raw = self._model.predict_proba(matrix)
        return [[float(value) for value in row] for row in raw]


def load_catboost_probability_model(model_path: str) -> ProbabilityModel:
    """Load a CatBoost JSON binary classifier for shadow-only inference."""
    from catboost import CatBoostClassifier

    model = CatBoostClassifier()
    model.load_model(model_path, format=CATBOOST_MODEL_FORMAT)
    return CatBoostProbabilityModel(cast(CatBoostClassifierLike, model))
