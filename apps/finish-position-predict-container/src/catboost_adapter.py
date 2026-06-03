"""Load a CatBoost JSON model and adapt it to ``BoosterLike``.

I/O wrapper around the native CatBoost runtime — not unit-tested (it needs the
compiled library + a real model.json), exercised at deploy time per DEPLOY.md.
Kept tiny so ``predict_upcoming`` can stay free of native imports until needed.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

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


def load_catboost_booster(model_path: str) -> BoosterLike:
    """Load ``model.json`` (CatBoost save_model format='json') into a booster."""
    from catboost import CatBoost

    model = CatBoost()
    model.load_model(model_path, format=CATBOOST_MODEL_FORMAT)
    return CatBoostBooster(model)
