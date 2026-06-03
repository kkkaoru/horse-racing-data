"""Load an XGBoost JSON model and adapt it to ``BoosterLike``.

I/O wrapper around the native XGBoost runtime — not unit-tested (it needs the
compiled library + a real model.json), exercised at deploy time per DEPLOY.md.
Feature rows are already float32-quantised by ``predict_lib.scorer`` per
FINISH_POSITION_MODEL_V7_LINEAGE.md section 10.4.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol


class XgboostBoosterLike(Protocol):
    def predict(self, data: object) -> Iterable[float]: ...


class XgboostBooster:
    """Adapt a loaded XGBoost Booster to the scorer's ``predict`` signature."""

    _booster: XgboostBoosterLike

    def __init__(self, booster: XgboostBoosterLike) -> None:
        self._booster = booster

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        import xgboost

        return [float(score) for score in self._booster.predict(xgboost.DMatrix(matrix))]


def load_xgboost_booster(model_path: str) -> XgboostBooster:
    """Load ``model.json`` (XGBoost booster.save_model) into a booster.

    Clears the booster's ``feature_names`` after load so predictions made
    against a positional DMatrix (built in ``predict_lib.scorer`` from the
    canonical metadata feature order) are not rejected by XGBoost's strict
    feature-name validation. The matrix column order already matches the
    metadata ``feature_names`` order from the same model artifact, so
    positional alignment is guaranteed; the booster's internal name list
    would otherwise force every caller to also build a named DMatrix.
    """
    import xgboost

    booster = xgboost.Booster()
    booster.load_model(model_path)
    booster.feature_names = None
    return XgboostBooster(booster)
