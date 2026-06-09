"""Load a LightGBM native-text model and adapt it to ``BoosterLike``.

I/O wrapper around the native LightGBM runtime — not unit-tested (it needs the
compiled library + a real ``model.txt``), exercised at deploy time per
DEPLOY.md. Mirrors ``catboost_adapter`` / ``xgboost_adapter`` so ``predict_lib``
stays free of native imports until a per-class LightGBM member is actually
loaded.

The iter 36 NAR class-C member is a LightGBM LambdaRank residual booster. For a
LambdaRank model ``Booster.predict`` returns the per-row ranking score, which is
exactly the per-row raw score the ensemble blend consumes. Feature rows are
scored POSITIONALLY by the member's own training feature order (resolved from
its ``metadata.json`` by ``predict_lib.ensemble_routing`` before scoring), so
the adapter passes the matrix straight through with no name-based reordering —
the same positional contract the XGBoost adapter relies on after clearing its
feature names. LightGBM ranking is robust at float64, so no float32 quantisation
is applied (``predict_lib.scorer.build_feature_row`` keeps the float64 path for
this architecture).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol, cast


class LightgbmBoosterLike(Protocol):
    # ``data`` and the return are typed ``object`` rather than narrowed: the
    # native ``lightgbm.Booster.predict`` returns ``ndarray | spmatrix |
    # List[spmatrix]`` (per the LightGBM type stubs), which does not satisfy a
    # narrowed ``Iterable[float]`` protocol; we coerce the result to a list of
    # floats in :meth:`LightgbmBooster.predict` instead. A LambdaRank model
    # returns a 1-D float ndarray (one ranking score per row).
    def predict(self, data: object) -> object: ...


def _coerce_scores(raw: object) -> list[float]:
    """Coerce a LightGBM ``predict`` result to a flat list of per-row floats.

    A LambdaRank model returns a 1-D float array (one ranking score per row),
    which is iterable; each element coerces to ``float``. Raises ``TypeError``
    via the ``iter`` call if a non-iterable result is ever returned, so a
    contract break fails loud rather than silently producing an empty vector.
    """
    return [float(score) for score in cast("Iterable[float]", raw)]


class LightgbmBooster:
    """Adapt a loaded LightGBM Booster to the scorer's ``predict`` signature."""

    _booster: LightgbmBoosterLike

    def __init__(self, booster: LightgbmBoosterLike) -> None:
        self._booster = booster

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        return _coerce_scores(self._booster.predict(matrix))


def load_lightgbm_booster(model_path: str) -> LightgbmBooster:
    """Load ``model.txt`` (LightGBM ``Booster.save_model`` text dump) into a booster.

    The text dump carries the model structure and the training feature order;
    the matrix the scorer builds is already projected onto the member's own
    ``metadata.json`` feature order, so the booster scores it positionally
    without any name-based realignment.
    """
    import lightgbm

    booster = lightgbm.Booster(model_file=model_path)
    return LightgbmBooster(booster)
