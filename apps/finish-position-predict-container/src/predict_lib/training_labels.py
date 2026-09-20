"""Encode explicit outcomes without inventing classified ranks for nonfinishes."""

from collections.abc import Sequence
from typing import Final

import numpy as np
import numpy.typing as npt

from predict_lib.rank_relevance import RelevanceMode, rank_gains
from predict_lib.training_admission import RunnerOutcome

_UNRANKED: Final[frozenset[str]] = frozenset({"dnf", "dq"})


def outcome_rank_gains(
    outcomes: Sequence[RunnerOutcome], *, mode: RelevanceMode
) -> npt.NDArray[np.float64]:
    """Encode outcomes after roster admission; this alone never authorizes fitting."""
    indices: list[int] = []
    finishes: list[int] = []
    for index, outcome in enumerate(outcomes):
        finish = _classified_finish(outcome)
        if finish is not None:
            indices.append(index)
            finishes.append(finish)
    labels = np.zeros(len(outcomes), dtype=np.float64)
    labels[indices] = rank_gains(finishes, mode=mode)
    return labels


def _classified_finish(outcome: RunnerOutcome) -> int | None:
    if outcome.disposition in _UNRANKED:
        if outcome.finish is not None:
            raise ValueError("An explicit nonfinish cannot also have a classified rank")
        return None
    if outcome.disposition != "classified" or type(outcome.finish) is not int:
        raise ValueError("Training relevance requires an explicit classified or unranked outcome")
    return outcome.finish
