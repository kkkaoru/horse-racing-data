"""Apply supplied physical roster evidence without losing feature rows silently."""

import re
from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

from predict_lib.training_roster_match import (
    FeatureRunnerKey,
    MatchedTrainingRows,
    TeacherScopeEvidence,
    match_training_rows,
)

_REQUIRED: Final[frozenset[str]] = frozenset(
    {"race_id", "ketto_toroku_bango", "umaban", "finish_position"}
)
_RESERVED: Final[frozenset[str]] = frozenset(
    {"teacher_disposition", "teacher_finish", "teacher_relevance"}
)


@dataclass(frozen=True)
class AdmittedTrainingFrame:
    active: pd.DataFrame
    withdrawn: pd.DataFrame
    matching: MatchedTrainingRows


def admit_training_frame(
    frame: pd.DataFrame,
    *,
    evidence: TeacherScopeEvidence,
    required_race_ids: frozenset[str],
) -> AdmittedTrainingFrame:
    """Return independent copies, or fail the entire frame before any model work.

    Supplied source references are not verified here. Missing finishes are accepted
    only when independent outcome evidence explicitly supports their disposition.
    """
    if not frame.columns.is_unique or not _REQUIRED.issubset(frame.columns):
        raise ValueError("Training frame requires unique identity and finish columns")
    if _RESERVED.intersection(frame.columns):
        raise ValueError("Training frame already contains reserved teacher columns")
    keys = tuple(
        _feature_key(race, horse, bib)
        for race, horse, bib in zip(
            frame["race_id"].tolist(),
            frame["ketto_toroku_bango"].tolist(),
            frame["umaban"].tolist(),
            strict=True,
        )
    )
    matching = match_training_rows(
        feature_rows=keys, evidence=evidence, required_race_ids=required_race_ids
    )
    _validate_finishes(frame, matching)
    active = frame.iloc[list(matching.active_indices)].copy(deep=True)
    withdrawn = frame.iloc[list(matching.withdrawn_indices)].copy(deep=True)
    active["teacher_disposition"] = pd.array(
        [outcome.disposition for outcome in matching.active_outcomes], dtype=object
    )
    active["teacher_finish"] = pd.array(
        [outcome.finish for outcome in matching.active_outcomes], dtype=object
    )
    return AdmittedTrainingFrame(active, withdrawn, matching)


def _feature_key(race: object, horse: object, bib: object) -> FeatureRunnerKey:
    if not isinstance(race, str) or not isinstance(horse, str):
        raise ValueError("Training identities must be strings, never coerced placeholders")
    return FeatureRunnerKey(race, horse, _bib(bib))


def _bib(value: object) -> int | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("Boolean bib is not a physical runner number")
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        return int(value)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if np.isnan(number):
            return None
        if number.is_integer():
            return int(number)
    raise ValueError("Bib must be an integer-valued number, digit string or missing")


def _validate_finishes(frame: pd.DataFrame, matching: MatchedTrainingRows) -> None:
    expected = dict(
        zip(
            matching.active_indices,
            (outcome.finish for outcome in matching.active_outcomes),
            strict=True,
        )
    )
    expected.update(dict.fromkeys(matching.withdrawn_indices, None))
    for index, value in enumerate(frame["finish_position"].tolist()):
        if not _finish_agrees(value, expected[index]):
            raise ValueError(f"Feature finish conflicts with independent outcome at row {index}")


def _finish_agrees(value: object, expected: int | None) -> bool:
    if value is None or value is pd.NA:
        return expected is None
    if isinstance(value, (bool, np.bool_)):
        return False
    if isinstance(value, (int, np.integer)):
        return expected is not None and bool(value == expected)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if np.isnan(number):
            return expected is None
        return expected is not None and bool(np.isfinite(number)) and number == expected
    if isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            return False
        return expected is not None and bool(np.isfinite(number)) and number == expected
    return False
