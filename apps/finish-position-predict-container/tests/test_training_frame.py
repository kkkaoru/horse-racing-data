"""Frame admission preserves covariates and rejects contradictions before fitting."""

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from predict_lib.training_admission import RunnerOutcome
from predict_lib.training_frame import admit_training_frame
from predict_lib.training_roster_match import (
    EvidenceReferences,
    TeacherRaceEvidence,
    TeacherScopeEvidence,
)


@pytest.fixture
def evidence() -> TeacherScopeEvidence:
    return TeacherScopeEvidence(
        EvidenceReferences("a" * 64, "b" * 64),
        (
            TeacherRaceEvidence(
                "r",
                2,
                (
                    RunnerOutcome("a", 1, "classified", 1),
                    RunnerOutcome("b", 19, "dnf", None),
                    RunnerOutcome("scratch", None, "withdrawn", None),
                ),
            ),
        ),
    )


@pytest.fixture
def frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "race_id": ["r", "r", "r"],
            "ketto_toroku_bango": ["b", "scratch", "a"],
            "umaban": ["19", None, 1.0],
            "finish_position": pd.array([pd.NA, np.nan, "1.0"], dtype=object),
            "experience": [np.nan, 0.0, 2.0],
        },
        index=[7, 3, 7],
        dtype=object,
    )


def test_copies_preserve_order_duplicate_indices_missingness_and_source_outcomes(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
) -> None:
    result = admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))
    assert result.active.index.tolist() == [7, 7]
    assert result.withdrawn.index.tolist() == [3]
    assert result.active["ketto_toroku_bango"].tolist() == ["b", "a"]
    assert result.active["umaban"].tolist() == ["19", 1.0]
    assert result.active["teacher_disposition"].tolist() == ["dnf", "classified"]
    assert result.active["teacher_finish"].tolist() == [None, 1]
    assert result.active["teacher_finish"].iloc[0] is None
    assert pd.isna(result.active["experience"].iloc[0])
    assert result.withdrawn["experience"].tolist() == [0.0]
    assert result.matching.active_indices == (0, 2)
    assert result.matching.withdrawn_indices == (1,)
    result.active["experience"] = 99.0
    result.withdrawn["experience"] = 99.0
    assert pd.isna(frame["experience"].iloc[0])
    assert frame["experience"].iloc[1] == 0.0
    assert frame.columns.tolist() == [
        "race_id",
        "ketto_toroku_bango",
        "umaban",
        "finish_position",
        "experience",
    ]


@pytest.mark.parametrize("bib", [1, np.int64(1), "01", 1.0])
@pytest.mark.parametrize("finish", [1, np.int64(1), 1.0, "1"])
def test_numeric_representations_are_checked_without_changing_original_columns(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
    bib: object,
    finish: object,
) -> None:
    frame.iat[2, 2] = bib
    frame.iat[2, 3] = finish
    result = admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))
    assert result.active["teacher_finish"].tolist() == [None, 1]
    assert result.matching.active_indices == (0, 2)


@pytest.mark.parametrize("missing", [None, pd.NA, np.nan])
def test_explicit_withdrawal_can_have_missing_bib_and_finish(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
    missing: object,
) -> None:
    frame.iat[1, 2] = missing
    frame.iat[1, 3] = missing
    result = admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))
    assert result.matching.withdrawn_indices == (1,)


@pytest.mark.parametrize("bib", [True, np.bool_(True), "x", 1.5, float("inf"), []])
def test_invalid_bib_is_not_coerced(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
    bib: object,
) -> None:
    frame.iat[2, 2] = bib
    with pytest.raises(ValueError, match=r"[Bb]ib"):
        admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))


@pytest.mark.parametrize(
    "finish",
    [
        None,
        pd.NA,
        np.nan,
        True,
        np.bool_(True),
        2,
        1.5,
        float("inf"),
        "garbage",
        "nan",
        [],
    ],
)
def test_classified_feature_finish_conflicts_fail_closed(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
    finish: object,
) -> None:
    frame.iat[2, 3] = finish
    with pytest.raises(ValueError, match="finish conflicts"):
        admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))


@pytest.mark.parametrize("finish", [0, 1.0, "1"])
def test_explicit_nonfinish_does_not_hide_a_conflicting_feature_rank(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
    finish: object,
) -> None:
    frame.iat[0, 3] = finish
    with pytest.raises(ValueError, match="finish conflicts"):
        admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))


@pytest.mark.parametrize("column", ["race_id", "ketto_toroku_bango"])
def test_missing_identity_is_not_stringified(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
    column: str,
) -> None:
    frame[column] = None
    with pytest.raises(ValueError, match="identities must be strings"):
        admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))


def test_missing_required_column_fails(frame: pd.DataFrame, evidence: TeacherScopeEvidence) -> None:
    with pytest.raises(ValueError, match="unique identity and finish columns"):
        admit_training_frame(
            frame.drop(columns="finish_position"),
            evidence=evidence,
            required_race_ids=frozenset({"r"}),
        )


def test_duplicate_columns_fail(frame: pd.DataFrame, evidence: TeacherScopeEvidence) -> None:
    duplicated = pd.concat([frame, frame[["race_id"]]], axis=1)
    with pytest.raises(ValueError, match="unique identity and finish columns"):
        admit_training_frame(duplicated, evidence=evidence, required_race_ids=frozenset({"r"}))


@pytest.mark.parametrize("reserved", ["teacher_finish", "teacher_relevance"])
def test_reserved_columns_are_not_overwritten(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
    reserved: str,
) -> None:
    frame[reserved] = None
    with pytest.raises(ValueError, match="reserved teacher columns"):
        admit_training_frame(frame, evidence=evidence, required_race_ids=frozenset({"r"}))


def test_absent_independent_count_is_not_replaced_by_frame_count(
    frame: pd.DataFrame,
    evidence: TeacherScopeEvidence,
) -> None:
    unverified = replace(evidence, races=(replace(evidence.races[0], declared_starters=None),))
    with pytest.raises(ValueError, match="starter_count_unverified"):
        admit_training_frame(frame, evidence=unverified, required_race_ids=frozenset({"r"}))
