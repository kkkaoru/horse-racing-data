"""Physical completeness must not be inferred from two equal retained subsets."""

from dataclasses import asdict

import pytest

from predict_lib.training_admission import (
    TeacherRunner,
    audit_teacher_roster,
    train_after_admission,
)


def forbidden_fit() -> None:
    raise AssertionError("A rejected or incomplete roster must never invoke training")


def test_runner_outcome_base_keeps_serialized_teacher_layout() -> None:
    assert asdict(TeacherRunner("a", 19, "dnf", None, False)) == {
        "horse_id": "a",
        "horse_number": 19,
        "disposition": "dnf",
        "finish": None,
        "feature_row_present": False,
    }


def test_withdrawn_runner_cannot_also_have_a_finish() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(
            TeacherRunner("a", 1, "classified", 1, True),
            TeacherRunner("b", 2, "withdrawn", 2, False),
        ),
    )
    assert result.issues == ("withdrawn_has_numeric_rank",)
    with pytest.raises(ValueError, match="withdrawn_has_numeric_rank"):
        train_after_admission(
            audits=(result,),
            required_race_ids=frozenset({"race"}),
            train=forbidden_fit,
        )


def test_equal_retained_subsets_do_not_pass_independent_starter_count() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=8,
        runners=(TeacherRunner("native:one", 4, "classified", 1, True),),
    )
    assert result.active_indices == (0,)
    assert result.issues == ("physical_starter_count_mismatch",)
    with pytest.raises(ValueError, match="physical_starter_count_mismatch"):
        train_after_admission(
            audits=(result,), required_race_ids=frozenset({"race"}), train=forbidden_fit
        )


def test_complete_roster_calls_fit_exactly_once_without_renumbering() -> None:
    called: list[bool] = []

    def fit() -> str:
        called.append(True)
        return "model"

    result = audit_teacher_roster(
        race_id="race",
        expected_starters=2,
        runners=(
            TeacherRunner("native:a", 1, "classified", 1, True),
            TeacherRunner("native:b", 19, "classified", 2, True),
        ),
    )
    assert result.issues == ()
    assert result.active_indices == (0, 1)
    assert (
        train_after_admission(audits=(result,), required_race_ids=frozenset({"race"}), train=fit)
        == "model"
    )
    assert called == [True]


def test_ties_dnf_dq_and_withdrawals_remain_distinct() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=5,
        runners=(
            TeacherRunner("a", 1, "classified", 1, True),
            TeacherRunner("b", 2, "classified", 1, True),
            TeacherRunner("c", 3, "classified", 3, True),
            TeacherRunner("d", 4, "dnf", None, True),
            TeacherRunner("e", 5, "dq", None, True),
            TeacherRunner("f", None, "withdrawn", None, False),
        ),
    )
    assert result.issues == ()
    assert result.active_indices == (0, 1, 2, 3, 4)
    assert result.withdrawn_indices == (5,)
    assert result.unresolved_indices == ()


def test_unresolved_participation_is_not_inferred_active_or_withdrawn() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=2,
        runners=(
            TeacherRunner("a", 1, "classified", 1, True),
            TeacherRunner("b", 2, "unknown_status", None, False),
        ),
    )
    assert result.active_indices == (0,)
    assert result.withdrawn_indices == ()
    assert result.unresolved_indices == (1,)
    assert result.issues == ("participation_unresolved", "physical_starter_count_mismatch")


def test_known_active_unknown_finish_is_not_dnf() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=2,
        runners=(
            TeacherRunner("a", 1, "classified", 1, True),
            TeacherRunner("b", 2, "unresolved_finish", None, True),
        ),
    )
    assert result.active_indices == (0, 1)
    assert result.issues == ("active_finish_unresolved",)


def test_unknown_metadata_is_not_replaced_by_retained_row_count() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=None,
        runners=(TeacherRunner("a", 1, "classified", 1, True),),
    )
    assert result.expected_starters is None
    assert result.issues == ("starter_count_unverified",)


def test_missing_feature_row_is_explicit_and_not_dropped_or_invented() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", 1, "classified", 1, False),),
    )
    assert result.active_indices == (0,)
    assert result.missing_feature_indices == (0,)
    assert result.issues == ()


@pytest.mark.parametrize("number", [None, 0, -1, True])
def test_unknown_bib_is_not_zero_or_a_generated_number(number: int | None) -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", number, "classified", 1, True),),
    )
    assert result.issues == ("missing_or_invalid_active_bib",)


def test_duplicate_physical_slot_and_identity_are_rejected() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=2,
        runners=(
            TeacherRunner("a", 1, "classified", 1, True),
            TeacherRunner("a", 1, "classified", 2, True),
        ),
    )
    assert result.issues == ("duplicate_active_bib", "duplicate_active_identity")


@pytest.mark.parametrize("identity", ["", " ", "0000000000", " a"])
def test_blank_placeholder_or_noncanonical_identity_is_rejected(identity: str) -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner(identity, 1, "classified", 1, True),),
    )
    assert result.issues == ("missing_or_placeholder_active_identity",)


@pytest.mark.parametrize("finish", [None, 0, -1, True])
def test_invalid_classified_finish(finish: int | None) -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", 1, "classified", finish, True),),
    )
    assert result.issues == ("invalid_classified_finish",)


def test_nonfinish_cannot_have_invented_last_place() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", 1, "dnf", 1, True),),
    )
    assert result.issues == ("nonfinish_has_numeric_rank",)


def test_competition_rank_gap_fails_for_complete_field() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=2,
        runners=(
            TeacherRunner("a", 1, "classified", 1, True),
            TeacherRunner("b", 2, "classified", 3, True),
        ),
    )
    assert result.issues == ("invalid_competition_ranks",)


def test_complete_nonfinish_evidence_does_not_invent_a_winner() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", 1, "dnf", None, True),),
    )
    assert result.active_indices == (0,)
    assert result.issues == ()


def test_no_active_runners_cannot_be_a_training_group() -> None:
    result = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", None, "withdrawn", None, False),),
    )
    assert result.issues == ("no_verified_active_runners", "physical_starter_count_mismatch")


@pytest.mark.parametrize("expected", [0, -1, True])
def test_invalid_expected_starters(expected: int) -> None:
    with pytest.raises(ValueError, match="Expected starters"):
        audit_teacher_roster(race_id="race", expected_starters=expected, runners=())


def test_blank_race_id() -> None:
    with pytest.raises(ValueError, match="Race identity"):
        audit_teacher_roster(race_id=" ", expected_starters=1, runners=())


def test_unrecognized_status_cannot_fall_through_as_a_nonstarter() -> None:
    with pytest.raises(ValueError, match="Unknown disposition"):
        audit_teacher_roster(
            race_id="race",
            expected_starters=1,
            runners=(TeacherRunner("a", 1, "typo", None, True),),
        )


def test_empty_audits_never_fit() -> None:
    with pytest.raises(ValueError, match="nonempty unique"):
        train_after_admission(audits=(), required_race_ids=frozenset(), train=forbidden_fit)


def test_duplicate_audits_never_fit() -> None:
    audit = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", 1, "classified", 1, True),),
    )
    with pytest.raises(ValueError, match="nonempty unique"):
        train_after_admission(
            audits=(audit, audit), required_race_ids=frozenset({"race"}), train=forbidden_fit
        )


def test_unrequested_race_never_fits() -> None:
    audit = audit_teacher_roster(
        race_id="extra",
        expected_starters=1,
        runners=(TeacherRunner("a", 1, "classified", 1, True),),
    )
    with pytest.raises(ValueError, match="complete required teacher scope"):
        train_after_admission(
            audits=(audit,), required_race_ids=frozenset({"race"}), train=forbidden_fit
        )


def test_dropping_an_entire_required_race_never_fits() -> None:
    audit = audit_teacher_roster(
        race_id="race",
        expected_starters=1,
        runners=(TeacherRunner("a", 1, "classified", 1, True),),
    )
    with pytest.raises(ValueError, match="complete required teacher scope"):
        train_after_admission(
            audits=(audit,), required_race_ids=frozenset({"race", "omitted"}), train=forbidden_fit
        )
