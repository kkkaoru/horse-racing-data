"""Supplied independent counts do not excuse wrong or missing physical feature keys."""

import pytest

from predict_lib.training_admission import RunnerOutcome
from predict_lib.training_roster_match import (
    EvidenceReferences,
    FeatureRunnerKey,
    TeacherRaceEvidence,
    TeacherScopeEvidence,
    match_training_rows,
)


def test_matching_preserves_feature_order_and_explicit_outcomes() -> None:
    evidence = TeacherScopeEvidence(
        EvidenceReferences("a" * 64, "b" * 64),
        (
            TeacherRaceEvidence(
                "r1",
                2,
                (
                    RunnerOutcome("a", 1, "classified", 1),
                    RunnerOutcome("b", 19, "dnf", None),
                    RunnerOutcome("scratch", None, "withdrawn", None),
                    RunnerOutcome("absent-scratch", None, "withdrawn", None),
                ),
            ),
            TeacherRaceEvidence(
                "r2",
                2,
                (
                    RunnerOutcome("c", 2, "classified", 1),
                    RunnerOutcome("d", 5, "dq", None),
                ),
            ),
        ),
    )
    result = match_training_rows(
        feature_rows=(
            FeatureRunnerKey("r1", "b", 19),
            FeatureRunnerKey("r1", "scratch", None),
            FeatureRunnerKey("r2", "d", 5),
            FeatureRunnerKey("r2", "c", 2),
            FeatureRunnerKey("r1", "a", 1),
        ),
        evidence=evidence,
        required_race_ids=frozenset({"r1", "r2"}),
    )
    assert result.active_indices == (0, 2, 3, 4)
    assert result.withdrawn_indices == (1,)
    assert result.active_outcomes == (
        RunnerOutcome("b", 19, "dnf", None),
        RunnerOutcome("d", 5, "dq", None),
        RunnerOutcome("c", 2, "classified", 1),
        RunnerOutcome("a", 1, "classified", 1),
    )
    assert result.audits[0].withdrawn_indices == (2, 3)
    assert result.audits[0].missing_feature_indices == ()
    assert result.audits[1].issues == ()
    assert (
        result.references.declared_counts_sha256
        == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    assert (
        result.references.runner_outcomes_sha256
        == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    )


@pytest.mark.parametrize(
    "references",
    [
        EvidenceReferences("", "b" * 64),
        EvidenceReferences("a" * 63, "b" * 64),
        EvidenceReferences("a" * 64, "G" * 64),
        EvidenceReferences("a" * 64, "b" * 64 + "\n"),
    ],
)
def test_missing_or_malformed_source_references_fail(references: EvidenceReferences) -> None:
    with pytest.raises(ValueError, match="source references"):
        match_training_rows(
            feature_rows=(),
            evidence=TeacherScopeEvidence(references, ()),
            required_race_ids=frozenset(),
        )


@pytest.mark.parametrize(
    "key",
    [
        FeatureRunnerKey("", "a", 1),
        FeatureRunnerKey(" r", "a", 1),
        FeatureRunnerKey("r", "", 1),
        FeatureRunnerKey("r", "a ", 1),
        FeatureRunnerKey("r", "a", 0),
        FeatureRunnerKey("r", "a", -1),
        FeatureRunnerKey("r", "a", True),
    ],
)
def test_feature_keys_are_not_coerced_or_invented(key: FeatureRunnerKey) -> None:
    with pytest.raises(ValueError, match=r"Feature identities|Feature bib"):
        match_training_rows(
            feature_rows=(key,),
            evidence=TeacherScopeEvidence(EvidenceReferences("a" * 64, "b" * 64), ()),
            required_race_ids=frozenset({"r"}),
        )


def test_duplicate_feature_key_cannot_reuse_one_source_runner() -> None:
    with pytest.raises(ValueError, match="Duplicate feature runner"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r", "a", 1), FeatureRunnerKey("r", "a", 1)),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (TeacherRaceEvidence("r", 1, (RunnerOutcome("a", 1, "classified", 1),)),),
            ),
            required_race_ids=frozenset({"r"}),
        )


def test_noncanonical_evidence_race_is_rejected() -> None:
    with pytest.raises(ValueError, match="Evidence race identity"):
        match_training_rows(
            feature_rows=(),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (TeacherRaceEvidence(" r", 1, (RunnerOutcome("a", 1, "classified", 1),)),),
            ),
            required_race_ids=frozenset({"r"}),
        )


@pytest.mark.parametrize(
    "feature_rows",
    [
        (),
        (FeatureRunnerKey("r", "wrong-horse", 1),),
        (FeatureRunnerKey("r", "a", 2),),
        (FeatureRunnerKey("r", "a", None),),
    ],
)
def test_missing_or_equal_size_wrong_rosters_fail(
    feature_rows: tuple[FeatureRunnerKey, ...],
) -> None:
    with pytest.raises(ValueError, match="lack feature rows"):
        match_training_rows(
            feature_rows=feature_rows,
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (TeacherRaceEvidence("r", 1, (RunnerOutcome("a", 1, "classified", 1),)),),
            ),
            required_race_ids=frozenset({"r"}),
        )


def test_swapped_bibs_fail_even_when_horse_set_and_bib_set_match() -> None:
    with pytest.raises(ValueError, match="lack feature rows"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r", "a", 2), FeatureRunnerKey("r", "b", 1)),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (
                    TeacherRaceEvidence(
                        "r",
                        2,
                        (
                            RunnerOutcome("a", 1, "classified", 1),
                            RunnerOutcome("b", 2, "classified", 2),
                        ),
                    ),
                ),
            ),
            required_race_ids=frozenset({"r"}),
        )


def test_missing_whole_feature_race_never_fits_a_surviving_subset() -> None:
    with pytest.raises(ValueError, match="lack feature rows"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r1", "a", 1),),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (
                    TeacherRaceEvidence("r1", 1, (RunnerOutcome("a", 1, "classified", 1),)),
                    TeacherRaceEvidence("r2", 1, (RunnerOutcome("b", 1, "classified", 1),)),
                ),
            ),
            required_race_ids=frozenset({"r1", "r2"}),
        )


def test_extra_feature_row_is_not_silently_discarded() -> None:
    with pytest.raises(ValueError, match="Unexplained feature runner"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r", "a", 1), FeatureRunnerKey("r", "extra", 2)),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (TeacherRaceEvidence("r", 1, (RunnerOutcome("a", 1, "classified", 1),)),),
            ),
            required_race_ids=frozenset({"r"}),
        )


def test_unverified_count_is_not_replaced_by_matching_feature_count() -> None:
    with pytest.raises(ValueError, match="starter_count_unverified"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r", "a", 1),),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (TeacherRaceEvidence("r", None, (RunnerOutcome("a", 1, "classified", 1),)),),
            ),
            required_race_ids=frozenset({"r"}),
        )


def test_omitted_evidence_race_fails_exact_required_scope() -> None:
    with pytest.raises(ValueError, match="complete required teacher scope"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r1", "a", 1),),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (TeacherRaceEvidence("r1", 1, (RunnerOutcome("a", 1, "classified", 1),)),),
            ),
            required_race_ids=frozenset({"r1", "r2"}),
        )


def test_same_source_key_active_and_withdrawn_is_ambiguous() -> None:
    with pytest.raises(ValueError, match="Ambiguous source records"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r", "a", 1),),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (
                    TeacherRaceEvidence(
                        "r",
                        1,
                        (
                            RunnerOutcome("a", 1, "classified", 1),
                            RunnerOutcome("a", 1, "withdrawn", None),
                        ),
                    ),
                ),
            ),
            required_race_ids=frozenset({"r"}),
        )


def test_withdrawn_boolean_bib_cannot_alias_valid_integer_feature_key() -> None:
    with pytest.raises(ValueError, match="boolean alias"):
        match_training_rows(
            feature_rows=(FeatureRunnerKey("r", "a", 2), FeatureRunnerKey("r", "scratch", 1)),
            evidence=TeacherScopeEvidence(
                EvidenceReferences("a" * 64, "b" * 64),
                (
                    TeacherRaceEvidence(
                        "r",
                        1,
                        (
                            RunnerOutcome("a", 2, "classified", 1),
                            RunnerOutcome("scratch", True, "withdrawn", None),
                        ),
                    ),
                ),
            ),
            required_race_ids=frozenset({"r"}),
        )
