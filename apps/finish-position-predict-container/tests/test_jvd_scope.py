"""Whole-scope failure must not expose evidence for otherwise valid races."""

from dataclasses import asdict, replace

import pytest

from predict_lib import jvd_scope
from predict_lib.jvd_outcomes import NormalizedJvdRunner, RawJvdDeclaration, RawJvdRunner
from predict_lib.jvd_scope import project_domestic_jvd_scope


@pytest.fixture
def runners() -> tuple[RawJvdRunner, ...]:
    return (
        RawJvdRunner("jra:2020:0229:06:01", "2010100001", "1", "7", "0", "01"),
        RawJvdRunner("jra:2020:0229:06:01", "2010100002", "2", "7", "0", "01"),
        RawJvdRunner("jra:2020:0229:06:01", "2010100003", "3", "7", "4", "00"),
        RawJvdRunner("jra:2020:0229:06:01", "2010100004", "4", "7", "1", "00"),
        RawJvdRunner("jra:2020:0229:06:01", "2010100005", "5", "7", "5", "00"),
        RawJvdRunner("jra:2020:0301:06:01", "2010100001", "1", "7", "0", "01"),
        RawJvdRunner("jra:2020:0301:06:01", "2010100002", "2", "7", "0", "02"),
    )


@pytest.fixture
def declarations() -> tuple[RawJvdDeclaration, ...]:
    return (
        RawJvdDeclaration("jra:2020:0301:06:01", "02", "7"),
        RawJvdDeclaration("jra:2020:0229:06:01", "04", "7"),
    )


def test_complete_scope_retains_ties_nonfinish_and_nonstarters(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=runners,
        declaration_rows=declarations,
    )
    assert result.issues == frozenset()
    assert result.race_evidence is not None
    assert len(result.race_evidence) == 2
    assert asdict(result.race_evidence[0]) == {
        "race_id": "jra:2020:0229:06:01",
        "declared_starters": 4,
        "outcomes": (
            {"horse_id": "2010100001", "horse_number": 1, "disposition": "classified", "finish": 1},
            {"horse_id": "2010100002", "horse_number": 2, "disposition": "classified", "finish": 1},
            {"horse_id": "2010100003", "horse_number": 3, "disposition": "dnf", "finish": None},
            {
                "horse_id": "2010100004",
                "horse_number": 4,
                "disposition": "withdrawn",
                "finish": None,
            },
            {"horse_id": "2010100005", "horse_number": 5, "disposition": "dq", "finish": None},
        ),
    }
    assert result.audits[0].missing_feature_indices == (0, 1, 2, 4)
    assert result.audits[0].withdrawn_indices == (3,)
    assert result.declarations[0].raw.race_id == "jra:2020:0301:06:01"
    assert result.runners[2].raw.finish_text == "00"


def test_empty_required_scope_does_not_discard_supplied_records(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset(), runner_rows=runners, declaration_rows=declarations
    )
    assert result.race_evidence is None
    assert result.issues == {
        "empty_required_scope",
        "unexpected_runner_races",
        "unexpected_declaration_races",
    }
    assert len(result.runners) == 7
    assert len(result.declarations) == 2


def test_missing_both_sources_preserves_requested_scope() -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01"}), runner_rows=(), declaration_rows=()
    )
    assert result.required_race_ids == {"jra:2020:0229:06:01"}
    assert result.issues == {"missing_runner_races", "missing_declaration_races"}
    assert result.race_evidence is None


def test_missing_whole_runner_race_blocks_valid_other_race(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=runners[:5],
        declaration_rows=declarations,
    )
    assert result.issues == {"missing_runner_races"}
    assert result.race_evidence is None


def test_duplicate_declaration_is_not_last_write_wins(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=runners,
        declaration_rows=(*declarations, declarations[0]),
    )
    assert result.issues == {"duplicate_declaration_race"}
    assert result.race_evidence is None
    assert len(result.declarations) == 3


def test_one_quarantined_runner_blocks_entire_output(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=(replace(runners[0], source_status="2"), *runners[1:]),
        declaration_rows=declarations,
    )
    assert result.issues == {"runner_projection_blocked"}
    assert result.race_evidence is None
    assert len(result.runners) == 7


def test_unknown_independent_count_is_not_inferred_from_runner_rows(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=runners,
        declaration_rows=(replace(declarations[0], declared_starters=None), declarations[1]),
    )
    assert result.issues == {"declaration_projection_blocked"}
    assert result.race_evidence is None


def test_duplicate_withdrawn_record_also_blocks_scope(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=(*runners, runners[3]),
        declaration_rows=declarations,
    )
    assert result.issues == {"duplicate_source_runner_key"}
    assert result.race_evidence is None
    assert len(result.runners) == 8


def test_canonical_bib_alias_does_not_hide_duplicate_source_runner(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=(*runners, replace(runners[0], horse_number="01")),
        declaration_rows=declarations,
    )
    assert result.issues == {"duplicate_source_runner_key", "physical_roster_defects"}
    assert result.race_evidence is None


def test_missing_active_row_blocks_scope_with_original_counts(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=(runners[0], runners[1], *runners[3:]),
        declaration_rows=declarations,
    )
    assert result.issues == {"physical_roster_defects"}
    assert result.race_evidence is None
    assert result.audits[0].issues == ("physical_starter_count_mismatch",)
    assert result.audits[1].issues == ()


def test_bad_competition_rank_blocks_otherwise_complete_scope(
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    result = project_domestic_jvd_scope(
        required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
        runner_rows=(*runners[:6], replace(runners[6], finish_text="03")),
        declaration_rows=declarations,
    )
    assert result.issues == {"physical_roster_defects"}
    assert result.race_evidence is None
    assert result.audits[1].issues == ("invalid_competition_ranks",)


def test_inconsistent_projector_contract_stops_before_evidence(
    monkeypatch: pytest.MonkeyPatch,
    runners: tuple[RawJvdRunner, ...],
    declarations: tuple[RawJvdDeclaration, ...],
) -> None:
    def inconsistent(row: RawJvdRunner) -> NormalizedJvdRunner:
        return NormalizedJvdRunner(row, None, None)

    monkeypatch.setattr(jvd_scope, "normalize_domestic_jvd_runner", inconsistent)
    with pytest.raises(ValueError, match="Successful runner projection lacks required fields"):
        project_domestic_jvd_scope(
            required_race_ids=frozenset({"jra:2020:0229:06:01", "jra:2020:0301:06:01"}),
            runner_rows=runners,
            declaration_rows=declarations,
        )
