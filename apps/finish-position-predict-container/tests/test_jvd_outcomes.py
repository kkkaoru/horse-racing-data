"""Raw projection does not infer a finish or discard an unsupported source row."""

from dataclasses import asdict, replace

import pytest

from predict_lib.jvd_outcomes import (
    RawJvdDeclaration,
    RawJvdRunner,
    normalize_domestic_jvd_declaration,
    normalize_domestic_jvd_runner,
)
from predict_lib.training_admission import RunnerOutcome


@pytest.fixture
def raw() -> RawJvdRunner:
    return RawJvdRunner("jra:2020:0229:06:01", "2010100001", "19", "7", "0", "03")


@pytest.mark.parametrize("code", ["0", "6", "7"])
def test_classified_final_rank_is_unchanged_including_remount_and_demotion(
    raw: RawJvdRunner, code: str
) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, abnormality_code=code))
    assert result.issue is None
    assert result.outcome == RunnerOutcome("2010100001", 19, "classified", 3)
    assert result.raw.finish_text == "03"


def test_complete_raw_record_is_preserved(raw: RawJvdRunner) -> None:
    result = normalize_domestic_jvd_runner(raw)
    assert asdict(result.raw) == {
        "race_id": "jra:2020:0229:06:01",
        "horse_id": "2010100001",
        "horse_number": "19",
        "source_status": "7",
        "abnormality_code": "0",
        "finish_text": "03",
    }


@pytest.mark.parametrize("code", ["1", "2", "3"])
def test_explicit_nonstarters_are_retained(raw: RawJvdRunner, code: str) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, abnormality_code=code, finish_text="00"))
    assert result.issue is None
    assert result.outcome == RunnerOutcome("2010100001", 19, "withdrawn", None)
    assert result.raw.finish_text == "00"


@pytest.mark.parametrize("finish", [None, "", "0", "00"])
def test_explicit_dnf_does_not_invent_a_numeric_rank(raw: RawJvdRunner, finish: str | None) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, abnormality_code="4", finish_text=finish))
    assert result.issue is None
    assert result.outcome == RunnerOutcome("2010100001", 19, "dnf", None)


def test_explicit_dq_and_padded_bib(raw: RawJvdRunner) -> None:
    result = normalize_domestic_jvd_runner(
        replace(raw, horse_number="01", abnormality_code="5", finish_text="00")
    )
    assert result.issue is None
    assert result.outcome == RunnerOutcome("2010100001", 1, "dq", None)
    assert result.raw.horse_number == "01"


@pytest.mark.parametrize(
    "status", [None, "", "0", "1", "2", "3", "4", "5", "6", "9", "A", "B", "X"]
)
def test_unsupported_publication_is_quarantined_not_normalized(
    raw: RawJvdRunner, status: str | None
) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, source_status=status))
    assert result.outcome is None
    assert result.issue == "unsupported_publication_status"
    assert result.raw.finish_text == "03"


@pytest.mark.parametrize(
    "race_id",
    [
        None,
        "",
        "jra:2020:0229:F1:01",
        "jra:2020:0229:51:01",
        "nra:2020:0229:06:01",
        "jra:2021:0229:06:01",
        "jra:0000:0101:06:01",
        "jra:2020:0229:06:13",
    ],
)
def test_invalid_or_nondomestic_race_is_quarantined(raw: RawJvdRunner, race_id: str | None) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, race_id=race_id))
    assert result.outcome is None
    assert result.issue == "unsupported_domestic_race_identity"


@pytest.mark.parametrize(
    "horse_id",
    [
        None,
        "",
        "0000000000",
        "H1",
        "2010100001 ",
        "\uff12\uff10\uff11\uff10\uff11\uff10\uff10\uff10\uff10\uff11",
    ],
)
def test_missing_placeholder_or_malformed_registration_is_quarantined(
    raw: RawJvdRunner, horse_id: str | None
) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, horse_id=horse_id))
    assert result.outcome is None
    assert result.issue == "invalid_registration_identity"


@pytest.mark.parametrize(
    "bib", [None, "", "0", "00", "-1", "True", "1.0", "\uff11\uff10\uff10", "100"]
)
def test_unknown_or_malformed_bib_is_quarantined(raw: RawJvdRunner, bib: str | None) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, horse_number=bib))
    assert result.outcome is None
    assert result.issue == "invalid_bib"


@pytest.mark.parametrize("code", [None, "", "8", "A", "00"])
def test_unknown_abnormality_is_not_inferred_from_positive_rank(
    raw: RawJvdRunner, code: str | None
) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, abnormality_code=code))
    assert result.outcome is None
    assert result.issue == "unknown_abnormality"


@pytest.mark.parametrize("finish", ["abc", "-1", "1.5", "1e0", "NaN", "inf", "\uff13", "100"])
def test_malformed_finish_is_not_silently_missing(raw: RawJvdRunner, finish: str) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, abnormality_code="4", finish_text=finish))
    assert result.outcome is None
    assert result.issue == "malformed_finish"


@pytest.mark.parametrize("code", ["0", "6", "7"])
@pytest.mark.parametrize("finish", [None, "", "0", "00"])
def test_missing_classified_finish_is_not_dnf(
    raw: RawJvdRunner, code: str, finish: str | None
) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, abnormality_code=code, finish_text=finish))
    assert result.outcome is None
    assert result.issue == "unresolved_classified_finish"


@pytest.mark.parametrize("code", ["1", "2", "3", "4", "5"])
def test_nonclassified_positive_rank_is_quarantined_without_correction(
    raw: RawJvdRunner, code: str
) -> None:
    result = normalize_domestic_jvd_runner(replace(raw, abnormality_code=code, finish_text="02"))
    assert result.outcome is None
    assert result.issue == "nonclassified_numeric_finish"
    assert result.raw.finish_text == "02"


def test_independent_ra_count_projection_preserves_its_raw_source() -> None:
    result = normalize_domestic_jvd_declaration(RawJvdDeclaration("jra:2020:0229:06:01", "09", "7"))
    assert result.declared_starters == 9
    assert result.issue is None
    assert asdict(result.raw) == {
        "race_id": "jra:2020:0229:06:01",
        "declared_starters": "09",
        "source_status": "7",
    }


@pytest.mark.parametrize("count", [None, "", "0", "00", "-1", "1.0", "True", "100", "\uff11"])
def test_independent_ra_count_does_not_fall_back_when_unknown(count: str | None) -> None:
    result = normalize_domestic_jvd_declaration(
        RawJvdDeclaration("jra:2020:0229:06:01", count, "7")
    )
    assert result.declared_starters is None
    assert result.issue == "invalid_declared_starters"


@pytest.mark.parametrize("status", [None, "0", "2", "5", "6", "9", "A", "B", "X"])
def test_nonfinal_or_unsupported_ra_is_not_admitted_by_numeric_count(status: str | None) -> None:
    result = normalize_domestic_jvd_declaration(
        RawJvdDeclaration("jra:2020:0229:06:01", "09", status)
    )
    assert result.declared_starters is None
    assert result.issue == "unsupported_publication_status"


@pytest.mark.parametrize("race_id", [None, "", "jra:2020:0229:F1:01", "jra:2021:0229:06:01"])
def test_ra_requires_supported_domestic_race_identity(race_id: str | None) -> None:
    result = normalize_domestic_jvd_declaration(RawJvdDeclaration(race_id, "09", "7"))
    assert result.declared_starters is None
    assert result.issue == "unsupported_domestic_race_identity"
