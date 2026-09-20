"""Conservative domestic JVD result/count projection, retaining original raw rows."""

import re
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

from predict_lib.training_admission import RunnerOutcome


@dataclass(frozen=True)
class RawJvdRunner:
    race_id: str | None
    horse_id: str | None
    horse_number: str | None
    source_status: str | None
    abnormality_code: str | None
    finish_text: str | None


@dataclass(frozen=True)
class NormalizedJvdRunner:
    raw: RawJvdRunner
    outcome: RunnerOutcome | None
    issue: str | None


@dataclass(frozen=True)
class RawJvdDeclaration:
    race_id: str | None
    declared_starters: str | None
    source_status: str | None


@dataclass(frozen=True)
class NormalizedJvdDeclaration:
    raw: RawJvdDeclaration
    declared_starters: int | None
    issue: str | None


_DISPOSITIONS = MappingProxyType(
    {
        "0": "classified",
        "1": "withdrawn",
        "2": "withdrawn",
        "3": "withdrawn",
        "4": "dnf",
        "5": "dq",
        "6": "classified",
        "7": "classified",
    }
)
_MISSING_FINISH: frozenset[str | None] = frozenset({None, "", "0", "00"})


def _domestic_race(value: str | None) -> bool:
    if value is None:
        return False
    match = re.fullmatch(
        r"jra:([0-9]{4}):([0-9]{2})([0-9]{2}):(?:0[1-9]|10):(?:0[1-9]|1[0-2])", value
    )
    if match is None:
        return False
    try:
        date(int(match[1]), int(match[2]), int(match[3]))
    except ValueError:
        return False
    return True


def _number(text: str | None) -> int | None:
    if text is None or re.fullmatch(r"[0-9]{1,2}", text) is None:
        return None
    return int(text)


def normalize_domestic_jvd_declaration(row: RawJvdDeclaration) -> NormalizedJvdDeclaration:
    """Project independently supplied RA starters, never infer them from runner rows."""
    if not _domestic_race(row.race_id):
        return NormalizedJvdDeclaration(row, None, "unsupported_domestic_race_identity")
    if row.source_status != "7":
        return NormalizedJvdDeclaration(row, None, "unsupported_publication_status")
    starters = _number(row.declared_starters)
    if starters is None or starters == 0:
        return NormalizedJvdDeclaration(row, None, "invalid_declared_starters")
    return NormalizedJvdDeclaration(row, starters, None)


def normalize_domestic_jvd_runner(row: RawJvdRunner) -> NormalizedJvdRunner:
    """Quarantine unsupported/contradictory rows; this does not authorize a scope or fit."""
    if not _domestic_race(row.race_id):
        return NormalizedJvdRunner(row, None, "unsupported_domestic_race_identity")
    if row.source_status != "7":
        return NormalizedJvdRunner(row, None, "unsupported_publication_status")
    if (
        row.horse_id is None
        or re.fullmatch(r"[0-9]{10}", row.horse_id) is None
        or row.horse_id == "0000000000"
    ):
        return NormalizedJvdRunner(row, None, "invalid_registration_identity")
    bib = _number(row.horse_number)
    if bib is None or bib == 0:
        return NormalizedJvdRunner(row, None, "invalid_bib")
    disposition = (
        _DISPOSITIONS.get(row.abnormality_code) if row.abnormality_code is not None else None
    )
    if disposition is None:
        return NormalizedJvdRunner(row, None, "unknown_abnormality")
    finish = _number(row.finish_text)
    if finish is None and row.finish_text not in _MISSING_FINISH:
        return NormalizedJvdRunner(row, None, "malformed_finish")
    if finish == 0:
        finish = None
    if disposition == "classified" and finish is None:
        return NormalizedJvdRunner(row, None, "unresolved_classified_finish")
    if disposition != "classified" and finish is not None:
        return NormalizedJvdRunner(row, None, "nonclassified_numeric_finish")
    return NormalizedJvdRunner(row, RunnerOutcome(row.horse_id, bib, disposition, finish), None)
