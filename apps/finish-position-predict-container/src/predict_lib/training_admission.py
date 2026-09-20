"""Fail closed on known physical-roster defects before invoking a new training job."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

ACTIVE_DISPOSITIONS: frozenset[str] = frozenset({"classified", "dnf", "dq", "unresolved_finish"})


@dataclass(frozen=True)
class RunnerOutcome:
    horse_id: str
    horse_number: int | None
    disposition: str
    finish: int | None


@dataclass(frozen=True)
class TeacherRunner(RunnerOutcome):
    feature_row_present: bool


@dataclass(frozen=True)
class RosterAdmission:
    race_id: str
    expected_starters: int | None
    active_indices: tuple[int, ...]
    withdrawn_indices: tuple[int, ...]
    unresolved_indices: tuple[int, ...]
    missing_feature_indices: tuple[int, ...]
    issues: tuple[str, ...]


def audit_teacher_roster(
    *, race_id: str, expected_starters: int | None, runners: tuple[TeacherRunner, ...]
) -> RosterAdmission:
    """Check independent starter count and physical slots; retain unresolved evidence."""
    if not race_id.strip():
        raise ValueError("Race identity must be explicit")
    if expected_starters is not None and (
        type(expected_starters) is not int or expected_starters < 1
    ):
        raise ValueError("Expected starters must be positive or explicitly unknown")
    active = tuple(i for i, row in enumerate(runners) if row.disposition in ACTIVE_DISPOSITIONS)
    withdrawn = tuple(i for i, row in enumerate(runners) if row.disposition == "withdrawn")
    unresolved = tuple(i for i, row in enumerate(runners) if row.disposition == "unknown_status")
    if len(active) + len(withdrawn) + len(unresolved) != len(runners):
        raise ValueError("Unknown disposition outside the evidence contract")
    rows = tuple(runners[i] for i in active)
    issues: set[str] = set()
    if expected_starters is None:
        issues.add("starter_count_unverified")
    elif len(rows) != expected_starters:
        issues.add("physical_starter_count_mismatch")
    if not rows:
        issues.add("no_verified_active_runners")
    if unresolved:
        issues.add("participation_unresolved")
    if any(runners[index].finish is not None for index in withdrawn):
        issues.add("withdrawn_has_numeric_rank")
    if any(
        not row.horse_id.strip()
        or set(row.horse_id.strip()) == {"0"}
        or row.horse_id != row.horse_id.strip()
        for row in rows
    ):
        issues.add("missing_or_placeholder_active_identity")
    if len({row.horse_id for row in rows}) != len(rows):
        issues.add("duplicate_active_identity")
    if any(type(row.horse_number) is not int or row.horse_number < 1 for row in rows):
        issues.add("missing_or_invalid_active_bib")
    if len({row.horse_number for row in rows}) != len(rows):
        issues.add("duplicate_active_bib")
    missing_features = tuple(i for i in active if runners[i].feature_row_present is not True)
    issues.update(_finish_issues(rows, expected_starters=expected_starters))
    return RosterAdmission(
        race_id,
        expected_starters,
        active,
        withdrawn,
        unresolved,
        missing_features,
        tuple(sorted(issues)),
    )


def _finish_issues(rows: tuple[TeacherRunner, ...], *, expected_starters: int | None) -> set[str]:
    issues: set[str] = set()
    finishes: list[int] = []
    for row in rows:
        if row.disposition == "unresolved_finish":
            issues.add("active_finish_unresolved")
        elif row.disposition == "classified":
            if type(row.finish) is not int or row.finish < 1:
                issues.add("invalid_classified_finish")
            else:
                finishes.append(row.finish)
        elif row.finish is not None:
            issues.add("nonfinish_has_numeric_rank")
    if len(rows) == expected_starters and not issues:
        expected = 1
        for rank, count in sorted(Counter(finishes).items()):
            if rank != expected:
                issues.add("invalid_competition_ranks")
            expected += count
    return issues


def train_after_admission[T](
    *,
    audits: tuple[RosterAdmission, ...],
    required_race_ids: frozenset[str],
    train: Callable[[], T],
) -> T:
    """Never treat an empty, duplicated or rejected audit set as permission to fit.

    The caller must supply every required teacher race; completeness of that scope,
    source hashes and PIT/identity provenance remain separate mandatory contracts.
    """
    if not audits or len({audit.race_id for audit in audits}) != len(audits):
        raise ValueError("Expected a nonempty unique teacher-race audit set")
    if frozenset(audit.race_id for audit in audits) != required_race_ids:
        raise ValueError("Audited races must equal the complete required teacher scope")
    blocked = {audit.race_id: audit.issues for audit in audits if audit.issues}
    if blocked:
        raise ValueError(f"Training blocked by roster defects: {blocked}")
    return train()
