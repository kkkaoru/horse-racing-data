"""Bind an entire requested domestic raw scope without exposing a surviving subset."""

from collections import Counter, defaultdict
from dataclasses import dataclass

from predict_lib.jvd_outcomes import (
    NormalizedJvdDeclaration,
    NormalizedJvdRunner,
    RawJvdDeclaration,
    RawJvdRunner,
    normalize_domestic_jvd_declaration,
    normalize_domestic_jvd_runner,
)
from predict_lib.training_admission import (
    RosterAdmission,
    RunnerOutcome,
    TeacherRunner,
    audit_teacher_roster,
)
from predict_lib.training_roster_match import TeacherRaceEvidence


@dataclass(frozen=True)
class JvdScopeProjection:
    required_race_ids: frozenset[str]
    runners: tuple[NormalizedJvdRunner, ...]
    declarations: tuple[NormalizedJvdDeclaration, ...]
    audits: tuple[RosterAdmission, ...]
    issues: frozenset[str]
    race_evidence: tuple[TeacherRaceEvidence, ...] | None


def _audit(race: TeacherRaceEvidence) -> RosterAdmission:
    return audit_teacher_roster(
        race_id=race.race_id,
        expected_starters=race.declared_starters,
        runners=tuple(
            TeacherRunner(row.horse_id, row.horse_number, row.disposition, row.finish, False)
            for row in race.outcomes
        ),
    )


def project_domestic_jvd_scope(
    *,
    required_race_ids: frozenset[str],
    runner_rows: tuple[RawJvdRunner, ...],
    declaration_rows: tuple[RawJvdDeclaration, ...],
) -> JvdScopeProjection:
    """No features, hashes or PIT are verified here; any defect blocks all race evidence."""
    runners = tuple(normalize_domestic_jvd_runner(row) for row in runner_rows)
    declarations = tuple(normalize_domestic_jvd_declaration(row) for row in declaration_rows)
    issues: set[str] = set()
    if not required_race_ids:
        issues.add("empty_required_scope")
    runner_races = {row.raw.race_id for row in runners}
    declared_races = {row.raw.race_id for row in declarations}
    if required_race_ids - runner_races:
        issues.add("missing_runner_races")
    if runner_races - required_race_ids:
        issues.add("unexpected_runner_races")
    if required_race_ids - declared_races:
        issues.add("missing_declaration_races")
    if declared_races - required_race_ids:
        issues.add("unexpected_declaration_races")
    if len(declared_races) != len(declarations):
        issues.add("duplicate_declaration_race")
    if any(row.issue is not None for row in runners):
        issues.add("runner_projection_blocked")
    if any(row.issue is not None for row in declarations):
        issues.add("declaration_projection_blocked")
    if issues:
        return JvdScopeProjection(
            required_race_ids, runners, declarations, (), frozenset(issues), None
        )

    grouped: dict[str | None, list[RunnerOutcome]] = defaultdict(list)
    keys: Counter[tuple[str | None, str, int | None]] = Counter()
    for row in runners:
        if row.outcome is None:
            raise ValueError("Successful runner projection lacks required fields")
        outcome = row.outcome
        grouped[row.raw.race_id].append(outcome)
        keys[(row.raw.race_id, outcome.horse_id, outcome.horse_number)] += 1
    counts = {row.raw.race_id: row.declared_starters for row in declarations}
    races = tuple(
        TeacherRaceEvidence(race_id, counts[race_id], tuple(grouped[race_id]))
        for race_id in sorted(required_race_ids)
    )
    if any(count > 1 for count in keys.values()):
        issues.add("duplicate_source_runner_key")
    audits = tuple(_audit(race) for race in races)
    if any(audit.issues for audit in audits):
        issues.add("physical_roster_defects")
    return JvdScopeProjection(
        required_race_ids,
        runners,
        declarations,
        audits,
        frozenset(issues),
        None if issues else races,
    )
