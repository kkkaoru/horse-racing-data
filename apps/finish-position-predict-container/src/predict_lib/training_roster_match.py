"""Match supplied roster evidence to feature rows; never infer evidence from row counts."""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from functools import partial
from itertools import chain
from typing import Final

from predict_lib.training_admission import (
    RosterAdmission,
    RunnerOutcome,
    TeacherRunner,
    audit_teacher_roster,
    train_after_admission,
)

_SHA256: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class FeatureRunnerKey:
    race_id: str
    horse_id: str
    horse_number: int | None


@dataclass(frozen=True)
class EvidenceReferences:
    declared_counts_sha256: str
    runner_outcomes_sha256: str


@dataclass(frozen=True)
class TeacherRaceEvidence:
    race_id: str
    declared_starters: int | None
    outcomes: tuple[RunnerOutcome, ...]


@dataclass(frozen=True)
class TeacherScopeEvidence:
    references: EvidenceReferences
    races: tuple[TeacherRaceEvidence, ...]


@dataclass(frozen=True)
class MatchedTrainingRows:
    active_indices: tuple[int, ...]
    withdrawn_indices: tuple[int, ...]
    active_outcomes: tuple[RunnerOutcome, ...]
    audits: tuple[RosterAdmission, ...]
    references: EvidenceReferences


def match_training_rows(
    *,
    feature_rows: Sequence[FeatureRunnerKey],
    evidence: TeacherScopeEvidence,
    required_race_ids: frozenset[str],
) -> MatchedTrainingRows:
    """Preserve row order and explicit outcomes, or reject the entire supplied scope.

    The caller must independently verify source bytes and provenance. Reference
    syntax and agreement with supplied evidence do not establish authenticity/PIT.
    No missing feature rows are synthesized; missing covariate values are untouched.
    """
    _validate_references(evidence.references)
    rows = tuple(feature_rows)
    _validate_feature_keys(rows)
    keys = frozenset(rows)
    audits = tuple(_audit_race(race, keys) for race in evidence.races)
    return train_after_admission(
        audits=audits,
        required_race_ids=required_race_ids,
        train=partial(_match_rows, rows=rows, evidence=evidence, audits=audits),
    )


def _validate_references(references: EvidenceReferences) -> None:
    for digest in (references.declared_counts_sha256, references.runner_outcomes_sha256):
        if _SHA256.fullmatch(digest) is None:
            raise ValueError("Evidence source references must be canonical SHA256 digests")


def _canonical(value: str) -> bool:
    return bool(value) and value == value.strip()


def _validate_feature_keys(rows: tuple[FeatureRunnerKey, ...]) -> None:
    for row in rows:
        if not _canonical(row.race_id) or not _canonical(row.horse_id):
            raise ValueError("Feature identities must be explicit and canonical")
        if row.horse_number is not None and (
            type(row.horse_number) is not int or row.horse_number < 1
        ):
            raise ValueError("Feature bib must be a positive integer or explicitly missing")
    if len(set(rows)) != len(rows):
        raise ValueError("Duplicate feature runner key")


def _audit_race(race: TeacherRaceEvidence, keys: frozenset[FeatureRunnerKey]) -> RosterAdmission:
    if not _canonical(race.race_id):
        raise ValueError("Evidence race identity must be explicit and canonical")
    runners = tuple(
        TeacherRunner(
            outcome.horse_id,
            outcome.horse_number,
            outcome.disposition,
            outcome.finish,
            FeatureRunnerKey(race.race_id, outcome.horse_id, outcome.horse_number) in keys,
        )
        for outcome in race.outcomes
    )
    return audit_teacher_roster(
        race_id=race.race_id, expected_starters=race.declared_starters, runners=runners
    )


def _source_entries(
    race: TeacherRaceEvidence, keys: frozenset[FeatureRunnerKey]
) -> Iterator[tuple[FeatureRunnerKey, RunnerOutcome]]:
    for outcome in race.outcomes:
        key = FeatureRunnerKey(race.race_id, outcome.horse_id, outcome.horse_number)
        # Withdrawn records without a feature row remain in the source audit.
        if outcome.disposition != "withdrawn" or key in keys:
            if outcome.horse_number is not None and type(outcome.horse_number) is not int:
                raise ValueError("Matched evidence bib must be an integer, not a boolean alias")
            yield key, outcome


def _source_index(
    races: tuple[TeacherRaceEvidence, ...], keys: frozenset[FeatureRunnerKey]
) -> dict[FeatureRunnerKey, RunnerOutcome]:
    indexed: dict[FeatureRunnerKey, RunnerOutcome] = {}
    entries = chain.from_iterable(_source_entries(race, keys) for race in races)
    for key, outcome in entries:
        if key in indexed:
            raise ValueError("Ambiguous source records for a feature runner")
        indexed[key] = outcome
    return indexed


def _match_rows(
    *,
    rows: tuple[FeatureRunnerKey, ...],
    evidence: TeacherScopeEvidence,
    audits: tuple[RosterAdmission, ...],
) -> MatchedTrainingRows:
    if any(audit.missing_feature_indices for audit in audits):
        raise ValueError("Active evidence runners lack feature rows; no replacement is permitted")
    indexed = _source_index(evidence.races, frozenset(rows))
    active: list[int] = []
    withdrawn: list[int] = []
    outcomes: list[RunnerOutcome] = []
    for index, key in enumerate(rows):
        outcome = indexed.get(key)
        if outcome is None:
            raise ValueError("Unexplained feature runner outside the supplied roster evidence")
        if outcome.disposition == "withdrawn":
            withdrawn.append(index)
            continue
        active.append(index)
        outcomes.append(outcome)
    return MatchedTrainingRows(
        tuple(active), tuple(withdrawn), tuple(outcomes), audits, evidence.references
    )
