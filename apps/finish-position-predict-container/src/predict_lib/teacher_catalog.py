"""Read byte-checked normalized evidence, without claiming source authenticity."""

import argparse
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from predict_lib.training_admission import RunnerOutcome
from predict_lib.training_roster_match import (
    EvidenceReferences,
    TeacherRaceEvidence,
    TeacherScopeEvidence,
)


@dataclass(frozen=True)
class TeacherCatalog:
    references: EvidenceReferences
    races: Mapping[str, TeacherRaceEvidence]

    def for_scope(self, required_race_ids: frozenset[str]) -> TeacherScopeEvidence:
        if not required_race_ids or not required_race_ids.issubset(self.races):
            raise ValueError("Teacher evidence must contain the entire nonempty requested scope")
        return TeacherScopeEvidence(
            self.references, tuple(self.races[race_id] for race_id in sorted(required_race_ids))
        )


def require_teacher_catalog(args: argparse.Namespace) -> TeacherCatalog:
    catalog: object = getattr(args, "teacher_catalog", None)
    if not isinstance(catalog, TeacherCatalog):
        raise ValueError("An explicit teacher catalog argument is required")
    return catalog


def add_teacher_evidence_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--teacher-declarations", type=Path, required=True)
    parser.add_argument("--teacher-outcomes", type=Path, required=True)
    parser.add_argument("--teacher-declarations-sha256", required=True)
    parser.add_argument("--teacher-outcomes-sha256", required=True)


def teacher_catalog_from_arguments(args: argparse.Namespace) -> TeacherCatalog:
    declarations: object = getattr(args, "teacher_declarations", None)
    outcomes: object = getattr(args, "teacher_outcomes", None)
    declarations_sha: object = getattr(args, "teacher_declarations_sha256", None)
    outcomes_sha: object = getattr(args, "teacher_outcomes_sha256", None)
    if not isinstance(declarations, Path) or not isinstance(outcomes, Path):
        raise ValueError("Required teacher evidence paths are missing or invalid")
    if not isinstance(declarations_sha, str) or not isinstance(outcomes_sha, str):
        raise ValueError("Required teacher evidence digests are missing or invalid")
    return load_teacher_catalog(
        declarations_path=declarations,
        outcomes_path=outcomes,
        expected=EvidenceReferences(declarations_sha, outcomes_sha),
    )


def load_teacher_catalog(
    *, declarations_path: Path, outcomes_path: Path, expected: EvidenceReferences
) -> TeacherCatalog:
    """Verify caller-provided hashes before interpreting the same immutable bytes.

    Normalization, independent raw-source provenance and PIT require separate
    evidence. This function neither invents declarations nor discards outcomes.
    """
    declarations = read_teacher_evidence_bytes(declarations_path, expected.declared_counts_sha256)
    outcomes = read_teacher_evidence_bytes(outcomes_path, expected.runner_outcomes_sha256)
    counts = _declarations(_rows(declarations, "independent-declarations-v1"))
    runners = _outcomes(_rows(outcomes, "runner-outcomes-v1"))
    if counts.keys() != runners.keys():
        raise ValueError("Declaration and outcome catalog race populations differ")
    races = {
        race_id: TeacherRaceEvidence(race_id, count, runners[race_id])
        for race_id, count in counts.items()
    }
    return TeacherCatalog(expected, MappingProxyType(races))


def read_teacher_evidence_bytes(path: Path, expected_sha256: str) -> bytes:
    """Read once and verify caller-supplied byte identity, not source authenticity."""
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected_sha256:
        raise ValueError(f"Teacher evidence SHA256 mismatch: {path}")
    return content


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON evidence key")
        result[key] = value
    return result


def _record(value: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or value.keys() != fields:
        raise ValueError("Unexpected teacher evidence record fields")
    return value


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("Teacher evidence records must be arrays")
    return value


def _rows(content: bytes, version: str) -> list[object]:
    decoded: object = json.loads(content, object_pairs_hook=_unique_object)
    root = _record(decoded, frozenset({"version", "races"}))
    if root["version"] != version:
        raise ValueError("Unsupported teacher evidence version")
    return _array(root["races"])


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Teacher evidence text must not be coerced")
    return value


def _race_id(value: object) -> str:
    result = _text(value)
    if not result or result != result.strip():
        raise ValueError("Teacher evidence race identity must be canonical")
    return result


def _optional_positive_integer(value: object) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 1:
        raise ValueError("Teacher evidence numbers must be positive integers or explicit null")
    return value


def _declarations(rows: list[object]) -> dict[str, int | None]:
    result: dict[str, int | None] = {}
    for value in rows:
        row = _record(value, frozenset({"race_id", "declared_starters"}))
        race_id = _race_id(row["race_id"])
        if race_id in result:
            raise ValueError("Duplicate declared race")
        result[race_id] = _optional_positive_integer(row["declared_starters"])
    return result


def _runner(value: object) -> RunnerOutcome:
    row = _record(value, frozenset({"horse_id", "horse_number", "disposition", "finish"}))
    return RunnerOutcome(
        _text(row["horse_id"]),
        _optional_positive_integer(row["horse_number"]),
        _text(row["disposition"]),
        _optional_positive_integer(row["finish"]),
    )


def _outcomes(rows: list[object]) -> dict[str, tuple[RunnerOutcome, ...]]:
    result: dict[str, tuple[RunnerOutcome, ...]] = {}
    for value in rows:
        row = _record(value, frozenset({"race_id", "outcomes"}))
        race_id = _race_id(row["race_id"])
        if race_id in result:
            raise ValueError("Duplicate outcome race")
        result[race_id] = tuple(_runner(runner) for runner in _array(row["outcomes"]))
    return result
