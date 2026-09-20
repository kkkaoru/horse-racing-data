"""Evidence is decoded from the same bytes whose expected hashes were checked."""

import hashlib
from argparse import Namespace
from pathlib import Path

import pytest

from predict_lib.teacher_catalog import (
    load_teacher_catalog,
    read_teacher_evidence_bytes,
    teacher_catalog_from_arguments,
)
from predict_lib.training_roster_match import EvidenceReferences


def test_shared_byte_checker_accepts_only_the_expected_bytes(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"abc")
    assert (
        read_teacher_evidence_bytes(
            source, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        )
        == b"abc"
    )
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        read_teacher_evidence_bytes(source, "0" * 64)


@pytest.fixture
def inputs(tmp_path: Path) -> tuple[Path, Path, EvidenceReferences]:
    declarations = tmp_path / "declarations.json"
    outcomes = tmp_path / "outcomes.json"
    declarations.write_text(
        '{"version":"independent-declarations-v1","races":[{"race_id":"r","declared_starters":2}]}',
        encoding="utf-8",
    )
    outcomes.write_text(
        '{"version":"runner-outcomes-v1","races":[{"race_id":"r","outcomes":['
        '{"horse_id":"a","horse_number":1,"disposition":"classified","finish":1},'
        '{"horse_id":"b","horse_number":19,"disposition":"dnf","finish":null},'
        '{"horse_id":"c","horse_number":null,"disposition":"withdrawn","finish":null},'
        '{"horse_id":"u","horse_number":4,"disposition":"unresolved","finish":null}]}]}',
        encoding="utf-8",
    )
    return (
        declarations,
        outcomes,
        EvidenceReferences(
            hashlib.sha256(declarations.read_bytes()).hexdigest(),
            hashlib.sha256(outcomes.read_bytes()).hexdigest(),
        ),
    )


@pytest.mark.parametrize(
    "field",
    [
        "teacher_declarations",
        "teacher_outcomes",
        "teacher_declarations_sha256",
        "teacher_outcomes_sha256",
    ],
)
def test_argument_boundary_requires_paths_and_digests(
    inputs: tuple[Path, Path, EvidenceReferences],
    field: str,
) -> None:
    declarations, outcomes, references = inputs
    args = Namespace(
        teacher_declarations=declarations,
        teacher_outcomes=outcomes,
        teacher_declarations_sha256=references.declared_counts_sha256,
        teacher_outcomes_sha256=references.runner_outcomes_sha256,
    )
    delattr(args, field)
    with pytest.raises(ValueError, match="Required teacher evidence"):
        teacher_catalog_from_arguments(args)


def test_catalog_preserves_nonstarters_unknowns_and_nonfinishes(
    inputs: tuple[Path, Path, EvidenceReferences],
) -> None:
    declarations, outcomes, references = inputs
    catalog = load_teacher_catalog(
        declarations_path=declarations,
        outcomes_path=outcomes,
        expected=references,
    )
    scope = catalog.for_scope(frozenset({"r"}))
    assert len(scope.races) == 1
    assert scope.races[0].declared_starters == 2
    assert len(scope.races[0].outcomes) == 4
    assert scope.races[0].outcomes[1].finish is None
    assert scope.races[0].outcomes[2].disposition == "withdrawn"
    assert scope.races[0].outcomes[3].disposition == "unresolved"


@pytest.mark.parametrize("required", [frozenset(), frozenset({"r", "absent"})])
def test_scope_cannot_be_empty_or_replaced_by_surviving_subset(
    inputs: tuple[Path, Path, EvidenceReferences],
    required: frozenset[str],
) -> None:
    declarations, outcomes, references = inputs
    catalog = load_teacher_catalog(
        declarations_path=declarations,
        outcomes_path=outcomes,
        expected=references,
    )
    with pytest.raises(ValueError, match="entire nonempty requested scope"):
        catalog.for_scope(required)


@pytest.mark.parametrize("target", ["declarations", "outcomes"])
def test_modified_source_bytes_fail_before_parsing(
    inputs: tuple[Path, Path, EvidenceReferences],
    target: str,
) -> None:
    declarations, outcomes, references = inputs
    changed = declarations if target == "declarations" else outcomes
    changed.write_bytes(b"not JSON")
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        load_teacher_catalog(
            declarations_path=declarations, outcomes_path=outcomes, expected=references
        )


def test_missing_source_is_not_regenerated(inputs: tuple[Path, Path, EvidenceReferences]) -> None:
    declarations, outcomes, references = inputs
    declarations.unlink()
    with pytest.raises(FileNotFoundError):
        load_teacher_catalog(
            declarations_path=declarations, outcomes_path=outcomes, expected=references
        )


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("[]", "record fields"),
        ("{}", "record fields"),
        ('{"version":"old","races":[]}', "Unsupported"),
        ('{"version":"independent-declarations-v1","races":{}}', "arrays"),
        ('{"version":"independent-declarations-v1","races":[{}]}', "record fields"),
        (
            '{"version":"independent-declarations-v1","races":[{"race_id":null,"declared_starters":2}]}',
            "text",
        ),
        (
            '{"version":"independent-declarations-v1","races":[{"race_id":"","declared_starters":2}]}',
            "canonical",
        ),
        (
            '{"version":"independent-declarations-v1",'
            '"races":[{"race_id":" r","declared_starters":2}]}',
            "canonical",
        ),
        (
            '{"version":"independent-declarations-v1","races":[{"race_id":"r","declared_starters":true}]}',
            "positive integers",
        ),
        (
            '{"version":"independent-declarations-v1","races":[{"race_id":"r","declared_starters":0}]}',
            "positive integers",
        ),
        (
            '{"version":"independent-declarations-v1","races":[{"race_id":"r","declared_starters":2.0}]}',
            "positive integers",
        ),
        (
            '{"version":"independent-declarations-v1","races":[{"race_id":"r","declared_starters":2},{"race_id":"r","declared_starters":2}]}',
            "Duplicate declared",
        ),
        ('{"version":"independent-declarations-v1","races":[],"races":[]}', "Duplicate JSON"),
        ('{"version":"independent-declarations-v1","races":[]}', "populations differ"),
    ],
)
def test_malformed_declarations_are_not_repaired(
    inputs: tuple[Path, Path, EvidenceReferences],
    text: str,
    message: str,
) -> None:
    declarations, outcomes, references = inputs
    declarations.write_text(text, encoding="utf-8")
    expected = EvidenceReferences(
        hashlib.sha256(declarations.read_bytes()).hexdigest(), references.runner_outcomes_sha256
    )
    with pytest.raises(ValueError, match=message):
        load_teacher_catalog(
            declarations_path=declarations, outcomes_path=outcomes, expected=expected
        )


def test_unknown_declaration_remains_unknown(inputs: tuple[Path, Path, EvidenceReferences]) -> None:
    declarations, outcomes, references = inputs
    declarations.write_text(
        '{"version":"independent-declarations-v1","races":[{"race_id":"r","declared_starters":null}]}',
        encoding="utf-8",
    )
    catalog = load_teacher_catalog(
        declarations_path=declarations,
        outcomes_path=outcomes,
        expected=EvidenceReferences(
            hashlib.sha256(declarations.read_bytes()).hexdigest(), references.runner_outcomes_sha256
        ),
    )
    assert catalog.for_scope(frozenset({"r"})).races[0].declared_starters is None


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (
            '{"version":"runner-outcomes-v1","races":[{"race_id":"r","outcomes":[]},{"race_id":"r","outcomes":[]}]}',
            "Duplicate outcome",
        ),
        ('{"version":"runner-outcomes-v1","races":[{"race_id":"r","outcomes":{}}]}', "arrays"),
        (
            '{"version":"runner-outcomes-v1","races":[{"race_id":"r","outcomes":[{}]}]}',
            "record fields",
        ),
        (
            '{"version":"runner-outcomes-v1","races":[{"race_id":"r","outcomes":[{"horse_id":1,"horse_number":1,"disposition":"classified","finish":1}]}]}',
            "text",
        ),
    ],
)
def test_malformed_outcomes_are_not_filtered(
    inputs: tuple[Path, Path, EvidenceReferences],
    text: str,
    message: str,
) -> None:
    declarations, outcomes, references = inputs
    outcomes.write_text(text, encoding="utf-8")
    expected = EvidenceReferences(
        references.declared_counts_sha256, hashlib.sha256(outcomes.read_bytes()).hexdigest()
    )
    with pytest.raises(ValueError, match=message):
        load_teacher_catalog(
            declarations_path=declarations, outcomes_path=outcomes, expected=expected
        )
