"""Explicit byte-bound raw sources and whole-scope normalized payloads; no writes/fits."""

import csv
import gzip
import hashlib
import io
import json
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Final, Protocol, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq

from predict_lib.jvd_outcomes import RawJvdDeclaration, RawJvdRunner
from predict_lib.jvd_scope import JvdScopeProjection, project_domestic_jvd_scope
from predict_lib.teacher_catalog import read_teacher_evidence_bytes
from predict_lib.training_admission import RunnerOutcome
from predict_lib.training_roster_match import EvidenceReferences


@dataclass(frozen=True)
class JvdRawInputs:
    runners_path: Path
    declarations_path: Path
    runners_sha256: str
    declarations_sha256: str


@dataclass(frozen=True)
class LoadedJvdScope:
    inputs: JvdRawInputs
    normalization_contract: str
    projection: JvdScopeProjection


@dataclass(frozen=True)
class JvdCatalogPayloads:
    declarations: bytes
    outcomes: bytes
    references: EvidenceReferences


class _ParquetReader(Protocol):
    @property
    def schema_arrow(self) -> object: ...

    def iter_batches(self, *, batch_size: int) -> Iterator[object]: ...

    def close(self) -> None: ...


@runtime_checkable
class _ColumnNames(Protocol):
    @property
    def names(self) -> list[str]: ...


@runtime_checkable
class _RecordBatch(Protocol):
    def to_pylist(self) -> list[dict[str, object]]: ...


NORMALIZATION_CONTRACT: Final[str] = "domestic-final-jvd-full-requested-scope-v1"
_RUNNER_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "race_id",
        "horse_id",
        "horse_number",
        "source_status",
        "abnormality_code",
        "finish_text",
    }
)
_DECLARATION_FIELDS: Final[frozenset[str]] = frozenset(
    {"race_id", "declared_starters", "source_status"}
)
_BATCH_SIZE: Final[int] = 65536


def _nullable_text(value: object) -> str | None:
    if value is None or isinstance(value, str):
        return value
    raise TypeError("Raw JVD fields must be text or null")


def _runner(record: dict[str, object]) -> RawJvdRunner:
    if record.keys() != _RUNNER_FIELDS:
        raise ValueError("Unexpected raw JVD runner fields")
    return RawJvdRunner(
        race_id=_nullable_text(record["race_id"]),
        horse_id=_nullable_text(record["horse_id"]),
        horse_number=_nullable_text(record["horse_number"]),
        source_status=_nullable_text(record["source_status"]),
        abnormality_code=_nullable_text(record["abnormality_code"]),
        finish_text=_nullable_text(record["finish_text"]),
    )


def _batch_rows(batch: object) -> Iterator[RawJvdRunner]:
    if not isinstance(batch, _RecordBatch):
        raise TypeError("Parquet batch does not expose raw records")
    return (_runner(record) for record in batch.to_pylist())


def _runners(content: bytes) -> tuple[RawJvdRunner, ...]:
    with pa.BufferReader(content) as buffer:
        reader: _ParquetReader = pq.ParquetFile(buffer)
        try:
            return _reader_rows(reader)
        finally:
            reader.close()


def _reader_rows(reader: _ParquetReader) -> tuple[RawJvdRunner, ...]:
    schema = reader.schema_arrow
    if not isinstance(schema, _ColumnNames):
        raise TypeError("Raw Parquet schema is unavailable")
    if len(schema.names) != len(_RUNNER_FIELDS) or frozenset(schema.names) != _RUNNER_FIELDS:
        raise ValueError("Unexpected raw JVD Parquet columns")
    return tuple(
        chain.from_iterable(
            _batch_rows(batch) for batch in reader.iter_batches(batch_size=_BATCH_SIZE)
        )
    )


def _declarations(content: bytes) -> tuple[RawJvdDeclaration, ...]:
    with io.StringIO(gzip.decompress(content).decode("utf-8"), newline="") as stream:
        reader = csv.DictReader(stream, strict=True)
        fields = reader.fieldnames
        if (
            fields is None
            or len(fields) != len(_DECLARATION_FIELDS)
            or frozenset(fields) != _DECLARATION_FIELDS
        ):
            raise ValueError("Unexpected raw JVD declaration columns")
        rows: list[RawJvdDeclaration] = []
        for record in reader:
            if record.keys() != _DECLARATION_FIELDS or any(
                value is None for value in record.values()
            ):
                raise ValueError("Incomplete or extra declaration CSV fields")
            rows.append(
                RawJvdDeclaration(
                    race_id=record["race_id"],
                    declared_starters=record["declared_starters"],
                    source_status=record["source_status"],
                )
            )
    return tuple(rows)


def load_domestic_jvd_scope(
    *, inputs: JvdRawInputs, required_race_ids: frozenset[str]
) -> LoadedJvdScope:
    """Parse only the verified bytes; do not infer source authenticity or PIT."""
    runners = read_teacher_evidence_bytes(inputs.runners_path, inputs.runners_sha256)
    declarations = read_teacher_evidence_bytes(inputs.declarations_path, inputs.declarations_sha256)
    projection = project_domestic_jvd_scope(
        required_race_ids=required_race_ids,
        runner_rows=_runners(runners),
        declaration_rows=_declarations(declarations),
    )
    return LoadedJvdScope(inputs, NORMALIZATION_CONTRACT, projection)


def _encode(document: dict[str, object]) -> bytes:
    return (
        json.dumps(
            document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def _outcome_record(race_id: str, outcomes: tuple[RunnerOutcome, ...]) -> dict[str, object]:
    return {
        "race_id": race_id,
        "outcomes": [
            {
                "horse_id": row.horse_id,
                "horse_number": row.horse_number,
                "disposition": row.disposition,
                "finish": row.finish,
            }
            for row in outcomes
        ],
    }


def serialize_jvd_scope(projection: JvdScopeProjection) -> JvdCatalogPayloads:
    """Serialize complete evidence only; no filesystem or model side effects."""
    races = projection.race_evidence
    if projection.issues or not races:
        raise ValueError("Cannot serialize a blocked or empty JVD scope")
    if (
        len(races) != len(projection.required_race_ids)
        or frozenset(race.race_id for race in races) != projection.required_race_ids
    ):
        raise ValueError("JVD evidence population differs from required scope")
    declarations = _encode(
        {
            "version": "independent-declarations-v1",
            "races": [
                {"race_id": race.race_id, "declared_starters": race.declared_starters}
                for race in races
            ],
        }
    )
    outcomes = _encode(
        {
            "version": "runner-outcomes-v1",
            "races": [_outcome_record(race.race_id, race.outcomes) for race in races],
        }
    )
    return JvdCatalogPayloads(
        declarations,
        outcomes,
        EvidenceReferences(
            hashlib.sha256(declarations).hexdigest(),
            hashlib.sha256(outcomes).hexdigest(),
        ),
    )
