"""Real small raw files, whole-scope byte binding, and normalized roundtrip."""

import csv
import gzip
import hashlib
import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from predict_lib import jvd_sources
from predict_lib.jvd_sources import JvdRawInputs, load_domestic_jvd_scope, serialize_jvd_scope
from predict_lib.teacher_catalog import load_teacher_catalog


@pytest.fixture
def inputs(tmp_path: Path) -> JvdRawInputs:
    runners = tmp_path / "runners.parquet"
    declarations = tmp_path / "declarations.csv.gz"
    pq.write_table(
        pa.table(
            {
                "race_id": ["jra:2020:0229:06:01"] * 3,
                "horse_id": ["2010100001", "2010100002", "2010100003"],
                "horse_number": ["01", "19", "03"],
                "source_status": ["7", "7", "7"],
                "abnormality_code": ["0", "4", "1"],
                "finish_text": ["01", None, "00"],
            }
        ),
        runners,
    )
    declarations.write_bytes(
        gzip.compress(b"race_id,declared_starters,source_status\njra:2020:0229:06:01,02,7\n")
    )
    return JvdRawInputs(
        runners,
        declarations,
        hashlib.sha256(runners.read_bytes()).hexdigest(),
        hashlib.sha256(declarations.read_bytes()).hexdigest(),
    )


def test_real_raw_files_roundtrip_through_existing_teacher_catalog(
    inputs: JvdRawInputs, tmp_path: Path
) -> None:
    loaded = load_domestic_jvd_scope(
        inputs=inputs, required_race_ids=frozenset({"jra:2020:0229:06:01"})
    )
    assert loaded.normalization_contract == "domestic-final-jvd-full-requested-scope-v1"
    assert loaded.projection.issues == frozenset()
    payloads = serialize_jvd_scope(loaded.projection)
    assert json.loads(payloads.declarations) == {
        "version": "independent-declarations-v1",
        "races": [{"race_id": "jra:2020:0229:06:01", "declared_starters": 2}],
    }
    declarations = tmp_path / "normalized-declarations.json"
    outcomes = tmp_path / "normalized-outcomes.json"
    declarations.write_bytes(payloads.declarations)
    outcomes.write_bytes(payloads.outcomes)
    catalog = load_teacher_catalog(
        declarations_path=declarations, outcomes_path=outcomes, expected=payloads.references
    )
    evidence = catalog.for_scope(frozenset({"jra:2020:0229:06:01"}))
    assert evidence.races[0].declared_starters == 2
    assert evidence.races[0].outcomes[1].disposition == "dnf"
    assert evidence.races[0].outcomes[1].horse_number == 19
    assert evidence.races[0].outcomes[1].finish is None
    assert evidence.races[0].outcomes[2].disposition == "withdrawn"
    assert payloads == serialize_jvd_scope(loaded.projection)


def test_captured_verified_bytes_are_parsed_without_reopening_mutated_sources(
    inputs: JvdRawInputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Path.read_bytes
    reads: Counter[str] = Counter()

    def read(path: Path) -> bytes:
        content = original(path)
        reads[path.name] += 1
        path.write_bytes(b"changed after capture")
        return content

    monkeypatch.setattr(Path, "read_bytes", read)
    loaded = load_domestic_jvd_scope(
        inputs=inputs, required_race_ids=frozenset({"jra:2020:0229:06:01"})
    )
    assert loaded.projection.issues == frozenset()
    assert reads == {"runners.parquet": 1, "declarations.csv.gz": 1}


@pytest.mark.parametrize("source", ["runners", "declarations"])
def test_both_source_hashes_are_checked_before_parsing(
    inputs: JvdRawInputs, source: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    changed = (
        replace(inputs, runners_sha256="0" * 64)
        if source == "runners"
        else replace(inputs, declarations_sha256="0" * 64)
    )

    def forbidden(_buffer: object) -> object:
        raise AssertionError("Must verify both inputs before parsing")

    monkeypatch.setattr(pq, "ParquetFile", forbidden)
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        load_domestic_jvd_scope(
            inputs=changed, required_race_ids=frozenset({"jra:2020:0229:06:01"})
        )


@pytest.mark.parametrize(
    "csv_text",
    [
        "",
        "race_id,race_id,source_status\n",
        "race_id,declared_starters,source_status,extra\n",
        "race_id,declared_starters,source_status\njra:2020:0229:06:01,02\n",
        "race_id,declared_starters,source_status\njra:2020:0229:06:01,02,7,extra\n",
        'race_id,declared_starters,source_status\n"unterminated',
    ],
)
def test_malformed_declaration_structure_is_rejected(inputs: JvdRawInputs, csv_text: str) -> None:
    content = gzip.compress(csv_text.encode())
    inputs.declarations_path.write_bytes(content)
    changed = replace(inputs, declarations_sha256=hashlib.sha256(content).hexdigest())
    with pytest.raises((ValueError, csv.Error)):
        load_domestic_jvd_scope(
            inputs=changed, required_race_ids=frozenset({"jra:2020:0229:06:01"})
        )


def test_unknown_raw_count_retains_diagnostics_but_cannot_serialize(inputs: JvdRawInputs) -> None:
    content = gzip.compress(b"race_id,declared_starters,source_status\njra:2020:0229:06:01,00,7\n")
    inputs.declarations_path.write_bytes(content)
    changed = replace(inputs, declarations_sha256=hashlib.sha256(content).hexdigest())
    loaded = load_domestic_jvd_scope(
        inputs=changed, required_race_ids=frozenset({"jra:2020:0229:06:01"})
    )
    assert len(loaded.projection.runners) == 3
    assert loaded.projection.declarations[0].raw.declared_starters == "00"
    assert loaded.projection.issues == {"declaration_projection_blocked"}
    with pytest.raises(ValueError, match="blocked or empty"):
        serialize_jvd_scope(loaded.projection)


@pytest.mark.parametrize("change", ["empty", "duplicate", "different-scope"])
def test_serializer_does_not_trust_an_inconsistent_projection(
    inputs: JvdRawInputs, change: str
) -> None:
    projection = load_domestic_jvd_scope(
        inputs=inputs, required_race_ids=frozenset({"jra:2020:0229:06:01"})
    ).projection
    assert projection.race_evidence is not None
    if change == "empty":
        projection = replace(projection, race_evidence=())
    elif change == "duplicate":
        projection = replace(projection, race_evidence=projection.race_evidence * 2)
    else:
        projection = replace(projection, required_race_ids=frozenset({"jra:2020:0301:06:01"}))
    with pytest.raises(ValueError, match="scope"):
        serialize_jvd_scope(projection)


@pytest.mark.parametrize(
    "mode", ["empty-records", "schema", "columns", "duplicate-columns", "batch", "record", "value"]
)
def test_arrow_boundary_failures_close_the_reader(
    inputs: JvdRawInputs, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    closed: list[bool] = []

    class Batch:
        def to_pylist(self) -> list[dict[str, object]]:
            if mode == "empty-records":
                return []
            if mode == "record":
                return [{}]
            return [
                {
                    "race_id": "jra:2020:0229:06:01",
                    "horse_id": "2010100001",
                    "horse_number": 1,
                    "source_status": "7",
                    "abnormality_code": "0",
                    "finish_text": "01",
                }
            ]

    class Reader:
        schema_arrow: object = pa.schema(
            [
                ("race_id", pa.string()),
                ("horse_id", pa.string()),
                ("horse_number", pa.string()),
                ("source_status", pa.string()),
                ("abnormality_code", pa.string()),
                ("finish_text", pa.string()),
            ]
        )

        def iter_batches(self, *, batch_size: int) -> Iterator[object]:
            yield object() if mode == "batch" else Batch()

        def close(self) -> None:
            closed.append(True)

    def reader(_buffer: object) -> Reader:
        result = Reader()
        if mode == "schema":
            result.schema_arrow = None
        if mode == "columns":
            result.schema_arrow = pa.schema([])
        if mode == "duplicate-columns":
            result.schema_arrow = pa.schema(
                [
                    ("race_id", pa.string()),
                    ("horse_id", pa.string()),
                    ("horse_number", pa.string()),
                    ("source_status", pa.string()),
                    ("abnormality_code", pa.string()),
                    ("race_id", pa.string()),
                ]
            )
        return result

    monkeypatch.setattr(jvd_sources.pq, "ParquetFile", reader)
    if mode == "empty-records":
        loaded = load_domestic_jvd_scope(
            inputs=inputs, required_race_ids=frozenset({"jra:2020:0229:06:01"})
        )
        assert loaded.projection.race_evidence is None
        assert loaded.projection.issues == {"missing_runner_races"}
    else:
        with pytest.raises((ValueError, TypeError)):
            load_domestic_jvd_scope(
                inputs=inputs, required_race_ids=frozenset({"jra:2020:0229:06:01"})
            )
    assert closed == [True]


def test_hash_match_does_not_make_corrupt_parquet_valid(inputs: JvdRawInputs) -> None:
    content = b"not a parquet file"
    inputs.runners_path.write_bytes(content)
    changed = replace(inputs, runners_sha256=hashlib.sha256(content).hexdigest())
    with pytest.raises(ValueError, match="Parquet"):
        load_domestic_jvd_scope(
            inputs=changed, required_race_ids=frozenset({"jra:2020:0229:06:01"})
        )


def test_hash_match_does_not_make_corrupt_gzip_valid(inputs: JvdRawInputs) -> None:
    content = b"not a gzip file"
    inputs.declarations_path.write_bytes(content)
    changed = replace(inputs, declarations_sha256=hashlib.sha256(content).hexdigest())
    with pytest.raises(gzip.BadGzipFile):
        load_domestic_jvd_scope(
            inputs=changed, required_race_ids=frozenset({"jra:2020:0229:06:01"})
        )


def test_gzip_declaration_requires_valid_utf8(inputs: JvdRawInputs) -> None:
    content = gzip.compress(b"\xff")
    inputs.declarations_path.write_bytes(content)
    changed = replace(inputs, declarations_sha256=hashlib.sha256(content).hexdigest())
    with pytest.raises(UnicodeDecodeError):
        load_domestic_jvd_scope(
            inputs=changed, required_race_ids=frozenset({"jra:2020:0229:06:01"})
        )
