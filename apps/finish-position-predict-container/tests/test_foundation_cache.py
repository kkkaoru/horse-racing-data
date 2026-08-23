from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

import pytest

from predict_lib.foundation_cache import (
    FOUNDATION_MANIFEST_MAX_BYTES,
    build_foundation_manifest_key,
    build_foundation_race_key,
    build_foundation_source_key,
    validate_foundation_objects,
)
from predict_lib.model_meta import Category
from predict_lib.r2_client import R2ObjectIdentity


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _payloads(
    *,
    category: Category = "jra",
    target_date: str = "20260823",
    venue: str = "1",
    race_number: str = "2",
    manifest_contract_updates: Mapping[str, object] | None = None,
    race_contract_updates: Mapping[str, object] | None = None,
    row_updates: Mapping[str, object] | None = None,
) -> tuple[bytes, bytes, frozenset[str]]:
    source = "jra" if category == "jra" else "nar"
    race_id = (
        f"{source}:{target_date[:4]}:{target_date[4:]}:{venue.zfill(2)}:{race_number.zfill(2)}"
    )
    names = ["race_id", "ketto_toroku_bango", "umaban", "value"]
    feature_hash = _hash("\n".join(names))
    entry_hash = _hash("horse-a:1\nhorse-b:2")
    feature_schema: list[object] = [
        {"convertedType": "UTF8", "name": "race_id", "physicalType": "BYTE_ARRAY"},
        {
            "convertedType": "UTF8",
            "name": "ketto_toroku_bango",
            "physicalType": "BYTE_ARRAY",
        },
        {"name": "umaban", "physicalType": "INT64"},
        {"name": "value", "physicalType": "DOUBLE"},
    ]
    source_contract: dict[str, object] = {
        "etag": "source-etag",
        "key": build_foundation_source_key(category, target_date),
        "version": "source-version",
    }
    manifest_contract: dict[str, object] = {
        "contractVersion": "day-base-race-foundation-v1",
        "featureHash": feature_hash,
        "featureSchema": feature_schema,
        "generationId": "generation",
        "raceCount": 1,
        "rowCount": 2,
        "schemaVersion": "1",
    }
    if manifest_contract_updates:
        manifest_contract.update(manifest_contract_updates)
    race_contract: dict[str, object] = {
        "contractVersion": "day-base-race-foundation-v1",
        "entrySetHash": entry_hash,
        "featureHash": feature_hash,
        "generationId": "generation",
        "rowCount": 2,
        "schemaVersion": "1",
    }
    if race_contract_updates:
        race_contract.update(race_contract_updates)
    first_row: dict[str, object] = {
        "race_id": race_id,
        "ketto_toroku_bango": "horse-a",
        "umaban": 1,
        "value": 0.5,
    }
    if row_updates:
        first_row.update(row_updates)
    race_key = build_foundation_race_key(category, target_date, venue, race_number)
    manifest: dict[str, object] = {
        "contract": manifest_contract,
        "races": [
            {
                "entrySetHash": entry_hash,
                "key": race_key,
                "raceId": race_id,
                "rowCount": 2,
            }
        ],
        "source": source_contract,
    }
    race: dict[str, object] = {
        "contract": race_contract,
        "raceId": race_id,
        "rows": [
            first_row,
            {
                "race_id": race_id,
                "ketto_toroku_bango": "horse-b",
                "umaban": 2,
                "value": None,
            },
        ],
        "source": source_contract,
    }
    return (
        json.dumps(manifest).encode(),
        json.dumps(race).encode(),
        frozenset({"horse-a:1", "horse-b:2"}),
    )


def test_key_builders_match_worker_contract() -> None:
    assert build_foundation_manifest_key("ban-ei", "20260823") == (
        "feat-daybase-race/catalog-v1/ban-ei/20260823/manifest.json"
    )
    assert build_foundation_race_key("jra", "20260823", "1", "2") == (
        "feat-daybase-race/catalog-v1/jra/20260823/01/02/foundation.json"
    )


@pytest.mark.parametrize("category,source", [("jra", "jra"), ("nar", "nar"), ("ban-ei", "nar")])
def test_validate_foundation_objects_accepts_exact_contract(
    category: Category, source: str
) -> None:
    manifest, race, entries = _payloads(category=category)
    result = validate_foundation_objects(
        category=category,
        target_date="20260823",
        target_race="1:2",
        manifest_bytes=manifest,
        race_bytes=race,
        source_identity=R2ObjectIdentity("source-etag", "source-version"),
        expected_entries=entries,
    )
    assert result.reason == "hit"
    assert result.rows is not None and len(result.rows) == 2
    assert result.rows[0]["race_id"] == f"{source}:2026:0823:01:02"
    assert result.schema is not None
    assert [field.physical_type for field in result.schema] == [
        "BYTE_ARRAY",
        "BYTE_ARRAY",
        "INT64",
        "DOUBLE",
    ]


@pytest.mark.parametrize(
    "manifest,race,reason",
    [
        (b"", b"{}", "invalid-json"),
        (b"not-json", b"{}", "invalid-json"),
        (b"[]", b"{}", "invalid-json"),
        (b"{}", b"x" * (2 * 1024 * 1024 + 1), "invalid-json"),
        (b"x" * (FOUNDATION_MANIFEST_MAX_BYTES + 1), b"{}", "invalid-json"),
    ],
)
def test_validate_foundation_objects_rejects_invalid_json(
    manifest: bytes, race: bytes, reason: str
) -> None:
    result = validate_foundation_objects(
        category="jra",
        target_date="20260823",
        target_race="1:2",
        manifest_bytes=manifest,
        race_bytes=race,
        source_identity=R2ObjectIdentity("source-etag", "source-version"),
        expected_entries=frozenset({"horse-a:1"}),
    )
    assert result.reason == reason
    assert result.rows is None


@pytest.mark.parametrize("target_date,target_race", [("bad", "1:2"), ("20260823", "bad")])
def test_validate_foundation_objects_rejects_invalid_request(
    target_date: str, target_race: str
) -> None:
    manifest, race, entries = _payloads()
    result = validate_foundation_objects(
        category="jra",
        target_date=target_date,
        target_race=target_race,
        manifest_bytes=manifest,
        race_bytes=race,
        source_identity=R2ObjectIdentity("source-etag", "source-version"),
        expected_entries=entries,
    )
    assert result.reason == "invalid-request-contract"


@pytest.mark.parametrize(
    "manifest_updates,race_updates,row_updates,entries,reason",
    [
        ({"schemaVersion": "0"}, {}, {}, None, "schema-version-mismatch"),
        ({"contractVersion": "old"}, {}, {}, None, "contract-version-mismatch"),
        ({"featureHash": "wrong"}, {}, {}, None, "invalid-feature-schema"),
        ({"featureSchema": []}, {}, {}, None, "invalid-feature-schema"),
        ({"featureSchema": [{"name": "x"}]}, {}, {}, None, "invalid-feature-schema"),
        ({"generationId": ""}, {}, {}, None, "invalid-manifest-contract"),
        ({"raceCount": 0}, {}, {}, None, "invalid-manifest-contract"),
        ({"rowCount": True}, {}, {}, None, "invalid-manifest-contract"),
        ({}, {"generationId": "wrong"}, {}, None, "race-contract-mismatch"),
        ({}, {"rowCount": 1}, {}, None, "race-contract-mismatch"),
        ({}, {}, {"race_id": "jra:2026:0823:01:03"}, None, "race-rows-mismatch"),
        ({}, {}, {"value": float("nan")}, None, "race-rows-mismatch"),
        ({}, {}, {"umaban": 0}, None, "race-rows-mismatch"),
        ({}, {}, {}, frozenset({"horse-a:1"}), "race-rows-mismatch"),
    ],
)
def test_validate_foundation_objects_fails_closed_on_contract_drift(
    manifest_updates: Mapping[str, object],
    race_updates: Mapping[str, object],
    row_updates: Mapping[str, object],
    entries: frozenset[str] | None,
    reason: str,
) -> None:
    manifest, race, expected = _payloads(
        manifest_contract_updates=manifest_updates,
        race_contract_updates=race_updates,
        row_updates=row_updates,
    )
    result = validate_foundation_objects(
        category="jra",
        target_date="20260823",
        target_race="1:2",
        manifest_bytes=manifest,
        race_bytes=race,
        source_identity=R2ObjectIdentity("source-etag", "source-version"),
        expected_entries=entries if entries is not None else expected,
    )
    assert result.reason == reason
    assert result.rows is None


def test_validate_foundation_objects_requires_source_identity() -> None:
    manifest, race, entries = _payloads()
    result = validate_foundation_objects(
        category="jra",
        target_date="20260823",
        target_race="1:2",
        manifest_bytes=manifest,
        race_bytes=race,
        source_identity=R2ObjectIdentity("stale", ""),
        expected_entries=entries,
    )
    assert result.reason == "manifest-source-mismatch"
