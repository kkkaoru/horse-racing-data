"""Fail-closed parser for Worker-materialized per-race day-base foundations."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from .model_meta import Category
from .r2_client import R2ObjectIdentity

SCHEMA_VERSION: Final[str] = "1"
FOUNDATION_CONTRACT_VERSION: Final[str] = "day-base-race-foundation-v1"
_FOUNDATION_PREFIX: Final[str] = "feat-daybase-race"
_GENERATION: Final[str] = "catalog-v1"
_FOUNDATION_FILE: Final[str] = "foundation.json"
_MANIFEST_FILE: Final[str] = "manifest.json"
_DAY_BASE_PREFIX: Final[str] = "feat-daybase"
_DAY_BASE_FILE: Final[str] = "features.parquet"
FOUNDATION_MANIFEST_MAX_BYTES: Final[int] = 512 * 1024
FOUNDATION_RACE_MAX_BYTES: Final[int] = 2 * 1024 * 1024
_MAX_RACES: Final[int] = 64
_MAX_ROWS: Final[int] = 1_024
_MAX_RACE_ROWS: Final[int] = 32
_RACE_ID: Final[str] = "race_id"
_KETTO: Final[str] = "ketto_toroku_bango"
_UMABAN: Final[str] = "umaban"

JsonScalar = bool | float | int | str | None
FoundationRow = dict[str, JsonScalar]


@dataclass(frozen=True)
class FoundationFeatureField:
    """Parquet field contract required to preserve all-null column types."""

    converted_type: str | None
    name: str
    physical_type: str
    precision: int | None
    scale: int | None
    type_length: int | None


@dataclass(frozen=True)
class FoundationLoadResult:
    """Validated race rows, or a stable miss reason with no rows."""

    reason: str
    rows: tuple[FoundationRow, ...] | None
    schema: tuple[FoundationFeatureField, ...] | None


def build_foundation_manifest_key(category: Category, target_date: str) -> str:
    return f"{_FOUNDATION_PREFIX}/{_GENERATION}/{category}/{target_date}/{_MANIFEST_FILE}"


def build_foundation_race_key(
    category: Category,
    target_date: str,
    venue_code: str,
    race_number: str,
) -> str:
    return (
        f"{_FOUNDATION_PREFIX}/{_GENERATION}/{category}/{target_date}/"
        f"{venue_code.zfill(2)}/{race_number.zfill(2)}/{_FOUNDATION_FILE}"
    )


def build_foundation_source_key(category: Category, target_date: str) -> str:
    return f"{_DAY_BASE_PREFIX}/{_GENERATION}/{category}/{target_date}/{_DAY_BASE_FILE}"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _json_object(data: bytes, maximum: int) -> Mapping[str, object] | None:
    if not data or len(data) > maximum:
        return None
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _string(mapping: Mapping[str, object], key: str) -> str | None:
    value = mapping.get(key)
    return value if isinstance(value, str) and value else None


def _integer(mapping: Mapping[str, object], key: str, maximum: int) -> int | None:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 < value <= maximum else None


def _mapping(mapping: Mapping[str, object], key: str) -> Mapping[str, object] | None:
    value = mapping.get(key)
    return value if isinstance(value, dict) else None


def _source_matches(
    value: object,
    source_key: str,
    source_identity: R2ObjectIdentity,
) -> bool:
    if not isinstance(value, dict):
        return False
    stored_version = value.get("version")
    return (
        value.get("key") == source_key
        and value.get("etag") == source_identity.etag
        and isinstance(stored_version, str)
        and (not source_identity.version or stored_version == source_identity.version)
    )


def _expected_race_id(category: Category, target_date: str, target_race: str) -> str | None:
    parts = target_race.split(":")
    if len(parts) != 2 or not all(part.isascii() and part.isdigit() for part in parts):
        return None
    venue, race = parts
    if not (1 <= len(venue) <= 2 and 1 <= len(race) <= 2 and len(target_date) == 8):
        return None
    source = "jra" if category == "jra" else "nar"
    return f"{source}:{target_date[:4]}:{target_date[4:]}:{venue.zfill(2)}:{race.zfill(2)}"


def _normalize_entry_token(ketto: object, umaban: object) -> str | None:
    if not isinstance(ketto, str) or not ketto.strip():
        return None
    if isinstance(umaban, bool) or not isinstance(umaban, (int, str)):
        return None
    text = str(umaban).strip()
    if not text.isascii() or not text.isdigit():
        return None
    number = int(text)
    return f"{ketto.strip()}:{number}" if number > 0 else None


def _validate_rows(
    value: object,
    expected_race_id: str,
    expected_feature_names: tuple[str, ...],
    expected_entry_hash: str,
    expected_entries: frozenset[str],
) -> tuple[FoundationRow, ...] | None:
    if not isinstance(value, list) or not (0 < len(value) <= _MAX_RACE_ROWS):
        return None
    rows: list[FoundationRow] = []
    ordered_names: tuple[str, ...] | None = None
    entries: list[str] = []
    for raw_row in value:
        if not isinstance(raw_row, dict) or not raw_row:
            return None
        names = tuple(raw_row)
        if ordered_names is None:
            ordered_names = names
        elif names != ordered_names:
            return None
        row: FoundationRow = {}
        for name, cell in raw_row.items():
            if not isinstance(name, str) or not name:
                return None
            if cell is not None and not isinstance(cell, (bool, float, int, str)):
                return None
            if isinstance(cell, float) and not math.isfinite(cell):
                return None
            row[name] = cell
        if row.get(_RACE_ID) != expected_race_id:
            return None
        token = _normalize_entry_token(row.get(_KETTO), row.get(_UMABAN))
        if token is None:
            return None
        entries.append(token)
        rows.append(row)
    if ordered_names != expected_feature_names:
        return None
    if len(set(entries)) != len(entries):
        return None
    if _sha256("\n".join(sorted(entries))) != expected_entry_hash:
        return None
    if frozenset(entries) != expected_entries:
        return None
    return tuple(rows)


def _optional_integer(mapping: Mapping[str, object], key: str) -> int | None:
    value = mapping.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _feature_schema(value: object, feature_hash: str) -> tuple[FoundationFeatureField, ...] | None:
    if not isinstance(value, list) or not value or len(value) > 512:
        return None
    fields: list[FoundationFeatureField] = []
    for raw_field in value:
        if not isinstance(raw_field, dict):
            return None
        name = _string(raw_field, "name")
        physical_type = _string(raw_field, "physicalType")
        converted_raw = raw_field.get("convertedType")
        if name is None or physical_type is None:
            return None
        if converted_raw is not None and not isinstance(converted_raw, str):
            return None
        fields.append(
            FoundationFeatureField(
                converted_type=converted_raw,
                name=name,
                physical_type=physical_type,
                precision=_optional_integer(raw_field, "precision"),
                scale=_optional_integer(raw_field, "scale"),
                type_length=_optional_integer(raw_field, "typeLength"),
            )
        )
    names = [field.name for field in fields]
    if len(set(names)) != len(names) or _sha256("\n".join(names)) != feature_hash:
        return None
    return tuple(fields)


def _manifest_race(
    value: object,
    *,
    expected_key: str,
    expected_race_id: str,
    race_count: int,
    row_count: int,
) -> Mapping[str, object] | None:
    if not isinstance(value, list) or len(value) != race_count:
        return None
    races = [race for race in value if isinstance(race, dict)]
    if len(races) != race_count:
        return None
    keys = [race.get("key") for race in races]
    race_ids = [race.get("raceId") for race in races]
    if not all(isinstance(key, str) for key in keys) or not all(
        isinstance(race_id, str) for race_id in race_ids
    ):
        return None
    if len(set(keys)) != race_count or len(set(race_ids)) != race_count:
        return None
    counts = [_integer(race, "rowCount", _MAX_RACE_ROWS) for race in races]
    if any(count is None for count in counts) or (
        sum(count for count in counts if count) != row_count
    ):
        return None
    return next(
        (
            race
            for race in races
            if race.get("key") == expected_key and race.get("raceId") == expected_race_id
        ),
        None,
    )


def validate_foundation_objects(
    *,
    category: Category,
    target_date: str,
    target_race: str,
    manifest_bytes: bytes,
    race_bytes: bytes,
    source_identity: R2ObjectIdentity,
    expected_entries: frozenset[str],
) -> FoundationLoadResult:
    """Validate the full Worker contract; never return partially trusted rows."""
    expected_race_id = _expected_race_id(category, target_date, target_race)
    if expected_race_id is None or not expected_entries:
        return FoundationLoadResult("invalid-request-contract", None, None)
    venue, race_number = target_race.split(":")
    expected_key = build_foundation_race_key(category, target_date, venue, race_number)
    source_key = build_foundation_source_key(category, target_date)
    manifest = _json_object(manifest_bytes, FOUNDATION_MANIFEST_MAX_BYTES)
    race = _json_object(race_bytes, FOUNDATION_RACE_MAX_BYTES)
    if manifest is None or race is None:
        return FoundationLoadResult("invalid-json", None, None)
    manifest_contract = _mapping(manifest, "contract")
    if manifest_contract is None or not _source_matches(
        manifest.get("source"), source_key, source_identity
    ):
        return FoundationLoadResult("manifest-source-mismatch", None, None)
    if manifest_contract.get("schemaVersion") != SCHEMA_VERSION:
        return FoundationLoadResult("schema-version-mismatch", None, None)
    if manifest_contract.get("contractVersion") != FOUNDATION_CONTRACT_VERSION:
        return FoundationLoadResult("contract-version-mismatch", None, None)
    feature_hash = _string(manifest_contract, "featureHash")
    generation_id = _string(manifest_contract, "generationId")
    race_count = _integer(manifest_contract, "raceCount", _MAX_RACES)
    row_count = _integer(manifest_contract, "rowCount", _MAX_ROWS)
    if feature_hash is None or generation_id is None or race_count is None or row_count is None:
        return FoundationLoadResult("invalid-manifest-contract", None, None)
    feature_schema = _feature_schema(manifest_contract.get("featureSchema"), feature_hash)
    if feature_schema is None:
        return FoundationLoadResult("invalid-feature-schema", None, None)
    manifest_race = _manifest_race(
        manifest.get("races"),
        expected_key=expected_key,
        expected_race_id=expected_race_id,
        race_count=race_count,
        row_count=row_count,
    )
    if manifest_race is None:
        return FoundationLoadResult("invalid-manifest-races", None, None)
    entry_hash = _string(manifest_race, "entrySetHash")
    race_row_count = _integer(manifest_race, "rowCount", _MAX_RACE_ROWS)
    race_contract = _mapping(race, "contract")
    if entry_hash is None or race_row_count is None or race_contract is None:
        return FoundationLoadResult("invalid-race-contract", None, None)
    if (
        race.get("raceId") != expected_race_id
        or race.get("source") != manifest.get("source")
        or not _source_matches(race.get("source"), source_key, source_identity)
    ):
        return FoundationLoadResult("race-source-mismatch", None, None)
    expected_contract: Mapping[str, object] = {
        "contractVersion": FOUNDATION_CONTRACT_VERSION,
        "entrySetHash": entry_hash,
        "featureHash": feature_hash,
        "generationId": generation_id,
        "rowCount": race_row_count,
        "schemaVersion": SCHEMA_VERSION,
    }
    if race_contract != expected_contract:
        return FoundationLoadResult("race-contract-mismatch", None, None)
    rows = _validate_rows(
        race.get("rows"),
        expected_race_id,
        tuple(field.name for field in feature_schema),
        entry_hash,
        expected_entries,
    )
    if rows is None or len(rows) != race_row_count:
        return FoundationLoadResult("race-rows-mismatch", None, None)
    return FoundationLoadResult("hit", rows, feature_schema)
