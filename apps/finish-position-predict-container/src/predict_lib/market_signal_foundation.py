"""Strict consumer contract for Worker-materialized market-signal foundations."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from .foundation_cache import (
    FoundationFeatureField,
    FoundationLoadResult,
    FoundationRow,
    build_foundation_manifest_key,
    build_foundation_race_key,
    build_foundation_source_key,
)
from .model_meta import Category
from .r2_client import R2ObjectIdentity

SCHEMA_VERSION: Final[str] = "1"
MARKET_SIGNAL_CONTRACT_VERSION: Final[str] = "race-chain-market-signal-foundation-v1"
MARKET_SIGNAL_FOUNDATION_MAX_BYTES: Final[int] = 2 * 1024 * 1024
MARKET_SIGNAL_ADDED_COLUMNS: Final[tuple[str, ...]] = (
    "tansho_odds_raw",
    "tansho_ninkijun_raw",
    "inverse_odds_implied_prob",
    "inverse_odds_market_share",
    "inverse_odds_rank_in_race",
    "popularity_rank_in_race",
    "odds_score_diff_from_race_avg",
    "popularity_score_diff_from_race_avg",
    "popularity_odds_disagreement",
    "form_market_edge",
)
MARKET_SIGNAL_FLOAT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tansho_odds_raw",
        "inverse_odds_implied_prob",
        "inverse_odds_market_share",
        "odds_score_diff_from_race_avg",
        "popularity_score_diff_from_race_avg",
        "popularity_odds_disagreement",
        "form_market_edge",
    }
)
MARKET_SIGNAL_INTEGER_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tansho_ninkijun_raw",
        "inverse_odds_rank_in_race",
        "popularity_rank_in_race",
    }
)

_FEATURE_PREFIX: Final[str] = "feat-racechain-market-signal"
_GENERATION: Final[str] = "catalog-v1"
_FEATURE_FILE: Final[str] = "foundation.json"
_MAX_ROWS: Final[int] = 32
_MAX_FEATURES: Final[int] = 522
_RACE_ID: Final[str] = "race_id"
_KETTO: Final[str] = "ketto_toroku_bango"
_UMABAN: Final[str] = "umaban"
_ODDS: Final[str] = "tansho_odds"
_POPULARITY: Final[str] = "tansho_ninkijun"
_ODDS_SCORE: Final[str] = "odds_score"
_POPULARITY_SCORE: Final[str] = "popularity_score"
_CAREER_WIN_RATE: Final[str] = "career_win_rate"
_CANONICAL_OVERWRITES: Final[frozenset[str]] = frozenset(
    {_ODDS, _POPULARITY, _ODDS_SCORE, _POPULARITY_SCORE}
)


@dataclass(frozen=True, slots=True)
class MarketSignalBaseEvidence:
    """Independently observed identities and validated base foundation contents."""

    base_rows: tuple[FoundationRow, ...]
    base_schema: tuple[FoundationFeatureField, ...]
    entry_set_hash: str
    foundation_identity: R2ObjectIdentity
    foundation_key: str
    generation_id: str
    manifest_identity: R2ObjectIdentity
    manifest_key: str
    source_identity: R2ObjectIdentity
    source_key: str


@dataclass(frozen=True, slots=True)
class MarketSignalLoadResult:
    """Fully attested Worker rows or one stable fail-closed miss reason."""

    reason: str
    rows: tuple[FoundationRow, ...] | None


def build_market_signal_foundation_key(
    category: Category,
    target_date: str,
    venue_code: str,
    race_number: str,
) -> str:
    """Return the dedicated non-final Worker artifact key."""
    return (
        f"{_FEATURE_PREFIX}/{_GENERATION}/{category}/{target_date}/"
        f"{venue_code.zfill(2)}/{race_number.zfill(2)}/{_FEATURE_FILE}"
    )


def market_signal_foundation_enabled(environment: Mapping[str, str]) -> bool:
    """Require an explicit opt-in; unset and every other value stay disabled."""
    return environment.get("WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED") == "1"


def build_market_signal_base_evidence(
    *,
    category: Category,
    target_date: str,
    target_race: str,
    manifest_bytes: bytes,
    foundation_bytes: bytes,
    foundation_result: FoundationLoadResult,
    source_identity: R2ObjectIdentity,
    foundation_identity: R2ObjectIdentity,
    manifest_identity: R2ObjectIdentity,
) -> MarketSignalBaseEvidence | None:
    """Bind a validated base result to independently observed R2 identities."""
    if foundation_result.rows is None or foundation_result.schema is None:
        return None
    parts = target_race.split(":")
    if len(parts) != 2:
        return None
    manifest = _json_object(manifest_bytes)
    foundation = _json_object(foundation_bytes)
    if manifest is None or foundation is None:
        return None
    manifest_contract = _mapping(manifest, "contract")
    race_contract = _mapping(foundation, "contract")
    if manifest_contract is None or race_contract is None:
        return None
    generation_id = _string(manifest_contract, "generationId")
    feature_hash = _string(manifest_contract, "featureHash")
    entry_set_hash = _string(race_contract, "entrySetHash")
    if (
        generation_id is None
        or entry_set_hash is None
        or race_contract.get("generationId") != generation_id
        or race_contract.get("featureHash") != feature_hash
        or feature_hash != _sha256("\n".join(field.name for field in foundation_result.schema))
    ):
        return None
    venue, race_number = parts
    return MarketSignalBaseEvidence(
        base_rows=foundation_result.rows,
        base_schema=foundation_result.schema,
        entry_set_hash=entry_set_hash,
        foundation_identity=foundation_identity,
        foundation_key=build_foundation_race_key(category, target_date, venue, race_number),
        generation_id=generation_id,
        manifest_identity=manifest_identity,
        manifest_key=build_foundation_manifest_key(category, target_date),
        source_identity=source_identity,
        source_key=build_foundation_source_key(category, target_date),
    )


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _json_object(data: bytes) -> Mapping[str, object] | None:
    if not data or len(data) > MARKET_SIGNAL_FOUNDATION_MAX_BYTES:
        return None
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _mapping(mapping: Mapping[str, object], key: str) -> Mapping[str, object] | None:
    value = mapping.get(key)
    return value if isinstance(value, dict) else None


def _string(mapping: Mapping[str, object], key: str) -> str | None:
    value = mapping.get(key)
    return value if isinstance(value, str) and value else None


def _positive_integer(mapping: Mapping[str, object], key: str, maximum: int) -> int | None:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 < value <= maximum else None


def _identity_matches(
    value: object,
    *,
    expected_key: str,
    expected_identity: R2ObjectIdentity,
) -> bool:
    if not isinstance(value, dict):
        return False
    return (
        value.get("key") == expected_key
        and value.get("etag") == expected_identity.etag
        and value.get("version") == expected_identity.version
    )


def _base_identity_matches(value: object, evidence: MarketSignalBaseEvidence) -> bool:
    if not isinstance(value, dict):
        return False
    return value == {
        "foundationEtag": evidence.foundation_identity.etag,
        "foundationKey": evidence.foundation_key,
        "foundationVersion": evidence.foundation_identity.version,
        "manifestEtag": evidence.manifest_identity.etag,
        "manifestKey": evidence.manifest_key,
        "manifestVersion": evidence.manifest_identity.version,
    }


def _entry_token(row: Mapping[str, object]) -> str | None:
    ketto = row.get(_KETTO)
    raw_number = row.get(_UMABAN)
    if not isinstance(ketto, str) or not ketto.strip():
        return None
    if isinstance(raw_number, bool) or not isinstance(raw_number, (int, str)):
        return None
    text = str(raw_number).strip()
    if not text.isascii() or not text.isdigit():
        return None
    number = int(text)
    return f"{ketto.strip()}:{number}" if 0 < number <= _MAX_ROWS else None


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _positive_number(value: object) -> float | None:
    number = _finite_number(value)
    return number if number is not None and number > 0 else None


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 < value <= _MAX_ROWS else None


def _js_number_token(value: float) -> str:
    """Match JavaScript String(number) for the bounded decimal odds domain."""
    return str(int(value)) if value.is_integer() else str(value)


def _snapshot_hash(rows: Sequence[Mapping[str, object]]) -> str | None:
    tokens: list[tuple[int, str]] = []
    for row in rows:
        horse_number = _positive_int(row.get(_UMABAN))
        odds = _positive_number(row.get(_ODDS))
        popularity = _positive_int(row.get(_POPULARITY))
        if horse_number is None or odds is None or popularity is None:
            return None
        tokens.append(
            (
                horse_number,
                f"{horse_number}:{_js_number_token(odds)}:{popularity}",
            )
        )
    if len({horse_number for horse_number, _ in tokens}) != len(tokens):
        return None
    return _sha256("\n".join(token for _, token in sorted(tokens)))


def _average(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _expected_market_values(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, float | int]] | None:
    odds: list[float] = []
    popularity: list[int] = []
    odds_scores: list[float] = []
    popularity_scores: list[float] = []
    career_rates: list[float | None] = []
    for row in rows:
        odds_value = _positive_number(row.get(_ODDS))
        popularity_value = _positive_int(row.get(_POPULARITY))
        odds_score = _finite_number(row.get(_ODDS_SCORE))
        popularity_score = _finite_number(row.get(_POPULARITY_SCORE))
        career_raw = row.get(_CAREER_WIN_RATE)
        career_rate = None if career_raw is None else _finite_number(career_raw)
        if (
            odds_value is None
            or popularity_value is None
            or odds_score is None
            or popularity_score is None
            or (career_raw is not None and career_rate is None)
        ):
            return None
        odds.append(odds_value)
        popularity.append(popularity_value)
        odds_scores.append(odds_score)
        popularity_scores.append(popularity_score)
        career_rates.append(career_rate)
    runner_count = len(rows)
    expected_odds_scores = [
        min(1.0, max(0.0, math.log(max(value, 1.0)) / math.log(300.0))) for value in odds
    ]
    expected_popularity_scores = [
        0.5 if runner_count <= 1 else min(1.0, max(0.0, (value - 1) / (runner_count - 1)))
        for value in popularity
    ]
    if odds_scores != expected_odds_scores or popularity_scores != expected_popularity_scores:
        return None
    implied = [1 / value for value in odds]
    total_implied = sum(implied)
    odds_score_average = _average(odds_scores)
    popularity_score_average = _average(popularity_scores)
    expected: list[dict[str, float | int]] = []
    for index, implied_probability in enumerate(implied):
        form_market_edge = career_rates[index]
        values: dict[str, float | int] = {
            "tansho_odds_raw": odds[index],
            "tansho_ninkijun_raw": popularity[index],
            "inverse_odds_implied_prob": implied_probability,
            "inverse_odds_market_share": implied_probability / total_implied,
            "inverse_odds_rank_in_race": 1
            + sum(candidate > implied_probability for candidate in implied),
            "popularity_rank_in_race": 1
            + sum(candidate < popularity[index] for candidate in popularity),
            "odds_score_diff_from_race_avg": odds_scores[index] - odds_score_average,
            "popularity_score_diff_from_race_avg": (
                popularity_scores[index] - popularity_score_average
            ),
            "popularity_odds_disagreement": abs(popularity_scores[index] - odds_scores[index]),
        }
        if form_market_edge is not None:
            values["form_market_edge"] = form_market_edge - implied_probability
        expected.append(values)
    return expected


def _same_market_value(actual: object, expected: float | int | None) -> bool:
    if expected is None:
        return actual is None
    if isinstance(expected, int):
        return isinstance(actual, int) and not isinstance(actual, bool) and actual == expected
    return (
        isinstance(actual, (float, int))
        and not isinstance(actual, bool)
        and math.isfinite(float(actual))
        and float(actual) == expected
    )


def _rows_match(
    value: object,
    *,
    expected_race_id: str,
    output_names: tuple[str, ...],
    evidence: MarketSignalBaseEvidence,
) -> tuple[FoundationRow, ...] | None:
    if not isinstance(value, list) or len(value) != len(evidence.base_rows):
        return None
    output_name_set = frozenset(output_names)
    rows: list[FoundationRow] = []
    for raw_row in value:
        if not isinstance(raw_row, dict) or frozenset(raw_row) != output_name_set:
            return None
        row: FoundationRow = {}
        for name, cell in raw_row.items():
            if not isinstance(name, str) or (
                cell is not None and not isinstance(cell, (bool, float, int, str))
            ):
                return None
            if isinstance(cell, float) and not math.isfinite(cell):
                return None
            row[name] = cell
        rows.append(row)
    expected_values = _expected_market_values(rows)
    if expected_values is None:
        return None
    for row, base_row, market_values in zip(rows, evidence.base_rows, expected_values, strict=True):
        if row.get(_RACE_ID) != expected_race_id or _entry_token(row) != _entry_token(base_row):
            return None
        for name in (field.name for field in evidence.base_schema):
            if name not in _CANONICAL_OVERWRITES and row.get(name) != base_row.get(name):
                return None
        for name in MARKET_SIGNAL_ADDED_COLUMNS:
            expected = market_values.get(name)
            if name == "form_market_edge" and name not in market_values:
                expected = None
            if not _same_market_value(row.get(name), expected):
                return None
    return tuple(rows)


def validate_market_signal_foundation(
    *,
    category: Category,
    target_date: str,
    target_race: str,
    artifact_bytes: bytes,
    evidence: MarketSignalBaseEvidence,
    expected_odds_snapshot_hash: str,
    expected_base_generation_id: str,
) -> MarketSignalLoadResult:
    """Validate all source/base/race/schema/snapshot/value attestations."""
    if category != "jra" or len(target_date) != 8:
        return MarketSignalLoadResult("unsupported-request", None)
    parts = target_race.split(":")
    if len(parts) != 2 or not all(part.isascii() and part.isdigit() for part in parts):
        return MarketSignalLoadResult("unsupported-request", None)
    venue, race_number = parts
    expected_race_id = (
        f"jra:{target_date[:4]}:{target_date[4:]}:{venue.zfill(2)}:{race_number.zfill(2)}"
    )
    artifact = _json_object(artifact_bytes)
    if artifact is None:
        return MarketSignalLoadResult("invalid-json", None)
    contract = _mapping(artifact, "contract")
    if contract is None:
        return MarketSignalLoadResult("invalid-contract", None)
    if contract.get("contractVersion") != MARKET_SIGNAL_CONTRACT_VERSION:
        return MarketSignalLoadResult("contract-version-mismatch", None)
    if contract.get("schemaVersion") != SCHEMA_VERSION:
        return MarketSignalLoadResult("schema-version-mismatch", None)
    if not _identity_matches(
        artifact.get("source"),
        expected_key=evidence.source_key,
        expected_identity=evidence.source_identity,
    ):
        return MarketSignalLoadResult("source-identity-mismatch", None)
    if not _base_identity_matches(artifact.get("base"), evidence):
        return MarketSignalLoadResult("base-identity-mismatch", None)
    input_names = tuple(field.name for field in evidence.base_schema)
    if len(input_names) + len(MARKET_SIGNAL_ADDED_COLUMNS) > _MAX_FEATURES or any(
        name in input_names for name in MARKET_SIGNAL_ADDED_COLUMNS
    ):
        return MarketSignalLoadResult("schema-collision", None)
    output_names = input_names + MARKET_SIGNAL_ADDED_COLUMNS
    added_columns = contract.get("addedColumns")
    if not isinstance(added_columns, list) or tuple(added_columns) != MARKET_SIGNAL_ADDED_COLUMNS:
        return MarketSignalLoadResult("added-columns-mismatch", None)
    input_feature_hash = _sha256("\n".join(input_names))
    output_feature_hash = _sha256("\n".join(output_names))
    row_count = _positive_integer(contract, "rowCount", _MAX_ROWS)
    if (
        contract.get("raceId") != expected_race_id
        or contract.get("entrySetHash") != evidence.entry_set_hash
        or expected_base_generation_id != evidence.generation_id
        or contract.get("baseGenerationId") != expected_base_generation_id
        or contract.get("inputFeatureHash") != input_feature_hash
        or contract.get("outputFeatureHash") != output_feature_hash
        or row_count != len(evidence.base_rows)
    ):
        return MarketSignalLoadResult("race-contract-mismatch", None)
    rows = _rows_match(
        artifact.get("rows"),
        expected_race_id=expected_race_id,
        output_names=output_names,
        evidence=evidence,
    )
    if rows is None:
        return MarketSignalLoadResult("row-value-mismatch", None)
    tokens = [_entry_token(row) for row in rows]
    if (
        any(token is None for token in tokens)
        or _sha256("\n".join(sorted(token for token in tokens if token is not None)))
        != evidence.entry_set_hash
    ):
        return MarketSignalLoadResult("entry-set-mismatch", None)
    snapshot_hash = _snapshot_hash(rows)
    if (
        snapshot_hash is None
        or contract.get("oddsSnapshotHash") != expected_odds_snapshot_hash
        or snapshot_hash != expected_odds_snapshot_hash
    ):
        return MarketSignalLoadResult("odds-snapshot-mismatch", None)
    telemetry = _mapping(artifact, "telemetry")
    if telemetry is None or any(
        (number := _finite_number(telemetry.get(name))) is None or number < 0
        for name in ("workerComputeMs", "totalMs")
    ):
        return MarketSignalLoadResult("invalid-telemetry", None)
    return MarketSignalLoadResult("hit", rows)
