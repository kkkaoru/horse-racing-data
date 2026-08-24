from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import replace

import pytest

from predict_lib.foundation_cache import FoundationFeatureField, FoundationLoadResult
from predict_lib.market_signal_foundation import (
    MARKET_SIGNAL_ADDED_COLUMNS,
    MARKET_SIGNAL_FOUNDATION_MAX_BYTES,
    MarketSignalBaseEvidence,
    build_market_signal_base_evidence,
    build_market_signal_foundation_key,
    market_signal_foundation_enabled,
    validate_market_signal_foundation,
)
from predict_lib.model_meta import Category
from predict_lib.r2_client import R2ObjectIdentity

Cell = bool | float | int | str | None


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _base_rows() -> tuple[dict[str, Cell], ...]:
    odds_a = 2.0
    odds_b = 4.0
    return (
        {
            "race_id": "jra:2026:0824:01:03",
            "ketto_toroku_bango": "horse-a",
            "umaban": 1,
            "tansho_odds": 9.9,
            "tansho_ninkijun": 2,
            "odds_score": math.log(9.9) / math.log(300.0),
            "popularity_score": 1.0,
            "career_win_rate": 0.2,
            "value": 10.0,
            "live_odds": odds_a,
        },
        {
            "race_id": "jra:2026:0824:01:03",
            "ketto_toroku_bango": "horse-b",
            "umaban": 2,
            "tansho_odds": 8.8,
            "tansho_ninkijun": 1,
            "odds_score": math.log(8.8) / math.log(300.0),
            "popularity_score": 0.0,
            "career_win_rate": None,
            "value": 20.0,
            "live_odds": odds_b,
        },
    )


def _schema() -> tuple[FoundationFeatureField, ...]:
    return tuple(
        FoundationFeatureField(None, name, "DOUBLE", None, None, None)
        for name in _base_rows()[0]
        if name != "live_odds"
    )


def _evidence() -> MarketSignalBaseEvidence:
    base_rows = tuple(
        {name: value for name, value in row.items() if name != "live_odds"} for row in _base_rows()
    )
    return MarketSignalBaseEvidence(
        base_rows=base_rows,
        base_schema=_schema(),
        entry_set_hash=_hash("horse-a:1\nhorse-b:2"),
        foundation_identity=R2ObjectIdentity("foundation-etag", "foundation-version"),
        foundation_key=("feat-daybase-race/catalog-v1/jra/20260824/01/03/foundation.json"),
        generation_id="base-generation",
        manifest_identity=R2ObjectIdentity("manifest-etag", "manifest-version"),
        manifest_key="feat-daybase-race/catalog-v1/jra/20260824/manifest.json",
        source_identity=R2ObjectIdentity("source-etag", "source-version"),
        source_key="feat-daybase/catalog-v1/jra/20260824/features.parquet",
    )


def _output_rows() -> list[dict[str, Cell]]:
    base = _base_rows()
    odds = [2.0, 4.0]
    popularity = [1, 2]
    odds_scores = [math.log(value) / math.log(300.0) for value in odds]
    popularity_scores = [0.0, 1.0]
    implied = [0.5, 0.25]
    rows: list[dict[str, Cell]] = []
    for index, base_row in enumerate(base):
        row = {name: value for name, value in base_row.items() if name != "live_odds"}
        row.update(
            {
                "tansho_odds": odds[index],
                "tansho_ninkijun": popularity[index],
                "odds_score": odds_scores[index],
                "popularity_score": popularity_scores[index],
                "tansho_odds_raw": odds[index],
                "tansho_ninkijun_raw": popularity[index],
                "inverse_odds_implied_prob": implied[index],
                "inverse_odds_market_share": implied[index] / 0.75,
                "inverse_odds_rank_in_race": index + 1,
                "popularity_rank_in_race": index + 1,
                "odds_score_diff_from_race_avg": odds_scores[index] - sum(odds_scores) / 2,
                "popularity_score_diff_from_race_avg": popularity_scores[index] - 0.5,
                "popularity_odds_disagreement": abs(popularity_scores[index] - odds_scores[index]),
                "form_market_edge": None
                if base_row["career_win_rate"] is None
                else float(base_row["career_win_rate"]) - implied[index],
            }
        )
        rows.append(row)
    return rows


def _artifact(
    *,
    contract_updates: Mapping[str, object] | None = None,
    envelope_updates: Mapping[str, object] | None = None,
    row_updates: Mapping[str, Cell] | None = None,
) -> bytes:
    evidence = _evidence()
    input_names = [field.name for field in evidence.base_schema]
    rows = _output_rows()
    if row_updates:
        rows[0].update(row_updates)
    contract: dict[str, object] = {
        "addedColumns": list(MARKET_SIGNAL_ADDED_COLUMNS),
        "baseGenerationId": "base-generation",
        "contractVersion": "race-chain-market-signal-foundation-v1",
        "entrySetHash": _hash("horse-a:1\nhorse-b:2"),
        "inputFeatureHash": _hash("\n".join(input_names)),
        "oddsSnapshotHash": _hash("1:2:1\n2:4:2"),
        "outputFeatureHash": _hash("\n".join(input_names + list(MARKET_SIGNAL_ADDED_COLUMNS))),
        "raceId": "jra:2026:0824:01:03",
        "rowCount": 2,
        "schemaVersion": "1",
    }
    if contract_updates:
        contract.update(contract_updates)
    envelope: dict[str, object] = {
        "base": {
            "foundationEtag": "foundation-etag",
            "foundationKey": evidence.foundation_key,
            "foundationVersion": "foundation-version",
            "manifestEtag": "manifest-etag",
            "manifestKey": evidence.manifest_key,
            "manifestVersion": "manifest-version",
        },
        "contract": contract,
        "rows": rows,
        "source": {
            "etag": "source-etag",
            "key": evidence.source_key,
            "version": "source-version",
        },
        "telemetry": {"totalMs": 9.5, "workerComputeMs": 0.5},
    }
    if envelope_updates:
        envelope.update(envelope_updates)
    return json.dumps(envelope).encode()


def test_key_and_flag_contracts_are_exact() -> None:
    assert build_market_signal_foundation_key("jra", "20260824", "1", "3") == (
        "feat-racechain-market-signal/catalog-v1/jra/20260824/01/03/foundation.json"
    )
    assert market_signal_foundation_enabled({"WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED": "1"})
    assert not market_signal_foundation_enabled({})
    assert not market_signal_foundation_enabled({"WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED": "true"})


def test_validate_accepts_fully_attested_equivalent_rows() -> None:
    result = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=_artifact(),
        evidence=_evidence(),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    assert result.reason == "hit"
    assert result.rows is not None
    assert result.rows[0]["tansho_odds_raw"] == 2.0
    assert result.rows[1]["form_market_edge"] is None


@pytest.mark.parametrize(
    "contract_updates,envelope_updates,row_updates,reason",
    [
        ({"contractVersion": "old"}, {}, {}, "contract-version-mismatch"),
        ({"schemaVersion": "2"}, {}, {}, "schema-version-mismatch"),
        ({"baseGenerationId": "stale"}, {}, {}, "race-contract-mismatch"),
        ({"entrySetHash": "wrong"}, {}, {}, "race-contract-mismatch"),
        ({"inputFeatureHash": "wrong"}, {}, {}, "race-contract-mismatch"),
        ({"outputFeatureHash": "wrong"}, {}, {}, "race-contract-mismatch"),
        ({"rowCount": True}, {}, {}, "race-contract-mismatch"),
        ({"oddsSnapshotHash": "wrong"}, {}, {}, "odds-snapshot-mismatch"),
        ({"addedColumns": []}, {}, {}, "added-columns-mismatch"),
        ({}, {"source": {}}, {}, "source-identity-mismatch"),
        ({}, {"source": None}, {}, "source-identity-mismatch"),
        ({}, {"base": {}}, {}, "base-identity-mismatch"),
        ({}, {"base": None}, {}, "base-identity-mismatch"),
        ({}, {"telemetry": {"totalMs": -1, "workerComputeMs": 1}}, {}, "invalid-telemetry"),
        ({}, {"telemetry": None}, {}, "invalid-telemetry"),
        ({}, {}, {"value": 999.0}, "row-value-mismatch"),
        ({}, {}, {"tansho_odds_raw": 9.0}, "row-value-mismatch"),
        ({}, {}, {"odds_score": 0.9}, "row-value-mismatch"),
        ({}, {}, {"ketto_toroku_bango": ""}, "row-value-mismatch"),
        ({}, {}, {"umaban": True}, "row-value-mismatch"),
        ({}, {}, {"tansho_odds": -1.0}, "row-value-mismatch"),
        ({}, {}, {"tansho_ninkijun": 0}, "row-value-mismatch"),
        ({}, {}, {"career_win_rate": "bad"}, "row-value-mismatch"),
        ({}, {}, {"popularity_score": "bad"}, "row-value-mismatch"),
        ({}, {}, {"form_market_edge": float("nan")}, "row-value-mismatch"),
        ({}, {}, {"unexpected": 1}, "row-value-mismatch"),
    ],
)
def test_validate_fails_closed_on_every_attested_boundary(
    contract_updates: Mapping[str, object],
    envelope_updates: Mapping[str, object],
    row_updates: Mapping[str, Cell],
    reason: str,
) -> None:
    result = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=_artifact(
            contract_updates=contract_updates,
            envelope_updates=envelope_updates,
            row_updates=row_updates,
        ),
        evidence=_evidence(),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    assert result.reason == reason
    assert result.rows is None


@pytest.mark.parametrize(
    "category,target_date,target_race",
    [("nar", "20260824", "1:3"), ("jra", "bad", "1:3"), ("jra", "20260824", "bad")],
)
def test_validate_rejects_unsupported_request(
    category: Category, target_date: str, target_race: str
) -> None:
    result = validate_market_signal_foundation(
        category=category,
        target_date=target_date,
        target_race=target_race,
        artifact_bytes=_artifact(),
        evidence=_evidence(),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    assert result.reason == "unsupported-request"


def test_validate_rejects_invalid_or_oversized_json() -> None:
    invalid = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=b"not-json",
        evidence=_evidence(),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    oversized = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=b"x" * (MARKET_SIGNAL_FOUNDATION_MAX_BYTES + 1),
        evidence=_evidence(),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    assert invalid.reason == "invalid-json"
    assert oversized.reason == "invalid-json"


def test_validate_rejects_missing_contract_and_schema_collision() -> None:
    missing_contract = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=b"{}",
        evidence=_evidence(),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    evidence = _evidence()
    colliding_schema = (
        *evidence.base_schema,
        FoundationFeatureField(None, "tansho_odds_raw", "DOUBLE", None, None, None),
    )
    collision = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=_artifact(),
        evidence=replace(evidence, base_schema=colliding_schema),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    assert missing_contract.reason == "invalid-contract"
    assert collision.reason == "schema-collision"


def test_validate_recomputes_entry_hash_after_row_validation() -> None:
    wrong_hash = _hash("different-entry-set")
    result = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=_artifact(contract_updates={"entrySetHash": wrong_hash}),
        evidence=replace(_evidence(), entry_set_hash=wrong_hash),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="base-generation",
    )
    assert result.reason == "entry-set-mismatch"


def test_validate_rejects_stale_request_attestation() -> None:
    stale_snapshot = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=_artifact(),
        evidence=_evidence(),
        expected_odds_snapshot_hash="0" * 64,
        expected_base_generation_id="base-generation",
    )
    stale_generation = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        artifact_bytes=_artifact(),
        evidence=_evidence(),
        expected_odds_snapshot_hash=_hash("1:2:1\n2:4:2"),
        expected_base_generation_id="stale-generation",
    )
    assert stale_snapshot.reason == "odds-snapshot-mismatch"
    assert stale_generation.reason == "race-contract-mismatch"


def test_build_base_evidence_binds_validated_contract_and_identities() -> None:
    evidence = _evidence()
    names = [field.name for field in evidence.base_schema]
    manifest = json.dumps(
        {
            "contract": {
                "featureHash": _hash("\n".join(names)),
                "generationId": "base-generation",
            }
        }
    ).encode()
    foundation = json.dumps(
        {
            "contract": {
                "entrySetHash": evidence.entry_set_hash,
                "featureHash": _hash("\n".join(names)),
                "generationId": "base-generation",
            }
        }
    ).encode()
    result = build_market_signal_base_evidence(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        manifest_bytes=manifest,
        foundation_bytes=foundation,
        foundation_result=FoundationLoadResult("hit", evidence.base_rows, evidence.base_schema),
        source_identity=evidence.source_identity,
        foundation_identity=evidence.foundation_identity,
        manifest_identity=evidence.manifest_identity,
    )
    assert result == evidence


def test_build_base_evidence_rejects_unvalidated_or_drifted_base() -> None:
    evidence = _evidence()
    missing = build_market_signal_base_evidence(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        manifest_bytes=b"{}",
        foundation_bytes=b"{}",
        foundation_result=FoundationLoadResult("miss", None, None),
        source_identity=evidence.source_identity,
        foundation_identity=evidence.foundation_identity,
        manifest_identity=evidence.manifest_identity,
    )
    drifted = build_market_signal_base_evidence(
        category="jra",
        target_date="20260824",
        target_race="1:3",
        manifest_bytes=json.dumps(
            {"contract": {"featureHash": "wrong", "generationId": "generation"}}
        ).encode(),
        foundation_bytes=json.dumps(
            {
                "contract": {
                    "entrySetHash": evidence.entry_set_hash,
                    "featureHash": "wrong",
                    "generationId": "generation",
                }
            }
        ).encode(),
        foundation_result=FoundationLoadResult("hit", evidence.base_rows, evidence.base_schema),
        source_identity=evidence.source_identity,
        foundation_identity=evidence.foundation_identity,
        manifest_identity=evidence.manifest_identity,
    )
    assert missing is None
    assert drifted is None


@pytest.mark.parametrize(
    "target_race,manifest_bytes,foundation_bytes",
    [
        ("bad", b"{}", b"{}"),
        ("1:3", b"not-json", b"{}"),
        ("1:3", b"{}", b"{}"),
    ],
)
def test_build_base_evidence_rejects_malformed_contract_boundaries(
    target_race: str, manifest_bytes: bytes, foundation_bytes: bytes
) -> None:
    evidence = _evidence()
    result = build_market_signal_base_evidence(
        category="jra",
        target_date="20260824",
        target_race=target_race,
        manifest_bytes=manifest_bytes,
        foundation_bytes=foundation_bytes,
        foundation_result=FoundationLoadResult("hit", evidence.base_rows, evidence.base_schema),
        source_identity=evidence.source_identity,
        foundation_identity=evidence.foundation_identity,
        manifest_identity=evidence.manifest_identity,
    )
    assert result is None
