from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd

from catboost_adapter import load_catboost_booster
from predict_lib.foundation_cache import FoundationFeatureField, FoundationRow
from predict_lib.market_signal_foundation import (
    MARKET_SIGNAL_ADDED_COLUMNS,
    MarketSignalBaseEvidence,
    validate_market_signal_foundation,
)
from predict_lib.r2_client import R2ObjectIdentity
from predict_lib.scorer import build_feature_matrix, score_matrix

APP_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = APP_DIR.parent.parent
LEGACY_SCRIPT = (
    REPO_ROOT
    / "apps/pc-keiba-viewer/src/scripts/finish-position-features/add-market-signal-features.py"
)
WORKER_MODULE = REPO_ROOT / "apps/finish-position-cron/src/race-chain-market-signal.ts"
MODEL_DIR = APP_DIR / "models/finish-position/jra/iter14-jra-cb-pacestyle-course-v8"
RACE_ID = "jra:2026:0824:01:03"


def _foundation_row(value: object) -> FoundationRow:
    if not isinstance(value, dict) or any(not isinstance(name, str) for name in value):
        raise TypeError("market-signal row must be an object with string keys")
    if any(
        cell is not None and not isinstance(cell, (bool, float, int, str))
        for cell in value.values()
    ):
        raise TypeError("market-signal row contains an unsupported cell")
    return {
        name: cell
        for name, cell in value.items()
        if isinstance(name, str) and (cell is None or isinstance(cell, (bool, float, int, str)))
    }


def _foundation_rows(value: object) -> list[FoundationRow]:
    if not isinstance(value, list):
        raise TypeError("market-signal output must be a row list")
    return [_foundation_row(row) for row in value]


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _base_rows() -> list[FoundationRow]:
    return [
        {
            "source": "jra",
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0824",
            "keibajo_code": "01",
            "race_bango": "03",
            "race_year": 2026,
            "race_id": RACE_ID,
            "ketto_toroku_bango": "horse-a",
            "umaban": 1,
            "tansho_odds": 2.0,
            "tansho_ninkijun": 1,
            "odds_score": 0.12152412607595545,
            "popularity_score": 0.0,
            "career_win_rate": 0.2,
        },
        {
            "source": "jra",
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0824",
            "keibajo_code": "01",
            "race_bango": "03",
            "race_year": 2026,
            "race_id": RACE_ID,
            "ketto_toroku_bango": "horse-b",
            "umaban": 2,
            "tansho_odds": 4.0,
            "tansho_ninkijun": 2,
            "odds_score": 0.2430482521519109,
            "popularity_score": 1.0,
            "career_win_rate": None,
        },
    ]


def _worker_rows(
    base_rows: list[FoundationRow],
) -> list[FoundationRow]:
    program = f"""
import {{ materializeRaceMarketSignals }} from {json.dumps(str(WORKER_MODULE))};
const rows = JSON.parse(await Bun.stdin.text());
const odds = new Map(rows.map((row) => [row.umaban, {{
  tanshoOdds: row.tansho_odds,
  tanshoNinkijun: row.tansho_ninkijun,
}}]));
const result = materializeRaceMarketSignals({{
  liveOddsByHorseNumber: odds,
  raceId: {json.dumps(RACE_ID)},
  rows,
}});
if (result.status !== "ready") throw new Error(result.reason);
process.stdout.write(JSON.stringify(result.rows));
"""
    completed = subprocess.run(
        ["bun", "-e", program],
        cwd=REPO_ROOT,
        input=json.dumps(base_rows),
        capture_output=True,
        check=True,
        text=True,
    )
    return _foundation_rows(json.loads(completed.stdout))


def _legacy_rows(base_rows: list[FoundationRow], tmp_path: Path) -> list[FoundationRow]:
    input_dir = tmp_path / "legacy-input"
    input_partition = input_dir / "race_year=2026"
    output_dir = tmp_path / "legacy-output"
    input_partition.mkdir(parents=True)
    pd.DataFrame(base_rows).to_parquet(input_partition / "features.parquet", index=False)
    subprocess.run(
        [
            sys.executable,
            str(LEGACY_SCRIPT),
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--target-race",
            "01:03",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
        text=True,
    )
    frame = pd.read_parquet(output_dir).sort_values("umaban")
    return _foundation_rows(frame.to_dict(orient="records"))


def _attested_worker_rows(
    base_rows: list[FoundationRow],
    worker_rows: list[FoundationRow],
    artifact_path: Path,
) -> tuple[FoundationRow, ...]:
    input_names = tuple(base_rows[0])
    entry_hash = _sha256("horse-a:1\nhorse-b:2")
    schema = tuple(
        FoundationFeatureField(None, name, "DOUBLE", None, None, None) for name in input_names
    )
    evidence = MarketSignalBaseEvidence(
        base_rows=tuple(dict(row) for row in base_rows),
        base_schema=schema,
        entry_set_hash=entry_hash,
        foundation_identity=R2ObjectIdentity("foundation-etag", "foundation-version"),
        foundation_key="feat-daybase-race/catalog-v1/jra/20260824/01/03/foundation.json",
        generation_id="generation",
        manifest_identity=R2ObjectIdentity("manifest-etag", "manifest-version"),
        manifest_key="feat-daybase-race/catalog-v1/jra/20260824/manifest.json",
        source_identity=R2ObjectIdentity("source-etag", "source-version"),
        source_key="feat-daybase/catalog-v1/jra/20260824/features.parquet",
    )
    artifact = {
        "base": {
            "foundationEtag": "foundation-etag",
            "foundationKey": evidence.foundation_key,
            "foundationVersion": "foundation-version",
            "manifestEtag": "manifest-etag",
            "manifestKey": evidence.manifest_key,
            "manifestVersion": "manifest-version",
        },
        "contract": {
            "addedColumns": list(MARKET_SIGNAL_ADDED_COLUMNS),
            "baseGenerationId": "generation",
            "contractVersion": "race-chain-market-signal-foundation-v1",
            "entrySetHash": entry_hash,
            "inputFeatureHash": _sha256("\n".join(input_names)),
            "oddsSnapshotHash": _sha256("1:2:1\n2:4:2"),
            "outputFeatureHash": _sha256("\n".join((*input_names, *MARKET_SIGNAL_ADDED_COLUMNS))),
            "raceId": RACE_ID,
            "rowCount": 2,
            "schemaVersion": "1",
        },
        "rows": worker_rows,
        "source": {
            "etag": "source-etag",
            "key": evidence.source_key,
            "version": "source-version",
        },
        "telemetry": {"totalMs": 1.0, "workerComputeMs": 0.1},
    }
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")
    result = validate_market_signal_foundation(
        category="jra",
        target_date="20260824",
        target_race="01:03",
        artifact_bytes=artifact_path.read_bytes(),
        evidence=evidence,
        expected_odds_snapshot_hash=_sha256("1:2:1\n2:4:2"),
        expected_base_generation_id="generation",
    )
    if result.rows is None:
        raise ValueError(f"Worker artifact did not validate: {result.reason}")
    return result.rows


def test_worker_foundation_matches_legacy_layer_and_production_model(tmp_path: Path) -> None:
    base_rows = _base_rows()
    worker_start = perf_counter()
    worker_rows = _worker_rows(base_rows)
    worker_compute_seconds = perf_counter() - worker_start
    consumer_start = perf_counter()
    attested_rows = _attested_worker_rows(
        base_rows, worker_rows, tmp_path / "market-foundation.json"
    )
    consumer_fetch_validate_seconds = perf_counter() - consumer_start
    legacy_start = perf_counter()
    legacy_rows = _legacy_rows(base_rows, tmp_path)
    legacy_layer_seconds = perf_counter() - legacy_start
    market_columns = list(MARKET_SIGNAL_ADDED_COLUMNS)
    worker_market = pd.DataFrame(attested_rows).sort_values("umaban")[market_columns]
    legacy_market = pd.DataFrame(legacy_rows).sort_values("umaban")[market_columns]
    pd.testing.assert_frame_equal(
        worker_market.reset_index(drop=True),
        legacy_market.reset_index(drop=True),
        check_exact=True,
        check_dtype=False,
    )

    metadata = json.loads((MODEL_DIR / "metadata.json").read_text(encoding="utf-8"))
    feature_names = metadata["feature_names"]
    worker_scoring_rows = pd.DataFrame(attested_rows).to_dict(orient="records")
    worker_matrix = build_feature_matrix(worker_scoring_rows, feature_names, "catboost")
    legacy_matrix = build_feature_matrix(legacy_rows, feature_names, "catboost")
    np.testing.assert_array_equal(worker_matrix, legacy_matrix)

    booster = load_catboost_booster(str(MODEL_DIR / "model.json"))
    worker_scores = score_matrix(booster, worker_matrix)
    legacy_scores = score_matrix(booster, legacy_matrix)
    assert worker_scores == legacy_scores
    print(
        "market-signal-benchmark="
        + json.dumps(
            {
                "consumer_local_fetch_validate_ms": round(
                    consumer_fetch_validate_seconds * 1000, 3
                ),
                "legacy_python_layer_ms": round(legacy_layer_seconds * 1000, 3),
                "worker_bun_cold_process_ms": round(worker_compute_seconds * 1000, 3),
            },
            sort_keys=True,
        )
    )
