"""Read-only provenance for a frozen rescore response, never serving authority."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from .race_id import parse_race_id
from .serve import build_r2_per_race_feat_cache_key

ScoreAdjustment = Literal["pytorch-mpx", "neural", "prophet", "none"]


@dataclass(frozen=True, slots=True)
class ServingEvidence:
    served_signature: str
    cell_variant: str
    adjustment: ScoreAdjustment
    prophet_weight: float | None


def freeze_rescore_payloads(
    *,
    payloads: Sequence[Mapping[str, str]],
    evidence_by_race: Mapping[str, ServingEvidence],
    category: str,
    run_date: str,
) -> list[dict[str, str]]:
    """Copy already-scored payloads while the caller still holds its pipeline lock.

    Unknown/out-of-scope evidence is omitted. No later read from a mutable
    category work directory may be paired with these serving decisions.
    """
    by_key: dict[str, ServingEvidence] = {}
    expected_source = "jra" if category == "jra" else "nar"
    for race_id, evidence in evidence_by_race.items():
        try:
            parts = parse_race_id(race_id)
        except ValueError:
            continue
        if parts.source != expected_source or parts.kaisai_nen + parts.kaisai_tsukihi != run_date:
            continue
        key = build_r2_per_race_feat_cache_key(
            category, run_date, parts.keibajo_code, parts.race_bango
        )
        if key in by_key:
            raise ValueError("ambiguous serving evidence for per-race parquet key")
        by_key[key] = evidence
    return [_freeze_payload(payload, by_key) for payload in payloads]


def _freeze_payload(
    payload: Mapping[str, str], by_key: Mapping[str, ServingEvidence]
) -> dict[str, str]:
    frozen = dict(payload)
    evidence = by_key.get(frozen["parquetKey"])
    if evidence is None:
        return frozen
    data = base64.b64decode(frozen["parquetBase64"], validate=True)
    frozen.update(
        {
            "servedSignature": evidence.served_signature,
            "servedCellVariant": evidence.cell_variant,
            "scoreAdjustment": evidence.adjustment,
            "prophetWeight": "disabled"
            if evidence.prophet_weight is None
            else str(evidence.prophet_weight),
            "scoreInputSha256": hashlib.sha256(data).hexdigest(),
        }
    )
    return frozen
