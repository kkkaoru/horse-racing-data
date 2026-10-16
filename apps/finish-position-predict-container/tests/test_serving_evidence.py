"""Frozen response provenance stays paired with the scored bytes, without I/O."""

from __future__ import annotations

import binascii

import pytest

from predict_lib.serving_evidence import ServingEvidence, freeze_rescore_payloads


def test_frozen_nar_payload_attaches_decision_and_exact_byte_digest() -> None:
    payload = {
        "parquetKey": "feat-cache/catalog-v1/nar/20260926/54/01/features.parquet",
        "parquetBase64": "YQ==",
    }
    result = freeze_rescore_payloads(
        payloads=[payload],
        category="nar",
        run_date="20260926",
        evidence_by_race={
            "nar:2026:0926:54:01": ServingEvidence(
                served_signature="actual-signature",
                cell_variant="sim",
                adjustment="prophet",
                prophet_weight=0.5,
            )
        },
    )
    payload["parquetBase64"] = "Yg=="
    assert result == [
        {
            "parquetKey": "feat-cache/catalog-v1/nar/20260926/54/01/features.parquet",
            "parquetBase64": "YQ==",
            "servedSignature": "actual-signature",
            "servedCellVariant": "sim",
            "scoreAdjustment": "prophet",
            "prophetWeight": "0.5",
            "scoreInputSha256": "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
        }
    ]


def test_unknown_or_out_of_scope_decisions_never_label_another_payload() -> None:
    assert freeze_rescore_payloads(
        payloads=[
            {
                "parquetKey": "feat-cache/catalog-v1/nar/20260926/54/01/features.parquet",
                "parquetBase64": "YQ==",
            }
        ],
        category="nar",
        run_date="20260926",
        evidence_by_race={
            "bad": ServingEvidence(
                served_signature="bad", cell_variant="sim", adjustment="none", prophet_weight=None
            ),
            "jra:2026:0926:54:01": ServingEvidence(
                served_signature="wrong-source",
                cell_variant="sim",
                adjustment="none",
                prophet_weight=None,
            ),
            "nar:2026:0927:54:01": ServingEvidence(
                served_signature="wrong-day",
                cell_variant="sim",
                adjustment="none",
                prophet_weight=None,
            ),
            "nar:2026:0926:54:02": ServingEvidence(
                served_signature="different-race",
                cell_variant="sim",
                adjustment="none",
                prophet_weight=None,
            ),
        },
    ) == [
        {
            "parquetKey": "feat-cache/catalog-v1/nar/20260926/54/01/features.parquet",
            "parquetBase64": "YQ==",
        }
    ]


def test_jra_payload_records_non_prophet_branch_without_inventing_a_weight() -> None:
    assert freeze_rescore_payloads(
        payloads=[
            {
                "parquetKey": "feat-cache/catalog-v1/jra/20260926/06/01/features.parquet",
                "parquetBase64": "YQ==",
            }
        ],
        category="jra",
        run_date="20260926",
        evidence_by_race={
            "jra:2026:0926:06:01": ServingEvidence(
                served_signature="actual-jra",
                cell_variant="cell",
                adjustment="pytorch-mpx",
                prophet_weight=None,
            )
        },
    ) == [
        {
            "parquetKey": "feat-cache/catalog-v1/jra/20260926/06/01/features.parquet",
            "parquetBase64": "YQ==",
            "servedSignature": "actual-jra",
            "servedCellVariant": "cell",
            "scoreAdjustment": "pytorch-mpx",
            "prophetWeight": "disabled",
            "scoreInputSha256": "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
        }
    ]


def test_colliding_padded_race_ids_cannot_label_a_payload() -> None:
    with pytest.raises(ValueError, match="ambiguous serving evidence"):
        freeze_rescore_payloads(
            payloads=[],
            category="nar",
            run_date="20260926",
            evidence_by_race={
                "nar:2026:0926:54:1": ServingEvidence(
                    served_signature="first",
                    cell_variant="sim",
                    adjustment="none",
                    prophet_weight=None,
                ),
                "nar:2026:0926:54:01": ServingEvidence(
                    served_signature="second",
                    cell_variant="sim",
                    adjustment="none",
                    prophet_weight=None,
                ),
            },
        )


def test_invalid_base64_cannot_acquire_provenance() -> None:
    with pytest.raises(binascii.Error):
        freeze_rescore_payloads(
            payloads=[
                {
                    "parquetKey": "feat-cache/catalog-v1/nar/20260926/54/01/features.parquet",
                    "parquetBase64": "invalid!",
                }
            ],
            category="nar",
            run_date="20260926",
            evidence_by_race={
                "nar:2026:0926:54:01": ServingEvidence(
                    served_signature="actual",
                    cell_variant="sim",
                    adjustment="none",
                    prophet_weight=None,
                )
            },
        )
