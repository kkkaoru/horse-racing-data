from __future__ import annotations

import pytest

from predict_lib.served_branch import ServedBranch


def test_signature_serializes_every_routing_dimension() -> None:
    branch = ServedBranch(
        routing_mode="nar_transformer_top2_consensus_swap",
        stage2_outcome="transformer-fused-swap-votes-2",
        stage2_model_version="nar-cell-consensus-v1",
        stage1_reason="fresh",
        final_model_version="nar-cell-consensus-v1",
    )

    assert branch.signature() == (
        "v1;mode=nar_transformer_top2_consensus_swap;"
        "stage2=transformer-fused-swap-votes-2;"
        "stage2-model=nar-cell-consensus-v1;stage1=fresh;final=nar-cell-consensus-v1"
    )


def test_signature_rejects_empty_or_ambiguous_tokens() -> None:
    empty = ServedBranch("direct", "direct", "model", "fresh", "")
    separator = ServedBranch("direct", "bad;token", "model", "fresh", "model")
    assignment = ServedBranch("direct", "bad=token", "model", "fresh", "model")

    with pytest.raises(ValueError, match="final contains an invalid token"):
        empty.signature()
    with pytest.raises(ValueError, match="stage2 contains an invalid token"):
        separator.signature()
    with pytest.raises(ValueError, match="stage2 contains an invalid token"):
        assignment.signature()
