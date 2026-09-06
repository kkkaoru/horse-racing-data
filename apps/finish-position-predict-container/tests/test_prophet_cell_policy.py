from __future__ import annotations

import json
from pathlib import Path

import pytest

from predict_lib.cell_router import load_cell_router
from predict_lib.prophet_cell_policy import (
    load_prophet_cell_policy,
    parse_prophet_cell_policy,
)


def test_parse_policy_resolves_cell_override_and_default() -> None:
    policy = parse_prophet_cell_policy(
        {
            "version": "test-v1",
            "default_enabled": True,
            "default_weight": 0.05,
            "categories": {
                "jra": {"cells": {"bad-cell": {"enabled": False, "weight": 0.1}}},
                "nar": {
                    "cells": {"good-cell": {"enabled": True, "weight": 0.02}},
                    "branches": {
                        "good-cell": {
                            "stage1-model": {"enabled": True, "weight": 0.07},
                            "disabled-model": {"enabled": False, "weight": 0.03},
                        }
                    },
                    "signatures": {
                        "good-cell": {
                            "maximum-path": {"enabled": True, "weight": 0.09},
                            "disabled-path": {"enabled": False, "weight": 0.04},
                        }
                    },
                },
                "ban-ei": {"cells": {}},
            },
        }
    )
    assert policy.version == "test-v1"
    assert policy.resolve("jra", "bad-cell").enabled is False
    assert policy.resolve("jra", "bad-cell").weight == 0.1
    assert policy.resolve("nar", "good-cell").enabled is True
    assert policy.resolve("nar", "good-cell").weight == 0.02
    assert policy.resolve("nar", "good-cell", "stage1-model").weight == 0.07
    assert policy.resolve("nar", "good-cell", "disabled-model").enabled is False
    assert policy.resolve("nar", "good-cell", "unknown-model").weight == 0.02
    assert policy.resolve("nar", "good-cell", "disabled-model", "maximum-path").weight == 0.09
    assert policy.resolve("nar", "good-cell", "stage1-model", "disabled-path").enabled is False
    assert policy.resolve("nar", "good-cell", "stage1-model", "unknown-path").weight == 0.07
    assert policy.resolve("ban-ei", "unknown").enabled is True
    assert policy.resolve("ban-ei", "unknown").weight == 0.05


def test_parse_policy_rejects_invalid_contracts() -> None:
    with pytest.raises(ValueError, match="root must be an object"):
        parse_prophet_cell_policy([])
    with pytest.raises(ValueError, match="version"):
        parse_prophet_cell_policy(
            {
                "version": "",
                "default_enabled": True,
                "default_weight": 0.05,
                "categories": {},
            }
        )
    with pytest.raises(ValueError, match="default_enabled"):
        parse_prophet_cell_policy(
            {
                "version": "v1",
                "default_enabled": "yes",
                "default_weight": 0.05,
                "categories": {},
            }
        )
    with pytest.raises(ValueError, match="default_weight must be numeric"):
        parse_prophet_cell_policy(
            {
                "version": "v1",
                "default_enabled": True,
                "default_weight": True,
                "categories": {},
            }
        )
    with pytest.raises(ValueError, match="must be in"):
        parse_prophet_cell_policy(
            {
                "version": "v1",
                "default_enabled": True,
                "default_weight": 0,
                "categories": {},
            }
        )


def test_parse_policy_rejects_invalid_cell_decision() -> None:
    with pytest.raises(ValueError, match="enabled must be boolean"):
        parse_prophet_cell_policy(
            {
                "version": "v1",
                "default_enabled": True,
                "default_weight": 0.05,
                "categories": {"nar": {"cells": {"bad": {"enabled": 1}}}},
            }
        )
    with pytest.raises(ValueError, match="weight must be numeric"):
        parse_prophet_cell_policy(
            {
                "version": "v1",
                "default_enabled": True,
                "default_weight": 0.05,
                "categories": {"nar": {"cells": {"bad": {"enabled": True, "weight": "0.1"}}}},
            }
        )


def test_baked_policy_covers_every_production_variant() -> None:
    policy = load_prophet_cell_policy()
    router = load_cell_router()
    assert set(policy.cells["jra"]) == set(router.routing_for("jra").variants)
    assert set(policy.cells["nar"]) == set(router.routing_for("nar").variants)
    assert set(policy.cells["ban-ei"]) == set(router.routing_for("ban-ei").variants)
    assert set(policy.branches["jra"]) <= set(policy.cells["jra"])
    assert set(policy.branches["nar"]) <= set(policy.cells["nar"])
    assert set(policy.branches["ban-ei"]) <= set(policy.cells["ban-ei"])
    assert sum(len(branches) for branches in policy.branches["jra"].values()) == 61
    assert sum(len(branches) for branches in policy.branches["nar"].values()) == 12
    assert sum(len(branches) for branches in policy.branches["ban-ei"].values()) == 2
    assert sum(len(signatures) for signatures in policy.signatures["jra"].values()) == 96
    assert sum(len(signatures) for signatures in policy.signatures["nar"].values()) == 23
    assert sum(len(signatures) for signatures in policy.signatures["ban-ei"].values()) == 2
    assert policy.version == "prophet-maximum-branch-weight-policy-2024-2026-v5"
    assert policy.resolve("jra", "sim").enabled is True
    assert policy.resolve("jra", "sim").weight == pytest.approx(0.03283382535396677)
    assert policy.resolve("nar", "sim").enabled is True
    assert policy.resolve("nar", "sim").weight == pytest.approx(0.00022546418631325156)
    assert policy.resolve("ban-ei", "base").enabled is True
    assert policy.resolve("ban-ei", "base").weight == pytest.approx(0.08656703186270287)
    assert policy.resolve("ban-ei", "sim").enabled is True
    assert policy.resolve("ban-ei", "sim").weight == pytest.approx(0.0025431311448551354)
    assert policy.resolve("nar", "c42_tc1").enabled is False
    c42_branch = "nar-cell-top1-42-c-sprint-summer-tc1-v1"
    assert policy.resolve("nar", "c42_tc1", c42_branch).enabled is True
    assert policy.resolve("nar", "c42_tc1", c42_branch).weight == pytest.approx(0.2025017432564682)
    assert policy.resolve("jra", "sim", "jra-cb-stage1-marketfree235-2013").enabled is False
    assert policy.resolve("ban-ei", "base", "banei-cb-v9-sim-2011").weight == pytest.approx(
        0.0662293792416862
    )
    gated_cell = "joken_005_dirt_1200_winter_summer_qsm_gated_top1"
    gated_branch = "jra-joken-005-dirt-1200-winter-summer-qsm-gated-v1"
    kept_signature = (
        "v1;mode=jra_variant_top1_swap;stage2=confidence-gate-kept-base;"
        f"stage2-model={gated_branch};stage1=fresh;final={gated_branch}"
    )
    swapped_signature = (
        "v1;mode=jra_variant_top1_swap;stage2=confidence-gate-swap;"
        f"stage2-model={gated_branch};stage1=fresh;final={gated_branch}"
    )
    assert policy.resolve("jra", gated_cell, gated_branch).enabled is True
    assert policy.resolve("jra", gated_cell, gated_branch).weight == pytest.approx(
        0.02811069064531458
    )
    assert policy.resolve("jra", gated_cell, gated_branch, kept_signature).enabled is True
    assert policy.resolve("jra", gated_cell, gated_branch, kept_signature).weight == pytest.approx(
        0.03533640895046754
    )
    assert policy.resolve("jra", gated_cell, gated_branch, swapped_signature).enabled is False


def test_load_policy_reads_json_and_wraps_io_failures(tmp_path: Path) -> None:
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "version": "loaded-v1",
                "default_enabled": True,
                "default_weight": 0.05,
                "categories": {},
            }
        ),
        encoding="utf-8",
    )
    assert load_prophet_cell_policy(path).version == "loaded-v1"
    with pytest.raises(RuntimeError, match="Unable to load Prophet cell policy"):
        load_prophet_cell_policy(tmp_path / "missing.json")
    path.write_text("{", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Unable to load Prophet cell policy"):
        load_prophet_cell_policy(path)
