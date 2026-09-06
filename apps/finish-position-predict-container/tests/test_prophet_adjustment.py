from __future__ import annotations

import math

from predict_lib.prophet_adjustment import (
    PROPHET_ENABLED_ENV,
    PROPHET_WEIGHT_ENV,
    adjust_prediction_rows_with_prophet,
    configured_prophet_weight,
)
from predict_lib.prophet_cell_policy import parse_prophet_cell_policy


def test_configured_prophet_weight_requires_category_and_valid_weight() -> None:
    assert configured_prophet_weight("nar", {}, cell_variant="unlisted") == 0.05
    assert configured_prophet_weight("nar", {PROPHET_ENABLED_ENV: "jra"}) is None
    assert (
        configured_prophet_weight(
            "nar",
            {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "bad"},
            cell_variant="unlisted",
        )
        is None
    )
    assert (
        configured_prophet_weight(
            "nar",
            {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "0"},
            cell_variant="unlisted",
        )
        is None
    )
    assert (
        configured_prophet_weight(
            "nar",
            {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "1.1"},
            cell_variant="unlisted",
        )
        is None
    )
    assert (
        configured_prophet_weight(
            "nar",
            {PROPHET_ENABLED_ENV: "jra, nar", PROPHET_WEIGHT_ENV: "0.05"},
            cell_variant="unlisted",
        )
        == 0.05
    )


def test_adjustment_reranks_with_precomputed_trends() -> None:
    rows = [
        ["model", "nar", "2026", "0903", "30", "01", "h1", 1, 1.0, 1],
        ["model", "nar", "2026", "0903", "30", "01", "h2", 2, 0.99, 2],
        ["model", "nar", "2026", "0903", "30", "01", "h3", 3, 0.98, 3],
    ]
    entries = [
        {
            "ketto_toroku_bango": "h1",
            "prophet_entity_performance_mean": 0.2,
            "prophet_entity_coverage": 3,
        },
        {
            "ketto_toroku_bango": "h2",
            "prophet_entity_performance_mean": 0.9,
            "prophet_entity_coverage": 2,
        },
        {
            "ketto_toroku_bango": "h3",
            "prophet_entity_performance_mean": 0.4,
            "prophet_entity_coverage": 1,
        },
    ]
    result = adjust_prediction_rows_with_prophet(
        rows,
        entries,
        "nar",
        {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "1"},
        cell_variant="unlisted",
    )
    assert result.applied is True
    assert result.reason == "applied"
    assert [row[6] for row in result.rows] == ["h2", "h1", "h3"]
    assert [row[9] for row in result.rows] == [1, 2, 3]
    assert rows[0][8] == 1.0


def test_adjustment_leaves_missing_runner_at_race_mean() -> None:
    rows = [
        ["model", "nar", "2026", "0903", "30", "01", "h1", 1, 2.0, 1],
        ["model", "nar", "2026", "0903", "30", "01", "h2", 2, 1.0, 2],
        ["model", "nar", "2026", "0903", "30", "01", "h3", 3, 0.0, 3],
        ["model", "nar", "2026", "0903", "30", "01", "h4", 4, -1.0, 4],
    ]
    entries = [
        {
            "ketto_toroku_bango": "h1",
            "prophet_entity_performance_mean": 0.8,
            "prophet_entity_coverage": 1,
        },
        {
            "ketto_toroku_bango": "h2",
            "prophet_entity_performance_mean": 0.2,
            "prophet_entity_coverage": 1,
        },
        {"ketto_toroku_bango": "h3"},
        {"ketto_toroku_bango": "h4"},
    ]
    result = adjust_prediction_rows_with_prophet(
        rows,
        entries,
        "nar",
        {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "0.1"},
        cell_variant="unlisted",
    )
    assert result.applied is True
    assert result.rows[2][8] == 0.0
    assert result.rows[3][8] == -1.0


def test_adjustment_fail_safe_reasons() -> None:
    row = ["model", "nar", "2026", "0903", "30", "01", "h1", 1, 1.0, 1]
    entry = {
        "ketto_toroku_bango": "h1",
        "prophet_entity_performance_mean": 0.5,
        "prophet_entity_coverage": 1,
    }
    disabled = adjust_prediction_rows_with_prophet(
        [row], [entry], "nar", {PROPHET_ENABLED_ENV: "off"}
    )
    assert disabled.reason == "disabled"
    mismatch = adjust_prediction_rows_with_prophet(
        [row],
        [],
        "nar",
        {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "0.1"},
        cell_variant="unlisted",
    )
    assert mismatch.reason == "row-entry-mismatch"
    insufficient = adjust_prediction_rows_with_prophet(
        [row],
        [entry],
        "nar",
        {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "0.1"},
        cell_variant="unlisted",
    )
    assert insufficient.reason == "insufficient-coverage"


def test_cell_policy_enables_good_cell_and_disables_bad_cell() -> None:
    policy = parse_prophet_cell_policy(
        {
            "version": "test-v1",
            "default_enabled": True,
            "default_weight": 0.05,
            "categories": {
                "nar": {
                    "cells": {
                        "good": {"enabled": True, "weight": 0.02},
                        "bad": {"enabled": False, "weight": 0.02},
                    },
                    "branches": {
                        "good": {
                            "stage1": {"enabled": True, "weight": 0.07},
                            "disabled": {"enabled": False, "weight": 0.03},
                        }
                    },
                    "signatures": {
                        "good": {
                            "swapped": {"enabled": True, "weight": 0.09},
                            "kept": {"enabled": False, "weight": 0.04},
                        }
                    },
                }
            },
        }
    )
    assert configured_prophet_weight("nar", {}, cell_variant="good", policy=policy) == 0.02
    assert configured_prophet_weight("nar", {}, cell_variant="bad", policy=policy) is None
    assert (
        configured_prophet_weight(
            "nar", {}, cell_variant="good", branch_variant="stage1", policy=policy
        )
        == 0.07
    )
    assert (
        configured_prophet_weight(
            "nar", {}, cell_variant="good", branch_variant="disabled", policy=policy
        )
        is None
    )
    assert (
        configured_prophet_weight(
            "nar",
            {},
            cell_variant="good",
            branch_variant="disabled",
            served_signature="swapped",
            policy=policy,
        )
        == 0.09
    )
    assert (
        configured_prophet_weight(
            "nar",
            {},
            cell_variant="good",
            branch_variant="stage1",
            served_signature="kept",
            policy=policy,
        )
        is None
    )
    assert configured_prophet_weight("nar", {}, cell_variant="unknown", policy=policy) == 0.05


def test_adjustment_rejects_invalid_and_degenerate_values() -> None:
    base_environment = {PROPHET_ENABLED_ENV: "nar", PROPHET_WEIGHT_ENV: "0.1"}
    entries = [
        {
            "ketto_toroku_bango": "h1",
            "prophet_entity_performance_mean": 0.5,
            "prophet_entity_coverage": 1,
        },
        {
            "ketto_toroku_bango": "h2",
            "prophet_entity_performance_mean": 0.5,
            "prophet_entity_coverage": 1,
        },
    ]
    invalid_rows = [
        ["model", "nar", "2026", "0903", "30", "01", "h1", 1, math.nan, 1],
        ["model", "nar", "2026", "0903", "30", "01", "h2", 2, 0.0, 2],
    ]
    invalid = adjust_prediction_rows_with_prophet(
        invalid_rows, entries, "nar", base_environment, cell_variant="unlisted"
    )
    assert invalid.reason == "invalid-score"
    same_score_rows = [
        ["model", "nar", "2026", "0903", "30", "01", "h1", 1, 1.0, 1],
        ["model", "nar", "2026", "0903", "30", "01", "h2", 2, 1.0, 2],
    ]
    degenerate = adjust_prediction_rows_with_prophet(
        same_score_rows, entries, "nar", base_environment, cell_variant="unlisted"
    )
    assert degenerate.reason == "degenerate-spread"
