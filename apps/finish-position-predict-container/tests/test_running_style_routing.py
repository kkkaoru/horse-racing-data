"""Tests for the tracked running-style cell-routing declaration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from predict_lib.running_style_routing import (
    RUNNING_STYLE_ROUTING_PATH,
    RunningStyleCategoryRouting,
    RunningStyleRoutingValidationError,
    RunningStyleVariant,
    load_running_style_cell_routing,
)


def _write(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "running_style_cell_routing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_tracked_running_style_routing_is_empty_by_default() -> None:
    routing = load_running_style_cell_routing()

    assert routing == {}


def test_running_style_routing_path_points_at_tracked_file() -> None:
    assert RUNNING_STYLE_ROUTING_PATH.name == "running_style_cell_routing.json"
    assert RUNNING_STYLE_ROUTING_PATH.exists()


def test_load_running_style_cell_routing_parses_full_shape(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {
            "jra": {
                "defaultVariantId": "latest",
                "rules": [
                    {
                        "conditions": [{"dimension": "venue", "values": ["05"]}],
                        "variantId": "tokyo-turf",
                    }
                ],
                "variants": {
                    "latest": {"modelKey": "running-style/models/jra/latest.flatbin"},
                    "tokyo-turf": {"modelKey": "running-style/models/jra/cells/tokyo-turf.flatbin"},
                },
            }
        },
    )

    routing = load_running_style_cell_routing(path)

    assert routing == {
        "jra": RunningStyleCategoryRouting(
            default_variant_id="latest",
            rule_variant_ids=("tokyo-turf",),
            variants={
                "latest": RunningStyleVariant(
                    model_key="running-style/models/jra/latest.flatbin"
                ),
                "tokyo-turf": RunningStyleVariant(
                    model_key="running-style/models/jra/cells/tokyo-turf.flatbin"
                ),
            },
        )
    }


def test_load_running_style_cell_routing_defaults_rules_and_variants(tmp_path: Path) -> None:
    path = _write(tmp_path, {"nar": {"defaultVariantId": "latest"}})

    routing = load_running_style_cell_routing(path)

    assert routing == {
        "nar": RunningStyleCategoryRouting(
            default_variant_id="latest",
            rule_variant_ids=(),
            variants={},
        )
    }


def test_reachable_model_keys_unions_default_and_rules() -> None:
    routing = RunningStyleCategoryRouting(
        default_variant_id="latest",
        rule_variant_ids=("tokyo-turf", "latest"),
        variants={
            "latest": RunningStyleVariant(model_key="running-style/models/jra/latest.flatbin"),
            "tokyo-turf": RunningStyleVariant(
                model_key="running-style/models/jra/cells/tokyo-turf.flatbin"
            ),
        },
    )

    assert routing.reachable_model_keys() == frozenset(
        {
            "running-style/models/jra/latest.flatbin",
            "running-style/models/jra/cells/tokyo-turf.flatbin",
        }
    )


def test_reachable_model_keys_rejects_undefined_default_variant() -> None:
    routing = RunningStyleCategoryRouting(
        default_variant_id="missing-variant",
        rule_variant_ids=(),
        variants={},
    )

    with pytest.raises(RunningStyleRoutingValidationError, match="undefined variant ids"):
        routing.reachable_model_keys()


def test_reachable_model_keys_rejects_undefined_rule_variant() -> None:
    routing = RunningStyleCategoryRouting(
        default_variant_id="latest",
        rule_variant_ids=("missing-variant",),
        variants={
            "latest": RunningStyleVariant(model_key="running-style/models/jra/latest.flatbin")
        },
    )

    with pytest.raises(RunningStyleRoutingValidationError, match="undefined variant ids"):
        routing.reachable_model_keys()


def test_load_running_style_cell_routing_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(RunningStyleRoutingValidationError, match="not found"):
        load_running_style_cell_routing(tmp_path / "missing.json")


def test_load_running_style_cell_routing_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "running_style_cell_routing.json"
    path.write_text("{", encoding="utf-8")

    with pytest.raises(RunningStyleRoutingValidationError, match="invalid running_style"):
        load_running_style_cell_routing(path)


def test_load_running_style_cell_routing_rejects_non_object(tmp_path: Path) -> None:
    path = _write(tmp_path, [])

    with pytest.raises(RunningStyleRoutingValidationError, match="must be an object"):
        load_running_style_cell_routing(path)


def test_load_running_style_cell_routing_rejects_unknown_category(tmp_path: Path) -> None:
    path = _write(tmp_path, {"usa": {"defaultVariantId": "latest"}})

    with pytest.raises(RunningStyleRoutingValidationError, match="unsupported categories"):
        load_running_style_cell_routing(path)


def test_load_running_style_cell_routing_rejects_missing_default_variant_id(
    tmp_path: Path,
) -> None:
    path = _write(tmp_path, {"jra": {}})

    with pytest.raises(RunningStyleRoutingValidationError, match="defaultVariantId"):
        load_running_style_cell_routing(path)


def test_load_running_style_cell_routing_rejects_non_array_rules(tmp_path: Path) -> None:
    path = _write(tmp_path, {"jra": {"defaultVariantId": "latest", "rules": {}}})

    with pytest.raises(RunningStyleRoutingValidationError, match=r"rules.*must be an array"):
        load_running_style_cell_routing(path)


def test_load_running_style_cell_routing_rejects_rule_without_variant_id(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {
            "jra": {
                "defaultVariantId": "latest",
                "rules": [{"conditions": []}],
                "variants": {"latest": {"modelKey": "running-style/models/jra/latest.flatbin"}},
            }
        },
    )

    with pytest.raises(RunningStyleRoutingValidationError, match="variantId"):
        load_running_style_cell_routing(path)


def test_load_running_style_cell_routing_rejects_variant_without_model_key(
    tmp_path: Path,
) -> None:
    path = _write(tmp_path, {"jra": {"defaultVariantId": "latest", "variants": {"latest": {}}}})

    with pytest.raises(RunningStyleRoutingValidationError, match="modelKey"):
        load_running_style_cell_routing(path)


def test_load_running_style_cell_routing_supports_ban_ei_category(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {
            "ban-ei": {
                "defaultVariantId": "latest",
                "variants": {"latest": {"modelKey": "running-style/models/ban-ei/latest.flatbin"}},
            }
        },
    )

    routing = load_running_style_cell_routing(path)

    assert routing["ban-ei"].reachable_model_keys() == frozenset(
        {"running-style/models/ban-ei/latest.flatbin"}
    )
