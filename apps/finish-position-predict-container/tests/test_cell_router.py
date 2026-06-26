from __future__ import annotations

import json
from pathlib import Path

import pytest

from predict_lib.cell_router import (
    CategoryRouting,
    CellRouter,
    CellRouteRule,
    base_architecture_for,
    build_base_metadata_r2_key,
    build_base_model_r2_key,
    load_cell_router,
)


def _banei_router() -> CellRouter:
    routing = CategoryRouting(
        sim_model_version="banei-cb-v9-sim-2011",
        base_model_version="banei-cb-v8-window2011-wf-15y",
        base_feature_count=111,
        default_variant="sim",
        rules=(CellRouteRule(dimension="grade_code", values=frozenset({"E"}), variant="base"),),
    )
    return CellRouter(routing={"ban-ei": routing})


def test_has_routing_true_for_configured_category() -> None:
    router = _banei_router()
    assert router.has_routing("ban-ei") is True


def test_has_routing_false_for_jra() -> None:
    router = _banei_router()
    assert router.has_routing("jra") is False


def test_has_routing_false_for_nar() -> None:
    router = _banei_router()
    assert router.has_routing("nar") is False


def test_routing_for_returns_category_routing() -> None:
    router = _banei_router()
    routing = router.routing_for("ban-ei")
    assert isinstance(routing, CategoryRouting)
    assert routing.sim_model_version == "banei-cb-v9-sim-2011"
    assert routing.base_model_version == "banei-cb-v8-window2011-wf-15y"
    assert routing.base_feature_count == 111
    assert routing.default_variant == "sim"


def test_resolve_variant_grade_e_matches_base() -> None:
    router = _banei_router()
    entries = [{"grade_code": "E"}]
    assert router.resolve_variant("ban-ei", entries) == "base"


def test_resolve_variant_grade_none_returns_default_sim() -> None:
    router = _banei_router()
    entries = [{"grade_code": None}]
    assert router.resolve_variant("ban-ei", entries) == "sim"


def test_resolve_variant_grade_empty_string_returns_default_sim() -> None:
    router = _banei_router()
    entries = [{"grade_code": ""}]
    assert router.resolve_variant("ban-ei", entries) == "sim"


def test_resolve_variant_grade_non_matching_value_returns_default_sim() -> None:
    router = _banei_router()
    entries = [{"grade_code": "A"}]
    assert router.resolve_variant("ban-ei", entries) == "sim"


def test_resolve_variant_empty_entries_returns_default_sim() -> None:
    router = _banei_router()
    assert router.resolve_variant("ban-ei", []) == "sim"


def test_resolve_variant_unconfigured_category_returns_sim() -> None:
    router = _banei_router()
    entries = [{"grade_code": "E"}]
    assert router.resolve_variant("jra", entries) == "sim"


def test_load_cell_router_real_config_has_ban_ei_routing() -> None:
    router = load_cell_router()
    assert router.has_routing("ban-ei") is True
    routing = router.routing_for("ban-ei")
    assert routing.sim_model_version == "banei-cb-v9-sim-2011"
    assert routing.base_model_version == "banei-cb-v8-window2011-wf-15y"
    assert routing.base_feature_count == 111
    assert routing.default_variant == "sim"
    assert router.resolve_variant("ban-ei", [{"grade_code": "E"}]) == "base"


def test_load_cell_router_missing_file_returns_empty_router(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.json"
    router = load_cell_router(missing)
    assert router.has_routing("ban-ei") is False
    assert router.has_routing("jra") is False
    assert router.has_routing("nar") is False


def test_load_cell_router_custom_path(tmp_path: Path) -> None:
    config = {
        "ban-ei": {
            "sim_model_version": "banei-cb-v9-sim-2011",
            "base_model_version": "banei-cb-v8-window2011-wf-15y",
            "base_feature_count": 111,
            "default_variant": "sim",
            "rules": [{"dimension": "grade_code", "values": ["E"], "variant": "base"}],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    router = load_cell_router(config_path)
    assert router.has_routing("ban-ei") is True
    assert router.resolve_variant("ban-ei", [{"grade_code": "E"}]) == "base"
    assert router.resolve_variant("ban-ei", [{"grade_code": "A"}]) == "sim"


def test_build_base_model_r2_key() -> None:
    key = build_base_model_r2_key("ban-ei", "banei-cb-v8-window2011-wf-15y", "model.json")
    assert key == "finish-position/ban-ei/banei-cb-v8-window2011-wf-15y/model.json"


def test_resolve_variant_first_matching_rule_wins(tmp_path: Path) -> None:
    config = {
        "ban-ei": {
            "sim_model_version": "banei-cb-v9-sim-2011",
            "base_model_version": "banei-cb-v8-window2011-wf-15y",
            "base_feature_count": 111,
            "default_variant": "sim",
            "rules": [
                {"dimension": "grade_code", "values": ["E"], "variant": "base"},
                {"dimension": "grade_code", "values": ["E"], "variant": "sim"},
            ],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    router = load_cell_router(config_path)
    assert router.resolve_variant("ban-ei", [{"grade_code": "E"}]) == "base"


def test_load_cell_router_rejects_non_object_root(tmp_path: Path) -> None:
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with pytest.raises(ValueError, match="'root' must be an object"):
        load_cell_router(config_path)


def test_load_cell_router_rejects_non_object_entry(tmp_path: Path) -> None:
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps({"ban-ei": "nope"}), encoding="utf-8")
    with pytest.raises(ValueError, match="'ban-ei' must be an object"):
        load_cell_router(config_path)


def test_load_cell_router_rejects_non_array_rules(tmp_path: Path) -> None:
    config = {
        "ban-ei": {
            "sim_model_version": "s",
            "base_model_version": "b",
            "base_feature_count": 111,
            "default_variant": "sim",
            "rules": "nope",
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError, match="'rules' must be an array"):
        load_cell_router(config_path)


def test_base_architecture_for_ban_ei() -> None:
    assert base_architecture_for("ban-ei") == "catboost"


def test_build_base_metadata_r2_key() -> None:
    key = build_base_metadata_r2_key("ban-ei", "banei-cb-v8-window2011-wf-15y")
    assert key == "finish-position/ban-ei/banei-cb-v8-window2011-wf-15y/metadata.json"
