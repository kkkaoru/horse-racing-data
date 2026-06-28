from __future__ import annotations

import json
from pathlib import Path

import pytest

from predict_lib.cell_router import (
    CategoryRouting,
    CellCondition,
    CellRouter,
    CellRouteRule,
    VariantSpec,
    all_conditions_match,
    build_base_metadata_r2_key,
    build_base_model_r2_key,
    derive_class,
    derive_distance_band,
    derive_season,
    derive_surface,
    load_cell_router,
    resolve_dimension,
)


def _banei_router() -> CellRouter:
    routing = CategoryRouting(
        default_variant="sim",
        variants={
            "sim": VariantSpec(
                model_version="banei-cb-v9-sim-2011",
                feature_count=130,
                architecture="catboost",
            ),
            "base": VariantSpec(
                model_version="banei-cb-v8-window2011-wf-15y",
                feature_count=111,
                architecture="catboost",
            ),
        },
        rules=(
            CellRouteRule(
                conditions=(CellCondition(dimension="grade_code", values=frozenset({"E"})),),
                variant="base",
            ),
        ),
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
    assert routing.base_architecture == "catboost"
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
    assert routing.base_architecture == "catboost"
    assert routing.default_variant == "sim"
    assert router.resolve_variant("ban-ei", [{"grade_code": "E"}]) == "base"


def test_load_cell_router_real_config_new_format() -> None:
    router = load_cell_router()
    routing = router.routing_for("ban-ei")
    assert len(routing.rules) == 1
    rule = routing.rules[0]
    assert rule.variant == "base"
    assert len(rule.conditions) == 1
    condition = rule.conditions[0]
    assert condition.dimension == "grade_code"
    assert condition.values == frozenset({"E"})


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
            "base_architecture": "catboost",
            "default_variant": "sim",
            "rules": [
                {
                    "conditions": [{"dimension": "grade_code", "values": ["E"]}],
                    "variant": "base",
                }
            ],
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
            "base_architecture": "catboost",
            "default_variant": "sim",
            "rules": [
                {
                    "conditions": [{"dimension": "grade_code", "values": ["E"]}],
                    "variant": "base",
                },
                {
                    "conditions": [{"dimension": "grade_code", "values": ["E"]}],
                    "variant": "sim",
                },
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
            "base_architecture": "catboost",
            "default_variant": "sim",
            "rules": "nope",
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError, match="'rules' must be an array"):
        load_cell_router(config_path)


def test_load_cell_router_rejects_non_array_conditions(tmp_path: Path) -> None:
    config = {
        "ban-ei": {
            "sim_model_version": "s",
            "base_model_version": "b",
            "base_feature_count": 111,
            "base_architecture": "catboost",
            "default_variant": "sim",
            "rules": [{"conditions": "nope", "variant": "base"}],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError, match="'conditions' must be an array"):
        load_cell_router(config_path)


def test_category_routing_base_architecture_accessible() -> None:
    router = _banei_router()
    routing = router.routing_for("ban-ei")
    assert routing.base_architecture == "catboost"


def test_routing_for_returns_base_architecture_xgboost(tmp_path: Path) -> None:
    config = {
        "nar": {
            "sim_model_version": "nar-sim",
            "base_model_version": "nar-base",
            "base_feature_count": 138,
            "base_architecture": "xgboost",
            "default_variant": "sim",
            "rules": [
                {
                    "conditions": [{"dimension": "nar_subclass", "values": ["C"]}],
                    "variant": "base",
                }
            ],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    router = load_cell_router(config_path)
    routing = router.routing_for("nar")
    assert routing.base_architecture == "xgboost"


def test_build_base_metadata_r2_key() -> None:
    key = build_base_metadata_r2_key("ban-ei", "banei-cb-v8-window2011-wf-15y")
    assert key == "finish-position/ban-ei/banei-cb-v8-window2011-wf-15y/metadata.json"


def test_variant_spec_fields_accessible() -> None:
    spec = VariantSpec(model_version="m", feature_count=130, architecture="catboost")
    assert spec.model_version == "m"
    assert spec.feature_count == 130
    assert spec.architecture == "catboost"


def test_variants_dict_access_pattern() -> None:
    router = _banei_router()
    routing = router.routing_for("ban-ei")
    assert routing.variants["base"].model_version == "banei-cb-v8-window2011-wf-15y"
    assert routing.variants["base"].feature_count == 111
    assert routing.variants["base"].architecture == "catboost"
    assert routing.variants["sim"].model_version == "banei-cb-v9-sim-2011"
    assert routing.variants["sim"].feature_count == 130


def test_backward_compat_properties_derive_from_variants() -> None:
    routing = CategoryRouting(
        default_variant="sim",
        variants={
            "sim": VariantSpec(model_version="sim-v", feature_count=200, architecture="catboost"),
            "base": VariantSpec(model_version="base-v", feature_count=150, architecture="xgboost"),
        },
        rules=(),
    )
    assert routing.sim_model_version == "sim-v"
    assert routing.base_model_version == "base-v"
    assert routing.base_feature_count == 150
    assert routing.base_architecture == "xgboost"


def test_load_cell_router_new_format_variants(tmp_path: Path) -> None:
    config = {
        "ban-ei": {
            "default_variant": "sim",
            "variants": {
                "sim": {
                    "model_version": "banei-cb-v9-sim-2011",
                    "feature_count": 130,
                    "architecture": "catboost",
                },
                "base": {
                    "model_version": "banei-cb-v8-window2011-wf-15y",
                    "feature_count": 111,
                    "architecture": "catboost",
                },
            },
            "rules": [
                {
                    "conditions": [{"dimension": "grade_code", "values": ["E"]}],
                    "variant": "base",
                }
            ],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    router = load_cell_router(config_path)
    routing = router.routing_for("ban-ei")
    assert routing.variants["sim"].feature_count == 130
    assert routing.variants["base"].feature_count == 111
    assert routing.sim_model_version == "banei-cb-v9-sim-2011"
    assert routing.base_model_version == "banei-cb-v8-window2011-wf-15y"
    assert router.resolve_variant("ban-ei", [{"grade_code": "E"}]) == "base"
    assert router.resolve_variant("ban-ei", [{"grade_code": "A"}]) == "sim"


def test_load_cell_router_new_format_three_variants(tmp_path: Path) -> None:
    config = {
        "jra": {
            "default_variant": "sim",
            "variants": {
                "sim": {
                    "model_version": "jra-sim",
                    "feature_count": 263,
                    "architecture": "catboost",
                },
                "base": {
                    "model_version": "jra-base",
                    "feature_count": 142,
                    "architecture": "catboost",
                },
                "etop2": {
                    "model_version": "jra-etop2",
                    "feature_count": 244,
                    "architecture": "xgboost",
                },
            },
            "rules": [
                {
                    "conditions": [{"dimension": "venue", "values": ["05"]}],
                    "variant": "etop2",
                }
            ],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    router = load_cell_router(config_path)
    routing = router.routing_for("jra")
    assert set(routing.variants) == {"sim", "base", "etop2"}
    assert routing.variants["etop2"].model_version == "jra-etop2"
    assert routing.variants["etop2"].feature_count == 244
    assert routing.variants["etop2"].architecture == "xgboost"


def test_resolve_variant_routes_to_third_variant(tmp_path: Path) -> None:
    config = {
        "jra": {
            "default_variant": "sim",
            "variants": {
                "sim": {
                    "model_version": "jra-sim",
                    "feature_count": 263,
                    "architecture": "catboost",
                },
                "base": {
                    "model_version": "jra-base",
                    "feature_count": 142,
                    "architecture": "catboost",
                },
                "etop2": {
                    "model_version": "jra-etop2",
                    "feature_count": 244,
                    "architecture": "xgboost",
                },
            },
            "rules": [
                {
                    "conditions": [{"dimension": "venue", "values": ["05"]}],
                    "variant": "etop2",
                }
            ],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    router = load_cell_router(config_path)
    assert router.resolve_variant("jra", [{"keibajo_code": "05"}]) == "etop2"
    assert router.resolve_variant("jra", [{"keibajo_code": "03"}]) == "sim"


def test_load_cell_router_old_format_auto_detected(tmp_path: Path) -> None:
    config = {
        "ban-ei": {
            "sim_model_version": "banei-cb-v9-sim-2011",
            "base_model_version": "banei-cb-v8-window2011-wf-15y",
            "base_feature_count": 111,
            "base_architecture": "catboost",
            "default_variant": "sim",
            "rules": [
                {
                    "conditions": [{"dimension": "grade_code", "values": ["E"]}],
                    "variant": "base",
                }
            ],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    router = load_cell_router(config_path)
    routing = router.routing_for("ban-ei")
    assert routing.sim_model_version == "banei-cb-v9-sim-2011"
    assert routing.base_model_version == "banei-cb-v8-window2011-wf-15y"
    assert routing.base_feature_count == 111
    assert routing.base_architecture == "catboost"
    assert set(routing.variants) == {"sim", "base"}
    assert router.resolve_variant("ban-ei", [{"grade_code": "E"}]) == "base"


def _jra_multi_condition_router() -> CellRouter:
    routing = CategoryRouting(
        default_variant="sim",
        variants={
            "sim": VariantSpec(model_version="jra-sim", feature_count=263, architecture="catboost"),
            "base": VariantSpec(
                model_version="jra-base", feature_count=142, architecture="catboost"
            ),
        },
        rules=(
            CellRouteRule(
                conditions=(
                    CellCondition(dimension="venue", values=frozenset({"03"})),
                    CellCondition(dimension="surface", values=frozenset({"turf"})),
                ),
                variant="base",
            ),
        ),
    )
    return CellRouter(routing={"jra": routing})


def test_multi_condition_and_matching() -> None:
    router = _jra_multi_condition_router()
    entries = [{"keibajo_code": "03", "track_code": "10"}]
    assert router.resolve_variant("jra", entries) == "base"


def test_multi_condition_partial_match_returns_default() -> None:
    router = _jra_multi_condition_router()
    entries = [{"keibajo_code": "03", "track_code": "23"}]
    assert router.resolve_variant("jra", entries) == "sim"


def test_conditions_with_venue_and_season() -> None:
    routing = CategoryRouting(
        default_variant="sim",
        variants={
            "sim": VariantSpec(model_version="jra-sim", feature_count=263, architecture="catboost"),
            "base": VariantSpec(
                model_version="jra-base", feature_count=142, architecture="catboost"
            ),
        },
        rules=(
            CellRouteRule(
                conditions=(
                    CellCondition(dimension="venue", values=frozenset({"05"})),
                    CellCondition(dimension="season", values=frozenset({"summer"})),
                ),
                variant="base",
            ),
        ),
    )
    router = CellRouter(routing={"jra": routing})
    entries = [{"keibajo_code": "05", "kaisai_tsukihi": "0728"}]
    assert router.resolve_variant("jra", entries) == "base"
    miss = [{"keibajo_code": "05", "kaisai_tsukihi": "0228"}]
    assert router.resolve_variant("jra", miss) == "sim"


def test_derived_surface_turf() -> None:
    assert derive_surface("10", "jra") == "turf"


def test_derived_surface_dirt() -> None:
    assert derive_surface("23", "jra") == "dirt"


def test_derived_surface_other() -> None:
    assert derive_surface("51", "jra") == "other"


def test_derived_surface_non_jra_is_dirt() -> None:
    assert derive_surface("10", "nar") == "dirt"


def test_derived_distance_band_sprint() -> None:
    assert derive_distance_band(1000) == "sprint"


def test_derived_distance_band_mile() -> None:
    assert derive_distance_band(1400) == "mile"


def test_derived_distance_band_intermediate() -> None:
    assert derive_distance_band(1800) == "intermediate"


def test_derived_distance_band_long() -> None:
    assert derive_distance_band(2200) == "long"


def test_derived_distance_band_extended() -> None:
    assert derive_distance_band(3000) == "extended"


def test_derived_season_spring() -> None:
    assert derive_season(4) == "spring"


def test_derived_season_summer() -> None:
    assert derive_season(7) == "summer"


def test_derived_season_autumn() -> None:
    assert derive_season(10) == "autumn"


def test_derived_season_winter() -> None:
    assert derive_season(1) == "winter"


def test_derived_class() -> None:
    assert derive_class("A") == "A"


def test_derived_class_empty_is_unknown() -> None:
    assert derive_class("") == "unknown"


def testresolve_dimension_venue() -> None:
    assert resolve_dimension({"keibajo_code": "03"}, "venue", "jra") == "03"


def testresolve_dimension_venue_none() -> None:
    assert resolve_dimension({}, "venue", "jra") is None


def testresolve_dimension_surface() -> None:
    assert resolve_dimension({"track_code": "10"}, "surface", "jra") == "turf"


def testresolve_dimension_surface_none() -> None:
    assert resolve_dimension({}, "surface", "jra") is None


def testresolve_dimension_distance_band() -> None:
    assert resolve_dimension({"kyori": "1000"}, "distance_band", "jra") == "sprint"


def testresolve_dimension_distance_band_none() -> None:
    assert resolve_dimension({}, "distance_band", "jra") is None


def testresolve_dimension_season_from_tsukihi() -> None:
    assert resolve_dimension({"kaisai_tsukihi": "0728"}, "season", "jra") == "summer"


def test_derived_season_summer_via_resolve() -> None:
    assert resolve_dimension({"kaisai_tsukihi": "0728"}, "season", "jra") == "summer"


def test_derived_season_from_race_id() -> None:
    entry = {"race_id": "jra:2026:0728:03:01"}
    assert resolve_dimension(entry, "season", "jra") == "summer"


def testresolve_dimension_season_non_digit_tsukihi_falls_back_to_race_id() -> None:
    entry = {"kaisai_tsukihi": "xx", "race_id": "jra:2026:0728:03:01"}
    assert resolve_dimension(entry, "season", "jra") == "summer"


def testresolve_dimension_season_short_race_id_returns_none() -> None:
    assert resolve_dimension({"race_id": "jra:2026"}, "season", "jra") is None


def testresolve_dimension_season_non_digit_race_id_returns_none() -> None:
    assert resolve_dimension({"race_id": "jra:2026:zz:03:01"}, "season", "jra") is None


def testresolve_dimension_season_missing_returns_none() -> None:
    assert resolve_dimension({}, "season", "jra") is None


def testresolve_dimension_class() -> None:
    assert resolve_dimension({"grade_code": "A"}, "class", "jra") == "A"


def testresolve_dimension_class_none() -> None:
    assert resolve_dimension({}, "class", "jra") is None


def testresolve_dimension_fallback_raw_column() -> None:
    assert resolve_dimension({"grade_code": "E"}, "grade_code", "ban-ei") == "E"


def testresolve_dimension_fallback_raw_column_none() -> None:
    assert resolve_dimension({}, "grade_code", "ban-ei") is None


def testall_conditions_match_true() -> None:
    conditions = (
        CellCondition(dimension="venue", values=frozenset({"03"})),
        CellCondition(dimension="surface", values=frozenset({"turf"})),
    )
    entry = {"keibajo_code": "03", "track_code": "10"}
    assert all_conditions_match(entry, conditions, "jra") is True


def testall_conditions_match_false_on_missing_dimension() -> None:
    conditions = (CellCondition(dimension="venue", values=frozenset({"03"})),)
    assert all_conditions_match({}, conditions, "jra") is False
