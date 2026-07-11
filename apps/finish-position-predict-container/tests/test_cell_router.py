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
    derive_field_band,
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


def test_load_cell_router_real_config_has_jra_703_routing() -> None:
    router = load_cell_router()
    assert router.has_routing("jra") is True
    routing = router.routing_for("jra")
    assert routing.default_variant == "sim"
    assert routing.variants["sim"].model_version == "jra-cb-v9-sim-2013-clean"
    assert (
        routing.variants["jockey_pedigree_703"].model_version
        == "jra-cb-v9-sim-2013-clean-jockey-pedigree269"
    )
    assert routing.variants["jockey_pedigree_703"].feature_count == 269
    assert (
        routing.variants["jockey_pedigree_703"].feature_set_hash
        == "1f70d678d48b485d4fcf593de786880c8fcf748e464174279f1dfe1251c9ef07"
    )
    assert router.resolve_variant("jra", [{"kyoso_joken_code": "703"}]) == "jockey_pedigree_703"
    assert router.resolve_variant("jra", [{"kyoso_joken_code": "701"}]) == "sim"


def test_load_cell_router_real_config_has_jra_prior_corner_routing() -> None:
    router = load_cell_router()
    routing = router.routing_for("jra")
    assert (
        routing.variants["prior_corner_dirt_smallfield_005"].model_version
        == "jra-cb-v10-prior-corner274-2013"
    )
    assert routing.variants["prior_corner_dirt_smallfield_005"].feature_count == 274
    assert (
        routing.variants["prior_corner_dirt_smallfield_005"].feature_set_hash
        == "0b90ab1c7e19ef8d61c2b5419bd034bf277600c73b3f4a05e3b1ff1d99bbbb22"
    )
    # field_band is derived from len(entries) -- the actual count of rows
    # being scored -- not from any entry's own "shusso_tosu" (that column is
    # unconditionally NULL on every row that passes through the near-miss
    # layer, so it can never be trusted here; see resolve_dimension). None of
    # these entries carry a "shusso_tosu" key at all, matching the real
    # poisoned production shape.
    hit_row = {"kyoso_joken_code": "005", "track_code": "23"}
    hit_entries = [hit_row] * 10  # 10 declared runners -> field_band f_le10.
    miss_field_entries = [hit_row] * 11  # 11 -> field_band f11_13, doesn't match.
    miss_class_row = {**hit_row, "kyoso_joken_code": "703"}
    miss_class_entries = [miss_class_row] * 10
    assert router.resolve_variant("jra", hit_entries) == "prior_corner_dirt_smallfield_005"
    assert router.resolve_variant("jra", miss_field_entries) == "sim"
    assert router.resolve_variant("jra", miss_class_entries) == "jockey_pedigree_703"


def test_load_cell_router_real_config_has_jra_hakodate_venue_routing() -> None:
    router = load_cell_router()
    routing = router.routing_for("jra")
    assert len(routing.rules) == 3
    hakodate = {"keibajo_code": "02"}
    assert router.resolve_variant("jra", [hakodate]) == "jockey_pedigree_703"
    non_hakodate = {"keibajo_code": "05"}
    assert router.resolve_variant("jra", [non_hakodate]) == "sim"


def test_load_cell_router_real_config_jra_rule_precedence_at_hakodate() -> None:
    # First-match-wins order: rule 1 (kyoso_joken_code=703), rule 2
    # (dirt/f_le10/005 prior-corner), rule 3 (venue=02 Hakodate, appended
    # last). A Hakodate race that also matches an earlier rule routes via
    # that earlier rule, not the venue rule.
    router = load_cell_router()

    # 703-class race at Hakodate: rule 1 wins (same variant as rule 3 would
    # give, but the precedence is what this test documents).
    class_703_at_hakodate = {"kyoso_joken_code": "703", "keibajo_code": "02"}
    assert router.resolve_variant("jra", [class_703_at_hakodate]) == "jockey_pedigree_703"

    # dirt / f_le10 / kyoso_joken_code=005 at a non-Hakodate venue (Kokura,
    # 10) still routes to the prior-corner variant via rule 2.
    prior_corner_at_kokura = {
        "kyoso_joken_code": "005",
        "track_code": "23",
        "shusso_tosu": 10,
        "keibajo_code": "10",
    }
    assert (
        router.resolve_variant("jra", [prior_corner_at_kokura])
        == "prior_corner_dirt_smallfield_005"
    )

    # Same dirt/f_le10/005 cell, but AT Hakodate: rule 2 still wins over rule
    # 3 because it appears earlier in the rules list.
    prior_corner_at_hakodate = {**prior_corner_at_kokura, "keibajo_code": "02"}
    assert (
        router.resolve_variant("jra", [prior_corner_at_hakodate])
        == "prior_corner_dirt_smallfield_005"
    )


def test_load_cell_router_real_config_jra_hakodate_falls_through_to_venue_rule() -> None:
    # A Hakodate race that matches neither rule 1 nor rule 2 falls through to
    # rule 3 (venue) and gets jockey_pedigree_703, not the category default.
    router = load_cell_router()
    hakodate_generic = {"keibajo_code": "02", "kyoso_joken_code": "701"}
    assert router.resolve_variant("jra", [hakodate_generic]) == "jockey_pedigree_703"

    non_hakodate_generic = {"keibajo_code": "05", "kyoso_joken_code": "701"}
    assert router.resolve_variant("jra", [non_hakodate_generic]) == "sim"


def test_load_cell_router_real_config_has_no_nar_routing() -> None:
    # The nar-xgb-cell-a957d8b4-v1 route was reverted 2026-07-03 (adopted on
    # broken cell_training_evaluations data). NAR must carry no cell routing so
    # every race falls through to the category default path.
    router = load_cell_router()
    assert router.has_routing("nar") is False
    former_cell_entry = {
        "grade_code": "E",
        "keibajo_code": "54",
        "kyori": 1400,
        "kaisai_tsukihi": "0702",
        "track_code": "20",
    }
    assert router.resolve_variant("nar", [former_cell_entry]) == "sim"


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


def test_all_conditions_match_threads_field_size_to_field_band() -> None:
    conditions = (CellCondition(dimension="field_band", values=frozenset({"f_le10"})),)
    # No "shusso_tosu" key on the entry at all -- matches the real poisoned
    # production shape -- but field_size makes the condition resolvable.
    assert all_conditions_match({}, conditions, "jra", field_size=8) is True


def test_all_conditions_match_field_band_fails_without_field_size_or_entry_value() -> None:
    conditions = (CellCondition(dimension="field_band", values=frozenset({"f_le10"})),)
    assert all_conditions_match({}, conditions, "jra") is False


def test_all_conditions_match_threads_card_max_race_bango_to_is_final_race() -> None:
    conditions = (CellCondition(dimension="is_final_race", values=frozenset({"true"})),)
    entry = {"race_id": "nar:2026:0712:54:10"}
    assert all_conditions_match(entry, conditions, "nar", card_max_race_bango=10) is True
    assert all_conditions_match(entry, conditions, "nar", card_max_race_bango=12) is False


def test_all_conditions_match_is_final_race_fails_without_card_max_race_bango() -> None:
    conditions = (CellCondition(dimension="is_final_race", values=frozenset({"true"})),)
    entry = {"race_id": "nar:2026:0712:54:10"}
    assert all_conditions_match(entry, conditions, "nar") is False


def _jra_prior_corner_router() -> CellRouter:
    """Mirrors the real cell_routing.json shape for prior_corner_dirt_smallfield_005."""
    routing = CategoryRouting(
        default_variant="sim",
        variants={
            "sim": VariantSpec(
                model_version="jra-cb-v9-sim-2013-clean",
                feature_count=250,
                architecture="catboost",
            ),
            "jockey_pedigree_703": VariantSpec(
                model_version="jra-cb-v9-sim-2013-clean-jockey-pedigree269",
                feature_count=269,
                architecture="catboost",
            ),
            "prior_corner_dirt_smallfield_005": VariantSpec(
                model_version="jra-cb-v10-prior-corner274-2013",
                feature_count=274,
                architecture="catboost",
            ),
        },
        rules=(
            CellRouteRule(
                conditions=(
                    CellCondition(dimension="kyoso_joken_code", values=frozenset({"703"})),
                ),
                variant="jockey_pedigree_703",
            ),
            CellRouteRule(
                conditions=(
                    CellCondition(dimension="surface", values=frozenset({"dirt"})),
                    CellCondition(dimension="field_band", values=frozenset({"f_le10"})),
                    CellCondition(dimension="kyoso_joken_code", values=frozenset({"005"})),
                ),
                variant="prior_corner_dirt_smallfield_005",
            ),
        ),
    )
    return CellRouter(routing={"jra": routing})


def test_resolve_variant_prior_corner_smallfield_fires_via_entries_length() -> None:
    """Regression for the real defect: 8 declared runners, dirt, kyoso_joken_code
    005 -- exactly today's live-repro race -- with NO usable "shusso_tosu" on
    any entry (the poisoned production shape). Before the fix this rule could
    never match (field_band always resolved to None); resolve_variant now
    derives field_band from len(entries) instead.
    """
    router = _jra_prior_corner_router()
    entries = [
        {"track_code": "20", "kyoso_joken_code": "005"} for _ in range(8)
    ]  # track_code "2*" -> dirt (see derive_surface); no shusso_tosu key at all.
    assert router.resolve_variant("jra", entries) == "prior_corner_dirt_smallfield_005"


def test_resolve_variant_prior_corner_smallfield_does_not_fire_for_large_field() -> None:
    router = _jra_prior_corner_router()
    entries = [{"track_code": "20", "kyoso_joken_code": "005"} for _ in range(16)]
    assert router.resolve_variant("jra", entries) == "sim"


def test_load_cell_router_parses_optional_variant_feature_contract(tmp_path: Path) -> None:
    config = {
        "nar": {
            "default_variant": "sim",
            "variants": {
                "sim": {
                    "model_version": "iter12-nar-xgb-hpo-v8-clean188",
                    "feature_count": 188,
                    "architecture": "xgboost",
                },
                "cell": {
                    "model_version": "nar-cell-v1",
                    "feature_count": 2,
                    "architecture": "xgboost",
                    "feature_set_hash": "hash-v1",
                    "feature_names": ["f2", "f1"],
                },
            },
            "rules": [
                {
                    "conditions": [{"dimension": "venue", "values": ["54"]}],
                    "variant": "cell",
                }
            ],
        }
    }
    config_path = tmp_path / "cell_routing.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    spec = load_cell_router(config_path).routing_for("nar").variants["cell"]
    assert spec.feature_set_hash == "hash-v1"
    assert spec.feature_names == ("f2", "f1")


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


def test_derived_field_band_boundaries() -> None:
    assert derive_field_band(10) == "f_le10"
    assert derive_field_band(11) == "f11_13"
    assert derive_field_band(13) == "f11_13"
    assert derive_field_band(14) == "f14_15"
    assert derive_field_band(15) == "f14_15"
    assert derive_field_band(16) == "f16p"


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


def testresolve_dimension_field_band() -> None:
    assert resolve_dimension({"shusso_tosu": "10"}, "field_band", "jra") == "f_le10"


def testresolve_dimension_field_band_none() -> None:
    assert resolve_dimension({}, "field_band", "jra") is None


def testresolve_dimension_field_band_uses_field_size_when_provided() -> None:
    # field_size (the actual count of rows being scored for the race) takes
    # precedence over the entry's own "shusso_tosu" -- that column is
    # unconditionally NULL on every row that passes through the near-miss
    # layer (add-near-miss-features.py re-emits it as a bare
    # ``cast(null as bigint)`` to reproduce a trained NAR CatBoost split), so
    # trusting field_size instead is what makes field_band resolvable again.
    entry = {"shusso_tosu": "99"}
    assert resolve_dimension(entry, "field_band", "jra", field_size=8) == "f_le10"


def testresolve_dimension_field_band_field_size_used_when_entry_has_no_shusso_tosu() -> None:
    # Regression for the real defect: the entry carries no usable
    # "shusso_tosu" at all (matches the poisoned production shape), but
    # field_size alone is enough to resolve field_band correctly.
    assert resolve_dimension({}, "field_band", "jra", field_size=12) == "f11_13"


def testresolve_dimension_field_band_falls_back_to_entry_when_field_size_omitted() -> None:
    # Backward compatibility: callers that don't pass field_size (direct unit
    # tests, any other future caller) keep reading entry["shusso_tosu"].
    assert resolve_dimension({"shusso_tosu": "16"}, "field_band", "jra") == "f16p"


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


def testresolve_dimension_is_final_race_true() -> None:
    entry = {"race_id": "nar:2026:0712:54:10"}
    assert resolve_dimension(entry, "is_final_race", "nar", card_max_race_bango=10) == "true"


def testresolve_dimension_is_final_race_true_with_leading_zero() -> None:
    # race_bango is zero-padded in the real race_id ("08" not "8"); int
    # comparison must still resolve correctly against a plain-int card max.
    entry = {"race_id": "nar:2026:0712:54:08"}
    assert resolve_dimension(entry, "is_final_race", "nar", card_max_race_bango=8) == "true"


def testresolve_dimension_is_final_race_false() -> None:
    entry = {"race_id": "nar:2026:0712:54:05"}
    assert resolve_dimension(entry, "is_final_race", "nar", card_max_race_bango=10) == "false"


def testresolve_dimension_is_final_race_none_without_card_max() -> None:
    # card_max_race_bango omitted (the default for every existing caller) --
    # this fails closed to None rather than guessing from the entry alone,
    # since a single race's own entries can never answer "is this the day's
    # last race".
    entry = {"race_id": "nar:2026:0712:54:10"}
    assert resolve_dimension(entry, "is_final_race", "nar") is None


def testresolve_dimension_is_final_race_none_without_race_id() -> None:
    assert resolve_dimension({}, "is_final_race", "nar", card_max_race_bango=10) is None


def testresolve_dimension_is_final_race_none_on_malformed_race_id() -> None:
    # Wrong part count -- parse_race_id raises ValueError, caught and turned
    # into the same fail-closed None as every other unresolvable dimension.
    entry = {"race_id": "nar:2026:0712"}
    assert resolve_dimension(entry, "is_final_race", "nar", card_max_race_bango=10) is None


def testresolve_dimension_is_final_race_none_on_non_digit_race_bango() -> None:
    entry = {"race_id": "nar:2026:0712:54:xx"}
    assert resolve_dimension(entry, "is_final_race", "nar", card_max_race_bango=10) is None


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


def _kochi_final_shaped_router() -> CellRouter:
    """Mirrors the not-yet-live shape documented in tmp/kochi-final/cell_design.md
    section 4 -- venue=54 AND is_final_race=true -- to prove resolve_variant
    can thread card_max_race_bango end-to-end through a real multi-condition
    rule, without this shape existing in the real cell_routing.json yet.
    """
    routing = CategoryRouting(
        default_variant="sim",
        variants={
            "sim": VariantSpec(
                model_version="iter12-nar-xgb-hpo-v8-clean188",
                feature_count=188,
                architecture="xgboost",
            ),
            "kochi_final": VariantSpec(
                model_version="nar-cb-kochi-final-v1",
                feature_count=50,
                architecture="catboost",
            ),
        },
        rules=(
            CellRouteRule(
                conditions=(
                    CellCondition(dimension="venue", values=frozenset({"54"})),
                    CellCondition(dimension="is_final_race", values=frozenset({"true"})),
                ),
                variant="kochi_final",
            ),
        ),
    )
    return CellRouter(routing={"nar": routing})


def test_resolve_variant_kochi_final_rule_fires_with_card_max_race_bango() -> None:
    router = _kochi_final_shaped_router()
    entries = [{"keibajo_code": "54", "race_id": "nar:2026:0712:54:10"}]
    assert router.resolve_variant("nar", entries, card_max_race_bango=10) == "kochi_final"


def test_resolve_variant_kochi_final_rule_does_not_fire_for_non_final_race() -> None:
    router = _kochi_final_shaped_router()
    entries = [{"keibajo_code": "54", "race_id": "nar:2026:0712:54:05"}]
    assert router.resolve_variant("nar", entries, card_max_race_bango=10) == "sim"


def test_resolve_variant_kochi_final_rule_fails_closed_without_card_max_race_bango() -> None:
    # The whole point of the fail-closed design: omitting card_max_race_bango
    # (every caller that doesn't yet compute it, which is every caller today)
    # must never accidentally route a race as "final" by guessing.
    router = _kochi_final_shaped_router()
    entries = [{"keibajo_code": "54", "race_id": "nar:2026:0712:54:10"}]
    assert router.resolve_variant("nar", entries) == "sim"


def test_resolve_variant_kochi_final_rule_does_not_fire_at_other_venues() -> None:
    router = _kochi_final_shaped_router()
    entries = [{"keibajo_code": "30", "race_id": "nar:2026:0712:30:10"}]
    assert router.resolve_variant("nar", entries, card_max_race_bango=10) == "sim"
