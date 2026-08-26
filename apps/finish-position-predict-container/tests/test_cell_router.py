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
    card_max_race_bango_for_race_id,
    derive_canonical_distance_band,
    derive_canonical_field_size_band,
    derive_card_max_race_bango_by_card,
    derive_class,
    derive_distance_band,
    derive_field_band,
    derive_season,
    derive_surface,
    load_cell_router,
    resolve_dimension,
    rule_is_effective,
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
    assert (
        router.resolve_variant("jra", [{"grade_code": "", "kyoso_joken_code": "703"}])
        == "joken_703"
    )
    assert (
        router.resolve_variant("jra", [{"grade_code": "E", "kyoso_joken_code": "703"}])
        == "jockey_pedigree_703"
    )


def test_load_cell_router_real_config_prioritizes_703_dirt_mile_summer() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_dirt_mile_summer_qsm_top1"]
    assert specialist.model_version == "jra-joken-703-querysoftmax-maxrange-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_703"
    summer_dirt_mile = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "23",
        "kyori": 1400,
        "race_date": "20260712",
    }
    spring_dirt_mile = {**summer_dirt_mile, "race_date": "20260412"}
    summer_turf_mile = {**summer_dirt_mile, "track_code": "10"}
    summer_dirt_intermediate = {**summer_dirt_mile, "kyori": 1800}
    graded_summer_dirt_mile = {**summer_dirt_mile, "grade_code": "E"}

    assert (
        router.resolve_variant("jra", [summer_dirt_mile]) == "joken_703_dirt_mile_summer_qsm_top1"
    )
    assert router.resolve_variant("jra", [spring_dirt_mile]) == "joken_703"
    assert router.resolve_variant("jra", [summer_turf_mile]) == "joken_703_turf_1400_qsm_gated_top1"
    assert (
        router.resolve_variant("jra", [summer_dirt_intermediate])
        == "joken_703_dirt_intermediate_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [graded_summer_dirt_mile]) == "jockey_pedigree_703"


def test_load_cell_router_real_config_prioritizes_005_dirt_mile_autumn_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_005_dirt_mile_autumn_yeti_gated_top1"]
    assert specialist.model_version == "jra-joken-005-dirt-mile-autumn-yeti-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_005"
    assert specialist.minimum_candidate_margin == 0.05
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    dirt_mile_autumn = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "23",
        "kyori": 1400,
        "race_date": "20261012",
    }

    assert (
        router.resolve_variant("jra", [dirt_mile_autumn])
        == "joken_005_dirt_mile_autumn_yeti_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**dirt_mile_autumn, "grade_code": "E"}])
        == "prior_corner_dirt_smallfield_005"
    )
    assert (
        router.resolve_variant("jra", [{**dirt_mile_autumn, "track_code": "10"}])
        == "joken_005_turf_mile_yeti_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**dirt_mile_autumn, "kyori": 1600}])
        == "joken_005_dirt_intermediate_autumn_yeti_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**dirt_mile_autumn, "race_date": "20260712"}])
        == "joken_005"
    )
    assert router.resolve_variant("jra", [{**dirt_mile_autumn, "race_date": None}]) == "joken_005"


def test_load_cell_router_real_config_prioritizes_005_dirt_mile_spring_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_005_dirt_mile_spring_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-005-dirt-mile-spring-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_005"
    assert specialist.minimum_candidate_margin == 0.02
    assert specialist.minimum_candidate_top_z == 1.25
    assert specialist.maximum_candidate_v2_rank == 20
    spring = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "23",
        "kyori": 1400,
        "race_date": "20260412",
    }

    assert router.resolve_variant("jra", [spring]) == "joken_005_dirt_mile_spring_qsm_gated_top1"
    assert router.resolve_variant("jra", [{**spring, "race_date": "20260712"}]) == "joken_005"
    assert (
        router.resolve_variant("jra", [{**spring, "race_date": "20261012"}])
        == "joken_005_dirt_mile_autumn_yeti_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**spring, "grade_code": "E"}])
        == "prior_corner_dirt_smallfield_005"
    )
    assert (
        router.resolve_variant("jra", [{**spring, "track_code": "10"}])
        == "joken_005_turf_mile_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**spring, "race_date": None}]) == "joken_005"


def test_load_cell_router_real_config_prioritizes_005_dirt_intermediate_autumn_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants[
        "joken_005_dirt_intermediate_autumn_yeti_gated_top1"
    ]
    assert specialist.model_version == "jra-joken-005-dirt-intermediate-autumn-yeti-gated-v1"
    assert specialist.minimum_candidate_margin == 0.2
    assert specialist.minimum_candidate_top_z == 1.25
    assert specialist.maximum_candidate_v2_rank == 20
    autumn = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "23",
        "kyori": 1800,
        "race_date": "20261012",
    }
    assert (
        router.resolve_variant("jra", [autumn])
        == "joken_005_dirt_intermediate_autumn_yeti_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**autumn, "race_date": "20260712"}])
        == "joken_005_dirt_1800_nonautumn_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**autumn, "race_date": None}]) == "joken_005"
    assert router.resolve_variant("jra", [{**autumn, "track_code": "10"}]) == "joken_005"
    assert (
        router.resolve_variant("jra", [{**autumn, "grade_code": "E"}])
        == "prior_corner_dirt_smallfield_005"
    )


def test_load_cell_router_real_config_prioritizes_005_dirt_1700_summer_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_005_dirt_1700_summer_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-005-dirt-1700-summer-qsm-gated-v1"
    assert specialist.minimum_candidate_margin == 0.2
    assert specialist.minimum_candidate_top_z == 1.25
    assert specialist.maximum_candidate_v2_rank == 20
    summer = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "23",
        "kyori": 1700,
        "race_date": "20260712",
    }
    assert router.resolve_variant("jra", [summer]) == "joken_005_dirt_1700_summer_qsm_gated_top1"
    assert (
        router.resolve_variant("jra", [{**summer, "kyori": 1800}])
        == "joken_005_dirt_1800_nonautumn_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**summer, "race_date": "20261012"}])
        == "joken_005_dirt_intermediate_autumn_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**summer, "race_date": None}]) == "joken_005"
    assert (
        router.resolve_variant("jra", [{**summer, "grade_code": "E"}])
        == "prior_corner_dirt_smallfield_005"
    )


def test_load_cell_router_real_config_prioritizes_005_dirt_1200_winter_summer_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants[
        "joken_005_dirt_1200_winter_summer_qsm_gated_top1"
    ]
    assert specialist.model_version == "jra-joken-005-dirt-1200-winter-summer-qsm-gated-v1"
    assert specialist.minimum_candidate_margin == 0.15
    assert specialist.minimum_candidate_top_z == 1.25
    assert specialist.maximum_candidate_v2_rank == 20
    winter = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "23",
        "kyori": 1200,
        "race_date": "20260212",
    }
    assert (
        router.resolve_variant("jra", [winter])
        == "joken_005_dirt_1200_winter_summer_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**winter, "race_date": "20260712"}])
        == "joken_005_dirt_1200_winter_summer_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**winter, "race_date": "20260412"}])
        == "joken_005_dirt_mile_spring_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**winter, "race_date": "20261012"}])
        == "joken_005_dirt_mile_autumn_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**winter, "kyori": 1300}]) == "joken_005"
    assert router.resolve_variant("jra", [{**winter, "race_date": None}]) == "joken_005"


def test_load_cell_router_real_config_prioritizes_005_dirt_1800_nonautumn_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_005_dirt_1800_nonautumn_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-005-dirt-1800-nonautumn-qsm-gated-v1"
    assert specialist.minimum_candidate_margin == 0.2
    assert specialist.minimum_candidate_top_z == 1.25
    assert specialist.maximum_candidate_v2_rank == 2
    spring = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "23",
        "kyori": 1800,
        "race_date": "20260412",
    }
    assert router.resolve_variant("jra", [spring]) == "joken_005_dirt_1800_nonautumn_qsm_gated_top1"
    assert (
        router.resolve_variant("jra", [{**spring, "race_date": "20260712"}])
        == "joken_005_dirt_1800_nonautumn_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**spring, "race_date": "20261012"}])
        == "joken_005_dirt_intermediate_autumn_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**spring, "kyori": 1900}]) == "joken_005"
    assert router.resolve_variant("jra", [{**spring, "race_date": None}]) == "joken_005"


def test_load_cell_router_real_config_prioritizes_005_turf_intermediate_spring_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants[
        "joken_005_turf_intermediate_spring_qsm_gated_top1"
    ]
    assert specialist.model_version == "jra-joken-005-turf-intermediate-spring-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_005"
    assert specialist.minimum_candidate_margin == 0.0
    assert specialist.minimum_candidate_top_z == 1.0
    assert specialist.maximum_candidate_v2_rank == 20
    spring = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "10",
        "kyori": 1800,
        "race_date": "20260412",
    }

    assert (
        router.resolve_variant("jra", [spring])
        == "joken_005_turf_intermediate_spring_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**spring, "race_date": "20260712"}]) == "joken_005"
    assert router.resolve_variant("jra", [{**spring, "grade_code": "E"}]) == "sim"
    assert (
        router.resolve_variant("jra", [{**spring, "track_code": "23"}])
        == "joken_005_dirt_1800_nonautumn_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**spring, "kyori": 2000}])
        == "joken_005_turf_long_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**spring, "race_date": None}]) == "joken_005"


def test_load_cell_router_real_config_prioritizes_005_turf_cells_with_confidence_gates() -> None:
    router = load_cell_router()
    routing = router.routing_for("jra")
    mile_specialist = routing.variants["joken_005_turf_mile_yeti_gated_top1"]
    assert mile_specialist.model_version == "jra-joken-005-turf-mile-yeti-gated-v1"
    assert mile_specialist.routing_mode == "jra_variant_top1_swap"
    assert mile_specialist.base_variant == "joken_005"
    assert mile_specialist.minimum_candidate_margin == 0.1
    assert mile_specialist.minimum_candidate_top_z == 1.25
    specialist = routing.variants["joken_005_turf_long_yeti_gated_top1"]
    assert specialist.model_version == "jra-joken-005-turf-long-hierarchical-qsm-gated-v2"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_005"
    assert specialist.minimum_candidate_margin == 0.3
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    turf_long = {
        "grade_code": None,
        "kyoso_joken_code": "005",
        "track_code": "10",
        "kyori": 2200,
        "race_date": "20260712",
    }

    assert (
        router.resolve_variant("jra", [{**turf_long, "kyori": 1400}])
        == "joken_005_turf_mile_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [turf_long]) == "joken_005_turf_long_yeti_gated_top1"
    assert router.resolve_variant("jra", [{**turf_long, "grade_code": "E"}]) == "sim"
    assert router.resolve_variant("jra", [{**turf_long, "track_code": "23"}]) == "joken_005"
    assert router.resolve_variant("jra", [{**turf_long, "kyori": 1800}]) == "joken_005"
    assert router.resolve_variant("jra", [{**turf_long, "track_code": None}]) == "joken_005"
    assert router.resolve_variant("jra", [{**turf_long, "kyori": None}]) == "joken_005"


def test_load_cell_router_real_config_prioritizes_010_dirt_intermediate_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_010_dirt_intermediate_yeti_gated_top1"]
    assert specialist.model_version == "jra-joken-010-dirt-intermediate-yeti-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_010"
    assert specialist.minimum_candidate_margin == 0.2
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    dirt_intermediate = {
        "grade_code": None,
        "kyoso_joken_code": "010",
        "track_code": "23",
        "kyori": 1800,
        "race_date": "20260712",
    }

    assert (
        router.resolve_variant("jra", [dirt_intermediate])
        == "joken_010_dirt_intermediate_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**dirt_intermediate, "grade_code": "E"}]) == "sim"
    assert router.resolve_variant("jra", [{**dirt_intermediate, "track_code": "10"}]) == "joken_010"
    assert router.resolve_variant("jra", [{**dirt_intermediate, "kyori": 2000}]) == "joken_010"
    assert router.resolve_variant("jra", [{**dirt_intermediate, "track_code": None}]) == "joken_010"
    assert router.resolve_variant("jra", [{**dirt_intermediate, "kyori": None}]) == "joken_010"


def test_load_cell_router_real_config_prioritizes_701_turf_long_confidence_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_701_turf_long_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-701-turf-long-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_701"
    assert specialist.minimum_candidate_margin == 0.5
    assert specialist.minimum_candidate_top_z == 1.5
    turf_long = {
        "grade_code": None,
        "kyoso_joken_code": "701",
        "track_code": "10",
        "kyori": 2200,
        "race_date": "20260712",
    }

    assert router.resolve_variant("jra", [turf_long]) == "joken_701_turf_long_qsm_gated_top1"
    assert router.resolve_variant("jra", [{**turf_long, "grade_code": "E"}]) == "sim"
    assert router.resolve_variant("jra", [{**turf_long, "track_code": "23"}]) == "joken_701"
    assert (
        router.resolve_variant("jra", [{**turf_long, "kyori": 1800}])
        == "joken_701_turf_intermediate_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**turf_long, "track_code": None}]) == "joken_701"
    assert router.resolve_variant("jra", [{**turf_long, "kyori": None}]) == "joken_701"


def test_load_cell_router_real_config_prioritizes_703_dirt_intermediate_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_dirt_intermediate_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-703-dirt-intermediate-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_703"
    assert specialist.minimum_candidate_margin == 0.01
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 2
    dirt_intermediate = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "23",
        "kyori": 1800,
        "race_date": "20260712",
    }

    assert (
        router.resolve_variant("jra", [dirt_intermediate])
        == "joken_703_dirt_intermediate_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**dirt_intermediate, "grade_code": "E"}])
        == "jockey_pedigree_703"
    )
    assert (
        router.resolve_variant("jra", [{**dirt_intermediate, "track_code": "10"}])
        == "joken_703_turf_intermediate_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**dirt_intermediate, "kyori": 2200}]) == "joken_703"
    assert router.resolve_variant("jra", [{**dirt_intermediate, "track_code": None}]) == "joken_703"
    assert router.resolve_variant("jra", [{**dirt_intermediate, "kyori": None}]) == "joken_703"


def test_load_cell_router_real_config_prioritizes_701_turf_mile_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_701_turf_mile_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-701-turf-mile-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_701"
    assert specialist.minimum_candidate_margin == 0.5
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    turf_mile = {
        "grade_code": None,
        "kyoso_joken_code": "701",
        "track_code": "10",
        "kyori": 1400,
        "race_date": "20260712",
    }

    assert router.resolve_variant("jra", [turf_mile]) == "joken_701_turf_mile_qsm_gated_top1"
    assert router.resolve_variant("jra", [{**turf_mile, "grade_code": "E"}]) == "sim"
    assert router.resolve_variant("jra", [{**turf_mile, "track_code": "23"}]) == "joken_701"
    assert (
        router.resolve_variant("jra", [{**turf_mile, "kyori": 1600}])
        == "joken_701_turf_intermediate_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**turf_mile, "track_code": None}]) == "joken_701"
    assert router.resolve_variant("jra", [{**turf_mile, "kyori": None}]) == "joken_701"


def test_load_cell_router_real_config_prioritizes_701_turf_intermediate_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_701_turf_intermediate_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-701-turf-intermediate-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_701"
    assert specialist.minimum_candidate_margin == 0.5
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    turf_intermediate = {
        "grade_code": None,
        "kyoso_joken_code": "701",
        "track_code": "10",
        "kyori": 1800,
        "race_date": "20260712",
    }

    assert (
        router.resolve_variant("jra", [turf_intermediate])
        == "joken_701_turf_intermediate_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**turf_intermediate, "grade_code": "E"}]) == "sim"
    assert router.resolve_variant("jra", [{**turf_intermediate, "track_code": "23"}]) == "joken_701"
    assert (
        router.resolve_variant("jra", [{**turf_intermediate, "kyori": 2000}])
        == "joken_701_turf_long_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**turf_intermediate, "track_code": None}]) == "joken_701"
    assert router.resolve_variant("jra", [{**turf_intermediate, "kyori": None}]) == "joken_701"


def test_load_cell_router_real_config_prioritizes_703_turf_1200_largefield_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants[
        "joken_703_turf_1200_largefield_yeti_gated_top1"
    ]
    assert specialist.model_version == "jra-joken-703-turf-1200-largefield-yeti-gated-v1"
    assert specialist.minimum_candidate_margin == 0.15
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    entry = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "10",
        "kyori": 1200,
        "race_date": "20260712",
    }
    assert (
        router.resolve_variant("jra", [entry] * 14)
        == "joken_703_turf_1200_largefield_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [entry] * 13) == "joken_703"
    assert (
        router.resolve_variant("jra", [{**entry, "kyori": 1400}] * 14)
        == "joken_703_turf_1400_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**entry, "grade_code": "E"}] * 14) == "jockey_pedigree_703"
    )


def test_load_cell_router_real_config_prioritizes_703_turf_1400_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_turf_1400_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-703-turf-1400-qsm-gated-v1"
    assert specialist.minimum_candidate_margin == 0.1
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    entry = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "10",
        "kyori": 1400,
        "race_date": "20260712",
    }
    assert router.resolve_variant("jra", [entry]) == "joken_703_turf_1400_qsm_gated_top1"
    assert router.resolve_variant("jra", [{**entry, "kyori": 1500}]) == "joken_703"
    assert (
        router.resolve_variant("jra", [{**entry, "track_code": "23"}])
        == "joken_703_dirt_mile_summer_qsm_top1"
    )
    assert router.resolve_variant("jra", [{**entry, "grade_code": "E"}]) == "jockey_pedigree_703"


def test_load_cell_router_real_config_prioritizes_703_turf_long_summer_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_turf_long_summer_yeti_gated_top1"]
    assert specialist.model_version == "jra-joken-703-turf-long-summer-yeti-gated-v1"
    assert specialist.minimum_candidate_margin == 0.2
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    summer = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "10",
        "kyori": 2200,
        "race_date": "20260712",
    }
    assert router.resolve_variant("jra", [summer]) == "joken_703_turf_long_summer_yeti_gated_top1"
    assert (
        router.resolve_variant("jra", [{**summer, "race_date": "20260412"}])
        == "joken_703_turf_long_spring_qsm_gated_top1"
    )
    assert router.resolve_variant("jra", [{**summer, "race_date": "20261012"}]) == "joken_703"
    assert router.resolve_variant("jra", [{**summer, "race_date": None}]) == "joken_703"
    assert router.resolve_variant("jra", [{**summer, "grade_code": "E"}]) == "jockey_pedigree_703"


def test_load_cell_router_real_config_prioritizes_703_turf_long_spring_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_turf_long_spring_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-703-turf-long-spring-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_703"
    assert specialist.minimum_candidate_margin == 0.1
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 2
    turf_long_spring = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "10",
        "kyori": 2200,
        "race_date": "20260412",
    }

    assert (
        router.resolve_variant("jra", [turf_long_spring])
        == "joken_703_turf_long_spring_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**turf_long_spring, "grade_code": "E"}])
        == "jockey_pedigree_703"
    )
    assert router.resolve_variant("jra", [{**turf_long_spring, "track_code": "23"}]) == "joken_703"
    assert router.resolve_variant("jra", [{**turf_long_spring, "kyori": 2400}]) == "joken_703"
    assert (
        router.resolve_variant("jra", [{**turf_long_spring, "race_date": "20260712"}])
        == "joken_703_turf_long_summer_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**turf_long_spring, "race_date": None}]) == "joken_703"


def test_load_cell_router_real_config_prioritizes_703_turf_intermediate_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_turf_intermediate_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-703-turf-intermediate-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_703"
    assert specialist.minimum_candidate_margin == 0.05
    assert specialist.minimum_candidate_top_z == 1.25
    assert specialist.maximum_candidate_v2_rank == 2
    turf_intermediate = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "10",
        "kyori": 1800,
        "race_date": "20260712",
    }

    assert (
        router.resolve_variant("jra", [turf_intermediate])
        == "joken_703_turf_intermediate_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**turf_intermediate, "grade_code": "E"}])
        == "jockey_pedigree_703"
    )
    assert (
        router.resolve_variant("jra", [{**turf_intermediate, "track_code": "23"}])
        == "joken_703_dirt_intermediate_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**turf_intermediate, "kyori": 2000}])
        == "joken_703_turf_long_summer_yeti_gated_top1"
    )
    assert router.resolve_variant("jra", [{**turf_intermediate, "track_code": None}]) == "joken_703"
    assert router.resolve_variant("jra", [{**turf_intermediate, "kyori": None}]) == "joken_703"


def test_load_cell_router_real_config_prioritizes_703_other_extended_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_other_extended_qsm_gated_top1"]
    assert specialist.model_version == "jra-joken-703-other-extended-qsm-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_703"
    assert specialist.minimum_candidate_margin == 0.15
    assert specialist.minimum_candidate_top_z == 1.25
    assert specialist.maximum_candidate_v2_rank == 2
    other_extended = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "51",
        "kyori": 3000,
        "race_date": "20260712",
    }

    assert (
        router.resolve_variant("jra", [other_extended]) == "joken_703_other_extended_qsm_gated_top1"
    )
    assert (
        router.resolve_variant("jra", [{**other_extended, "grade_code": "E"}])
        == "jockey_pedigree_703"
    )
    assert router.resolve_variant("jra", [{**other_extended, "track_code": "10"}]) == "joken_703"
    assert router.resolve_variant("jra", [{**other_extended, "kyori": 2200}]) == "joken_703"
    assert router.resolve_variant("jra", [{**other_extended, "track_code": None}]) == "joken_703"
    assert router.resolve_variant("jra", [{**other_extended, "kyori": None}]) == "joken_703"


def test_load_cell_router_real_config_prioritizes_703_dirt_sprint_gate() -> None:
    router = load_cell_router()
    specialist = router.routing_for("jra").variants["joken_703_dirt_sprint_yeti_gated_top1"]
    assert specialist.model_version == "jra-joken-703-dirt-sprint-yeti-gated-v1"
    assert specialist.routing_mode == "jra_variant_top1_swap"
    assert specialist.base_variant == "joken_703"
    assert specialist.minimum_candidate_margin == 0.02
    assert specialist.minimum_candidate_top_z == 1.5
    assert specialist.maximum_candidate_v2_rank == 20
    dirt_sprint = {
        "grade_code": None,
        "kyoso_joken_code": "703",
        "track_code": "23",
        "kyori": 1000,
        "race_date": "20260112",
    }

    assert router.resolve_variant("jra", [dirt_sprint]) == "joken_703_dirt_sprint_yeti_gated_top1"
    assert (
        router.resolve_variant("jra", [{**dirt_sprint, "grade_code": "E"}]) == "jockey_pedigree_703"
    )
    assert router.resolve_variant("jra", [{**dirt_sprint, "track_code": "10"}]) == "joken_703"
    assert router.resolve_variant("jra", [{**dirt_sprint, "kyori": 1200}]) == "joken_703"
    assert router.resolve_variant("jra", [{**dirt_sprint, "track_code": None}]) == "joken_703"
    assert router.resolve_variant("jra", [{**dirt_sprint, "kyori": None}]) == "joken_703"


def test_load_cell_router_real_config_routes_every_ungraded_joken_code() -> None:
    router = load_cell_router()
    routing = router.routing_for("jra")
    for code in ("005", "010", "016", "701", "703", "999"):
        variant = f"joken_{code}"
        assert routing.variants[variant].model_version == f"jra-joken-{code}-pooled-yetirank-v2"
        assert routing.variants[variant].feature_count == 113
        entry = {"grade_code": None, "kyoso_joken_code": code}
        assert router.resolve_variant("jra", [entry]) == variant


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
    assert router.resolve_variant("jra", hit_entries) == "joken_005"
    assert router.resolve_variant("jra", miss_field_entries) == "joken_005"
    assert router.resolve_variant("jra", miss_class_entries) == "joken_703"


def test_load_cell_router_real_config_has_jra_hakodate_venue_routing() -> None:
    router = load_cell_router()
    routing = router.routing_for("jra")
    assert len(routing.rules) == 31
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
    class_703_at_hakodate = {
        "grade_code": "E",
        "kyoso_joken_code": "703",
        "keibajo_code": "02",
    }
    assert router.resolve_variant("jra", [class_703_at_hakodate]) == "jockey_pedigree_703"

    # dirt / f_le10 / kyoso_joken_code=005 at a non-Hakodate venue (Kokura,
    # 10) still routes to the prior-corner variant via rule 2.
    prior_corner_at_kokura = {
        "grade_code": "E",
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
    hakodate_generic = {
        "grade_code": "E",
        "keibajo_code": "02",
        "kyoso_joken_code": "701",
    }
    assert router.resolve_variant("jra", [hakodate_generic]) == "jockey_pedigree_703"

    non_hakodate_generic = {
        "grade_code": "E",
        "keibajo_code": "05",
        "kyoso_joken_code": "701",
    }
    assert router.resolve_variant("jra", [non_hakodate_generic]) == "sim"


def test_load_cell_router_real_config_unmatched_nar_race_uses_default() -> None:
    router = load_cell_router()
    assert router.has_routing("nar") is True
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
        {"track_code": "23", "kyoso_joken_code": "005"} for _ in range(8)
    ]  # track_code 23 -> dirt (see derive_surface); no shusso_tosu key at all.
    assert router.resolve_variant("jra", entries) == "prior_corner_dirt_smallfield_005"


def test_resolve_variant_prior_corner_smallfield_does_not_fire_for_large_field() -> None:
    router = _jra_prior_corner_router()
    entries = [{"track_code": "23", "kyoso_joken_code": "005"} for _ in range(16)]
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


# Boundary regression tests for the 2026-07-17 bug-regression-test audit
# (item K): track_code 20/21/22 are turf-course configurations (confirmed
# against the local PG mirror's actual race identities -- track_code=20 is
# 天皇賞(春) at Kyoto, track_code=21 is スポーツニッポン賞ステイヤーズ
# ステークス at Nakayama, both long-distance graded turf races), not dirt.
# The previous "track_code.startswith('2')" implementation misclassified all
# three as dirt. These pin the exact turf/dirt boundary (19 = last turf code
# before the gap, 20/21/22 = the previously-misclassified turf codes,
# 23 = first dirt code) so a future regression to the naive prefix check
# fails here immediately.
def test_derived_surface_track_code_19_is_turf() -> None:
    assert derive_surface("19", "jra") == "turf"


def test_derived_surface_track_code_20_is_turf_not_dirt() -> None:
    assert derive_surface("20", "jra") == "turf"


def test_derived_surface_track_code_21_is_turf_not_dirt() -> None:
    assert derive_surface("21", "jra") == "turf"


def test_derived_surface_track_code_22_is_turf_not_dirt() -> None:
    assert derive_surface("22", "jra") == "turf"


def test_derived_surface_track_code_23_is_dirt() -> None:
    assert derive_surface("23", "jra") == "dirt"


def test_derived_surface_track_code_29_is_dirt() -> None:
    assert derive_surface("29", "jra") == "dirt"


def test_derived_surface_track_code_30_is_other() -> None:
    assert derive_surface("30", "jra") == "other"


# Cross-package parity test: subgroup_diagnostics.py (apps/pc-keiba-viewer)
# independently derives the SAME surface classification for cell-level
# accuracy evaluation, using an explicit range-based JRA_TURF_CODES (10-22) /
# JRA_DIRT_CODES (23-29) frozenset construction rather than this module's
# (now range-matching) logic. The two implementations are NOT allowed to
# import each other in production (separate deployable packages, cell_router
# is pure-stdlib and predict_lib has no polars/pandas dependency
# subgroup_diagnostics.py would pull in) -- this test is the one place they
# are cross-checked, importing pc-keiba-viewer's module directly (safe here:
# get_surface_label/JRA_TURF_CODES/JRA_DIRT_CODES have no import-time
# dependency beyond polars, already a transitive test dependency is not
# required since only the surface function itself, not the polars-using
# expression builder, is exercised). Found via the 2026-07-17 audit: this
# repo's system doc §6 claims the routing and eval-store cell definitions
# are consistent -- they were not, for exactly the 20/21/22 boundary this
# test pins for every JRA track_code from 00 to 99.
#
# NOTE: the actual cross-package parity check lives in
# apps/pc-keiba-viewer/tests/test_subgroup_diagnostics.py, not here.
# subgroup_diagnostics.py depends on polars (needed for its
# expression-builder path), which is not a finish-position-predict-container
# dependency (this container is deliberately lean -- it does not do any
# dataframe work) and is not something this package's test suite should pull
# in just to exercise one small pure function. cell_router.py's own
# derive_surface (and its transitive imports: model_meta.py, race_id.py) is
# pure-stdlib, so the cross-import works safely in the OTHER direction --
# see that test file for the actual parity assertion.


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


def test_derived_class_empty_grade_uses_kyoso_joken_code() -> None:
    assert derive_class("", "703") == "joken-703"
    assert derive_class("  ", " 005 ") == "joken-005"


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


def testresolve_dimension_class_uses_kyoso_joken_code_when_grade_is_missing() -> None:
    entry = {"grade_code": None, "kyoso_joken_code": "703"}
    assert resolve_dimension(entry, "class", "jra") == "joken-703"


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


def test_derive_card_max_race_bango_by_card_single_card() -> None:
    race_ids = [
        "nar:2026:0712:54:01",
        "nar:2026:0712:54:10",
        "nar:2026:0712:54:05",
    ]
    assert derive_card_max_race_bango_by_card(race_ids) == {("2026", "0712", "54"): 10}


def test_derive_card_max_race_bango_by_card_groups_by_card() -> None:
    race_ids = [
        "nar:2026:0712:54:10",
        "nar:2026:0712:30:12",
        "nar:2026:0711:54:08",
    ]
    assert derive_card_max_race_bango_by_card(race_ids) == {
        ("2026", "0712", "54"): 10,
        ("2026", "0712", "30"): 12,
        ("2026", "0711", "54"): 8,
    }


def test_derive_card_max_race_bango_by_card_skips_malformed_race_id() -> None:
    race_ids = ["nar:2026:0712", "nar:2026:0712:54:10"]
    assert derive_card_max_race_bango_by_card(race_ids) == {("2026", "0712", "54"): 10}


def test_derive_card_max_race_bango_by_card_skips_non_digit_race_bango() -> None:
    race_ids = ["nar:2026:0712:54:xx", "nar:2026:0712:54:07"]
    assert derive_card_max_race_bango_by_card(race_ids) == {("2026", "0712", "54"): 7}


def test_derive_card_max_race_bango_by_card_empty_input() -> None:
    assert derive_card_max_race_bango_by_card([]) == {}


def test_card_max_race_bango_for_race_id_hit() -> None:
    by_card = {("2026", "0712", "54"): 10}
    assert card_max_race_bango_for_race_id("nar:2026:0712:54:05", by_card) == 10


def test_card_max_race_bango_for_race_id_miss_returns_none() -> None:
    by_card = {("2026", "0712", "54"): 10}
    assert card_max_race_bango_for_race_id("nar:2026:0711:54:05", by_card) is None


def test_card_max_race_bango_for_race_id_malformed_returns_none() -> None:
    by_card = {("2026", "0712", "54"): 10}
    assert card_max_race_bango_for_race_id("not-a-race-id", by_card) is None


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


def test_canonical_cell_boundaries() -> None:
    assert derive_canonical_distance_band(1400) == "sprint"
    assert derive_canonical_distance_band(1401) == "mile"
    assert derive_canonical_distance_band(1800) == "mile"
    assert derive_canonical_distance_band(1801) == "intermediate"
    assert derive_canonical_distance_band(2201) == "long"
    assert derive_canonical_distance_band(2801) == "extended"
    assert derive_canonical_field_size_band(8) == "small"
    assert derive_canonical_field_size_band(9) == "medium"
    assert derive_canonical_field_size_band(14) == "medium"
    assert derive_canonical_field_size_band(15) == "large"


def test_resolve_canonical_dimensions_uses_live_field_size() -> None:
    entry = {"kyori": 1400, "shusso_tosu": None}
    assert resolve_dimension(entry, "canonical_distance_band", "nar") == "sprint"
    assert resolve_dimension(entry, "canonical_field_size_band", "nar", field_size=9) == "medium"
    assert resolve_dimension(entry, "canonical_field_size_band", "nar") is None


@pytest.mark.parametrize(
    ("entry", "effective_after", "expected"),
    [
        ({"kaisai_nen": "2026", "kaisai_tsukihi": "0701"}, "2026-06-30", True),
        ({"kaisai_nen": "2026", "kaisai_tsukihi": "0630"}, "2026-06-30", False),
        ({"race_id": "nar:2026:0701:30:01"}, "2026-06-30", True),
        ({"race_id": "bad"}, "2026-06-30", False),
        ({}, "bad", False),
        ({}, None, True),
    ],
)
def test_rule_is_effective(
    entry: dict[str, object], effective_after: str | None, expected: bool
) -> None:
    assert rule_is_effective(entry, effective_after) is expected


def test_real_nar_mukatsu_condition2_routes_to_top2_specialist() -> None:
    router = load_cell_router()
    routing = router.routing_for("nar")
    specialist = routing.variants["mukatsu30_tc2_top2"]
    assert specialist.routing_mode == "nar_transformer_top2_swap"
    assert specialist.minimum_candidate_margin == 0.2
    common = {
        "keibajo_code": "30",
        "nar_subclass": "MUKATSU",
        "kyori": 1200,
        "kaisai_nen": "2026",
        "kaisai_tsukihi": "0827",
        "track_code": "23",
        "current_baba_condition": "2",
    }
    medium = [common for _ in range(9)]
    assert router.resolve_variant("nar", medium) == "mukatsu30_tc2_top2"
    assert router.resolve_variant("nar", medium[:8]) == "sim"


def test_real_nar_sonoda_condition2_routes_to_top2_consensus() -> None:
    router = load_cell_router()
    routing = router.routing_for("nar")
    specialist = routing.variants["c50_tc2_consensus"]
    assert specialist.routing_mode == "nar_transformer_top2_consensus_swap"
    assert specialist.consensus_variants == (
        "c50_tc2_consensus_market",
        "c50_tc2_consensus_pedigree",
    )
    assert specialist.consensus_required_votes == 2
    common = {
        "keibajo_code": "50",
        "nar_subclass": "C",
        "kyori": 1200,
        "kaisai_nen": "2026",
        "kaisai_tsukihi": "0827",
        "track_code": "23",
        "current_baba_condition": "2",
    }
    medium = [common for _ in range(9)]
    assert router.resolve_variant("nar", medium) == "c50_tc2_consensus"
    assert router.resolve_variant("nar", medium[:8]) == "sim"


def test_real_nar_top1_routes_are_date_and_cell_gated() -> None:
    router = load_cell_router()
    routing = router.routing_for("nar")
    assert routing.variants["c30_tc2_adaptive"].routing_mode == "nar_transformer_top1_swap"
    common = {
        "keibajo_code": "30",
        "nar_subclass": "C",
        "kyori": 1200,
        "kaisai_nen": "2026",
        "track_code": "23",
        "current_baba_condition": "2",
    }
    before = [{**common, "kaisai_tsukihi": "0630"} for _ in range(9)]
    after = [{**common, "kaisai_tsukihi": "0701"} for _ in range(9)]
    assert router.resolve_variant("nar", before) == "sim"
    assert router.resolve_variant("nar", after) == "c30_tc2_adaptive"
    assert router.resolve_variant("nar", after[:8]) == "sim"
