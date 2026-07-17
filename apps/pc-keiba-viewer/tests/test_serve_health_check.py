"""Tests for serve_health_check module.

Covers all pure-compute helpers directly (no I/O), plus thin
integration-style tests for the I/O wrapper functions with mocked
psycopg cursors/connections and mocked subprocess calls. No test opens a
real network connection or spawns a real subprocess.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from pytest import CaptureFixture

import serve_health_check as subject

# ── format_race_id ───────────────────────────────────────────────────────────


def test_format_race_id_builds_colon_separated_string() -> None:
    result = subject.format_race_id("jra", ("2026", "0712", "02", "01"))
    assert result == "jra:2026:0712:02:01"


# ── parse_post_time_jst ──────────────────────────────────────────────────────


def test_parse_post_time_jst_none_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0711", None) is None


def test_parse_post_time_jst_empty_string_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0711", "") is None


def test_parse_post_time_jst_placeholder_0000_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0711", "0000") is None


def test_parse_post_time_jst_wrong_length_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0711", "950") is None


def test_parse_post_time_jst_non_digit_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0711", "12ab") is None


def test_parse_post_time_jst_hour_out_of_range_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0711", "2401") is None


def test_parse_post_time_jst_minute_out_of_range_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0711", "0960") is None


def test_parse_post_time_jst_invalid_calendar_date_returns_none() -> None:
    assert subject.parse_post_time_jst("2026", "0230", "0950") is None


def test_parse_post_time_jst_valid_returns_aware_jst_datetime() -> None:
    result = subject.parse_post_time_jst("2026", "0711", "0950")
    assert result == datetime(2026, 7, 11, 9, 50, 0, tzinfo=subject.JST)


# ── is_post_time_passed ──────────────────────────────────────────────────────


def test_is_post_time_passed_none_returns_false() -> None:
    now = datetime(2026, 7, 12, 10, 0, 0, tzinfo=subject.JST)
    assert subject.is_post_time_passed(None, now) is False


def test_is_post_time_passed_strictly_before_now_returns_true() -> None:
    post_time = datetime(2026, 7, 12, 9, 0, 0, tzinfo=subject.JST)
    now = datetime(2026, 7, 12, 10, 0, 0, tzinfo=subject.JST)
    assert subject.is_post_time_passed(post_time, now) is True


def test_is_post_time_passed_equal_to_now_returns_true() -> None:
    now = datetime(2026, 7, 12, 10, 0, 0, tzinfo=subject.JST)
    assert subject.is_post_time_passed(now, now) is True


def test_is_post_time_passed_after_now_returns_false() -> None:
    post_time = datetime(2026, 7, 12, 15, 0, 0, tzinfo=subject.JST)
    now = datetime(2026, 7, 12, 10, 0, 0, tzinfo=subject.JST)
    assert subject.is_post_time_passed(post_time, now) is False


# ── find_coverage_gaps ───────────────────────────────────────────────────────


def test_find_coverage_gaps_empty_confirmed_races_returns_empty() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    assert subject.find_coverage_gaps([], set(), now) == []


def test_find_coverage_gaps_missing_prediction_after_post_time_is_a_gap() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed = [
        ("2026", "0712", "02", "01", "1010", "10", 1200, 16, "A", "703"),
    ]
    gaps = subject.find_coverage_gaps(confirmed, set(), now)
    assert gaps == [("2026", "0712", "02", "01")]


def test_find_coverage_gaps_race_with_prediction_is_not_a_gap() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed = [
        ("2026", "0712", "02", "01", "1010", "10", 1200, 16, "A", "703"),
    ]
    predicted = {("2026", "0712", "02", "01")}
    gaps = subject.find_coverage_gaps(confirmed, predicted, now)
    assert gaps == []


def test_find_coverage_gaps_future_post_time_is_not_a_gap() -> None:
    # Post time (23:50) is later today than "now" (20:00) -- not yet served,
    # not a gap.
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed = [
        ("2026", "0712", "02", "12", "2350", "10", 1200, 16, "A", "703"),
    ]
    gaps = subject.find_coverage_gaps(confirmed, set(), now)
    assert gaps == []


def test_find_coverage_gaps_unparseable_post_time_fails_closed_not_a_gap() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed = [
        ("2026", "0712", "02", "01", None, "10", 1200, 16, "A", "703"),
    ]
    gaps = subject.find_coverage_gaps(confirmed, set(), now)
    assert gaps == []


def test_find_coverage_gaps_multiple_races_mixed() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed = [
        ("2026", "0712", "02", "01", "1010", "10", 1200, 16, "A", "703"),  # gap
        ("2026", "0712", "02", "02", "1040", "10", 1200, 16, "A", "703"),  # covered
        ("2026", "0712", "03", "01", "2350", "10", 1200, 16, "A", "703"),  # future
    ]
    predicted = {("2026", "0712", "02", "02")}
    gaps = subject.find_coverage_gaps(confirmed, predicted, now)
    assert gaps == [("2026", "0712", "02", "01")]


# ── distinct_race_keys ───────────────────────────────────────────────────────


def test_distinct_race_keys_empty_returns_empty_set() -> None:
    assert subject.distinct_race_keys([], "2026", "0712") == set()


def test_distinct_race_keys_dedups_across_model_versions() -> None:
    rows: list[subject.PredictionRow] = [
        ("02", "01", "champion", 1.2),
        ("02", "01", "jockey-pedigree269", 0.1),
    ]
    result = subject.distinct_race_keys(rows, "2026", "0712")
    assert result == {("2026", "0712", "02", "01")}


def test_distinct_race_keys_separates_different_races() -> None:
    rows: list[subject.PredictionRow] = [
        ("02", "01", "champion", 1.2),
        ("03", "02", "champion", 1.1),
    ]
    result = subject.distinct_race_keys(rows, "2026", "0712")
    assert result == {("2026", "0712", "02", "01"), ("2026", "0712", "03", "02")}


# ── group_quality_rows ───────────────────────────────────────────────────────


def test_group_quality_rows_empty_returns_empty_dict() -> None:
    assert subject.group_quality_rows([], "2026", "0712") == {}


def test_group_quality_rows_groups_by_race_and_model_version() -> None:
    rows: list[subject.PredictionRow] = [
        ("02", "01", "champion", 1.2),
        ("02", "01", "champion", 0.8),
    ]
    result = subject.group_quality_rows(rows, "2026", "0712")
    assert result == {("2026", "0712", "02", "01", "champion"): [1.2, 0.8]}


def test_group_quality_rows_separates_different_model_versions_same_race() -> None:
    # The core Cluster A/B scenario: the SAME race carries rows under TWO
    # different model_versions -- these must land in separate groups, not
    # be blended together.
    rows: list[subject.PredictionRow] = [
        ("02", "01", "jra-cb-v9-sim-2013-clean", 1.4),
        ("02", "01", "jra-cb-v9-sim-2013-clean-jockey-pedigree269", 0.1),
    ]
    result = subject.group_quality_rows(rows, "2026", "0711")
    assert result == {
        ("2026", "0711", "02", "01", "jra-cb-v9-sim-2013-clean"): [1.4],
        ("2026", "0711", "02", "01", "jra-cb-v9-sim-2013-clean-jockey-pedigree269"): [0.1],
    }


def test_group_quality_rows_skips_none_predicted_score() -> None:
    rows: list[subject.PredictionRow] = [
        ("02", "01", "champion", 1.2),
        ("02", "01", "champion", None),
    ]
    result = subject.group_quality_rows(rows, "2026", "0712")
    assert result == {("2026", "0712", "02", "01", "champion"): [1.2]}


# ── compute_quality_groups ───────────────────────────────────────────────────


def test_compute_quality_groups_empty_returns_empty_list() -> None:
    assert subject.compute_quality_groups({}) == []


def test_compute_quality_groups_low_stddev_flagged_degraded() -> None:
    groups = {("2026", "0712", "02", "01", "champion"): [0.10, 0.12, 0.09, 0.11]}
    result = subject.compute_quality_groups(groups)
    assert len(result) == 1
    assert result[0].degraded is True
    assert result[0].sample_count == 4


def test_compute_quality_groups_high_stddev_not_flagged() -> None:
    groups = {("2026", "0711", "02", "01", "champion"): [0.5, 2.5, 1.0, 1.8]}
    result = subject.compute_quality_groups(groups)
    assert len(result) == 1
    assert result[0].degraded is False


def test_compute_quality_groups_boundary_exactly_at_threshold_not_degraded() -> None:
    # Construct a group whose population stddev is exactly 0.3: values
    # [-0.3, 0.3] around a mean of 0 -> pstdev == 0.3 exactly.
    groups = {("2026", "0712", "02", "01", "champion"): [-0.3, 0.3]}
    result = subject.compute_quality_groups(groups)
    assert result[0].stddev == pytest.approx(0.3)
    assert result[0].degraded is False


def test_compute_quality_groups_single_value_group_is_degraded() -> None:
    groups = {("2026", "0712", "02", "01", "champion"): [1.5]}
    result = subject.compute_quality_groups(groups)
    assert result[0].stddev == pytest.approx(0.0)
    assert result[0].degraded is True


# ── rollup_quality_by_race ───────────────────────────────────────────────────


def test_rollup_quality_by_race_empty_returns_empty_list() -> None:
    assert subject.rollup_quality_by_race([]) == []


def test_rollup_quality_by_race_all_healthy() -> None:
    groups = [
        subject.QualityGroupResult(
            kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
            model_version="champion", sample_count=16, stddev=1.2, degraded=False,
        ),
    ]
    result = subject.rollup_quality_by_race(groups)
    assert result == [
        subject.RaceQualityRollup(
            kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
            status=subject.RACE_STATUS_FULLY_HEALTHY,
        ),
    ]


def test_rollup_quality_by_race_all_degraded() -> None:
    groups = [
        subject.QualityGroupResult(
            kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="02", race_bango="01",
            model_version="champion", sample_count=16, stddev=0.09, degraded=True,
        ),
    ]
    result = subject.rollup_quality_by_race(groups)
    assert result[0].status == subject.RACE_STATUS_FULLY_DEGRADED


def test_rollup_quality_by_race_mixed_is_partially_degraded() -> None:
    # This is the core 2026-07-11 validation scenario: the SAME race has one
    # healthy group (default model) and one degraded group (routed variant).
    groups = [
        subject.QualityGroupResult(
            kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
            model_version="jra-cb-v9-sim-2013-clean", sample_count=16, stddev=1.4, degraded=False,
        ),
        subject.QualityGroupResult(
            kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
            model_version="jra-cb-v9-sim-2013-clean-jockey-pedigree269", sample_count=16,
            stddev=0.1, degraded=True,
        ),
    ]
    result = subject.rollup_quality_by_race(groups)
    assert len(result) == 1
    assert result[0].status == subject.RACE_STATUS_PARTIALLY_DEGRADED


def test_rollup_quality_by_race_multiple_races_independent() -> None:
    groups = [
        subject.QualityGroupResult(
            kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="02", race_bango="01",
            model_version="champion", sample_count=16, stddev=0.09, degraded=True,
        ),
        subject.QualityGroupResult(
            kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="03", race_bango="02",
            model_version="champion", sample_count=14, stddev=1.1, degraded=False,
        ),
    ]
    result = subject.rollup_quality_by_race(groups)
    statuses = {(r.keibajo_code, r.race_bango): r.status for r in result}
    assert statuses == {
        ("02", "01"): subject.RACE_STATUS_FULLY_DEGRADED,
        ("03", "02"): subject.RACE_STATUS_FULLY_HEALTHY,
    }


# ── derive_surface ────────────────────────────────────────────────────────────


def test_derive_surface_jra_track_code_starts_1_is_turf() -> None:
    assert subject.derive_surface("10", "jra") == "turf"


def test_derive_surface_jra_track_code_starts_2_is_dirt() -> None:
    assert subject.derive_surface("23", "jra") == "dirt"


def test_derive_surface_jra_track_code_other_prefix_is_other() -> None:
    assert subject.derive_surface("51", "jra") == "other"


def test_derive_surface_non_jra_always_dirt() -> None:
    assert subject.derive_surface("10", "nar") == "dirt"


# ── derive_distance_band ──────────────────────────────────────────────────────


def test_derive_distance_band_sprint() -> None:
    assert subject.derive_distance_band(1199) == "sprint"


def test_derive_distance_band_mile_lower_boundary() -> None:
    assert subject.derive_distance_band(1200) == "mile"


def test_derive_distance_band_mile_upper() -> None:
    assert subject.derive_distance_band(1599) == "mile"


def test_derive_distance_band_intermediate_lower_boundary() -> None:
    assert subject.derive_distance_band(1600) == "intermediate"


def test_derive_distance_band_intermediate_upper() -> None:
    assert subject.derive_distance_band(1999) == "intermediate"


def test_derive_distance_band_long_lower_boundary() -> None:
    assert subject.derive_distance_band(2000) == "long"


def test_derive_distance_band_long_upper() -> None:
    assert subject.derive_distance_band(2399) == "long"


def test_derive_distance_band_extended_lower_boundary() -> None:
    assert subject.derive_distance_band(2400) == "extended"


# ── derive_field_band ─────────────────────────────────────────────────────────


def test_derive_field_band_le10_boundary() -> None:
    assert subject.derive_field_band(10) == "f_le10"


def test_derive_field_band_11_13_lower_boundary() -> None:
    assert subject.derive_field_band(11) == "f11_13"


def test_derive_field_band_11_13_upper_boundary() -> None:
    assert subject.derive_field_band(13) == "f11_13"


def test_derive_field_band_14_15_lower_boundary() -> None:
    assert subject.derive_field_band(14) == "f14_15"


def test_derive_field_band_14_15_upper_boundary() -> None:
    assert subject.derive_field_band(15) == "f14_15"


def test_derive_field_band_16_plus() -> None:
    assert subject.derive_field_band(16) == "f16p"


# ── derive_season ─────────────────────────────────────────────────────────────


def test_derive_season_spring() -> None:
    assert subject.derive_season(4) == "spring"


def test_derive_season_summer() -> None:
    assert subject.derive_season(7) == "summer"


def test_derive_season_autumn() -> None:
    assert subject.derive_season(10) == "autumn"


def test_derive_season_winter() -> None:
    assert subject.derive_season(1) == "winter"


# ── derive_class ──────────────────────────────────────────────────────────────


def test_derive_class_returns_grade_code_when_present() -> None:
    assert subject.derive_class("A") == "A"


def test_derive_class_returns_unknown_when_empty() -> None:
    assert subject.derive_class("") == "unknown"


# ── resolve_routing_dimension ────────────────────────────────────────────────


def test_resolve_routing_dimension_venue_present() -> None:
    entry: dict[str, object] = {"keibajo_code": "02"}
    assert subject.resolve_routing_dimension(entry, "venue", "jra") == "02"


def test_resolve_routing_dimension_venue_missing_returns_none() -> None:
    entry: dict[str, object] = {}
    assert subject.resolve_routing_dimension(entry, "venue", "jra") is None


def test_resolve_routing_dimension_surface_present() -> None:
    entry: dict[str, object] = {"track_code": "10"}
    assert subject.resolve_routing_dimension(entry, "surface", "jra") == "turf"


def test_resolve_routing_dimension_surface_missing_returns_none() -> None:
    entry: dict[str, object] = {}
    assert subject.resolve_routing_dimension(entry, "surface", "jra") is None


def test_resolve_routing_dimension_distance_band_present() -> None:
    entry: dict[str, object] = {"kyori": 1200}
    assert subject.resolve_routing_dimension(entry, "distance_band", "jra") == "mile"


def test_resolve_routing_dimension_distance_band_missing_returns_none() -> None:
    entry: dict[str, object] = {}
    assert subject.resolve_routing_dimension(entry, "distance_band", "jra") is None


def test_resolve_routing_dimension_field_band_present() -> None:
    entry: dict[str, object] = {"shusso_tosu": 16}
    assert subject.resolve_routing_dimension(entry, "field_band", "jra") == "f16p"


def test_resolve_routing_dimension_field_band_missing_returns_none() -> None:
    entry: dict[str, object] = {}
    assert subject.resolve_routing_dimension(entry, "field_band", "jra") is None


def test_resolve_routing_dimension_season_present() -> None:
    entry: dict[str, object] = {"kaisai_tsukihi": "0712"}
    assert subject.resolve_routing_dimension(entry, "season", "jra") == "summer"


def test_resolve_routing_dimension_season_missing_returns_none() -> None:
    entry: dict[str, object] = {}
    assert subject.resolve_routing_dimension(entry, "season", "jra") is None


def test_resolve_routing_dimension_season_non_digit_returns_none() -> None:
    entry: dict[str, object] = {"kaisai_tsukihi": "xx12"}
    assert subject.resolve_routing_dimension(entry, "season", "jra") is None


def test_resolve_routing_dimension_class_present() -> None:
    entry: dict[str, object] = {"grade_code": "A"}
    assert subject.resolve_routing_dimension(entry, "class", "jra") == "A"


def test_resolve_routing_dimension_class_missing_returns_none() -> None:
    entry: dict[str, object] = {}
    assert subject.resolve_routing_dimension(entry, "class", "jra") is None


def test_resolve_routing_dimension_generic_fallback_present() -> None:
    entry: dict[str, object] = {"kyoso_joken_code": "703"}
    assert subject.resolve_routing_dimension(entry, "kyoso_joken_code", "jra") == "703"


def test_resolve_routing_dimension_generic_fallback_missing_returns_none() -> None:
    entry: dict[str, object] = {}
    assert subject.resolve_routing_dimension(entry, "kyoso_joken_code", "jra") is None


def test_resolve_routing_dimension_unsupported_is_final_race_falls_back_to_none() -> None:
    # No live rule uses is_final_race and this tool has no card-level
    # context -- the entry dict never carries that key, so the generic
    # fallback returns None (fail-closed), matching the real
    # resolve_dimension's own behaviour whenever card_max_race_bango is
    # unavailable.
    entry: dict[str, object] = {"keibajo_code": "02"}
    assert subject.resolve_routing_dimension(entry, "is_final_race", "jra") is None


# ── all_routing_conditions_match ─────────────────────────────────────────────


def test_all_routing_conditions_match_empty_conditions_is_true() -> None:
    entry: dict[str, object] = {}
    assert subject.all_routing_conditions_match(entry, (), "jra") is True


def test_all_routing_conditions_match_single_condition_matches() -> None:
    entry: dict[str, object] = {"keibajo_code": "02"}
    conditions = (subject.RoutingCondition(dimension="venue", values=frozenset({"02"})),)
    assert subject.all_routing_conditions_match(entry, conditions, "jra") is True


def test_all_routing_conditions_match_single_condition_value_not_in_set() -> None:
    entry: dict[str, object] = {"keibajo_code": "05"}
    conditions = (subject.RoutingCondition(dimension="venue", values=frozenset({"02"})),)
    assert subject.all_routing_conditions_match(entry, conditions, "jra") is False


def test_all_routing_conditions_match_dimension_resolves_none_is_false() -> None:
    entry: dict[str, object] = {}
    conditions = (subject.RoutingCondition(dimension="venue", values=frozenset({"02"})),)
    assert subject.all_routing_conditions_match(entry, conditions, "jra") is False


def test_all_routing_conditions_match_requires_all_conditions_true() -> None:
    entry: dict[str, object] = {"track_code": "20", "shusso_tosu": 8, "kyoso_joken_code": "005"}
    conditions = (
        subject.RoutingCondition(dimension="surface", values=frozenset({"dirt"})),
        subject.RoutingCondition(dimension="field_band", values=frozenset({"f_le10"})),
        subject.RoutingCondition(dimension="kyoso_joken_code", values=frozenset({"005"})),
    )
    assert subject.all_routing_conditions_match(entry, conditions, "jra") is True


def test_all_routing_conditions_match_one_of_several_conditions_fails() -> None:
    entry: dict[str, object] = {"track_code": "20", "shusso_tosu": 16, "kyoso_joken_code": "005"}
    conditions = (
        subject.RoutingCondition(dimension="surface", values=frozenset({"dirt"})),
        subject.RoutingCondition(dimension="field_band", values=frozenset({"f_le10"})),
        subject.RoutingCondition(dimension="kyoso_joken_code", values=frozenset({"005"})),
    )
    assert subject.all_routing_conditions_match(entry, conditions, "jra") is False


# ── resolve_expected_model_version ───────────────────────────────────────────


def _sample_jra_routing_config() -> subject.CategoryRoutingConfig:
    """Mirrors the real cell_routing.json's "jra" entry structure/values."""
    return subject.CategoryRoutingConfig(
        default_variant="sim",
        variant_model_versions={
            "sim": "jra-cb-v9-sim-2013-clean",
            "jockey_pedigree_703": "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
            "prior_corner_dirt_smallfield_005": "jra-cb-v10-prior-corner274-2013",
        },
        rules=(
            subject.RoutingRule(
                conditions=(
                    subject.RoutingCondition(dimension="kyoso_joken_code", values=frozenset({"703"})),
                ),
                variant="jockey_pedigree_703",
            ),
            subject.RoutingRule(
                conditions=(
                    subject.RoutingCondition(dimension="surface", values=frozenset({"dirt"})),
                    subject.RoutingCondition(dimension="field_band", values=frozenset({"f_le10"})),
                    subject.RoutingCondition(dimension="kyoso_joken_code", values=frozenset({"005"})),
                ),
                variant="prior_corner_dirt_smallfield_005",
            ),
            subject.RoutingRule(
                conditions=(
                    subject.RoutingCondition(dimension="venue", values=frozenset({"02"})),
                ),
                variant="jockey_pedigree_703",
            ),
        ),
    )


def test_resolve_expected_model_version_first_rule_matches() -> None:
    config = _sample_jra_routing_config()
    entry: dict[str, object] = {"kyoso_joken_code": "703", "keibajo_code": "05"}
    result = subject.resolve_expected_model_version(config, entry, "jra")
    assert result == "jra-cb-v9-sim-2013-clean-jockey-pedigree269"


def test_resolve_expected_model_version_venue_rule_matches_when_earlier_rules_dont() -> None:
    config = _sample_jra_routing_config()
    entry: dict[str, object] = {
        "kyoso_joken_code": "016", "keibajo_code": "02", "track_code": "10", "shusso_tosu": 16,
    }
    result = subject.resolve_expected_model_version(config, entry, "jra")
    assert result == "jra-cb-v9-sim-2013-clean-jockey-pedigree269"


def test_resolve_expected_model_version_no_rule_matches_returns_default() -> None:
    config = _sample_jra_routing_config()
    entry: dict[str, object] = {
        "kyoso_joken_code": "016", "keibajo_code": "05", "track_code": "10", "shusso_tosu": 16,
    }
    result = subject.resolve_expected_model_version(config, entry, "jra")
    assert result == "jra-cb-v9-sim-2013-clean"


def test_resolve_expected_model_version_smallfield_dirt_rule_matches() -> None:
    config = _sample_jra_routing_config()
    entry: dict[str, object] = {
        "kyoso_joken_code": "005", "keibajo_code": "05", "track_code": "20", "shusso_tosu": 8,
    }
    result = subject.resolve_expected_model_version(config, entry, "jra")
    assert result == "jra-cb-v10-prior-corner274-2013"


# ── parse_cell_routing_config ─────────────────────────────────────────────────


def test_parse_cell_routing_config_category_absent_returns_none() -> None:
    payload = {"jra": {"default_variant": "sim", "variants": {}, "rules": []}}
    assert subject.parse_cell_routing_config(payload, "nar") is None


def test_parse_cell_routing_config_parses_real_shape() -> None:
    payload = {
        "jra": {
            "default_variant": "sim",
            "variants": {
                "sim": {"model_version": "jra-cb-v9-sim-2013-clean", "feature_count": 250, "architecture": "catboost"},
                "jockey_pedigree_703": {
                    "model_version": "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
                    "feature_count": 269, "architecture": "catboost",
                },
            },
            "rules": [
                {
                    "conditions": [{"dimension": "kyoso_joken_code", "values": ["703"]}],
                    "variant": "jockey_pedigree_703",
                },
            ],
        },
    }
    config = subject.parse_cell_routing_config(payload, "jra")
    assert config is not None
    assert config.default_variant == "sim"
    assert config.variant_model_versions == {
        "sim": "jra-cb-v9-sim-2013-clean",
        "jockey_pedigree_703": "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
    }
    assert len(config.rules) == 1
    assert config.rules[0].variant == "jockey_pedigree_703"
    assert config.rules[0].conditions == (
        subject.RoutingCondition(dimension="kyoso_joken_code", values=frozenset({"703"})),
    )


def test_parse_cell_routing_config_reads_real_repo_file() -> None:
    payload = subject.load_cell_routing_json()
    config = subject.parse_cell_routing_config(payload, "jra")
    assert config is not None
    assert config.default_variant == "sim"
    assert "jra-cb-v9-sim-2013-clean" in config.variant_model_versions.values()


# ── model_versions_by_race_key ───────────────────────────────────────────────


def test_model_versions_by_race_key_empty_returns_empty_dict() -> None:
    assert subject.model_versions_by_race_key([], "2026", "0712") == {}


def test_model_versions_by_race_key_groups_multiple_versions_per_race() -> None:
    rows: list[subject.PredictionRow] = [
        ("02", "01", "champion", 1.2),
        ("02", "01", "jockey-pedigree269", 0.1),
    ]
    result = subject.model_versions_by_race_key(rows, "2026", "0711")
    assert result == {("2026", "0711", "02", "01"): {"champion", "jockey-pedigree269"}}


# ── find_routing_mismatches ──────────────────────────────────────────────────


def test_find_routing_mismatches_no_config_returns_empty() -> None:
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "02", "01", "1010", "10", 1200, 16, "A", "703"),
    ]
    result = subject.find_routing_mismatches(confirmed, {}, None, "nar")
    assert result == []


def test_find_routing_mismatches_expected_version_present_no_mismatch() -> None:
    config = _sample_jra_routing_config()
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "02", "01", "1010", "10", 1200, 16, "A", "703"),
    ]
    model_versions_by_race = {
        ("2026", "0712", "02", "01"): {"jra-cb-v9-sim-2013-clean-jockey-pedigree269"},
    }
    result = subject.find_routing_mismatches(confirmed, model_versions_by_race, config, "jra")
    assert result == []


def test_find_routing_mismatches_expected_version_missing_is_a_mismatch() -> None:
    config = _sample_jra_routing_config()
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "02", "01", "1010", "10", 1200, 16, "A", "703"),
    ]
    result = subject.find_routing_mismatches(confirmed, {}, config, "jra")
    assert result == [
        subject.RoutingMismatch(
            kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="02", race_bango="01",
            expected_model_version="jra-cb-v9-sim-2013-clean-jockey-pedigree269",
        ),
    ]


def test_find_routing_mismatches_default_variant_present_no_mismatch() -> None:
    config = _sample_jra_routing_config()
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "05", "01", "1010", "10", 1200, 16, "A", "016"),
    ]
    model_versions_by_race = {("2026", "0712", "05", "01"): {"jra-cb-v9-sim-2013-clean"}}
    result = subject.find_routing_mismatches(confirmed, model_versions_by_race, config, "jra")
    assert result == []


# ── flag_burst_buckets ────────────────────────────────────────────────────────


def test_flag_burst_buckets_empty_returns_empty() -> None:
    assert subject.flag_burst_buckets([]) == []


def test_flag_burst_buckets_count_above_threshold_flagged() -> None:
    minute = datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc)
    result = subject.flag_burst_buckets([(minute, 22)])
    assert result == [subject.BurstBucket(minute_utc=minute, race_count=22)]


def test_flag_burst_buckets_count_exactly_at_threshold_not_flagged() -> None:
    minute = datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc)
    result = subject.flag_burst_buckets([(minute, 10)])
    assert result == []


def test_flag_burst_buckets_count_below_threshold_not_flagged() -> None:
    minute = datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc)
    result = subject.flag_burst_buckets([(minute, 3)])
    assert result == []


def test_flag_burst_buckets_multiple_buckets_mixed() -> None:
    minute_a = datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc)
    minute_b = datetime(2026, 7, 12, 5, 52, 0, tzinfo=timezone.utc)
    result = subject.flag_burst_buckets([(minute_a, 22), (minute_b, 3)])
    assert result == [subject.BurstBucket(minute_utc=minute_a, race_count=22)]


# ── BurstBucket.minute_jst ───────────────────────────────────────────────────


def test_burst_bucket_minute_jst_converts_utc_to_jst() -> None:
    bucket = subject.BurstBucket(
        minute_utc=datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc), race_count=22,
    )
    assert bucket.minute_jst == datetime(2026, 7, 12, 14, 51, 0, tzinfo=subject.JST)


# ── determine_exit_code ───────────────────────────────────────────────────────


def test_determine_exit_code_all_clean_returns_0() -> None:
    assert subject.determine_exit_code(0, 0, 0, 0) == 0


def test_determine_exit_code_coverage_gap_returns_1() -> None:
    assert subject.determine_exit_code(1, 0, 0, 0) == 1


def test_determine_exit_code_degraded_quality_returns_1() -> None:
    assert subject.determine_exit_code(0, 1, 0, 0) == 1


def test_determine_exit_code_routing_mismatch_returns_1() -> None:
    assert subject.determine_exit_code(0, 0, 1, 0) == 1


def test_determine_exit_code_burst_bucket_returns_1() -> None:
    assert subject.determine_exit_code(0, 0, 0, 1) == 1


def test_determine_exit_code_all_anomalies_returns_1() -> None:
    assert subject.determine_exit_code(3, 5, 2, 1) == 1


# ── build_d1_gap_events_query ─────────────────────────────────────────────────


def test_build_d1_gap_events_query_shape() -> None:
    result = subject.build_d1_gap_events_query("20260712", "jra")
    assert result == (
        "SELECT COUNT(*) AS gap_event_count FROM finish_position_coverage_gap_events "
        "WHERE run_ymd = '20260712' AND category = 'jra'"
    )


def test_build_d1_gap_events_query_escapes_single_quote_in_category() -> None:
    result = subject.build_d1_gap_events_query("20260712", "ja'ra")
    assert "category = 'ja''ra'" in result


# ── parse_d1_gap_events_count ─────────────────────────────────────────────────


def test_parse_d1_gap_events_count_valid_shape() -> None:
    stdout = json.dumps([{"results": [{"gap_event_count": 6}], "success": True}])
    assert subject.parse_d1_gap_events_count(stdout) == 6


def test_parse_d1_gap_events_count_malformed_json_returns_none() -> None:
    assert subject.parse_d1_gap_events_count("not json{{{") is None


def test_parse_d1_gap_events_count_payload_not_a_list_returns_none() -> None:
    assert subject.parse_d1_gap_events_count(json.dumps({"results": []})) is None


def test_parse_d1_gap_events_count_empty_list_returns_none() -> None:
    assert subject.parse_d1_gap_events_count(json.dumps([])) is None


def test_parse_d1_gap_events_count_first_statement_not_a_dict_returns_none() -> None:
    assert subject.parse_d1_gap_events_count(json.dumps(["not-a-dict"])) is None


def test_parse_d1_gap_events_count_results_missing_returns_none() -> None:
    assert subject.parse_d1_gap_events_count(json.dumps([{"success": True}])) is None


def test_parse_d1_gap_events_count_results_empty_returns_none() -> None:
    assert subject.parse_d1_gap_events_count(json.dumps([{"results": []}])) is None


def test_parse_d1_gap_events_count_first_row_not_a_dict_returns_none() -> None:
    assert subject.parse_d1_gap_events_count(json.dumps([{"results": ["not-a-dict"]}])) is None


def test_parse_d1_gap_events_count_count_not_an_int_returns_none() -> None:
    stdout = json.dumps([{"results": [{"gap_event_count": "six"}]}])
    assert subject.parse_d1_gap_events_count(stdout) is None


# ── build_health_check_report ─────────────────────────────────────────────────


def _real_routing_payload() -> dict[str, object]:
    return dict(subject.load_cell_routing_json())


def test_build_health_check_report_fully_clean_day() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "05", "01", "1010", "10", 1200, 16, "A", "016"),
    ]
    predictions: list[subject.PredictionRow] = [
        ("05", "01", "jra-cb-v9-sim-2013-clean", 0.2),
        ("05", "01", "jra-cb-v9-sim-2013-clean", 0.9),
        ("05", "01", "jra-cb-v9-sim-2013-clean", 1.6),
    ]
    report = subject.build_health_check_report(
        "20260712", "jra", confirmed, predictions, [], _real_routing_payload(), 2, now,
    )
    assert report.checked_race_count == 1
    assert report.coverage_gaps == ()
    assert report.degraded_quality_groups == ()
    assert report.routing_supported is True
    assert report.routing_mismatches == ()
    assert report.burst_buckets == ()
    assert report.d1_gap_event_count == 2
    assert report.exit_code == 0


def test_build_health_check_report_coverage_gap_flips_exit_code() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "05", "01", "1010", "10", 1200, 16, "A", "016"),
    ]
    report = subject.build_health_check_report(
        "20260712", "jra", confirmed, [], [], _real_routing_payload(), None, now,
    )
    assert len(report.coverage_gaps) == 1
    assert report.exit_code == 1


def test_build_health_check_report_mixed_quality_matches_07_11_shape() -> None:
    # Mirrors the real 07-11 validation shape: ONE race carries a healthy
    # default-model group and a degraded routed-variant group at once.
    now = datetime(2026, 7, 11, 20, 0, 0, tzinfo=subject.JST)
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0711", "02", "01", "0950", "10", 1200, 16, "A", "016"),
    ]
    predictions: list[subject.PredictionRow] = [
        ("02", "01", "jra-cb-v9-sim-2013-clean", 1.4),
        ("02", "01", "jra-cb-v9-sim-2013-clean", 0.3),
        ("02", "01", "jra-cb-v9-sim-2013-clean-jockey-pedigree269", 0.10),
        ("02", "01", "jra-cb-v9-sim-2013-clean-jockey-pedigree269", 0.12),
    ]
    report = subject.build_health_check_report(
        "20260711", "jra", confirmed, predictions, [], _real_routing_payload(), 0, now,
    )
    assert len(report.quality_groups) == 2
    assert len(report.degraded_quality_groups) == 1
    assert report.quality_rollup == (
        subject.RaceQualityRollup(
            kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
            status=subject.RACE_STATUS_PARTIALLY_DEGRADED,
        ),
    )
    assert report.exit_code == 1


def test_build_health_check_report_routing_mismatch_flips_exit_code() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "02", "01", "1010", "10", 1200, 16, "A", "016"),
    ]
    predictions: list[subject.PredictionRow] = [
        ("02", "01", "jra-cb-v9-sim-2013-clean", 1.2),
    ]
    report = subject.build_health_check_report(
        "20260712", "jra", confirmed, predictions, [], _real_routing_payload(), None, now,
    )
    # venue=02 routes to jockey_pedigree_703, but only the plain default was
    # served -> mismatch.
    assert len(report.routing_mismatches) == 1
    assert report.exit_code == 1


def test_build_health_check_report_burst_flips_exit_code() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    minute = datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc)
    report = subject.build_health_check_report(
        "20260712", "jra", [], [], [(minute, 22)], _real_routing_payload(), None, now,
    )
    assert len(report.burst_buckets) == 1
    assert report.exit_code == 1


def test_build_health_check_report_routing_unsupported_category() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "54", "01", "1010", "10", 1200, 8, "A", "016"),
    ]
    report = subject.build_health_check_report(
        "20260712", "nar", confirmed, [], [], _real_routing_payload(), None, now,
    )
    assert report.routing_supported is False
    assert report.routing_mismatches == ()


def test_build_health_check_report_d1_none_passthrough() -> None:
    now = datetime(2026, 7, 12, 20, 0, 0, tzinfo=subject.JST)
    report = subject.build_health_check_report(
        "20260712", "jra", [], [], [], _real_routing_payload(), None, now,
    )
    assert report.d1_gap_event_count is None


# ── format_report / section formatters ───────────────────────────────────────


def _clean_report() -> subject.HealthCheckReport:
    return subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=1,
        coverage_gaps=(), quality_groups=(), quality_rollup=(),
        routing_supported=True, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=2,
    )


def test_format_coverage_section_ok_when_empty() -> None:
    lines = subject._format_coverage_section(_clean_report())
    assert lines == ["[1] Coverage: OK (0 gaps)"]


def test_format_coverage_section_lists_gaps() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=2,
        coverage_gaps=(("2026", "0712", "02", "05"),), quality_groups=(), quality_rollup=(),
        routing_supported=True, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=None,
    )
    lines = subject._format_coverage_section(report)
    assert lines == [
        "[1] Coverage: 1 gap(s) found",
        "      - jra:2026:0712:02:05",
    ]


def test_format_quality_section_ok_when_no_degraded() -> None:
    lines = subject._format_quality_section(_clean_report())
    assert lines == [
        "[2] Quality (Cluster-B signature): OK (0 degraded groups)",
        "      Per-race rollup: fully_healthy=0 partially_degraded=0 fully_degraded=0",
    ]


def test_format_quality_section_lists_degraded_and_rollup_counts() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=1,
        coverage_gaps=(),
        quality_groups=(
            subject.QualityGroupResult(
                kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="02", race_bango="01",
                model_version="champion", sample_count=16, stddev=0.095, degraded=True,
            ),
        ),
        quality_rollup=(
            subject.RaceQualityRollup(
                kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="02", race_bango="01",
                status=subject.RACE_STATUS_FULLY_DEGRADED,
            ),
            subject.RaceQualityRollup(
                kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="03", race_bango="02",
                status=subject.RACE_STATUS_FULLY_HEALTHY,
            ),
            subject.RaceQualityRollup(
                kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="10", race_bango="03",
                status=subject.RACE_STATUS_PARTIALLY_DEGRADED,
            ),
        ),
        routing_supported=True, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=None,
    )
    lines = subject._format_quality_section(report)
    assert lines == [
        "[2] Quality (Cluster-B signature): 1 degraded (race, model_version) group(s) found (stddev < 0.3)",
        "      - jra:2026:0712:02:01 model_version=champion stddev=0.095 n=16",
        "      Per-race rollup: fully_healthy=1 partially_degraded=1 fully_degraded=1",
    ]


def test_format_routing_section_not_supported() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="nar", checked_race_count=0,
        coverage_gaps=(), quality_groups=(), quality_rollup=(),
        routing_supported=False, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=None,
    )
    lines = subject._format_routing_section(report)
    assert lines == ["[3] Routing parity: N/A (no cell-routing rules for category=nar)"]


def test_format_routing_section_ok_when_no_mismatches() -> None:
    lines = subject._format_routing_section(_clean_report())
    assert lines == ["[3] Routing parity: OK (0 mismatches)"]


def test_format_routing_section_lists_mismatches() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=1,
        coverage_gaps=(), quality_groups=(), quality_rollup=(),
        routing_supported=True,
        routing_mismatches=(
            subject.RoutingMismatch(
                kaisai_nen="2026", kaisai_tsukihi="0712", keibajo_code="02", race_bango="03",
                expected_model_version="jra-cb-v9-sim-2013-clean-jockey-pedigree269",
            ),
        ),
        burst_buckets=(), d1_gap_event_count=None,
    )
    lines = subject._format_routing_section(report)
    assert lines == [
        "[3] Routing parity: 1 mismatch(es) found",
        "      - jra:2026:0712:02:03 expected=jra-cb-v9-sim-2013-clean-jockey-pedigree269",
    ]


def test_format_burst_section_ok_when_empty() -> None:
    lines = subject._format_burst_section(_clean_report())
    assert lines == ["[4] Burst detection: OK (0 minute-buckets > 10 races)"]


def test_format_burst_section_lists_flagged_minutes() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=0,
        coverage_gaps=(), quality_groups=(), quality_rollup=(),
        routing_supported=True, routing_mismatches=(),
        burst_buckets=(
            subject.BurstBucket(
                minute_utc=datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc), race_count=22,
            ),
        ),
        d1_gap_event_count=None,
    )
    lines = subject._format_burst_section(report)
    assert lines == [
        "[4] Burst detection: 1 minute-bucket(s) flagged",
        "      - 2026-07-12 05:51:00 UTC (2026-07-12 14:51:00 JST): 22 races",
    ]


def test_format_d1_section_unavailable() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=0,
        coverage_gaps=(), quality_groups=(), quality_rollup=(),
        routing_supported=True, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=None,
    )
    lines = subject._format_d1_section(report)
    assert lines == ["[5] D1 self-heal activity (informational): N/A (wrangler unavailable)"]


def test_format_d1_section_shows_count() -> None:
    lines = subject._format_d1_section(_clean_report())
    assert lines == ["[5] D1 self-heal activity (informational): 2 event(s)"]


def test_format_report_ok_verdict() -> None:
    report = subject.format_report(_clean_report())
    assert "=== Serve Health Check: 20260712 (JRA) ===" in report
    assert "Exit code: 0 (OK)" in report


def test_format_report_anomaly_verdict() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=1,
        coverage_gaps=(("2026", "0712", "02", "01"),), quality_groups=(), quality_rollup=(),
        routing_supported=True, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=None,
    )
    formatted = subject.format_report(report)
    assert "Exit code: 1 (anomalies found)" in formatted


# ── report_to_dict ────────────────────────────────────────────────────────────


def test_report_to_dict_json_serializable_clean_report() -> None:
    result = subject.report_to_dict(_clean_report())
    # Should not raise.
    serialized = json.dumps(result)
    assert '"exit_code": 0' in serialized
    assert result["coverage_gaps"] == []
    assert result["quality_degraded_groups"] == []


def test_report_to_dict_only_includes_degraded_groups_not_healthy() -> None:
    report = subject.HealthCheckReport(
        date_str="20260711", category="jra", checked_race_count=1,
        coverage_gaps=(),
        quality_groups=(
            subject.QualityGroupResult(
                kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
                model_version="champion", sample_count=16, stddev=1.4, degraded=False,
            ),
            subject.QualityGroupResult(
                kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
                model_version="jockey-pedigree269", sample_count=16, stddev=0.1, degraded=True,
            ),
        ),
        quality_rollup=(
            subject.RaceQualityRollup(
                kaisai_nen="2026", kaisai_tsukihi="0711", keibajo_code="02", race_bango="01",
                status=subject.RACE_STATUS_PARTIALLY_DEGRADED,
            ),
        ),
        routing_supported=True, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=None,
    )
    result = subject.report_to_dict(report)
    assert result["quality_degraded_count"] == 1
    assert len(result["quality_degraded_groups"]) == 1
    assert result["quality_degraded_groups"][0]["model_version"] == "jockey-pedigree269"
    assert len(result["quality_rollup"]) == 1
    assert result["quality_rollup"][0]["status"] == "partially_degraded"


def test_report_to_dict_burst_buckets_include_utc_and_jst() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="jra", checked_race_count=0,
        coverage_gaps=(), quality_groups=(), quality_rollup=(),
        routing_supported=True, routing_mismatches=(),
        burst_buckets=(
            subject.BurstBucket(
                minute_utc=datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc), race_count=22,
            ),
        ),
        d1_gap_event_count=None,
    )
    result = subject.report_to_dict(report)
    assert result["burst_buckets"] == [
        {
            "minute_utc": "2026-07-12T05:51:00+00:00",
            "minute_jst": "2026-07-12T14:51:00+09:00",
            "race_count": 22,
        },
    ]


def test_report_to_dict_routing_supported_false() -> None:
    report = subject.HealthCheckReport(
        date_str="20260712", category="nar", checked_race_count=0,
        coverage_gaps=(), quality_groups=(), quality_rollup=(),
        routing_supported=False, routing_mismatches=(), burst_buckets=(),
        d1_gap_event_count=None,
    )
    result = subject.report_to_dict(report)
    assert result["routing_supported"] is False


# ── parse_args ────────────────────────────────────────────────────────────────


def test_parse_args_defaults() -> None:
    args = subject.parse_args([])
    assert args.date is None
    assert args.category == "jra"
    assert args.json_output is False


def test_parse_args_explicit_date() -> None:
    args = subject.parse_args(["--date", "20260712"])
    assert args.date == "20260712"


def test_parse_args_explicit_category() -> None:
    args = subject.parse_args(["--category", "ban-ei"])
    assert args.category == "ban-ei"


def test_parse_args_json_flag() -> None:
    args = subject.parse_args(["--json"])
    assert args.json_output is True


# ── validate_date_arg ─────────────────────────────────────────────────────────


def test_validate_date_arg_valid_does_not_raise() -> None:
    subject.validate_date_arg("20260712")


def test_validate_date_arg_wrong_length_raises() -> None:
    with pytest.raises(ValueError, match="YYYYMMDD"):
        subject.validate_date_arg("2026-07-12")


def test_validate_date_arg_non_digit_raises() -> None:
    with pytest.raises(ValueError, match="YYYYMMDD"):
        subject.validate_date_arg("2026071x")


def test_validate_date_arg_invalid_calendar_date_raises() -> None:
    with pytest.raises(ValueError, match="Invalid date"):
        subject.validate_date_arg("20261399")


# ── resolve_date_arg ──────────────────────────────────────────────────────────


def test_resolve_date_arg_explicit_value_returned_as_is() -> None:
    now = datetime(2026, 7, 17, 12, 0, 0, tzinfo=subject.JST)
    assert subject.resolve_date_arg("20260712", now) == "20260712"


def test_resolve_date_arg_none_resolves_to_now() -> None:
    now = datetime(2026, 7, 17, 12, 0, 0, tzinfo=subject.JST)
    assert subject.resolve_date_arg(None, now) == "20260717"


# ── connect_neon ──────────────────────────────────────────────────────────────


def test_connect_neon_sets_read_only_and_autocommit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEON_PRIMARY_URL", "postgresql://fake-host/fake-db")
    mock_conn = MagicMock()
    with patch("serve_health_check.psycopg.connect", return_value=mock_conn) as mock_connect:
        result = subject.connect_neon()
    assert result is mock_conn
    assert mock_conn.read_only is True
    assert mock_conn.autocommit is True
    mock_connect.assert_called_once_with("postgresql://fake-host/fake-db")


def test_connect_neon_raises_keyerror_when_env_var_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NEON_PRIMARY_URL", raising=False)
    with pytest.raises(KeyError):
        subject.connect_neon()


# ── query_confirmed_races / query_predictions / query_burst_buckets (mocked) ─


def _make_mock_conn(fetchall_return: Sequence[Sequence[object]]) -> MagicMock:
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = fetchall_return
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn


def test_query_confirmed_races_jra_uses_jvd_ra_table() -> None:
    mock_conn = _make_mock_conn([])
    subject.query_confirmed_races(mock_conn, "20260712", "jra")
    sql_call = mock_conn.cursor.return_value.execute.call_args[0][0]
    assert "FROM jvd_ra" in sql_call


def test_query_confirmed_races_nar_uses_nvd_ra_table() -> None:
    mock_conn = _make_mock_conn([])
    subject.query_confirmed_races(mock_conn, "20260712", "nar")
    sql_call = mock_conn.cursor.return_value.execute.call_args[0][0]
    assert "FROM nvd_ra" in sql_call


def test_query_confirmed_races_filters_placeholder_shusso_tosu() -> None:
    mock_conn = _make_mock_conn([])
    subject.query_confirmed_races(mock_conn, "20260712", "jra")
    sql_call = mock_conn.cursor.return_value.execute.call_args[0][0]
    assert "trim(shusso_tosu) NOT IN ('', '00')" in sql_call


def test_query_confirmed_races_binds_kaisai_nen_and_tsukihi() -> None:
    mock_conn = _make_mock_conn([])
    subject.query_confirmed_races(mock_conn, "20260712", "jra")
    bound_params = mock_conn.cursor.return_value.execute.call_args[0][1]
    assert bound_params == ("2026", "0712")


def test_query_confirmed_races_returns_fetchall_rows() -> None:
    rows = [("2026", "0712", "02", "01", "1540", "10", 1200, 16, "A", "703")]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_confirmed_races(mock_conn, "20260712", "jra")
    assert result == rows


def test_query_predictions_binds_source_category() -> None:
    mock_conn = _make_mock_conn([])
    subject.query_predictions(mock_conn, "20260712", "jra")
    bound_params = mock_conn.cursor.return_value.execute.call_args[0][1]
    assert bound_params == ("jra", "2026", "0712")


def test_query_predictions_returns_fetchall_rows() -> None:
    rows = [("02", "01", "jra-cb-v9-sim-2013-clean", 1.2)]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_predictions(mock_conn, "20260712", "jra")
    assert result == rows


def test_query_burst_buckets_sql_uses_date_trunc_minute() -> None:
    mock_conn = _make_mock_conn([])
    subject.query_burst_buckets(mock_conn, "20260712", "jra")
    sql_call = mock_conn.cursor.return_value.execute.call_args[0][0]
    assert "date_trunc('minute', prediction_generated_at)" in sql_call
    assert "COUNT(DISTINCT (keibajo_code, race_bango))" in sql_call


def test_query_burst_buckets_returns_fetchall_rows() -> None:
    rows = [(datetime(2026, 7, 12, 5, 51, 0, tzinfo=timezone.utc), 22)]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_burst_buckets(mock_conn, "20260712", "jra")
    assert result == rows


# ── load_cell_routing_json ────────────────────────────────────────────────────


def test_load_cell_routing_json_reads_real_file() -> None:
    payload = subject.load_cell_routing_json()
    assert "jra" in payload
    assert "ban-ei" in payload


def test_load_cell_routing_json_custom_path(tmp_path: Path) -> None:
    fake_path = tmp_path / "cell_routing.json"
    fake_payload = {"jra": {"default_variant": "sim", "variants": {}, "rules": []}}
    fake_path.write_text(json.dumps(fake_payload), encoding="utf-8")
    payload = subject.load_cell_routing_json(fake_path)
    assert payload == fake_payload


# ── query_d1_gap_event_count (mocked subprocess) ─────────────────────────────


def test_query_d1_gap_event_count_returns_parsed_value() -> None:
    fake_stdout = json.dumps([{"results": [{"gap_event_count": 3}]}])
    mock_completed = MagicMock(stdout=fake_stdout)
    with patch("serve_health_check.subprocess.run", return_value=mock_completed):
        result = subject.query_d1_gap_event_count("20260712", "jra")
    assert result == 3


def test_query_d1_gap_event_count_command_shape() -> None:
    mock_completed = MagicMock(stdout=json.dumps([{"results": [{"gap_event_count": 0}]}]))
    with patch("serve_health_check.subprocess.run", return_value=mock_completed) as mock_run:
        subject.query_d1_gap_event_count("20260712", "jra")
    call_args, call_kwargs = mock_run.call_args
    command = call_args[0]
    assert command[:5] == ["bunx", "wrangler", "d1", "execute", "finish-position-cron-db"]
    assert "--remote" in command
    assert "--json" in command
    assert call_kwargs["cwd"] == subject.FINISH_POSITION_CRON_DIR
    assert call_kwargs["check"] is True


def test_query_d1_gap_event_count_returns_none_when_wrangler_missing() -> None:
    with patch("serve_health_check.subprocess.run", side_effect=FileNotFoundError("no wrangler")):
        result = subject.query_d1_gap_event_count("20260712", "jra")
    assert result is None


def test_query_d1_gap_event_count_returns_none_on_called_process_error() -> None:
    with patch(
        "serve_health_check.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, ["wrangler"]),
    ):
        result = subject.query_d1_gap_event_count("20260712", "jra")
    assert result is None


def test_query_d1_gap_event_count_returns_none_on_timeout() -> None:
    with patch(
        "serve_health_check.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd=["wrangler"], timeout=30),
    ):
        result = subject.query_d1_gap_event_count("20260712", "jra")
    assert result is None


# ── _fail ──────────────────────────────────────────────────────────────────────


def test_fail_text_mode_prints_to_stderr_and_returns_2(capsys: CaptureFixture[str]) -> None:
    code = subject._fail("boom", json_output=False)
    assert code == 2
    captured = capsys.readouterr()
    assert captured.err == "ERROR: boom\n"
    assert captured.out == ""


def test_fail_json_mode_prints_json_to_stdout_and_returns_2(capsys: CaptureFixture[str]) -> None:
    code = subject._fail("boom", json_output=True)
    assert code == 2
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed == {"error": "tool_failure", "message": "boom"}


# ── run (integration-style, mocked wrapper functions) ────────────────────────


def test_run_invalid_date_raises() -> None:
    with pytest.raises(ValueError, match="YYYYMMDD"):
        subject.run("2026-07-12", "jra", now=datetime(2026, 7, 12, 20, 0, tzinfo=subject.JST))


def test_run_missing_env_var_returns_exit_2(
    monkeypatch: pytest.MonkeyPatch, capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        subject, "connect_neon", MagicMock(side_effect=KeyError("NEON_PRIMARY_URL")),
    )
    code = subject.run("20260712", "jra")
    assert code == 2
    captured = capsys.readouterr()
    assert "NEON_PRIMARY_URL" in captured.err


def test_run_connection_failure_returns_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subject, "connect_neon",
        MagicMock(side_effect=psycopg.OperationalError("connection refused")),
    )
    code = subject.run("20260712", "jra")
    assert code == 2


def test_run_query_failure_returns_exit_2_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_conn = MagicMock()
    monkeypatch.setattr(subject, "connect_neon", MagicMock(return_value=mock_conn))
    monkeypatch.setattr(
        subject, "query_confirmed_races", MagicMock(side_effect=psycopg.Error("bad SQL")),
    )
    code = subject.run("20260712", "jra")
    assert code == 2
    mock_conn.close.assert_called_once()


def test_run_cell_routing_load_failure_returns_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_conn = MagicMock()
    monkeypatch.setattr(subject, "connect_neon", MagicMock(return_value=mock_conn))
    monkeypatch.setattr(subject, "query_confirmed_races", MagicMock(return_value=[]))
    monkeypatch.setattr(subject, "query_predictions", MagicMock(return_value=[]))
    monkeypatch.setattr(subject, "query_burst_buckets", MagicMock(return_value=[]))
    monkeypatch.setattr(
        subject, "load_cell_routing_json", MagicMock(side_effect=OSError("no such file")),
    )
    code = subject.run("20260712", "jra")
    assert code == 2


def test_run_clean_report_returns_exit_0(
    monkeypatch: pytest.MonkeyPatch, capsys: CaptureFixture[str],
) -> None:
    mock_conn = MagicMock()
    monkeypatch.setattr(subject, "connect_neon", MagicMock(return_value=mock_conn))
    monkeypatch.setattr(subject, "query_confirmed_races", MagicMock(return_value=[]))
    monkeypatch.setattr(subject, "query_predictions", MagicMock(return_value=[]))
    monkeypatch.setattr(subject, "query_burst_buckets", MagicMock(return_value=[]))
    monkeypatch.setattr(
        subject, "load_cell_routing_json",
        MagicMock(return_value={"jra": {"default_variant": "sim", "variants": {
            "sim": {"model_version": "jra-cb-v9-sim-2013-clean", "feature_count": 250, "architecture": "catboost"},
        }, "rules": []}}),
    )
    monkeypatch.setattr(subject, "query_d1_gap_event_count", MagicMock(return_value=1))
    code = subject.run("20260712", "jra")
    assert code == 0
    mock_conn.close.assert_called_once()
    captured = capsys.readouterr()
    assert "=== Serve Health Check: 20260712 (JRA) ===" in captured.out


def test_run_anomaly_found_returns_exit_1(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_conn = MagicMock()
    monkeypatch.setattr(subject, "connect_neon", MagicMock(return_value=mock_conn))
    confirmed: list[subject.ConfirmedRaceRow] = [
        ("2026", "0712", "05", "01", "1010", "10", 1200, 16, "A", "016"),
    ]
    monkeypatch.setattr(subject, "query_confirmed_races", MagicMock(return_value=confirmed))
    monkeypatch.setattr(subject, "query_predictions", MagicMock(return_value=[]))
    monkeypatch.setattr(subject, "query_burst_buckets", MagicMock(return_value=[]))
    monkeypatch.setattr(
        subject, "load_cell_routing_json",
        MagicMock(return_value={"jra": {"default_variant": "sim", "variants": {
            "sim": {"model_version": "jra-cb-v9-sim-2013-clean", "feature_count": 250, "architecture": "catboost"},
        }, "rules": []}}),
    )
    monkeypatch.setattr(subject, "query_d1_gap_event_count", MagicMock(return_value=None))
    code = subject.run(
        "20260712", "jra", now=datetime(2026, 7, 12, 23, 59, tzinfo=subject.JST),
    )
    assert code == 1


def test_run_json_output_prints_valid_json(
    monkeypatch: pytest.MonkeyPatch, capsys: CaptureFixture[str],
) -> None:
    mock_conn = MagicMock()
    monkeypatch.setattr(subject, "connect_neon", MagicMock(return_value=mock_conn))
    monkeypatch.setattr(subject, "query_confirmed_races", MagicMock(return_value=[]))
    monkeypatch.setattr(subject, "query_predictions", MagicMock(return_value=[]))
    monkeypatch.setattr(subject, "query_burst_buckets", MagicMock(return_value=[]))
    monkeypatch.setattr(
        subject, "load_cell_routing_json",
        MagicMock(return_value={"jra": {"default_variant": "sim", "variants": {
            "sim": {"model_version": "jra-cb-v9-sim-2013-clean", "feature_count": 250, "architecture": "catboost"},
        }, "rules": []}}),
    )
    monkeypatch.setattr(subject, "query_d1_gap_event_count", MagicMock(return_value=None))
    code = subject.run("20260712", "jra", json_output=True)
    assert code == 0
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["exit_code"] == 0
    assert parsed["date_str"] == "20260712"


# ── main ──────────────────────────────────────────────────────────────────────


def test_main_exits_with_run_return_code(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_args = argparse.Namespace(date="20260712", category="jra", json_output=False)
    monkeypatch.setattr(subject, "parse_args", MagicMock(return_value=fake_args))
    monkeypatch.setattr(subject, "run", MagicMock(return_value=0))
    with pytest.raises(SystemExit) as exc_info:
        subject.main()
    assert exc_info.value.code == 0


def test_main_maps_unhandled_exception_to_exit_2(
    monkeypatch: pytest.MonkeyPatch, capsys: CaptureFixture[str],
) -> None:
    fake_args = argparse.Namespace(date="20260712", category="jra", json_output=False)
    monkeypatch.setattr(subject, "parse_args", MagicMock(return_value=fake_args))
    monkeypatch.setattr(subject, "run", MagicMock(side_effect=RuntimeError("unexpected bug")))
    with pytest.raises(SystemExit) as exc_info:
        subject.main()
    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "RuntimeError" in captured.err
