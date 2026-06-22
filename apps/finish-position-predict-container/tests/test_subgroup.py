from predict_lib.subgroup import (
    DISTANCE_BAND_EXTENDED,
    DISTANCE_BAND_INTERMEDIATE,
    DISTANCE_BAND_LONG,
    DISTANCE_BAND_MILE,
    DISTANCE_BAND_SPRINT,
    FIELD_SIZE_LARGE,
    FIELD_SIZE_MEDIUM,
    FIELD_SIZE_SMALL,
    SEASON_AUTUMN,
    SEASON_SPRING,
    SEASON_SUMMER,
    SEASON_WINTER,
    SUBGROUP_DIMENSIONS,
    SURFACE_DIRT,
    SURFACE_OBSTACLE,
    SURFACE_TURF,
    classify_all,
    classify_distance_band,
    classify_field_size_band,
    classify_season_band,
    classify_surface,
)


def test_classify_distance_band_none_returns_none() -> None:
    assert classify_distance_band(None) is None


def test_classify_distance_band_sprint_boundary_1400() -> None:
    assert classify_distance_band(1400) == DISTANCE_BAND_SPRINT


def test_classify_distance_band_sprint_typical_1200() -> None:
    assert classify_distance_band(1200) == DISTANCE_BAND_SPRINT


def test_classify_distance_band_mile_just_above_sprint_1401() -> None:
    assert classify_distance_band(1401) == DISTANCE_BAND_MILE


def test_classify_distance_band_mile_boundary_1800() -> None:
    assert classify_distance_band(1800) == DISTANCE_BAND_MILE


def test_classify_distance_band_intermediate_just_above_mile_1801() -> None:
    assert classify_distance_band(1801) == DISTANCE_BAND_INTERMEDIATE


def test_classify_distance_band_intermediate_boundary_2200() -> None:
    assert classify_distance_band(2200) == DISTANCE_BAND_INTERMEDIATE


def test_classify_distance_band_long_just_above_intermediate_2201() -> None:
    assert classify_distance_band(2201) == DISTANCE_BAND_LONG


def test_classify_distance_band_long_boundary_2800() -> None:
    assert classify_distance_band(2800) == DISTANCE_BAND_LONG


def test_classify_distance_band_extended_just_above_long_2801() -> None:
    assert classify_distance_band(2801) == DISTANCE_BAND_EXTENDED


def test_classify_distance_band_extended_typical_3600() -> None:
    assert classify_distance_band(3600) == DISTANCE_BAND_EXTENDED


def test_classify_field_size_band_none_returns_none() -> None:
    assert classify_field_size_band(None) is None


def test_classify_field_size_band_small_boundary_8() -> None:
    assert classify_field_size_band(8) == FIELD_SIZE_SMALL


def test_classify_field_size_band_small_typical_5() -> None:
    assert classify_field_size_band(5) == FIELD_SIZE_SMALL


def test_classify_field_size_band_medium_just_above_small_9() -> None:
    assert classify_field_size_band(9) == FIELD_SIZE_MEDIUM


def test_classify_field_size_band_medium_boundary_14() -> None:
    assert classify_field_size_band(14) == FIELD_SIZE_MEDIUM


def test_classify_field_size_band_large_just_above_medium_15() -> None:
    assert classify_field_size_band(15) == FIELD_SIZE_LARGE


def test_classify_field_size_band_large_typical_18() -> None:
    assert classify_field_size_band(18) == FIELD_SIZE_LARGE


def test_classify_season_band_none_returns_none() -> None:
    assert classify_season_band(None) is None


def test_classify_season_band_too_short_returns_none() -> None:
    assert classify_season_band("3") is None


def test_classify_season_band_non_numeric_head_returns_none() -> None:
    assert classify_season_band("XX01") is None


def test_classify_season_band_spring_march() -> None:
    assert classify_season_band("0315") == SEASON_SPRING


def test_classify_season_band_spring_may() -> None:
    assert classify_season_band("0531") == SEASON_SPRING


def test_classify_season_band_summer_june() -> None:
    assert classify_season_band("0601") == SEASON_SUMMER


def test_classify_season_band_summer_august() -> None:
    assert classify_season_band("0820") == SEASON_SUMMER


def test_classify_season_band_autumn_september() -> None:
    assert classify_season_band("0905") == SEASON_AUTUMN


def test_classify_season_band_autumn_november() -> None:
    assert classify_season_band("1130") == SEASON_AUTUMN


def test_classify_season_band_winter_december() -> None:
    assert classify_season_band("1225") == SEASON_WINTER


def test_classify_season_band_winter_january() -> None:
    assert classify_season_band("0110") == SEASON_WINTER


def test_classify_season_band_winter_february() -> None:
    assert classify_season_band("0214") == SEASON_WINTER


def test_classify_surface_none_returns_none() -> None:
    assert classify_surface(None) is None


def test_classify_surface_empty_string_returns_none() -> None:
    assert classify_surface("  ") is None


def test_classify_surface_turf_lower_bound_10() -> None:
    assert classify_surface("10") == SURFACE_TURF


def test_classify_surface_turf_upper_bound_22() -> None:
    assert classify_surface("22") == SURFACE_TURF


def test_classify_surface_dirt_lower_bound_23() -> None:
    assert classify_surface("23") == SURFACE_DIRT


def test_classify_surface_dirt_upper_bound_29() -> None:
    assert classify_surface("29") == SURFACE_DIRT


def test_classify_surface_obstacle_lower_bound_51() -> None:
    assert classify_surface("51") == SURFACE_OBSTACLE


def test_classify_surface_obstacle_upper_bound_59() -> None:
    assert classify_surface("59") == SURFACE_OBSTACLE


def test_classify_surface_strips_whitespace() -> None:
    assert classify_surface(" 24 ") == SURFACE_DIRT


def test_classify_surface_unknown_code_returns_none() -> None:
    assert classify_surface("99") is None


def test_subgroup_dimensions_tuple() -> None:
    assert SUBGROUP_DIMENSIONS == (
        "distance_band",
        "field_size_band",
        "season_band",
        "surface",
        "class_code",
        "venue",
    )


def test_classify_all_complete_example() -> None:
    assert classify_all(
        kyori=2000,
        shusso_tosu=16,
        kaisai_tsukihi="0405",
        track_code="17",
        class_code="010",
        keibajo_code="05",
    ) == {
        "distance_band": DISTANCE_BAND_INTERMEDIATE,
        "field_size_band": FIELD_SIZE_LARGE,
        "season_band": SEASON_SPRING,
        "surface": SURFACE_TURF,
        "class_code": "010",
        "venue": "05",
    }


def test_classify_all_all_none_defaults() -> None:
    assert classify_all() == {
        "distance_band": None,
        "field_size_band": None,
        "season_band": None,
        "surface": None,
        "class_code": None,
        "venue": None,
    }
