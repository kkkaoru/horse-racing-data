"""Tests for mlflow_tracking.cells."""

from __future__ import annotations

from mlflow_tracking import cells


def test_classify_distance_band_returns_none_for_none() -> None:
    assert cells.classify_distance_band(None) is None


def test_classify_distance_band_sprint_at_upper_boundary() -> None:
    assert cells.classify_distance_band(1399) == cells.DISTANCE_BAND_SPRINT


def test_classify_distance_band_extended_sprint_at_1400() -> None:
    assert cells.classify_distance_band(1400) == cells.DISTANCE_BAND_EXTENDED_SPRINT


def test_classify_distance_band_extended_sprint_typical_1500() -> None:
    assert cells.classify_distance_band(1500) == cells.DISTANCE_BAND_EXTENDED_SPRINT


def test_classify_distance_band_mile_just_above_extended_sprint() -> None:
    assert cells.classify_distance_band(1501) == cells.DISTANCE_BAND_MILE


def test_classify_distance_band_mile_at_lower_boundary() -> None:
    assert cells.classify_distance_band(1600) == cells.DISTANCE_BAND_MILE


def test_classify_distance_band_mile_at_upper_boundary() -> None:
    assert cells.classify_distance_band(1800) == cells.DISTANCE_BAND_MILE


def test_classify_distance_band_intermediate_at_upper_boundary() -> None:
    assert cells.classify_distance_band(2200) == cells.DISTANCE_BAND_INTERMEDIATE


def test_classify_distance_band_long_at_upper_boundary() -> None:
    assert cells.classify_distance_band(2800) == cells.DISTANCE_BAND_LONG


def test_classify_distance_band_extended_above_long() -> None:
    assert cells.classify_distance_band(2801) == cells.DISTANCE_BAND_EXTENDED


def test_classify_field_size_band_returns_none_for_none() -> None:
    assert cells.classify_field_size_band(None) is None


def test_classify_field_size_band_small_at_boundary() -> None:
    assert cells.classify_field_size_band(8) == cells.FIELD_SIZE_SMALL


def test_classify_field_size_band_medium_at_boundary() -> None:
    assert cells.classify_field_size_band(14) == cells.FIELD_SIZE_MEDIUM


def test_classify_field_size_band_large_above_medium() -> None:
    assert cells.classify_field_size_band(15) == cells.FIELD_SIZE_LARGE


def test_classify_season_band_returns_none_for_none() -> None:
    assert cells.classify_season_band(None) is None


def test_classify_season_band_returns_none_for_short_string() -> None:
    assert cells.classify_season_band("1") is None


def test_classify_season_band_returns_none_for_non_digit_head() -> None:
    assert cells.classify_season_band("ab01") is None


def test_classify_season_band_spring() -> None:
    assert cells.classify_season_band("0401") == cells.SEASON_SPRING


def test_classify_season_band_summer() -> None:
    assert cells.classify_season_band("0701") == cells.SEASON_SUMMER


def test_classify_season_band_autumn() -> None:
    assert cells.classify_season_band("1001") == cells.SEASON_AUTUMN


def test_classify_season_band_winter() -> None:
    assert cells.classify_season_band("0101") == cells.SEASON_WINTER


def test_classify_surface_returns_none_for_none() -> None:
    assert cells.classify_surface(None) is None


def test_classify_surface_returns_none_for_blank_string() -> None:
    assert cells.classify_surface("   ") is None


def test_classify_surface_turf() -> None:
    assert cells.classify_surface("10") == cells.SURFACE_TURF


def test_classify_surface_obstacle() -> None:
    assert cells.classify_surface("51") == cells.SURFACE_OBSTACLE


def test_classify_surface_dirt() -> None:
    assert cells.classify_surface("23") == cells.SURFACE_DIRT


def test_classify_surface_returns_none_for_unknown_code() -> None:
    assert cells.classify_surface("99") is None


def test_classify_all_combines_every_dimension() -> None:
    result = cells.classify_all(
        kyori=1500,
        shusso_tosu=10,
        kaisai_tsukihi="0615",
        track_code="24",
        class_code="A",
        keibajo_code="05",
    )
    assert result == {
        "distance_band": cells.DISTANCE_BAND_EXTENDED_SPRINT,
        "field_size_band": cells.FIELD_SIZE_MEDIUM,
        "season_band": cells.SEASON_SUMMER,
        "surface": cells.SURFACE_DIRT,
        "class_code": "A",
        "venue": "05",
    }


def test_classify_all_defaults_to_all_none() -> None:
    result = cells.classify_all()
    assert result == {
        "distance_band": None,
        "field_size_band": None,
        "season_band": None,
        "surface": None,
        "class_code": None,
        "venue": None,
    }


def test_normalize_cell_columns_renames_known_alias() -> None:
    result = cells.normalize_cell_columns({"keibajo_code": "05"})
    assert result == {"venue": "05"}


def test_normalize_cell_columns_leaves_canonical_key_untouched() -> None:
    result = cells.normalize_cell_columns({"venue": "05"})
    assert result == {"venue": "05"}


def test_normalize_cell_columns_canonical_wins_over_alias_collision() -> None:
    result = cells.normalize_cell_columns({"venue": "05", "keibajo_code": "99"})
    assert result == {"venue": "05"}


def test_normalize_cell_columns_passes_through_unknown_keys() -> None:
    result = cells.normalize_cell_columns({"unrelated_column": "value"})
    assert result == {"unrelated_column": "value"}


def test_normalize_cell_columns_renames_multiple_aliases_at_once() -> None:
    result = cells.normalize_cell_columns(
        {
            "keibajo_code": "05",
            "kyori_band": "mile",
            "current_baba_condition": "good",
            "kyoso_joken_code": "A",
        }
    )
    assert result == {
        "venue": "05",
        "distance_band": "mile",
        "track_condition": "good",
        "class_code": "A",
    }


def test_normalize_cell_columns_renames_class_label_alias() -> None:
    result = cells.normalize_cell_columns({"class_label": "703"})
    assert result == {"class_code": "703"}


def test_normalize_cell_columns_renames_season_alias() -> None:
    result = cells.normalize_cell_columns({"season": "summer"})
    assert result == {"season_band": "summer"}


def test_normalize_cell_columns_canonical_wins_over_class_label_collision() -> None:
    result = cells.normalize_cell_columns({"class_label": "703", "class_code": "999"})
    assert result == {"class_code": "999"}


def test_normalize_cell_columns_canonical_wins_over_season_collision() -> None:
    result = cells.normalize_cell_columns({"season": "summer", "season_band": "winter"})
    assert result == {"season_band": "winter"}
