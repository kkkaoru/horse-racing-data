"""Event-cell boundaries and complete past-entrant scope tests."""

from dataclasses import replace
from datetime import date

import pytest
from dedicated_cells import CHALLENGE, ST_LITE, event_fold
from predict_lib.jra_cell_scope import JraRace, JraRaceIndex


@pytest.mark.parametrize("name", ["チャレンジカップ", "朝日チャレンジカップ"])
@pytest.mark.parametrize("month", [9, 12])
def test_challenge_follows_event_not_season(name: str, month: int) -> None:
    race = JraRace(
        "race", date(2023, month, 1), "09", 2000, "11", "999", "C", name, "13", "1", ("horse",)
    )
    assert CHALLENGE.matches(race) is True


@pytest.mark.parametrize(
    ("venue", "distance", "track", "condition", "name"),
    [
        ("08", 2000, "11", "999", "チャレンジカップ"),
        ("09", 1800, "11", "999", "チャレンジカップ"),
        ("09", 2000, "24", "999", "チャレンジカップ"),
        ("09", 2000, "11", "703", "チャレンジカップ"),
        ("09", 2000, "11", "999", "チャレンジカップ別競走"),
    ],
)
def test_event_does_not_merge_other_courses(
    venue: str, distance: int, track: str, condition: str, name: str
) -> None:
    race = JraRace(
        "race",
        date(2023, 12, 1),
        venue,
        distance,
        track,
        condition,
        "C",
        name,
        "13",
        "1",
        ("horse",),
    )
    assert CHALLENGE.matches(race) is False


@pytest.mark.parametrize(
    "name", ["朝日杯セントライト記念", "ラジオ日本賞セントライト記念", "セントライト記念"]
)
def test_st_lite_explicit_sponsor_aliases(name: str) -> None:
    race = JraRace(
        "race", date(2023, 9, 1), "06", 2200, "11", "999", "B", name, "13", "1", ("horse",)
    )
    assert ST_LITE.matches(race) is True
    assert ST_LITE.matches(replace(race, race_date=date(2023, 12, 1))) is False
    assert ST_LITE.matches(replace(race, race_name="ラジオ日本賞")) is False


def test_base_keys_preserve_all_explicit_boundaries() -> None:
    assert len(CHALLENGE.base_keys()) == 8
    assert len(ST_LITE.base_keys()) == 3
    assert CHALLENGE.base_keys()[0].canonical == (
        "venue=09;distance=2000;season=spring;surface=turf;class=999;race=name:チャレンジカップ"
    )


def test_scope_contains_old_names_and_other_venues_but_no_future() -> None:
    race = JraRace(
        "evaluation",
        date(2023, 12, 2),
        "09",
        2000,
        "11",
        "999",
        "C",
        "チャレンジカップ",
        "13",
        "1",
        ("new-horse",),
    )
    history = replace(
        race,
        race_id="history",
        race_date=date(2010, 9, 1),
        race_name="朝日チャレンジカップ",
        horse_ids=("old-horse",),
    )
    cross = replace(
        race,
        race_id="cross",
        race_date=date(2012, 5, 1),
        venue="05",
        condition_code="005",
        horse_ids=("old-horse", "rival"),
    )
    prior = replace(
        race, race_id="prior", race_date=date(2022, 11, 1), venue="04", condition_code="703"
    )
    same_day = replace(prior, race_id="same-day", race_date=date(2023, 1, 1))
    future = replace(prior, race_id="future", race_date=date(2024, 1, 1))
    wrong_venue = replace(race, race_id="wrong-venue", venue="08")
    too_old = replace(history, race_id="too-old", race_date=date(2002, 1, 1))
    races = (race, history, cross, prior, same_day, future, wrong_venue, too_old)
    result = event_fold(
        cell=CHALLENGE,
        races=races,
        index=JraRaceIndex(races),
        year=2023,
        observed_before=date(2026, 9, 13),
    )
    assert result.cell_id == "jra-event-hanshin-challenge-2000-v1"
    assert result.cutoff == date(2023, 1, 1)
    assert result.history_start == date(2003, 1, 1)
    assert result.training_race_ids == ("cross", "history", "prior")
    assert result.seed_horse_ids == ("new-horse", "old-horse")
    assert result.evaluation_race_ids == ("evaluation",)
    assert result.seed_race_ids == ("evaluation", "history")


def test_observation_boundary_excludes_upcoming_outcomes() -> None:
    race = JraRace(
        "upcoming",
        date(2026, 9, 13),
        "06",
        2200,
        "11",
        "999",
        "B",
        "朝日杯セントライト記念",
        "13",
        "1",
        ("horse",),
    )
    result = event_fold(
        cell=ST_LITE,
        races=(race,),
        index=JraRaceIndex((race,)),
        year=2026,
        observed_before=date(2026, 9, 13),
    )
    assert result.evaluation_race_ids == ()
    assert result.training_race_ids == ()


@pytest.mark.parametrize("year", [2019, 2027])
def test_out_of_range_year_fails(year: int) -> None:
    with pytest.raises(ValueError, match="Evaluation years"):
        event_fold(
            cell=ST_LITE,
            races=(),
            index=JraRaceIndex(()),
            year=year,
            observed_before=date(2026, 9, 13),
        )


def test_invalid_observation_boundary_fails() -> None:
    with pytest.raises(ValueError, match="Observation boundary"):
        event_fold(
            cell=ST_LITE,
            races=(),
            index=JraRaceIndex(()),
            year=2023,
            observed_before=date(2023, 1, 1),
        )
