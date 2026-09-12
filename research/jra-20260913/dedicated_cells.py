"""Explicit event cells with immutable venue boundaries and twenty-year entrant history."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import chain

from predict_lib.jra_cell_scope import (
    JraCellKey,
    JraRace,
    JraRaceIndex,
    Season,
    derive_season,
    derive_surface,
    normalize_race_name,
)

SEASONS: tuple[Season, ...] = ("spring", "summer", "autumn", "winter")


@dataclass(frozen=True)
class DedicatedCell:
    cell_id: str
    venue: str
    distance: int
    names: tuple[str, ...]
    season: Season | None
    routing_cell_id: str

    def matches(self, race: JraRace) -> bool:
        return (
            race.venue.strip() == self.venue
            and race.distance == self.distance
            and derive_surface(race.track_code) == "turf"
            and race.condition_code.strip() == "999"
            and normalize_race_name(race.race_name) in self.names
            and (self.season is None or derive_season(race.race_date.month) == self.season)
        )

    def base_keys(self) -> tuple[JraCellKey, ...]:
        seasons = SEASONS if self.season is None else (self.season,)
        return tuple(chain.from_iterable(self._keys_for_name(name, seasons) for name in self.names))

    def _keys_for_name(self, name: str, seasons: tuple[Season, ...]) -> tuple[JraCellKey, ...]:
        return tuple(
            JraCellKey(self.venue, self.distance, season, "turf", "999", f"name:{name}")
            for season in seasons
        )


@dataclass(frozen=True)
class EventFold:
    cell_id: str
    routing_cell_id: str
    evaluation_year: int
    cutoff: date
    history_start: date
    seed_race_ids: tuple[str, ...]
    seed_horse_ids: tuple[str, ...]
    training_race_ids: tuple[str, ...]
    evaluation_race_ids: tuple[str, ...]


ST_LITE: DedicatedCell = DedicatedCell(
    cell_id="jra-event-nakayama-st-lite-2200-v1",
    venue="06",
    distance=2200,
    names=("朝日杯セントライト記念", "ラジオ日本賞セントライト記念", "セントライト記念"),
    season="autumn",
    routing_cell_id="jra-cell-1cc9628bdc7b13a2",
)
CHALLENGE: DedicatedCell = DedicatedCell(
    cell_id="jra-event-hanshin-challenge-2000-v1",
    venue="09",
    distance=2000,
    names=("チャレンジカップ", "朝日チャレンジカップ"),
    season=None,
    routing_cell_id="jra-cell-8c1fd54d5a4ba6e3",
)


def _union_ids(collections: Iterable[tuple[str, ...]]) -> tuple[str, ...]:
    return tuple(sorted(set(chain.from_iterable(collections))))


def event_fold(
    *,
    cell: DedicatedCell,
    races: Sequence[JraRace],
    index: JraRaceIndex,
    year: int,
    observed_before: date,
) -> EventFold:
    """Union public canonical scopes; never rewrite dates or include future labels."""
    if not 2020 <= year <= 2026:
        raise ValueError("Evaluation years must stay within 2020..2026")
    cutoff = date(year, 1, 1)
    if observed_before <= cutoff:
        raise ValueError("Observation boundary must follow the fold cutoff")
    evaluation = tuple(
        race
        for race in races
        if race.race_date.year == year and race.race_date < observed_before and cell.matches(race)
    )
    scopes = tuple(index.build_entrant_scope(key, cutoff, evaluation) for key in cell.base_keys())
    return EventFold(
        cell_id=cell.cell_id,
        routing_cell_id=cell.routing_cell_id,
        evaluation_year=year,
        cutoff=cutoff,
        history_start=date(year - 20, 1, 1),
        seed_race_ids=_union_ids(scope.seed_race_ids for scope in scopes),
        seed_horse_ids=_union_ids(scope.seed_horse_ids for scope in scopes),
        training_race_ids=_union_ids(scope.training_race_ids for scope in scopes),
        evaluation_race_ids=tuple(sorted(race.race_id for race in evaluation)),
    )
