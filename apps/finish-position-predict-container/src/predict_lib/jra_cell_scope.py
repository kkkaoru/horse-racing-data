"""Canonical JRA evaluation cells and point-in-time 20-year training scopes.

An evaluation cell is deliberately narrower than its training population.  The
cell identifies races by venue, exact distance, season, surface, and JRA
condition code.  Open-class code 999 additionally requires a race identity;
there is no generic 999 key. A fold seeds horses from evaluation entrants and
all races of the target cell in the preceding 20 years. Training then includes
every pre-cutoff race those horses ran inside that lookback, regardless of
venue or race conditions. All runners of selected races belong to training.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import islice
from typing import Final, Literal

OPEN_CLASS_CODE: Final[str] = "999"
LOOKBACK_YEARS: Final[int] = 20
MINIMUM_DIVERSE_TRAINING_RACES: Final[int] = 100
RELATED_DISTANCE_METERS: Final[int] = 400
RELATED_DISTANCE_SEED_LIMIT: Final[int] = 64
VENUE_SURFACE_SEED_LIMIT: Final[int] = 128
EVALUATION_YEAR_FROM: Final[int] = 2020
EVALUATION_YEAR_TO: Final[int] = 2026
DOMESTIC_JRA_VENUES: Final[frozenset[str]] = frozenset(
    {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
)
_BLANK_TOKENS: Final[frozenset[str]] = frozenset({"", "nan", "none", "<na>"})
_SPACE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\s+", re.UNICODE)

Season = Literal["spring", "summer", "autumn", "winter"]
Surface = Literal["turf", "dirt", "obstacle", "other"]


@dataclass(frozen=True, slots=True)
class JraRace:
    race_id: str
    race_date: date
    venue: str
    distance: int
    track_code: str
    condition_code: str
    grade_code: str
    race_name: str
    race_type_code: str
    weight_type_code: str
    horse_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class JraCellKey:
    venue: str
    distance: int
    season: Season
    surface: Surface
    condition_code: str
    race_identity: str | None

    def __post_init__(self) -> None:
        if not self.venue or self.distance <= 0 or not self.condition_code:
            raise ValueError("JRA cell dimensions must be non-empty")
        if self.condition_code == OPEN_CLASS_CODE and self.race_identity is None:
            raise ValueError("JRA condition 999 requires a race-specific identity")
        if self.condition_code != OPEN_CLASS_CODE and self.race_identity is not None:
            raise ValueError("race identity is only permitted for JRA condition 999")

    @property
    def canonical(self) -> str:
        identity = self.race_identity or "-"
        return (
            f"venue={self.venue};distance={self.distance};season={self.season};"
            f"surface={self.surface};class={self.condition_code};race={identity}"
        )

    @property
    def cell_id(self) -> str:
        digest = hashlib.sha256(self.canonical.encode()).hexdigest()[:16]
        return f"jra-cell-{digest}"


@dataclass(frozen=True, slots=True)
class JraFoldScope:
    cell: JraCellKey
    evaluation_year: int
    cutoff: date
    history_start: date
    seed_race_ids: tuple[str, ...]
    seed_horse_ids: tuple[str, ...]
    training_race_ids: tuple[str, ...]
    training_horse_rows: int
    evaluation_race_ids: tuple[str, ...]
    evaluation_horse_rows: int
    training_scope_mode: str
    related_seed_race_count: int
    training_target_cell_race_count: int
    training_cross_cell_race_count: int
    cell_history_seed_race_ids: tuple[str, ...] = ()

    @property
    def covers_evaluation_size(self) -> bool:
        return self.training_horse_rows >= self.evaluation_horse_rows

    @property
    def is_diverse(self) -> bool:
        return self.training_cross_cell_race_count > 0


def derive_season(month: int) -> Season:
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    if month in {9, 10, 11}:
        return "autumn"
    if month in {12, 1, 2}:
        return "winter"
    raise ValueError(f"invalid calendar month: {month}")


def derive_surface(track_code: str) -> Surface:
    text = track_code.strip()
    if not text.isdigit():
        return "other"
    code = int(text)
    if 10 <= code <= 22:
        return "turf"
    if 23 <= code <= 29:
        return "dirt"
    if 51 <= code <= 59:
        return "obstacle"
    return "other"


def normalize_race_name(value: object) -> str | None:
    text = unicodedata.normalize("NFKC", str(value)).strip()
    text = _SPACE_PATTERN.sub("", text)
    return None if text.lower() in _BLANK_TOKENS else text


def _open_race_identity(race: JraRace) -> str:
    race_name = normalize_race_name(race.race_name)
    return f"name:{race_name}" if race_name is not None else "unnamed"


def cell_for_race(race: JraRace) -> JraCellKey:
    venue = race.venue.strip()
    if venue not in DOMESTIC_JRA_VENUES:
        raise ValueError(f"unsupported domestic JRA venue: {venue}")
    surface = derive_surface(race.track_code)
    condition = race.condition_code.strip()
    return JraCellKey(
        venue=venue,
        distance=race.distance,
        season=derive_season(race.race_date.month),
        surface=surface,
        condition_code=condition,
        race_identity=(_open_race_identity(race) if condition == OPEN_CLASS_CODE else None),
    )


def history_start_for(cutoff: date) -> date:
    return cutoff.replace(year=cutoff.year - LOOKBACK_YEARS)


def group_observed_cells(
    races: Iterable[JraRace],
    *,
    year_from: int = EVALUATION_YEAR_FROM,
    year_to: int = EVALUATION_YEAR_TO,
) -> dict[JraCellKey, tuple[JraRace, ...]]:
    grouped: dict[JraCellKey, list[JraRace]] = {}
    for race in races:
        if (
            race.venue.strip() in DOMESTIC_JRA_VENUES
            and year_from <= race.race_date.year <= year_to
        ):
            grouped.setdefault(cell_for_race(race), []).append(race)
    return {
        cell: tuple(sorted(cell_races, key=lambda race: (race.race_date, race.race_id)))
        for cell, cell_races in grouped.items()
    }


def _unique_sorted(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


class JraRaceIndex:
    """Indexed race graph for repeated cell/fold scope resolution."""

    def __init__(self, races: Iterable[JraRace]) -> None:
        by_id: dict[str, JraRace] = {}
        by_cell: dict[JraCellKey, list[JraRace]] = {}
        by_horse: dict[str, list[JraRace]] = {}
        by_venue_surface: dict[tuple[str, Surface], list[JraRace]] = {}
        for race in races:
            if race.race_id in by_id:
                raise ValueError(f"duplicate JRA race identity: {race.race_id}")
            by_id[race.race_id] = race
            if race.venue.strip() in DOMESTIC_JRA_VENUES:
                by_venue_surface.setdefault(
                    (race.venue.strip(), derive_surface(race.track_code)), []
                ).append(race)
                by_cell.setdefault(cell_for_race(race), []).append(race)
            for horse_id in set(race.horse_ids):
                if horse_id.strip():
                    by_horse.setdefault(horse_id, []).append(race)
        self._by_id: dict[str, JraRace] = by_id
        self._by_cell: dict[JraCellKey, tuple[JraRace, ...]] = {
            cell: tuple(sorted(items, key=lambda item: (item.race_date, item.race_id)))
            for cell, items in by_cell.items()
        }
        self._by_horse: dict[str, tuple[JraRace, ...]] = {
            horse_id: tuple(sorted(items, key=lambda item: (item.race_date, item.race_id)))
            for horse_id, items in by_horse.items()
        }
        self._by_venue_surface: dict[tuple[str, Surface], tuple[JraRace, ...]] = {
            key: tuple(
                sorted(
                    items,
                    key=lambda item: (item.race_date, item.race_id),
                    reverse=True,
                )
            )
            for key, items in by_venue_surface.items()
        }

    def _build_scope(
        self,
        cell: JraCellKey,
        cutoff: date,
        evaluation_races: Sequence[JraRace],
        *,
        additional_seed_races: Sequence[JraRace] = (),
        training_scope_mode: str = "cell-20y-entrant-history",
    ) -> JraFoldScope:
        history_start = history_start_for(cutoff)
        historical_cell_races = tuple(
            race for race in self._by_cell.get(cell, ()) if history_start <= race.race_date < cutoff
        )
        seed_races = tuple(evaluation_races) + historical_cell_races + tuple(additional_seed_races)
        seed_horses = frozenset(
            horse_id for race in seed_races for horse_id in race.horse_ids if horse_id.strip()
        )
        training_ids = {
            race.race_id
            for horse_id in seed_horses
            for race in self._by_horse.get(horse_id, ())
            if history_start <= race.race_date < cutoff
        }
        training_races = [self._by_id[race_id] for race_id in training_ids]
        target_cell_count = sum(
            1
            for race in training_races
            if race.venue.strip() in DOMESTIC_JRA_VENUES and cell_for_race(race) == cell
        )
        return JraFoldScope(
            cell=cell,
            evaluation_year=cutoff.year,
            cutoff=cutoff,
            history_start=history_start,
            seed_race_ids=_unique_sorted(race.race_id for race in seed_races),
            seed_horse_ids=_unique_sorted(seed_horses),
            training_race_ids=_unique_sorted(training_ids),
            training_horse_rows=sum(len(race.horse_ids) for race in training_races),
            evaluation_race_ids=_unique_sorted(race.race_id for race in evaluation_races),
            evaluation_horse_rows=sum(len(race.horse_ids) for race in evaluation_races),
            training_scope_mode=training_scope_mode,
            related_seed_race_count=len(additional_seed_races),
            training_target_cell_race_count=target_cell_count,
            training_cross_cell_race_count=len(training_races) - target_cell_count,
            cell_history_seed_race_ids=_unique_sorted(
                race.race_id for race in historical_cell_races
            ),
        )

    def build_entrant_scope(
        self,
        cell: JraCellKey,
        cutoff: date,
        evaluation_races: Sequence[JraRace],
    ) -> JraFoldScope:
        return self._build_scope(cell, cutoff, evaluation_races)

    def build_fold_scope(self, cell: JraCellKey, evaluation_year: int) -> JraFoldScope:
        if not EVALUATION_YEAR_FROM <= evaluation_year <= EVALUATION_YEAR_TO:
            raise ValueError("evaluation year must be within 2020..2026")
        cutoff = date(evaluation_year, 1, 1)
        evaluation_races = [
            race for race in self._by_cell.get(cell, ()) if race.race_date.year == evaluation_year
        ]
        return self.build_expanded_scope(cell, cutoff, evaluation_races)

    def build_target_date_scope(self, cell: JraCellKey, target_date: date) -> JraFoldScope:
        evaluation_races = [
            race for race in self._by_cell.get(cell, ()) if race.race_date == target_date
        ]
        return self.build_expanded_scope(cell, target_date, evaluation_races)

    def _related_course_races(
        self,
        cell: JraCellKey,
        cutoff: date,
        *,
        maximum_distance_delta: int | None,
        limit: int,
    ) -> tuple[JraRace, ...]:
        history_start = history_start_for(cutoff)
        candidates = (
            race
            for race in self._by_venue_surface.get((cell.venue, cell.surface), ())
            if history_start <= race.race_date < cutoff
            and (
                maximum_distance_delta is None
                or abs(race.distance - cell.distance) <= maximum_distance_delta
            )
            and cell_for_race(race) != cell
        )
        return tuple(islice(candidates, limit))

    def build_related_distance_scope(
        self,
        cell: JraCellKey,
        cutoff: date,
        evaluation_races: Sequence[JraRace],
    ) -> JraFoldScope:
        related = self._related_course_races(
            cell,
            cutoff,
            maximum_distance_delta=RELATED_DISTANCE_METERS,
            limit=RELATED_DISTANCE_SEED_LIMIT,
        )
        return self._build_scope(
            cell,
            cutoff,
            evaluation_races,
            additional_seed_races=related,
            training_scope_mode="related-distance-track-bias",
        )

    def build_venue_surface_scope(
        self,
        cell: JraCellKey,
        cutoff: date,
        evaluation_races: Sequence[JraRace],
    ) -> JraFoldScope:
        broad = self._related_course_races(
            cell,
            cutoff,
            maximum_distance_delta=None,
            limit=VENUE_SURFACE_SEED_LIMIT,
        )
        return self._build_scope(
            cell,
            cutoff,
            evaluation_races,
            additional_seed_races=broad,
            training_scope_mode="venue-surface-track-bias",
        )

    def build_expanded_scope(
        self,
        cell: JraCellKey,
        cutoff: date,
        evaluation_races: Sequence[JraRace],
        *,
        minimum_training_races: int = MINIMUM_DIVERSE_TRAINING_RACES,
    ) -> JraFoldScope:
        """Expand entrant history with related-course cohorts, never same-cell-only data."""
        entrant_scope = self.build_entrant_scope(cell, cutoff, evaluation_races)
        if (
            len(entrant_scope.training_race_ids) >= minimum_training_races
            and entrant_scope.is_diverse
        ):
            return entrant_scope
        related_scope = self.build_related_distance_scope(cell, cutoff, evaluation_races)
        if (
            len(related_scope.training_race_ids) >= minimum_training_races
            and related_scope.is_diverse
        ):
            return related_scope
        return self.build_venue_surface_scope(cell, cutoff, evaluation_races)


def build_fold_scope(
    cell: JraCellKey,
    races: Sequence[JraRace],
    evaluation_year: int,
) -> JraFoldScope:
    return JraRaceIndex(races).build_fold_scope(cell, evaluation_year)


def topk_winner_hits(
    predictions_by_race: Mapping[str, Sequence[str]],
    winners_by_race: Mapping[str, str],
) -> dict[int, int]:
    hits = {depth: 0 for depth in range(1, 6)}
    for race_id, winner in winners_by_race.items():
        ranking = predictions_by_race.get(race_id)
        if ranking is None or len(ranking) == 0:
            continue
        for depth in hits:
            hits[depth] += int(winner in ranking[:depth])
    return hits
