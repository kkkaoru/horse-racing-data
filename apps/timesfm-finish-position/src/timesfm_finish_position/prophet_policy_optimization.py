"""Deterministic cell-specific Prophet weight selection."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations, groupby, pairwise
from typing import Final

TOP_K_COUNT: Final[int] = 5
BASELINE_WEIGHT: Final[float] = 0.0
MAXIMUM_WEIGHT: Final[float] = 1.0


@dataclass(frozen=True, slots=True)
class OptimizedCellPolicy:
    """Selected policy and its aggregate winner-in-TopK evidence."""

    enabled: bool
    weight: float
    selected_weight: float | None
    baseline_hits: tuple[int, ...]
    selected_hits: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class LinearRunnerScore:
    """One runner's score as ``intercept + weight * adjustment``."""

    horse_id: str
    intercept: float
    adjustment: float


@dataclass(frozen=True, slots=True)
class LinearRaceScores:
    """Linear scores and all official winners for one race."""

    winners: tuple[LinearRunnerScore, ...]
    competitors: tuple[LinearRunnerScore, ...]


@dataclass(frozen=True, slots=True)
class _CrossingEvent:
    weight: float
    race_index: int
    winner_rank_deltas: tuple[tuple[int, int], ...]


def _validated_hits(weight: float, values: Sequence[object]) -> tuple[int, ...]:
    if not math.isfinite(weight) or weight < BASELINE_WEIGHT or weight > MAXIMUM_WEIGHT:
        raise ValueError("Candidate weights must be finite and in [0, 1]")
    raw_hits = tuple(values)
    if len(raw_hits) != TOP_K_COUNT:
        raise ValueError("Each candidate must contain exactly five TopK hit counts")
    if any(isinstance(hit, bool) or not isinstance(hit, int) or hit < 0 for hit in raw_hits):
        raise ValueError("TopK hit counts must be non-negative integers")
    hits = tuple(hit for hit in raw_hits if isinstance(hit, int) and not isinstance(hit, bool))
    if any(left > right for left, right in pairwise(hits)):
        raise ValueError("TopK hit counts must be monotonically non-decreasing")
    return hits


def select_optimal_cell_weight(
    candidate_topk_hits: Mapping[float, Sequence[object]],
    *,
    has_support: bool,
    default_weight: float,
) -> OptimizedCellPolicy:
    """Select weight by Top1..Top5 lexicographic hits, then minimum weight."""
    if (
        not math.isfinite(default_weight)
        or default_weight <= 0.0
        or default_weight > MAXIMUM_WEIGHT
    ):
        raise ValueError("Default weight must be finite and in (0, 1]")
    candidates = {
        float(weight): _validated_hits(float(weight), hits)
        for weight, hits in candidate_topk_hits.items()
    }
    baseline_hits = candidates.get(BASELINE_WEIGHT)
    if baseline_hits is None:
        raise ValueError("Candidate weights must include the weight-zero baseline")
    if not has_support:
        return OptimizedCellPolicy(
            enabled=True,
            weight=default_weight,
            selected_weight=None,
            baseline_hits=baseline_hits,
            selected_hits=baseline_hits,
        )
    selected_weight, selected_hits = max(
        candidates.items(),
        key=lambda item: (*item[1], -item[0]),
    )
    improved = selected_hits > baseline_hits
    return OptimizedCellPolicy(
        enabled=improved,
        weight=selected_weight if improved else default_weight,
        selected_weight=selected_weight,
        baseline_hits=baseline_hits,
        selected_hits=selected_hits,
    )


def _runner_precedes(
    other: LinearRunnerScore,
    winner: LinearRunnerScore,
    weight: float,
) -> bool:
    other_score = other.intercept + weight * other.adjustment
    winner_score = winner.intercept + weight * winner.adjustment
    return other_score > winner_score or (
        other_score == winner_score and other.horse_id < winner.horse_id
    )


def _winner_ranks(race: LinearRaceScores, weight: float) -> list[int]:
    all_runners = race.winners + race.competitors
    return [
        1 + sum(_runner_precedes(other, winner, weight) for other in all_runners)
        for winner in race.winners
    ]


def _hits_for_rank(rank: int) -> tuple[int, ...]:
    return tuple(int(rank <= top_k) for top_k in range(1, TOP_K_COUNT + 1))


def _aggregate_hits(races: Sequence[LinearRaceScores], weight: float) -> tuple[int, ...]:
    totals = [0] * TOP_K_COUNT
    for race in races:
        best_winner_rank = min(_winner_ranks(race, weight))
        for index, hit in enumerate(_hits_for_rank(best_winner_rank)):
            totals[index] += hit
    return tuple(totals)


def _crossing_weight(first: LinearRunnerScore, second: LinearRunnerScore) -> float | None:
    denominator = first.adjustment - second.adjustment
    if denominator == 0.0:
        return None
    weight = (second.intercept - first.intercept) / denominator
    if not math.isfinite(weight) or weight <= BASELINE_WEIGHT or weight >= MAXIMUM_WEIGHT:
        return None
    return weight


def _winner_competitor_event(
    race_index: int,
    winner_index: int,
    winner: LinearRunnerScore,
    competitor: LinearRunnerScore,
) -> _CrossingEvent | None:
    weight = _crossing_weight(winner, competitor)
    if weight is None:
        return None
    delta = 1 if competitor.adjustment > winner.adjustment else -1
    return _CrossingEvent(weight, race_index, ((winner_index, delta),))


def _winner_pair_event(
    race_index: int,
    first_index: int,
    first: LinearRunnerScore,
    second_index: int,
    second: LinearRunnerScore,
) -> _CrossingEvent | None:
    weight = _crossing_weight(first, second)
    if weight is None:
        return None
    first_delta = 1 if second.adjustment > first.adjustment else -1
    return _CrossingEvent(
        weight,
        race_index,
        ((first_index, first_delta), (second_index, -first_delta)),
    )


def _winner_competitor_events(
    race_index: int,
    race: LinearRaceScores,
) -> Iterable[_CrossingEvent]:
    for winner_index, winner in enumerate(race.winners):
        for competitor in race.competitors:
            event = _winner_competitor_event(race_index, winner_index, winner, competitor)
            if event is not None:
                yield event


def _winner_pair_events(
    race_index: int,
    race: LinearRaceScores,
) -> Iterable[_CrossingEvent]:
    indexed_winners = tuple(enumerate(race.winners))
    for (first_index, first), (second_index, second) in combinations(indexed_winners, 2):
        event = _winner_pair_event(race_index, first_index, first, second_index, second)
        if event is not None:
            yield event


def _all_crossing_events(races: Sequence[LinearRaceScores]) -> list[_CrossingEvent]:
    events = [
        event
        for race_index, race in enumerate(races)
        for event in (
            *_winner_competitor_events(race_index, race),
            *_winner_pair_events(race_index, race),
        )
    ]
    events.sort(key=lambda event: event.weight)
    return events


def _apply_crossing_group(
    group: Sequence[_CrossingEvent],
    winner_ranks: list[list[int]],
    aggregate_hits: list[int],
) -> None:
    deltas_by_race: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for event in group:
        for winner_index, delta in event.winner_rank_deltas:
            deltas_by_race[event.race_index][winner_index] += delta
    for race_index, rank_deltas in deltas_by_race.items():
        old_hits = _hits_for_rank(min(winner_ranks[race_index]))
        for winner_index, delta in rank_deltas.items():
            winner_ranks[race_index][winner_index] += delta
        new_hits = _hits_for_rank(min(winner_ranks[race_index]))
        for index, (old_hit, new_hit) in enumerate(zip(old_hits, new_hits, strict=True)):
            aggregate_hits[index] += new_hit - old_hit


def _stable_interval_candidates(
    races: Sequence[LinearRaceScores],
) -> dict[float, tuple[int, ...]]:
    baseline_hits = _aggregate_hits(races, BASELINE_WEIGHT)
    candidates = {BASELINE_WEIGHT: baseline_hits}
    winner_ranks = [_winner_ranks(race, BASELINE_WEIGHT) for race in races]
    aggregate_hits = list(baseline_hits)
    grouped_events = [
        (weight, list(group))
        for weight, group in groupby(_all_crossing_events(races), key=lambda event: event.weight)
    ]
    for index, (weight, group) in enumerate(grouped_events):
        _apply_crossing_group(group, winner_ranks, aggregate_hits)
        next_weight = (
            grouped_events[index + 1][0] if index + 1 < len(grouped_events) else MAXIMUM_WEIGHT
        )
        candidates[weight + (next_weight - weight) / 2.0] = tuple(aggregate_hits)
    candidates[MAXIMUM_WEIGHT] = _aggregate_hits(races, MAXIMUM_WEIGHT)
    return candidates


def optimize_linear_race_weights(
    races: Sequence[LinearRaceScores],
    *,
    default_weight: float,
) -> OptimizedCellPolicy:
    """Optimize by sweeping stable winner-rank crossing intervals.

    The non-monotonic TopK objective is unsuitable for binary or ternary
    search. Sorting analytic score crossings gives an exact O(E log E) sweep
    over stable intervals without a fixed grid or Cartesian cell search.
    """
    return select_optimal_cell_weight(
        _stable_interval_candidates(races),
        has_support=bool(races),
        default_weight=default_weight,
    )
