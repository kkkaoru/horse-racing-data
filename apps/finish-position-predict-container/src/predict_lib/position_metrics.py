"""Exact-position metrics, distinct from winner inclusion in the predicted top K."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class PositionObservation:
    race_id: str
    predicted_rank: int
    actual_finish: int | None


@dataclass(frozen=True)
class ExactPositionMetrics:
    race_count: int
    support: tuple[int, ...]
    hits: tuple[int, ...]
    accuracy: tuple[float | None, ...]


def exact_position_metrics(rows: Sequence[PositionObservation]) -> ExactPositionMetrics:
    """Count one hit per race/position; ties accept any horse at the actual position.

    Missing actual positions have zero support, not a fabricated miss or zero rate.
    None means unranked here; the caller must establish participation/outcome status.
    This metric does not certify the completeness of the supplied race population.
    """
    grouped: dict[str, list[PositionObservation]] = defaultdict(list)
    for row in rows:
        _validate_observation(row)
        grouped[row.race_id].append(row)
    support = [0] * 5
    hits = [0] * 5
    for race in grouped.values():
        race_support, race_hits = _race_counts(race)
        support = [a + b for a, b in zip(support, race_support, strict=True)]
        hits = [a + b for a, b in zip(hits, race_hits, strict=True)]
    return _summary(race_count=len(grouped), support=support, hits=hits)


def parse_exact_position_metrics(payload: object) -> ExactPositionMetrics:
    """Validate serialized counts and recompute derived (possibly rounded) rates."""
    if not isinstance(payload, Mapping):
        raise ValueError("Exact metrics must contain a count mapping")
    return _summary(
        race_count=payload.get("race_count"),
        support=payload.get("support"),
        hits=payload.get("hits"),
    )


def combine_exact_position_metrics(
    metrics: Sequence[ExactPositionMetrics],
) -> ExactPositionMetrics:
    """Pool hits/support, not fold percentages; unsupported positions remain None."""
    races = 0
    support = [0] * 5
    hits = [0] * 5
    for item in metrics:
        valid = _summary(race_count=item.race_count, support=item.support, hits=item.hits)
        races += valid.race_count
        support = [a + b for a, b in zip(support, valid.support, strict=True)]
        hits = [a + b for a, b in zip(hits, valid.hits, strict=True)]
    return _summary(race_count=races, support=support, hits=hits)


def _summary(*, race_count: object, support: object, hits: object) -> ExactPositionMetrics:
    races = _nonnegative_integer(race_count)
    supports = _position_counts(support)
    hit_counts = _position_counts(hits)
    if any(hit > count or count > races for hit, count in zip(hit_counts, supports, strict=True)):
        raise ValueError("Exact hits/support exceed their denominator")
    accuracy = tuple(
        hit / count if count else None for hit, count in zip(hit_counts, supports, strict=True)
    )
    return ExactPositionMetrics(races, supports, hit_counts, accuracy)


def _nonnegative_integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("Metric counts must be nonnegative integers")
    return value


def _position_counts(value: object) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)) or len(value) != 5:
        raise ValueError("Exact metrics need five integer position counts")
    return tuple(_nonnegative_integer(item) for item in value)


def _validate_observation(row: PositionObservation) -> None:
    if not row.race_id.strip() or row.race_id != row.race_id.strip():
        raise ValueError("Race identity must be explicit and trimmed")
    if type(row.predicted_rank) is not int or row.predicted_rank < 1:
        raise ValueError("Predicted rank must be a positive integer")
    if row.actual_finish is not None and (
        type(row.actual_finish) is not int or row.actual_finish < 1
    ):
        raise ValueError("Actual finish must be a positive integer or explicitly unranked")


def _race_counts(rows: Sequence[PositionObservation]) -> tuple[tuple[int, ...], tuple[int, ...]]:
    by_rank = {row.predicted_rank: row.actual_finish for row in rows}
    if set(by_rank) != set(range(1, len(rows) + 1)):
        raise ValueError("Predicted ranks must be a complete unique race permutation")
    actual = {row.actual_finish for row in rows if row.actual_finish is not None}
    if any(rank > len(rows) for rank in actual):
        raise ValueError("Actual finish exceeds the supplied race population")
    support = tuple(int(rank in actual) for rank in range(1, 6))
    hits = tuple(int(by_rank.get(rank) == rank) for rank in range(1, 6))
    return support, hits
