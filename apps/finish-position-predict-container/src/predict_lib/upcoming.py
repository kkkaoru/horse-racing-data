"""Turn scored race entries into UPSERT-ready prediction rows.

Pure transform: given one race's entries (each a mapping with at least
``ketto_toroku_bango`` / ``umaban`` / the model features) plus the raw scores
produced by the injected booster, produce the flat value tuples that
``upsert_sql.build_upsert_sql`` expects. PG access and model loading happen in
``predict_upcoming.py``; this layer is fully deterministic and testable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .model_meta import Category, model_version_for
from .race_id import RaceIdParts, parse_race_id
from .rank import RankedHorse, ScoredHorse, rank_within_race

KETTO_FIELD: str = "ketto_toroku_bango"
UMABAN_FIELD: str = "umaban"


def _to_scored_horse(entry: Mapping[str, object], score: float) -> ScoredHorse:
    """Build a ``ScoredHorse`` from an entry mapping and its raw score."""
    return ScoredHorse(
        ketto_toroku_bango=str(entry[KETTO_FIELD]),
        umaban=int(_coerce_umaban(entry.get(UMABAN_FIELD))),
        predicted_score=float(score),
    )


def _coerce_umaban(value: object) -> int:
    """Coerce umaban to int, defaulting missing/None to 0 (entry-only row)."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return int(float(str(value)))


def rank_race_entries(
    entries: Sequence[Mapping[str, object]],
    scores: Sequence[float],
) -> list[RankedHorse]:
    """Rank one race's entries from their parallel raw scores.

    Raises ``ValueError`` when the entry and score sequences differ in length so
    a misaligned scoring call never produces silently shifted ranks.
    """
    if len(entries) != len(scores):
        message = f"entries ({len(entries)}) and scores ({len(scores)}) length mismatch"
        raise ValueError(message)
    scored = [
        _to_scored_horse(entry, score) for entry, score in zip(entries, scores, strict=True)
    ]
    return rank_within_race(scored)


def build_prediction_rows(
    race_id: str,
    category: Category,
    ranked: Sequence[RankedHorse],
) -> list[list[object]]:
    """Flatten ranked horses into UPSERT value tuples for one race.

    Column order matches ``upsert_sql.INSERT_COLUMNS``. ``predicted_top1_prob`` /
    ``predicted_top3_prob`` / ``predicted_finish_position`` are left ``None`` —
    the v7-lineage rankers emit a relevance score + rank, not calibrated
    probabilities (mirrors the importer's ``flattenForInsert``).
    """
    parts: RaceIdParts = parse_race_id(race_id)
    model_version = model_version_for(category)
    return [
        [
            model_version,
            parts.source,
            parts.kaisai_nen,
            parts.kaisai_tsukihi,
            parts.keibajo_code,
            parts.race_bango,
            horse.ketto_toroku_bango,
            horse.umaban,
            horse.predicted_score,
            horse.predicted_rank,
            None,
            None,
            None,
        ]
        for horse in ranked
    ]
