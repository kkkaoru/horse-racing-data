"""Turn scored race entries into UPSERT-ready prediction rows.

Pure transform: given one race's entries (each a mapping with at least
``ketto_toroku_bango`` / ``umaban`` / the model features) plus the raw scores
produced by the injected booster, produce the flat value tuples that
``upsert_sql.build_upsert_sql`` expects. PG access and model loading happen in
``predict_upcoming.py``; this layer is fully deterministic and testable.

Each row also carries the race's subgroup metadata (``subgroup.classify_all``):
the dimensions are constant across a race so they are classified once from the
race's representative entry plus the parsed ``race_id`` and the persisted subset
(``upsert_sql.PREDICTION_SUBGROUP_COLUMNS``) is appended to every horse's row in that order.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .late_binding import coerce_optional_int
from .model_meta import Category, model_version_for
from .race_id import RaceIdParts, parse_race_id
from .rank import RankedHorse, ScoredHorse, rank_within_race
from .subgroup import classify_all
from .upsert_sql import PREDICTION_SUBGROUP_COLUMNS

KETTO_FIELD: str = "ketto_toroku_bango"
UMABAN_FIELD: str = "umaban"
KYORI_FIELD: str = "kyori"
SHUSSO_TOSU_FIELD: str = "shusso_tosu"
TRACK_CODE_FIELD: str = "track_code"
# Per-category race-class column carrying the subgroup ``class_code`` value.
# Mirrors ``predict_upcoming.CLASS_CODE_FIELD_BY_CATEGORY``: JRA reads the numeric
# ``kyoso_joken_code``, NAR reads the derived ``nar_subclass``. Ban-ei has no
# per-class column so its ``class_code`` stays ``None``.
CLASS_CODE_FIELD_BY_CATEGORY: Mapping[Category, str] = {
    "jra": "kyoso_joken_code",
    "nar": "nar_subclass",
}


def _coerce_optional_text(value: object) -> str | None:
    """Coerce a parquet cell to a non-empty ``str`` or ``None``.

    ``None`` and blank / whitespace-only strings collapse to ``None`` so a
    missing race-class or track code defers to the subgroup classifier's None
    handling instead of a spurious empty-string label.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return text


def _race_class_code(category: Category, entry: Mapping[str, object]) -> str | None:
    """Return the race's per-class ``class_code`` from one entry, or ``None``.

    The column is per-category (JRA ``kyoso_joken_code`` / NAR ``nar_subclass``);
    categories without a per-class column (Ban-ei) return ``None``.
    """
    field = CLASS_CODE_FIELD_BY_CATEGORY.get(category)
    if field is None:
        return None
    return _coerce_optional_text(entry.get(field))


def _subgroup_values(
    category: Category,
    parts: RaceIdParts,
    race_entry: Mapping[str, object] | None,
) -> list[object]:
    """Classify the race's persisted subgroup columns into a value list.

    Returns one value per :data:`upsert_sql.PREDICTION_SUBGROUP_COLUMNS` in that exact order
    so the result aligns with the trailing subgroup columns of
    ``upsert_sql.INSERT_COLUMNS``. ``classify_all`` also derives ``venue`` from
    ``keibajo_code`` but the predictions table does not persist it (the keibajo is
    already a primary-key column), so only the ``PREDICTION_SUBGROUP_COLUMNS`` subset is
    emitted. ``race_entry`` is ``None`` only for legacy callers without entry
    context; the entry-derived dimensions then fall back to ``None`` while the
    race-id-derived season dimension is still populated.
    """
    entry: Mapping[str, object] = race_entry if race_entry is not None else {}
    classified = classify_all(
        kyori=coerce_optional_int(entry.get(KYORI_FIELD)),
        shusso_tosu=coerce_optional_int(entry.get(SHUSSO_TOSU_FIELD)),
        kaisai_tsukihi=parts.kaisai_tsukihi,
        track_code=_coerce_optional_text(entry.get(TRACK_CODE_FIELD)),
        class_code=_race_class_code(category, entry),
        keibajo_code=parts.keibajo_code,
    )
    return [classified[column] for column in PREDICTION_SUBGROUP_COLUMNS]


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
    model_version: str | None = None,
    race_entry: Mapping[str, object] | None = None,
) -> list[list[object]]:
    """Flatten ranked horses into UPSERT value tuples for one race.

    Column order matches ``upsert_sql.INSERT_COLUMNS``. ``predicted_top1_prob`` /
    ``predicted_top3_prob`` / ``predicted_finish_position`` are left ``None`` —
    the v7-lineage rankers emit a relevance score + rank, not calibrated
    probabilities (mirrors the importer's ``flattenForInsert``).

    ``model_version`` defaults to the category-global label (``model_version_for``)
    so existing callers stay backwards-compatible. Per-class JRA routing passes
    the resolved ``resolve_per_class_model_version`` value so an active per-class
    winner lands its predictions under its own ``model_version`` row in PG.

    ``race_entry`` is the race's representative feature entry (any horse — the
    subgroup dimensions are race-level constants). It supplies ``kyori`` /
    ``shusso_tosu`` / ``track_code`` / the per-class code so the trailing
    ``upsert_sql.PREDICTION_SUBGROUP_COLUMNS`` of each row are populated. ``None`` leaves the
    entry-derived dimensions ``None`` (season still classifies from the
    ``race_id``).
    """
    parts: RaceIdParts = parse_race_id(race_id)
    resolved_model_version = model_version if model_version is not None else model_version_for(
        category
    )
    subgroup_values = _subgroup_values(category, parts, race_entry)
    return [
        [
            resolved_model_version,
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
            *subgroup_values,
        ]
        for horse in ranked
    ]
