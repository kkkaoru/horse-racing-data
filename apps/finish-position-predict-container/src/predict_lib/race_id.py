"""Parse / format the canonical ``race_id`` string.

A ``race_id`` is ``{source}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}``
(five colon-separated parts), mirroring ``blend-and-insert.py`` and the TypeScript
``import-finish-position-predictions`` ``parseRaceId`` helper. Keeping a single
parser here avoids drift between the container predictor and the viewer importer.
"""

from __future__ import annotations

from typing import Final, NamedTuple

RACE_ID_PART_COUNT: Final[int] = 5
RACE_ID_SEPARATOR: Final[str] = ":"


class RaceIdParts(NamedTuple):
    """The five components decoded from a ``race_id`` string."""

    source: str
    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str


def parse_race_id(race_id: str) -> RaceIdParts:
    """Split a ``race_id`` into its five components.

    Raises ``ValueError`` when the string does not have exactly five non-empty
    parts so a malformed id never silently produces a partial primary key.
    """
    parts = race_id.split(RACE_ID_SEPARATOR)
    if len(parts) != RACE_ID_PART_COUNT:
        message = f"race_id must have {RACE_ID_PART_COUNT} parts, got {len(parts)}: {race_id}"
        raise ValueError(message)
    if any(part == "" for part in parts):
        message = f"race_id parts must be non-empty: {race_id}"
        raise ValueError(message)
    return RaceIdParts(
        source=parts[0],
        kaisai_nen=parts[1],
        kaisai_tsukihi=parts[2],
        keibajo_code=parts[3],
        race_bango=parts[4],
    )


def format_race_id(parts: RaceIdParts) -> str:
    """Re-join ``RaceIdParts`` into the canonical ``race_id`` string."""
    return RACE_ID_SEPARATOR.join(parts)
