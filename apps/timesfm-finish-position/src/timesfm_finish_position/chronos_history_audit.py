"""Repair partial non-domestic JVD context using official starter counts."""

from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DOMESTIC_VENUES: tuple[str, ...] = tuple(f"{number:02d}" for number in range(1, 11))


def repair_non_domestic_context(
    *,
    history: Path,
    output: Path,
    declared_sizes: Mapping[str, int],
    include_domestic: bool = False,
) -> None:
    """Change only performance/field size; other partial-race features stay uncertified.

    JVD can contain only the Japanese entrants and winner of an overseas race.
    Their row count is not a valid normalization denominator. Every represented
    non-domestic JVD race requires an explicit official starter count.
    include_domestic also corrects domestic transfer context denominators.
    """
    table = pq.read_table(history)
    races = np.asarray(table["race_id"].to_pylist(), dtype=np.str_)
    venues = np.asarray(table["venue_code"].to_pylist(), dtype=np.str_)
    selected_context = np.char.startswith(races, "jra:")
    if not include_domestic:
        selected_context &= ~np.isin(venues, DOMESTIC_VENUES)
    if set(races[selected_context]) != set(declared_sizes):
        raise ValueError("Official metadata must exactly cover the selected JVD contexts")
    sizes = np.asarray(table["field_size"].to_pylist(), dtype=np.int64)
    finish = np.asarray(table["finish_position"].to_pylist(), dtype=np.int64)
    ratings = np.asarray(table["performance_rating"].to_pylist(), dtype=np.float64)
    if any(isinstance(size, bool) or size < 1 for size in declared_sizes.values()):
        raise ValueError("Official starter count must be a positive integer")
    official = np.asarray(
        [declared_sizes[str(race)] for race in races[selected_context]], dtype=np.int64
    )
    if np.any((finish[selected_context] < 1) | (finish[selected_context] > official)) or np.any(
        sizes[selected_context] > official
    ):
        raise ValueError("Official starter count conflicts with observed finishers")
    sizes[selected_context] = official
    ratings[selected_context] = 1.0 - (finish[selected_context] - 1.0) / np.maximum(official - 1, 1)
    table = table.set_column(
        table.schema.get_field_index("field_size"), "field_size", pa.array(sizes)
    )
    table = table.set_column(
        table.schema.get_field_index("performance_rating"), "performance_rating", pa.array(ratings)
    )
    pq.write_table(table, output)
