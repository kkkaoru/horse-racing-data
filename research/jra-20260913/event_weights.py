"""A fixed target-event group-mass intervention, retaining all broad-history races."""

from __future__ import annotations

from collections.abc import Collection

import numpy as np
from numpy.typing import NDArray

EVENT_GROUP_MASS: float = 0.5


def balanced_event_weights(
    race_ids: NDArray[np.str_], event_race_ids: Collection[str]
) -> NDArray[np.float64]:
    """Assign half the unique-race weight mass to each stratum; no zero weights.

    Every row within a race receives that race's group weight. Normalization is
    over unique races, not rows, and does not claim equal realized loss gradients.
    """
    if race_ids.ndim != 1 or race_ids.size == 0 or np.any(race_ids == ""):
        raise ValueError("Race IDs must be a nonempty one-dimensional array of nonempty IDs")
    unique, inverse = np.unique(race_ids, return_inverse=True)
    event_ids = set(event_race_ids)
    if event_ids - set(unique.tolist()):
        raise ValueError("Event races must belong to the training population")
    event = np.isin(unique, tuple(event_ids))
    event_count = int(event.sum())
    other_count = len(unique) - event_count
    if event_count == 0 or other_count == 0:
        raise ValueError("Both event and other-race strata are required")
    weights = np.where(
        event,
        len(unique) * EVENT_GROUP_MASS / event_count,
        len(unique) * (1 - EVENT_GROUP_MASS) / other_count,
    )
    return np.asarray(weights[inverse], dtype=np.float64)
