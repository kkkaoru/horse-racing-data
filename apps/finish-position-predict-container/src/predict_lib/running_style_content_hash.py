"""Timestamp-independent fingerprint for finish-position running-style inputs."""

from __future__ import annotations

import hashlib
import json
import math
import struct
from collections.abc import Iterable

type RunningStyleContentRow = tuple[
    str,
    str,
    str,
    str,
    int,
    str,
    float,
    float,
    float,
    float,
    int,
]


def _float64_hex(value: float) -> str:
    return struct.pack(">d", value).hex()


def _canonical_row(row: RunningStyleContentRow) -> str:
    (
        source,
        run_ymd,
        venue_raw,
        race_raw,
        horse_number,
        ketto_toroku_bango,
        p_nige,
        p_senkou,
        p_sashi,
        p_oikomi,
        predicted_class,
    ) = row
    venue = int(venue_raw)
    race = int(race_raw)
    ketto = ketto_toroku_bango.strip()
    probabilities = (float(p_nige), float(p_senkou), float(p_sashi), float(p_oikomi))
    if source not in {"jra", "nar"}:
        raise ValueError("invalid running-style source")
    if len(run_ymd) != 8 or not run_ymd.isdigit():
        raise ValueError("invalid running-style date")
    if venue <= 0 or race <= 0 or horse_number <= 0 or not ketto:
        raise ValueError("invalid running-style identity")
    if predicted_class not in {0, 1, 2, 3}:
        raise ValueError("invalid running-style class")
    if not all(math.isfinite(value) for value in probabilities):
        raise ValueError("invalid running-style probability")
    values = [
        f"{source}:{run_ymd}:{venue:02d}:{race:02d}",
        str(horse_number),
        ketto,
        *(_float64_hex(value) for value in probabilities),
        str(predicted_class),
    ]
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def compute_running_style_content_hash(rows: Iterable[RunningStyleContentRow]) -> str:
    """Return SHA-256 over sorted exact feature-cell representations."""

    canonical_rows = sorted(_canonical_row(row) for row in rows)
    return hashlib.sha256("\n".join(canonical_rows).encode()).hexdigest()
