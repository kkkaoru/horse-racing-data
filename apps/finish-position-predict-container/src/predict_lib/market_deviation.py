"""Serving twin of ``add_exotic_market_deviation_features.py`` (win-odds free).

Win (単勝) odds and popularity are banned as finish-position inputs. The only
odds-derived inputs are two 馬連 (umaren) / 3連複 (sanrenpuku) *rank residual*
columns: ``log(share * N)`` minus its rank-conditional expectation, so the
market's own ordering (= popularity) is removed. Training computes them from
JV-Data final odds; serving computes them here from the hot worker's latest
``umaren`` / ``3renpuku`` combinations right before scoring. The math and the
baseline lookup are shared with the offline layer (parity pinned by
``tests/test_market_deviation.py``).
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

UMAREN_FEATURE: Final[str] = "exotic_umaren_p2_rank_residual"
SANRENPUKU_FEATURE: Final[str] = "exotic_sanrenpuku_p3_rank_residual"
MARKET_DEVIATION_FEATURES: Final[tuple[str, ...]] = (UMAREN_FEATURE, SANRENPUKU_FEATURE)
UMAREN_POOL_KEY: Final[str] = "umaren"
SANRENPUKU_POOL_KEY: Final[str] = "3renpuku"
MIN_RUNNERS: Final[int] = 3
UMABAN_FIELD: Final[str] = "umaban"
BASELINE_PATH_ENV: Final[str] = "MARKET_DEVIATION_BASELINE_PATH"
DEFAULT_BASELINE_PATH: Final[str] = "/app/lookups/exotic-market-deviation-baseline.json"
"""Baked by the Dockerfile from ``apps/pc-keiba-viewer/finish-position/lookups``."""

Entry = Mapping[str, object]


@dataclass(frozen=True)
class RaceOddsPools:
    """Normalized per-horse shares of the umaren (top-2) and sanrenpuku (top-3) pools."""

    umaren: Mapping[int, float]
    sanrenpuku: Mapping[int, float]


@dataclass(frozen=True)
class Baseline:
    """Per-category ``E[log(share*N) | N, rank]`` keyed by ``"N:rank"``."""

    umaren: Mapping[str, float]
    sanrenpuku: Mapping[str, float]


def baseline_path() -> Path:
    """Resolve the baked baseline lookup (env override for local runs)."""
    return Path(os.environ.get(BASELINE_PATH_ENV, DEFAULT_BASELINE_PATH))


def load_baseline(path: Path, category: str) -> Baseline:
    """Read the per-category rank-conditional expectations."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    pools = payload["categories"][category]
    return Baseline(umaren=dict(pools["umaren"]), sanrenpuku=dict(pools["sanrenpuku"]))


def _normalize(raw: Mapping[int, float]) -> dict[int, float]:
    total = sum(raw.values())
    if total <= 0:
        return {}
    return {horse: value / total for horse, value in raw.items()}


def _add_inverse(shares: dict[int, float], horses: Iterable[int], odds: float) -> None:
    for horse in horses:
        shares[horse] = shares.get(horse, 0.0) + 1.0 / odds


def shares_from_combinations(entries: object, horse_count: int) -> dict[int, float]:
    """Convert ``[{"combination": "1-2", "odds": 5.3}, ...]`` into normalized shares."""
    if not isinstance(entries, list):
        return {}
    raw: dict[int, float] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        parts = str(entry.get("combination", "")).split("-")
        odds = entry.get("odds")
        if len(parts) != horse_count or not all(part.strip().isdigit() for part in parts):
            continue
        if isinstance(odds, bool) or not isinstance(odds, (int, float)) or odds <= 0:
            continue
        _add_inverse(raw, [int(part) for part in parts], float(odds))
    return _normalize(raw)


def pools_from_latest(latest: object) -> RaceOddsPools:
    """Build pools from the hot worker's ``latest`` odds map (missing pools -> empty)."""
    if not isinstance(latest, dict):
        return RaceOddsPools(umaren={}, sanrenpuku={})
    return RaceOddsPools(
        umaren=shares_from_combinations(latest.get(UMAREN_POOL_KEY), 2),
        sanrenpuku=shares_from_combinations(latest.get(SANRENPUKU_POOL_KEY), 3),
    )


def market_ranks(shares: Mapping[int, float]) -> dict[int, float]:
    """1-based market rank (largest share first); tied shares get their average rank."""
    return {
        horse: 1
        + sum(1 for other in shares.values() if other > share)
        + (sum(1 for other in shares.values() if other == share) - 1) / 2
        for horse, share in shares.items()
    }


def baseline_key(runner_count: int, rank: float) -> str:
    """Lookup key ``"N:rank"`` with the rank rounded to the nearest half."""
    return f"{runner_count}:{round(rank * 2) / 2}"


def rank_residuals(shares: Mapping[int, float], expected: Mapping[str, float]) -> dict[int, float]:
    """``log(share*N) - E[. | N, rank]`` for horses whose (N, rank) cell is in the baseline."""
    count = len(shares)
    ranks = market_ranks(shares)
    residuals: dict[int, float] = {}
    for horse, share in shares.items():
        mean = expected.get(baseline_key(count, ranks[horse]))
        if mean is not None:
            residuals[horse] = math.log(share * count) - mean
    return residuals


def compute_race_features(pools: RaceOddsPools, baseline: Baseline) -> dict[int, dict[str, float]]:
    """Return ``{umaban: {feature: value}}`` for horses priced in both pools."""
    common = sorted(set(pools.umaren) & set(pools.sanrenpuku))
    if len(common) < MIN_RUNNERS:
        return {}
    umaren = rank_residuals(_normalize({h: pools.umaren[h] for h in common}), baseline.umaren)
    sanrenpuku = rank_residuals(
        _normalize({h: pools.sanrenpuku[h] for h in common}), baseline.sanrenpuku
    )
    features: dict[int, dict[str, float]] = {}
    for horse in common:
        values: dict[str, float] = {}
        if horse in umaren:
            values[UMAREN_FEATURE] = umaren[horse]
        if horse in sanrenpuku:
            values[SANRENPUKU_FEATURE] = sanrenpuku[horse]
        features[horse] = values
    return features


def _umaban(entry: Entry) -> int | None:
    value = entry.get(UMABAN_FIELD)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or math.isnan(float(value)):
        return None
    return int(value)


def attach_market_deviation(
    races: Mapping[str, Sequence[Entry]],
    pools_by_race_id: Mapping[str, RaceOddsPools],
    baseline: Baseline,
) -> dict[str, list[dict[str, object]]]:
    """Overwrite the deviation columns on every entry; NaN when unpriced.

    Races without pools (odds not yet published, fetch failure) get NaN, the
    same representation the offline layer stores for races lacking final odds.
    """
    updated: dict[str, list[dict[str, object]]] = {}
    for race_id, entries in races.items():
        pools = pools_by_race_id.get(race_id)
        features = compute_race_features(pools, baseline) if pools is not None else {}
        rows: list[dict[str, object]] = []
        for entry in entries:
            umaban = _umaban(entry)
            horse = features.get(umaban, {}) if umaban is not None else {}
            rows.append(
                {**entry, **{name: horse.get(name, math.nan) for name in MARKET_DEVIATION_FEATURES}}
            )
        updated[race_id] = rows
    return updated
