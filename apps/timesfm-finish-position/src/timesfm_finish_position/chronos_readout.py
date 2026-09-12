"""Development-only complete-source-race market blend diagnostics."""

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow.parquet as pq

MARKET_WEIGHTS = (0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.975, 0.99, 0.995, 0.999, 1.0)


@dataclass(frozen=True)
class ReadoutRace:
    horse_ids: npt.NDArray[np.str_]
    market: npt.NDArray[np.float64]
    finish: npt.NDArray[np.int64]
    temporal: npt.NDArray[np.float64]


def winner_hits(races: list[ReadoutRace], market_weight: float) -> list[int]:
    if not 0 <= market_weight <= 1:
        raise ValueError("Market weight must be in [0,1]")
    hits = np.zeros(5, dtype=np.int64)
    for race in races:
        scores = race.market.copy()
        known = np.isfinite(race.temporal)
        scores[known] = (
            market_weight * race.market[known] + (1 - market_weight) * race.temporal[known]
        )
        order = np.lexsort((race.horse_ids, -scores))
        ranks = np.empty(len(order), dtype=np.int64)
        ranks[order] = np.arange(1, len(order) + 1)
        hits += np.asarray(
            [np.sum((race.finish == 1) & (ranks <= depth)) for depth in range(1, 6)],
            dtype=np.int64,
        )
    return [int(value) for value in hits]


def evaluate_readout(source: Path, *, forecast_column: str) -> dict[str, object]:
    table = pq.read_table(source).to_pydict()
    races = np.asarray(table["race_id"], dtype=np.str_)
    horses = np.asarray(table["horse_id"], dtype=np.str_)
    finish = np.asarray(table["finish_position"], dtype=np.int64)
    odds = np.asarray(table["decimal_odds"], dtype=np.float64)
    field_size = np.asarray(table["field_size"], dtype=np.int64)
    temporal = np.asarray(table[forecast_column], dtype=np.float64)
    grouped: dict[str, list[int]] = {}
    for index, race_id in enumerate(races):
        grouped.setdefault(str(race_id), []).append(index)
    included: list[ReadoutRace] = []
    excluded: list[str] = []
    for race_id, rows in grouped.items():
        indices = np.asarray(rows)
        if len(set(horses[indices])) != len(indices) or not np.all(
            field_size[indices] == len(indices)
        ):
            raise ValueError(f"Duplicate or incomplete source race: {race_id}")
        if not np.all(np.isfinite(odds[indices])) or np.any(odds[indices] <= 1):
            excluded.append(race_id)
            continue
        order = np.lexsort((horses[indices], odds[indices]))
        market = np.empty(len(indices), dtype=np.float64)
        market[order] = np.linspace(1, 0, len(indices))
        included.append(ReadoutRace(horses[indices], market, finish[indices], temporal[indices]))
    if not included:
        raise ValueError("No races with valid market evidence")
    baseline = winner_hits(included, 1.0)
    results: list[dict[str, object]] = []
    for weight in MARKET_WEIGHTS:
        hits = winner_hits(included, weight)
        delta = [model - market for model, market in zip(hits, baseline, strict=True)]
        results.append(
            {
                "market_weight": weight,
                "hits": hits,
                "delta": delta,
                "development_guard": delta[0] > 0 and all(value >= 0 for value in delta[1:]),
            }
        )
    return {
        "forecast_column": forecast_column,
        "races": len(included),
        "excluded_invalid_market_races": excluded,
        "market_hits": baseline,
        "readouts": results,
        "production_eligible": False,
        "limitations": [
            "Final odds proxy, not attested PIT odds",
            "Complete groups relative to finisher export, not starter-roster attestation",
            "No exact current-model comparator",
            "Development evidence only",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--forecast-column", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = evaluate_readout(args.source, forecast_column=args.forecast_column)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)


if __name__ == "__main__":
    main()
