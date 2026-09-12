"""Development-only canonical-cell ranking diagnostics on cached CPU forecasts."""

from collections import Counter
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow.parquet as pq

from timesfm_finish_position.chronos_readout import ReadoutRace

MARKET_WEIGHTS = (0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0)
DOMESTIC_VENUES = frozenset(f"{number:02d}" for number in range(1, 11))
MARKET_LOCK_ROUNDOFF_ULPS: int = 8


def relative_scores(values: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    """Convert known forecasts to within-race percentiles; no singleton ranking."""
    result = np.full(len(values), np.nan, dtype=np.float64)
    known = np.flatnonzero(np.isfinite(values))
    if len(known) < 2:
        return result
    inverse, counts = np.unique(values[known], return_inverse=True, return_counts=True)[1:]
    average_positions = np.cumsum(counts) - (counts + 1) / 2
    result[known] = average_positions[inverse] / (len(known) - 1)
    return result


def prediction_order(race: ReadoutRace, market_weight: float) -> npt.NDArray[np.int64]:
    """Deterministic shared ordering for exact metrics and race-level swap audits."""
    if not 0 <= market_weight <= 1:
        raise ValueError("Invalid market weight")
    scores = race.market.copy()
    known = np.isfinite(race.temporal)
    scores[known] = market_weight * race.market[known] + (1 - market_weight) * race.temporal[known]
    return np.lexsort((race.horse_ids, -scores)).astype(np.int64, copy=False)


def market_order_locked_by_range(race: ReadoutRace, market_weight: float) -> bool:
    """Sufficient, not necessary, no-swap certificate for the observed score range.

    Missing temporal scores use the market, matching prediction_order. A positive
    margin between every adjacent market score exceeds even the worst opposing
    temporal difference when w * min_gap > (1-w) * temporal_range, with a
    conservative roundoff guard. This does not constrain future output ranges.
    """
    if not 0 <= market_weight <= 1:
        raise ValueError("Invalid market weight")
    if not np.isfinite(race.market).all():
        raise ValueError("Nonfinite market scores")
    if len(race.market) < 2 or market_weight == 1:
        return True
    market = race.market[prediction_order(race, 1.0)]
    effective = np.where(np.isfinite(race.temporal), race.temporal, race.market)
    gap = float(np.min(market[:-1] - market[1:]))
    spread = float(np.ptp(effective))
    scale = max(1.0, float(np.max(np.abs(effective))), float(np.max(np.abs(market))))
    roundoff = MARKET_LOCK_ROUNDOFF_ULPS * np.finfo(np.float64).eps * scale
    return bool(market_weight * gap > (1 - market_weight) * spread + roundoff)


def exact_hits(races: Sequence[ReadoutRace], market_weight: float) -> list[int]:
    """Count exact ordinal matches, not winner-in-TopK recall."""
    if not 0 <= market_weight <= 1:
        raise ValueError("Invalid market weight")
    hits = np.zeros(5, dtype=np.int64)
    for race in races:
        finish = race.finish[prediction_order(race, market_weight)][:5]
        hits[: len(finish)] += (finish == np.arange(1, len(finish) + 1)).astype(np.int64)
    return hits.tolist()


def exact_support(races: Sequence[ReadoutRace]) -> list[int]:
    support = np.zeros(5, dtype=np.int64)
    for race in races:
        support += np.isin(np.arange(1, 6), race.finish).astype(np.int64)
    return support.tolist()


def race_hits(races: Sequence[ReadoutRace], market_weight: float) -> list[int]:
    """Count races with at least one official winner in TopK, including dead heats."""
    if not 0 <= market_weight <= 1:
        raise ValueError("Invalid market weight")
    hits = np.zeros(5, dtype=np.int64)
    for race in races:
        order = prediction_order(race, market_weight)
        winners = race.finish[order] == 1
        hits += np.array([np.any(winners[:depth]) for depth in range(1, 6)], dtype=np.int64)
    return hits.tolist()


def _cell_order(item: tuple[str, int]) -> tuple[int, str]:
    return -item[1], item[0]


def diagnose_cells(
    *, head: Path, lora: Path, mapping: Path, max_cells: int = 6, minimum_races: int = 8
) -> dict[str, object]:
    if max_cells < 1 or minimum_races < 1:
        raise ValueError("Cell count and minimum races must be positive")
    order = [("race_id", "ascending"), ("horse_id", "ascending")]
    head_table, lora_table = pq.read_table(head).sort_by(order), pq.read_table(lora).sort_by(order)
    if not head_table.drop(["chronos_cpu"]).equals(lora_table.drop(["chronos_cpu"])):
        raise ValueError("Head and LoRA source cohorts must match exactly")
    frame = head_table.to_pandas()
    if (
        frame[["race_id", "horse_id"]].isna().any().any()
        or frame.duplicated(["race_id", "horse_id"]).any()
    ):
        raise ValueError("Null or duplicate runner identity")
    if not frame.race_date.between("20230101", "20231231").all():
        raise ValueError("This diagnostic is restricted to the frozen 2023 development cohort")
    frame["lora_cpu"] = lora_table["chronos_cpu"].to_numpy()
    cells = pq.read_table(mapping).to_pandas()
    if cells.race_id.duplicated().any():
        raise ValueError("Duplicate canonical race mapping")
    frame = frame[frame.venue_code.isin(DOMESTIC_VENUES)].merge(
        cells[["race_id", "cell_id", "condition_code", "race_identity"]],
        on="race_id",
        how="left",
        validate="many_to_one",
    )
    if frame.cell_id.isna().any():
        raise ValueError("Missing canonical domestic race mapping")
    records: dict[str, list[tuple[ReadoutRace, ReadoutRace]]] = {}
    complete_counts: Counter[str] = Counter()
    exclusions: Counter[str] = Counter()
    spread: dict[str, list[float]] = {}
    for race_id, group in frame.groupby("race_id", sort=True):
        if group.field_size.nunique() != 1 or len(group) != int(group.field_size.iloc[0]):
            raise ValueError(f"Incomplete source race: {race_id}")
        cell = str(group.cell_id.iloc[0])
        if str(group.condition_code.iloc[0]) == "999" and not str(
            group.race_identity.iloc[0]
        ).startswith("name:"):
            exclusions["unnamed_open"] += 1
            continue
        complete_counts[cell] += 1
        odds = group.decimal_odds.to_numpy(dtype=np.float64)
        if (
            not np.isfinite(odds).all()
            or np.any(odds <= 1)
            or not (group.finish_position == 1).any()
        ):
            exclusions[f"invalid_market_or_winner:{cell}"] += 1
            continue
        identities = group.horse_id.to_numpy(dtype=np.str_)
        market_order = np.lexsort((identities, odds))
        market = np.empty(len(group), dtype=np.float64)
        market[market_order] = np.linspace(1.0, 0.0, len(group))
        finish = group.finish_position.to_numpy(dtype=np.int64)
        head_values = group.chronos_cpu.to_numpy(dtype=np.float64)
        lora_values = group.lora_cpu.to_numpy(dtype=np.float64)
        records.setdefault(cell, []).append(
            (
                ReadoutRace(identities, market, finish, head_values),
                ReadoutRace(identities, market, finish, lora_values),
            )
        )
        known = head_values[np.isfinite(head_values)]
        if len(known) > 1:
            spread.setdefault(cell, []).append(float(np.ptp(known)))
    selected = [
        cell
        for cell, count in sorted(complete_counts.items(), key=_cell_order)
        if count >= minimum_races
    ][:max_cells]
    results: list[dict[str, object]] = []
    for cell in selected:
        pairs = records.get(cell, [])
        head_races = [pair[0] for pair in pairs]
        market_hits = race_hits(head_races, 1.0)
        readouts = _model_readouts(pairs, market_hits)
        results.append(
            {
                "cell_id": cell,
                "complete_source_races": complete_counts[cell],
                "evaluated_races": len(pairs),
                "market_hits": market_hits,
                "head_median_within_race_spread": float(np.median(spread[cell]))
                if cell in spread
                else None,
                "runners": sum(len(race.horse_ids) for race in head_races),
                "head_known_runners": sum(
                    int(np.isfinite(race.temporal).sum()) for race in head_races
                ),
                "readouts": readouts,
            }
        )
    return {
        "selection": "2023 complete-source counts only; descending count then cell ID",
        "metric": "races with any winner in TopK",
        "cells": results,
        "exclusions": dict(exclusions),
        "incumbent_comparison": "unavailable",
        "production_eligible": False,
    }


def _model_readouts(
    pairs: list[tuple[ReadoutRace, ReadoutRace]], baseline: list[int]
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, name in enumerate(("head", "lora")):
        raw = [pair[index] for pair in pairs]
        relative = [
            ReadoutRace(race.horse_ids, race.market, race.finish, relative_scores(race.temporal))
            for race in raw
        ]
        rows.extend(_readouts(raw, name, "raw", baseline))
        rows.extend(_readouts(relative, name, "relative", baseline))
    return rows


def _readouts(
    races: list[ReadoutRace], model: str, representation: str, baseline: list[int]
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for weight in MARKET_WEIGHTS:
        hits = race_hits(races, weight)
        delta = [value - base for value, base in zip(hits, baseline, strict=True)]
        rows.append(
            {
                "model": model,
                "representation": representation,
                "market_weight": weight,
                "hits": hits,
                "delta": delta,
                "development_market_guard": delta[0] > 0 and all(value >= 0 for value in delta[1:]),
            }
        )
    return rows
