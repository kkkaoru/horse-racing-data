#!/usr/bin/env python3
"""Append win-odds-free "market deviation" features from 馬連 / 3連複 odds.

Win (単勝) odds and popularity are banned as finish-position inputs because a
ranking built on them reproduces the popularity order. 馬連 (umaren) and 3連複
(sanrenpuku) odds are allowed, but only as *deviation from the market's own
ordering*: a raw implied probability simply re-encodes popularity.

Per race, each pool is turned into overround-normalized per-horse shares over the
horses priced in both pools, and every horse gets

  ``log(share * N) - E[log(share * N) | N, market rank]``

where ``E`` is a fixed per-category baseline fitted on 2013-2024 final odds
(``finish-position/lookups/exotic-market-deviation-baseline.json``). Subtracting
the rank-conditional expectation removes the popularity order itself: on 2025
JRA races the residuals have within-race Spearman -0.08 / -0.02 with win
popularity while keeping -0.16 / -0.12 with the finish position. Rank-shape
features that still tracked popularity (log p3 - log p2: +0.30) were rejected.

Bulk/offline mode reads final odds (``data_kubun='5'``) from ``{jvd,nvd}_o2`` /
``{jvd,nvd}_o5``. Serving computes the same values in
``finish-position-predict-container/src/predict_lib/market_deviation.py``.
A race missing either pool, or a (N, rank) cell absent from the baseline, gets NULL.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb

from _catalog_attach import attach_source_catalog

RaceKey = tuple[str, str, str, str]

UMAREN_FEATURE: Final[str] = "exotic_umaren_p2_rank_residual"
SANRENPUKU_FEATURE: Final[str] = "exotic_sanrenpuku_p3_rank_residual"
FEATURE_COLUMNS: Final[tuple[str, ...]] = (UMAREN_FEATURE, SANRENPUKU_FEATURE)
# JV-Data O2 / O5 packed layouts (verified against stored rows: an 18-runner
# umaren string is 153 * 13 chars, sanrenpuku 816 * 15 chars).
UMAREN_STRIDE: Final[int] = 13  # h1(2) h2(2) odds(6, x10) popularity(3)
UMAREN_ODDS_SLICE: Final[slice] = slice(4, 10)
SANRENPUKU_STRIDE: Final[int] = 15  # h1(2) h2(2) h3(2) odds(6, x10) popularity(3)
SANRENPUKU_ODDS_SLICE: Final[slice] = slice(6, 12)
ODDS_SCALE: Final[float] = 10.0
MIN_RUNNERS: Final[int] = 3
FINAL_ODDS_DATA_KUBUN: Final[str] = "5"
BANEI_KEIBAJO_CODE: Final[str] = "83"
DEFAULT_BASELINE_PATH: Final[Path] = (
    Path(__file__).resolve().parents[3]
    / "finish-position/lookups/exotic-market-deviation-baseline.json"
)


@dataclass(frozen=True)
class RaceOdds:
    umaren: Mapping[int, float]
    sanrenpuku: Mapping[int, float]


@dataclass(frozen=True)
class Baseline:
    umaren: Mapping[str, float]
    sanrenpuku: Mapping[str, float]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_exotic_market_deviation_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--category", choices=("jra", "nar", "ban-ei"), required=True)
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    return parser.parse_args(argv)


def load_baseline(path: Path, category: str) -> Baseline:
    """Read the per-category rank-conditional expectations."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    pools = payload["categories"][category]
    return Baseline(umaren=dict(pools["umaren"]), sanrenpuku=dict(pools["sanrenpuku"]))


def _add_inverse(shares: dict[int, float], horses: Iterable[int], odds: float) -> None:
    for horse in horses:
        shares[horse] = shares.get(horse, 0.0) + 1.0 / odds


def _normalize(raw: Mapping[int, float]) -> dict[int, float]:
    total = sum(raw.values())
    if total <= 0:
        return {}
    return {horse: value / total for horse, value in raw.items()}


def _decode_packed(
    packed: str, stride: int, horse_count: int, odds_slice: slice
) -> dict[int, float]:
    raw: dict[int, float] = {}
    for start in range(0, len(packed) - stride + 1, stride):
        entry = packed[start : start + stride]
        horse_text = [entry[index * 2 : index * 2 + 2] for index in range(horse_count)]
        odds_text = entry[odds_slice]
        if not all(text.isdigit() for text in (*horse_text, odds_text)):
            continue
        horses = [int(text) for text in horse_text]
        odds = int(odds_text) / ODDS_SCALE
        if odds <= 0 or 0 in horses:
            continue
        _add_inverse(raw, horses, odds)
    return _normalize(raw)


def decode_umaren(packed: str) -> dict[int, float]:
    """Decode a JV-Data O2 string into per-horse normalized top-2 shares."""
    return _decode_packed(packed, UMAREN_STRIDE, 2, UMAREN_ODDS_SLICE)


def decode_sanrenpuku(packed: str) -> dict[int, float]:
    """Decode a JV-Data O5 string into per-horse normalized top-3 shares."""
    return _decode_packed(packed, SANRENPUKU_STRIDE, 3, SANRENPUKU_ODDS_SLICE)


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


def compute_race_features(odds: RaceOdds, baseline: Baseline) -> dict[int, dict[str, float]]:
    """Return ``{umaban: {feature: value}}`` for horses priced in both pools."""
    common = sorted(set(odds.umaren) & set(odds.sanrenpuku))
    if len(common) < MIN_RUNNERS:
        return {}
    umaren = rank_residuals(_normalize({h: odds.umaren[h] for h in common}), baseline.umaren)
    sanrenpuku = rank_residuals(
        _normalize({h: odds.sanrenpuku[h] for h in common}), baseline.sanrenpuku
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


def _odds_tables(category: str) -> tuple[str, str, str]:
    prefix = "jvd" if category == "jra" else "nvd"
    if category == "ban-ei":
        venue_filter = f"t.keibajo_code = '{BANEI_KEIBAJO_CODE}'"
    elif category == "nar":
        venue_filter = f"t.keibajo_code <> '{BANEI_KEIBAJO_CODE}'"
    else:
        venue_filter = "true"
    return f"pg.{prefix}_o2", f"pg.{prefix}_o5", venue_filter


def load_warehouse_odds(
    con: duckdb.DuckDBPyConnection, category: str, races: Sequence[RaceKey]
) -> dict[RaceKey, RaceOdds]:
    """Fetch final umaren / sanrenpuku strings for ``races`` from the warehouse."""
    if not races:
        return {}
    o2_table, o5_table, venue_filter = _odds_tables(category)
    year_list = ", ".join(f"'{year}'" for year in sorted({race[0] for race in races}))
    con.execute(
        "create or replace temp table target_races "
        "(kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar)"
    )
    con.executemany("insert into target_races values (?, ?, ?, ?)", [list(race) for race in races])
    rows = con.execute(
        f"""
        select t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
               o2.odds_umaren, o5.odds_sanrenpuku
        from target_races t
        join (select * from {o2_table} where kaisai_nen in ({year_list})
              and data_kubun = '{FINAL_ODDS_DATA_KUBUN}') o2
          using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        join (select * from {o5_table} where kaisai_nen in ({year_list})
              and data_kubun = '{FINAL_ODDS_DATA_KUBUN}') o5
          using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        where {venue_filter}
        """
    ).fetchall()
    return {
        (str(r[0]), str(r[1]), str(r[2]), str(r[3])): RaceOdds(
            umaren=decode_umaren(str(r[4] or "")), sanrenpuku=decode_sanrenpuku(str(r[5] or ""))
        )
        for r in rows
    }


def build_feature_rows(
    entries: Sequence[tuple[str, str, str, str, int]],
    odds_by_race: Mapping[RaceKey, RaceOdds],
    baseline: Baseline,
) -> list[tuple[object, ...]]:
    """Return one ``(race cols..., umaban, features...)`` row per input entry."""
    features_by_race = {
        race: compute_race_features(odds, baseline) for race, odds in odds_by_race.items()
    }
    rows: list[tuple[object, ...]] = []
    for nen, tsukihi, keibajo, bango, umaban in entries:
        horse = features_by_race.get((nen, tsukihi, keibajo, bango), {}).get(umaban, {})
        rows.append((nen, tsukihi, keibajo, bango, umaban, *(horse.get(c) for c in FEATURE_COLUMNS)))
    return rows


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    baseline = load_baseline(args.baseline, args.category)
    con = duckdb.connect()
    input_glob = (args.input_dir / "**" / "*.parquet").as_posix()
    con.execute(
        "create temp table base as select * from "
        f"read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)"
    )
    entries = [
        (str(r[0]), str(r[1]), str(r[2]), str(r[3]), int(r[4]))
        for r in con.execute(
            "select distinct kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, "
            "cast(umaban as integer) from base where umaban is not null"
        ).fetchall()
    ]
    attach_source_catalog(con, args.pg_url)
    odds_by_race = load_warehouse_odds(con, args.category, sorted({entry[:4] for entry in entries}))
    con.execute(
        "create temp table deviation (kaisai_nen varchar, kaisai_tsukihi varchar, "
        "keibajo_code varchar, race_bango varchar, umaban integer, "
        + ", ".join(f"{column} double" for column in FEATURE_COLUMNS)
        + ")"
    )
    rows = build_feature_rows(entries, odds_by_race, baseline)
    if rows:
        placeholders = ", ".join("?" for _ in range(5 + len(FEATURE_COLUMNS)))
        con.executemany(f"insert into deviation values ({placeholders})", rows)
    con.execute(
        f"""
        create temp table output as
        select b.* exclude ({", ".join(FEATURE_COLUMNS)}),
               {", ".join(f"d.{column}" for column in FEATURE_COLUMNS)}
        from (select *, {", ".join(f"null::double as {c}" for c in FEATURE_COLUMNS)} from base) b
        left join deviation d
          on b.kaisai_nen = d.kaisai_nen and b.kaisai_tsukihi = d.kaisai_tsukihi
         and b.keibajo_code = d.keibajo_code and b.race_bango = d.race_bango
         and cast(b.umaban as integer) = d.umaban
        """
    )
    if args.output_dir.exists():
        shutil.rmtree(args.output_dir)
    args.output_dir.mkdir(parents=True)
    years = [row[0] for row in con.execute("select distinct race_year from output order by 1").fetchall()]
    for year in years:
        year_dir = args.output_dir / f"race_year={year}"
        year_dir.mkdir(parents=True)
        target = (year_dir / "data_0.parquet").as_posix()
        con.execute(
            f"copy (select * exclude (race_year) from output where race_year = {year}) "
            f"to '{target}' (format parquet)"
        )


if __name__ == "__main__":
    main()
