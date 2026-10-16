"""Reproduce Arc walk-forward evaluation from a hash-pinned local DB snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import TypeGuard

import pandas as pd

from arc_finish_model import (
    EVALUATION_YEARS,
    FEATURES,
    build_dataset,
    predict_card,
    train_before,
    unordered_topk,
)


def is_record(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict) and all(isinstance(key, str) for key in value)


def snapshot_frame(value: object) -> pd.DataFrame:
    if not isinstance(value, list):
        raise ValueError("Snapshot table must be an array of records")
    records: list[dict[str, object]] = []
    for row in value:
        if not is_record(row):
            raise ValueError("Snapshot row must have string keys")
        records.append(row)
    return pd.DataFrame(records)


def load_dataset(
    *, source: Path, races: Path, source_sha256: str
) -> tuple[pd.DataFrame, dict[str, str]]:
    raw = source.read_bytes()
    if hashlib.sha256(raw).hexdigest() != source_sha256:
        raise ValueError("Source snapshot SHA256 mismatch")
    payload: object = json.loads(raw)
    race_payload: object = json.loads(races.read_text(encoding="utf-8"))
    if not is_record(payload) or not is_record(race_payload):
        raise ValueError("Source and race snapshots must be JSON objects")
    official = snapshot_frame(race_payload["races"])
    dates = (
        official["kaisai_nen"]
        + "-"
        + official["kaisai_tsukihi"].str[:2]
        + "-"
        + official["kaisai_tsukihi"].str[2:]
    )
    expected = dict(zip(dates, official["shusso_tosu"].astype(int), strict=True))
    return build_dataset(
        snapshot_frame(payload["history"]),
        identities=snapshot_frame(payload["identities"]),
        people_daily=snapshot_frame(payload["peopleDaily"]),
        expected_fields=expected,
    )


def evaluate_fold(data: pd.DataFrame, *, year: int, output: Path) -> dict[str, object]:
    card = data.loc[data["race_date"].str.startswith(str(year))]
    if card.empty:
        raise ValueError(f"Missing evaluation year {year}")
    cutoff = str(card["race_date"].iloc[0])
    model = train_before(data, cutoff=cutoff)
    model.save_model(output / f"arc-before-{year}.json")
    predictions = predict_card(model, card)
    predictions.to_json(
        output / f"predictions-{year}.json", orient="records", double_precision=15
    )
    predicted = predictions["horse_id"].tolist()
    actual = card.sort_values(["finish", "horse_id"])["horse_id"].tolist()
    training = data.loc[data["race_date"] < cutoff]
    return {
        "year": year,
        "cutoff": cutoff,
        "trainingMaxDate": str(training["race_date"].max()),
        "trainingRaces": int(training["race_date"].nunique()),
        "runners": len(card),
        "predicted": predicted,
        "actual": actual,
        "metrics": unordered_topk(predicted, actual),
    }


def train_snapshot(
    *,
    source: Path,
    races: Path,
    source_sha256: str,
    cutoff: str,
    output: Path,
) -> dict[str, object]:
    data, excluded = load_dataset(
        source=source, races=races, source_sha256=source_sha256
    )
    # Exclusive directory creation prevents accidental evidence/model replacement.
    output.mkdir(parents=True, exist_ok=False)
    data.to_json(output / "features.json", orient="records", double_precision=15)
    folds = [evaluate_fold(data, year=year, output=output) for year in EVALUATION_YEARS]
    model = train_before(data, cutoff=cutoff)
    model.save_model(output / "arc-production.json")
    report: dict[str, object] = {
        "sourceSha256": source_sha256,
        "raceSnapshotSha256": hashlib.sha256(races.read_bytes()).hexdigest(),
        "features": FEATURES,
        "excluded": excluded,
        "trainingFields": data.groupby("race_date").size().to_dict(),
        "folds": folds,
        "productionCutoffExclusive": cutoff,
        "productionTrainingMaxDate": str(
            data.loc[data["race_date"] < cutoff, "race_date"].max()
        ),
        "productionModelSha256": hashlib.sha256(
            (output / "arc-production.json").read_bytes()
        ).hexdigest(),
        "metricDefinition": "precision@k = |predicted[:k] intersect actual[:k]| / k; exact_set_hit = all k match; order ignored",
        "limitations": "Three evaluation races only; retrospective event-date filtering, not archived pre-race availability. No final odds. No calibrated probabilities. Missing form remains missing.",
    }
    (output / "evaluation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--races", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = train_snapshot(
        source=args.source,
        races=args.races,
        source_sha256=args.source_sha256,
        cutoff=args.cutoff,
        output=args.output,
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
