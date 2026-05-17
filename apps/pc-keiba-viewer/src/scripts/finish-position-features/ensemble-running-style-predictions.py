#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Average probability ensemble for running-style predictions.

Reads two (or more) JSONL prediction files emitted by running_style_lightgbm
and writes a single JSONL with averaged p_nige/p_senkou/p_sashi/p_oikomi
plus argmax predicted_label and predicted_class.

Run with:
  .venv/bin/python src/scripts/finish-position-features/ensemble-running-style-predictions.py \\
    --inputs tmp/eval-rs-jra-v4/2024-2025.jsonl tmp/eval-rs-jra-v4-nocw/2024-2025.jsonl \\
    --output tmp/eval-rs-jra-ens/2024-2025.jsonl
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TypedDict

CLASS_LABELS: tuple[str, str, str, str] = ("nige", "senkou", "sashi", "oikomi")
PROBABILITY_COLUMNS: tuple[str, str, str, str] = ("p_nige", "p_senkou", "p_sashi", "p_oikomi")


class PredictionKey(TypedDict):
    race_id: str
    ketto_toroku_bango: str
    umaban: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ensemble_running_style_predictions")
    parser.add_argument("--inputs", nargs="+", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def load_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if line == "":
                continue
            records.append(json.loads(line))
    return records


def make_key(record: dict[str, object]) -> tuple[str, str, int]:
    return (
        str(record.get("race_id", "")),
        str(record.get("ketto_toroku_bango", "")),
        int(record.get("umaban") or 0),
    )


def index_by_key(records: list[dict[str, object]]) -> dict[tuple[str, str, int], dict[str, object]]:
    return {make_key(rec): rec for rec in records}


def average_probabilities(records: list[dict[str, object]]) -> dict[str, float]:
    averaged: dict[str, float] = {}
    for column in PROBABILITY_COLUMNS:
        values = [float(rec.get(column, 0.0)) for rec in records]
        averaged[column] = sum(values) / len(values) if values else 0.0
    return averaged


def argmax_label(probabilities: dict[str, float]) -> tuple[str, int]:
    best_idx = 0
    best_value = probabilities[PROBABILITY_COLUMNS[0]]
    for idx, column in enumerate(PROBABILITY_COLUMNS):
        if probabilities[column] > best_value:
            best_value = probabilities[column]
            best_idx = idx
    return CLASS_LABELS[best_idx], best_idx


def ensemble_records(loaded: list[list[dict[str, object]]]) -> list[dict[str, object]]:
    indexed = [index_by_key(records) for records in loaded]
    common_keys = set(indexed[0].keys())
    for index in indexed[1:]:
        common_keys &= set(index.keys())
    out: list[dict[str, object]] = []
    for key in sorted(common_keys):
        records = [index[key] for index in indexed]
        probabilities = average_probabilities(records)
        predicted_label, predicted_class = argmax_label(probabilities)
        first = records[0]
        ensemble_record: dict[str, object] = {
            "race_id": first.get("race_id"),
            "ketto_toroku_bango": first.get("ketto_toroku_bango"),
            "umaban": first.get("umaban"),
            "race_year": first.get("race_year"),
            "target_running_style_class": first.get("target_running_style_class"),
            **probabilities,
            "predicted_label": predicted_label,
            "predicted_class": predicted_class,
        }
        out.append(ensemble_record)
    return out


def write_jsonl(records: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()
    loaded = [load_jsonl(path) for path in args.inputs]
    ensembled = ensemble_records(loaded)
    write_jsonl(ensembled, args.output)
    print(json.dumps({"inputs": [str(p) for p in args.inputs], "output": str(args.output), "rows": len(ensembled)}))


if __name__ == "__main__":
    main()
