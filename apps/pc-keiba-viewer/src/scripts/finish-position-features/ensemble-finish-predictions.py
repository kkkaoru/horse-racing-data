#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Average two finish-position prediction JSONLs (typically lambdarank-lightgbm
+ transformer) by predicted_rank, then re-rank within race so the output keeps
the contract expected by import-finish-position-predictions.ts (race_id +
ketto_toroku_bango + umaban + predicted_score + predicted_rank).

Run with:
  .venv/bin/python src/scripts/finish-position-features/ensemble-finish-predictions.py \\
    --inputs tmp/.../jra-lgbm/2024-2025.jsonl tmp/.../jra-trans/2024-2025.jsonl \\
    --output tmp/.../jra-ensemble/2024-2025.jsonl
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ensemble_finish_predictions")
    parser.add_argument("--inputs", nargs="+", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if line == "":
                continue
            records.append(json.loads(line))
    return records


def index_by_key(rows: list[dict[str, object]]) -> dict[tuple[str, str], dict[str, object]]:
    return {(str(row["race_id"]), str(row["ketto_toroku_bango"])): row for row in rows}


def average_rank(records: list[dict[str, object]]) -> float:
    return sum(float(row["predicted_rank"]) for row in records) / len(records)


def merge_records(loaded: list[list[dict[str, object]]]) -> list[dict[str, object]]:
    indexed = [index_by_key(records) for records in loaded]
    common_keys = set(indexed[0].keys())
    for table in indexed[1:]:
        common_keys &= set(table.keys())
    merged: list[dict[str, object]] = []
    for key in sorted(common_keys):
        rows = [table[key] for table in indexed]
        avg = average_rank(rows)
        head = rows[0]
        merged.append({
            "ketto_toroku_bango": head["ketto_toroku_bango"],
            "predicted_rank": avg,
            "predicted_score": -avg,
            "race_id": head["race_id"],
            "umaban": head["umaban"],
        })
    return merged


def assign_ranks_within_race(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(str(row["race_id"]), []).append(row)
    output: list[dict[str, object]] = []
    for race_id in sorted(grouped):
        ordered = sorted(grouped[race_id], key=lambda r: (float(r["predicted_rank"]), int(r["umaban"])))
        for rank, row in enumerate(ordered, start=1):
            output.append({**row, "predicted_rank": rank})
    return output


def write_jsonl(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()
    loaded = [load_jsonl(path) for path in args.inputs]
    merged = merge_records(loaded)
    ranked = assign_ranks_within_race(merged)
    write_jsonl(ranked, args.output)
    print(json.dumps({"inputs": [str(p) for p in args.inputs], "output": str(args.output), "rows": len(ranked)}))


if __name__ == "__main__":
    main()
