#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Average per-horse predicted_score across N JSONL prediction files and re-rank
within each race.

Each input JSONL must follow the schema:
  {race_id, ketto_toroku_bango, umaban, predicted_score, predicted_rank}

The averaged JSONL is written in the same schema, with predicted_rank recomputed
from the averaged scores (rank 1 = highest score, ties broken by stable sort).
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="average-jsonl-predictions")
    parser.add_argument(
        "--inputs",
        nargs="+",
        type=Path,
        required=True,
        help="Two or more JSONL prediction files to average.",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(f"non-object JSONL line in {path}: {line!r}")
            rows.append(payload)
    return rows


def _row_key(row: dict[str, object]) -> tuple[str, str]:
    return (str(row["race_id"]), str(row["ketto_toroku_bango"]))


def _as_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise TypeError(f"cannot convert {type(value)!r} to float")


def _average_scores(
    inputs: list[Path],
) -> dict[tuple[str, str], dict[str, object]]:
    accumulator: dict[tuple[str, str], list[float]] = defaultdict(list)
    metadata: dict[tuple[str, str], dict[str, object]] = {}
    for path in inputs:
        rows = _load_jsonl(path)
        for row in rows:
            key = _row_key(row)
            accumulator[key].append(_as_float(row["predicted_score"]))
            if key not in metadata:
                metadata[key] = {
                    "race_id": row["race_id"],
                    "ketto_toroku_bango": row["ketto_toroku_bango"],
                    "umaban": row.get("umaban"),
                }
    averaged: dict[tuple[str, str], dict[str, object]] = {}
    for key, scores in accumulator.items():
        meta = metadata[key]
        averaged[key] = {
            "race_id": meta["race_id"],
            "ketto_toroku_bango": meta["ketto_toroku_bango"],
            "umaban": meta["umaban"],
            "predicted_score": sum(scores) / len(scores),
        }
    return averaged


def _group_by_race(
    averaged: dict[tuple[str, str], dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in averaged.values():
        grouped[str(row["race_id"])].append(row)
    return grouped


def _rerank_within_race(
    grouped: dict[str, list[dict[str, object]]],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for race_id in sorted(grouped.keys()):
        horses = grouped[race_id]
        horses_sorted = sorted(
            horses,
            key=lambda row: (
                -_as_float(row["predicted_score"]),
                str(row["ketto_toroku_bango"]),
            ),
        )
        for rank, row in enumerate(horses_sorted, start=1):
            row_out = dict(row)
            row_out["predicted_rank"] = rank
            output.append(row_out)
    return output


def average_predictions(inputs: list[Path], output: Path) -> int:
    if len(inputs) < 2:
        raise ValueError("at least two input JSONLs are required for averaging")
    averaged = _average_scores(inputs)
    grouped = _group_by_race(averaged)
    final_rows = _rerank_within_race(grouped)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for row in final_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(final_rows)


def main() -> None:
    args = _parse_args()
    count = average_predictions(list(args.inputs), args.output)
    print(json.dumps({"output": str(args.output), "rows": count}, ensure_ascii=False))


if __name__ == "__main__":
    main()
