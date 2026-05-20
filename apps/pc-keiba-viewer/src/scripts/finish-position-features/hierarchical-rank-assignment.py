#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Hierarchical rank re-assignment with cascade gate.

Inputs:
  --top1-jsonl     : 現 active model の predicted_rank (これが rank 1 を支配)
  --place2-jsonl   : place2 specialist の predicted_score (rank 1 を除いた中で最大スコアが rank 2 候補)
  --place3-jsonl   : place3 specialist (任意。無い場合は rank 3 は top1 model fallback)
  --cascade-threshold : top1 model の rank1 と rank2 のスコア差 normalize 値 (default 0.05)
                        threshold 未満なら top1 model も迷っているとみなして specialist 不採用
  --output         : 再割り当て後の jsonl (1 行 / 馬)

ロジック (per race):
  1. rank 1 = top1 model の predicted_rank == 1
  2. rank 2 候補:
     - top1 model の rank 2 (top1_rank2)
     - place2 specialist の rank 1 を rank 1 から除外したもの (p2_pick)
     - if cascade_confidence > threshold and p2_pick != rank1 → rank 2 = p2_pick
     - else → rank 2 = top1_rank2
  3. rank 3 候補: 同様 (place3 specialist or top1 model fallback)
  4. rank 4+: top1 model order (rank 1,2,3 除外後)

Tie-break: predicted_score 同点なら umaban 昇順
Output: race_id, ketto_toroku_bango, umaban, predicted_score (per-race rank normalized to [0,1]), predicted_rank
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import TypedDict


class HorsePred(TypedDict):
    race_id: str
    ketto_toroku_bango: str
    umaban: int
    predicted_score: float
    predicted_rank: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="hierarchical_rank_assignment")
    parser.add_argument("--top1-jsonl", type=Path, required=True)
    parser.add_argument("--place2-jsonl", type=Path, required=True)
    parser.add_argument("--place3-jsonl", type=Path, default=None,
                        help="Optional. If omitted, rank 3 falls back to top1 model's rank 3.")
    parser.add_argument("--cascade-threshold", type=float, default=0.05,
                        help="Normalized score gap (rank1-rank2)/rank1 below which specialist is NOT used.")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def load_predictions_by_race(path: Path) -> dict[str, list[HorsePred]]:
    by_race: dict[str, list[HorsePred]] = defaultdict(list)
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            pred: HorsePred = {
                "race_id": row["race_id"],
                "ketto_toroku_bango": row["ketto_toroku_bango"],
                "umaban": int(row["umaban"]) if row.get("umaban") is not None else -1,
                "predicted_score": float(row["predicted_score"]),
                "predicted_rank": int(row["predicted_rank"]),
            }
            by_race[pred["race_id"]].append(pred)
    return by_race


def sort_by_score(horses: list[HorsePred]) -> list[HorsePred]:
    return sorted(horses, key=lambda h: (-h["predicted_score"], h["umaban"]))


def cascade_confidence(top1_sorted: list[HorsePred]) -> float:
    """(rank1_score - rank2_score) / abs(rank1_score) で確信度。score が負も許容。"""
    if len(top1_sorted) < 2:
        return 1.0
    s1 = top1_sorted[0]["predicted_score"]
    s2 = top1_sorted[1]["predicted_score"]
    denom = max(abs(s1), 1e-9)
    return (s1 - s2) / denom


def assign_ranks_for_race(
    race_id: str,
    top1_horses: list[HorsePred],
    place2_horses: list[HorsePred] | None,
    place3_horses: list[HorsePred] | None,
    cascade_threshold: float,
) -> list[HorsePred]:
    top1_sorted = sort_by_score(top1_horses)
    confidence = cascade_confidence(top1_sorted)
    by_umaban: dict[int, HorsePred] = {h["umaban"]: h for h in top1_horses}
    chosen_order: list[int] = []
    used: set[int] = set()

    if not top1_sorted:
        return []

    rank1 = top1_sorted[0]
    chosen_order.append(rank1["umaban"])
    used.add(rank1["umaban"])

    rank2_pick: HorsePred | None = None
    if place2_horses and confidence > cascade_threshold:
        p2_sorted = sort_by_score([h for h in place2_horses if h["umaban"] not in used])
        if p2_sorted:
            candidate = p2_sorted[0]
            actual = by_umaban.get(candidate["umaban"])
            if actual is not None:
                rank2_pick = actual
    if rank2_pick is None:
        for h in top1_sorted:
            if h["umaban"] not in used:
                rank2_pick = h
                break
    if rank2_pick is not None:
        chosen_order.append(rank2_pick["umaban"])
        used.add(rank2_pick["umaban"])

    rank3_pick: HorsePred | None = None
    if place3_horses and confidence > cascade_threshold:
        p3_sorted = sort_by_score([h for h in place3_horses if h["umaban"] not in used])
        if p3_sorted:
            candidate = p3_sorted[0]
            actual = by_umaban.get(candidate["umaban"])
            if actual is not None:
                rank3_pick = actual
    if rank3_pick is None:
        for h in top1_sorted:
            if h["umaban"] not in used:
                rank3_pick = h
                break
    if rank3_pick is not None:
        chosen_order.append(rank3_pick["umaban"])
        used.add(rank3_pick["umaban"])

    for h in top1_sorted:
        if h["umaban"] not in used:
            chosen_order.append(h["umaban"])
            used.add(h["umaban"])

    n = len(chosen_order)
    out: list[HorsePred] = []
    for rank, umaban in enumerate(chosen_order, start=1):
        horse = by_umaban[umaban]
        normalized = (n - rank) / max(n - 1, 1)
        out.append({
            "race_id": race_id,
            "ketto_toroku_bango": horse["ketto_toroku_bango"],
            "umaban": umaban,
            "predicted_score": float(normalized),
            "predicted_rank": rank,
        })
    return out


def main() -> None:
    args = parse_args()
    top1_by_race = load_predictions_by_race(args.top1_jsonl)
    place2_by_race = load_predictions_by_race(args.place2_jsonl)
    place3_by_race = (
        load_predictions_by_race(args.place3_jsonl) if args.place3_jsonl is not None else {}
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    total_races = 0
    cascade_skipped_count = 0
    with args.output.open("w", encoding="utf-8") as fp:
        for race_id, top1_horses in top1_by_race.items():
            place2_horses = place2_by_race.get(race_id)
            place3_horses = place3_by_race.get(race_id) if place3_by_race else None
            conf = cascade_confidence(sort_by_score(top1_horses))
            if conf <= args.cascade_threshold:
                cascade_skipped_count += 1
            assigned = assign_ranks_for_race(
                race_id, top1_horses, place2_horses, place3_horses, args.cascade_threshold,
            )
            for row in assigned:
                fp.write(json.dumps(row) + "\n")
            total_races += 1

    stats = {
        "total_races": total_races,
        "cascade_skipped_races": cascade_skipped_count,
        "cascade_skip_ratio": cascade_skipped_count / max(total_races, 1),
        "cascade_threshold": args.cascade_threshold,
    }
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
