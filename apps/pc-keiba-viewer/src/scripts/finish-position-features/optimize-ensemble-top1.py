#!/usr/bin/env python3
"""Grid-search ensemble weights to maximize top1_accuracy (rank-normalized blend)."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from itertools import product
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--actuals", type=Path, required=True)
    p.add_argument("--jsonl", action="append", required=True, help="name=label:path")
    p.add_argument("--validation-year", type=int, required=True)
    p.add_argument("--test-year", type=int, required=True)
    p.add_argument("--grid-step", type=float, default=0.1)
    p.add_argument("--output-best", type=Path, required=True)
    p.add_argument("--output-blend", type=Path, required=True)
    return p.parse_args()


def parse_spec(spec: str) -> tuple[str, Path]:
    _nm, rest = spec.split("=", 1)
    label, path = rest.split(":", 1)
    return label, Path(path)


def load_actuals(path: Path) -> dict[tuple[str, str], int]:
    actuals: dict[tuple[str, str], int] = {}
    with path.open() as f:
        header = f.readline().rstrip("\n").split(",")
        ri = header.index("race_id"); ki = header.index("ketto_toroku_bango"); fi = header.index("finish_position")
        for line in f:
            cells = line.rstrip("\n").split(",")
            actuals[(cells[ri], cells[ki])] = int(cells[fi])
    return actuals


def load_scores(path: Path) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = {}
    with path.open() as f:
        for line in f:
            r = json.loads(line)
            out[(str(r["race_id"]), str(r["ketto_toroku_bango"]))] = float(r["predicted_score"])
    return out


def load_umaban(path: Path) -> dict[tuple[str, str], int]:
    out: dict[tuple[str, str], int] = {}
    with path.open() as f:
        for line in f:
            r = json.loads(line)
            if r.get("umaban") is not None:
                out[(str(r["race_id"]), str(r["ketto_toroku_bango"]))] = int(r["umaban"])
    return out


def normalize_within_race(scores: dict[tuple[str, str], float]) -> dict[tuple[str, str], float]:
    by_race: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (rid, hid), s in scores.items():
        by_race[rid].append((hid, s))
    out: dict[tuple[str, str], float] = {}
    for rid, hs in by_race.items():
        n = len(hs)
        if n <= 1:
            for h, _ in hs: out[(rid, h)] = 0.5
            continue
        srt = sorted(hs, key=lambda x: -x[1])
        for rank, (h, _) in enumerate(srt):
            out[(rid, h)] = (n - 1 - rank) / (n - 1)
    return out


def year(rid: str) -> int:
    return int(rid.split(":")[1])


def compute_top1(weights: list[float], normalized: list[dict[tuple[str, str], float]], actuals: dict[tuple[str, str], int]) -> float:
    blended: dict[tuple[str, str], float] = defaultdict(float)
    for norm, w in zip(normalized, weights):
        if w == 0: continue
        for k, v in norm.items():
            blended[k] += w * v
    by_race: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (rid, hid), s in blended.items():
        if (rid, hid) in actuals:
            by_race[rid].append((hid, s))
    hits, total = 0, 0
    for rid, hs in by_race.items():
        if not hs: continue
        top_hid = max(hs, key=lambda x: x[1])[0]
        if actuals.get((rid, top_hid)) == 1: hits += 1
        total += 1
    return hits / max(total, 1)


def simplex_weights(d: int, step: float) -> list[list[float]]:
    n = int(round(1 / step))
    out = []
    for combo in product(range(n + 1), repeat=d - 1):
        last = n - sum(combo)
        if last < 0: continue
        out.append([c * step for c in combo] + [last * step])
    return out


def main() -> None:
    args = parse_args()
    specs = [parse_spec(s) for s in args.jsonl]
    labels = [l for l, _ in specs]
    raw = [load_scores(p) for _, p in specs]
    umaban = {}
    for _, p in specs:
        umaban.update(load_umaban(p))
    actuals = load_actuals(args.actuals)
    val_act = {k: v for k, v in actuals.items() if year(k[0]) == args.validation_year}
    test_act = {k: v for k, v in actuals.items() if year(k[0]) == args.test_year}
    val_normalized = [normalize_within_race({k: v for k, v in r.items() if year(k[0]) == args.validation_year}) for r in raw]
    test_normalized = [normalize_within_race({k: v for k, v in r.items() if year(k[0]) == args.test_year}) for r in raw]
    candidates = simplex_weights(len(labels), args.grid_step)
    best_w: list[float] | None = None
    best_top1 = -1.0
    for w in candidates:
        s = compute_top1(w, val_normalized, val_act)
        if s > best_top1:
            best_top1 = s; best_w = w
    if best_w is None:
        raise SystemExit("no candidates evaluated")
    test_top1 = compute_top1(best_w, test_normalized, test_act)
    all_normalized = [normalize_within_race(r) for r in raw]
    blended: dict[tuple[str, str], float] = defaultdict(float)
    for norm, w in zip(all_normalized, best_w):
        if w == 0: continue
        for k, v in norm.items():
            blended[k] += w * v
    by_race: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (rid, hid), s in blended.items():
        by_race[rid].append((hid, s))
    args.output_blend.parent.mkdir(parents=True, exist_ok=True)
    with args.output_blend.open("w") as f:
        for rid in sorted(by_race):
            ranked = sorted(by_race[rid], key=lambda x: -x[1])
            for rank, (hid, sc) in enumerate(ranked, start=1):
                row = {"race_id": rid, "ketto_toroku_bango": hid, "predicted_score": sc, "predicted_rank": rank}
                if (rid, hid) in umaban: row["umaban"] = umaban[(rid, hid)]
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    result = {
        "models": labels,
        "objective": "top1_accuracy",
        "best_weights": dict(zip(labels, best_w)),
        "validation_year": args.validation_year,
        "validation_top1": best_top1,
        "test_year": args.test_year,
        "test_top1": test_top1,
        "num_candidates_searched": len(candidates),
    }
    args.output_best.parent.mkdir(parents=True, exist_ok=True)
    args.output_best.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
