#!/usr/bin/env python3
"""Blend per-model upcoming-race JSONL predictions with fixed weights, rank
within race, and upsert into race_finish_position_model_predictions."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import cast

DEFAULT_PG = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--jsonl", action="append", required=True, help="label=weight:path (e.g. lambdarank=0.45:tmp/jra-lambdarank.jsonl)")
    p.add_argument("--model-version", required=True)
    p.add_argument("--source", required=True, help="jra or nar")
    p.add_argument("--output-jsonl", type=Path, required=True)
    p.add_argument("--pg-url", default=os.environ.get("PG_URL", DEFAULT_PG))
    return p.parse_args()


def load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def normalize_within_race(rows: list[dict[str, object]]) -> dict[tuple[str, str], float]:
    by_race: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in rows:
        by_race[cast(str, r["race_id"])].append(
            (str(r["ketto_toroku_bango"]), float(cast(float, r["predicted_score"])))
        )
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


def main() -> None:
    args = parse_args()
    components: list[tuple[str, float, list[dict[str, object]]]] = []
    for spec in args.jsonl:
        label, rest = spec.split("=", 1)
        weight_str, path_str = rest.split(":", 1)
        components.append((label, float(weight_str), load_jsonl(Path(path_str))))
    normalized = [(w, normalize_within_race(rows)) for _, w, rows in components]
    umaban_lookup: dict[tuple[str, str], int] = {}
    for _, _, rows in components:
        for r in rows:
            k = (cast(str, r["race_id"]), str(r["ketto_toroku_bango"]))
            if "umaban" in r and r["umaban"] is not None:
                umaban_lookup[k] = int(cast(int, r["umaban"]))
    blended: dict[tuple[str, str], float] = defaultdict(float)
    for w, norm in normalized:
        if w == 0: continue
        for k, v in norm.items():
            blended[k] += w * v
    by_race: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (rid, hid), s in blended.items():
        by_race[rid].append((hid, s))
    rows_out: list[dict[str, object]] = []
    for rid in sorted(by_race):
        ranked = sorted(by_race[rid], key=lambda x: -x[1])
        for rank, (hid, sc) in enumerate(ranked, start=1):
            r: dict[str, object] = {
                "race_id": rid,
                "ketto_toroku_bango": hid,
                "predicted_score": sc,
                "predicted_rank": rank,
            }
            if (rid, hid) in umaban_lookup:
                r["umaban"] = umaban_lookup[(rid, hid)]
            rows_out.append(r)
    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with args.output_jsonl.open("w") as f:
        for r in rows_out:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    # parse race_id into pg fields and emit insert rows
    # race_id like "jra:2024:0101:45:08" => source / kaisai_nen / kaisai_tsukihi / keibajo_code / race_bango
    insert_rows: list[dict[str, object]] = []
    for r in rows_out:
        parts = cast(str, r["race_id"]).split(":")
        if len(parts) != 5:
            continue
        src, kaisai_nen, mmdd, keibajo, bango = parts
        kaisai_tsukihi = mmdd
        insert_rows.append({
            "model_version": args.model_version,
            "source": src,
            "kaisai_nen": kaisai_nen,
            "kaisai_tsukihi": kaisai_tsukihi,
            "keibajo_code": keibajo,
            "race_bango": bango,
            "ketto_toroku_bango": r["ketto_toroku_bango"],
            "umaban": r.get("umaban", 0),
            "predicted_score": r["predicted_score"],
            "predicted_rank": r["predicted_rank"],
        })
    # Build SQL VALUES
    if not insert_rows:
        print(json.dumps({"output_jsonl": str(args.output_jsonl), "inserted": 0}))
        return
    cols = ["model_version", "source", "kaisai_nen", "kaisai_tsukihi", "keibajo_code",
            "race_bango", "ketto_toroku_bango", "umaban", "predicted_score", "predicted_rank"]
    def esc(s: object) -> str:
        if s is None: return "null"
        if isinstance(s, (int, float)): return str(s)
        return "'" + str(s).replace("'", "''") + "'"
    values_sql = ",\n".join(
        "(" + ",".join(esc(r[c]) for c in cols) + ")"
        for r in insert_rows
    )
    sql = (
        f"insert into race_finish_position_model_predictions ({','.join(cols)}) values\n"
        f"{values_sql}\n"
        "on conflict (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)\n"
        "do update set umaban = excluded.umaban, predicted_score = excluded.predicted_score, predicted_rank = excluded.predicted_rank, prediction_generated_at = now();"
    )
    sql_path = args.output_jsonl.with_suffix(".sql")
    sql_path.write_text(sql, encoding="utf-8")
    res = subprocess.run(
        ["psql", args.pg_url, "-c", sql],
        capture_output=True, text=True
    )
    if res.returncode != 0:
        print("PSQL ERROR:", res.stderr)
        raise SystemExit(1)
    print(json.dumps({"output_jsonl": str(args.output_jsonl), "inserted_rows": len(insert_rows), "races": len(by_race)}))


if __name__ == "__main__":
    main()
