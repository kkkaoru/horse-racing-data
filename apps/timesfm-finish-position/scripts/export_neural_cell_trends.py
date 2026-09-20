#!/usr/bin/env python3
"""Export per-runner neural ranking scores into the cell-policy trend layout.

The production cell-policy evaluator consumes per-runner trend parquets with
``race_id``/``horse_id``/``ranking_score`` (see ``load_lab_trends``). This script
reshapes an existing portable neural prediction artifact into that layout so the
same analytic per-cell weight search can be run against the neural signal.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="export_neural_cell_trends")
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    table = pq.read_table(args.predictions, columns=["race_id", "horse_id", "prediction"])
    scores = [float(value) for value in table.column("prediction").to_pylist()]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "race_id": table.column("race_id"),
                "horse_id": table.column("horse_id"),
                "ranking_score": pa.array(scores, type=pa.float64()),
            }
        ),
        args.output,
    )
    print(f"{args.output} rows={len(scores)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
