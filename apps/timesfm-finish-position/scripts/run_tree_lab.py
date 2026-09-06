#!/usr/bin/env python3
"""Run nested walk-forward tree rankers and persist aligned OOF predictions."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from timesfm_finish_position.tabular_evaluation import (
    evaluate_tree_fold,
    load_tabular_dataset,
    write_prediction_frame,
    write_tree_report,
)
from timesfm_finish_position.tree_rankers import RankerKind

DEFAULT_INPUT = Path("tmp/nar-tabular-pit-2020-2026.parquet")
DEFAULT_OUTPUT = Path("tmp/tree-lab")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--estimators", type=int, default=150)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--year", type=int, action="append", dest="years")
    parser.add_argument(
        "--model", choices=[kind.value for kind in RankerKind], action="append", dest="models"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = tuple(args.years or (2024, 2025, 2026))
    kinds = tuple(RankerKind(value) for value in (args.models or tuple(RankerKind)))
    dataset = load_tabular_dataset(args.input)
    results = []
    for kind in kinds:
        for year in years:
            result, predictions = evaluate_tree_fold(
                dataset,
                kind,
                year,
                estimators=args.estimators,
                threads=args.threads,
            )
            results.append(result)
            write_prediction_frame(
                args.output / "predictions" / f"{kind.value}-{year}.parquet", predictions
            )
            print(json.dumps(asdict(result), sort_keys=True), flush=True)
    write_tree_report(args.output / "report.json", tuple(results))


if __name__ == "__main__":
    main()
