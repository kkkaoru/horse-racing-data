#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Slice a Hive-partitioned feature parquet to labeled rows in a date range.

Useful when feeding the finish-position Transformer `train` subcommand,
which has no date filter and crashes if it sees NaN finish_position
values mid-batch. Copies only rows where race_date is between
[--from-date, --to-date] (inclusive) and finish_position is non-null,
preserving the race_year partition layout.

Run with:
  .venv/bin/python src/scripts/filter_labeled_parquet.py \\
    --input ../../tmp/feat-v15-rs/jra \\
    --from-date 20160101 --to-date 20251231 \\
    --output ../../tmp/feat-v15-rs-labeled/jra
"""
from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="filter_labeled_parquet")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--from-date", type=str, required=True)
    parser.add_argument("--to-date", type=str, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = pl.read_parquet(args.input)
    in_range = df.filter(
        pl.col("finish_position").is_not_null()
        & (pl.col("race_date") >= args.from_date)
        & (pl.col("race_date") <= args.to_date)
    )
    args.output.mkdir(parents=True, exist_ok=True)
    in_range.write_parquet(args.output, partition_by=["race_year"], mkdir=True)
    print(f"input={df.height} labeled_in_range={in_range.height} output={args.output}")


if __name__ == "__main__":
    main()
