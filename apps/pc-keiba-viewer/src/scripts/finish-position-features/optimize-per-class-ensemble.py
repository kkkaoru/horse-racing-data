#!/usr/bin/env python3
"""Thin CLI shim for ``optimize_per_class_ensemble`` (iter 23 per-class HPO).

The actual logic lives in the underscored sibling module so that pytest /
basedpyright / coverage can locate it via standard ``--cov=`` flags. This
file exists because the project convention places CLI entry points under
``src/scripts/finish-position-features/`` with hyphenated filenames.

Run with:
    uv run python src/scripts/finish-position-features/optimize-per-class-ensemble.py \\
        --class-code 005 --baseline-parquet-dir ... --pg-url ... \\
        --output-summary ... --output-manifest-dir ...
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import optimize_per_class_ensemble as driver


if __name__ == "__main__":
    raise SystemExit(driver.main())
