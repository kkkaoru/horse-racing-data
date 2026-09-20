#!/usr/bin/env python3
"""Materialize an offline copy of the Container's production feature snapshot.

Runs the same DuckDB base build and the same per-category layer chain the
Container shells out to at serving time, so a model trained on the result sees
exactly the feature surface production can compute. The frozen Prophet lookup
layer is skipped: it requires a single target date and only appends post-model
adjustment inputs.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
VIEWER_SCRIPTS = REPO_ROOT / "apps/pc-keiba-viewer/src/scripts"
BASE_BUILDER = VIEWER_SCRIPTS / "finish_position_features_duckdb.py"
LAYER_DIR = VIEWER_SCRIPTS / "finish-position-features"
CONTAINER_SRC = REPO_ROOT / "apps/finish-position-predict-container/src"
RESOURCE_SCRIPTS: frozenset[str] = frozenset(
    {
        "add-near-miss-features.py",
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        "add-trainer-stable-affinity-features.py",
        "add-pacestyle-features.py",
        "add-relationship-r1-features.py",
        "add-similar-race-features.py",
        "add-sire-venue-bias-features.py",
    }
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="build_production_feature_snapshot")
    parser.add_argument("--category", choices=("jra", "nar", "ban-ei"), required=True)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse an existing base directory and any completed layer directory",
    )
    parser.add_argument(
        "--skip-layer",
        action="append",
        dest="skip_layers",
        default=[],
        help="Layer basename to skip (repeatable) when no model needs its columns",
    )
    return parser.parse_args(argv)


def run(argv: list[str]) -> None:
    print(f"$ {' '.join(argv)}", flush=True)
    subprocess.run(argv, check=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sys.path.insert(0, str(CONTAINER_SRC))
    from predict_lib.pipeline_args import (
        NEAR_MISS_SCRIPT,
        PROPHET_ENTITY_TREND_SCRIPT,
        build_layer_argv,
        layer_chain_for,
    )

    args.work_dir.mkdir(parents=True, exist_ok=True)
    base_dir = args.work_dir / "base"
    if args.resume and base_dir.exists():
        print(f"reusing base: {base_dir}", flush=True)
    else:
        if base_dir.exists():
            shutil.rmtree(base_dir)
        run(
            [
                args.python_bin,
                str(BASE_BUILDER),
                "--category",
                args.category,
                "--from-date",
                args.from_date,
                "--to-date",
                args.to_date,
                "--output-dir",
                str(base_dir),
                "--pg-url",
                args.pg_url,
                "--threads",
                str(args.threads),
                "--memory-limit",
                args.memory_limit,
            ]
        )
    source = base_dir
    skipped = set(args.skip_layers)
    for index, script in enumerate(layer_chain_for(args.category)):
        if script == PROPHET_ENTITY_TREND_SCRIPT or script in skipped:
            print(f"skipping {script}", flush=True)
            continue
        target = args.work_dir / f"layer-{index:02d}"
        if args.resume and target.exists():
            print(f"reusing {target}", flush=True)
            source = target
            continue
        if target.exists():
            shutil.rmtree(target)
        argv = build_layer_argv(
            script,
            args.category,
            LAYER_DIR,
            source,
            target,
            args.pg_url,
        )
        argv[0] = args.python_bin
        if script == NEAR_MISS_SCRIPT:
            argv += [
                "--target-from-date",
                args.from_date,
                "--target-to-date",
                args.to_date,
            ]
        if script in RESOURCE_SCRIPTS:
            argv += ["--threads", str(args.threads), "--memory-limit", args.memory_limit]
        run(argv)
        source = target
    if args.output_dir.exists():
        shutil.rmtree(args.output_dir)
    shutil.move(str(source), str(args.output_dir))
    print(f"snapshot ready: {args.output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
