"""CLI for the local-only six-arm TimesFM accuracy experiment."""

from __future__ import annotations

import argparse
from pathlib import Path

from .domain import DEFAULT_TIMESFM_REVISION, ExperimentConfig
from .evaluation import run_experiment, write_report

APP_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = (
    APP_ROOT.parent
    / "pc-keiba-viewer"
    / "tmp"
    / "candidate-prerace-weather-nar-banei-2026-08-24"
    / "nar_current_exact_race_rows.parquet"
)
DEFAULT_OUTPUT = APP_ROOT / "tmp" / "timesfm-3-evaluation-2024-2026.json"
LICENSE_FLAG = "--accept-non-commercial-license"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse an explicit research-only execution contract."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--checkpoint", default="google/timesfm-3.0-pytorch")
    parser.add_argument("--checkpoint-revision", default=DEFAULT_TIMESFM_REVISION)
    parser.add_argument("--context-length", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--scratch-lags", type=int, default=16)
    parser.add_argument("--bootstrap-repetitions", type=int, default=2_000)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--device")
    parser.add_argument(LICENSE_FLAG, action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run all six arms without connecting to production services."""
    args = parse_args(argv)
    if not args.accept_non_commercial_license:
        raise SystemExit(
            f"TimesFM 3.0 weights are non-commercial and non-production; pass {LICENSE_FLAG} "
            "to acknowledge this local research-only run."
        )
    config = ExperimentConfig(
        input_path=args.input,
        output_path=args.output,
        checkpoint=args.checkpoint,
        checkpoint_revision=args.checkpoint_revision,
        context_length=args.context_length,
        batch_size=args.batch_size,
        scratch_lags=args.scratch_lags,
        bootstrap_repetitions=args.bootstrap_repetitions,
        seed=args.seed,
        device=args.device,
    )
    report = run_experiment(config)
    write_report(report, config.output_path)
    print(config.output_path)


if __name__ == "__main__":
    main()
