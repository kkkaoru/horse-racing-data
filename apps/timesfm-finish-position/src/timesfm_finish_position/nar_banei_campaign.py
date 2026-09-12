"""Cache official TimesFM forecasts for the local-PG exact-cell campaign."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import polars as pl

from .forecasting import TimesFm3Forecaster, resolve_timesfm_device
from .nar_banei_temporal import build_queries, forecast_targets, observation_index

PROFILES: dict[str, tuple[tuple[str, ...], int | None, bool]] = {
    "performance-full": (("performance",), None, False),
    "performance-speed-full": (("performance", "relative_speed"), None, False),
    "performance-32": (("performance",), 32, False),
    "performance-frozen": (("performance",), None, True),
    "day-speed-full": (("day_speed",), None, False),
    "performance-day-speed-full": (("performance", "day_speed"), None, False),
    "body-full": (("body_weight",), None, False),
    "performance-body-full": (("performance", "body_weight"), None, False),
}
DEFAULT_PROFILES = (
    "performance-full",
    "performance-speed-full",
    "performance-32",
    "performance-frozen",
    "day-speed-full",
    "performance-day-speed-full",
)


def file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--profiles", nargs="+", choices=tuple(PROFILES), default=list(DEFAULT_PROFILES)
    )
    parser.add_argument("--years", nargs="+", type=int)
    parser.add_argument("--cells", nargs="+")
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--device")
    parser.add_argument("--accept-non-commercial-license", action="store_true")
    args = parser.parse_args(argv)
    if not args.accept_non_commercial_license:
        parser.error("Explicit acceptance of the non-commercial research license is required")
    targets = pl.read_parquet(args.input / "targets.parquet")
    if args.years:
        targets = targets.filter(pl.col("year").is_in(args.years))
    if args.cells:
        targets = targets.filter(pl.col("cell_id").is_in(args.cells))
    if targets.is_empty():
        raise ValueError("No matching evaluation entrants")
    index = observation_index(pl.read_parquet(args.input / "observations.parquet"), targets)
    source_hashes = {
        name: file_digest(args.input / name) for name in ("targets.parquet", "observations.parquet")
    }
    forecaster = TimesFm3Forecaster(
        "google/timesfm-3.0-pytorch", args.batch_size, resolve_timesfm_device(args.device)
    )
    for profile in args.profiles:
        columns, cap, frozen = PROFILES[profile]
        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "source": source_hashes,
                    "profile": profile,
                    "settings": PROFILES[profile],
                    "query_code": file_digest(Path(__file__).with_name("nar_banei_temporal.py")),
                    "backend": forecaster.backend,
                    "revision": forecaster.checkpoint_revision,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        for key, cohort in targets.partition_by(["cell_id", "year"], as_dict=True).items():
            cell_id, year = key
            output = args.output / profile / str(cell_id) / str(year)
            report_path = output / "report.json"
            if report_path.exists() and (output / "forecasts.parquet").exists():
                if (
                    json.loads(report_path.read_text(encoding="utf-8"))["fingerprint"]
                    != fingerprint
                ):
                    raise ValueError(
                        "Cached forecast provenance changed; choose a new output directory"
                    )
                continue
            started = time.perf_counter()
            queries = build_queries(
                index, cohort, columns=columns, max_history=cap, frozen_year=frozen
            )
            result = forecast_targets(cohort, queries, forecaster, columns=columns)
            if result.filter(
                (pl.col("history_count") > 0)
                & (pl.col("latest_history_date") >= pl.col("race_date"))
            ).height:
                raise ValueError("A forecast consumed same-day or future observations")
            output.mkdir(parents=True, exist_ok=True)
            result.write_parquet(output / "forecasts.parquet")
            report = {
                "cell_id": cell_id,
                "year": year,
                "profile": profile,
                "rows": result.height,
                "queries": len(queries),
                "history_available_rows": result.filter(pl.col("history_count") > 0).height,
                "elapsed_seconds": time.perf_counter() - started,
                "backend": forecaster.backend,
                "fingerprint": fingerprint,
                "source_hashes": source_hashes,
                "checkpoint": forecaster.checkpoint,
                "checkpoint_revision": forecaster.checkpoint_revision,
                "research_only": True,
                "noncommercial_personal_validation": True,
                "production_integration": False,
                "promotion_eligible": False,
            }
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
