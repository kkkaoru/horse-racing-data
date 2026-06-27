"""Verify feature-column parity across the three sources that drive scoring.

The finish-position models project every input row onto the *exact* ordered
``feature_names`` list stored in ``metadata.json`` before scoring. When the
feature-store parquet (the builder's real output) is missing a column that the
model expects, the projection silently fills it with ``0.0`` — the model does
not crash, it just scores on a dead signal. That is more dangerous than a hard
failure, so this tool exists as a pre-deploy gate.

It cross-checks three sources and reports mismatches:

1. Feature-store parquet column set    (``--features-parquet`` dir or file)
2. Model ``metadata.json`` feature_names (``--metadata``)
3. (optional) ``model_meta.json`` feature_counts (``--model-meta`` + ``--category``)

Usage:
    uv run python verify_feature_parity.py \\
        --features-parquet ../apps/pc-keiba-viewer/tmp/feat-jra-fresh-base-market \\
        --metadata ../apps/.../iter20-jra-cb-2013-v8/metadata.json

    # pre-deploy gate: WARNING mismatches also fail the build
    uv run python verify_feature_parity.py ... --strict

Exit codes:
    0  every source agrees (or only harmless extra parquet columns / INFO)
    1  FATAL: feature_names columns missing from parquet (would score 0.0),
       OR (with --strict) any WARNING-level count mismatch
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

_PARQUET_GLOB = "race_year=*/*.parquet"


def find_first_parquet(features_path: Path) -> Path | None:
    """Return one representative parquet file for *features_path*.

    A directory is globbed for ``race_year=*/*.parquet`` (year-sharded feature
    store); the first match wins because every shard shares one schema. A path
    that is itself a ``.parquet`` file is returned as-is. ``None`` means nothing
    usable was found.
    """
    if features_path.is_file():
        return features_path
    if features_path.is_dir():
        matches = sorted(features_path.glob(_PARQUET_GLOB))
        return matches[0] if matches else None
    return None


def read_parquet_columns(parquet_file: Path) -> set[str]:
    """Read only the parquet schema (no row groups) and return its column set."""
    schema = pq.ParquetFile(parquet_file).schema_arrow
    return set(schema.names)


def load_metadata(metadata_path: Path) -> tuple[list[str], int]:
    """Return ``(feature_names, feature_count)`` from a model ``metadata.json``."""
    raw: dict[str, object] = json.loads(metadata_path.read_text(encoding="utf-8"))
    names_raw = raw.get("feature_names")
    feature_names = [str(name) for name in names_raw] if isinstance(names_raw, list) else []
    count_raw = raw.get("feature_count")
    feature_count = int(count_raw) if isinstance(count_raw, int) else len(feature_names)
    return feature_names, feature_count


def load_model_meta_count(model_meta_path: Path, category: str | None) -> int | None:
    """Return ``feature_counts[category]`` from ``model_meta.json`` or ``None``.

    ``None`` is returned when no category is supplied or the category is absent,
    so the caller can skip that comparison rather than fail on it.
    """
    if category is None:
        return None
    raw: dict[str, object] = json.loads(model_meta_path.read_text(encoding="utf-8"))
    counts = raw.get("feature_counts")
    if not isinstance(counts, dict):
        return None
    value = counts.get(category)
    return int(value) if isinstance(value, int) else None


@dataclass(frozen=True)
class ParityReport:
    """Outcome of comparing feature_names against the parquet schema and counts."""

    missing_in_parquet: list[str]
    extra_in_parquet: list[str]
    count_mismatch: bool
    model_meta_mismatch: bool

    @property
    def is_fatal(self) -> bool:
        return len(self.missing_in_parquet) > 0

    @property
    def has_warning(self) -> bool:
        return self.count_mismatch or self.model_meta_mismatch


def compare(
    feature_names: list[str],
    feature_count: int,
    parquet_columns: set[str],
    model_meta_count: int | None,
) -> ParityReport:
    """Pure comparison: build a :class:`ParityReport` from the three sources."""
    expected = set(feature_names)
    missing_in_parquet = sorted(name for name in expected if name not in parquet_columns)
    extra_in_parquet = sorted(col for col in parquet_columns if col not in expected)
    count_mismatch = feature_count != len(feature_names)
    model_meta_mismatch = (
        model_meta_count is not None and model_meta_count != len(feature_names)
    )
    return ParityReport(
        missing_in_parquet=missing_in_parquet,
        extra_in_parquet=extra_in_parquet,
        count_mismatch=count_mismatch,
        model_meta_mismatch=model_meta_mismatch,
    )


def _log_fatal(report: ParityReport) -> None:
    logger.error(
        "FATAL: %d feature_names column(s) missing from parquet schema "
        "(these score 0.0 in production): %s",
        len(report.missing_in_parquet),
        ", ".join(report.missing_in_parquet),
    )


def _log_warnings(
    report: ParityReport,
    feature_count: int,
    feature_names: list[str],
    model_meta_count: int | None,
) -> None:
    if report.count_mismatch:
        logger.warning(
            "metadata feature_count (%d) != len(feature_names) (%d)",
            feature_count,
            len(feature_names),
        )
    if report.model_meta_mismatch:
        logger.warning(
            "model_meta feature_counts (%s) != len(feature_names) (%d)",
            model_meta_count,
            len(feature_names),
        )


def _log_info(report: ParityReport) -> None:
    if report.extra_in_parquet:
        logger.info(
            "%d parquet column(s) not in feature_names (unused at train, harmless)",
            len(report.extra_in_parquet),
        )


def decide_exit_code(report: ParityReport, *, strict: bool) -> int:
    """Map a report + ``--strict`` flag to a process exit code (0 or 1)."""
    if report.is_fatal:
        return 1
    if strict and report.has_warning:
        return 1
    return 0


def report_and_exit_code(
    report: ParityReport,
    feature_names: list[str],
    feature_count: int,
    model_meta_count: int | None,
    *,
    strict: bool,
) -> int:
    """Emit log lines for *report* and return the resulting exit code."""
    if report.is_fatal:
        _log_fatal(report)
    _log_warnings(report, feature_count, feature_names, model_meta_count)
    _log_info(report)
    exit_code = decide_exit_code(report, strict=strict)
    if exit_code == 0 and not report.has_warning:
        logger.info(
            "OK: all %d feature_names present in parquet; counts consistent",
            len(feature_names),
        )
    return exit_code


def run_verification(
    features_parquet: Path,
    metadata_path: Path,
    model_meta_path: Path | None,
    category: str | None,
    *,
    strict: bool,
) -> int:
    """Load every source, compare, log, and return the exit code.

    Returns ``1`` early when no parquet file can be located, since that means
    the feature store does not exist for this run and parity is unprovable.
    """
    parquet_file = find_first_parquet(features_parquet)
    if parquet_file is None:
        logger.error("No parquet file found under %s", features_parquet)
        return 1
    logger.info("Using parquet schema from %s", parquet_file)
    parquet_columns = read_parquet_columns(parquet_file)
    feature_names, feature_count = load_metadata(metadata_path)
    model_meta_count = (
        load_model_meta_count(model_meta_path, category)
        if model_meta_path is not None
        else None
    )
    report = compare(feature_names, feature_count, parquet_columns, model_meta_count)
    return report_and_exit_code(
        report,
        feature_names,
        feature_count,
        model_meta_count,
        strict=strict,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify feature-column parity across feature store, model "
        "metadata, and model_meta.",
    )
    parser.add_argument(
        "--features-parquet",
        required=True,
        metavar="PATH",
        help="Feature-store parquet directory (race_year=*/*.parquet) or a file",
    )
    parser.add_argument(
        "--metadata",
        required=True,
        metavar="PATH",
        help="Model metadata.json with feature_names / feature_count",
    )
    parser.add_argument(
        "--model-meta",
        metavar="PATH",
        help="Optional model_meta.json with feature_counts[category]",
    )
    parser.add_argument(
        "--category",
        metavar="NAME",
        help="Category key (jra/nar/ban-ei) for model_meta feature_counts lookup",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat WARNING-level count mismatches as failures (deploy gate)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _parse_args(argv)
    model_meta_path = Path(args.model_meta) if args.model_meta else None
    return run_verification(
        Path(args.features_parquet),
        Path(args.metadata),
        model_meta_path,
        args.category,
        strict=args.strict,
    )


if __name__ == "__main__":
    sys.exit(main())
