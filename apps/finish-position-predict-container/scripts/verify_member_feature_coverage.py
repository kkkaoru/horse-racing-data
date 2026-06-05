#!/usr/bin/env python3
"""Offline diagnostic: prove the feature parquet covers every ensemble member.

The P0 per-class fix scores each ensemble member POSITIONALLY against the
member's OWN ``feature_names`` (read from its sibling ``metadata.json``) and the
inference ``LAYER_CHAIN`` for jra + nar now ends with
``add-relationship-r1-features.py`` (12 relationship columns). At scoring time
The runtime scorer 0-fills any missing entry keys (legacy behaviour preserved
after the WIP), so an offline coverage check is the only signal that a
training-time feature is silently dropped at inference. This script reproduces
that coverage check OFFLINE so a feature-layer drift is caught and reported
before the run silently 0-fills a relationship column.

Run with ``uv run`` so the app's virtualenv (duckdb, predict_lib on the src
path) is active::

    uv run python scripts/verify_member_feature_coverage.py \\
        --features-parquet 'apps/.../race_year=2026/*.parquet' \\
        --category jra \\
        --models-dir apps/finish-position-predict-container/models

For each registered per-class ensemble of ``--category`` every manifest member
is checked: ``missing = set(member.feature_names) - {score_col} - parquet_cols``
where ``score_col`` is the synthetic baseline-score feature
(:data:`predict_lib.ensemble_routing.SCORE_FEATURE_BY_CATEGORY`) that the
two-pass scorer injects at runtime rather than reading from the parquet. One
``PASS`` / ``FAIL`` line is printed per member; the process exits ``1`` when any
member FAILs (informative — a gap means the parquet lacks columns the member
needs), else ``0``.

This file is an offline diagnostic only: ``pyproject.toml`` scopes pytest to
``testpaths = ["tests"]`` and coverage to ``--cov=predict_lib``, so ``scripts/``
is outside both the test-collection and the coverage surface.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

# ``predict_lib`` lives under the app ``src`` dir, which is on ``pythonpath``
# only for pytest (pyproject ``pythonpath = ["src"]``), NOT for a bare
# ``uv run python scripts/...``. When the package does not already resolve, the
# sibling ``src`` dir is prepended so this standalone diagnostic can import it.
# The branch is a no-op when ``predict_lib`` is already importable.
if importlib.util.find_spec("predict_lib") is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import duckdb

from predict_lib.booster_pool import load_member_feature_names
from predict_lib.ensemble_routing import SCORE_FEATURE_BY_CATEGORY
from predict_lib.model_meta import (
    METADATA_FILE_NAME,
    R2_KEY_PREFIX,
    Category,
    model_version_for,
    resolve_category,
)
from predict_lib.per_class import (
    PER_CLASS_MODEL_VERSIONS,
    PER_CLASS_SUBDIR,
    EnsembleMember,
    load_ensemble_manifest,
)

DESCRIBE_PARQUET_SQL: str = "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0"
MAX_MISSING_PRINTED: int = 15
SUPPORTED_CATEGORIES: tuple[Category, ...] = ("jra", "nar")


@dataclass(frozen=True)
class MemberCoverage:
    """Coverage verdict for one ensemble member against the parquet columns.

    ``missing`` is the sorted set of feature names the member needs but the
    parquet lacks (already excluding the injected ``score_col``). ``passed`` is
    ``True`` exactly when ``missing`` is empty.
    """

    class_code: str
    ensemble_model_version: str
    member_model_version: str
    missing: tuple[str, ...]

    @property
    def passed(self) -> bool:
        """Return ``True`` when no required feature is absent from the parquet."""
        return not self.missing


def read_parquet_columns(features_parquet: str) -> frozenset[str]:
    """Return the column names of ``features_parquet`` (path or glob) via duckdb.

    Uses ``DESCRIBE SELECT * ... LIMIT 0`` so only the schema is read — no rows
    are scanned. The glob is passed as a bound parameter so a path containing
    shell-special characters cannot break the query.
    """
    connection = duckdb.connect()
    try:
        described = connection.execute(DESCRIBE_PARQUET_SQL, [features_parquet]).fetchall()
    finally:
        connection.close()
    return frozenset(str(row[0]) for row in described)


def _member_metadata_path(
    models_dir: Path,
    category: Category,
    class_code: str,
    member: EnsembleMember,
) -> Path:
    """Return the sibling ``metadata.json`` path for one ensemble member.

    Mirrors the runtime resolution in
    :func:`predict_lib.ensemble_routing.init_member_pool`: the category-global
    baseline member is read from ``<root>/<category>/<baseline_mv>/`` (the same
    path the single-model loader uses), while every per-class residual member is
    read from ``<root>/<category>/per-class/<class_code>/<member_mv>/``.
    """
    models_root = models_dir / R2_KEY_PREFIX
    if member.model_version == model_version_for(category):
        return models_root / category / member.model_version / METADATA_FILE_NAME
    return (
        models_root
        / category
        / PER_CLASS_SUBDIR
        / class_code
        / member.model_version
        / METADATA_FILE_NAME
    )


def evaluate_member(
    models_dir: Path,
    category: Category,
    class_code: str,
    ensemble_model_version: str,
    member: EnsembleMember,
    parquet_columns: frozenset[str],
) -> MemberCoverage:
    """Compute the missing-feature set for one member against the parquet.

    ``missing = set(member.feature_names) - {score_col} - parquet_columns`` where
    ``score_col`` is the synthetic baseline-score feature injected at runtime
    (excluded because the scorer supplies it after scoring the baseline, not the
    feature layer). The member's ordered ``feature_names`` is read from its
    sibling ``metadata.json`` through the same loader the container uses.
    """
    metadata_path = _member_metadata_path(models_dir, category, class_code, member)
    feature_names = load_member_feature_names(metadata_path)
    score_col = SCORE_FEATURE_BY_CATEGORY.get(category)
    required = set(feature_names)
    if score_col is not None:
        required.discard(score_col)
    missing = tuple(sorted(required - parquet_columns))
    return MemberCoverage(
        class_code=class_code,
        ensemble_model_version=ensemble_model_version,
        member_model_version=member.model_version,
        missing=missing,
    )


def _class_codes_for_category(category: Category) -> tuple[str, ...]:
    """Return the registered per-class codes for ``category`` in sorted order."""
    codes = {code for cat, code in PER_CLASS_MODEL_VERSIONS if cat == category}
    return tuple(sorted(codes))


def collect_coverage(
    models_dir: Path,
    category: Category,
    parquet_columns: frozenset[str],
) -> list[MemberCoverage]:
    """Evaluate every member of every registered ensemble for ``category``.

    Walks :data:`predict_lib.per_class.PER_CLASS_MODEL_VERSIONS` for the
    category, parses each ensemble's ``manifest.json`` through
    :func:`predict_lib.per_class.load_ensemble_manifest`, and evaluates every
    member's parquet coverage. A class whose manifest does not parse is skipped
    (it would fall back to the single-model path at runtime, which needs no
    member coverage).
    """
    results: list[MemberCoverage] = []
    for class_code in _class_codes_for_category(category):
        ensemble = load_ensemble_manifest(models_dir, category, class_code)
        if ensemble is None:
            continue
        results.extend(
            evaluate_member(
                models_dir,
                category,
                class_code,
                ensemble.model_version,
                member,
                parquet_columns,
            )
            for member in ensemble.members
        )
    return results


def format_coverage_line(coverage: MemberCoverage) -> str:
    """Render one member's verdict as a single grep-friendly line.

    Shows ``PASS`` / ``FAIL``, the ensemble + member ``model_version``, the
    missing-column count, and the first :data:`MAX_MISSING_PRINTED` missing
    names (a trailing ``(+N more)`` when the set is larger).
    """
    verdict = "PASS" if coverage.passed else "FAIL"
    head = (
        f"{verdict} class={coverage.class_code} "
        f"ensemble={coverage.ensemble_model_version} "
        f"member={coverage.member_model_version} "
        f"n_missing={len(coverage.missing)}"
    )
    if coverage.passed:
        return head
    shown = coverage.missing[:MAX_MISSING_PRINTED]
    overflow = len(coverage.missing) - len(shown)
    suffix = f" (+{overflow} more)" if overflow > 0 else ""
    return f"{head} missing={list(shown)}{suffix}"


def run_features_parquet_mode(
    features_parquet: str,
    category: Category,
    models_dir: Path,
) -> int:
    """Read the parquet columns, evaluate every member, print + return exit code.

    Prints one line per member plus a summary footer, and returns ``1`` when any
    member FAILs so the script is usable as a CI / pre-deploy gate.
    """
    parquet_columns = read_parquet_columns(features_parquet)
    coverages = collect_coverage(models_dir, category, parquet_columns)
    for coverage in coverages:
        print(format_coverage_line(coverage))
    failures = [coverage for coverage in coverages if not coverage.passed]
    print(
        f"summary category={category} parquet_columns={len(parquet_columns)} "
        f"members_checked={len(coverages)} failures={len(failures)}"
    )
    return 1 if failures else 0


def _resolve_supported_category(raw: str) -> Category:
    """Narrow ``raw`` to a supported ``Category`` (jra / nar) or raise.

    ``argparse`` already restricts the choices, but the explicit narrowing keeps
    the value typed as :data:`predict_lib.model_meta.Category` for the callees
    and rejects ban-ei (no per-class registry) defensively.
    """
    category = resolve_category(raw)
    if category not in SUPPORTED_CATEGORIES:
        message = f"unsupported category for per-class coverage: {category}"
        raise ValueError(message)
    return category


def build_arg_parser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser for the diagnostic."""
    parser = argparse.ArgumentParser(
        description=(
            "Verify the feature parquet covers every per-class ensemble member's "
            "required feature columns (offline diagnostic for the P0 per-class fix)."
        )
    )
    parser.add_argument(
        "--features-parquet",
        required=True,
        help="Path or glob to the feature parquet (read schema-only via duckdb).",
    )
    parser.add_argument(
        "--category",
        required=True,
        choices=list(SUPPORTED_CATEGORIES),
        help="Per-class category to verify (jra or nar).",
    )
    parser.add_argument(
        "--models-dir",
        required=True,
        type=Path,
        help="Models root containing finish-position/<category>/... metadata.json.",
    )
    return parser


def run(argv: Sequence[str]) -> int:
    """Parse ``argv`` and run the features-parquet coverage check."""
    args = build_arg_parser().parse_args(argv)
    category = _resolve_supported_category(args.category)
    return run_features_parquet_mode(args.features_parquet, category, args.models_dir)


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
