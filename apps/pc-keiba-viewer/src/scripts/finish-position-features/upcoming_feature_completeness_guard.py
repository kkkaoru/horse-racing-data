"""Guard against serve-feature-completeness bugs: validate that pre-race-computable
features are non-null for UPCOMING race rows in a feature parquet/DataFrame.

Background
----------
Three bugs were found where computable-pre-race features were NULL/zero for
UPCOMING races because layers joined the stale ``race_entry_corner_features``
table (which has no upcoming rows) without a fallback:

  - odds/popularity (timing fix)
  - market-signal layer (commit 5c3aa12)
  - futan layer (commit ebd4636)

These features account for ~27 % of JRA feature importance and are invisible to
walk-forward evaluation (WF only sees historical rows where the features are
non-null).  This module provides ``assert_upcoming_feature_completeness`` so the
serve path can detect a recurrence at build time.

Invocation
----------
The guard is called from ``pipeline_runner.build_pipeline`` immediately after the
final parquet is produced (``current.rename(final_dir)`` step).  On violation it
logs a loud ERROR and raises ``UpcomingFeatureCompletenessError`` so the prediction
run hard-fails rather than silently serving zeroed features.

It can also be called standalone::

    python upcoming_feature_completeness_guard.py --parquet-dir /path/to/final

Feature allow-list
------------------
MUST_BE_PRESENT_FEATURES
    Features that must have a non-null rate >= NULL_RATE_THRESHOLD for UPCOMING
    rows (finish_position IS NULL).  Any column absent from the DataFrame triggers
    an error (missing column = entire feature was dropped / never written).

    Market-signal group (all from add-market-signal-features.py):
      inverse_odds_implied_prob, inverse_odds_market_share,
      odds_score_diff_from_race_avg, inverse_odds_rank_in_race,
      popularity_rank_in_race

    Odds / popularity base scores (from finish_position_features_duckdb.py):
      odds_score, popularity_score

    Futan group (from add-futan-juryo-features.py):
      futan_juryo, futan_juryo_rank_in_race, past_futan_juryo_avg5

    Race identity / base:
      shusso_tosu, umaban_norm

RANK_FEATURES_ALL_EQUAL_FORBIDDEN
    A subset of the must-present features that must NOT be all-equal within any
    single race.  The bogus all-1 pattern that occurred pre-fix (when
    tansho_odds_raw was NULL for all horses → every horse got rank 1 from NULLS
    LAST) is caught by this check.  Applied only to races with >= 2 runners.

EXPECTED_NULL_FOR_UPCOMING
    Columns that are legitimately NULL for unrun races (post-race signals).
    These are excluded from the must-present check so the guard never
    false-positives on corner positions, time_sa, kohan_3f, or finish_position.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Final, final, override

if TYPE_CHECKING:
    import pandas as pd

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

NULL_RATE_THRESHOLD: Final[float] = 0.90
"""Minimum non-null rate required for each must-present feature (0–1 scale)."""

MUST_BE_PRESENT_FEATURES: Final[tuple[str, ...]] = (
    # Market-signal group (add-market-signal-features.py)
    "inverse_odds_implied_prob",
    "inverse_odds_market_share",
    "odds_score_diff_from_race_avg",
    "inverse_odds_rank_in_race",
    "popularity_rank_in_race",
    # Odds / popularity base scores (finish_position_features_duckdb.py)
    "odds_score",
    "popularity_score",
    # Futan group (add-futan-juryo-features.py)
    "futan_juryo",
    "futan_juryo_rank_in_race",
    "past_futan_juryo_avg5",
    # Race identity / base (finish_position_features_duckdb.py)
    "shusso_tosu",
    "umaban_norm",
)

RANK_FEATURES_ALL_EQUAL_FORBIDDEN: Final[tuple[str, ...]] = (
    "inverse_odds_rank_in_race",
    "popularity_rank_in_race",
    "futan_juryo_rank_in_race",
)
"""Rank features that must not be all-equal within a race (bogus all-1 pattern)."""

EXPECTED_NULL_FOR_UPCOMING: Final[frozenset[str]] = frozenset(
    {
        # Post-race outcomes
        "finish_position",
        "finish_norm",
        # Corner positions (only known after the race is run)
        "corner1_norm",
        "corner3_norm",
        "corner4_norm",
        # Sectional / time signals
        "time_sa",
        "kohan_3f",
    }
)
"""Columns that are legitimately NULL for UPCOMING races; excluded from guard."""

MIN_RUNNERS_FOR_RANK_CHECK: Final[int] = 2
"""Only flag all-equal ranks for races with at least this many runners."""


# ---------------------------------------------------------------------------
# Violation types
# ---------------------------------------------------------------------------


class UpcomingFeatureCompletenessError(RuntimeError):
    """Raised when the upcoming-feature completeness guard detects a violation.

    On the serve path this causes the prediction run to hard-fail so zeroed
    features are never served to the model.  The test suite hard-asserts on this
    exception to prove that any regression that re-introduces a NULL-producing
    layer is caught immediately.
    """


@final
class ViolationDetail:
    """Single violation detail returned by ``check_upcoming_feature_completeness``."""

    __slots__: tuple[str, str, str] = ("feature", "violation_type", "detail")

    def __init__(self, feature: str, violation_type: str, detail: str) -> None:
        self.feature: str = feature
        self.violation_type: str = violation_type
        self.detail: str = detail

    @override
    def __repr__(self) -> str:
        return (
            f"ViolationDetail(feature={self.feature!r}, "
            f"violation_type={self.violation_type!r}, detail={self.detail!r})"
        )


# ---------------------------------------------------------------------------
# Core check (pure logic — no I/O, no pandas import at module level)
# ---------------------------------------------------------------------------


def check_upcoming_feature_completeness(
    df: pd.DataFrame,
    *,
    must_be_present: Sequence[str] = MUST_BE_PRESENT_FEATURES,
    rank_features: Sequence[str] = RANK_FEATURES_ALL_EQUAL_FORBIDDEN,
    null_rate_threshold: float = NULL_RATE_THRESHOLD,
    upcoming_col: str = "finish_position",
    race_id_cols: Sequence[str] = (
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
    ),
) -> list[ViolationDetail]:
    """Check feature completeness for UPCOMING rows (finish_position IS NULL).

    Parameters
    ----------
    df:
        A ``pandas.DataFrame`` produced by the serve-path feature pipeline.
        Must contain a column named by ``upcoming_col`` (default
        ``finish_position``); rows where that column IS NULL are considered
        UPCOMING.
    must_be_present:
        Feature names that must have a non-null rate >= ``null_rate_threshold``
        among UPCOMING rows.
    rank_features:
        Feature names that must not be all-equal within any single race among
        UPCOMING rows with >= 2 runners.
    null_rate_threshold:
        Minimum fraction of non-null values required (default 0.90 = 90 %).
    upcoming_col:
        Column used to identify UPCOMING rows (IS NULL → UPCOMING).
    race_id_cols:
        Columns that together identify a single race, used for the
        per-race rank-degeneration check.

    Returns
    -------
    list[ViolationDetail]
        Empty list means all checks passed.  Non-empty means at least one
        violation was found.  The caller decides whether to raise or log.

    Notes
    -----
    Missing columns (not in the DataFrame at all) are treated as a violation
    immediately — they indicate a layer that failed to write its output.
    """
    import pandas as _pd

    violations: list[ViolationDetail] = []

    # If the upcoming column is absent the guard cannot distinguish upcoming
    # vs historical rows; treat the entire frame as upcoming to be conservative.
    if upcoming_col in df.columns:
        upcoming_mask = df[upcoming_col].isna()
    else:
        upcoming_mask = _pd.Series([True] * len(df), index=df.index)

    upcoming = df[upcoming_mask]

    if len(upcoming) == 0:
        # No upcoming rows — nothing to check.
        return violations

    # --- Check 1: missing columns (entire feature absent) -------------------
    for feature in must_be_present:
        if feature not in df.columns:
            violations.append(
                ViolationDetail(
                    feature=feature,
                    violation_type="missing_column",
                    detail=(
                        f"column '{feature}' is absent from the feature DataFrame "
                        f"(expected in MUST_BE_PRESENT_FEATURES)"
                    ),
                )
            )

    # --- Check 2: null-rate threshold for present columns -------------------
    present_must = [f for f in must_be_present if f in df.columns]
    total_upcoming = len(upcoming)

    for feature in present_must:
        non_null_count = int(upcoming[feature].notna().sum())
        non_null_rate = non_null_count / total_upcoming
        if non_null_rate < null_rate_threshold:
            pct = non_null_rate * 100.0
            thr_pct = null_rate_threshold * 100.0
            violations.append(
                ViolationDetail(
                    feature=feature,
                    violation_type="null_rate_below_threshold",
                    detail=(
                        f"column '{feature}' has {pct:.1f}% non-null "
                        f"({non_null_count}/{total_upcoming} upcoming rows) "
                        f"— required >= {thr_pct:.0f}%"
                    ),
                )
            )

    # --- Check 3: rank degeneration (all-equal within race) -----------------
    race_id_present = [c for c in race_id_cols if c in df.columns]
    rank_present = [f for f in rank_features if f in df.columns]

    if race_id_present and rank_present and len(upcoming) > 0:
        for race_key, race_frame in upcoming.groupby(list(race_id_present)):
            if len(race_frame) < MIN_RUNNERS_FOR_RANK_CHECK:
                continue
            for feature in rank_present:
                col_series = race_frame[feature].dropna()
                if len(col_series) < MIN_RUNNERS_FOR_RANK_CHECK:
                    continue
                unique_vals = col_series.unique()
                if len(unique_vals) == 1:
                    violations.append(
                        ViolationDetail(
                            feature=feature,
                            violation_type="rank_all_equal",
                            detail=(
                                f"column '{feature}' is all-equal "
                                f"(value={unique_vals[0]!r}) for race "
                                f"{race_key!r} with {len(race_frame)} runners "
                                f"— indicates bogus all-same-rank bug"
                            ),
                        )
                    )

    return violations


def assert_upcoming_feature_completeness(
    df: pd.DataFrame,
    *,
    must_be_present: Sequence[str] = MUST_BE_PRESENT_FEATURES,
    rank_features: Sequence[str] = RANK_FEATURES_ALL_EQUAL_FORBIDDEN,
    null_rate_threshold: float = NULL_RATE_THRESHOLD,
    upcoming_col: str = "finish_position",
    race_id_cols: Sequence[str] = (
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
    ),
) -> None:
    """Assert feature completeness for UPCOMING rows; raise on violation.

    Convenience wrapper around ``check_upcoming_feature_completeness`` that
    raises ``UpcomingFeatureCompletenessError`` when any violation is found.
    Logs each violation as an ERROR before raising.

    Use this on the serve path where a violation must hard-fail the run.
    Use ``check_upcoming_feature_completeness`` directly in tests that want to
    assert on the list of violations.

    Parameters
    ----------
    df:
        A ``pandas.DataFrame`` produced by the serve-path feature pipeline.

    Raises
    ------
    UpcomingFeatureCompletenessError
        When at least one violation is found.  All violations are logged before
        raising so the full picture is visible in a single run.
    """
    violations = check_upcoming_feature_completeness(
        df,
        must_be_present=must_be_present,
        rank_features=rank_features,
        null_rate_threshold=null_rate_threshold,
        upcoming_col=upcoming_col,
        race_id_cols=race_id_cols,
    )
    if not violations:
        return
    for v in violations:
        print(
            f"[upcoming-feature-completeness] ERROR {v.violation_type}: {v.detail}",
            file=sys.stderr,
        )
    summary = (
        f"upcoming feature completeness check failed with {len(violations)} violation(s): "
        + "; ".join(v.detail for v in violations)
    )
    raise UpcomingFeatureCompletenessError(summary)


# ---------------------------------------------------------------------------
# Parquet helper (used by the standalone CLI and the serve-path hook)
# ---------------------------------------------------------------------------


def load_upcoming_parquet(parquet_dir: Path) -> pd.DataFrame:
    """Read a hive-partitioned parquet directory into a DataFrame.

    Parameters
    ----------
    parquet_dir:
        Directory produced by the feature pipeline (``race_year=YYYY/...``).

    Returns
    -------
    pandas.DataFrame
    """
    import pandas as _pd

    return _pd.read_parquet(parquet_dir)


def check_parquet_dir(
    parquet_dir: Path,
    *,
    must_be_present: Sequence[str] = MUST_BE_PRESENT_FEATURES,
    rank_features: Sequence[str] = RANK_FEATURES_ALL_EQUAL_FORBIDDEN,
    null_rate_threshold: float = NULL_RATE_THRESHOLD,
) -> list[ViolationDetail]:
    """Load a parquet directory and check upcoming feature completeness.

    Returns an empty list when all checks pass.
    """
    df = load_upcoming_parquet(parquet_dir)
    return check_upcoming_feature_completeness(
        df,
        must_be_present=must_be_present,
        rank_features=rank_features,
        null_rate_threshold=null_rate_threshold,
    )


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="upcoming_feature_completeness_guard",
        description=(
            "Validate that pre-race-computable features are non-null "
            "for UPCOMING rows in a feature parquet directory."
        ),
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        required=True,
        help="Hive-partitioned parquet directory to validate.",
    )
    parser.add_argument(
        "--null-rate-threshold",
        type=float,
        default=NULL_RATE_THRESHOLD,
        help=f"Minimum non-null rate [0-1] (default {NULL_RATE_THRESHOLD}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_cli_args(argv)
    violations = check_parquet_dir(
        args.parquet_dir,
        null_rate_threshold=args.null_rate_threshold,
    )
    if not violations:
        print(
            f"[upcoming-feature-completeness] OK: no violations found "
            f"in {args.parquet_dir}",
            file=sys.stderr,
        )
        return 0
    for v in violations:
        print(
            f"[upcoming-feature-completeness] ERROR {v.violation_type}: {v.detail}",
            file=sys.stderr,
        )
    print(
        f"[upcoming-feature-completeness] FAIL: {len(violations)} violation(s)",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
