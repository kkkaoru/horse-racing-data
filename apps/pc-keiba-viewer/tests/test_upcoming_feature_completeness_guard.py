"""Tests for upcoming_feature_completeness_guard.

This test file is the PRIMARY PROTECTION against serve-feature-completeness bugs.
It constructs synthetic upcoming-race DataFrames with deliberately-NULL
market-signal and futan columns and asserts that the guard catches them — exactly
what would have caught the three historical bugs (odds/popularity, market-signal
commit 5c3aa12, futan commit ebd4636).

Minimal dependency surface: only pandas + the guard module.  No DuckDB, no PG,
no file I/O (except the parquet round-trip tests).
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from upcoming_feature_completeness_guard import (
    MUST_BE_PRESENT_FEATURES,
    NULL_RATE_THRESHOLD,
    RANK_FEATURES_ALL_EQUAL_FORBIDDEN,
    UpcomingFeatureCompletenessError,
    ViolationDetail,
    assert_upcoming_feature_completeness,
    check_parquet_dir,
    check_upcoming_feature_completeness,
    main,
    parse_cli_args,
)


# ---------------------------------------------------------------------------
# Helpers: synthetic DataFrames
# ---------------------------------------------------------------------------


def _base_upcoming_race(
    *,
    num_horses: int = 3,
    race_date: str = "20260607",
    race_year: int = 2026,
    include_finish_position: bool = True,
) -> pd.DataFrame:
    """Build a synthetic upcoming-race DataFrame (finish_position NULL).

    All MUST_BE_PRESENT_FEATURES are filled with sane non-null values; the
    caller can override columns after calling this helper to test NULL scenarios.
    Works for any num_horses >= 1.
    """
    n = num_horses
    # Generate spread scores: descending from ~0.9 to ~0.1
    odds_scores = [round(0.9 - 0.8 * i / max(n - 1, 1), 4) for i in range(n)]
    pop_scores = [round(0.8 - 0.7 * i / max(n - 1, 1), 4) for i in range(n)]
    inv_prob = [round(0.20 - 0.15 * i / max(n - 1, 1), 4) for i in range(n)]
    inv_share = [round(1.0 / n, 4)] * n  # equal share as default
    odds_diff = [round((n / 2 - i) * 0.1, 4) for i in range(n)]
    futan = [round(54.0 + 2.0 * i, 1) for i in range(n)]
    past_futan = [round(54.0 + 1.0 * i, 1) for i in range(n)]
    row_data = {
        "source": ["jra"] * n,
        "kaisai_nen": ["2026"] * n,
        "kaisai_tsukihi": ["0607"] * n,
        "keibajo_code": ["05"] * n,
        "race_bango": ["11"] * n,
        "ketto_toroku_bango": [f"horse_{i}" for i in range(n)],
        "race_date": [race_date] * n,
        "race_year": [race_year] * n,
        "umaban": list(range(1, n + 1)),
        # Race identity / base
        "shusso_tosu": [n] * n,
        "umaban_norm": [
            round((i - 1) / max(n - 1, 1), 4) for i in range(1, n + 1)
        ],
        # Odds / popularity base scores
        "odds_score": odds_scores,
        "popularity_score": pop_scores,
        # Market-signal group
        "inverse_odds_implied_prob": inv_prob,
        "inverse_odds_market_share": inv_share,
        "odds_score_diff_from_race_avg": odds_diff,
        "inverse_odds_rank_in_race": list(range(1, n + 1)),
        "popularity_rank_in_race": list(range(1, n + 1)),
        # Futan group
        "futan_juryo": futan,
        "futan_juryo_rank_in_race": list(range(n, 0, -1)),
        "past_futan_juryo_avg5": past_futan,
        # Legitimately-NULL post-race columns
        "finish_norm": [None] * n,
        "time_sa": [None] * n,
        "kohan_3f": [None] * n,
        "corner1_norm": [None] * n,
        "corner3_norm": [None] * n,
        "corner4_norm": [None] * n,
    }
    if include_finish_position:
        row_data["finish_position"] = [None] * n
    return pd.DataFrame(row_data)


def _clean_upcoming_df(num_horses: int = 3) -> pd.DataFrame:
    """A fully-populated upcoming-race DataFrame that should pass all checks."""
    return _base_upcoming_race(num_horses=num_horses)


def _historical_df(num_horses: int = 2) -> pd.DataFrame:
    """A historical DataFrame (finish_position NOT NULL) — guard should pass."""
    df = _base_upcoming_race(num_horses=num_horses)
    df["finish_position"] = list(range(1, num_horses + 1))
    return df


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------


def test_must_be_present_features_is_non_empty() -> None:
    assert len(MUST_BE_PRESENT_FEATURES) > 0


def test_rank_features_all_equal_forbidden_is_subset_of_must_be_present() -> None:
    for f in RANK_FEATURES_ALL_EQUAL_FORBIDDEN:
        assert f in MUST_BE_PRESENT_FEATURES, (
            f"{f!r} is in RANK_FEATURES_ALL_EQUAL_FORBIDDEN but not in MUST_BE_PRESENT_FEATURES"
        )


def test_null_rate_threshold_is_between_zero_and_one() -> None:
    assert 0.0 < NULL_RATE_THRESHOLD <= 1.0


def test_market_signal_features_in_must_be_present() -> None:
    for feat in (
        "inverse_odds_implied_prob",
        "inverse_odds_market_share",
        "odds_score_diff_from_race_avg",
        "inverse_odds_rank_in_race",
        "popularity_rank_in_race",
    ):
        assert feat in MUST_BE_PRESENT_FEATURES


def test_futan_features_in_must_be_present() -> None:
    for feat in ("futan_juryo", "futan_juryo_rank_in_race", "past_futan_juryo_avg5"):
        assert feat in MUST_BE_PRESENT_FEATURES


def test_base_features_in_must_be_present() -> None:
    for feat in ("shusso_tosu", "umaban_norm", "odds_score", "popularity_score"):
        assert feat in MUST_BE_PRESENT_FEATURES


# ---------------------------------------------------------------------------
# ViolationDetail
# ---------------------------------------------------------------------------


def test_violation_detail_repr_contains_fields() -> None:
    v = ViolationDetail("my_col", "null_rate_below_threshold", "some detail text")
    r = repr(v)
    assert "my_col" in r
    assert "null_rate_below_threshold" in r
    assert "some detail text" in r


# ---------------------------------------------------------------------------
# SYNTHETIC REGRESSION TEST: NULL market-signal columns caught
#
# This test would have caught commit 5c3aa12 where add-market-signal-features.py
# joined only race_entry_corner_features (no upcoming rows) and produced NULL
# inverse_odds_implied_prob / inverse_odds_market_share for all upcoming horses.
# ---------------------------------------------------------------------------


def test_null_market_signal_inverse_odds_implied_prob_is_caught() -> None:
    """Guard must catch all-NULL inverse_odds_implied_prob for upcoming rows."""
    df = _base_upcoming_race()
    df["inverse_odds_implied_prob"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = [v.feature for v in violations]
    assert "inverse_odds_implied_prob" in features_flagged


def test_null_market_signal_inverse_odds_market_share_is_caught() -> None:
    """Guard must catch all-NULL inverse_odds_market_share for upcoming rows."""
    df = _base_upcoming_race()
    df["inverse_odds_market_share"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = [v.feature for v in violations]
    assert "inverse_odds_market_share" in features_flagged


def test_null_market_signal_odds_score_diff_is_caught() -> None:
    """Guard must catch all-NULL odds_score_diff_from_race_avg for upcoming rows."""
    df = _base_upcoming_race()
    df["odds_score_diff_from_race_avg"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = [v.feature for v in violations]
    assert "odds_score_diff_from_race_avg" in features_flagged


# ---------------------------------------------------------------------------
# SYNTHETIC REGRESSION TEST: NULL futan columns caught
#
# This test would have caught commit ebd4636 where add-futan-juryo-features.py
# joined only race_entry_corner_features (no upcoming rows) and produced NULL
# futan_juryo / futan_juryo_rank_in_race / past_futan_juryo_avg5 for all upcoming
# horses.
# ---------------------------------------------------------------------------


def test_null_futan_juryo_is_caught() -> None:
    """Guard must catch all-NULL futan_juryo for upcoming rows."""
    df = _base_upcoming_race()
    df["futan_juryo"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = [v.feature for v in violations]
    assert "futan_juryo" in features_flagged


def test_null_futan_juryo_rank_in_race_is_caught() -> None:
    """Guard must catch all-NULL futan_juryo_rank_in_race for upcoming rows."""
    df = _base_upcoming_race()
    df["futan_juryo_rank_in_race"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = [v.feature for v in violations]
    assert "futan_juryo_rank_in_race" in features_flagged


def test_null_past_futan_juryo_avg5_is_caught() -> None:
    """Guard must catch all-NULL past_futan_juryo_avg5 for upcoming rows."""
    df = _base_upcoming_race()
    df["past_futan_juryo_avg5"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = [v.feature for v in violations]
    assert "past_futan_juryo_avg5" in features_flagged


# ---------------------------------------------------------------------------
# SYNTHETIC REGRESSION TEST: both market-signal AND futan simultaneously NULL
# (worst-case: both layers broke at once)
# ---------------------------------------------------------------------------


def test_null_market_signal_and_futan_both_caught() -> None:
    """Guard catches violations from multiple broken layers in a single call."""
    df = _base_upcoming_race()
    df["inverse_odds_implied_prob"] = None
    df["inverse_odds_market_share"] = None
    df["futan_juryo"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = {v.feature for v in violations}
    assert "inverse_odds_implied_prob" in features_flagged
    assert "inverse_odds_market_share" in features_flagged
    assert "futan_juryo" in features_flagged


# ---------------------------------------------------------------------------
# Missing column (entire layer dropped from DataFrame)
# ---------------------------------------------------------------------------


def test_missing_column_is_caught() -> None:
    """A column not present at all in the DataFrame triggers a missing_column violation."""
    df = _base_upcoming_race().drop(columns=["inverse_odds_implied_prob"])
    violations = check_upcoming_feature_completeness(df)
    missing = [v for v in violations if v.violation_type == "missing_column"]
    features_flagged = {v.feature for v in missing}
    assert "inverse_odds_implied_prob" in features_flagged


def test_missing_futan_column_is_caught() -> None:
    """A dropped futan_juryo column triggers a missing_column violation."""
    df = _base_upcoming_race().drop(columns=["futan_juryo"])
    violations = check_upcoming_feature_completeness(df)
    missing = [v for v in violations if v.violation_type == "missing_column"]
    assert any(v.feature == "futan_juryo" for v in missing)


# ---------------------------------------------------------------------------
# Rank all-equal (bogus all-1 pattern)
# ---------------------------------------------------------------------------


def test_rank_all_equal_inverse_odds_rank_is_caught() -> None:
    """Guard must catch inverse_odds_rank_in_race all-equal=1 (bogus all-1 bug)."""
    df = _base_upcoming_race(num_horses=3)
    df["inverse_odds_rank_in_race"] = 1
    violations = check_upcoming_feature_completeness(df)
    rank_violations = [v for v in violations if v.violation_type == "rank_all_equal"]
    features_flagged = {v.feature for v in rank_violations}
    assert "inverse_odds_rank_in_race" in features_flagged


def test_rank_all_equal_popularity_rank_is_caught() -> None:
    """Guard must catch popularity_rank_in_race all-equal=1."""
    df = _base_upcoming_race(num_horses=3)
    df["popularity_rank_in_race"] = 1
    violations = check_upcoming_feature_completeness(df)
    rank_violations = [v for v in violations if v.violation_type == "rank_all_equal"]
    features_flagged = {v.feature for v in rank_violations}
    assert "popularity_rank_in_race" in features_flagged


def test_rank_all_equal_futan_rank_is_caught() -> None:
    """Guard must catch futan_juryo_rank_in_race all-equal (same weight all horses)."""
    df = _base_upcoming_race(num_horses=3)
    df["futan_juryo_rank_in_race"] = 1
    violations = check_upcoming_feature_completeness(df)
    rank_violations = [v for v in violations if v.violation_type == "rank_all_equal"]
    features_flagged = {v.feature for v in rank_violations}
    assert "futan_juryo_rank_in_race" in features_flagged


def test_rank_all_equal_not_flagged_for_single_runner_race() -> None:
    """A single-runner race with all ranks=1 is not a violation (only 1 horse)."""
    df = _base_upcoming_race(num_horses=1)
    violations = check_upcoming_feature_completeness(df)
    rank_violations = [v for v in violations if v.violation_type == "rank_all_equal"]
    assert len(rank_violations) == 0


def test_distinct_ranks_not_flagged() -> None:
    """Distinct ranks [1, 2, 3] must not be flagged as all-equal."""
    df = _clean_upcoming_df(num_horses=3)
    violations = check_upcoming_feature_completeness(df)
    rank_violations = [v for v in violations if v.violation_type == "rank_all_equal"]
    assert len(rank_violations) == 0


# ---------------------------------------------------------------------------
# Clean frame passes all checks
# ---------------------------------------------------------------------------


def test_clean_upcoming_frame_passes() -> None:
    """A properly-populated upcoming-race frame must produce zero violations."""
    df = _clean_upcoming_df()
    violations = check_upcoming_feature_completeness(df)
    assert violations == []


def test_clean_multi_race_frame_passes() -> None:
    """Two races worth of upcoming rows with valid features — no violations."""
    df1 = _base_upcoming_race(num_horses=3)
    df2 = _base_upcoming_race(num_horses=2, race_date="20260608", race_year=2026)
    df2["kaisai_tsukihi"] = "0608"
    df2["race_bango"] = "12"
    df2["ketto_toroku_bango"] = ["horse_x", "horse_y"]
    df2["umaban"] = [1, 2]
    df2["umaban_norm"] = [0.0, 1.0]
    df2["odds_score"] = [0.5, 0.3]
    df2["popularity_score"] = [0.4, 0.2]
    df2["inverse_odds_implied_prob"] = [0.2, 0.1]
    df2["inverse_odds_market_share"] = [0.67, 0.33]
    df2["odds_score_diff_from_race_avg"] = [0.1, -0.1]
    df2["inverse_odds_rank_in_race"] = [1, 2]
    df2["popularity_rank_in_race"] = [1, 2]
    df2["futan_juryo"] = [54.0, 56.0]
    df2["futan_juryo_rank_in_race"] = [2, 1]
    df2["past_futan_juryo_avg5"] = [54.0, 55.0]
    df = pd.concat([df1, df2], ignore_index=True)
    violations = check_upcoming_feature_completeness(df)
    assert violations == []


# ---------------------------------------------------------------------------
# Historical-only frame passes (no upcoming rows → nothing to check)
# ---------------------------------------------------------------------------


def test_historical_only_frame_passes() -> None:
    """Frame with no upcoming rows (all finish_position non-null) must pass."""
    df = _historical_df()
    violations = check_upcoming_feature_completeness(df)
    assert violations == []


def test_empty_frame_passes() -> None:
    """Empty DataFrame must pass (0 rows → 0 upcoming rows → nothing to check)."""
    df = pd.DataFrame(columns=list(MUST_BE_PRESENT_FEATURES) + ["finish_position"])
    violations = check_upcoming_feature_completeness(df)
    assert violations == []


# ---------------------------------------------------------------------------
# No finish_position column — guard treats all rows as upcoming (conservative)
# ---------------------------------------------------------------------------


def test_no_finish_position_column_treats_all_as_upcoming() -> None:
    """When finish_position column is absent, guard conservatively checks all rows."""
    df = _base_upcoming_race(include_finish_position=False)
    df["inverse_odds_implied_prob"] = None
    violations = check_upcoming_feature_completeness(df)
    features_flagged = {v.feature for v in violations}
    assert "inverse_odds_implied_prob" in features_flagged


def test_no_finish_position_column_clean_frame_passes() -> None:
    """Frame without finish_position and valid features passes (all rows 'upcoming')."""
    df = _base_upcoming_race(include_finish_position=False)
    violations = check_upcoming_feature_completeness(df)
    assert violations == []


# ---------------------------------------------------------------------------
# Partial null rate — exactly at threshold boundary
# ---------------------------------------------------------------------------


def test_null_rate_exactly_at_threshold_passes() -> None:
    """Null rate exactly equal to threshold (90 %) must pass (>= threshold)."""
    # 10 horses: 9 non-null = 90 % non-null (1 null out of 10 rows)
    num_horses = 10
    df = _base_upcoming_race(num_horses=num_horses)
    # Set position 9 (last) to NULL — 9/10 non-null = exactly 90 %
    vals = [0.1] * 9 + [None]
    df["inverse_odds_implied_prob"] = vals
    violations = check_upcoming_feature_completeness(df, null_rate_threshold=0.9)
    null_rate_violations = [v for v in violations if v.violation_type == "null_rate_below_threshold"]
    assert len(null_rate_violations) == 0


def test_null_rate_one_below_threshold_is_caught() -> None:
    """Null rate 1 below threshold (89 % with threshold=90 %) must be caught."""
    # 9 horses: 8 non-null = 88.9 % < 90 %
    num_horses = 9
    df = _base_upcoming_race(num_horses=num_horses)
    vals = [0.1] * 8 + [None]
    df["inverse_odds_implied_prob"] = vals
    violations = check_upcoming_feature_completeness(df, null_rate_threshold=0.9)
    null_rate_violations = [
        v for v in violations
        if v.violation_type == "null_rate_below_threshold" and v.feature == "inverse_odds_implied_prob"
    ]
    assert len(null_rate_violations) == 1


# ---------------------------------------------------------------------------
# Mixed upcoming + historical rows
# ---------------------------------------------------------------------------


def test_null_in_historical_rows_does_not_trigger_violation() -> None:
    """NULL in a must-present feature is only flagged for upcoming rows,
    not for historical rows where that feature may legitimately differ."""
    df_upcoming = _base_upcoming_race(num_horses=3)
    df_historical = _base_upcoming_race(num_horses=2)
    df_historical["kaisai_tsukihi"] = "0601"
    df_historical["finish_position"] = [1, 2]
    # NULL futan only in historical rows
    df_historical["futan_juryo"] = None
    df = pd.concat([df_upcoming, df_historical], ignore_index=True)
    violations = check_upcoming_feature_completeness(df)
    null_rate_violations = [
        v for v in violations
        if v.violation_type == "null_rate_below_threshold" and v.feature == "futan_juryo"
    ]
    assert len(null_rate_violations) == 0


# ---------------------------------------------------------------------------
# assert_upcoming_feature_completeness wrapper
# ---------------------------------------------------------------------------


def test_assert_raises_on_null_market_signal() -> None:
    """assert_upcoming_feature_completeness must raise on NULL inverse_odds_implied_prob."""
    df = _base_upcoming_race()
    df["inverse_odds_implied_prob"] = None
    with pytest.raises(UpcomingFeatureCompletenessError):
        assert_upcoming_feature_completeness(df)


def test_assert_raises_on_null_futan() -> None:
    """assert_upcoming_feature_completeness must raise on NULL futan_juryo."""
    df = _base_upcoming_race()
    df["futan_juryo"] = None
    with pytest.raises(UpcomingFeatureCompletenessError):
        assert_upcoming_feature_completeness(df)


def test_assert_raises_on_missing_column() -> None:
    """assert_upcoming_feature_completeness must raise when a column is absent."""
    df = _clean_upcoming_df().drop(columns=["inverse_odds_market_share"])
    with pytest.raises(UpcomingFeatureCompletenessError):
        assert_upcoming_feature_completeness(df)


def test_assert_raises_on_rank_all_equal() -> None:
    """assert_upcoming_feature_completeness must raise on all-equal rank."""
    df = _base_upcoming_race(num_horses=3)
    df["inverse_odds_rank_in_race"] = 1
    with pytest.raises(UpcomingFeatureCompletenessError):
        assert_upcoming_feature_completeness(df)


def test_assert_does_not_raise_on_clean_frame() -> None:
    """assert_upcoming_feature_completeness must not raise for a clean frame."""
    df = _clean_upcoming_df()
    assert_upcoming_feature_completeness(df)  # no exception


def test_assert_raises_error_message_contains_feature_name() -> None:
    """The raised error message must contain the offending feature name."""
    df = _base_upcoming_race()
    df["futan_juryo"] = None
    with pytest.raises(UpcomingFeatureCompletenessError, match="futan_juryo"):
        assert_upcoming_feature_completeness(df)


def test_assert_logs_to_stderr_on_violation(capsys: pytest.CaptureFixture[str]) -> None:
    """Each violation must be logged to stderr before raising."""
    df = _base_upcoming_race()
    df["inverse_odds_implied_prob"] = None
    with pytest.raises(UpcomingFeatureCompletenessError):
        assert_upcoming_feature_completeness(df)
    captured = capsys.readouterr()
    assert "inverse_odds_implied_prob" in captured.err
    assert "ERROR" in captured.err


# ---------------------------------------------------------------------------
# Custom parameters
# ---------------------------------------------------------------------------


def test_custom_null_rate_threshold_stricter() -> None:
    """With threshold=1.0 even a single NULL is a violation."""
    df = _base_upcoming_race(num_horses=3)
    # Set 2 out of 3 non-null = 66.7 % — below threshold=1.0
    df["futan_juryo"] = [54.0, None, 56.0]
    violations = check_upcoming_feature_completeness(df, null_rate_threshold=1.0)
    features_flagged = {v.feature for v in violations if v.violation_type == "null_rate_below_threshold"}
    assert "futan_juryo" in features_flagged


def test_custom_must_be_present_only_checks_specified_features() -> None:
    """Caller can narrow the check to a specific set of features."""
    df = _base_upcoming_race()
    # NULL all market-signal but specify only futan in must_be_present → no violation
    df["inverse_odds_implied_prob"] = None
    violations = check_upcoming_feature_completeness(
        df,
        must_be_present=["futan_juryo"],
    )
    assert violations == []


def test_custom_upcoming_col_uses_correct_column() -> None:
    """Custom upcoming_col parameter is respected."""
    df = _base_upcoming_race(include_finish_position=False)
    df["my_label"] = None  # treat as upcoming marker
    df["inverse_odds_implied_prob"] = None
    violations = check_upcoming_feature_completeness(
        df,
        must_be_present=["inverse_odds_implied_prob"],
        upcoming_col="my_label",
    )
    features_flagged = {v.feature for v in violations}
    assert "inverse_odds_implied_prob" in features_flagged


# ---------------------------------------------------------------------------
# parse_cli_args
# ---------------------------------------------------------------------------


def test_parse_cli_args_parquet_dir(tmp_path: Path) -> None:
    args = parse_cli_args(["--parquet-dir", str(tmp_path)])
    assert args.parquet_dir == tmp_path


def test_parse_cli_args_null_rate_threshold_override(tmp_path: Path) -> None:
    args = parse_cli_args(["--parquet-dir", str(tmp_path), "--null-rate-threshold", "0.85"])
    assert args.null_rate_threshold == pytest.approx(0.85)


def test_parse_cli_args_default_threshold(tmp_path: Path) -> None:
    args = parse_cli_args(["--parquet-dir", str(tmp_path)])
    assert args.null_rate_threshold == pytest.approx(NULL_RATE_THRESHOLD)


# ---------------------------------------------------------------------------
# main() — standalone CLI
# ---------------------------------------------------------------------------


def test_main_returns_zero_for_clean_parquet(tmp_path: Path) -> None:
    """main() returns 0 when no violations are found in a clean parquet dir."""
    parquet_dir = tmp_path / "features"
    parquet_dir.mkdir()
    df = _clean_upcoming_df()
    df.to_parquet(parquet_dir / "part-0.parquet", index=False)
    result = main(["--parquet-dir", str(parquet_dir)])
    assert result == 0


def test_main_returns_one_for_null_futan(tmp_path: Path) -> None:
    """main() returns 1 when futan_juryo is all-NULL for upcoming rows."""
    parquet_dir = tmp_path / "features"
    parquet_dir.mkdir()
    df = _base_upcoming_race()
    df["futan_juryo"] = None
    df.to_parquet(parquet_dir / "part-0.parquet", index=False)
    result = main(["--parquet-dir", str(parquet_dir)])
    assert result == 1


def test_main_returns_one_for_null_market_signal(tmp_path: Path) -> None:
    """main() returns 1 when market-signal features are all-NULL for upcoming rows."""
    parquet_dir = tmp_path / "features"
    parquet_dir.mkdir()
    df = _base_upcoming_race()
    df["inverse_odds_implied_prob"] = None
    df["inverse_odds_market_share"] = None
    df.to_parquet(parquet_dir / "part-0.parquet", index=False)
    result = main(["--parquet-dir", str(parquet_dir)])
    assert result == 1


def test_main_logs_ok_message_to_stderr_on_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """main() logs an OK message to stderr when no violations are found."""
    parquet_dir = tmp_path / "features"
    parquet_dir.mkdir()
    df = _clean_upcoming_df()
    df.to_parquet(parquet_dir / "part-0.parquet", index=False)
    main(["--parquet-dir", str(parquet_dir)])
    captured = capsys.readouterr()
    assert "OK" in captured.err


def test_main_logs_fail_message_to_stderr_on_violation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """main() logs a FAIL message to stderr when violations are found."""
    parquet_dir = tmp_path / "features"
    parquet_dir.mkdir()
    df = _base_upcoming_race()
    df["futan_juryo"] = None
    df.to_parquet(parquet_dir / "part-0.parquet", index=False)
    main(["--parquet-dir", str(parquet_dir)])
    captured = capsys.readouterr()
    assert "FAIL" in captured.err
    assert "futan_juryo" in captured.err


# ---------------------------------------------------------------------------
# check_parquet_dir helper
# ---------------------------------------------------------------------------


def test_check_parquet_dir_clean(tmp_path: Path) -> None:
    """check_parquet_dir returns empty list for a clean parquet."""
    parquet_dir = tmp_path / "features"
    parquet_dir.mkdir()
    df = _clean_upcoming_df()
    df.to_parquet(parquet_dir / "part-0.parquet", index=False)
    violations = check_parquet_dir(parquet_dir)
    assert violations == []


def test_check_parquet_dir_null_market_signal(tmp_path: Path) -> None:
    """check_parquet_dir catches NULL inverse_odds_implied_prob."""
    parquet_dir = tmp_path / "features"
    parquet_dir.mkdir()
    df = _base_upcoming_race()
    df["inverse_odds_implied_prob"] = None
    df.to_parquet(parquet_dir / "part-0.parquet", index=False)
    violations = check_parquet_dir(parquet_dir)
    features_flagged = {v.feature for v in violations}
    assert "inverse_odds_implied_prob" in features_flagged


# ---------------------------------------------------------------------------
# Rank check skipped when rank_features list is empty
# ---------------------------------------------------------------------------


def test_rank_check_skipped_when_rank_features_empty() -> None:
    """When rank_features=[] the outer if-guard short-circuits to return violations.

    Exercises the branch 268->291 (if race_id_present and rank_present ...) where
    rank_present is empty so the condition is False and the block is skipped.
    """
    df = _base_upcoming_race(num_horses=3)
    df["inverse_odds_rank_in_race"] = 1  # would be caught if rank_features used
    violations = check_upcoming_feature_completeness(df, rank_features=[])
    rank_violations = [v for v in violations if v.violation_type == "rank_all_equal"]
    assert len(rank_violations) == 0
