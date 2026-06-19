"""Unit tests for ``predict_lib.late_binding``.

Covers the 5 late-binding recompute formulas (odds_score / popularity_score /
weight_diff_from_avg) for present / None / median-fallback / clamp-bound inputs
across every category, plus ``apply_late_binding_to_entry`` (5-column overwrite,
early-column preservation, shusso_tosu runner-count source, coerce of str/None
cells).  No I/O — these are pure functions.
"""

from __future__ import annotations

import math

from predict_lib.late_binding import (
    OddsSnapshot,
    WeightSnapshot,
    apply_late_binding_to_entry,
    coerce_optional_float,
    coerce_optional_int,
    compute_odds_score,
    compute_popularity_score,
    compute_weight_diff,
)

# ---------------------------------------------------------------------------
# compute_odds_score
# ---------------------------------------------------------------------------


def test_compute_odds_score_present_value() -> None:
    result = compute_odds_score(7.3, "nar")
    assert result == math.log(7.3) / math.log(300)


def test_compute_odds_score_clamp_upper_bound() -> None:
    result = compute_odds_score(10000.0, "nar")
    assert result == 1.0


def test_compute_odds_score_floor_at_one() -> None:
    result = compute_odds_score(0.5, "jra")
    assert result == math.log(1.0) / math.log(300)


def test_compute_odds_score_exactly_one_is_zero() -> None:
    result = compute_odds_score(1.0, "jra")
    assert result == 0.0


def test_compute_odds_score_none_uses_jra_median() -> None:
    result = compute_odds_score(None, "jra")
    assert result == 0.5664


def test_compute_odds_score_none_uses_nar_median() -> None:
    result = compute_odds_score(None, "nar")
    assert result == 0.5048


def test_compute_odds_score_none_uses_banei_median() -> None:
    result = compute_odds_score(None, "ban-ei")
    assert result == 0.5048


def test_compute_odds_score_zero_uses_median() -> None:
    result = compute_odds_score(0.0, "nar")
    assert result == 0.5048


def test_compute_odds_score_negative_uses_median() -> None:
    result = compute_odds_score(-3.0, "jra")
    assert result == 0.5664


# ---------------------------------------------------------------------------
# compute_popularity_score
# ---------------------------------------------------------------------------


def test_compute_popularity_score_favourite_is_zero() -> None:
    result = compute_popularity_score(1, 16, "jra")
    assert result == 0.0


def test_compute_popularity_score_last_is_one() -> None:
    result = compute_popularity_score(16, 16, "jra")
    assert result == 1.0


def test_compute_popularity_score_mid_value() -> None:
    result = compute_popularity_score(5, 11, "nar")
    assert result == 0.4


def test_compute_popularity_score_runner_count_one_uses_median() -> None:
    result = compute_popularity_score(1, 1, "nar")
    assert result == 0.5


def test_compute_popularity_score_runner_count_zero_uses_median() -> None:
    result = compute_popularity_score(3, 0, "jra")
    assert result == 0.5


def test_compute_popularity_score_runner_count_none_uses_median() -> None:
    result = compute_popularity_score(3, None, "ban-ei")
    assert result == 0.5


def test_compute_popularity_score_ninkijun_none_uses_median() -> None:
    result = compute_popularity_score(None, 12, "nar")
    assert result == 0.5


# ---------------------------------------------------------------------------
# compute_weight_diff
# ---------------------------------------------------------------------------


def test_compute_weight_diff_present() -> None:
    result = compute_weight_diff(460.0, 452.0)
    assert result == 8.0


def test_compute_weight_diff_negative() -> None:
    result = compute_weight_diff(448.0, 452.0)
    assert result == -4.0


def test_compute_weight_diff_bataiju_none() -> None:
    result = compute_weight_diff(None, 452.0)
    assert result is None


def test_compute_weight_diff_avg_none() -> None:
    result = compute_weight_diff(460.0, None)
    assert result is None


def test_compute_weight_diff_both_none() -> None:
    result = compute_weight_diff(None, None)
    assert result is None


# ---------------------------------------------------------------------------
# coerce helpers
# ---------------------------------------------------------------------------


def test_coerce_optional_float_none() -> None:
    assert coerce_optional_float(None) is None


def test_coerce_optional_float_empty_string() -> None:
    assert coerce_optional_float("") is None


def test_coerce_optional_float_whitespace_string() -> None:
    assert coerce_optional_float("   ") is None


def test_coerce_optional_float_numeric_string() -> None:
    assert coerce_optional_float("452.5") == 452.5


def test_coerce_optional_float_non_numeric_string() -> None:
    assert coerce_optional_float("abc") is None


def test_coerce_optional_float_int_value() -> None:
    assert coerce_optional_float(16) == 16.0


def test_coerce_optional_float_float_value() -> None:
    assert coerce_optional_float(3.5) == 3.5


def test_coerce_optional_float_bool_is_none() -> None:
    assert coerce_optional_float(True) is None


def test_coerce_optional_float_unsupported_type_is_none() -> None:
    assert coerce_optional_float([1, 2]) is None


def test_coerce_optional_int_float_truncates() -> None:
    assert coerce_optional_int(12.0) == 12


def test_coerce_optional_int_numeric_string() -> None:
    assert coerce_optional_int("16") == 16


def test_coerce_optional_int_none() -> None:
    assert coerce_optional_int(None) is None


def test_coerce_optional_int_empty_string() -> None:
    assert coerce_optional_int("") is None


# ---------------------------------------------------------------------------
# apply_late_binding_to_entry
# ---------------------------------------------------------------------------


def test_apply_late_binding_overwrites_tansho_odds() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": 450.0, "tansho_odds": 99.9}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(458.0), "nar")
    assert result["tansho_odds"] == 4.5


def test_apply_late_binding_overwrites_tansho_ninkijun() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": 450.0, "tansho_ninkijun": 99}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(458.0), "nar")
    assert result["tansho_ninkijun"] == 3


def test_apply_late_binding_recomputes_odds_score() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": 450.0}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(458.0), "nar")
    assert result["odds_score"] == math.log(4.5) / math.log(300)


def test_apply_late_binding_recomputes_popularity_score() -> None:
    entry: dict[str, object] = {"shusso_tosu": 11, "weight_avg_5": 450.0}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 5), WeightSnapshot(458.0), "nar")
    assert result["popularity_score"] == 0.4


def test_apply_late_binding_recomputes_weight_diff() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": 450.0}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(458.0), "nar")
    assert result["weight_diff_from_avg"] == 8.0


def test_apply_late_binding_uses_shusso_tosu_for_runner_count() -> None:
    entry: dict[str, object] = {"shusso_tosu": 6, "weight_avg_5": 450.0}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 6), WeightSnapshot(458.0), "nar")
    assert result["popularity_score"] == 1.0


def test_apply_late_binding_coerces_str_shusso_tosu() -> None:
    entry: dict[str, object] = {"shusso_tosu": "11", "weight_avg_5": "450.0"}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 5), WeightSnapshot(458.0), "nar")
    assert result["popularity_score"] == 0.4


def test_apply_late_binding_none_shusso_tosu_uses_median() -> None:
    entry: dict[str, object] = {"shusso_tosu": None, "weight_avg_5": 450.0}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 5), WeightSnapshot(458.0), "nar")
    assert result["popularity_score"] == 0.5


def test_apply_late_binding_none_odds_uses_median() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": 450.0}
    result = apply_late_binding_to_entry(
        entry, OddsSnapshot(None, None), WeightSnapshot(458.0), "jra"
    )
    assert result["odds_score"] == 0.5664


def test_apply_late_binding_none_bataiju_weight_diff_none() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": 450.0}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(None), "nar")
    assert result["weight_diff_from_avg"] is None


def test_apply_late_binding_none_weight_avg_5_weight_diff_none() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": None}
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(458.0), "nar")
    assert result["weight_diff_from_avg"] is None


def test_apply_late_binding_preserves_early_columns() -> None:
    entry: dict[str, object] = {
        "shusso_tosu": 12,
        "weight_avg_5": 450.0,
        "jockey_win_rate": 0.21,
    }
    result = apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(458.0), "nar")
    assert result["jockey_win_rate"] == 0.21


def test_apply_late_binding_does_not_mutate_input() -> None:
    entry: dict[str, object] = {"shusso_tosu": 12, "weight_avg_5": 450.0, "odds_score": 0.1}
    apply_late_binding_to_entry(entry, OddsSnapshot(4.5, 3), WeightSnapshot(458.0), "nar")
    assert entry["odds_score"] == 0.1
