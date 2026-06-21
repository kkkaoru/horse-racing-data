"""Tests for predict_lib.etop2_override — E-top2 place-preserving XGB override.

Covers:
  - XGB#1 == CB#2 case (override fires, CB#1 and CB#2 swap, CB#3 unchanged)
  - XGB#1 == CB#1 case (no override — already agree)
  - XGB#1 ∈ CB#3+ case (no override — would disturb place3)
  - class 701 exclusion (no override regardless of XGB score)
  - class None (treated as eligible — override can fire)
  - rank-1 uniqueness after override (exactly one highest score)
  - place3 preserved (CB#3 horse retains its score after override)
  - single-horse race (no override possible)
  - two-horse race (override swaps when condition met)
  - is_etop2_override_active reflects the same condition
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.etop2_override import (
    ETOP2_EXCLUDED_CLASS,
    ETOP2_MODEL_VERSION,
    ETOP2_XGB_MODEL_VERSION,
    apply_etop2_scores,
    is_etop2_override_active,
)

# ---------------------------------------------------------------------------
# Constants


def test_excluded_class_is_701() -> None:
    assert ETOP2_EXCLUDED_CLASS == "701"


def test_model_version_label() -> None:
    assert ETOP2_MODEL_VERSION == "iter22-jra-etop2"


def test_xgb_model_version_label() -> None:
    assert ETOP2_XGB_MODEL_VERSION == "xgb-jra-2013-v8"


# ---------------------------------------------------------------------------
# apply_etop2_scores — XGB#1 == CB#2 (override fires)


def test_xgb1_equals_cb2_swaps_rank1_and_rank2() -> None:
    """When XGB#1 == CB#2 the scores at rank-1 and rank-2 positions are swapped."""
    # CB ranking: horse A=3.0 (rank-1), B=2.0 (rank-2), C=1.0 (rank-3)
    # XGB: horse B is XGB#1 (highest xgb score)
    cb_scores = [3.0, 2.0, 1.0]  # A, B, C
    xgb_scores = [1.0, 5.0, 0.5]  # B is XGB#1 (index 1)
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")

    # B (index 1) should be rank-1 (highest score), A (index 0) rank-2
    assert result[1] > result[0], "B must outrank A after override"
    assert result[0] > result[2], "A must outrank C after override"
    # C (rank-3) unchanged in relative order
    assert result[2] == cb_scores[2], "CB#3 score must be unchanged"


def test_xgb1_equals_cb2_cb3_stays_at_rank3() -> None:
    """CB#3 position is preserved by construction after E-top2 override."""
    # 5-horse race; CB ranking by score: 5,4,3,2,1 → horse 0=rank1, 1=rank2, ...
    cb_scores = [5.0, 4.0, 3.0, 2.0, 1.0]
    xgb_scores = [1.0, 9.0, 0.5, 0.3, 0.1]  # horse 1 is XGB#1 (CB#2)
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")

    # After override: horse 1 highest, horse 0 second, horses 2/3/4 unchanged
    sorted_result = sorted(range(5), key=lambda i: -result[i])
    assert sorted_result[0] == 1, "CB#2 horse becomes rank-1"
    assert sorted_result[1] == 0, "CB#1 horse becomes rank-2"
    assert sorted_result[2] == 2, "CB#3 horse stays rank-3"
    assert sorted_result[3] == 3
    assert sorted_result[4] == 4
    # Verify CB#3+ scores unchanged
    assert result[2] == cb_scores[2]
    assert result[3] == cb_scores[3]
    assert result[4] == cb_scores[4]


def test_xgb1_equals_cb2_exactly_one_rank1() -> None:
    """After override there is exactly one horse with the maximum score."""
    cb_scores = [10.0, 8.0, 6.0]
    xgb_scores = [2.0, 15.0, 1.0]  # index 1 is XGB#1 = CB#2
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="005")
    max_score = max(result)
    n_max = sum(1 for s in result if s == max_score)
    assert n_max == 1, "Exactly one horse should have the maximum score"


def test_xgb1_equals_cb2_injected_scores_above_all_cb() -> None:
    """Promoted pair receive scores above the highest original CB score."""
    cb_scores = [10.0, 8.0, 6.0]
    xgb_scores = [2.0, 15.0, 1.0]
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="010")
    cb_max = max(cb_scores)
    assert result[1] > cb_max, "New rank-1 horse score must exceed original CB max"
    assert result[0] > cb_max, "New rank-2 horse score must exceed original CB max"


# ---------------------------------------------------------------------------
# apply_etop2_scores — XGB#1 == CB#1 (no override)


def test_xgb1_equals_cb1_no_change() -> None:
    """When XGB#1 agrees with CB#1 the output equals CatBoost scores unchanged."""
    cb_scores = [5.0, 3.0, 1.0]
    xgb_scores = [9.0, 2.0, 1.0]  # horse 0 is XGB#1 = CB#1
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")
    assert result == cb_scores


# ---------------------------------------------------------------------------
# apply_etop2_scores — XGB#1 ∈ CB#3+ (no override)


def test_xgb1_in_cb3plus_no_change() -> None:
    """When XGB#1 is CB#3 or deeper, output equals CatBoost scores unchanged."""
    cb_scores = [5.0, 4.0, 3.0, 2.0]
    xgb_scores = [0.5, 0.3, 8.0, 0.1]  # horse 2 is XGB#1 = CB#3
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")
    assert result == cb_scores


def test_xgb1_in_cb4_no_change() -> None:
    """XGB#1 == CB#4 — no override."""
    cb_scores = [5.0, 4.0, 3.0, 2.0, 1.0]
    xgb_scores = [0.5, 0.3, 0.2, 8.0, 0.1]  # horse 3 is XGB#1 = CB#4
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")
    assert result == cb_scores


# ---------------------------------------------------------------------------
# apply_etop2_scores — class 701 exclusion


def test_class_701_excluded_xgb1_equals_cb2() -> None:
    """Even when XGB#1 == CB#2, class 701 returns pure CatBoost scores."""
    cb_scores = [5.0, 4.0, 3.0]
    xgb_scores = [1.0, 9.0, 0.5]  # horse 1 is XGB#1 = CB#2
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="701")
    assert result == cb_scores, "Class 701 must never override"


def test_class_701_string_match_is_exact() -> None:
    """Class code '7010' (hypothetical) is not excluded — only exact '701'."""
    cb_scores = [5.0, 4.0, 3.0]
    xgb_scores = [1.0, 9.0, 0.5]
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="7010")
    # With XGB#1 == CB#2 and class '7010' (not excluded), override fires
    assert result[1] > result[0], "class '7010' is not excluded — override must fire"


# ---------------------------------------------------------------------------
# apply_etop2_scores — class None (eligible)


def test_class_none_treated_as_eligible() -> None:
    """None race_class is not excluded; E-top2 fires when XGB#1 == CB#2."""
    cb_scores = [5.0, 4.0, 3.0]
    xgb_scores = [1.0, 9.0, 0.5]  # horse 1 is XGB#1 = CB#2
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class=None)
    # Override must fire
    assert result[1] > result[0], "None class must not be excluded"


# ---------------------------------------------------------------------------
# apply_etop2_scores — edge cases: single-horse and two-horse races


def test_single_horse_no_override() -> None:
    """A single-horse race cannot have CB#2 — no override, scores unchanged."""
    cb_scores = [3.0]
    xgb_scores = [5.0]
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")
    assert result == cb_scores


def test_two_horse_race_override_fires() -> None:
    """Two-horse race: XGB#1 == CB#2 → swap. Both horses have valid ranks."""
    cb_scores = [4.0, 2.0]
    xgb_scores = [0.5, 8.0]  # horse 1 is XGB#1 = CB#2
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")
    assert result[1] > result[0], "horse 1 becomes rank-1 in two-horse race"


def test_two_horse_race_class_701_no_override() -> None:
    """Two-horse race class 701 — no override."""
    cb_scores = [4.0, 2.0]
    xgb_scores = [0.5, 8.0]
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="701")
    assert result == cb_scores


# ---------------------------------------------------------------------------
# apply_etop2_scores — result type


def test_returns_list_of_floats() -> None:
    """Return type is always list[float]."""
    cb_scores = [3.0, 2.0, 1.0]
    xgb_scores = [1.0, 5.0, 0.5]
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")
    assert isinstance(result, list)
    assert all(isinstance(s, float) for s in result)


def test_result_length_matches_input() -> None:
    """Output length always equals input length."""
    for n in (1, 2, 3, 8, 18):
        cb_scores = list(range(n, 0, -1))
        xgb_scores = [float(i) for i in range(n)]
        result = apply_etop2_scores(
            [float(s) for s in cb_scores], xgb_scores, race_class="703"
        )
        assert len(result) == n


# ---------------------------------------------------------------------------
# is_etop2_override_active


def test_active_when_xgb1_equals_cb2() -> None:
    cb_scores = [5.0, 4.0, 3.0]
    xgb_scores = [1.0, 9.0, 0.5]  # XGB#1 = horse 1 = CB#2
    assert is_etop2_override_active(cb_scores, xgb_scores, race_class="703") is True


def test_not_active_when_xgb1_equals_cb1() -> None:
    cb_scores = [5.0, 4.0, 3.0]
    xgb_scores = [9.0, 2.0, 0.5]  # XGB#1 = horse 0 = CB#1
    assert is_etop2_override_active(cb_scores, xgb_scores, race_class="703") is False


def test_not_active_when_xgb1_in_cb3plus() -> None:
    cb_scores = [5.0, 4.0, 3.0, 2.0]
    xgb_scores = [0.5, 0.3, 9.0, 0.1]  # XGB#1 = horse 2 = CB#3
    assert is_etop2_override_active(cb_scores, xgb_scores, race_class="703") is False


def test_not_active_for_class_701() -> None:
    cb_scores = [5.0, 4.0, 3.0]
    xgb_scores = [1.0, 9.0, 0.5]  # Would override but class 701
    assert is_etop2_override_active(cb_scores, xgb_scores, race_class="701") is False


def test_not_active_single_horse() -> None:
    assert is_etop2_override_active([5.0], [9.0], race_class="703") is False


def test_active_consistent_with_apply() -> None:
    """is_etop2_override_active() == (argmax changed by apply_etop2_scores)."""
    test_cases = [
        ([5.0, 4.0, 3.0], [1.0, 9.0, 0.5], "703"),   # should fire
        ([5.0, 4.0, 3.0], [9.0, 2.0, 0.5], "703"),   # XGB#1==CB#1, no fire
        ([5.0, 4.0, 3.0], [0.5, 0.3, 9.0], "703"),   # XGB#1==CB#3, no fire
        ([5.0, 4.0, 3.0], [1.0, 9.0, 0.5], "701"),   # class 701, no fire
        ([5.0, 4.0, 3.0], [1.0, 9.0, 0.5], None),    # None class, fires
    ]
    for cb, xgb, cls in test_cases:
        active = is_etop2_override_active(cb, xgb, race_class=cls)
        original_argmax = cb.index(max(cb))
        new_scores = apply_etop2_scores(cb, xgb, race_class=cls)
        new_argmax = new_scores.index(max(new_scores))
        changed = original_argmax != new_argmax
        assert active == changed, (
            f"is_active={active} but argmax_changed={changed} for "
            f"cb={cb} xgb={xgb} class={cls}"
        )


# ---------------------------------------------------------------------------
# apply_etop2_scores — property: place3 structurally preserved


def test_place3_preserved_when_override_fires() -> None:
    """When override fires, the horse at CB#3 must remain at output rank-3."""
    # 6-horse race; CB#3 is horse 2 (score=3.0)
    cb_scores = [5.0, 4.0, 3.0, 2.0, 1.0, 0.5]
    xgb_scores = [0.1, 9.0, 0.5, 0.3, 0.2, 0.1]  # horse 1 is XGB#1 = CB#2
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")

    sorted_result = sorted(range(6), key=lambda i: -result[i])
    assert sorted_result[2] == 2, "CB#3 horse (index 2) must remain at output rank-3"


def test_place3_unchanged_no_override() -> None:
    """When no override fires, all scores are identical to CB."""
    cb_scores = [5.0, 4.0, 3.0, 2.0]
    xgb_scores = [9.0, 0.5, 0.3, 0.1]  # XGB#1 = CB#1, no override
    result = apply_etop2_scores(cb_scores, xgb_scores, race_class="703")
    assert result == cb_scores
