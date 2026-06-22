"""Tests for predict_lib.nar_etop2_override — NAR E-top2 per-class CB override.

The NAR override is the mirror image of the JRA one: XGBoost is the BASE and
CatBoost CB-2013 supplies the override signal, gated by per-class routing
(``NAR_ETOP2_ADOPT_CLASSES`` = {A, B, NEW, other}).

Covers:
  - ADOPT class, CB#1 == XGB#2 (override fires, XGB#1/XGB#2 swap, XGB#3 unchanged)
  - ADOPT class, CB#1 == XGB#1 (no override — already agree)
  - ADOPT class, CB#1 ∈ XGB#3+ (no override — would disturb place3)
  - REJECT classes C / OP / MUKATSU (always pure XGB regardless of CB score)
  - class None (safety — never overrides)
  - per-class routing for every named NAR sub-class
  - rank-1 uniqueness + injected scores above all XGB after override
  - place3 structurally preserved (XGB#3 horse retains rank-3)
  - single-horse and two-horse races
  - is_nar_etop2_override_active reflects the same condition
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.nar_etop2_override import (
    apply_nar_etop2_scores,
    is_nar_etop2_override_active,
)

# Convenience class codes for readability in the test cases below.
ADOPT_A: str = "A"
ADOPT_B: str = "B"
ADOPT_NEW: str = "NEW"
ADOPT_OTHER: str = "other"
REJECT_C: str = "C"
REJECT_OP: str = "OP"
REJECT_MUKATSU: str = "MUKATSU"


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — ADOPT class, CB#1 == XGB#2 (override fires)


def test_cb1_equals_xgb2_swaps_rank1_and_rank2() -> None:
    """When CB#1 == XGB#2 the scores at rank-1 and rank-2 positions are swapped."""
    # XGB ranking: horse A=3.0 (rank-1), B=2.0 (rank-2), C=1.0 (rank-3)
    # CB: horse B is CB#1 (highest cb score)
    xgb_scores = [3.0, 2.0, 1.0]  # A, B, C
    cb_scores = [1.0, 5.0, 0.5]  # B is CB#1 (index 1) == XGB#2
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_A)

    # B (index 1) should be rank-1 (highest score), A (index 0) rank-2
    assert result[1] > result[0], "B must outrank A after override"
    assert result[0] > result[2], "A must outrank C after override"
    assert result[2] == xgb_scores[2], "XGB#3 score must be unchanged"


def test_cb1_equals_xgb2_xgb3_stays_at_rank3() -> None:
    """XGB#3 position is preserved by construction after the override."""
    # 5-horse race; XGB ranking by score: 5,4,3,2,1 → horse 0=rank1, 1=rank2, ...
    xgb_scores = [5.0, 4.0, 3.0, 2.0, 1.0]
    cb_scores = [1.0, 9.0, 0.5, 0.3, 0.1]  # horse 1 is CB#1 == XGB#2
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_B)

    sorted_result = sorted(range(5), key=lambda i: -result[i])
    assert sorted_result[0] == 1, "XGB#2 horse becomes rank-1"
    assert sorted_result[1] == 0, "XGB#1 horse becomes rank-2"
    assert sorted_result[2] == 2, "XGB#3 horse stays rank-3"
    assert sorted_result[3] == 3
    assert sorted_result[4] == 4
    assert result[2] == xgb_scores[2]
    assert result[3] == xgb_scores[3]
    assert result[4] == xgb_scores[4]


def test_cb1_equals_xgb2_exactly_one_rank1() -> None:
    """After override there is exactly one horse with the maximum score."""
    xgb_scores = [10.0, 8.0, 6.0]
    cb_scores = [2.0, 15.0, 1.0]  # index 1 is CB#1 == XGB#2
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_NEW)
    max_score = max(result)
    n_max = sum(1 for s in result if s == max_score)
    assert n_max == 1, "Exactly one horse should have the maximum score"


def test_cb1_equals_xgb2_injected_scores_above_all_xgb() -> None:
    """Promoted pair receive scores above the highest original XGB score."""
    xgb_scores = [10.0, 8.0, 6.0]
    cb_scores = [2.0, 15.0, 1.0]
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_OTHER)
    xgb_max = max(xgb_scores)
    assert result[1] > xgb_max, "New rank-1 horse score must exceed original XGB max"
    assert result[0] > xgb_max, "New rank-2 horse score must exceed original XGB max"


def test_promoted_score_offsets_match_spec() -> None:
    """Promoted gets max(xgb)+1.0, demoted gets max(xgb)+0.5 (exact offsets)."""
    xgb_scores = [10.0, 8.0, 6.0]
    cb_scores = [2.0, 15.0, 1.0]  # index 1 is CB#1 == XGB#2
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_A)
    xgb_max = max(xgb_scores)
    assert result[1] == xgb_max + 1.0, "XGB#2 horse promoted to max+1.0"
    assert result[0] == xgb_max + 0.5, "XGB#1 horse demoted to max+0.5"


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — ADOPT class, CB#1 == XGB#1 (no override)


def test_cb1_equals_xgb1_no_change() -> None:
    """When CB#1 agrees with XGB#1 the output equals XGB scores unchanged."""
    xgb_scores = [5.0, 3.0, 1.0]
    cb_scores = [9.0, 2.0, 1.0]  # horse 0 is CB#1 == XGB#1
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_A)
    assert result == xgb_scores


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — ADOPT class, CB#1 ∈ XGB#3+ (no override)


def test_cb1_in_xgb3plus_no_change() -> None:
    """When CB#1 is XGB#3 or deeper, output equals XGB scores unchanged."""
    xgb_scores = [5.0, 4.0, 3.0, 2.0]
    cb_scores = [0.5, 0.3, 8.0, 0.1]  # horse 2 is CB#1 == XGB#3
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_B)
    assert result == xgb_scores


def test_cb1_in_xgb4_no_change() -> None:
    """CB#1 == XGB#4 — no override."""
    xgb_scores = [5.0, 4.0, 3.0, 2.0, 1.0]
    cb_scores = [0.5, 0.3, 0.2, 8.0, 0.1]  # horse 3 is CB#1 == XGB#4
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_NEW)
    assert result == xgb_scores


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — REJECT classes (per-class routing)


def test_class_c_rejected_cb1_equals_xgb2() -> None:
    """Even when CB#1 == XGB#2, class C returns pure XGB scores."""
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]  # horse 1 is CB#1 == XGB#2
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=REJECT_C)
    assert result == xgb_scores, "Class C must never override"


def test_class_op_rejected_cb1_equals_xgb2() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=REJECT_OP)
    assert result == xgb_scores, "Class OP must never override"


def test_class_mukatsu_rejected_cb1_equals_xgb2() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=REJECT_MUKATSU)
    assert result == xgb_scores, "Class MUKATSU must never override"


def test_unregistered_raw_class_rejected() -> None:
    """A raw / unnormalised class code not in the allowlist routes to pure XGB."""
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class="UNKNOWN")
    assert result == xgb_scores


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — class None (safety, never overrides)


def test_class_none_never_overrides() -> None:
    """None class is treated as unknown and never triggers the override."""
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]  # would fire if class were ADOPT
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=None)
    assert result == xgb_scores, "None class must never override (safety)"


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — every ADOPT class fires, every REJECT class does not


def test_all_adopt_classes_fire_when_condition_met() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]  # CB#1 == XGB#2
    for cls in (ADOPT_A, ADOPT_B, ADOPT_NEW, ADOPT_OTHER):
        result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=cls)
        assert result[1] > result[0], f"override must fire for ADOPT class {cls}"


def test_all_reject_classes_skip_when_condition_met() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]  # CB#1 == XGB#2
    for cls in (REJECT_C, REJECT_OP, REJECT_MUKATSU):
        result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=cls)
        assert result == xgb_scores, f"override must NOT fire for REJECT class {cls}"


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — edge cases: single-horse and two-horse races


def test_single_horse_no_override() -> None:
    """A single-horse race cannot have XGB#2 — no override, scores unchanged."""
    xgb_scores = [3.0]
    cb_scores = [5.0]
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_A)
    assert result == xgb_scores


def test_two_horse_race_override_fires() -> None:
    """Two-horse ADOPT race: CB#1 == XGB#2 → swap."""
    xgb_scores = [4.0, 2.0]
    cb_scores = [0.5, 8.0]  # horse 1 is CB#1 == XGB#2
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_B)
    assert result[1] > result[0], "horse 1 becomes rank-1 in two-horse race"


def test_two_horse_race_reject_class_no_override() -> None:
    """Two-horse REJECT class — no override."""
    xgb_scores = [4.0, 2.0]
    cb_scores = [0.5, 8.0]
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=REJECT_C)
    assert result == xgb_scores


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — result type / length


def test_returns_list_of_floats() -> None:
    xgb_scores = [3.0, 2.0, 1.0]
    cb_scores = [1.0, 5.0, 0.5]
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_A)
    assert isinstance(result, list)
    assert all(isinstance(s, float) for s in result)


def test_result_length_matches_input() -> None:
    for n in (1, 2, 3, 8, 18):
        xgb_scores = [float(s) for s in range(n, 0, -1)]
        cb_scores = [float(i) for i in range(n)]
        result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_A)
        assert len(result) == n


# ---------------------------------------------------------------------------
# is_nar_etop2_override_active


def test_active_when_cb1_equals_xgb2() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]  # CB#1 = horse 1 = XGB#2
    assert is_nar_etop2_override_active(xgb_scores, cb_scores, nar_class=ADOPT_A) is True


def test_not_active_when_cb1_equals_xgb1() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [9.0, 2.0, 0.5]  # CB#1 = horse 0 = XGB#1
    assert is_nar_etop2_override_active(xgb_scores, cb_scores, nar_class=ADOPT_A) is False


def test_not_active_when_cb1_in_xgb3plus() -> None:
    xgb_scores = [5.0, 4.0, 3.0, 2.0]
    cb_scores = [0.5, 0.3, 9.0, 0.1]  # CB#1 = horse 2 = XGB#3
    assert is_nar_etop2_override_active(xgb_scores, cb_scores, nar_class=ADOPT_B) is False


def test_not_active_for_reject_class() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]  # Would override but class C
    assert is_nar_etop2_override_active(xgb_scores, cb_scores, nar_class=REJECT_C) is False


def test_not_active_for_class_none() -> None:
    xgb_scores = [5.0, 4.0, 3.0]
    cb_scores = [1.0, 9.0, 0.5]
    assert is_nar_etop2_override_active(xgb_scores, cb_scores, nar_class=None) is False


def test_not_active_single_horse() -> None:
    assert is_nar_etop2_override_active([5.0], [9.0], nar_class=ADOPT_A) is False


def test_active_consistent_with_apply() -> None:
    """is_nar_etop2_override_active() == (argmax changed by apply_nar_etop2_scores)."""
    test_cases = [
        ([5.0, 4.0, 3.0], [1.0, 9.0, 0.5], ADOPT_A),  # should fire
        ([5.0, 4.0, 3.0], [9.0, 2.0, 0.5], ADOPT_B),  # CB#1==XGB#1, no fire
        ([5.0, 4.0, 3.0], [0.5, 0.3, 9.0], ADOPT_NEW),  # CB#1==XGB#3, no fire
        ([5.0, 4.0, 3.0], [1.0, 9.0, 0.5], REJECT_C),  # reject class, no fire
        ([5.0, 4.0, 3.0], [1.0, 9.0, 0.5], None),  # None class, no fire
        ([5.0, 4.0, 3.0], [1.0, 9.0, 0.5], ADOPT_OTHER),  # should fire
    ]
    for xgb, cb, cls in test_cases:
        active = is_nar_etop2_override_active(xgb, cb, nar_class=cls)
        original_argmax = xgb.index(max(xgb))
        new_scores = apply_nar_etop2_scores(xgb, cb, nar_class=cls)
        new_argmax = new_scores.index(max(new_scores))
        changed = original_argmax != new_argmax
        assert active == changed, (
            f"is_active={active} but argmax_changed={changed} for xgb={xgb} cb={cb} class={cls}"
        )


# ---------------------------------------------------------------------------
# apply_nar_etop2_scores — property: place3 structurally preserved


def test_place3_preserved_when_override_fires() -> None:
    """When override fires, the horse at XGB#3 must remain at output rank-3."""
    # 6-horse race; XGB#3 is horse 2 (score=3.0)
    xgb_scores = [5.0, 4.0, 3.0, 2.0, 1.0, 0.5]
    cb_scores = [0.1, 9.0, 0.5, 0.3, 0.2, 0.1]  # horse 1 is CB#1 == XGB#2
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_A)

    sorted_result = sorted(range(6), key=lambda i: -result[i])
    assert sorted_result[2] == 2, "XGB#3 horse (index 2) must remain at output rank-3"


def test_place3_unchanged_no_override() -> None:
    """When no override fires, all scores are identical to XGB."""
    xgb_scores = [5.0, 4.0, 3.0, 2.0]
    cb_scores = [9.0, 0.5, 0.3, 0.1]  # CB#1 == XGB#1, no override
    result = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class=ADOPT_B)
    assert result == xgb_scores
