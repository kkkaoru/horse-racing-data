"""Tests for feature-vector building + booster scoring with a fake booster."""

from __future__ import annotations

import struct
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.scorer import (
    assert_feature_count,
    build_feature_matrix,
    build_feature_row,
    score_matrix,
)


class _FakeBooster:
    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        return [sum(row) for row in matrix]


def test_build_feature_row_catboost_order_preserved() -> None:
    row = build_feature_row({"a": 1.0, "b": 2.0, "c": 3.0}, ["c", "a", "b"], "catboost")
    assert row == [3.0, 1.0, 2.0]


def test_build_feature_row_missing_defaults_to_zero() -> None:
    row = build_feature_row({"a": 5.0}, ["a", "missing"], "catboost")
    assert row == [5.0, 0.0]


def test_build_feature_row_none_is_zero() -> None:
    row = build_feature_row({"a": None}, ["a"], "catboost")
    assert row == [0.0]


def test_build_feature_row_bool_true_is_one() -> None:
    row = build_feature_row({"flag": True}, ["flag"], "catboost")
    assert row == [1.0]


def test_build_feature_row_bool_false_is_zero() -> None:
    row = build_feature_row({"flag": False}, ["flag"], "catboost")
    assert row == [0.0]


def test_build_feature_row_string_number_coerced() -> None:
    row = build_feature_row({"x": "12.5"}, ["x"], "catboost")
    assert row == [12.5]


def test_build_feature_row_empty_string_is_zero() -> None:
    row = build_feature_row({"x": "  "}, ["x"], "catboost")
    assert row == [0.0]


def test_build_feature_row_xgboost_float32_quantised() -> None:
    expected = struct.unpack("f", struct.pack("f", 0.1))[0]
    row = build_feature_row({"x": 0.1}, ["x"], "xgboost")
    assert row == [expected]


def test_build_feature_row_lightgbm_keeps_float64() -> None:
    # LightGBM ranking is robust at float64 — ONLY XGBoost is float32-quantised.
    # 0.1 is NOT representable in float32, so the un-quantised float64 value
    # differs from the XGBoost-quantised one, pinning the architecture branch.
    quantised = struct.unpack("f", struct.pack("f", 0.1))[0]
    row = build_feature_row({"x": 0.1}, ["x"], "lightgbm")
    assert row == [0.1]
    assert row != [quantised]


def test_build_feature_row_lightgbm_order_preserved() -> None:
    row = build_feature_row({"a": 1.0, "b": 2.0, "c": 3.0}, ["c", "a", "b"], "lightgbm")
    assert row == [3.0, 1.0, 2.0]


def test_build_feature_matrix_two_entries() -> None:
    matrix = build_feature_matrix(
        [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": 4.0}], ["a", "b"], "catboost"
    )
    assert matrix == [[1.0, 2.0], [3.0, 4.0]]


def test_assert_feature_count_ok() -> None:
    assert_feature_count(["a", "b"], 2)


def test_assert_feature_count_mismatch() -> None:
    with pytest.raises(ValueError, match="expected 3 features"):
        assert_feature_count(["a", "b"], 3)


def test_score_matrix_uses_injected_booster() -> None:
    scores = score_matrix(_FakeBooster(), [[1.0, 2.0], [3.0, 4.0]])
    assert scores == [3.0, 7.0]
