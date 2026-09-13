"""Race context identifiers stay distinct from numeric measurements and missingness."""

import math

import pytest
from static_race_context import numeric_race_code


def test_venue_code_keeps_live_decimal_semantics() -> None:
    assert numeric_race_code(" 06 ") == 6.0


def test_class_code_keeps_live_decimal_semantics() -> None:
    assert numeric_race_code("999") == 999.0


def test_zero_code_is_not_automatically_missing() -> None:
    assert numeric_race_code("000") == 0.0


def test_foreign_code_has_separate_range() -> None:
    assert numeric_race_code("A8") == 1368.0


def test_lowercase_foreign_code_is_normalized() -> None:
    assert numeric_race_code("a8") == 1368.0


def test_none_is_not_zero_filled() -> None:
    assert math.isnan(numeric_race_code(None))


def test_blank_is_not_zero_filled() -> None:
    assert math.isnan(numeric_race_code(" "))


@pytest.mark.parametrize("value", ["?", "-1", "1.0", "1234", "\uff16"])
def test_malformed_codes_fail(value: str) -> None:
    with pytest.raises(ValueError, match="ASCII"):
        numeric_race_code(value)
