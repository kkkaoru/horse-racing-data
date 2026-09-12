"""Validation of policy evaluation row boundaries."""

import pytest

from timesfm_finish_position.policy_report import require_category, require_float, require_int


def test_category_accepts_jra() -> None:
    assert require_category("jra") == "jra"


def test_category_accepts_nar() -> None:
    assert require_category("nar") == "nar"


def test_category_accepts_banei() -> None:
    assert require_category("ban-ei") == "ban-ei"


def test_category_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="Unsupported prediction category"):
        require_category("unknown")


def test_float_accepts_numeric_text() -> None:
    assert require_float("1.5") == 1.5


def test_float_accepts_number() -> None:
    assert require_float(2) == 2.0


@pytest.mark.parametrize("value", [None, True, {}, "not-numeric"])
def test_float_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValueError):
        require_float(value)


def test_int_accepts_numeric_text() -> None:
    assert require_int("2") == 2


def test_int_preserves_truncation() -> None:
    assert require_int(2.9) == 2


@pytest.mark.parametrize("value", [None, True, {}, "2.9"])
def test_int_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValueError):
        require_int(value)
