"""Result overlays cannot silently change horse identities or source stages."""

import pytest

from timesfm_finish_position.chronos_source_overlay import apply_source_overlay


def test_explicit_dnf_keeps_identity_and_source_version() -> None:
    source = {
        "ketto_toroku_bango": "2120240001",
        "data_kubun": "2",
        "kakutei_chakujun": "00",
        "ijo_kubun_code": "0",
        "tansho_odds": "0000",
    }
    assert apply_source_overlay(
        source, expected=source, replacement={"ijo_kubun_code": "4", "tansho_odds": "0102"}
    ) == {
        "ketto_toroku_bango": "2120240001",
        "data_kubun": "2",
        "kakutei_chakujun": "00",
        "ijo_kubun_code": "4",
        "tansho_odds": "0102",
    }
    assert source["ijo_kubun_code"] == "0"


def test_explicit_cancellation() -> None:
    assert apply_source_overlay(
        {"ijo_kubun_code": "0"},
        expected={"ijo_kubun_code": "0"},
        replacement={"ijo_kubun_code": "1"},
    ) == {"ijo_kubun_code": "1"}


def test_changed_snapshot() -> None:
    with pytest.raises(ValueError, match="snapshot changed"):
        apply_source_overlay(
            {"ijo_kubun_code": "4"},
            expected={"ijo_kubun_code": "0"},
            replacement={"ijo_kubun_code": "1"},
        )


@pytest.mark.parametrize(
    "replacement", [{}, {"ketto_toroku_bango": "2120240002"}, {"data_kubun": "7"}]
)
def test_protected_fields(replacement: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        apply_source_overlay({}, expected={}, replacement=replacement)


def test_cannot_add_column() -> None:
    with pytest.raises(ValueError, match="cannot add"):
        apply_source_overlay({}, expected={}, replacement={"shusso_tosu": "10"})


def test_invalid_code() -> None:
    with pytest.raises(ValueError, match="Invalid official overlay value"):
        apply_source_overlay(
            {"ijo_kubun_code": "0"},
            expected={"ijo_kubun_code": "0"},
            replacement={"ijo_kubun_code": "9"},
        )


def test_official_denominator() -> None:
    assert apply_source_overlay(
        {"shusso_tosu": "00", "nyusen_tosu": "00", "extra": None},
        expected={"shusso_tosu": "00", "nyusen_tosu": "00", "extra": None},
        replacement={"shusso_tosu": "10", "nyusen_tosu": "09"},
    ) == {
        "shusso_tosu": "10",
        "nyusen_tosu": "09",
        "extra": None,
    }
