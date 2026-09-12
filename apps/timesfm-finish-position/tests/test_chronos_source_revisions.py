"""Source revision precedence is independent of whether a runner finished."""

from dataclasses import asdict

import pytest

from timesfm_finish_position.chronos_source_revisions import resolve_source_revisions


@pytest.fixture
def rows() -> list[dict[str, str]]:
    return [
        {
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0831",
            "keibajo_code": "83",
            "race_bango": "03",
            "umaban": "04",
            "ketto_toroku_bango": "2120250226",
            "bamei": " Horse　",
            "data_kubun": "2",
            "data_sakusei_nengappi": "20260831",
            "kakutei_chakujun": "01",
            "ijo_kubun_code": "0",
            "tansho_odds": "0033",
        },
        {
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0831",
            "keibajo_code": "83",
            "race_bango": "03",
            "umaban": "04",
            "ketto_toroku_bango": "2120260328",
            "bamei": "Horse",
            "data_kubun": "7",
            "data_sakusei_nengappi": "20260901",
            "kakutei_chakujun": "00",
            "ijo_kubun_code": "4",
            "tansho_odds": "0033",
        },
    ]


def test_final_nonfinisher_replaces_preliminary_winner(rows: list[dict[str, str]]) -> None:
    result = resolve_source_revisions(rows)
    assert asdict(result[0]) == {
        "key": ("2026", "0831", "83", "03", "04"),
        "superseded_registry": "2120250226",
        "current_registry": "2120260328",
        "name": "Horse",
        "old_created": "2026-08-31",
        "current_created": "2026-09-01",
        "reason": "preliminary_identity_superseded_by_final",
    }
    assert rows[0]["ketto_toroku_bango"] == "2120250226"


def test_matching_placeholder_correction(rows: list[dict[str, str]]) -> None:
    old = dict(rows[1], ketto_toroku_bango="0000000000", data_sakusei_nengappi="20260831")
    assert resolve_source_revisions([old, rows[1]])[0].reason == "placeholder_registry_corrected"
    assert resolve_source_revisions([rows[1]]) == []
    assert resolve_source_revisions([]) == []


@pytest.mark.parametrize("field,value", [("bamei", "Other"), ("data_sakusei_nengappi", "20260902")])
def test_conflicting_identity(rows: list[dict[str, str]], field: str, value: str) -> None:
    rows[0][field] = value
    with pytest.raises(ValueError, match="Conflicting"):
        resolve_source_revisions(rows)


def test_multiple_final_identities(rows: list[dict[str, str]]) -> None:
    rows[0]["data_kubun"] = "7"
    with pytest.raises(ValueError, match="Ambiguous"):
        resolve_source_revisions(rows)


def test_no_final_identity(rows: list[dict[str, str]]) -> None:
    rows[1]["data_kubun"] = "2"
    with pytest.raises(ValueError, match="Ambiguous"):
        resolve_source_revisions(rows)


def test_same_registry_cannot_be_silently_deduplicated(rows: list[dict[str, str]]) -> None:
    rows[0]["ketto_toroku_bango"] = "2120260328"
    with pytest.raises(ValueError, match="Conflicting"):
        resolve_source_revisions(rows)


def test_conflicting_placeholder_facts(rows: list[dict[str, str]]) -> None:
    rows[0]["ketto_toroku_bango"] = "0000000000"
    rows[0]["data_kubun"] = "7"
    with pytest.raises(ValueError, match="Unsupported"):
        resolve_source_revisions(rows)


def test_unsupported_revision_kind(rows: list[dict[str, str]]) -> None:
    rows[0]["data_kubun"] = "6"
    with pytest.raises(ValueError, match="Unsupported"):
        resolve_source_revisions(rows)


@pytest.mark.parametrize(
    "field,value",
    [
        ("bamei", ""),
        ("ketto_toroku_bango", "bad"),
        ("data_sakusei_nengappi", "bad"),
        ("data_sakusei_nengappi", "20260230"),
    ],
)
def test_invalid_source_fields(rows: list[dict[str, str]], field: str, value: str) -> None:
    rows[0][field] = value
    with pytest.raises(ValueError):
        resolve_source_revisions(rows)


def test_missing_field(rows: list[dict[str, str]]) -> None:
    del rows[0]["bamei"]
    with pytest.raises(ValueError, match="Missing"):
        resolve_source_revisions(rows)
