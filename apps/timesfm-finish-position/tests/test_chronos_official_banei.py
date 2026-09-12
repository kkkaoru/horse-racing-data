"""Provisional official PDF extraction keeps explicit nonfinishers."""

from decimal import Decimal

import pytest

from timesfm_finish_position.chronos_official_banei import (
    OfficialRace,
    OfficialRunner,
    parse_official_banei_pdf,
    parse_official_banei_table,
)


@pytest.fixture
def table() -> str:
    return "1 1 1 テスト 620.0 騎 手 2.18.1 ① 1.0 計 1 頭"


def test_normal_and_nonfinishers() -> None:
    text = (
        "1 1 1 テスト 620.0 騎手 2.18.1 2 2 ミカン 620.0 騎手 競走中止 "
        "3 3 リンゴ 620.0 騎手 出走取消 ① 1.0 ② 12.3 計 2 頭"
    )
    assert parse_official_banei_table(number=1, text=text) == OfficialRace(
        1,
        2,
        (
            OfficialRunner(1, "テスト", 1, "0", Decimal("1.0")),
            OfficialRunner(2, "ミカン", None, "4", Decimal("12.3")),
            OfficialRunner(3, "リンゴ", None, "1", None),
        ),
    )


def test_disqualification_and_exclusion() -> None:
    text = "1 1 テスト 620.0 騎手 失格 2 2 ミカン 620.0 騎手 競走除外 ① 5.0 計 1 頭"
    assert parse_official_banei_table(number=2, text=text) == OfficialRace(
        2,
        1,
        (
            OfficialRunner(1, "テスト", None, "5", Decimal("5.0")),
            OfficialRunner(2, "ミカン", None, "3", None),
        ),
    )


def test_dead_heat() -> None:
    text = "1 1 1 テスト 620.0 騎手 2.18.1 1 2 2 ミカン 620.0 騎手 2.18.1 ① 2.0 ② 3.0 計 2 頭"
    assert parse_official_banei_table(number=1, text=text).runners == (
        OfficialRunner(1, "テスト", 1, "0", Decimal("2.0")),
        OfficialRunner(2, "ミカン", 1, "0", Decimal("3.0")),
    )


@pytest.mark.parametrize(
    "text,error",
    [
        ("", "Missing official starter total"),
        ("計 1 頭", "Missing or duplicate"),
        ("1 1 1 テスト 620.0 騎手 2 1 1 ミカン 620.0 騎手 計 2 頭", "Missing or duplicate"),
        ("1 1 テスト 620.0 騎手 不明 計 1 頭", "Missing or ambiguous"),
        ("1 1 テスト 620.0 騎手 競走中止 出走取消 計 1 頭", "Missing or ambiguous"),
        ("1 1 1 テスト 620.0 騎手 競走中止 計 1 頭", "Conflicting classified"),
        ("1 1 1 テスト 620.0 騎手 ① 2.0 計 0 頭", "total disagrees"),
        ("1 1 1 テスト 620.0 騎手 ① 2.0 計 1 0 頭", "total disagrees"),
        ("1 1 1 テスト 620.0 騎手 計 1 頭", "Missing or invalid official win odds"),
        ("1 1 1 テスト 620.0 騎手 ① 0.0 計 1 頭", "Missing or invalid official win odds"),
    ],
)
def test_bad_tables(text: str, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        parse_official_banei_table(number=1, text=text)


def test_ten_starters_with_split_total() -> None:
    text = (
        "1 1 1 アカ 620.0 騎手 2 2 2 アオ 620.0 騎手 "
        "3 3 3 ミドリ 620.0 騎手 4 4 4 シロ 620.0 騎手 "
        "5 5 5 クロ 620.0 騎手 6 6 6 モモ 620.0 騎手 "
        "7 7 7 キイロ 620.0 騎手 8 7 8 チャイロ 620.0 騎手 "
        "9 8 9 キン 620.0 騎手 10 8 10 ギン 620.0 騎手 "
        "① 1.0 ② 2.0 ③ 3.0 ④ 4.0 ⑤ 5.0 ⑥ 6.0 ⑦ 7.0 ⑧ 8.0 ⑨ 9.0 ⑩ 10.0 計 1 0 頭"
    )
    race = parse_official_banei_table(number=10, text=text)
    assert race.starter_count == 10
    assert len(race.runners) == 10
    assert race.runners[-1] == OfficialRunner(10, "ギン", 10, "0", Decimal("10.0"))


def test_invalid_race_number(table: str) -> None:
    with pytest.raises(ValueError, match="Invalid official race number"):
        parse_official_banei_table(number=0, text=table)


def test_incomplete_document(table: str) -> None:
    with pytest.raises(ValueError, match="twelve ordered"):
        parse_official_banei_pdf("第 1 1 競走 " + table)


def test_complete_document(table: str) -> None:
    text = (
        "第 1 1 競走 "
        + table
        + "第 2 2 競走 "
        + table
        + "第 3 3 競走 "
        + table
        + "第 4 4 競走 "
        + table
        + "第 5 5 競走 "
        + table
        + "第 6 6 競走 "
        + table
        + "第 7 7 競走 "
        + table
        + "第 8 8 競走 "
        + table
        + "第 9 9 競走 "
        + table
        + "第10 10競走 "
        + table
        + "第1111競走 "
        + table
        + "第12 12競走 "
        + table
    )
    races = parse_official_banei_pdf(text)
    assert len(races) == 12
    assert races[-1] == OfficialRace(12, 1, (OfficialRunner(1, "テスト", 1, "0", Decimal("1.0")),))
