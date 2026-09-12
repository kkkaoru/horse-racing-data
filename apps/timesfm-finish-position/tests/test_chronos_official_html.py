"""HTML evidence is independent of flattened PDF column order."""

from decimal import Decimal

import pytest

from timesfm_finish_position.chronos_official_banei import OfficialRunner
from timesfm_finish_position.chronos_official_html import (
    HtmlRunner,
    parse_official_banei_html,
    parse_official_flat_html,
)


@pytest.fixture
def row() -> str:
    return (
        '<tr class="tBorder"><td class="a">1</td><td class="c">6</td>'
        '<td class="d horseName"><a> ジェイドルーリー </a></td>'
        '<td class="f">牝 4</td><td class="o">6</td><td class="p"></td></tr>'
    )


def test_archival_missing_odds_remains_starter(row: str) -> None:
    assert parse_official_banei_html("<table><tr><th>Header</th></tr>" + row + "</table>") == (
        HtmlRunner(OfficialRunner(6, "ジェイドルーリー", 1, "0", None), "牝 4", 6),
    )


def test_present_odds(row: str) -> None:
    assert parse_official_banei_html(row.replace('class="p"></td>', 'class="p">10.2</td>')) == (
        HtmlRunner(OfficialRunner(6, "ジェイドルーリー", 1, "0", Decimal("10.2")), "牝 4", 6),
    )


def test_explicit_cancel(row: str) -> None:
    html = row.replace('class="a">1', 'class="a">取消').replace('class="o">6', 'class="o">')
    assert parse_official_banei_html(html) == (
        HtmlRunner(OfficialRunner(6, "ジェイドルーリー", None, "1", None), "牝 4", None),
    )


def test_explicit_nonfinisher(row: str) -> None:
    assert parse_official_banei_html(row.replace('class="a">1', 'class="a">中止')) == (
        HtmlRunner(OfficialRunner(6, "ジェイドルーリー", None, "4", None), "牝 4", 6),
    )


@pytest.mark.parametrize(
    "old,new",
    [
        ('class="a">1', 'class="a">不明'),
        ('class="a">1', 'class="a">0'),
        ('class="c">6', 'class="c">11'),
        ('class="o">6', 'class="o">0'),
        ('class="p"></td>', 'class="p">NaN</td>'),
        ('class="p"></td>', 'class="p">0.9</td>'),
        ('class="f">牝 4', 'class="f">'),
        ('class="d horseName"', 'class="z"'),
        ('class="d horseName"', 'class="c"'),
        ('class="c"', ""),
    ],
)
def test_invalid_values(row: str, old: str, new: str) -> None:
    with pytest.raises(ValueError):
        parse_official_banei_html(row.replace(old, new))


def test_nonfinisher_status_in_margin_column(row: str) -> None:
    html = row.replace('class="a">1', 'class="a">').replace("</tr>", '<td class="l">中止</td></tr>')
    assert parse_official_banei_html(html) == (
        HtmlRunner(OfficialRunner(6, "ジェイドルーリー", None, "4", None), "牝 4", 6),
    )


def test_blank_finish_is_not_inferred_as_nonstarter(row: str) -> None:
    with pytest.raises(ValueError, match="Unknown official finish status"):
        parse_official_banei_html(row.replace('class="a">1', 'class="a">'))


def test_conflicting_finish_columns(row: str) -> None:
    with pytest.raises(ValueError, match="Conflicting official finish columns"):
        parse_official_banei_html(row.replace("</tr>", '<td class="l">中止</td></tr>'))


def test_flat_field_limit_is_separate_from_banei(row: str) -> None:
    html = (
        row.replace('class="a">1', 'class="a">14')
        .replace('class="c">6', 'class="c">14')
        .replace('class="o">6', 'class="o">14')
    )
    assert parse_official_flat_html(html) == (
        HtmlRunner(OfficialRunner(14, "ジェイドルーリー", 14, "0", None), "牝 4", 14),
    )
    with pytest.raises(ValueError, match="Invalid official finish"):
        parse_official_banei_html(html)


def test_flat_rejects_out_of_range_bib(row: str) -> None:
    with pytest.raises(ValueError, match="Invalid official bib"):
        parse_official_flat_html(row.replace('class="c">6', 'class="c">17'))


def test_duplicate_bib(row: str) -> None:
    with pytest.raises(ValueError, match="Duplicate official bib"):
        parse_official_banei_html(row + row)


@pytest.mark.parametrize(
    "html",
    [
        "",
        '<tr class="tBorder">',
        '<tr class="tBorder"><tr class="tBorder">',
        '<tr class="tBorder"><td class="a"><td class="c">',
        '<tr class="tBorder"><td class="a"></tr>',
    ],
)
def test_incomplete_or_nested_table(html: str) -> None:
    with pytest.raises(ValueError):
        parse_official_banei_html(html)
