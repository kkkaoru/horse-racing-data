"""Independent structured extraction of official NAR result HTML."""

from dataclasses import dataclass
from decimal import Decimal
from html.parser import HTMLParser

from timesfm_finish_position.chronos_official_banei import OfficialRunner

BANEI_MAX_FIELD_SIZE: int = 10
NAR_MAX_FIELD_SIZE: int = 16
REQUIRED: frozenset[str] = frozenset({"a", "c", "d", "f", "o", "p"})
STATUS_CODES: dict[str, str] = {"取消": "1", "除外": "3", "中止": "4", "失格": "5"}


@dataclass(frozen=True)
class HtmlRunner:
    runner: OfficialRunner
    sex_age: str
    popularity: int | None


class _ResultTable(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[dict[str, str]] = []
        self.row: dict[str, str] | None = None
        self.column: str | None = None
        self.fragments: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        classes = (attributes.get("class") or "").split()
        if tag == "tr" and "tBorder" in classes:
            if self.row is not None:
                raise ValueError("Nested official result row")
            self.row = {}
        if tag == "td" and self.row is not None:
            if self.column is not None or not classes:
                raise ValueError("Ambiguous official result column")
            self.column = classes[0]
            self.fragments = []

    def handle_data(self, data: str) -> None:
        if self.column is not None:
            self.fragments.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self.row is not None and self.column is not None:
            if self.column in self.row:
                raise ValueError("Duplicate official result column")
            self.row[self.column] = " ".join("".join(self.fragments).split())
            self.column = None
        if tag == "tr" and self.row is not None:
            if self.column is not None:
                raise ValueError("Unclosed official result column")
            self.rows.append(self.row)
            self.row = None


def _parse_row(row: dict[str, str], *, max_field_size: int) -> HtmlRunner:
    if not REQUIRED.issubset(row) or not row["d"] or not row["f"]:
        raise ValueError("Incomplete official result row")
    status = row["a"]
    margin_status = row.get("l", "")
    if not status:
        status = margin_status
    elif margin_status in STATUS_CODES and status != margin_status:
        raise ValueError("Conflicting official finish columns")
    finish = int(status) if status.isascii() and status.isdigit() else None
    if finish is not None:
        if not 1 <= finish <= max_field_size:
            raise ValueError("Invalid official finish")
        code = "0"
    else:
        if status not in STATUS_CODES:
            raise ValueError("Unknown official finish status")
        code = STATUS_CODES[status]
    bib = int(row["c"])
    popularity = int(row["o"]) if row["o"] else None
    odds = Decimal(row["p"]) if row["p"] else None
    if not 1 <= bib <= max_field_size or (
        popularity is not None and not 1 <= popularity <= max_field_size
    ):
        raise ValueError("Invalid official bib or popularity")
    if odds is not None and (not odds.is_finite() or odds < 1):
        raise ValueError("Invalid official odds")
    return HtmlRunner(OfficialRunner(bib, row["d"], finish, code, odds), row["f"], popularity)


def _parse_html(text: str, *, max_field_size: int) -> tuple[HtmlRunner, ...]:
    parser = _ResultTable()
    parser.feed(text)
    parser.close()
    if parser.row is not None or not parser.rows:
        raise ValueError("Incomplete official result table")
    rows = tuple(_parse_row(row, max_field_size=max_field_size) for row in parser.rows)
    if len({row.runner.bib for row in rows}) != len(rows):
        raise ValueError("Duplicate official bib")
    return rows


def parse_official_banei_html(text: str) -> tuple[HtmlRunner, ...]:
    """Keep archival missing odds distinct from DNS; enforce Ban-ei's ten-horse bound."""
    return _parse_html(text, max_field_size=BANEI_MAX_FIELD_SIZE)


def parse_official_flat_html(text: str) -> tuple[HtmlRunner, ...]:
    """Extract NAR flat evidence without relaxing the separate Ban-ei bound."""
    return _parse_html(text, max_field_size=NAR_MAX_FIELD_SIZE)
