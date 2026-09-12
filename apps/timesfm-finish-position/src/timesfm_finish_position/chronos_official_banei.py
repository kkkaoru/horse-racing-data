"""Parse the bounded runner table of a PDFKit-extracted official Ban-ei PDF.

This extracts evidence, not vendor identities. Callers must independently match
all bibs/names and verify odds alignment before applying any source overlay.
"""

import re
from dataclasses import dataclass
from decimal import Decimal

HEADER = re.compile(r"第\s*(?P<number>1[0-2]|[1-9])\s*(?P=number)\s*競走")
TOTAL = re.compile(r"計\s*([0-9][0-9\s]*)頭")
RUNNER = re.compile(
    r"(?<![\d.])(?:(?P<finish>1[0-2]|[1-9])\s+)?"
    r"(?P<frame>[1-8])\s+(?P<bib>10|[1-9])\s+"
    r"(?P<name>[^\s\d]+)\s+\d{3}\.\d\s+"
)
ODDS = re.compile(r"[①②③④⑤⑥⑦⑧⑨⑩]\s+(\d+\.\d+)")
STATUS = re.compile(r"出走取消|競走除外|競走中止|失格")
STATUS_CODES: dict[str, str] = {"出走取消": "1", "競走除外": "3", "競走中止": "4", "失格": "5"}
NONSTARTER_CODES: frozenset[str] = frozenset({"1", "3"})


@dataclass(frozen=True)
class OfficialRunner:
    bib: int
    name: str
    finish: int | None
    abnormal_code: str
    win_odds: Decimal | None


@dataclass(frozen=True)
class OfficialRace:
    number: int
    starter_count: int
    runners: tuple[OfficialRunner, ...]


def _runner(match: re.Match[str], following: str) -> OfficialRunner:
    finish = match.group("finish")
    statuses = STATUS.findall(following)
    if finish is not None:
        if statuses:
            raise ValueError("Conflicting classified finish and abnormal status")
        code = "0"
    else:
        if len(statuses) != 1:
            raise ValueError("Missing or ambiguous unclassified runner status")
        code = STATUS_CODES[statuses[0]]
    return OfficialRunner(
        int(match.group("bib")),
        match.group("name"),
        None if finish is None else int(finish),
        code,
        None,
    )


def _with_odds(runners: list[OfficialRunner], odds: list[Decimal]) -> tuple[OfficialRunner, ...]:
    prices = iter(odds)
    result: list[OfficialRunner] = []
    for runner in runners:
        price = None if runner.abnormal_code in NONSTARTER_CODES else next(prices)
        result.append(
            OfficialRunner(runner.bib, runner.name, runner.finish, runner.abnormal_code, price)
        )
    return tuple(result)


def parse_official_banei_table(*, number: int, text: str) -> OfficialRace:
    """Extract a single provisional table, rejecting incomplete roster evidence."""
    if not 1 <= number <= 12:
        raise ValueError("Invalid official race number")
    total = TOTAL.search(text)
    if total is None:
        raise ValueError("Missing official starter total")
    count = int("".join(total.group(1).split()))
    table = text[: total.start()]
    matches = list(RUNNER.finditer(table))
    runners: list[OfficialRunner] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(table)
        runners.append(_runner(match, table[match.end() : end]))
    if not runners or len({r.bib for r in runners}) != len(runners):
        raise ValueError("Missing or duplicate official runner")
    actual_count = sum(r.abnormal_code not in NONSTARTER_CODES for r in runners)
    if count < 1 or actual_count != count:
        raise ValueError("Official starter total disagrees with parsed roster")
    odds = [Decimal(match.group(1)) for match in ODDS.finditer(table)]
    if len(odds) != count or any(price < 1 for price in odds):
        raise ValueError("Missing or invalid official win odds")
    return OfficialRace(number, count, _with_odds(runners, odds))


def parse_official_banei_pdf(text: str) -> tuple[OfficialRace, ...]:
    """Require a complete 12-race document; never infer DNS from absent odds.

    Values remain provisional until independently cross-checked against the
    official HTML roster and the original source identity records.
    """
    headers = list(HEADER.finditer(text))
    if [int(match.group("number")) for match in headers] != list(range(1, 13)):
        raise ValueError("Expected all twelve ordered official race headers")
    races: list[OfficialRace] = []
    for index, header in enumerate(headers):
        end = headers[index + 1].start() if index + 1 < len(headers) else len(text)
        races.append(
            parse_official_banei_table(
                number=int(header.group("number")), text=text[header.end() : end]
            )
        )
    return tuple(races)
