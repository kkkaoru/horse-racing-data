"""Load trainer/owner records from a local keiba data_sources archive.

Published JRA and netkeiba names are folded (NFKC, no affiliation, no
spaces) and matched to archive filenames. Missing files stay unresolved
so the scorer can keep its netkeiba person history.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from urllib.error import URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from overseas_finish_features import (
    PersonSummary,
    prize_points_for_start,
    SHOW_FINISH_POSITION,
    WIN_FINISH_POSITION,
)

JRA_TRAINER_PATTERN: re.Pattern[str] = re.compile(
    r'class="trainer"[^>]*>([\s\S]*?)</p>',
    re.IGNORECASE,
)
JRA_OWNER_PATTERN: re.Pattern[str] = re.compile(
    r'class="owner"[^>]*>([\s\S]*?)</p>',
    re.IGNORECASE,
)
NETKEIBA_TRAINER_TITLE_PATTERN: re.Pattern[str] = re.compile(
    r"/trainer/result/recent/[^\"']+[\"'][^>]*title=\"([^\"]+)\"",
    re.IGNORECASE,
)
NETKEIBA_OWNER_TITLE_PATTERN: re.Pattern[str] = re.compile(
    r"/owner/(?:result/recent/)?[^\"']+[\"'][^>]*title=\"([^\"]+)\"",
    re.IGNORECASE,
)
NETKEIBA_OWNER_TEXT_PATTERN: re.Pattern[str] = re.compile(
    r"<a[^>]+href=[\"'][^\"']*/owner/(?:result/recent/)?[^\"'/]+/?[\"'][^>]*>([\s\S]*?)</a>",
    re.IGNORECASE,
)
TAG_PATTERN: re.Pattern[str] = re.compile(r"<[^>]+>")
AFFILIATION_PATTERN: re.Pattern[str] = re.compile(r"（[^）]+）|\([^)]+\)")
DATE_PATTERN: re.Pattern[str] = re.compile(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})")
FINISH_HEADER_PATTERN: re.Pattern[str] = re.compile(r"着順")
DATE_HEADER_PATTERN: re.Pattern[str] = re.compile(r"年月日|日付")
NAME_HEADER_PATTERN: re.Pattern[str] = re.compile(r"レース名")
PRIZE_HEADER_PATTERN: re.Pattern[str] = re.compile(r"賞金")
TRAINER_KIND: str = "trainer"
OWNER_KIND: str = "owner"
NETKEIBA_GENERIC_LINK_LABELS: frozenset[str] = frozenset(
    {
        "owner",
        "trainer",
        "近走成績",
        "馬主",
        "調教師",
    }
)
BURKE_ARCHIVE_STEMS: tuple[str, ...] = ("K.バーク", "Karl Richard Burke")
FERLAND_ARCHIVE_STEMS: tuple[str, ...] = ("Christophe Ferland", "C.フェルラン")
GRAFFARD_ARCHIVE_STEMS: tuple[str, ...] = ("F.グラファール", "Francis Henri Graffard")
OBRIEN_ARCHIVE_STEMS: tuple[str, ...] = (
    "A.オブライエン",
    "Aidan O'Brien",
    "Aidan Patrick O'Brien",
)
COOLMORE_ARCHIVE_STEMS: tuple[str, ...] = (
    "Westerberg_Mrs J Magnier_M Tabor_D Smith",
    "Michael Tabor & Derrick Smith & Mrs John Magnier",
)
AGA_KHAN_ARCHIVE_STEMS: tuple[str, ...] = ("Aga Khan Studs SCEA",)
WERTHEIMER_ARCHIVE_STEMS: tuple[str, ...] = ("Wertheimer & Frere",)

# JRA official + netkeiba short labels -> archive filename stems.
ARCHIVE_NAME_ALIASES: dict[str, tuple[str, ...]] = {
    "k.バーク": BURKE_ARCHIVE_STEMS,
    "バーク": BURKE_ARCHIVE_STEMS,
    "c.フェルラン": FERLAND_ARCHIVE_STEMS,
    "フェルラン": FERLAND_ARCHIVE_STEMS,
    "f.グラファール": GRAFFARD_ARCHIVE_STEMS,
    "グラファール": GRAFFARD_ARCHIVE_STEMS,
    "a.オブライエン": OBRIEN_ARCHIVE_STEMS,
    "オブライエン": OBRIEN_ARCHIVE_STEMS,
    "田中博康": ("田中博康",),
    "武井亮": ("武井亮",),
    "キャロットファーム": ("キャロットファーム",),
    "キャロット": ("キャロットファーム",),
    "wertheimer&frere": WERTHEIMER_ARCHIVE_STEMS,
    "ウェルトハイマーエフレール": WERTHEIMER_ARCHIVE_STEMS,
    "ウェルトハイマー": WERTHEIMER_ARCHIVE_STEMS,
    "agakhanstudssc": AGA_KHAN_ARCHIVE_STEMS,
    "agakhanstudsscea": AGA_KHAN_ARCHIVE_STEMS,
    "アーガーカーンスタッズsc": AGA_KHAN_ARCHIVE_STEMS,
    "アーガーカーン": AGA_KHAN_ARCHIVE_STEMS,
    "derricksmith,etal.": COOLMORE_ARCHIVE_STEMS,
    "derricksmith,etal": COOLMORE_ARCHIVE_STEMS,
    "d.スミス": COOLMORE_ARCHIVE_STEMS,
    "westerbergmrsjmagniermtabordsmith": COOLMORE_ARCHIVE_STEMS,
    "westerberg_mrsjmagnier_mtabor_dsmith": COOLMORE_ARCHIVE_STEMS,
    "s.ワッテル": ("S.ワッテル",),
    "ワッテル": ("S.ワッテル",),
    "w.ハガス": ("W.ハガス",),
    "ハガス": ("W.ハガス",),
    "p.ヴァルディヴィエルソ": ("P.ヴァルディヴィエルソ",),
    "p.バルディビエルソ": ("P.ヴァルディヴィエルソ",),
    "バルディビエルソ": ("P.ヴァルディヴィエルソ",),
    "ヴァルディヴィエルソ": ("P.ヴァルディヴィエルソ",),
    "j.スタック": ("J.スタック",),
    "スタック": ("J.スタック",),
    "exorsofthelatesmobaid": ("EXORS OF THE LATE S M OBAID",),
    "exorsofthelatesheikhmohammedobaid": ("EXORS OF THE LATE S M OBAID",),
    "ph.betts": ("PH.BETTS",),
    "phbetts": ("PH.BETTS",),
    "patrickhughbetts": ("PH.BETTS",),
    "saeedsuhail": ("SAEED SUHAIL",),
    "yeguadacenturionslu": ("Yeguada Centurion SLU",),
    "caytonparkstud/mmej.ma": ("Cayton Park Stud & Mrs John Magnier",),
    "caytonparkstud&mrsjohnmagnier": ("Cayton Park Stud & Mrs John Magnier",),
}
JRA_JSON_RUNNERS_KEY: str = "runners"
JRA_JSON_HORSE_NUMBER_KEY: str = "horse_number"
JRA_JSON_TRAINER_KEY: str = "trainer"
JRA_JSON_OWNER_KEY: str = "owner"
JRA_JSON_PAST_RUNS_KEY: str = "past_runs"
JRA_JSON_DATE_KEY: str = "date"
JRA_JSON_RACE_NAME_KEY: str = "race_name"
JRA_JSON_FINISH_KEY: str = "finish_position"
JRA_JSON_GRADE_KEY: str = "grade"
HTML_TEXT_ENCODINGS: tuple[str, ...] = ("utf-8", "cp932", "euc-jp")
TABLE_ROW_PATTERN: re.Pattern[str] = re.compile(r"<tr\b[^>]*>([\s\S]*?)</tr>", re.IGNORECASE)
TABLE_CELL_PATTERN: re.Pattern[str] = re.compile(r"<td\b[^>]*>([\s\S]*?)</td>", re.IGNORECASE)
TABLE_HEADER_CELL_PATTERN: re.Pattern[str] = re.compile(r"<th\b[^>]*>([\s\S]*?)</th>", re.IGNORECASE)
NETKEIBA_RESULT_TABLE_MARKER: str = 'summary="年度別成績"'
NETKEIBA_FINISH_HEADER_PATTERN: re.Pattern[str] = re.compile(r"着\s*順")
JRAVAN_DATE_HEADER_PATTERN: re.Pattern[str] = re.compile(r"開催日")
JRAVAN_RESULT_SECTION_PATTERN: re.Pattern[str] = re.compile(
    r'id="horse--result"[^>]*>([\s\S]*?)<div\s+id="horse--long"',
    re.IGNORECASE,
)
ARCHIVE_CSV_HEADERS: tuple[str, ...] = ("年月日", "レース名", "着順", "賞金(万円)")
HTTP_URL_PREFIXES: tuple[str, ...] = ("http://", "https://")
HTML_FETCH_USER_AGENT: str = "pc-keiba-viewer-overseas-fp/1.0"
HTML_FETCH_TIMEOUT_SECONDS: int = 30
SOURCE_URL_TRACKING_QUERY_KEYS: frozenset[str] = frozenset({"rf"})
SOURCE_CACHE_FILE_SUFFIX: str = ".html"
SOURCE_COMBO_FILE_SUFFIX: str = ".txt"
SOURCE_CACHE_COMBO_DIRNAME: str = "combos"
OVERSEAS_TRAIN_SCRIPT: str = "src/scripts/train_and_predict_finish_position_overseas.py"
JRA_ACCESS_SD_HREF_PATTERN: re.Pattern[str] = re.compile(
    r'href=["\']([^"\']*accessSD\.html\?CNAME=pk01dde[^"\']+)["\']',
    re.IGNORECASE,
)
JRA_ORIGIN: str = "https://www.jra.go.jp"
NETKEIBA_HORSE_URL_TEMPLATE: str = "https://db.netkeiba.com/horse/{horse_id}"
JRAVAN_TRAINER_PATTERN: re.Pattern[str] = re.compile(
    r'class="raceTable__details__line__item--horse__info">\s*([^<]+)<br',
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class PublishedPersonNames:
    trainers: tuple[str, ...]
    owners: tuple[str, ...]
    trainer_extra_stems: tuple[tuple[str, ...], ...] = ()
    owner_extra_stems: tuple[tuple[str, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class ArchiveStart:
    raced: date
    race_name: str
    finish: int | None
    prize_man_yen: float | None = None


@dataclass(frozen=True, slots=True)
class PlannedOverseasCard:
    slug: str
    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str
    race_name: str
    venue_name: str
    aliases: tuple[str, ...] = ()
    jra_race_page_url: str = ""
    jra_card_url: str = ""
    netkeiba_card_url: str = ""
    jravan_card_url: str = ""


@dataclass(frozen=True, slots=True)
class OverseasPrepareStatus:
    card: PlannedOverseasCard
    on_sale_list: bool
    jra_card_url: str
    netkeiba_card_url: str
    jravan_card_url: str
    next_command: str


@dataclass(frozen=True, slots=True)
class OverseasSourceUrls:
    jra_url: str = ""
    netkeiba_url: str = ""
    jravan_url: str = ""


JRA_OVERSEAS_SALE_LIST_URL_TEMPLATE: str = "https://www.jra.go.jp/keiba/overseas/racelist/{year}.html"
JRA_OVERSEAS_SALE_DATE_PATTERN: re.Pattern[str] = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
JRA_OVERSEAS_SALE_PAGE_PATTERN: re.Pattern[str] = re.compile(
    r'href=["\'](/keiba/overseas/race/[^"\']+)["\'][^>]*>([\s\S]*?)</a>',
    re.IGNORECASE,
)
PLANNED_OVERSEAS_CARDS: tuple[PlannedOverseasCard, ...] = (
    PlannedOverseasCard(
        slug="jacques-le-marois-2026",
        kaisai_nen="2026",
        kaisai_tsukihi="0816",
        keibajo_code="A8",
        race_bango="04",
        race_name="ジャックルマロワ賞",
        venue_name="ドーヴィル",
        aliases=("jlm", "ジャックルマロワ", "jacques-le-marois"),
        jra_race_page_url="https://www.jra.go.jp/keiba/overseas/race/2026jlm/index.html",
        jra_card_url=(
            "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
        ),
        netkeiba_card_url=(
            "https://race.netkeiba.com/race/shutuba_abroad.html?race_id=2026C4010104&rf=race_submenu"
        ),
        jravan_card_url="https://world.jra-van.jp/race/jacqueslemarois/2026/racecard/",
    ),
    PlannedOverseasCard(
        slug="international-stakes-2026",
        kaisai_nen="2026",
        kaisai_tsukihi="0819",
        keibajo_code="A6",
        race_bango="04",
        race_name="インターナショナルステークス",
        venue_name="ヨーク",
        aliases=("intl", "international-stakes", "インターナショナルs", "英インターナショナル"),
        jra_race_page_url="",
        jra_card_url="",
        netkeiba_card_url="",
    ),
)


def fold_person_name(name: str) -> str:
    """NFKC fold used to compare JRA, netkeiba, and archive filenames."""
    folded = unicodedata.normalize("NFKC", name)
    folded = AFFILIATION_PATTERN.sub("", folded)
    folded = folded.replace("．", ".").replace("・", "").replace(" ", "").replace("　", "")
    return folded.casefold()


def _clean_html_text(value: str) -> str:
    text = TAG_PATTERN.sub(" ", value)
    text = text.replace("&amp;", "&").replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_jra_trainer_owner_names(html: str) -> PublishedPersonNames:
    trainers = tuple(_clean_html_text(match.group(1)) for match in JRA_TRAINER_PATTERN.finditer(html))
    owners = tuple(_clean_html_text(match.group(1)) for match in JRA_OWNER_PATTERN.finditer(html))
    return PublishedPersonNames(trainers=trainers, owners=owners)


def _is_generic_netkeiba_label(value: str) -> bool:
    return value.strip().casefold() in NETKEIBA_GENERIC_LINK_LABELS


def _non_generic_labels(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(value for value in values if value != "" and not _is_generic_netkeiba_label(value))


def parse_netkeiba_trainer_names(html: str) -> tuple[str, ...]:
    return _non_generic_labels(
        _clean_html_text(match.group(1)) for match in NETKEIBA_TRAINER_TITLE_PATTERN.finditer(html)
    )


def parse_jravan_racecard_trainers(html: str) -> tuple[str, ...]:
    """Trainer labels from a JRA-VAN World racecard horse-info cell."""
    names = (_clean_html_text(match.group(1)) for match in JRAVAN_TRAINER_PATTERN.finditer(html))
    return tuple(name for name in names if name != "")


def parse_netkeiba_owner_names(html: str) -> tuple[str, ...]:
    """Read /owner/ title= first, then link text when titles are missing."""
    titled = _non_generic_labels(
        _clean_html_text(match.group(1)) for match in NETKEIBA_OWNER_TITLE_PATTERN.finditer(html)
    )
    if titled:
        return titled
    return _non_generic_labels(
        _clean_html_text(match.group(1)) for match in NETKEIBA_OWNER_TEXT_PATTERN.finditer(html)
    )


def _empty_extra_stems(names: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    return ((),) * len(names)


def _slot_name_and_extra(jra_name: str, netkeiba_name: str) -> tuple[str, tuple[str, ...]]:
    if jra_name != "":
        if netkeiba_name != "" and fold_person_name(netkeiba_name) != fold_person_name(jra_name):
            return jra_name, (netkeiba_name,)
        return jra_name, ()
    return netkeiba_name, ()


def _merge_source_names(
    jra_names: tuple[str, ...],
    netkeiba_names: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[tuple[str, ...], ...]]:
    if not jra_names:
        return netkeiba_names, _empty_extra_stems(netkeiba_names)
    if not netkeiba_names:
        return jra_names, _empty_extra_stems(jra_names)
    length = max(len(jra_names), len(netkeiba_names))
    slots = [
        _slot_name_and_extra(
            jra_names[index] if index < len(jra_names) else "",
            netkeiba_names[index] if index < len(netkeiba_names) else "",
        )
        for index in range(length)
    ]
    return tuple(slot[0] for slot in slots), tuple(slot[1] for slot in slots)


def optimize_published_names(*, jra_html: str, netkeiba_html: str) -> PublishedPersonNames:
    """Prefer official JRA labels and keep netkeiba shorts as extra alias stems."""
    jra = parse_jra_trainer_owner_names(jra_html)
    trainers, trainer_extra_stems = _merge_source_names(
        jra.trainers,
        parse_netkeiba_trainer_names(netkeiba_html),
    )
    owners, owner_extra_stems = _merge_source_names(
        jra.owners,
        parse_netkeiba_owner_names(netkeiba_html),
    )
    return PublishedPersonNames(
        trainers=trainers,
        owners=owners,
        trainer_extra_stems=trainer_extra_stems,
        owner_extra_stems=owner_extra_stems,
    )


def _json_horse_number(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 1:
        return value
    if isinstance(value, str) and value.isdigit():
        parsed = int(value)
        if parsed >= 1:
            return parsed
    return None


def _json_person_name(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def parse_jra_card_json(payload: object) -> PublishedPersonNames:
    """Read trainer/owner names from official JRA card JSON, keyed by horse_number."""
    if not isinstance(payload, dict):
        return PublishedPersonNames(trainers=(), owners=())
    runners = payload.get(JRA_JSON_RUNNERS_KEY)
    if not isinstance(runners, list):
        return PublishedPersonNames(trainers=(), owners=())
    slots: dict[int, tuple[str, str]] = {}
    for runner in runners:
        if not isinstance(runner, dict):
            continue
        umaban = _json_horse_number(runner.get(JRA_JSON_HORSE_NUMBER_KEY))
        if umaban is None:
            continue
        slots[umaban] = (
            _json_person_name(runner.get(JRA_JSON_TRAINER_KEY)),
            _json_person_name(runner.get(JRA_JSON_OWNER_KEY)),
        )
    if not slots:
        return PublishedPersonNames(trainers=(), owners=())
    size = max(slots)
    trainers = tuple(slots[index][0] if index in slots else "" for index in range(1, size + 1))
    owners = tuple(slots[index][1] if index in slots else "" for index in range(1, size + 1))
    return PublishedPersonNames(
        trainers=trainers,
        owners=owners,
        trainer_extra_stems=_empty_extra_stems(trainers),
        owner_extra_stems=_empty_extra_stems(owners),
    )


def parse_netkeiba_horse_page_owner(html: str) -> str:
    """Owner name from a netkeiba horse detail page `/owner/` title."""
    names = parse_netkeiba_owner_names(html)
    if not names:
        return ""
    return names[0]


def read_html_text(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in HTML_TEXT_ENCODINGS:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def load_netkeiba_horse_page_owners(directory: Path) -> dict[str, str]:
    """Map `{source_horse_id}.html` stems to owner names on those pages."""
    if not directory.is_dir():
        return {}
    owners: dict[str, str] = {}
    for path in directory.iterdir():
        if path.suffix.lower() != ".html":
            continue
        name = parse_netkeiba_horse_page_owner(read_html_text(path))
        if name == "":
            continue
        owners[path.stem] = name
    return owners


def attach_owner_stems_from_horse_pages(
    published: PublishedPersonNames,
    *,
    owner_by_umaban: dict[int, str],
) -> PublishedPersonNames:
    """Keep JRA/JSON labels; add horse-page owner names as same-umaban aliases."""
    owners = list(published.owners)
    extras = list(published.owner_extra_stems)
    if owner_by_umaban:
        size = max(len(owners), max(owner_by_umaban))
        while len(owners) < size:
            owners.append("")
        while len(extras) < size:
            extras.append(())
    for umaban, name in owner_by_umaban.items():
        if umaban < 1 or umaban > len(owners) or name == "":
            continue
        index = umaban - 1
        current = owners[index]
        extra = list(extras[index])
        if current == "":
            owners[index] = name
        elif fold_person_name(name) != fold_person_name(current):
            extra.append(name)
        extras[index] = tuple(extra)
    return PublishedPersonNames(
        trainers=published.trainers,
        owners=tuple(owners),
        trainer_extra_stems=published.trainer_extra_stems,
        owner_extra_stems=tuple(extras),
    )


def attach_trainer_stems_from_jravan(
    published: PublishedPersonNames,
    jravan_trainers: Sequence[str],
) -> PublishedPersonNames:
    """Keep JRA/netkeiba trainer labels; add JRA-VAN names as same-index aliases."""
    trainers = list(published.trainers)
    extras = list(published.trainer_extra_stems)
    if jravan_trainers:
        size = max(len(trainers), len(jravan_trainers))
        while len(trainers) < size:
            trainers.append("")
        while len(extras) < size:
            extras.append(())
    for index, name in enumerate(jravan_trainers):
        if name == "":
            continue
        current = trainers[index]
        extra = list(extras[index])
        if current == "":
            trainers[index] = name
        elif fold_person_name(name) != fold_person_name(current):
            extra.append(name)
        extras[index] = tuple(extra)
    return PublishedPersonNames(
        trainers=tuple(trainers),
        owners=published.owners,
        trainer_extra_stems=tuple(extras),
        owner_extra_stems=published.owner_extra_stems,
    )


def _candidate_stems(published_name: str, extra_stems: Sequence[str] = ()) -> tuple[str, ...]:
    seeds = tuple(stem for stem in (published_name, *extra_stems) if stem != "")
    aliases: list[str] = []
    for seed in seeds:
        aliases.extend(ARCHIVE_NAME_ALIASES.get(fold_person_name(seed), ()))
    return (*seeds, *aliases)


def resolve_archive_file(
    *,
    kind: str,
    published_name: str,
    archive_root: Path,
    extra_stems: Sequence[str] = (),
) -> Path | None:
    directory = archive_root / f"{kind}s"
    if not directory.is_dir():
        return None
    wanted = {fold_person_name(stem) for stem in _candidate_stems(published_name, extra_stems)}
    csv_files = [path for path in directory.iterdir() if path.suffix.lower() == ".csv"]
    matches = [path for path in csv_files if fold_person_name(path.stem) in wanted]
    if not matches:
        return None
    return sorted(matches, key=lambda path: path.name)[0]


def parse_archive_date(value: str) -> date | None:
    matched = DATE_PATTERN.search(value)
    if matched is None:
        return None
    try:
        return date(int(matched.group(1)), int(matched.group(2)), int(matched.group(3)))
    except ValueError:
        return None


def _header_index(headers: list[str], pattern: re.Pattern[str]) -> int | None:
    for index, header in enumerate(headers):
        if pattern.search(header) is not None:
            return index
    return None


def load_archive_person_summary(
    path: Path,
    *,
    before: date,
) -> PersonSummary:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    if not rows:
        return PersonSummary(
            starts=0,
            wins=0,
            shows=0,
            win_rate=None,
            show_rate=None,
            prize_points=0.0,
            prize_per_start=None,
        )
    headers = rows[0]
    finish_index = _header_index(headers, FINISH_HEADER_PATTERN)
    date_index = _header_index(headers, DATE_HEADER_PATTERN)
    name_index = _header_index(headers, NAME_HEADER_PATTERN)
    prize_index = _header_index(headers, PRIZE_HEADER_PATTERN)
    if finish_index is None or date_index is None:
        return PersonSummary(
            starts=0,
            wins=0,
            shows=0,
            win_rate=None,
            show_rate=None,
            prize_points=0.0,
            prize_per_start=None,
        )
    starts = 0
    wins = 0
    shows = 0
    prize_points = 0.0
    for row in rows[1:]:
        if max(finish_index, date_index) >= len(row):
            continue
        raced = parse_archive_date(row[date_index])
        if raced is None or raced >= before:
            continue
        finish_text = row[finish_index].strip()
        try:
            finish = int(finish_text)
        except ValueError:
            finish = None
        race_name = row[name_index] if name_index is not None and name_index < len(row) else ""
        money = 0.0
        if prize_index is not None and prize_index < len(row) and row[prize_index].strip() != "":
            try:
                money = float(row[prize_index].replace(",", ""))
            except ValueError:
                money = 0.0
        starts += 1
        if finish == WIN_FINISH_POSITION:
            wins += 1
        if finish is not None and finish <= SHOW_FINISH_POSITION:
            shows += 1
        if money > 0.0:
            prize_points += money
        else:
            prize_points += prize_points_for_start(finish_position=finish, race_name=race_name)
    if starts == 0:
        return PersonSummary(
            starts=0,
            wins=0,
            shows=0,
            win_rate=None,
            show_rate=None,
            prize_points=0.0,
            prize_per_start=None,
        )
    return PersonSummary(
        starts=starts,
        wins=wins,
        shows=shows,
        win_rate=wins / float(starts),
        show_rate=shows / float(starts),
        prize_points=prize_points,
        prize_per_start=prize_points / float(starts),
    )


def prefer_richer_person(left: PersonSummary, right: PersonSummary) -> PersonSummary:
    """Keep the sample with more starts (archive vs netkeiba)."""
    if left.starts >= right.starts:
        return left
    return right


def load_archive_people_by_umaban(
    *,
    kind: str,
    published_names: Sequence[str],
    archive_root: Path,
    before: date,
    extra_stems: Sequence[Sequence[str]] = (),
) -> dict[int, PersonSummary]:
    summaries: dict[int, PersonSummary] = {}
    for index, published in enumerate(published_names, start=1):
        extra = extra_stems[index - 1] if index - 1 < len(extra_stems) else ()
        path = resolve_archive_file(
            kind=kind,
            published_name=published,
            archive_root=archive_root,
            extra_stems=extra,
        )
        if path is None:
            continue
        summaries[index] = load_archive_person_summary(path, before=before)
    return summaries


def _optional_finish(value: str) -> int | None:
    text = value.strip()
    if text == "":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _optional_prize(value: str) -> float | None:
    text = value.replace(",", "").strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_netkeiba_result_table(html: str) -> tuple[ArchiveStart, ...]:
    """Parse a netkeiba trainer/owner result table into archive starts."""
    marker = html.find(NETKEIBA_RESULT_TABLE_MARKER)
    if marker < 0:
        return ()
    end = html.find("</table>", marker)
    if end < 0:
        return ()
    rows = TABLE_ROW_PATTERN.findall(html[marker:end])
    if not rows:
        return ()
    headers = [_clean_html_text(cell) for cell in TABLE_HEADER_CELL_PATTERN.findall(rows[0])]
    date_index = _header_index(headers, DATE_HEADER_PATTERN)
    name_index = _header_index(headers, NAME_HEADER_PATTERN)
    finish_index = _header_index(headers, NETKEIBA_FINISH_HEADER_PATTERN)
    prize_index = _header_index(headers, PRIZE_HEADER_PATTERN)
    if date_index is None or finish_index is None:
        return ()
    starts: list[ArchiveStart] = []
    for row in rows[1:]:
        cells = [_clean_html_text(cell) for cell in TABLE_CELL_PATTERN.findall(row)]
        if max(date_index, finish_index) >= len(cells):
            continue
        raced = parse_archive_date(cells[date_index])
        if raced is None:
            continue
        race_name = cells[name_index] if name_index is not None and name_index < len(cells) else ""
        prize = None
        if prize_index is not None and prize_index < len(cells):
            prize = _optional_prize(cells[prize_index])
        starts.append(
            ArchiveStart(
                raced=raced,
                race_name=race_name,
                finish=_optional_finish(cells[finish_index]),
                prize_man_yen=prize,
            )
        )
    return tuple(starts)


def parse_jravan_horse_results(html: str) -> tuple[ArchiveStart, ...]:
    """Parse a JRA-VAN World horse result table."""
    matched = JRAVAN_RESULT_SECTION_PATTERN.search(html)
    if matched is None:
        return ()
    rows = TABLE_ROW_PATTERN.findall(matched.group(1))
    if not rows:
        return ()
    headers = [_clean_html_text(cell) for cell in TABLE_HEADER_CELL_PATTERN.findall(rows[0])]
    date_index = _header_index(headers, JRAVAN_DATE_HEADER_PATTERN)
    name_index = _header_index(headers, NAME_HEADER_PATTERN)
    finish_index = _header_index(headers, NETKEIBA_FINISH_HEADER_PATTERN)
    if date_index is None or finish_index is None:
        return ()
    starts: list[ArchiveStart] = []
    for row in rows[1:]:
        cells = [_clean_html_text(cell) for cell in TABLE_CELL_PATTERN.findall(row)]
        if max(date_index, finish_index) >= len(cells):
            continue
        raced = parse_archive_date(cells[date_index])
        if raced is None:
            continue
        race_name = cells[name_index] if name_index is not None and name_index < len(cells) else ""
        starts.append(
            ArchiveStart(
                raced=raced,
                race_name=race_name,
                finish=_optional_finish(cells[finish_index]),
                prize_man_yen=None,
            )
        )
    return tuple(starts)


def parse_jra_card_past_runs(payload: object, *, umaban: int) -> tuple[ArchiveStart, ...]:
    """Official JRA card past runs for one horse number."""
    if not isinstance(payload, dict):
        return ()
    runners = payload.get(JRA_JSON_RUNNERS_KEY)
    if not isinstance(runners, list):
        return ()
    for runner in runners:
        if not isinstance(runner, dict):
            continue
        if _json_horse_number(runner.get(JRA_JSON_HORSE_NUMBER_KEY)) != umaban:
            continue
        past_runs = runner.get(JRA_JSON_PAST_RUNS_KEY)
        if not isinstance(past_runs, list):
            return ()
        starts: list[ArchiveStart] = []
        for item in past_runs:
            if not isinstance(item, dict):
                continue
            raced = parse_archive_date(_json_person_name(item.get(JRA_JSON_DATE_KEY)))
            if raced is None:
                continue
            race_name = _json_person_name(item.get(JRA_JSON_RACE_NAME_KEY))
            grade = _json_person_name(item.get(JRA_JSON_GRADE_KEY))
            if grade != "":
                race_name = f"{race_name}({grade})"
            finish_value = item.get(JRA_JSON_FINISH_KEY)
            finish = finish_value if isinstance(finish_value, int) else _optional_finish(str(finish_value))
            starts.append(
                ArchiveStart(
                    raced=raced,
                    race_name=race_name,
                    finish=finish,
                    prize_man_yen=None,
                )
            )
        return tuple(starts)
    return ()


def merge_archive_starts(left: Sequence[ArchiveStart], right: Sequence[ArchiveStart]) -> tuple[ArchiveStart, ...]:
    """Deduplicate starts by date + race name, keeping the first copy."""
    seen: set[tuple[date, str, int | None, float | None]] = set()
    merged: list[ArchiveStart] = []
    for start in (*left, *right):
        key = (start.raced, start.race_name, start.finish, start.prize_man_yen)
        if key in seen:
            continue
        seen.add(key)
        merged.append(start)
    return tuple(merged)


def url_cache_path(cache_dir: Path, url: str) -> Path:
    digest = hashlib.sha256(normalize_source_url(url).encode("utf-8")).hexdigest()
    return cache_dir / f"{digest}{SOURCE_CACHE_FILE_SUFFIX}"


def source_combo_manifest_path(cache_dir: Path, urls: OverseasSourceUrls) -> Path:
    return cache_dir / SOURCE_CACHE_COMBO_DIRNAME / f"{source_url_combo_key(urls)}{SOURCE_COMBO_FILE_SUFFIX}"


def read_cached_url_text(cache_dir: Path, url: str) -> str | None:
    path = url_cache_path(cache_dir, url)
    if not path.is_file():
        return None
    return read_html_text(path)


def write_text_if_changed(path: Path, text: str) -> bool:
    """Write UTF-8 text only when the on-disk bytes would change."""
    if path.is_file() and path.read_text(encoding="utf-8") == text:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return True


def write_cached_url_text(cache_dir: Path, url: str, text: str) -> bool:
    return write_text_if_changed(url_cache_path(cache_dir, url), text)


def remember_source_url_combo(cache_dir: Path, urls: OverseasSourceUrls) -> bool:
    payload = "\n".join(
        (
            normalize_source_url(urls.jra_url),
            normalize_source_url(urls.netkeiba_url),
            normalize_source_url(urls.jravan_url),
            "",
        )
    )
    return write_text_if_changed(source_combo_manifest_path(cache_dir, urls), payload)


def _download_url_text(url: str) -> str:
    request = Request(
        url,
        headers={"Accept": "text/html,application/xhtml+xml", "User-Agent": HTML_FETCH_USER_AGENT},
    )
    try:
        with urlopen(request, timeout=HTML_FETCH_TIMEOUT_SECONDS) as response:
            raw = response.read()
    except URLError as exc:
        raise URLError(f"Failed to fetch {url}") from exc
    for encoding in HTML_TEXT_ENCODINGS:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def fetch_url_text(url: str, *, cache_dir: Path | None = None) -> str:
    """Fetch HTML/text with a browser UA. Same normalized URL reuses the cache."""
    if cache_dir is not None:
        cached = read_cached_url_text(cache_dir, url)
        if cached is not None:
            return cached
    text = _download_url_text(url)
    if cache_dir is not None:
        write_cached_url_text(cache_dir, url, text)
    return text


def read_html_source(path_or_url: str, *, cache_dir: Path | None = None) -> str:
    """Read a local HTML file or fetch an http(s) URL."""
    if path_or_url.startswith(HTTP_URL_PREFIXES):
        return fetch_url_text(path_or_url, cache_dir=cache_dir)
    path = Path(path_or_url)
    if not path.is_file():
        return ""
    return read_html_text(path)


def extract_jra_shutuba_url(html: str) -> str:
    """Return the official accessSD 出馬表 URL from a JRA overseas race page."""
    matched = JRA_ACCESS_SD_HREF_PATTERN.search(html)
    if matched is None:
        return ""
    href = matched.group(1).replace("&amp;", "&")
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{JRA_ORIGIN}{href}"
    return f"{JRA_ORIGIN}/{href}"


def fetch_netkeiba_horse_owner(horse_id: str, *, cache_dir: Path | None = None) -> str:
    if horse_id == "":
        return ""
    html = fetch_url_text(
        NETKEIBA_HORSE_URL_TEMPLATE.format(horse_id=horse_id),
        cache_dir=cache_dir,
    )
    return parse_netkeiba_horse_page_owner(html)


def planned_overseas_card(slug: str) -> PlannedOverseasCard | None:
    wanted = slug.strip().casefold()
    if wanted == "":
        return None
    for card in PLANNED_OVERSEAS_CARDS:
        names = (card.slug, *card.aliases, card.race_name)
        if any(wanted == name.casefold() for name in names):
            return card
    return None


def planned_card_jra_url(card: PlannedOverseasCard) -> str:
    if card.jra_card_url != "":
        return card.jra_card_url
    return card.jra_race_page_url


def normalize_source_url(url: str) -> str:
    """Canonicalize a published card URL so tracking params do not fork caches."""
    stripped = url.strip()
    if stripped == "":
        return ""
    parsed = urlsplit(stripped)
    kept = tuple(
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.casefold() not in SOURCE_URL_TRACKING_QUERY_KEYS
    )
    return urlunsplit(
        (
            parsed.scheme.casefold(),
            parsed.netloc.casefold(),
            parsed.path,
            urlencode(kept, safe="/", quote_via=quote),
            "",
        )
    )


def prefer_existing_source_url(current: str, incoming: str) -> str:
    if incoming == "":
        return current
    if current != "" and normalize_source_url(current) == normalize_source_url(incoming):
        return current
    return incoming


def source_urls_from_card(card: PlannedOverseasCard) -> OverseasSourceUrls:
    return OverseasSourceUrls(
        jra_url=card.jra_card_url,
        netkeiba_url=card.netkeiba_card_url,
        jravan_url=card.jravan_card_url,
    )


def source_url_combo_key(urls: OverseasSourceUrls) -> str:
    payload = "\n".join(
        (
            normalize_source_url(urls.jra_url),
            normalize_source_url(urls.netkeiba_url),
            normalize_source_url(urls.jravan_url),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def apply_source_urls(card: PlannedOverseasCard, urls: OverseasSourceUrls) -> PlannedOverseasCard:
    """Override catalog URLs when a CLI/source argument is a new identity."""
    return replace(
        card,
        jra_card_url=prefer_existing_source_url(card.jra_card_url, urls.jra_url),
        netkeiba_card_url=prefer_existing_source_url(card.netkeiba_card_url, urls.netkeiba_url),
        jravan_card_url=prefer_existing_source_url(card.jravan_card_url, urls.jravan_url),
    )


def overseas_predict_command(card: PlannedOverseasCard, *, jra_card_url: str) -> str:
    parts = [
        "uv run python",
        OVERSEAS_TRAIN_SCRIPT,
        "--predict-only",
        f"--card {card.slug}",
    ]
    if jra_card_url != "":
        parts.append(f"--jra-url {jra_card_url}")
    if card.netkeiba_card_url != "":
        parts.append(f"--netkeiba-url {card.netkeiba_card_url}")
    if card.jravan_card_url != "":
        parts.append(f"--jravan-url {card.jravan_card_url}")
    return " ".join(parts)


def parse_jra_overseas_sale_list(html: str) -> tuple[tuple[str, str, str], ...]:
    """Return (mmdd, race_name, race_page_url) rows from the official sale list."""
    rows: list[tuple[str, str, str]] = []
    for row_html in TABLE_ROW_PATTERN.findall(html):
        dated = JRA_OVERSEAS_SALE_DATE_PATTERN.search(_clean_html_text(row_html))
        page = JRA_OVERSEAS_SALE_PAGE_PATTERN.search(row_html)
        if dated is None or page is None:
            continue
        month = dated.group(2).zfill(2)
        day = dated.group(3).zfill(2)
        name = _clean_html_text(page.group(2))
        href = page.group(1)
        url = href if href.startswith("http") else f"{JRA_ORIGIN}{href}"
        rows.append((f"{month}{day}", name, url))
    return tuple(rows)


def sale_list_match(card: PlannedOverseasCard, listings: Sequence[tuple[str, str, str]]) -> str:
    target = fold_person_name(card.race_name)
    for mmdd, name, url in listings:
        if mmdd != card.kaisai_tsukihi:
            continue
        if target in fold_person_name(name) or fold_person_name(name) in target:
            return url
    return ""


def prepare_overseas_card(
    card: PlannedOverseasCard,
    *,
    sale_list_html: str,
    race_page_html: str,
) -> OverseasPrepareStatus:
    listings = parse_jra_overseas_sale_list(sale_list_html)
    sale_page = sale_list_match(card, listings)
    card_url = card.jra_card_url
    if card_url == "" and race_page_html != "":
        card_url = extract_jra_shutuba_url(race_page_html)
    return OverseasPrepareStatus(
        card=card,
        on_sale_list=sale_page != "",
        jra_card_url=card_url,
        netkeiba_card_url=card.netkeiba_card_url,
        jravan_card_url=card.jravan_card_url,
        next_command=overseas_predict_command(card, jra_card_url=card_url),
    )


def format_archive_csv(starts: Sequence[ArchiveStart]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(list(ARCHIVE_CSV_HEADERS))
    for start in starts:
        finish_text = "" if start.finish is None else str(start.finish)
        prize_text = "" if start.prize_man_yen is None else str(start.prize_man_yen)
        writer.writerow(
            [
                f"{start.raced.year}年{start.raced.month}月{start.raced.day}日",
                start.race_name,
                finish_text,
                prize_text,
            ]
        )
    return buffer.getvalue()


def write_archive_csv(path: Path, starts: Sequence[ArchiveStart]) -> bool:
    return write_text_if_changed(path, format_archive_csv(starts))
