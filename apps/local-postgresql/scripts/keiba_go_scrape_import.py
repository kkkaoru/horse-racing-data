#!/usr/bin/env python3
"""Scrape keiba.go.jp RaceList+DebaTable and upsert into local nvd_ra / nvd_se.

Example:
  bun run scrape:keiba-go -- --date 20260901

The command discovers the day's venues and race numbers from TodayRaceInfoTop,
then fetches each venue's RaceList and every DebaTable URL. babaCode maps to
local keibajo_code via NAR_BABA_TO_KEIBAJO (same as
packages/horse-racing-realtime). Meeting kai/nichime can be passed with
--meta baba=kai:nichi, or inferred from surrounding nvd_ra rows.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import difflib
import hashlib
import json
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date
from html import unescape
from pathlib import Path

import psycopg2
import psycopg2.extras

SCRIPT_DIR = Path(__file__).resolve().parent
PKG_DIR = SCRIPT_DIR.parent
REPO_ROOT = PKG_DIR.parent.parent
LOCAL_ENV = PKG_DIR / ".env"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# keiba.go.jp babaCode -> local keibajo_code (inverse of LOCAL_KEIBAJO_TO_NAR_BABA_CODE)
NAR_BABA_TO_KEIBAJO: dict[str, str] = {
    "36": "30",
    "10": "35",
    "11": "36",
    "18": "42",
    "19": "43",
    "20": "44",
    "21": "45",
    "22": "46",
    "23": "47",
    "24": "48",
    "27": "50",
    "28": "51",
    "31": "54",
    "32": "55",
    "03": "83",
}

PERSON_NAME_TRANSLATION = str.maketrans(
    {
        "澤": "沢",
        "櫻": "桜",
        "廣": "広",
        "國": "国",
        "髙": "高",
        "﨑": "崎",
        "邊": "辺",
        "邉": "辺",
        "齋": "斎",
        "齊": "斉",
        "濵": "浜",
        "德": "徳",
        "惠": "恵",
        "會": "会",
        "嶋": "島",
        "ヶ": "ケ",
    }
)
OWNER_LEGAL_FORMS = (
    "株式会社",
    "有限会社",
    "合同会社",
    "合資会社",
    "合名会社",
    "一般社団法人",
    "公益社団法人",
    "一般財団法人",
    "公益財団法人",
    "（株）",
    "(株)",
    "㈱",
    "（有）",
    "(有)",
    "㈲",
    "（同）",
    "(同)",
)

KEIBAJO_LABEL: dict[str, str] = {
    "30": "門別",
    "35": "盛岡",
    "36": "水沢",
    "42": "浦和",
    "43": "船橋",
    "44": "大井",
    "45": "川崎",
    "46": "金沢",
    "47": "笠松",
    "48": "名古屋",
    "50": "園田",
    "51": "姫路",
    "54": "高知",
    "55": "佐賀",
    "83": "帯広",
}


@dataclass(frozen=True)
class PastRaceKey:
    ymd: str
    baba: str
    race_number: str


@dataclass
class Entry:
    umaban: str
    wakuban: str
    bamei: str
    jockey: str | None
    trainer: str | None
    futan: float | None
    bataiju: int | None
    sex: str | None
    age: int | None
    status: str | None = None
    lineage_login_code: str | None = None
    rider_license_no: str | None = None
    trainer_license_no: str | None = None
    owner: str | None = None
    past_races: tuple[PastRaceKey, ...] = ()


@dataclass(frozen=True)
class PersonRecord:
    code: str
    full_name: str
    short_name: str
    last_seen: str


@dataclass(frozen=True)
class OwnerRecord:
    code: str
    name: str


@dataclass(frozen=True)
class ProfileTarget:
    url: str
    cache_path: Path


@dataclass
class Race:
    baba: str
    keibajo: str
    race_bango: str
    name: str
    hasso: str
    kyori: str
    track_code: str
    entries: list[Entry] = field(default_factory=list)


@dataclass(frozen=True)
class VenueDiscovery:
    baba: str
    label: str
    race_numbers: tuple[str, ...]


@dataclass(frozen=True)
class VenueTarget:
    baba: str
    keibajo: str
    label: str
    kai: str
    nichi: str
    race_numbers: tuple[str, ...]


def load_dsn(env_path: Path = LOCAL_ENV) -> str:
    if not env_path.exists():
        raise SystemExit(f"DATABASE_URL env file missing: {env_path}")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip().strip("'").strip('"')
    raise SystemExit(f"DATABASE_URL missing in {env_path}")


def yobi_code_for_ymd(ymd: str) -> str:
    """JRDB-like weekday: Sun=1 ... Sat=7."""
    d = date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]))
    return str(d.isoweekday() % 7 + 1)


def race_date_query(ymd: str) -> str:
    return f"{ymd[:4]}%2F{ymd[4:6]}%2F{ymd[6:8]}"


def fetch(url: str, dest: Path, *, refresh: bool = False) -> str:
    if not refresh and dest.exists() and dest.stat().st_size > 1000:
        return dest.read_text(encoding="utf-8", errors="replace")
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    text = data.decode("utf-8", errors="replace")
    dest.write_text(text, encoding="utf-8")
    time.sleep(0.35)
    return text


def strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", html))).strip()


def normalize_person_name(name: str) -> str:
    without_affiliation = re.sub(r"[（(][^）)]*[）)]", "", name)
    return re.sub(r"[\s　]+", "", without_affiliation).translate(
        PERSON_NAME_TRANSLATION
    )


def person_record_sort_key(record: PersonRecord) -> tuple[str, str, str, str]:
    return (
        normalize_person_name(record.short_name),
        normalize_person_name(record.full_name),
        record.short_name,
        record.full_name,
    )


def owner_record_sort_key(record: OwnerRecord) -> tuple[str, str]:
    return record.code, record.name


def person_name_similarity(profile_name: str, candidate_name: str) -> float:
    profile = normalize_person_name(profile_name)
    candidate = normalize_person_name(candidate_name)
    if not profile or not candidate:
        return 0.0
    if profile == candidate:
        return 1.0
    if min(len(profile), len(candidate)) >= 3 and (
        profile in candidate or candidate in profile
    ):
        return 0.9 + 0.09 * min(len(profile), len(candidate)) / max(
            len(profile), len(candidate)
        )
    return difflib.SequenceMatcher(a=profile, b=candidate).ratio()


def normalize_owner_name(name: str) -> str:
    normalized = re.sub(r"[\s　]+", "", name).translate(PERSON_NAME_TRANSLATION)
    for legal_form in OWNER_LEGAL_FORMS:
        normalized = normalized.replace(legal_form, "")
    return normalized.removeprefix("組）").removeprefix("(組)")


def prefetch_profile(target: ProfileTarget, *, refresh: bool) -> None:
    fetch(target.url, target.cache_path, refresh=refresh)


def prefetch_entity_profiles(
    races: list[Race], *, profile_cache: Path, refresh: bool
) -> None:
    targets: dict[Path, ProfileTarget] = {}
    for entry in (entry for race in races for entry in race.entries):
        if entry.rider_license_no is not None:
            path = profile_cache / f"rider_{entry.rider_license_no}.html"
            targets[path] = ProfileTarget(
                url=rider_profile_url(entry.rider_license_no), cache_path=path
            )
        if entry.trainer_license_no is not None:
            path = profile_cache / f"trainer_{entry.trainer_license_no}.html"
            targets[path] = ProfileTarget(
                url=trainer_profile_url(entry.trainer_license_no), cache_path=path
            )
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(prefetch_profile, target, refresh=refresh)
            for target in targets.values()
        ]
        for future in futures:
            future.result()


def parse_profile_name(html: str) -> str:
    match = re.search(
        r'<h4[^>]*class=["\'][^"\']*\bodd_title\b(?![^"\']*\bmini\b)[^"\']*["\'][^>]*>([\s\S]*?)</h4>',
        html,
        flags=re.IGNORECASE,
    )
    if match is None:
        raise SystemExit("keiba.go.jp entity profile did not contain a full name")
    name = strip_tags(match.group(1))
    if not name:
        raise SystemExit("keiba.go.jp entity profile contained an empty full name")
    return name


def rows_fingerprint(*row_sets: list[dict]) -> str:
    canonical_rows = [
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for rows in row_sets
        for row in rows
    ]
    canonical_rows.sort()
    payload = "\n".join(canonical_rows).encode()
    return hashlib.sha256(payload).hexdigest()


def write_json_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(path)


def today_top_url(ymd: str) -> str:
    return (
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop?"
        f"k_raceDate={race_date_query(ymd)}"
    )


def race_list_url(ymd: str, baba: str) -> str:
    return (
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?"
        f"k_raceDate={race_date_query(ymd)}&k_babaCode={baba}"
    )


def deba_table_url(ymd: str, baba: str, race_number: str) -> str:
    return (
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?"
        f"k_raceDate={race_date_query(ymd)}&k_raceNo={int(race_number)}"
        f"&k_babaCode={baba}"
    )


def rider_profile_url(license_no: str) -> str:
    return (
        "https://www.keiba.go.jp/KeibaWeb/DataRoom/RiderMark?"
        f"k_riderLicenseNo={license_no}"
    )


def trainer_profile_url(license_no: str) -> str:
    return (
        "https://www.keiba.go.jp/KeibaWeb/DataRoom/TrainerMark?"
        f"k_trainerLicenseNo={license_no}"
    )


def _query_value(url: str, name: str) -> str | None:
    match = re.search(
        rf"(?:[?&]|&amp;){re.escape(name)}=([^&\"']+)",
        url,
        flags=re.IGNORECASE,
    )
    return urllib.parse.unquote(unescape(match.group(1))) if match else None


def parse_today_venues(html: str, ymd: str) -> list[VenueDiscovery]:
    """Discover exact-date venues and race numbers from TodayRaceInfoTop."""
    target_date = f"{ymd[:4]}/{ymd[4:6]}/{ymd[6:8]}"
    labels: dict[str, str] = {}
    race_numbers: dict[str, set[str]] = {}

    for match in re.finditer(
        r'<a[^>]+href=["\']([^"\']*RaceList\?[^"\']+)["\'][^>]*>([\s\S]*?)</a>',
        html,
        flags=re.IGNORECASE,
    ):
        href, body = match.groups()
        if _query_value(href, "k_raceDate") != target_date:
            continue
        baba = _query_value(href, "k_babaCode")
        if baba is None:
            continue
        baba = baba.zfill(2)
        labels.setdefault(baba, re.sub(r"\s+", "", strip_tags(body)))
        race_numbers.setdefault(baba, set())

    for match in re.finditer(r"DebaTable\?[^\"']+", html, flags=re.IGNORECASE):
        href = match.group(0)
        if _query_value(href, "k_raceDate") != target_date:
            continue
        baba = _query_value(href, "k_babaCode")
        race_number = _query_value(href, "k_raceNo")
        if baba is None or race_number is None:
            continue
        baba = baba.zfill(2)
        if baba not in race_numbers:
            continue
        race_numbers[baba].add(str(int(race_number)))

    discoveries: list[VenueDiscovery] = []
    for baba, label in labels.items():
        numbers = tuple(sorted(race_numbers[baba], key=int))
        if not numbers:
            raise SystemExit(
                f"TodayRaceInfoTop listed babaCode={baba} without races for date={ymd}"
            )
        discoveries.append(VenueDiscovery(baba=baba, label=label, race_numbers=numbers))
    if not discoveries:
        raise SystemExit(f"TodayRaceInfoTop listed no venues for date={ymd}")
    return discoveries


def select_venue_discoveries(
    discoveries: list[VenueDiscovery], requested_babas: list[str] | None
) -> list[VenueDiscovery]:
    if requested_babas is None:
        return discoveries
    by_baba = {venue.baba: venue for venue in discoveries}
    selected: list[VenueDiscovery] = []
    for raw_baba in requested_babas:
        baba = raw_baba.zfill(2) if raw_baba.isdigit() else raw_baba
        venue = by_baba.get(baba)
        if venue is None:
            raise SystemExit(f"babaCode={baba} is not listed on TodayRaceInfoTop")
        selected.append(venue)
    return selected


def pad_name(name: str, width: int = 36) -> str:
    name = name.replace("\u3000", "").strip()
    return (name + ("\u3000" * width))[:width]


def pad_short(name: str, width: int = 8) -> str:
    name = re.sub(r"（.*?）", "", name).replace("\u3000", "").strip()
    return (name + ("\u3000" * width))[:width]


def futan_to_code(kg: float | None) -> str:
    if kg is None:
        return "000"
    return f"{round(kg * 10):03d}"


def batai_to_code(kg: int | None) -> str:
    if kg is None:
        return "   "
    return f"{kg:03d}"


def sex_code(sex: str | None) -> str:
    if sex == "牡":
        return "1"
    if sex == "牝":
        return "2"
    if sex == "セ":
        return "3"
    return "0"


def parse_racelist_races(html: str, baba: str) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    for m in re.finditer(
        rf'DebaTable\?[^"\']*k_raceNo=(\d+)[^"\']*k_babaCode={baba}',
        html,
        flags=re.IGNORECASE,
    ):
        rno = m.group(1)
        chunk = html[m.start() : m.start() + 500]
        plain = strip_tags(chunk)
        name_m = re.search(r"td>\s*(.+?)\s*(?:左|右|芝)", plain)
        name = name_m.group(1).strip() if name_m else ""
        dist_m = re.search(r"(芝)?(?:左|右)?(\d{3,4})m", plain)
        dist = dist_m.group(0) if dist_m else ""
        out.append((rno, name, dist))
    seen: set[str] = set()
    uniq: list[tuple[str, str, str]] = []
    for item in out:
        if item[0] in seen:
            continue
        seen.add(item[0])
        uniq.append(item)
    return uniq


def parse_deba_meta(html: str) -> tuple[str, str, str]:
    h4 = re.search(r"<h4[^>]*>([\s\S]*?)</h4>", html, flags=re.IGNORECASE)
    plain = strip_tags(h4.group(1) if h4 else "")
    time_m = re.search(r"(\d{1,2}):(\d{2})発走", plain)
    hasso = f"{int(time_m.group(1)):02d}{time_m.group(2)}" if time_m else "0000"
    h3 = re.search(r"<h3[^>]*>([\s\S]*?)</h3>", html, flags=re.IGNORECASE)
    name = strip_tags(h3.group(1) if h3 else "")
    course_m = re.search(r"(ダート|芝|障)[^\d]{0,8}(\d{3,4})", html)
    if course_m:
        surface, dist = course_m.group(1), course_m.group(2)
    else:
        surface, dist = "ダート", "0000"
    return hasso, name, surface + dist


def calculate_wakuban(horse_count: int, umaban: int) -> int:
    if not 1 <= horse_count <= 16:
        raise ValueError(f"horse_count must be between 1 and 16: {horse_count}")
    if not 1 <= umaban <= horse_count:
        raise ValueError(
            f"umaban must be between 1 and horse_count: umaban={umaban} "
            f"horse_count={horse_count}"
        )
    if horse_count <= 8:
        return umaban
    single_horse_frames = 16 - horse_count
    if umaban <= single_horse_frames:
        return umaban
    return single_horse_frames + math.ceil((umaban - single_horse_frames) / 2)


def apply_calculated_wakuban(entries: list[Entry]) -> None:
    horse_count = len(entries)
    horse_numbers = sorted(int(entry.umaban) for entry in entries)
    if horse_numbers != list(range(1, horse_count + 1)):
        raise SystemExit(f"DebaTable horse numbers are not contiguous: {horse_numbers}")
    for entry in entries:
        expected = calculate_wakuban(horse_count, int(entry.umaban))
        if entry.wakuban != "0" and int(entry.wakuban) != expected:
            raise SystemExit(
                f"DebaTable wakuban mismatch: umaban={entry.umaban} "
                f"html={entry.wakuban} expected={expected}"
            )
        entry.wakuban = str(expected)


def parse_deba_entries(html: str) -> list[Entry]:
    entries: list[Entry] = []
    parts = re.split(
        r'<tr[^>]*class=["\'][^"\']*tBorder[^"\']*["\'][^>]*>',
        html,
        flags=re.IGNORECASE,
    )
    for block in parts[1:]:
        umaban = re.search(
            r'class=["\'][^"\']*horseNum[^"\']*["\'][^>]*>\s*(\d{1,2})\s*<',
            block,
            flags=re.IGNORECASE,
        )
        if not umaban:
            continue
        waku = re.search(
            r'class=["\'][^"\']*courseNum[^"\']*["\'][^>]*>\s*(\d+)\s*<',
            block,
            flags=re.IGNORECASE,
        )
        bamei_m = re.search(
            r'class=["\'][^"\']*horseName[^"\']*["\'][^>]*>([\s\S]*?)</a>',
            block,
            flags=re.IGNORECASE,
        )
        jockey_m = re.search(
            r'class=["\'][^"\']*jockeyName[^"\']*["\'][^>]*>([\s\S]*?)</a>',
            block,
            flags=re.IGNORECASE,
        )
        entity_cells = re.findall(
            r'<td\s+colspan=["\']?3["\']?[^>]*>([\s\S]*?)</td>\s*'
            r'<td\s+colspan=["\']?1["\']?[^>]*>([\s\S]*?)</td>',
            block,
            flags=re.IGNORECASE,
        )
        trainer = strip_tags(entity_cells[1][1]) if len(entity_cells) >= 2 else None
        owner = strip_tags(entity_cells[2][1]) if len(entity_cells) >= 3 else None
        lineage_match = re.search(
            r"HorseMarkInfo\?k_lineageLoginCode=(\d+)", block, flags=re.IGNORECASE
        )
        rider_match = re.search(
            r"RiderMark\?k_riderLicenseNo=(\d+)", block, flags=re.IGNORECASE
        )
        trainer_match = re.search(
            r"TrainerMark\?k_trainerLicenseNo=(\d+)", block, flags=re.IGNORECASE
        )
        past_races: list[PastRaceKey] = []
        for past_match in re.finditer(
            r"RaceMarkTable\?[^\"']+", block, flags=re.IGNORECASE
        ):
            past_url = past_match.group(0)
            past_date = _query_value(past_url, "k_raceDate")
            past_baba = _query_value(past_url, "k_babaCode")
            past_number = _query_value(past_url, "k_raceNo")
            if past_date is None or past_baba is None or past_number is None:
                continue
            past_races.append(
                PastRaceKey(
                    ymd=past_date.replace("/", ""),
                    baba=past_baba.zfill(2),
                    race_number=f"{int(past_number):02d}",
                )
            )
        kg_line = re.search(r"(\d+)\s*人\s+(\d{3})\s+[^\d]{1,20}?(\d{2}\.\d)", block)
        bataiju = int(kg_line.group(2)) if kg_line else None
        futan = float(kg_line.group(3)) if kg_line else None
        if futan is None:
            futan_m = re.search(r"(\d{2}\.\d)\s*(?:kg)?", strip_tags(block))
            futan = float(futan_m.group(1)) if futan_m else None
        sex_age = re.search(r"(牡|牝|セ)\s*(\d{1,2})", block)
        status = None
        info_cells = re.findall(
            r'<td[^>]*class=["\'][^"\']*\binfo\b[^"\']*["\'][^>]*>([\s\S]*?)</td>',
            block,
            flags=re.IGNORECASE,
        )
        for cell in info_cells:
            normalized = strip_tags(cell)
            for label in ("出場停止", "出走取消", "取消", "競走除外", "除外"):
                if label in normalized:
                    status = label
                    break
            if status:
                break
        bamei = strip_tags(bamei_m.group(1)) if bamei_m else ""
        jockey = strip_tags(jockey_m.group(1)) if jockey_m else None
        entries.append(
            Entry(
                umaban=f"{int(umaban.group(1)):02d}",
                wakuban=waku.group(1) if waku else "0",
                bamei=bamei,
                jockey=jockey,
                trainer=trainer,
                futan=futan,
                bataiju=bataiju,
                sex=sex_age.group(1) if sex_age else None,
                age=int(sex_age.group(2)) if sex_age else None,
                status=status,
                lineage_login_code=lineage_match.group(1) if lineage_match else None,
                rider_license_no=rider_match.group(1) if rider_match else None,
                trainer_license_no=trainer_match.group(1) if trainer_match else None,
                owner=owner,
                past_races=tuple(dict.fromkeys(past_races)),
            )
        )
    if entries:
        apply_calculated_wakuban(entries)
    return entries


def infer_track_code(
    cur, keibajo: str, surface_dist: str, exclude_ymd: str
) -> tuple[str, str]:
    dirt = "ダート" in surface_dist or surface_dist.startswith("ダ")
    dist_m = re.search(r"(\d{3,4})", surface_dist)
    dist = dist_m.group(1).zfill(4) if dist_m else "0000"
    track_like = "2%" if dirt else "1%"
    cur.execute(
        """
        SELECT track_code, count(*) n FROM nvd_ra
        WHERE keibajo_code=%s AND kyori=%s AND track_code LIKE %s
          AND kaisai_nen||kaisai_tsukihi >= '20250101'
          AND kaisai_nen||kaisai_tsukihi<>%s
        GROUP BY 1 ORDER BY n DESC, track_code ASC LIMIT 1
        """,
        (keibajo, dist, track_like, exclude_ymd),
    )
    row = cur.fetchone()
    if row:
        return row[0], dist
    cur.execute(
        """
        SELECT track_code, count(*) n FROM nvd_ra
        WHERE keibajo_code=%s AND track_code LIKE %s
          AND kaisai_nen||kaisai_tsukihi >= '20250101'
          AND kaisai_nen||kaisai_tsukihi<>%s
        GROUP BY 1 ORDER BY n DESC, track_code ASC LIMIT 1
        """,
        (keibajo, track_like, exclude_ymd),
    )
    row = cur.fetchone()
    return (row[0] if row else ("24" if dirt else "11")), dist


def load_horse_by_id(
    cur, ketto_toroku_bango: str, exclude_ymd: str
) -> dict[str, object] | None:
    cur.execute(
        """
        SELECT ketto_toroku_bango, seibetsu_code, hinshu_code, moshoku_code, barei,
               tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho,
               banushi_code, banushimei, fukushoku_hyoji, umakigo_code
        FROM nvd_se
        WHERE ketto_toroku_bango=%s
          AND kaisai_nen||kaisai_tsukihi<>%s
        ORDER BY kaisai_nen DESC, kaisai_tsukihi DESC,
                 keibajo_code DESC, race_bango DESC, umaban DESC
        LIMIT 1
        """,
        (ketto_toroku_bango, exclude_ymd),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute(
            """
            SELECT ketto_toroku_bango, seibetsu_code, hinshu_code, moshoku_code,
                   NULL::text AS barei, NULL::text AS tozai_shozoku_code,
                   chokyoshi_code, chokyoshimei_ryakusho,
                   banushi_code, banushimei,
                   NULL::text AS fukushoku_hyoji, '00'::text AS umakigo_code
            FROM nvd_nu
            WHERE ketto_toroku_bango=%s
            LIMIT 1
            """,
            (ketto_toroku_bango,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    columns = [description[0] for description in cur.description]
    return dict(zip(columns, row))


def lookup_horse(cur, entry: Entry, ymd: str) -> dict[str, object] | None:
    key = normalize_person_name(entry.bamei)
    if not key or entry.lineage_login_code is None:
        return None

    if entry.age is not None:
        birth_year = str(int(ymd[:4]) - entry.age)
        cur.execute(
            """
            SELECT DISTINCT ketto_toroku_bango
            FROM nvd_nu
            WHERE replace(replace(bamei,'　',''),' ','')=%s
              AND substring(seinengappi, 1, 4)=%s
              AND seibetsu_code=%s
              AND ketto_toroku_bango<>'0000000000'
            """,
            (key, birth_year, sex_code(entry.sex)),
        )
        master_ids = {row[0] for row in cur.fetchall()}
        if len(master_ids) == 1:
            return load_horse_by_id(cur, master_ids.pop(), ymd)
        if len(master_ids) > 1:
            return None

    past_keys = [
        f"{past.ymd}:{NAR_BABA_TO_KEIBAJO[past.baba]}:{past.race_number}"
        for past in entry.past_races
        if past.baba in NAR_BABA_TO_KEIBAJO
    ]
    candidate_ids: set[str] = set()
    if past_keys:
        cur.execute(
            """
            SELECT DISTINCT ketto_toroku_bango
            FROM nvd_se
            WHERE replace(replace(bamei,'　',''),' ','')=%s
              AND ketto_toroku_bango<>'0000000000'
              AND (
                kaisai_nen||kaisai_tsukihi||':'||keibajo_code||':'||race_bango
              )=ANY(%s)
            """,
            (key, past_keys),
        )
        candidate_ids.update(row[0] for row in cur.fetchall())

    if not candidate_ids:
        cur.execute(
            """
            SELECT DISTINCT ketto_toroku_bango FROM (
              SELECT ketto_toroku_bango, bamei FROM nvd_se
              WHERE ketto_toroku_bango<>'0000000000'
                AND kaisai_nen||kaisai_tsukihi<>%s
              UNION ALL
              SELECT ketto_toroku_bango, bamei FROM nvd_nu
              WHERE ketto_toroku_bango<>'0000000000'
            ) candidates
            WHERE replace(replace(bamei,'　',''),' ','')=%s
            """,
            (ymd, key),
        )
        candidate_ids.update(row[0] for row in cur.fetchall())

    if len(candidate_ids) != 1:
        return None
    return load_horse_by_id(cur, candidate_ids.pop(), ymd)


def build_person_index(
    cur, entity_type: str, exclude_ymd: str
) -> dict[str, set[PersonRecord]]:
    if entity_type == "jockey":
        cur.execute(
            """
            SELECT ks.kishu_code, ks.kishumei, ks.kishumei_ryakusho,
                   greatest(
                     ks.data_sakusei_nengappi,
                     coalesce(max(se.kaisai_nen||se.kaisai_tsukihi), '')
                   ) AS last_seen
            FROM nvd_ks ks
            LEFT JOIN nvd_se se
              ON se.kishu_code=ks.kishu_code
             AND se.kaisai_nen||se.kaisai_tsukihi<>%s
            GROUP BY ks.kishu_code, ks.kishumei, ks.kishumei_ryakusho,
                     ks.data_sakusei_nengappi
            """,
            (exclude_ymd,),
        )
    elif entity_type == "trainer":
        cur.execute(
            """
            SELECT ch.chokyoshi_code, ch.chokyoshimei, ch.chokyoshimei_ryakusho,
                   greatest(
                     ch.data_sakusei_nengappi,
                     coalesce(max(se.kaisai_nen||se.kaisai_tsukihi), '')
                   ) AS last_seen
            FROM nvd_ch ch
            LEFT JOIN nvd_se se
              ON se.chokyoshi_code=ch.chokyoshi_code
             AND se.kaisai_nen||se.kaisai_tsukihi<>%s
            GROUP BY ch.chokyoshi_code, ch.chokyoshimei,
                     ch.chokyoshimei_ryakusho, ch.data_sakusei_nengappi
            """,
            (exclude_ymd,),
        )
    else:
        raise ValueError(f"Unsupported person entity type: {entity_type}")

    index: dict[str, set[PersonRecord]] = {}
    for code, full_name, short_name, last_seen in cur.fetchall():
        record = PersonRecord(
            code=code,
            full_name=full_name,
            short_name=short_name,
            last_seen=last_seen,
        )
        for raw_name in (full_name, short_name):
            normalized = normalize_person_name(raw_name or "")
            if normalized:
                index.setdefault(normalized, set()).add(record)

    if entity_type == "jockey":
        cur.execute(
            """
            SELECT kishu_code, kishumei_ryakusho,
                   max(kaisai_nen||kaisai_tsukihi) AS last_seen
            FROM nvd_se
            WHERE kishu_code<>'00000'
              AND kaisai_nen||kaisai_tsukihi<>%s
            GROUP BY kishu_code, kishumei_ryakusho
            """,
            (exclude_ymd,),
        )
    else:
        cur.execute(
            """
            SELECT chokyoshi_code, chokyoshimei_ryakusho,
                   max(kaisai_nen||kaisai_tsukihi) AS last_seen
            FROM nvd_se
            WHERE chokyoshi_code<>'00000'
              AND kaisai_nen||kaisai_tsukihi<>%s
            GROUP BY chokyoshi_code, chokyoshimei_ryakusho
            """,
            (exclude_ymd,),
        )
    for code, short_name, last_seen in cur.fetchall():
        normalized = normalize_person_name(short_name or "")
        if normalized:
            index.setdefault(normalized, set()).add(
                PersonRecord(
                    code=code,
                    full_name=short_name,
                    short_name=short_name,
                    last_seen=last_seen,
                )
            )
    return index


def resolve_person(
    *,
    entity_type: str,
    license_no: str | None,
    index: dict[str, set[PersonRecord]],
    profile_cache: Path,
    refresh: bool,
) -> PersonRecord:
    if license_no is None:
        raise SystemExit(f"keiba.go.jp {entity_type} profile identifier is missing")
    if entity_type == "jockey":
        url = rider_profile_url(license_no)
        cache_path = profile_cache / f"rider_{license_no}.html"
    elif entity_type == "trainer":
        url = trainer_profile_url(license_no)
        cache_path = profile_cache / f"trainer_{license_no}.html"
    else:
        raise ValueError(f"Unsupported person entity type: {entity_type}")

    profile_name = parse_profile_name(fetch(url, cache_path, refresh=refresh))
    normalized_profile = normalize_person_name(profile_name)
    candidates = {
        record
        for record in index.get(normalized_profile, set())
        if record.code.strip("0")
    }
    if not candidates:
        all_records = {
            record
            for records in index.values()
            for record in records
            if record.code.strip("0")
        }
        scored = {
            record: max(
                person_name_similarity(profile_name, record.full_name),
                person_name_similarity(profile_name, record.short_name),
            )
            for record in all_records
        }
        best_score = max(scored.values(), default=0.0)
        if best_score >= 0.72:
            candidates = {
                record for record, score in scored.items() if score == best_score
            }
    newest_marker = max((record.last_seen for record in candidates), default="")
    newest = {record for record in candidates if record.last_seen == newest_marker}
    codes = {record.code for record in newest}
    if len(codes) != 1:
        raise SystemExit(
            f"Cannot uniquely relate {entity_type} license={license_no} "
            f"name={profile_name!r} to local master; candidates={sorted(codes)}"
        )
    code = codes.pop()
    record = min(
        (candidate for candidate in newest if candidate.code == code),
        key=person_record_sort_key,
    )
    return PersonRecord(
        code=record.code,
        full_name=profile_name,
        short_name=record.short_name,
        last_seen=record.last_seen,
    )


def insert_missing_person_masters(
    cur,
    *,
    ymd: str,
    jockeys: set[PersonRecord],
    trainers: set[PersonRecord],
) -> None:
    for jockey in jockeys:
        cur.execute(
            """
            INSERT INTO nvd_ks (
              record_id, data_kubun, data_sakusei_nengappi,
              kishu_code, kishumei, kishumei_ryakusho
            ) VALUES ('KS', '2', %s, %s, %s, %s)
            ON CONFLICT (kishu_code) DO NOTHING
            """,
            (
                ymd,
                jockey.code,
                pad_name(jockey.full_name, 34),
                pad_short(jockey.short_name, 8),
            ),
        )
    for trainer in trainers:
        cur.execute(
            """
            INSERT INTO nvd_ch (
              record_id, data_kubun, data_sakusei_nengappi,
              chokyoshi_code, chokyoshimei, chokyoshimei_ryakusho
            ) VALUES ('CH', '2', %s, %s, %s, %s)
            ON CONFLICT (chokyoshi_code) DO NOTHING
            """,
            (
                ymd,
                trainer.code,
                pad_name(trainer.full_name, 34),
                pad_short(trainer.short_name, 8),
            ),
        )


def build_owner_index(cur) -> dict[str, set[OwnerRecord]]:
    cur.execute("SELECT banushi_code, banushimei_hojinkaku, banushimei FROM nvd_bn")
    index: dict[str, set[OwnerRecord]] = {}
    for code, legal_name, owner_name in cur.fetchall():
        if not code.strip("0"):
            continue
        record = OwnerRecord(code=code, name=owner_name)
        for raw_name in (legal_name, owner_name):
            normalized = normalize_owner_name(raw_name or "")
            if normalized:
                index.setdefault(normalized, set()).add(record)
    return index


def resolve_owner(
    owner_name: str | None,
    horse: dict[str, object],
    index: dict[str, set[OwnerRecord]],
) -> OwnerRecord:
    if owner_name is None or not owner_name.strip():
        raise SystemExit("keiba.go.jp entry is missing its current owner")
    normalized = normalize_owner_name(owner_name)
    candidates = index.get(normalized, set())
    horse_code = str(horse.get("banushi_code") or "")
    horse_owner = str(horse.get("banushimei") or "")
    if horse_code.strip("0") and normalize_owner_name(horse_owner) == normalized:
        return OwnerRecord(code=horse_code, name=horse_owner)

    by_code: dict[str, OwnerRecord] = {}
    for candidate in sorted(candidates, key=owner_record_sort_key):
        by_code.setdefault(candidate.code, candidate)
    if horse_code in by_code:
        return by_code[horse_code]
    if len(by_code) == 1:
        return next(iter(by_code.values()))
    return OwnerRecord(code="000000", name=pad_name(owner_name, 64))


def insert_unknown_owner_masters(cur, *, ymd: str, owners: set[OwnerRecord]) -> None:
    for owner in owners:
        if owner.code != "000000":
            continue
        cur.execute(
            """
            INSERT INTO nvd_bn (
              record_id, data_kubun, data_sakusei_nengappi,
              banushi_code, banushimei_hojinkaku, banushimei
            ) VALUES ('BN', '2', %s, %s, %s, %s)
            ON CONFLICT (banushi_code, banushimei) DO UPDATE SET
              data_sakusei_nengappi=EXCLUDED.data_sakusei_nengappi,
              banushimei_hojinkaku=EXCLUDED.banushimei_hojinkaku
            """,
            (ymd, owner.code, owner.name, owner.name),
        )


def blank_ra(
    *,
    ymd: str,
    yobi: str,
    keibajo: str,
    kai: str,
    nichi: str,
    race_bango: str,
    name: str,
    hasso: str,
    kyori: str,
    track: str,
    toroku: int,
    shusso: int,
) -> dict:
    year, md = ymd[:4], ymd[4:]
    name_pad = (name + ("\u3000" * 60))[:60]
    return {
        "record_id": "RA",
        "data_kubun": "2",
        "data_sakusei_nengappi": ymd,
        "kaisai_nen": year,
        "kaisai_tsukihi": md,
        "keibajo_code": keibajo,
        "kaisai_kai": kai,
        "kaisai_nichime": nichi,
        "race_bango": race_bango,
        "yobi_code": yobi,
        "tokubetsu_kyoso_bango": "0000",
        "kyosomei_hondai": name_pad,
        "kyosomei_fukudai": "\u3000" * 60,
        "kyosomei_kakkonai": "\u3000" * 60,
        "kyosomei_hondai_eur": " " * 60,
        "kyosomei_fukudai_eur": " " * 60,
        "kyosomei_kakkonai_eur": " " * 60,
        "kyosomei_ryakusho_10": name_pad[:20],
        "kyosomei_ryakusho_6": name_pad[:12],
        "kyosomei_ryakusho_3": name_pad[:6],
        "kyosomei_kubun": "0",
        "jusho_kaiji": "000",
        "grade_code": " ",
        "grade_code_henkomae": " ",
        "kyoso_shubetsu_code": "49",
        "kyoso_kigo_code": "000",
        "juryo_shubetsu_code": "3",
        "kyoso_joken_code_2sai": "000",
        "kyoso_joken_code_3sai": "000",
        "kyoso_joken_code_4sai": "000",
        "kyoso_joken_code_5sai_ijo": "000",
        "kyoso_joken_code": "000",
        "kyoso_joken_meisho": ("\u3000" * 60)[:60],
        "kyori": kyori,
        "kyori_henkomae": "0000",
        "track_code": track,
        "track_code_henkomae": "00",
        "course_kubun": "  ",
        "course_kubun_henkomae": "  ",
        "honshokin": "0" * 56,
        "honshokin_henkomae": "0" * 40,
        "fukashokin": "0" * 40,
        "fukashokin_henkomae": "0" * 24,
        "hasso_jikoku": hasso,
        "hasso_jikoku_henkomae": "0000",
        "toroku_tosu": f"{toroku:02d}",
        "shusso_tosu": f"{shusso:02d}",
        "nyusen_tosu": "00",
        "tenko_code": "0",
        "babajotai_code_shiba": "0",
        "babajotai_code_dirt": "0",
        "lap_time": "0" * 75,
        "shogai_mile_time": "0000",
        "zenhan_3f": "000",
        "zenhan_4f": "000",
        "kohan_3f": "000",
        "kohan_4f": "000",
        "corner_tsuka_juni_1": "00" + " " * 70,
        "corner_tsuka_juni_2": "00" + " " * 70,
        "corner_tsuka_juni_3": "00" + " " * 70,
        "corner_tsuka_juni_4": "00" + " " * 70,
        "record_koshin_kubun": "0",
        "mining_kubun": "0",
        "yoso_soha_time": "00000",
        "yoso_gosa_plus": "0000",
        "yoso_gosa_minus": "0000",
        "yoso_juni": "00",
        "kyakushitsu_hantei": "0",
    }


def blank_se(
    *,
    ymd: str,
    keibajo: str,
    kai: str,
    nichi: str,
    race_bango: str,
    entry: Entry,
    horse: dict[str, object],
    kishu: tuple[str, str],
    chokyo: tuple[str, str],
    owner: OwnerRecord,
) -> dict:
    year, md = ymd[:4], ymd[4:]
    ijo = "1" if entry.status else "0"
    if entry.age:
        barei = f"{entry.age:02d}"
    elif horse and horse.get("barei"):
        barei = str(horse["barei"])
    else:
        barei = "00"
    return {
        "record_id": "SE",
        "data_kubun": "2",
        "data_sakusei_nengappi": ymd,
        "kaisai_nen": year,
        "kaisai_tsukihi": md,
        "keibajo_code": keibajo,
        "kaisai_kai": kai,
        "kaisai_nichime": nichi,
        "race_bango": race_bango,
        "wakuban": entry.wakuban[:1],
        "umaban": entry.umaban,
        "ketto_toroku_bango": horse.get("ketto_toroku_bango"),
        "bamei": pad_name(entry.bamei),
        "umakigo_code": horse.get("umakigo_code") or "00",
        "seibetsu_code": sex_code(entry.sex)
        if entry.sex
        else (horse.get("seibetsu_code") or "0"),
        "hinshu_code": horse.get("hinshu_code") or "1",
        "moshoku_code": horse.get("moshoku_code") or "00",
        "barei": barei,
        "tozai_shozoku_code": horse.get("tozai_shozoku_code") or "3",
        "chokyoshi_code": chokyo[0],
        "chokyoshimei_ryakusho": chokyo[1],
        "banushi_code": owner.code,
        "banushimei": owner.name,
        "fukushoku_hyoji": horse.get("fukushoku_hyoji") or ("\u3000" * 60),
        "yobi_1": "\u3000" * 60,
        "futan_juryo": futan_to_code(entry.futan),
        "futan_juryo_henkomae": "000",
        "blinker_shiyo_kubun": "0",
        "yobi_2": "0",
        "kishu_code": kishu[0],
        "kishu_code_henkomae": "00000",
        "kishumei_ryakusho": kishu[1],
        "kishumei_ryakusho_henkomae": "\u3000" * 8,
        "kishu_minarai_code": "0",
        "kishu_minarai_code_henkomae": "0",
        "bataiju": batai_to_code(entry.bataiju),
        "zogen_fugo": " ",
        "zogen_sa": "   ",
        "ijo_kubun_code": ijo,
        "nyusen_juni": "00",
        "kakutei_chakujun": "00",
        "dochaku_kubun": "0",
        "dochaku_tosu": "0",
        "soha_time": "0000",
        "chakusa_code_1": "   ",
        "chakusa_code_2": "   ",
        "chakusa_code_3": "   ",
        "corner_1": "00",
        "corner_2": "00",
        "corner_3": "00",
        "corner_4": "00",
        "tansho_odds": "0000",
        "tansho_ninkijun": "00",
        "kakutoku_honshokin": "00000000",
        "kakutoku_fukashokin": "00000000",
        "yobi_3": "000",
        "yobi_4": "000",
        "kohan_4f": "000",
        "kohan_3f": "000",
        "aiteuma_joho_1": "0000000000" + ("\u3000" * 18),
        "aiteuma_joho_2": "0000000000" + ("\u3000" * 18),
        "aiteuma_joho_3": "0000000000" + ("\u3000" * 18),
        "time_sa": "0000",
        "record_koshin_kubun": "0",
        "mining_kubun": "0",
        "yoso_soha_time": "00000",
        "yoso_gosa_plus": "0000",
        "yoso_gosa_minus": "0000",
        "yoso_juni": "00",
        "kyakushitsu_hantei": "0",
    }


def infer_kai_nichi(cur, keibajo: str, ymd: str) -> tuple[str, str]:
    """Infer kaisai_kai / kaisai_nichime from prior/next cards for the venue."""
    cur.execute(
        """
        SELECT kaisai_kai, kaisai_nichime, kaisai_nen||kaisai_tsukihi AS ymd
        FROM nvd_ra
        WHERE keibajo_code=%s AND kaisai_nen||kaisai_tsukihi < %s
        ORDER BY kaisai_nen||kaisai_tsukihi DESC
        LIMIT 1
        """,
        (keibajo, ymd),
    )
    prior = cur.fetchone()
    cur.execute(
        """
        SELECT kaisai_kai, kaisai_nichime, kaisai_nen||kaisai_tsukihi AS ymd
        FROM nvd_ra
        WHERE keibajo_code=%s AND kaisai_nen||kaisai_tsukihi > %s
        ORDER BY kaisai_nen||kaisai_tsukihi ASC
        LIMIT 1
        """,
        (keibajo, ymd),
    )
    nxt = cur.fetchone()

    if prior and nxt and prior[0] == nxt[0]:
        kai = prior[0]
        prior_n = int(prior[1])
        next_n = int(nxt[1])
        if next_n > prior_n + 1:
            return kai, f"{prior_n + 1:02d}"
        if next_n == prior_n + 1:
            # No gap; still place between by reusing prior+1 only if dates allow —
            # prefer prior+1 and warn via stderr if collision risk.
            return kai, f"{prior_n + 1:02d}"
        return kai, f"{max(prior_n + 1, next_n - 1):02d}"

    if prior and nxt and prior[0] != nxt[0]:
        # Target day is likely the first day of next kai (or last of prior).
        # Prefer last of prior when next is day 01 of a new kai with room.
        if int(nxt[1]) == 1:
            return prior[0], f"{int(prior[1]) + 1:02d}"
        return nxt[0], f"{max(1, int(nxt[1]) - 1):02d}"

    if prior:
        return prior[0], f"{int(prior[1]) + 1:02d}"
    if nxt:
        nichi = max(1, int(nxt[1]) - 1)
        return nxt[0], f"{nichi:02d}"
    raise SystemExit(
        f"Cannot infer kaisai_kai/nichime for keibajo={keibajo} date={ymd}; pass --meta"
    )


def parse_meta_args(meta_args: list[str]) -> dict[str, tuple[str, str]]:
    """Parse --meta baba=kai:nichi into {baba: (kai, nichi)}."""
    out: dict[str, tuple[str, str]] = {}
    for raw in meta_args:
        if "=" not in raw or ":" not in raw.split("=", 1)[1]:
            raise SystemExit(f"Invalid --meta {raw!r}; expected baba=kai:nichi")
        baba, rest = raw.split("=", 1)
        kai, nichi = rest.split(":", 1)
        baba = baba.strip().zfill(2) if baba.strip().isdigit() else baba.strip()
        if len(baba) == 1:
            baba = baba.zfill(2)
        out[baba] = (kai.strip().zfill(2), nichi.strip().zfill(2))
    return out


def resolve_venues(
    cur,
    *,
    ymd: str,
    discoveries: list[VenueDiscovery],
    meta: dict[str, tuple[str, str]],
) -> list[VenueTarget]:
    venues: list[VenueTarget] = []
    for discovery in discoveries:
        baba_norm = discovery.baba
        keibajo = NAR_BABA_TO_KEIBAJO.get(baba_norm)
        if not keibajo:
            raise SystemExit(
                f"Unknown babaCode={baba_norm}; add to NAR_BABA_TO_KEIBAJO"
            )
        if baba_norm in meta:
            kai, nichi = meta[baba_norm]
        else:
            kai, nichi = infer_kai_nichi(cur, keibajo, ymd)
            print(
                f"  inferred meeting keibajo={keibajo} kai={kai} nichi={nichi}",
                file=sys.stderr,
            )
        venues.append(
            VenueTarget(
                baba=baba_norm,
                keibajo=keibajo,
                label=discovery.label or KEIBAJO_LABEL.get(keibajo, keibajo),
                kai=kai,
                nichi=nichi,
                race_numbers=discovery.race_numbers,
            )
        )
    return venues


def insert_rows(cur, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
        (table,),
    )
    valid = {r[0] for r in cur.fetchall()}
    cols = [c for c in rows[0] if c in valid]
    trimmed = [{k: row[k] for k in cols} for row in rows]
    placeholders = ",".join([f"%({c})s" for c in cols])
    psycopg2.extras.execute_batch(
        cur,
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})",
        trimmed,
        page_size=100 if table == "nvd_se" else 50,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Scrape keiba.go.jp and import into local nvd_ra/nvd_se (data_kubun=2)"
    )
    p.add_argument("--date", required=True, help="YYYYMMDD")
    p.add_argument(
        "--baba",
        default=None,
        help="Optional comma-separated babaCode subset; default discovers all venues.",
    )
    p.add_argument(
        "--meta",
        action="append",
        default=[],
        help="Meeting override baba=kai:nichi (repeatable). Else inferred from DB.",
    )
    p.add_argument(
        "--cache-dir",
        default=None,
        help="HTML cache directory (default: <repo>/tmp/keiba-go-scrape/<date>)",
    )
    p.add_argument(
        "--env-file",
        default=str(LOCAL_ENV),
        help="Path to .env containing DATABASE_URL",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape and resolve only; do not DELETE/INSERT",
    )
    p.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore cached HTML and refetch every official page.",
    )
    p.add_argument(
        "--yobi",
        default=None,
        help="Override yobi_code (default: derived from --date)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    ymd = args.date.strip()
    if not re.fullmatch(r"\d{8}", ymd):
        raise SystemExit("--date must be YYYYMMDD")
    year, md = ymd[:4], ymd[4:]
    yobi = args.yobi or yobi_code_for_ymd(ymd)
    requested_babas = None
    if args.baba is not None:
        requested_babas = [b.strip() for b in args.baba.split(",") if b.strip()]
        if not requested_babas:
            raise SystemExit("--baba must list at least one babaCode")
    meta = parse_meta_args(args.meta)
    cache_dir = (
        Path(args.cache_dir)
        if args.cache_dir
        else REPO_ROOT / "tmp" / "keiba-go-scrape" / ymd
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    dsn = load_dsn(Path(args.env_file))
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    top_html = fetch(
        today_top_url(ymd), cache_dir / "today_top.html", refresh=args.refresh
    )
    discoveries = select_venue_discoveries(
        parse_today_venues(top_html, ymd), requested_babas
    )
    venues = resolve_venues(cur, ymd=ymd, discoveries=discoveries, meta=meta)
    venue_by_baba = {v.baba: v for v in venues}
    scraped: list[Race] = []

    for venue in venues:
        print(
            f"=== scrape {venue.label} baba={venue.baba} keibajo={venue.keibajo} "
            f"kai={venue.kai} nichi={venue.nichi} ==="
        )
        racelist_html = fetch(
            race_list_url(ymd, venue.baba),
            cache_dir / f"racelist_{venue.baba}.html",
            refresh=args.refresh,
        )
        races_meta = parse_racelist_races(racelist_html, venue.baba)
        meta_by_number = {race[0]: race[1:] for race in races_meta}
        listed_numbers = set(meta_by_number)
        discovered_numbers = set(venue.race_numbers)
        if listed_numbers != discovered_numbers:
            raise SystemExit(
                f"RaceList mismatch babaCode={venue.baba}: "
                f"top={sorted(discovered_numbers, key=int)} "
                f"raceList={sorted(listed_numbers, key=int)}"
            )
        print(f"  races listed: {len(venue.race_numbers)}")
        for rno in venue.race_numbers:
            rname, rdist = meta_by_number[rno]
            html = fetch(
                deba_table_url(ymd, venue.baba, rno),
                cache_dir / f"deba_{venue.baba}_{rno}.html",
                refresh=args.refresh,
            )
            hasso, name, surface_dist = parse_deba_meta(html)
            if not name:
                name = rname
            track, kyori = infer_track_code(
                cur, venue.keibajo, surface_dist or rdist, ymd
            )
            entries = parse_deba_entries(html)
            if not entries:
                raise SystemExit(
                    f"DebaTable has no entries date={ymd} babaCode={venue.baba} race={rno}"
                )
            race = Race(
                baba=venue.baba,
                keibajo=venue.keibajo,
                race_bango=f"{int(rno):02d}",
                name=name,
                hasso=hasso,
                kyori=kyori,
                track_code=track,
                entries=entries,
            )
            scraped.append(race)
            print(
                f"  R{race.race_bango} {hasso} {kyori}/{track} "
                f"entries={len(entries)} {name[:40]}"
            )

    summary_path = cache_dir / "scraped_summary.json"
    write_json_atomic(
        summary_path,
        [
            {
                "keibajo": race.keibajo,
                "race": race.race_bango,
                "hasso": race.hasso,
                "kyori": race.kyori,
                "track": race.track_code,
                "name": race.name,
                "n": len(race.entries),
            }
            for race in scraped
        ],
    )

    profile_cache = cache_dir.parent / "profiles"
    prefetch_entity_profiles(scraped, profile_cache=profile_cache, refresh=args.refresh)
    jockey_index = build_person_index(cur, "jockey", ymd)
    trainer_index = build_person_index(cur, "trainer", ymd)
    owner_index = build_owner_index(cur)
    target_codes = sorted({v.keibajo for v in venues})
    ra_rows: list[dict] = []
    se_rows: list[dict] = []
    jockey_masters: set[PersonRecord] = set()
    trainer_masters: set[PersonRecord] = set()
    owner_masters: set[OwnerRecord] = set()
    relation_rows: list[dict[str, str]] = []

    for race in scraped:
        venue = venue_by_baba[race.baba]
        ra_rows.append(
            blank_ra(
                ymd=ymd,
                yobi=yobi,
                keibajo=race.keibajo,
                kai=venue.kai,
                nichi=venue.nichi,
                race_bango=race.race_bango,
                name=race.name,
                hasso=race.hasso,
                kyori=race.kyori,
                track=race.track_code,
                toroku=len(race.entries),
                shusso=sum(entry.status is None for entry in race.entries),
            )
        )
        for entry in race.entries:
            horse = lookup_horse(cur, entry, ymd)
            if horse is None:
                raise SystemExit(
                    f"Cannot uniquely relate horse lineage={entry.lineage_login_code} "
                    f"name={entry.bamei!r} race={race.keibajo}-{race.race_bango}"
                )
            jockey = resolve_person(
                entity_type="jockey",
                license_no=entry.rider_license_no,
                index=jockey_index,
                profile_cache=profile_cache,
                refresh=args.refresh,
            )
            trainer = resolve_person(
                entity_type="trainer",
                license_no=entry.trainer_license_no,
                index=trainer_index,
                profile_cache=profile_cache,
                refresh=args.refresh,
            )
            owner = resolve_owner(entry.owner, horse, owner_index)
            jockey_masters.add(jockey)
            trainer_masters.add(trainer)
            owner_masters.add(owner)
            se_rows.append(
                blank_se(
                    ymd=ymd,
                    keibajo=race.keibajo,
                    kai=venue.kai,
                    nichi=venue.nichi,
                    race_bango=race.race_bango,
                    entry=entry,
                    horse=horse,
                    kishu=(jockey.code, jockey.short_name),
                    chokyo=(trainer.code, trainer.short_name),
                    owner=owner,
                )
            )
            relation_rows.append(
                {
                    "keibajo": race.keibajo,
                    "race": race.race_bango,
                    "umaban": entry.umaban,
                    "lineageLoginCode": entry.lineage_login_code or "",
                    "kettoTorokuBango": str(horse["ketto_toroku_bango"]),
                    "riderLicenseNo": entry.rider_license_no or "",
                    "kishuCode": jockey.code,
                    "trainerLicenseNo": entry.trainer_license_no or "",
                    "chokyoshiCode": trainer.code,
                    "ownerName": entry.owner or "",
                    "banushiCode": owner.code,
                }
            )

    result_fingerprint = rows_fingerprint(ra_rows, se_rows)
    fingerprint_path = cache_dir / "import_fingerprint.json"
    previous_fingerprint = None
    if fingerprint_path.exists():
        previous_payload = json.loads(fingerprint_path.read_text(encoding="utf-8"))
        if isinstance(previous_payload, dict):
            previous_fingerprint = previous_payload.get("fingerprint")
    relation_path = cache_dir / "entity_relations.json"
    if args.dry_run:
        dry_run_relation_path = cache_dir / "entity_relations.dry-run.json"
        dry_run_rows_path = cache_dir / "import_rows.dry-run.json"
        write_json_atomic(dry_run_relation_path, relation_rows)
        write_json_atomic(dry_run_rows_path, {"races": ra_rows, "entries": se_rows})
        print(
            f"dry-run: scraped {len(scraped)} races and resolved "
            f"{len(relation_rows)} entries; wrote {summary_path} and "
            f"{dry_run_relation_path}; fingerprint={result_fingerprint}; "
            f"idempotentWithCommitted={previous_fingerprint == result_fingerprint}"
        )
        conn.rollback()
        conn.close()
        return 0

    cur.execute(
        "DELETE FROM nvd_se WHERE kaisai_nen=%s AND kaisai_tsukihi=%s AND keibajo_code = ANY(%s)",
        (year, md, target_codes),
    )
    deleted_se = cur.rowcount
    cur.execute(
        "DELETE FROM nvd_ra WHERE kaisai_nen=%s AND kaisai_tsukihi=%s AND keibajo_code = ANY(%s)",
        (year, md, target_codes),
    )
    deleted_ra = cur.rowcount
    print(f"deleted existing target rows se={deleted_se} ra={deleted_ra}")

    insert_missing_person_masters(
        cur,
        ymd=ymd,
        jockeys=jockey_masters,
        trainers=trainer_masters,
    )
    insert_unknown_owner_masters(cur, ymd=ymd, owners=owner_masters)
    insert_rows(cur, "nvd_ra", ra_rows)
    insert_rows(cur, "nvd_se", se_rows)

    cur.execute(
        """
        SELECT keibajo_code, count(*) FROM nvd_ra
        WHERE kaisai_nen=%s AND kaisai_tsukihi=%s AND keibajo_code = ANY(%s)
        GROUP BY 1 ORDER BY 1
        """,
        (year, md, target_codes),
    )
    print("nvd_ra after", cur.fetchall())
    cur.execute(
        """
        SELECT keibajo_code, count(*) FROM nvd_se
        WHERE kaisai_nen=%s AND kaisai_tsukihi=%s AND keibajo_code = ANY(%s)
        GROUP BY 1 ORDER BY 1
        """,
        (year, md, target_codes),
    )
    print("nvd_se after", cur.fetchall())
    cur.execute(
        """
        SELECT
          count(*) FILTER (WHERE nu.ketto_toroku_bango IS NULL) AS horses,
          count(*) FILTER (WHERE ks.kishu_code IS NULL) AS jockeys,
          count(*) FILTER (WHERE ch.chokyoshi_code IS NULL) AS trainers,
          count(*) FILTER (WHERE bn.banushi_code IS NULL) AS owners
        FROM nvd_se se
        LEFT JOIN nvd_nu nu ON nu.ketto_toroku_bango=se.ketto_toroku_bango
        LEFT JOIN nvd_ks ks ON ks.kishu_code=se.kishu_code
        LEFT JOIN nvd_ch ch ON ch.chokyoshi_code=se.chokyoshi_code
        LEFT JOIN nvd_bn bn
          ON bn.banushi_code=se.banushi_code AND bn.banushimei=se.banushimei
        WHERE se.kaisai_nen=%s AND se.kaisai_tsukihi=%s
          AND se.keibajo_code=ANY(%s)
        """,
        (year, md, target_codes),
    )
    missing_relations = cur.fetchone()
    print("missing entity relations", missing_relations, "report", relation_path)
    if missing_relations != (0, 0, 0, 0):
        conn.rollback()
        raise SystemExit(f"Entity relation validation failed: {missing_relations}")
    conn.commit()
    write_json_atomic(relation_path, relation_rows)
    write_json_atomic(
        fingerprint_path,
        {
            "date": ymd,
            "fingerprint": result_fingerprint,
            "raceCount": len(ra_rows),
            "entryCount": len(se_rows),
        },
    )
    print(
        "import fingerprint",
        result_fingerprint,
        "idempotentWithPrevious",
        previous_fingerprint == result_fingerprint,
    )
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
