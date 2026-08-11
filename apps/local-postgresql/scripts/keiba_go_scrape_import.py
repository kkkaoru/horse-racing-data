#!/usr/bin/env python3
"""Scrape keiba.go.jp RaceList+DebaTable and upsert into local nvd_ra / nvd_se.

Example:
  bun run scrape:keiba-go -- --date 20260811 --baba 36,10,18,23 \\
    --meta 36=09:04 --meta 10=06:03 --meta 18=05:03 --meta 23=07:01

babaCode maps to local keibajo_code via NAR_BABA_TO_KEIBAJO (same as
packages/horse-racing-realtime). Meeting kai/nichime can be passed with
--meta baba=kai:nichi, or inferred from surrounding nvd_ra rows.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
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
class VenueTarget:
    baba: str
    keibajo: str
    label: str
    kai: str
    nichi: str


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


def fetch(url: str, dest: Path) -> str:
    if dest.exists() and dest.stat().st_size > 1000:
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


def pad_name(name: str, width: int = 36) -> str:
    name = name.replace("\u3000", "").strip()
    return (name + ("\u3000" * width))[:width]


def pad_short(name: str, width: int = 8) -> str:
    name = re.sub(r"（.*?）", "", name).replace("\u3000", "").strip()
    return (name + ("\u3000" * width))[:width]


def futan_to_code(kg: float | None) -> str:
    if kg is None:
        return "000"
    return f"{int(round(kg * 10)):03d}"


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
        rf'DebaTable\?[^"\']*k_raceNo=(\d+)[^"\']*k_babaCode={baba}', html, flags=re.I
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
    h4 = re.search(r"<h4[^>]*>([\s\S]*?)</h4>", html, flags=re.I)
    plain = strip_tags(h4.group(1) if h4 else "")
    time_m = re.search(r"(\d{1,2}):(\d{2})発走", plain)
    hasso = f"{int(time_m.group(1)):02d}{time_m.group(2)}" if time_m else "0000"
    h3 = re.search(r"<h3[^>]*>([\s\S]*?)</h3>", html, flags=re.I)
    name = strip_tags(h3.group(1) if h3 else "")
    course_m = re.search(r"(ダート|芝|障)[^\d]{0,8}(\d{3,4})", html)
    if course_m:
        surface, dist = course_m.group(1), course_m.group(2)
    else:
        surface, dist = "ダート", "0000"
    return hasso, name, surface + dist


def parse_deba_entries(html: str) -> list[Entry]:
    entries: list[Entry] = []
    parts = re.split(r'<tr[^>]*class=["\'][^"\']*tBorder[^"\']*["\'][^>]*>', html, flags=re.I)
    for block in parts[1:]:
        umaban = re.search(
            r'class=["\'][^"\']*horseNum[^"\']*["\'][^>]*>\s*(\d{1,2})\s*<', block, flags=re.I
        )
        if not umaban:
            continue
        waku = re.search(
            r'class=["\'][^"\']*courseNum[^"\']*["\'][^>]*>\s*(\d+)\s*<', block, flags=re.I
        )
        bamei_m = re.search(
            r'class=["\'][^"\']*horseName[^"\']*["\'][^>]*>([\s\S]*?)</a>', block, flags=re.I
        )
        jockey_m = re.search(
            r'class=["\'][^"\']*jockeyName[^"\']*["\'][^>]*>([\s\S]*?)</a>', block, flags=re.I
        )
        trainer = None
        trainer_m = re.search(r"調教師[^<]*</th>\s*<td[^>]*>([\s\S]*?)</td>", block, flags=re.I)
        if trainer_m:
            trainer = strip_tags(trainer_m.group(1))
        kg_line = re.search(r"(\d+)\s*人\s+(\d{3})\s+[^\d]{1,20}?(\d{2}\.\d)", block)
        bataiju = int(kg_line.group(2)) if kg_line else None
        futan = float(kg_line.group(3)) if kg_line else None
        if futan is None:
            futan_m = re.search(r"(\d{2}\.\d)\s*(?:kg)?", strip_tags(block))
            futan = float(futan_m.group(1)) if futan_m else None
        sex_age = re.search(r"(牡|牝|セ)\s*(\d{1,2})", block)
        status = None
        for label in ("取消", "除外", "スクラッチ", "出走取消"):
            if label in block:
                status = label
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
            )
        )
    return entries


def infer_track_code(cur, keibajo: str, surface_dist: str) -> tuple[str, str]:
    dirt = "ダート" in surface_dist or surface_dist.startswith("ダ")
    dist_m = re.search(r"(\d{3,4})", surface_dist)
    dist = dist_m.group(1).zfill(4) if dist_m else "0000"
    track_like = "2%" if dirt else "1%"
    cur.execute(
        """
        SELECT track_code, count(*) n FROM nvd_ra
        WHERE keibajo_code=%s AND kyori=%s AND track_code LIKE %s
          AND kaisai_nen||kaisai_tsukihi >= '20250101'
        GROUP BY 1 ORDER BY n DESC LIMIT 1
        """,
        (keibajo, dist, track_like),
    )
    row = cur.fetchone()
    if row:
        return row[0], dist
    cur.execute(
        """
        SELECT track_code, count(*) n FROM nvd_ra
        WHERE keibajo_code=%s AND track_code LIKE %s
          AND kaisai_nen||kaisai_tsukihi >= '20250101'
        GROUP BY 1 ORDER BY n DESC LIMIT 1
        """,
        (keibajo, track_like),
    )
    row = cur.fetchone()
    return (row[0] if row else ("24" if dirt else "11")), dist


def lookup_horse(cur, bamei: str) -> dict | None:
    key = bamei.replace("\u3000", "").strip()
    if not key:
        return None
    cur.execute(
        """
        SELECT ketto_toroku_bango, seibetsu_code, hinshu_code, moshoku_code, barei,
               tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho,
               banushi_code, banushimei, fukushoku_hyoji, umakigo_code
        FROM nvd_se
        WHERE replace(bamei,'　','') LIKE %s
        ORDER BY kaisai_nen DESC, kaisai_tsukihi DESC
        LIMIT 1
        """,
        (key + "%",),
    )
    row = cur.fetchone()
    if row:
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    cur.execute(
        """
        SELECT ketto_toroku_bango, seibetsu_code, hinshu_code, moshoku_code,
               NULL::text AS barei, NULL::text AS tozai_shozoku_code,
               NULL::text AS chokyoshi_code, NULL::text AS chokyoshimei_ryakusho,
               NULL::text AS banushi_code, NULL::text AS banushimei,
               NULL::text AS fukushoku_hyoji, '00'::text AS umakigo_code
        FROM nvd_nu
        WHERE replace(bamei,'　','') LIKE %s
        ORDER BY ketto_toroku_bango DESC
        LIMIT 1
        """,
        (key + "%",),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def lookup_jockey(cur, name: str | None) -> tuple[str, str]:
    if not name:
        return "00000", "　　　　　　"
    key = re.sub(r"（.*?）", "", name).replace("\u3000", "").replace(" ", "").strip()
    cur.execute(
        """
        SELECT kishu_code, kishumei_ryakusho FROM nvd_se
        WHERE replace(replace(kishumei_ryakusho,'　',''),' ','') LIKE %s
        ORDER BY kaisai_nen DESC, kaisai_tsukihi DESC LIMIT 1
        """,
        (key[:3] + "%",),
    )
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return "00000", pad_short(key, 8)


def lookup_trainer(cur, name: str | None, horse: dict | None) -> tuple[str, str]:
    if horse and horse.get("chokyoshi_code") and str(horse["chokyoshi_code"]).strip("0"):
        return horse["chokyoshi_code"], horse.get("chokyoshimei_ryakusho") or "　　　　　　"
    if not name:
        return "00000", "　　　　　　"
    key = re.sub(r"（.*?）", "", name).replace("\u3000", "").strip()
    cur.execute(
        """
        SELECT chokyoshi_code, chokyoshimei_ryakusho FROM nvd_se
        WHERE replace(chokyoshimei_ryakusho,'　','') LIKE %s
        ORDER BY kaisai_nen DESC, kaisai_tsukihi DESC LIMIT 1
        """,
        (key[:3] + "%",),
    )
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return "00000", pad_short(key, 8)


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
        "shusso_tosu": "00",
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
    horse: dict | None,
    kishu: tuple[str, str],
    chokyo: tuple[str, str],
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
        "ketto_toroku_bango": (horse or {}).get("ketto_toroku_bango") or "0000000000",
        "bamei": pad_name(entry.bamei),
        "umakigo_code": (horse or {}).get("umakigo_code") or "00",
        "seibetsu_code": sex_code(entry.sex)
        if entry.sex
        else ((horse or {}).get("seibetsu_code") or "0"),
        "hinshu_code": (horse or {}).get("hinshu_code") or "1",
        "moshoku_code": (horse or {}).get("moshoku_code") or "00",
        "barei": barei,
        "tozai_shozoku_code": (horse or {}).get("tozai_shozoku_code") or "3",
        "chokyoshi_code": chokyo[0],
        "chokyoshimei_ryakusho": chokyo[1],
        "banushi_code": (horse or {}).get("banushi_code") or "000000",
        "banushimei": (horse or {}).get("banushimei") or ("\u3000" * 64),
        "fukushoku_hyoji": (horse or {}).get("fukushoku_hyoji") or ("\u3000" * 60),
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
    baba_list: list[str],
    meta: dict[str, tuple[str, str]],
) -> list[VenueTarget]:
    venues: list[VenueTarget] = []
    for baba in baba_list:
        baba_norm = baba.zfill(2) if baba.isdigit() else baba
        keibajo = NAR_BABA_TO_KEIBAJO.get(baba_norm)
        if not keibajo:
            raise SystemExit(f"Unknown babaCode={baba_norm}; add to NAR_BABA_TO_KEIBAJO")
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
                label=KEIBAJO_LABEL.get(keibajo, keibajo),
                kai=kai,
                nichi=nichi,
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
    cols = [c for c in rows[0].keys() if c in valid]
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
        required=True,
        help="Comma-separated keiba.go.jp babaCode list (e.g. 36,10,18,23)",
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
    baba_list = [b.strip() for b in args.baba.split(",") if b.strip()]
    if not baba_list:
        raise SystemExit("--baba must list at least one babaCode")
    meta = parse_meta_args(args.meta)
    cache_dir = Path(args.cache_dir) if args.cache_dir else REPO_ROOT / "tmp" / "keiba-go-scrape" / ymd
    cache_dir.mkdir(parents=True, exist_ok=True)

    dsn = load_dsn(Path(args.env_file))
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    venues = resolve_venues(cur, ymd=ymd, baba_list=baba_list, meta=meta)
    venue_by_baba = {v.baba: v for v in venues}
    scraped: list[Race] = []
    unresolved: list[dict] = []
    date_q = race_date_query(ymd)

    for venue in venues:
        print(
            f"=== scrape {venue.label} baba={venue.baba} keibajo={venue.keibajo} "
            f"kai={venue.kai} nichi={venue.nichi} ==="
        )
        racelist_url = (
            "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?"
            f"k_raceDate={date_q}&k_babaCode={venue.baba}"
        )
        racelist_html = fetch(racelist_url, cache_dir / f"racelist_{venue.baba}.html")
        races_meta = parse_racelist_races(racelist_html, venue.baba)
        print(f"  races listed: {len(races_meta)}")
        for rno, rname, rdist in races_meta:
            deba_url = (
                "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?"
                f"k_raceDate={date_q}&k_raceNo={rno}&k_babaCode={venue.baba}"
            )
            html = fetch(deba_url, cache_dir / f"deba_{venue.baba}_{rno}.html")
            hasso, name, surface_dist = parse_deba_meta(html)
            if not name:
                name = rname
            track, kyori = infer_track_code(cur, venue.keibajo, surface_dist or rdist)
            entries = parse_deba_entries(html)
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
    summary_path.write_text(
        json.dumps(
            [
                {
                    "keibajo": r.keibajo,
                    "race": r.race_bango,
                    "hasso": r.hasso,
                    "kyori": r.kyori,
                    "track": r.track_code,
                    "name": r.name,
                    "n": len(r.entries),
                }
                for r in scraped
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    if args.dry_run:
        print(f"dry-run: scraped {len(scraped)} races; wrote {summary_path}")
        conn.rollback()
        conn.close()
        return 0

    target_codes = sorted({v.keibajo for v in venues})
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

    ra_rows: list[dict] = []
    se_rows: list[dict] = []
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
            )
        )
        for entry in race.entries:
            horse = lookup_horse(cur, entry.bamei)
            if horse is None:
                unresolved.append(
                    {
                        "keibajo": race.keibajo,
                        "race": race.race_bango,
                        "umaban": entry.umaban,
                        "bamei": entry.bamei,
                    }
                )
            kishu = lookup_jockey(cur, entry.jockey)
            chokyo = lookup_trainer(cur, entry.trainer, horse)
            se_rows.append(
                blank_se(
                    ymd=ymd,
                    keibajo=race.keibajo,
                    kai=venue.kai,
                    nichi=venue.nichi,
                    race_bango=race.race_bango,
                    entry=entry,
                    horse=horse,
                    kishu=kishu,
                    chokyo=chokyo,
                )
            )

    insert_rows(cur, "nvd_ra", ra_rows)
    insert_rows(cur, "nvd_se", se_rows)
    conn.commit()

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
    unresolved_path = cache_dir / "unresolved.json"
    unresolved_path.write_text(
        json.dumps(unresolved, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("unresolved horses", len(unresolved), "->", unresolved_path)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
