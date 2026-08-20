#!/usr/bin/env python3
# pyright: reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingParameterType=false, reportArgumentType=false, reportAttributeAccessIssue=false, reportIndexIssue=false, reportCallIssue=false, reportOperatorIssue=false
"""Train an overseas finish-position LightGBM model and predict tonight's race.

Standalone script — no project-internal imports.

Usage:
  cd apps/pc-keiba-viewer
  set -a; . ../local-postgresql/.env; set +a
  uv run python src/scripts/train_and_predict_finish_position_overseas.py
  bun run predict:jacques-le-marois
  uv run python src/scripts/train_and_predict_finish_position_overseas.py \\
    --predict-only --card jacques-le-marois \\
    --jra-url URL --netkeiba-url URL --jravan-url URL
"""
from __future__ import annotations

import argparse
import os
import pickle
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen
import json

import lightgbm as lgb
import pandas as pd
import psycopg
from sklearn.metrics import mean_absolute_error, roc_auc_score

from keiba_archive_records import (
    apply_source_urls,
    attach_owner_stems_from_horse_pages,
    attach_trainer_stems_from_jravan,
    extract_jra_shutuba_url,
    fetch_netkeiba_horse_owner,
    fetch_url_text,
    load_archive_people_by_umaban,
    load_netkeiba_horse_page_owners,
    optimize_published_names,
    parse_jra_card_json,
    parse_jra_overseas_sale_list,
    parse_jravan_racecard_trainers,
    planned_card_jra_url,
    planned_overseas_card,
    prefer_richer_person,
    prepare_overseas_card,
    read_html_source,
    remember_source_url_combo,
    sale_list_match,
    source_urls_from_card,
    JRA_OVERSEAS_SALE_LIST_URL_TEMPLATE,
    PLANNED_OVERSEAS_CARDS,
    OverseasPrepareStatus,
    OverseasSourceUrls,
)
from overseas_finish_features import (
    FormSummary,
    InvertedScoreModel,
    PersonSummary,
    attach_tansho_odds,
    engineer_features,
    score_overseas_card,
    summarize_netkeiba_form,
    summarize_netkeiba_person,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
APP_DIR = SCRIPT_DIR.parent.parent  # apps/pc-keiba-viewer
REPO_ROOT = APP_DIR.parent.parent  # repo root
MODEL_DIR = APP_DIR / "tmp" / "models" / "overseas-fp-v3"
DEFAULT_MODEL_VERSION = "overseas-lgbm-fp-v3"
LIVE_ODDS_API_BASE = "https://sync-realtime-data-hot.kkk4oru.com/api/odds"
LIVE_ODDS_USER_AGENT = "pc-keiba-viewer-overseas-fp/1.0"
DEFAULT_KEIBA_DATA_SOURCES_DIR = Path("/Users/kkk4oru/ghq/github.com/kkkaoru/keiba/src/data_sources")
KEIBA_DATA_SOURCES_ENV = "KEIBA_DATA_SOURCES_DIR"
JRA_CARD_HTML_ENV = "OVERSEA_JRA_CARD_HTML"
JRA_CARD_JSON_ENV = "OVERSEA_JRA_CARD_JSON"
NETKEIBA_CARD_HTML_ENV = "OVERSEA_NETKEIBA_CARD_HTML"
NETKEIBA_HORSE_HTML_DIR_ENV = "OVERSEA_NETKEIBA_HORSE_HTML_DIR"
JRA_CARD_URL_ENV = "OVERSEA_JRA_CARD_URL"
NETKEIBA_CARD_URL_ENV = "OVERSEA_NETKEIBA_CARD_URL"
JRAVAN_CARD_HTML_ENV = "OVERSEA_JRAVAN_CARD_HTML"
JRAVAN_CARD_URL_ENV = "OVERSEA_JRAVAN_CARD_URL"
JRA_RACE_PAGE_URL_ENV = "OVERSEA_JRA_RACE_PAGE_URL"
SOURCE_CACHE_DIR_ENV = "OVERSEA_SOURCE_CACHE_DIR"
DEFAULT_SOURCE_CACHE_DIR: Path = APP_DIR / "tmp" / "overseas-source-cache"
EMPTY_SOURCE_URLS: OverseasSourceUrls = OverseasSourceUrls()
KNOWN_JRA_CARD_URLS: dict[tuple[str, str, str, str], str] = {
    (
        card.kaisai_nen,
        card.kaisai_tsukihi,
        card.keibajo_code,
        card.race_bango,
    ): planned_card_jra_url(card)
    for card in PLANNED_OVERSEAS_CARDS
    if planned_card_jra_url(card) != ""
}
KNOWN_NETKEIBA_CARD_URLS: dict[tuple[str, str, str, str], str] = {
    (
        card.kaisai_nen,
        card.kaisai_tsukihi,
        card.keibajo_code,
        card.race_bango,
    ): card.netkeiba_card_url
    for card in PLANNED_OVERSEAS_CARDS
    if card.netkeiba_card_url != ""
}
KNOWN_JRAVAN_CARD_URLS: dict[tuple[str, str, str, str], str] = {
    (
        card.kaisai_nen,
        card.kaisai_tsukihi,
        card.keibajo_code,
        card.race_bango,
    ): card.jravan_card_url
    for card in PLANNED_OVERSEAS_CARDS
    if card.jravan_card_url != ""
}
DISCOVER_OVERSEAS_SQL = """\
SELECT DISTINCT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
FROM jvd_ra
WHERE keibajo_code ~ '^[A-Z]'
  AND kaisai_nen = %(kaisai_nen)s
  AND kaisai_tsukihi = %(kaisai_tsukihi)s
ORDER BY race_bango
"""
JV_TANSHO_ODDS_SCALE = 10.0
RACE_SPLIT_TRAIN_FRACTION = 0.8

TRAINING_SQL = """\
SELECT s.kaisai_nen,
       s.kaisai_tsukihi,
       s.keibajo_code,
       s.race_bango,
       s.umaban,
       trim(s.kakutei_chakujun) AS finish_position,
       trim(s.futan_juryo)      AS weight_raw,
       trim(s.barei)            AS age_raw,
       s.seibetsu_code          AS sex,
       s.kishu_code,
       s.chokyoshi_code,
       s.ketto_toroku_bango,
       trim(s.tansho_odds)      AS tansho_odds,
       trim(s.tansho_ninkijun)  AS tansho_ninkijun,
       r.kyori,
       r.track_code,
       trim(r.grade_code)       AS grade,
       trim(r.shusso_tosu)      AS field_size_raw
FROM jvd_se s
JOIN jvd_ra r
  ON s.kaisai_nen     = r.kaisai_nen
 AND s.kaisai_tsukihi = r.kaisai_tsukihi
 AND s.keibajo_code   = r.keibajo_code
 AND s.race_bango     = r.race_bango
WHERE s.data_kubun = 'B'
  AND trim(s.kakutei_chakujun) NOT IN ('', '00')
  AND trim(s.futan_juryo) NOT IN ('', '000')
"""

NETKEIBA_SOURCE_ID_SQL = """\
SELECT umaban, source_horse_id, source_jockey_id, source_trainer_id, source_owner_id
FROM oversea_runner_source_id
WHERE race_source = 'jra'
  AND kaisai_nen = %(kaisai_nen)s
  AND kaisai_tsukihi = %(kaisai_tsukihi)s
  AND keibajo_code = %(keibajo_code)s
  AND race_bango = %(race_bango)s
  AND source = 'netkeiba'
"""

NETKEIBA_HISTORY_SQL = """\
SELECT source, source_horse_id, race_date, finish_position, distance_metres,
       race_day_sequence, race_name
FROM oversea_horse_race_history
WHERE source = 'netkeiba'
  AND source_horse_id = ANY(%s)
"""

NETKEIBA_PERSON_HISTORY_SQL = """\
SELECT source, person_kind, source_person_id, race_date, finish_position, race_name
FROM oversea_person_race_history
WHERE source = 'netkeiba'
  AND source_person_id = ANY(%s)
"""

IDENTITY_NAMES_SQL = """\
SELECT umaban, trainer_name_full, owner_name_full
FROM oversea_runner_identity
WHERE kaisai_nen = %(kaisai_nen)s
  AND kaisai_tsukihi = %(kaisai_tsukihi)s
  AND keibajo_code = %(keibajo_code)s
  AND race_bango = %(race_bango)s
ORDER BY umaban
"""

PREDICT_SQL_TEMPLATE = """\
SELECT s.umaban,
       trim(s.kakutei_chakujun) AS finish_position,
       trim(s.futan_juryo)      AS weight_raw,
       trim(s.barei)            AS age_raw,
       s.seibetsu_code          AS sex,
       s.kishu_code,
       s.chokyoshi_code,
       s.ketto_toroku_bango,
       trim(s.tansho_odds)      AS tansho_odds,
       trim(s.tansho_ninkijun)  AS tansho_ninkijun,
       r.kyori,
       r.track_code,
       trim(r.grade_code)       AS grade,
       trim(r.shusso_tosu)      AS field_size_raw
FROM jvd_se s
JOIN jvd_ra r
  ON s.kaisai_nen     = r.kaisai_nen
 AND s.kaisai_tsukihi = r.kaisai_tsukihi
 AND s.keibajo_code   = r.keibajo_code
 AND s.race_bango     = r.race_bango
WHERE s.data_kubun = 'B'
  AND s.kaisai_nen     = %(kaisai_nen)s
  AND s.kaisai_tsukihi = %(kaisai_tsukihi)s
  AND s.keibajo_code   = %(keibajo_code)s
  AND s.race_bango     = %(race_bango)s
"""

CREATE_PREDICTIONS_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS race_finish_position_model_predictions (
    model_version              text        NOT NULL,
    source                     text        NOT NULL,
    kaisai_nen                 text        NOT NULL,
    kaisai_tsukihi             text        NOT NULL,
    keibajo_code               text        NOT NULL,
    race_bango                 text        NOT NULL,
    ketto_toroku_bango         text        NOT NULL,
    umaban                     integer     NOT NULL,
    predicted_score            numeric,
    predicted_rank             integer,
    predicted_top1_prob        numeric,
    predicted_top3_prob        numeric,
    predicted_finish_position  numeric,
    prediction_generated_at    timestamp with time zone,
    distance_band              text,
    field_size_band            text,
    season_band                text,
    class_code                 text,
    surface                    text,
    PRIMARY KEY (model_version, source, kaisai_nen, kaisai_tsukihi,
                 keibajo_code, race_bango, ketto_toroku_bango)
);
"""

CREATE_ACTIVE_MODELS_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS finish_position_active_models (
    category      text PRIMARY KEY,
    model_version text NOT NULL,
    activated_at  timestamp with time zone NOT NULL
);
"""

UPSERT_PREDICTION_SQL = """\
INSERT INTO race_finish_position_model_predictions (
    model_version, source, kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, ketto_toroku_bango, umaban,
    predicted_score, predicted_rank, predicted_top1_prob,
    predicted_top3_prob, predicted_finish_position,
    prediction_generated_at, distance_band, field_size_band,
    season_band, class_code, surface
) VALUES (
    %(model_version)s, %(source)s, %(kaisai_nen)s, %(kaisai_tsukihi)s,
    %(keibajo_code)s, %(race_bango)s, %(ketto_toroku_bango)s, %(umaban)s,
    %(predicted_score)s, %(predicted_rank)s, %(predicted_top1_prob)s,
    %(predicted_top3_prob)s, %(predicted_finish_position)s,
    %(prediction_generated_at)s, %(distance_band)s, %(field_size_band)s,
    %(season_band)s, %(class_code)s, %(surface)s
)
ON CONFLICT (model_version, source, kaisai_nen, kaisai_tsukihi,
             keibajo_code, race_bango, ketto_toroku_bango)
DO UPDATE SET
    umaban                    = EXCLUDED.umaban,
    predicted_score           = EXCLUDED.predicted_score,
    predicted_rank            = EXCLUDED.predicted_rank,
    predicted_top1_prob       = EXCLUDED.predicted_top1_prob,
    predicted_top3_prob       = EXCLUDED.predicted_top3_prob,
    predicted_finish_position = EXCLUDED.predicted_finish_position,
    prediction_generated_at   = EXCLUDED.prediction_generated_at,
    distance_band             = EXCLUDED.distance_band,
    field_size_band           = EXCLUDED.field_size_band,
    season_band               = EXCLUDED.season_band,
    class_code                = EXCLUDED.class_code,
    surface                   = EXCLUDED.surface
"""

UPSERT_ACTIVE_MODEL_SQL = """\
INSERT INTO finish_position_active_models (category, model_version, activated_at)
VALUES (%(category)s, %(model_version)s, %(activated_at)s)
ON CONFLICT (category)
DO UPDATE SET model_version = EXCLUDED.model_version,
              activated_at  = EXCLUDED.activated_at
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[overseas-fp] {msg}", flush=True)


def read_env_file(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE .env file (no shell expansion)."""
    env: dict[str, str] = {}
    if not path.is_file():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        env[key] = value
    return env


def default_pg_url() -> str:
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    db = os.environ.get("POSTGRES_DB", "horse_racing")
    return f"postgresql://{user}:{password}@127.0.0.1:15432/{db}"


def default_neon_url() -> str | None:
    # Try repo root .env
    env = read_env_file(REPO_ROOT / ".env")
    url = env.get("NEON_PRIMARY_URL")
    if url:
        return url
    # Fallback: environment variable
    return os.environ.get("NEON_PRIMARY_URL")


def save_model(model: object, name: str) -> Path:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    path = MODEL_DIR / f"{name}.pkl"
    with open(path, "wb") as f:
        pickle.dump(model, f)
    log(f"Saved model: {path}")
    return path


def load_saved_models() -> (
    tuple[InvertedScoreModel | lgb.LGBMRegressor, lgb.LGBMClassifier, lgb.LGBMClassifier] | None
):
    regressor_path = MODEL_DIR / "regressor.pkl"
    top1_path = MODEL_DIR / "classifier_top1.pkl"
    top3_path = MODEL_DIR / "classifier_top3.pkl"
    if not regressor_path.is_file() or not top1_path.is_file() or not top3_path.is_file():
        return None
    with regressor_path.open("rb") as handle:
        regressor = pickle.load(handle)
    with top1_path.open("rb") as handle:
        clf_top1 = pickle.load(handle)
    with top3_path.open("rb") as handle:
        clf_top3 = pickle.load(handle)
    if not isinstance(regressor, (InvertedScoreModel, lgb.LGBMRegressor)):
        return None
    if not isinstance(clf_top1, lgb.LGBMClassifier):
        return None
    if not isinstance(clf_top3, lgb.LGBMClassifier):
        return None
    log(f"Loaded saved models from {MODEL_DIR}")
    return regressor, clf_top1, clf_top3


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
def step1_extract_training_data(pg_url: str) -> pd.DataFrame:
    log("STEP 1: Extracting training data ...")
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(TRAINING_SQL)
            description = cur.description
            columns = [column.name for column in description] if description else []
            rows = cur.fetchall()
    df = decode_jv_tansho_odds(pd.DataFrame(rows, columns=columns))
    log(f"  Extracted {len(df)} overseas runner rows with finish positions")
    return df


def _race_id_series(df: pd.DataFrame) -> pd.Series:
    return (
        df["kaisai_nen"].astype(str)
        + df["kaisai_tsukihi"].astype(str)
        + df["keibajo_code"].astype(str)
        + df["race_bango"].astype(str)
    )


def _kaisai_race_date(*, kaisai_nen: str, kaisai_tsukihi: str) -> str:
    return f"{kaisai_nen}{kaisai_tsukihi}"


def _card_distance(runners: pd.DataFrame) -> int:
    kyori = pd.to_numeric(runners["kyori"], errors="coerce")
    if kyori.notna().any():
        return int(kyori.dropna().iloc[0])
    return 0


def _fetch_predict_runners(
    conn: psycopg.Connection,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            PREDICT_SQL_TEMPLATE,
            {
                "kaisai_nen": kaisai_nen,
                "kaisai_tsukihi": kaisai_tsukihi,
                "keibajo_code": keibajo_code,
                "race_bango": race_bango,
            },
        )
        description = cur.description
        columns = [column.name for column in description] if description else []
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _load_netkeiba_form_by_umaban(
    conn: psycopg.Connection,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
    race_date: str,
    target_distance: int,
) -> dict[int, FormSummary]:
    """Load netkeiba mappings + history. Missing tables become empty form."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                NETKEIBA_SOURCE_ID_SQL,
                {
                    "kaisai_nen": kaisai_nen,
                    "kaisai_tsukihi": kaisai_tsukihi,
                    "keibajo_code": keibajo_code,
                    "race_bango": race_bango,
                },
            )
            mappings = cur.fetchall()
        if not mappings:
            return {}
        source_ids = [str(row[1]).strip() for row in mappings]
        with conn.cursor() as cur:
            cur.execute(NETKEIBA_HISTORY_SQL, (source_ids,))
            history_rows = cur.fetchall()
            description = cur.description
            columns = [column.name for column in description] if description else []
    except psycopg.Error as error:
        log(f"  Oversea form unavailable ({type(error).__name__}): {error}")
        return {}
    history = pd.DataFrame(history_rows, columns=columns)
    form_by_umaban: dict[int, FormSummary] = {}
    for row in mappings:
        umaban_raw = row[0]
        source_horse_id = row[1]
        try:
            umaban = int(str(umaban_raw).strip())
        except ValueError:
            continue
        form_by_umaban[umaban] = summarize_netkeiba_form(
            history,
            source_horse_id=str(source_horse_id).strip(),
            race_date=race_date,
            target_distance=target_distance,
        )
    return form_by_umaban


def _load_netkeiba_people_by_umaban(
    conn: psycopg.Connection,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
    race_date: str,
) -> tuple[dict[int, PersonSummary], dict[int, PersonSummary], dict[int, PersonSummary]]:
    empty: tuple[dict[int, PersonSummary], dict[int, PersonSummary], dict[int, PersonSummary]] = (
        {},
        {},
        {},
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                NETKEIBA_SOURCE_ID_SQL,
                {
                    "kaisai_nen": kaisai_nen,
                    "kaisai_tsukihi": kaisai_tsukihi,
                    "keibajo_code": keibajo_code,
                    "race_bango": race_bango,
                },
            )
            mappings = cur.fetchall()
        if not mappings:
            return empty
        person_ids = [
            str(value).strip()
            for row in mappings
            for value in row[2:]
            if value is not None and str(value).strip() != ""
        ]
        history_rows: list[tuple[object, ...]] = []
        columns: list[str] = []
        if person_ids:
            with conn.cursor() as cur:
                cur.execute(NETKEIBA_PERSON_HISTORY_SQL, (person_ids,))
                history_rows = cur.fetchall()
                description = cur.description
                columns = [column.name for column in description] if description else []
    except psycopg.Error as error:
        log(f"  Oversea person stats unavailable ({type(error).__name__}): {error}")
        return empty
    history = pd.DataFrame(history_rows, columns=columns)
    jockeys: dict[int, PersonSummary] = {}
    trainers: dict[int, PersonSummary] = {}
    owners: dict[int, PersonSummary] = {}
    for row in mappings:
        try:
            umaban = int(str(row[0]).strip())
        except ValueError:
            continue
        jockey_id = "" if row[2] is None else str(row[2]).strip()
        trainer_id = "" if row[3] is None else str(row[3]).strip()
        owner_id = "" if row[4] is None else str(row[4]).strip()
        if jockey_id != "":
            jockeys[umaban] = summarize_netkeiba_person(
                history,
                person_kind="jockey",
                source_person_id=jockey_id,
                race_date=race_date,
            )
        if trainer_id != "":
            trainers[umaban] = summarize_netkeiba_person(
                history,
                person_kind="trainer",
                source_person_id=trainer_id,
                race_date=race_date,
            )
        if owner_id != "":
            owners[umaban] = summarize_netkeiba_person(
                history,
                person_kind="owner",
                source_person_id=owner_id,
                race_date=race_date,
            )
    return jockeys, trainers, owners


def _read_optional_html(env_key: str) -> str:
    path_value = os.environ.get(env_key, "")
    if path_value == "":
        return ""
    return read_html_source(path_value)


def _race_key(
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> tuple[str, str, str, str]:
    return (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)


def _first_nonempty(*values: str) -> str:
    for value in values:
        if value != "":
            return value
    return ""


def _known_source_url(
    known: dict[tuple[str, str, str, str], str],
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> str:
    return known.get(
        _race_key(
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
        ),
        "",
    )


def _source_cache_dir() -> Path:
    env_dir = os.environ.get(SOURCE_CACHE_DIR_ENV, "")
    if env_dir != "":
        return Path(env_dir)
    return DEFAULT_SOURCE_CACHE_DIR


def _html_from_url_or_cache(*, html_env: str, url: str, cache_dir: Path) -> str:
    cached = _read_optional_html(html_env)
    if cached != "":
        return cached
    if url == "":
        return ""
    return read_html_source(url, cache_dir=cache_dir)


def _jra_card_url_for_race(*, cli_url: str, known_url: str, cache_dir: Path) -> str:
    url = _first_nonempty(cli_url, os.environ.get(JRA_CARD_URL_ENV, ""), known_url)
    if url != "":
        return url
    race_page = os.environ.get(JRA_RACE_PAGE_URL_ENV, "")
    if race_page == "":
        return ""
    return extract_jra_shutuba_url(read_html_source(race_page, cache_dir=cache_dir))


def _fetch_published_html_for_race(
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
    source_urls: OverseasSourceUrls,
) -> tuple[str, str, str]:
    cache_dir = _source_cache_dir()
    jra_url = _jra_card_url_for_race(
        cli_url=source_urls.jra_url,
        known_url=_known_source_url(
            KNOWN_JRA_CARD_URLS,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
        ),
        cache_dir=cache_dir,
    )
    netkeiba_url = _first_nonempty(
        source_urls.netkeiba_url,
        os.environ.get(NETKEIBA_CARD_URL_ENV, ""),
        _known_source_url(
            KNOWN_NETKEIBA_CARD_URLS,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
        ),
    )
    jravan_url = _first_nonempty(
        source_urls.jravan_url,
        os.environ.get(JRAVAN_CARD_URL_ENV, ""),
        _known_source_url(
            KNOWN_JRAVAN_CARD_URLS,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
        ),
    )
    remember_source_url_combo(
        cache_dir,
        OverseasSourceUrls(jra_url=jra_url, netkeiba_url=netkeiba_url, jravan_url=jravan_url),
    )
    jra_html = _html_from_url_or_cache(
        html_env=JRA_CARD_HTML_ENV,
        url=jra_url,
        cache_dir=cache_dir,
    )
    netkeiba_html = _html_from_url_or_cache(
        html_env=NETKEIBA_CARD_HTML_ENV,
        url=netkeiba_url,
        cache_dir=cache_dir,
    )
    jravan_html = _html_from_url_or_cache(
        html_env=JRAVAN_CARD_HTML_ENV,
        url=jravan_url,
        cache_dir=cache_dir,
    )
    return jra_html, netkeiba_html, jravan_html


def _published_names_from_identity(
    conn: psycopg.Connection,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> tuple[list[str], list[str]]:
    try:
        with conn.cursor() as cur:
            cur.execute(
                IDENTITY_NAMES_SQL,
                {
                    "kaisai_nen": kaisai_nen,
                    "kaisai_tsukihi": kaisai_tsukihi,
                    "keibajo_code": keibajo_code,
                    "race_bango": race_bango,
                },
            )
            rows = cur.fetchall()
    except psycopg.Error:
        return [], []
    trainers = ["" if row[1] is None else str(row[1]).strip() for row in rows]
    owners = ["" if row[2] is None else str(row[2]).strip() for row in rows]
    return trainers, owners


def _owner_names_by_umaban(
    conn: psycopg.Connection,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
    owners_by_horse_id: dict[str, str],
) -> dict[int, str]:
    if not owners_by_horse_id:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                NETKEIBA_SOURCE_ID_SQL,
                {
                    "kaisai_nen": kaisai_nen,
                    "kaisai_tsukihi": kaisai_tsukihi,
                    "keibajo_code": keibajo_code,
                    "race_bango": race_bango,
                },
            )
            rows = cur.fetchall()
    except psycopg.Error:
        return {}
    mapped: dict[int, str] = {}
    for row in rows:
        umaban = int(str(row[0]).strip())
        horse_id = "" if row[1] is None else str(row[1]).strip()
        owner_name = owners_by_horse_id.get(horse_id, "")
        if owner_name != "":
            mapped[umaban] = owner_name
    return mapped


def _fetch_owner_names_from_source_ids(
    conn: psycopg.Connection,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> dict[str, str]:
    try:
        with conn.cursor() as cur:
            cur.execute(
                NETKEIBA_SOURCE_ID_SQL,
                {
                    "kaisai_nen": kaisai_nen,
                    "kaisai_tsukihi": kaisai_tsukihi,
                    "keibajo_code": keibajo_code,
                    "race_bango": race_bango,
                },
            )
            rows = cur.fetchall()
    except psycopg.Error:
        return {}
    owners: dict[str, str] = {}
    for row in rows:
        horse_id = "" if row[1] is None else str(row[1]).strip()
        if horse_id == "" or horse_id in owners:
            continue
        try:
            name = fetch_netkeiba_horse_owner(horse_id, cache_dir=_source_cache_dir())
        except URLError:
            continue
        if name != "":
            owners[horse_id] = name
    return owners


def _merge_keiba_archive_people(
    conn: psycopg.Connection,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
    trainers: dict[int, PersonSummary],
    owners: dict[int, PersonSummary],
    source_urls: OverseasSourceUrls,
) -> tuple[dict[int, PersonSummary], dict[int, PersonSummary]]:
    archive_root = Path(os.environ.get(KEIBA_DATA_SOURCES_ENV, str(DEFAULT_KEIBA_DATA_SOURCES_DIR)))
    jra_json_path = os.environ.get(JRA_CARD_JSON_ENV, "")
    published_trainers: list[str] = []
    published_owners: list[str] = []
    trainer_extra_stems: list[tuple[str, ...]] = []
    owner_extra_stems: list[tuple[str, ...]] = []
    published = None
    if jra_json_path != "":
        json_path = Path(jra_json_path)
        if json_path.is_file():
            published = parse_jra_card_json(json.loads(json_path.read_text(encoding="utf-8")))
    jra_html, netkeiba_html, jravan_html = _fetch_published_html_for_race(
        kaisai_nen=kaisai_nen,
        kaisai_tsukihi=kaisai_tsukihi,
        keibajo_code=keibajo_code,
        race_bango=race_bango,
        source_urls=source_urls,
    )
    if published is None and (jra_html != "" or netkeiba_html != ""):
        published = optimize_published_names(jra_html=jra_html, netkeiba_html=netkeiba_html)
    if published is not None and jravan_html != "":
        published = attach_trainer_stems_from_jravan(
            published,
            parse_jravan_racecard_trainers(jravan_html),
        )
    if published is not None:
        horse_dir_value = os.environ.get(NETKEIBA_HORSE_HTML_DIR_ENV, "")
        if horse_dir_value != "":
            owners_by_horse_id = load_netkeiba_horse_page_owners(Path(horse_dir_value))
        else:
            owners_by_horse_id = _fetch_owner_names_from_source_ids(
                conn,
                kaisai_nen=kaisai_nen,
                kaisai_tsukihi=kaisai_tsukihi,
                keibajo_code=keibajo_code,
                race_bango=race_bango,
            )
        if owners_by_horse_id:
            owner_by_umaban = _owner_names_by_umaban(
                conn,
                kaisai_nen=kaisai_nen,
                kaisai_tsukihi=kaisai_tsukihi,
                keibajo_code=keibajo_code,
                race_bango=race_bango,
                owners_by_horse_id=owners_by_horse_id,
            )
            published = attach_owner_stems_from_horse_pages(
                published,
                owner_by_umaban=owner_by_umaban,
            )
        published_trainers = list(published.trainers)
        published_owners = list(published.owners)
        trainer_extra_stems = list(published.trainer_extra_stems)
        owner_extra_stems = list(published.owner_extra_stems)
    if not published_trainers or not published_owners:
        identity_trainers, identity_owners = _published_names_from_identity(
            conn,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
        )
        if not published_trainers:
            published_trainers = identity_trainers
            trainer_extra_stems = []
        if not published_owners:
            published_owners = identity_owners
            owner_extra_stems = []
    before = date(int(kaisai_nen), int(kaisai_tsukihi[:2]), int(kaisai_tsukihi[2:]))
    archive_trainers = load_archive_people_by_umaban(
        kind="trainer",
        published_names=published_trainers,
        archive_root=archive_root,
        before=before,
        extra_stems=trainer_extra_stems,
    )
    archive_owners = load_archive_people_by_umaban(
        kind="owner",
        published_names=published_owners,
        archive_root=archive_root,
        before=before,
        extra_stems=owner_extra_stems,
    )
    merged_trainers = dict(trainers)
    for umaban, summary in archive_trainers.items():
        current = merged_trainers.get(umaban, PersonSummary(0, 0, 0, None, None, 0.0, None))
        merged_trainers[umaban] = prefer_richer_person(summary, current)
    merged_owners = dict(owners)
    for umaban, summary in archive_owners.items():
        current = merged_owners.get(umaban, PersonSummary(0, 0, 0, None, None, 0.0, None))
        merged_owners[umaban] = prefer_richer_person(summary, current)
    log(
        "  Keiba archive people "
        f"trainer={sum(1 for item in archive_trainers.values() if item.starts > 0)} "
        f"owner={sum(1 for item in archive_owners.values() if item.starts > 0)}"
    )
    return merged_trainers, merged_owners


def decode_jv_tansho_odds(frame: pd.DataFrame) -> pd.DataFrame:
    """Convert JV tenths (`0032`) into decimal odds (`3.2`)."""
    if "tansho_odds" not in frame.columns:
        return frame
    decoded = frame.copy()
    raw = pd.to_numeric(decoded["tansho_odds"], errors="coerce")
    decoded["tansho_odds"] = raw / JV_TANSHO_ODDS_SCALE
    return decoded


def fetch_live_tansho(race_key: str) -> tuple[dict[int, float], dict[int, int]]:
    url = f"{LIVE_ODDS_API_BASE}/{race_key}"
    request = Request(url, headers={"User-Agent": LIVE_ODDS_USER_AGENT})
    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, json.JSONDecodeError, TimeoutError) as error:
        log(f"  Live odds fetch failed: {error}")
        return {}, {}
    latest = payload.get("latest") if isinstance(payload, dict) else None
    tansho = latest.get("tansho") if isinstance(latest, dict) else None
    if not isinstance(tansho, list):
        return {}, {}
    odds_by_umaban: dict[int, float] = {}
    ninki_by_umaban: dict[int, int] = {}
    for row in tansho:
        if not isinstance(row, dict):
            continue
        try:
            umaban = int(str(row.get("combination")))
            odds = float(row.get("odds"))
            ninki = int(row.get("rank"))
        except (TypeError, ValueError):
            continue
        odds_by_umaban[umaban] = odds
        ninki_by_umaban[umaban] = ninki
    return odds_by_umaban, ninki_by_umaban


def step2_engineer_features(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, pd.Series, list[int]]:
    log("STEP 2: Engineering features ...")
    race_id = _race_id_series(df)
    ordered = df.assign(_race_id=race_id).sort_values(["_race_id", "umaban"], kind="mergesort")
    features = engineer_features(ordered)
    target = ordered["finish_position"].astype(int)
    groups = ordered.groupby("_race_id", sort=False).size().tolist()
    log(f"  Features shape: {features.shape}")
    log(f"  Target range: {target.min()} - {target.max()}, mean={target.mean():.2f}")
    log(f"  Races: {len(groups)}")
    return features, target, ordered["_race_id"], groups


def step3_train_models(
    features: pd.DataFrame,
    target: pd.Series,
    race_ids: pd.Series,
    groups: list[int],
) -> tuple[InvertedScoreModel, lgb.LGBMClassifier, lgb.LGBMClassifier]:
    log("STEP 3: Training LightGBM ranker + classifiers ...")
    unique_races = pd.Series(sorted(race_ids.unique()))
    cut = max(1, int(len(unique_races) * RACE_SPLIT_TRAIN_FRACTION))
    train_races = set(unique_races.iloc[:cut].tolist())
    train_mask = race_ids.isin(train_races)
    x_train = features.loc[train_mask]
    x_test = features.loc[~train_mask]
    y_train = target.loc[train_mask]
    y_test = target.loc[~train_mask]
    if int((~train_mask).sum()) == 0:
        x_train = features
        y_train = target
        x_test = features.iloc[0:0]
        y_test = target.iloc[0:0]
        train_groups = groups
    else:
        train_groups = (
            race_ids.loc[train_mask].groupby(race_ids.loc[train_mask], sort=False).size().tolist()
        )
    log(f"  Train: {len(x_train)} rows, Test: {len(x_test)} rows, train races: {len(train_groups)}")

    relevance_train = (y_train.max() - y_train + 1).astype(int)
    ranker = lgb.LGBMRanker(
        n_estimators=200,
        learning_rate=0.05,
        num_leaves=15,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
        objective="lambdarank",
    )
    ranker.fit(x_train, relevance_train, group=train_groups)
    finish_model = InvertedScoreModel(ranker)
    if len(x_test) > 0:
        pred_test = finish_model.predict(x_test)
        mae_test = mean_absolute_error(y_test, pred_test)
        log(f"  Ranker-as-finish MAE — test: {mae_test:.4f}")

    # --- Classifier: top-1 (win) ---
    y_top1_train = (y_train == 1).astype(int)
    y_top1_test = (y_test == 1).astype(int)
    clf_top1 = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
    )
    clf_top1.fit(x_train, y_top1_train)
    if len(x_test) > 0:
        top1_prob_test = clf_top1.predict_proba(x_test)
        if top1_prob_test.shape[1] == 2:
            auc_top1 = roc_auc_score(y_top1_test, top1_prob_test[:, 1])
        else:
            auc_top1 = float("nan")
        log(f"  Top-1 Classifier AUC — test: {auc_top1:.4f}")

    # --- Classifier: top-3 (place) ---
    y_top3_train = (y_train <= 3).astype(int)
    y_top3_test = (y_test <= 3).astype(int)
    clf_top3 = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
    )
    clf_top3.fit(x_train, y_top3_train)
    if len(x_test) > 0:
        top3_prob_test = clf_top3.predict_proba(x_test)
        if top3_prob_test.shape[1] == 2:
            auc_top3 = roc_auc_score(y_top3_test, top3_prob_test[:, 1])
        else:
            auc_top3 = float("nan")
        log(f"  Top-3 Classifier AUC — test: {auc_top3:.4f}")

    # Save models
    save_model(finish_model, "regressor")
    save_model(clf_top1, "classifier_top1")
    save_model(clf_top3, "classifier_top3")

    return finish_model, clf_top1, clf_top3


def step4_predict_race(
    pg_url: str,
    regressor: InvertedScoreModel | lgb.LGBMRegressor,
    clf_top1: lgb.LGBMClassifier,
    clf_top3: lgb.LGBMClassifier,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
    source_urls: OverseasSourceUrls = EMPTY_SOURCE_URLS,
) -> pd.DataFrame:
    log("STEP 4: Predicting tonight's race ...")
    with psycopg.connect(pg_url) as conn:
        df = decode_jv_tansho_odds(
            _fetch_predict_runners(
                conn,
                kaisai_nen=kaisai_nen,
                kaisai_tsukihi=kaisai_tsukihi,
                keibajo_code=keibajo_code,
                race_bango=race_bango,
            )
        )
        if df.empty:
            log("  ERROR: No runners found for the target race!")
            sys.exit(1)
        race_date = _kaisai_race_date(
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
        )
        form_by_umaban = _load_netkeiba_form_by_umaban(
            conn,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
            race_date=race_date,
            target_distance=_card_distance(df),
        )
        jockey_by_umaban, trainer_by_umaban, owner_by_umaban = _load_netkeiba_people_by_umaban(
            conn,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
            race_date=race_date,
        )
        trainer_by_umaban, owner_by_umaban = _merge_keiba_archive_people(
            conn,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
            trainers=trainer_by_umaban,
            owners=owner_by_umaban,
            source_urls=source_urls,
        )

    log(f"  Found {len(df)} runners")
    if form_by_umaban:
        with_starts = sum(1 for summary in form_by_umaban.values() if summary.starts > 0)
        log(f"  Attached netkeiba form for {with_starts} runners")
    else:
        log("  No netkeiba form rows; using model/market only")
    log(
        "  Attached people stats "
        f"jockey={sum(1 for item in jockey_by_umaban.values() if item.starts > 0)} "
        f"trainer={sum(1 for item in trainer_by_umaban.values() if item.starts > 0)} "
        f"owner={sum(1 for item in owner_by_umaban.values() if item.starts > 0)}"
    )
    race_key = f"jra:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango.zfill(2)}"
    odds_by_umaban, ninki_by_umaban = fetch_live_tansho(race_key)
    if odds_by_umaban:
        df = attach_tansho_odds(
            df,
            odds_by_umaban=odds_by_umaban,
            ninki_by_umaban=ninki_by_umaban,
        )
        log(f"  Attached live tansho for {len(odds_by_umaban)} runners")

    result = score_overseas_card(
        df,
        regressor=regressor,
        clf_top1=clf_top1,
        clf_top3=clf_top3,
        form_by_umaban=form_by_umaban,
        jockey_by_umaban=jockey_by_umaban,
        trainer_by_umaban=trainer_by_umaban,
        owner_by_umaban=owner_by_umaban,
    )

    log("  Predictions:")
    log(f"  {'umaban':>6} {'rank':>5} {'finish_pos':>10} {'top1_prob':>10} {'top3_prob':>10}")
    for _, row in result.iterrows():
        log(
            f"  {row['umaban']:>6} {row['predicted_rank']:>5} "
            f"{row['predicted_finish_position']:>10.2f} "
            f"{row['predicted_top1_prob']:>10.4f} "
            f"{row['predicted_top3_prob']:>10.4f}"
        )

    return result


def step5_write_local_pg(
    pg_url: str,
    predictions: pd.DataFrame,
    model_version: str,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> None:
    log("STEP 5: Writing predictions to local PG ...")
    now = datetime.now(timezone.utc)

    conn = psycopg.connect(pg_url)
    try:
        with conn.cursor() as cur:
            for _, row in predictions.iterrows():
                cur.execute(
                    UPSERT_PREDICTION_SQL,
                    {
                        "model_version": model_version,
                        "source": "overseas",
                        "kaisai_nen": kaisai_nen,
                        "kaisai_tsukihi": kaisai_tsukihi,
                        "keibajo_code": keibajo_code,
                        "race_bango": race_bango,
                        "ketto_toroku_bango": row["ketto_toroku_bango"],
                        "umaban": int(row["umaban"]),
                        "predicted_score": float(row["predicted_score"]),
                        "predicted_rank": int(row["predicted_rank"]),
                        "predicted_top1_prob": float(row["predicted_top1_prob"]),
                        "predicted_top3_prob": float(row["predicted_top3_prob"]),
                        "predicted_finish_position": float(row["predicted_finish_position"]),
                        "prediction_generated_at": now,
                        "distance_band": "2000-2400",
                        "field_size_band": "09",
                        "season_band": "summer",
                        "class_code": row["grade"],
                        "surface": row["track_code"],
                    },
                )

            cur.execute(
                UPSERT_ACTIVE_MODEL_SQL,
                {
                    "category": "overseas",
                    "model_version": model_version,
                    "activated_at": now,
                },
            )
        conn.commit()
        log(f"  Upserted {len(predictions)} prediction rows + 1 active model row")
    finally:
        conn.close()


def step6_sync_neon(
    neon_url: str | None,
    predictions: pd.DataFrame,
    model_version: str,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> None:
    log("STEP 6: Syncing to Neon ...")
    if not neon_url:
        log("  WARNING: NEON_PRIMARY_URL not found — skipping Neon sync")
        return

    try:
        conn = psycopg.connect(neon_url, connect_timeout=15, autocommit=True)
    except Exception as exc:
        log(f"  WARNING: Cannot connect to Neon ({type(exc).__name__}) — skipping sync")
        return

    try:
        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            # Ensure tables exist (autocommit needed for DDL on some Neon endpoints)
            cur.execute(CREATE_PREDICTIONS_TABLE_SQL)
            cur.execute(CREATE_ACTIVE_MODELS_TABLE_SQL)

            # Delete existing rows for this model+race, then insert fresh
            cur.execute(
                "DELETE FROM race_finish_position_model_predictions"
                " WHERE model_version = %s AND source = %s"
                " AND kaisai_nen = %s AND kaisai_tsukihi = %s"
                " AND keibajo_code = %s AND race_bango = %s",
                (model_version, "overseas", kaisai_nen, kaisai_tsukihi,
                 keibajo_code, race_bango),
            )

            insert_sql = """\
INSERT INTO race_finish_position_model_predictions (
    model_version, source, kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, ketto_toroku_bango, umaban,
    predicted_score, predicted_rank, predicted_top1_prob,
    predicted_top3_prob, predicted_finish_position,
    prediction_generated_at, distance_band, field_size_band,
    season_band, class_code, surface
) VALUES (
    %(model_version)s, %(source)s, %(kaisai_nen)s, %(kaisai_tsukihi)s,
    %(keibajo_code)s, %(race_bango)s, %(ketto_toroku_bango)s, %(umaban)s,
    %(predicted_score)s, %(predicted_rank)s, %(predicted_top1_prob)s,
    %(predicted_top3_prob)s, %(predicted_finish_position)s,
    %(prediction_generated_at)s, %(distance_band)s, %(field_size_band)s,
    %(season_band)s, %(class_code)s, %(surface)s
)
"""
            for _, row in predictions.iterrows():
                cur.execute(
                    insert_sql,
                    {
                        "model_version": model_version,
                        "source": "overseas",
                        "kaisai_nen": kaisai_nen,
                        "kaisai_tsukihi": kaisai_tsukihi,
                        "keibajo_code": keibajo_code,
                        "race_bango": race_bango,
                        "ketto_toroku_bango": row["ketto_toroku_bango"],
                        "umaban": int(row["umaban"]),
                        "predicted_score": float(row["predicted_score"]),
                        "predicted_rank": int(row["predicted_rank"]),
                        "predicted_top1_prob": float(row["predicted_top1_prob"]),
                        "predicted_top3_prob": float(row["predicted_top3_prob"]),
                        "predicted_finish_position": float(row["predicted_finish_position"]),
                        "prediction_generated_at": now,
                        "distance_band": "2000-2400",
                        "field_size_band": "09",
                        "season_band": "summer",
                        "class_code": row["grade"],
                        "surface": row["track_code"],
                    },
                )

            # Upsert active model (DELETE + INSERT to avoid constraint mismatch)
            cur.execute(
                "DELETE FROM finish_position_active_models WHERE category = %s",
                ("overseas",),
            )
            cur.execute(
                "INSERT INTO finish_position_active_models (category, model_version, activated_at)"
                " VALUES (%s, %s, %s)",
                ("overseas", model_version, now),
            )
        log(f"  Neon sync complete: {len(predictions)} predictions + 1 active model")
    except Exception as exc:
        log(f"  WARNING: Neon sync failed ({type(exc).__name__}: {exc}) — local data is safe")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def _discover_overseas_races(
    pg_url: str,
    *,
    kaisai_nen: str,
    kaisai_tsukihi: str,
) -> list[tuple[str, str, str, str]]:
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                DISCOVER_OVERSEAS_SQL,
                {"kaisai_nen": kaisai_nen, "kaisai_tsukihi": kaisai_tsukihi},
            )
            rows = cur.fetchall()
    return [(str(row[0]), str(row[1]), str(row[2]), str(row[3])) for row in rows]


def run_one_card(
    *,
    pg_url: str,
    neon_url: str | None,
    model_version: str,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
    regressor: InvertedScoreModel | lgb.LGBMRegressor,
    clf_top1: lgb.LGBMClassifier,
    clf_top3: lgb.LGBMClassifier,
    source_urls: OverseasSourceUrls = EMPTY_SOURCE_URLS,
) -> None:
    log(f"Target race: {kaisai_nen}/{kaisai_tsukihi}/{keibajo_code}/R{race_bango}")
    predictions = step4_predict_race(
        pg_url,
        regressor,
        clf_top1,
        clf_top3,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        source_urls=source_urls,
    )
    if "127.0.0.1" in pg_url or "localhost" in pg_url:
        step5_write_local_pg(
            pg_url,
            predictions,
            model_version,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
        )
    step6_sync_neon(
        neon_url,
        predictions,
        model_version,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
    )


def log_prepare_status(status: OverseasPrepareStatus) -> None:
    card = status.card
    log(
        f"Prepare {card.slug}: {card.kaisai_nen}/{card.kaisai_tsukihi}/"
        f"{card.keibajo_code}/R{card.race_bango} {card.venue_name} {card.race_name}"
    )
    log(f"  on official sale list: {status.on_sale_list}")
    log(f"  JRA card URL: {status.jra_card_url if status.jra_card_url else '(not published yet)'}")
    log(
        "  netkeiba card URL: "
        f"{status.netkeiba_card_url if status.netkeiba_card_url else '(not published yet)'}"
    )
    log(
        "  JRA-VAN card URL: "
        f"{status.jravan_card_url if status.jravan_card_url else '(not published yet)'}"
    )
    log(f"  next: {status.next_command}")


def run_prepare(slug: str, source_urls: OverseasSourceUrls = EMPTY_SOURCE_URLS) -> int:
    card = planned_overseas_card(slug)
    if card is None:
        log(f"Unknown overseas card: {slug}")
        return 1
    card = apply_source_urls(card, source_urls)
    cache_dir = _source_cache_dir()
    remember_source_url_combo(cache_dir, source_urls_from_card(card))
    list_url = JRA_OVERSEAS_SALE_LIST_URL_TEMPLATE.format(year=card.kaisai_nen)
    sale_html = fetch_url_text(list_url, cache_dir=cache_dir)
    race_html = ""
    race_page = card.jra_race_page_url
    if race_page == "":
        race_page = sale_list_match(card, parse_jra_overseas_sale_list(sale_html))
    if race_page != "":
        race_html = read_html_source(race_page, cache_dir=cache_dir)
    status = prepare_overseas_card(card, sale_list_html=sale_html, race_page_html=race_html)
    log_prepare_status(status)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train overseas finish-position model and predict target race"
    )
    parser.add_argument("--pg-url", default=None, help="Local PG connection URL")
    parser.add_argument("--neon-url", default=None, help="Neon PG connection URL")
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION)
    parser.add_argument("--kaisai-nen", default="2026")
    parser.add_argument("--kaisai-tsukihi", default="0816")
    parser.add_argument("--keibajo-code", default="A8")
    parser.add_argument("--race-bango", default="04")
    parser.add_argument(
        "--card",
        default=None,
        help="Planned overseas card slug (e.g. international-stakes, jacques-le-marois)",
    )
    parser.add_argument(
        "--prepare",
        action="store_true",
        help="Resolve JRA/netkeiba/JRA-VAN URLs for --card and print readiness",
    )
    parser.add_argument(
        "--jra-url",
        default="",
        help="JRA official card URL (accessSD.html?CNAME=...)",
    )
    parser.add_argument(
        "--netkeiba-url",
        default="",
        help="netkeiba overseas shutuba URL",
    )
    parser.add_argument(
        "--jravan-url",
        default="",
        help="JRA-VAN World racecard URL",
    )
    parser.add_argument(
        "--predict-only",
        action="store_true",
        help="Reuse saved overseas models instead of retraining",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Predict every letter-venue (A*) race on the given date",
    )
    args = parser.parse_args()
    source_urls = OverseasSourceUrls(
        jra_url=args.jra_url,
        netkeiba_url=args.netkeiba_url,
        jravan_url=args.jravan_url,
    )
    if args.card is not None:
        selected = planned_overseas_card(args.card)
        if selected is None:
            log(f"Unknown overseas card: {args.card}")
            return
        selected = apply_source_urls(selected, source_urls)
        args.kaisai_nen = selected.kaisai_nen
        args.kaisai_tsukihi = selected.kaisai_tsukihi
        args.keibajo_code = selected.keibajo_code
        args.race_bango = selected.race_bango
    if args.prepare:
        slug = args.card if args.card is not None else "international-stakes-2026"
        raise SystemExit(run_prepare(slug, source_urls))

    pg_url = args.pg_url or default_pg_url()
    neon_url = args.neon_url or default_neon_url()
    log(f"Model version: {args.model_version}")
    saved = load_saved_models() if args.predict_only else None
    if saved is None:
        raw_df = step1_extract_training_data(pg_url)
        features, target, race_ids, groups = step2_engineer_features(raw_df)
        regressor, clf_top1, clf_top3 = step3_train_models(features, target, race_ids, groups)
    else:
        regressor, clf_top1, clf_top3 = saved
    cards = [
        (args.kaisai_nen, args.kaisai_tsukihi, args.keibajo_code, args.race_bango),
    ]
    if args.discover:
        cards = _discover_overseas_races(
            pg_url,
            kaisai_nen=args.kaisai_nen,
            kaisai_tsukihi=args.kaisai_tsukihi,
        )
        if not cards:
            log("  No overseas letter-venue races found for that date")
            return
    for kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango in cards:
        run_one_card(
            pg_url=pg_url,
            neon_url=neon_url,
            model_version=args.model_version,
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
            regressor=regressor,
            clf_top1=clf_top1,
            clf_top3=clf_top3,
            source_urls=source_urls,
        )
    log("Done.")


if __name__ == "__main__":
    main()
