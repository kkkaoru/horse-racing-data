"""Pooled ranker for any JRA-sold overseas meeting, trained on its own past editions.

The cohort is every race held at the same venue code on a past day that ran one of
the meeting's (normalized) titles, so editions of all races of the meeting train one
model. Features use only history strictly before each race day. No odds, no
popularity and no gate (past gates are mostly unknown). Partial fields are kept:
they train, but only races with a known top five are evaluated.

Usage (local PG settings in the environment):
  uv run python src/scripts/overseas_event_model.py 20261101 A4 OUTPUT_DIR
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg
from xgboost import XGBRanker

from arc_finish_model import FEATURES as FORM_FEATURES
from arc_finish_model import PERSON_KINDS, entry_features, unordered_topk

CLASS_FEATURES: tuple[str, ...] = (
    "horse_g1_starts",
    "horse_g1_best",
    "horse_graded_wins",
    "horse_surface_starts",
    "horse_surface_win_rate",
    "horse_distance_win_rate",
)
FEATURES: tuple[str, ...] = (
    *FORM_FEATURES,
    *CLASS_FEATURES,
    "race_distance",
    "race_turf",
    "field_size",
)
G1_PATTERN: str = r"\((?:G1|GI|Jpn1|JpnI)\)"
GRADED_PATTERN: str = r"\((?:G[123]|GI{1,3}|Jpn[123]|JpnI{1,3})\)"
TURF_SURFACE: str = "芝"
DISTANCE_WINDOW_METRES: int = 200
# JV ijo_kubun_code: 1 取消, 2 除外 (before the start), 3 競走除外. 4/5 started and stay runners.
NON_STARTER_CODES: frozenset[str] = frozenset({"1", "2", "3"})
TURF_TRACK_PREFIX: str = "1"
EVENT_HISTORY_YEARS: int = 30
EVALUATION_FOLDS: int = 4
TOP_K: int = 5
MIN_TRAINING_RACES: int = 2
MODEL_VERSION_PREFIX: str = "overseas-event-v1"
KV_TTL_SECONDS: int = 72 * 60 * 60
BASE_PARAMETERS: Mapping[str, object] = {
    "learning_rate": 0.05,
    "min_child_weight": 5,
    "reg_lambda": 1,
    "random_state": 42,
    "n_jobs": 2,
    "tree_method": "hist",
}
OBJECTIVES: tuple[str, ...] = ("rank:pairwise", "rank:ndcg")
DEPTHS: tuple[int, ...] = (1, 2, 3)
TREE_COUNTS: tuple[int, ...] = (60, 240)
# Plain pre-race orderings; a negative weight means a lower value is better.
BASELINES: Mapping[str, Mapping[str, float]] = {
    "horseWinRate": {"horse_win_rate": 1.0},
    "jockeyWinRate": {"jockey_win_rate": 1.0},
    "horseG1Best": {"horse_g1_best": -1.0},
    "formBlend": {
        "horse_win_rate": 1.0,
        "jockey_win_rate": 1.0,
        "horse_avg_finish": -1.0,
        "horse_g1_best": -1.0,
    },
}
TITLE_SQL: str = "btrim(replace(kyosomei_hondai, chr(12288), ''))"
RACES_SQL: str = f"""
WITH titles AS (
  SELECT DISTINCT {TITLE_SQL} AS title FROM jvd_ra
  WHERE kaisai_nen = %(year)s AND kaisai_tsukihi = %(mmdd)s AND keibajo_code = %(venue)s
), days AS (
  SELECT DISTINCT kaisai_nen, kaisai_tsukihi FROM jvd_ra
  WHERE keibajo_code = %(venue)s AND {TITLE_SQL} IN (SELECT title FROM titles)
    AND kaisai_nen || kaisai_tsukihi <= %(date)s AND kaisai_nen >= %(min_year)s
)
SELECT r.kaisai_nen || r.kaisai_tsukihi AS race_day, r.race_bango, {TITLE_SQL} AS title,
       r.kyori, r.track_code
FROM jvd_ra r JOIN days USING (kaisai_nen, kaisai_tsukihi)
WHERE r.keibajo_code = %(venue)s
ORDER BY 1, 2
"""
RUNNERS_SQL: str = """
SELECT s.kaisai_nen || s.kaisai_tsukihi AS race_day, s.race_bango, s.umaban,
       s.kakutei_chakujun, s.ijo_kubun_code,
       o.source_horse_id, o.source_jockey_id, o.source_trainer_id, o.source_owner_id
FROM jvd_se s
LEFT JOIN oversea_runner_source_id o
  ON o.kaisai_nen = s.kaisai_nen AND o.kaisai_tsukihi = s.kaisai_tsukihi
 AND o.keibajo_code = s.keibajo_code AND o.race_bango = s.race_bango AND o.umaban = s.umaban
WHERE s.keibajo_code = %(venue)s AND s.kaisai_nen || s.kaisai_tsukihi = ANY(%(days)s)
ORDER BY 1, 2, 3
"""
HISTORY_SQL: str = """
SELECT source, source_horse_id, race_date::text AS race_date, race_day_sequence,
       finish_position, finish_position_text, distance_metres, race_name, surface
FROM oversea_horse_race_history
WHERE source = 'netkeiba' AND source_horse_id = ANY(%(horses)s)
"""
PEOPLE_SQL: str = """
SELECT person_kind, source_person_id, race_date::text AS race_date, count(*) AS starts,
       count(*) FILTER (WHERE finish_position = 1) AS wins,
       count(*) FILTER (WHERE finish_position BETWEEN 1 AND 3) AS shows
FROM oversea_person_race_history
WHERE source = 'netkeiba' AND source_person_id = ANY(%(people)s)
GROUP BY 1, 2, 3
"""
DSN_KEYS: tuple[str, ...] = ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
DSN_NAMES: tuple[str, ...] = ("host", "port", "user", "password", "dbname")
MEETING_PATTERN: re.Pattern[str] = re.compile(r"^\d{8}$")
VENUE_PATTERN: re.Pattern[str] = re.compile(r"^[A-Z][0-9A-Z]$")


Scorer = Callable[[pd.DataFrame], list[float]]
Preparer = Callable[[pd.DataFrame], Scorer]


Fetch = Callable[[str, Mapping[str, object]], pd.DataFrame]


@dataclass(frozen=True)
class Meeting:
    date: str
    venue: str

    def __post_init__(self) -> None:
        if not MEETING_PATTERN.match(self.date) or not VENUE_PATTERN.match(self.venue):
            raise ValueError("Meeting needs YYYYMMDD and an overseas venue code")


@dataclass(frozen=True)
class Fold:
    summary: dict[str, object]
    results: dict[str, list[list[dict[str, int | float | bool]]]]


@dataclass(frozen=True)
class Sources:
    races: pd.DataFrame
    runners: pd.DataFrame
    history: pd.DataFrame
    people: pd.DataFrame


def iso_day(day: str) -> str:
    return f"{day[:4]}-{day[4:6]}-{day[6:]}"


def cursor_fetch(cursor: psycopg.Cursor[tuple[object, ...]]) -> Fetch:
    def fetch(query: str, params: Mapping[str, object]) -> pd.DataFrame:
        _ = cursor.execute(query.encode("utf-8"), params)
        return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description or []])

    return fetch


def load_sources(fetch: Fetch, meeting: Meeting) -> Sources:
    races = fetch(
        RACES_SQL,
        {
            "year": meeting.date[:4],
            "mmdd": meeting.date[4:],
            "venue": meeting.venue,
            "date": meeting.date,
            "min_year": str(int(meeting.date[:4]) - EVENT_HISTORY_YEARS),
        },
    )
    if races.empty or not races["race_day"].eq(meeting.date).any():
        raise ValueError("No races are registered for this meeting")
    runners = fetch(
        RUNNERS_SQL,
        {"venue": meeting.venue, "days": sorted(set(races["race_day"]))},
    )
    horses = sorted({text_value(h) for h in runners["source_horse_id"]} - {""})
    people = sorted(
        {text_value(value) for kind in PERSON_KINDS for value in runners[f"source_{kind}_id"]} - {""}
    )
    return Sources(
        races=races,
        runners=runners,
        history=fetch(HISTORY_SQL, {"horses": horses}),
        people=fetch(PEOPLE_SQL, {"people": people}),
    )


def text_value(value: object) -> str:
    """SQL NULL arrives as None or NaN; both mean the partial source left the id empty."""
    return "" if value is None or (isinstance(value, float) and np.isnan(value)) else str(value).strip()


def people_daily(people: pd.DataFrame) -> pd.DataFrame:
    return people.astype({"starts": float, "wins": float, "shows": float})


def finish_value(text: object, field: int) -> float:
    """Unplaced (0 / blank) runners that started rank after every placed runner."""
    value = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    return float(value) if pd.notna(value) and value >= 1 else float(field + 1)


def rate(wins: float, starts: float) -> float:
    return wins / starts if starts else float("nan")


def class_features(history: pd.DataFrame, *, race_date: str, turf: bool, distance: int) -> list[float]:
    """Grade, surface and distance record from starts strictly before the race day."""
    prior = history.loc[history["race_date"] < race_date]
    names = prior["race_name"].fillna("").astype(str)
    finish = pd.to_numeric(prior["finish_position"], errors="coerce")
    wins = finish == 1
    g1 = names.str.contains(G1_PATTERN)
    surface = (prior["surface"].fillna("").astype(str) == TURF_SURFACE) == turf
    near = (pd.to_numeric(prior["distance_metres"], errors="coerce") - distance).abs() <= DISTANCE_WINDOW_METRES
    best = finish[g1].min()
    return [
        float(g1.sum()),
        float(best) if pd.notna(best) else float("nan"),
        float((wins & names.str.contains(GRADED_PATTERN)).sum()),
        float(surface.sum()),
        rate(float((wins & surface).sum()), float(surface.sum())),
        rate(float((wins & near).sum()), float(near.sum())),
    ]


def race_rows(card: pd.DataFrame, sources: Sources, daily: pd.DataFrame) -> list[dict[str, object]]:
    """card carries its race columns (kyori, track_code, title) on every runner row."""
    info = card.iloc[0]
    day, race = str(info["race_day"]), str(info["race_bango"])
    distance = int(info["kyori"])
    turf = str(info["track_code"]).startswith(TURF_TRACK_PREFIX)
    rows: list[dict[str, object]] = []
    for entry in card.itertuples(index=False):
        horse = text_value(entry.source_horse_id)
        history = sources.history.loc[sources.history["source_horse_id"] == horse]
        features = entry_features(
            history,
            horse_id=horse,
            race_date=iso_day(day),
            people_ids={
                kind: text_value(getattr(entry, f"source_{kind}_id")) for kind in PERSON_KINDS
            },
            people_daily=daily,
            target_distance=distance,
        )
        rows.append(
            {
                **dict(zip(FORM_FEATURES, features, strict=True)),
                **dict(
                    zip(
                        CLASS_FEATURES,
                        class_features(history, race_date=iso_day(day), turf=turf, distance=distance),
                        strict=True,
                    )
                ),
                "race_distance": float(distance),
                "race_turf": float(turf),
                "field_size": float(len(card)),
                "race_day": day,
                "race_bango": race,
                "race_key": f"{day}{race}",
                "title": str(info["title"]),
                "umaban": str(entry.umaban),
                # Unidentified partial-source runners stay distinct within their race.
                "horse_id": horse or f"{day}{race}:{entry.umaban}",
                "finish": finish_value(entry.kakutei_chakujun, len(card)),
            }
        )
    return rows


def runner_rows(sources: Sources) -> pd.DataFrame:
    """One row per starter with features from history strictly before its race day."""
    starters = sources.runners.loc[
        ~sources.runners["ijo_kubun_code"].astype(str).isin(NON_STARTER_CODES)
    ]
    cards = starters.merge(sources.races, on=["race_day", "race_bango"], how="inner")
    daily = people_daily(sources.people)
    return pd.DataFrame(
        [
            row
            for _, card in cards.groupby(["race_day", "race_bango"], sort=True)
            for row in race_rows(card, sources, daily)
        ]
    )


def fit(train: pd.DataFrame, parameters: Mapping[str, object]) -> XGBRanker:
    train = train.sort_values(["race_key", "horse_id"])
    if train["race_key"].nunique() < MIN_TRAINING_RACES:
        raise ValueError("At least two earlier races are required")
    groups = train.groupby("race_key", sort=False).size().to_numpy()
    relevance = (train["field_size"] + 1 - train["finish"]).clip(lower=0)
    model = XGBRanker(**BASE_PARAMETERS, **parameters)
    model.fit(train.loc[:, list(FEATURES)], relevance, group=groups)
    return model


def model_preparer(parameters: Mapping[str, object]) -> Preparer:
    def prepare(train: pd.DataFrame) -> Scorer:
        model = fit(train, parameters)
        return lambda card: model.predict(card.loc[:, list(FEATURES)]).tolist()

    return prepare


def percentile_score(card: pd.DataFrame, weights: Mapping[str, float]) -> list[float]:
    """Sum of within-race percentile ranks; a missing value ranks below every known one."""
    total = pd.Series(0.0, index=card.index)
    for column, sign in weights.items():
        total += (card[column] * sign).rank(pct=True).fillna(0.0)
    return total.tolist()


def baseline_preparer(weights: Mapping[str, float]) -> Preparer:
    return lambda _train: lambda card: percentile_score(card, weights)


CANDIDATES: Mapping[str, Preparer] = {
    **{name: baseline_preparer(weights) for name, weights in BASELINES.items()},
    **{
        f"xgb-{objective.split(':')[1]}-d{depth}-n{trees}": model_preparer(
            {"objective": objective, "max_depth": depth, "n_estimators": trees}
        )
        for objective in OBJECTIVES
        for depth in DEPTHS
        for trees in TREE_COUNTS
    },
}


def rank_order(card: pd.DataFrame, scores: Sequence[float]) -> list[str]:
    values = np.asarray(scores, dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("Scores must be finite")
    ranked = card.assign(score=values).sort_values(["score", "horse_id"], ascending=[False, True])
    return ranked["horse_id"].tolist()


def evaluable(card: pd.DataFrame) -> bool:
    placed = sorted(int(x) for x in card["finish"] if x <= TOP_K)
    return len(card) >= TOP_K and placed == list(range(1, TOP_K + 1))


def actual_order(card: pd.DataFrame) -> list[str]:
    return card.sort_values(["finish", "horse_id"])["horse_id"].tolist()


def summarize(results: Sequence[Sequence[Mapping[str, int | float | bool]]]) -> list[dict[str, object]]:
    return [
        {
            "k": k,
            "races": len(results),
            "exactSetHits": sum(bool(race[k - 1]["exact_set_hit"]) for race in results),
            "meanPrecision": float(np.mean([float(race[k - 1]["precision"]) for race in results])),
        }
        for k in range(1, TOP_K + 1)
    ]


def selection_key(summary: Sequence[Mapping[str, object]]) -> tuple[float, int]:
    """Set overlap first (less noisy on few races), then exact unordered set hits."""
    return (
        sum(float(str(row["meanPrecision"])) for row in summary),
        sum(int(str(row["exactSetHits"])) for row in summary),
    )


def evaluate_year(past: pd.DataFrame, year: str) -> Fold | None:
    cutoff = min(day for day in past["race_day"] if day.startswith(year))
    train = past.loc[past["race_day"] < cutoff]
    cards = [card for _, card in past.loc[past["race_day"].str.startswith(year)].groupby("race_key")]
    scored = [card for card in cards if evaluable(card)]
    if train["race_key"].nunique() < MIN_TRAINING_RACES or not scored:
        return None
    scorers = {name: prepare(train) for name, prepare in CANDIDATES.items()}
    return Fold(
        summary={
            "year": year,
            "trainingRaces": int(train["race_key"].nunique()),
            "races": len(cards),
            "evaluatedRaces": [str(card["title"].iloc[0]) for card in scored],
        },
        results={
            name: [unordered_topk(rank_order(card, scorer(card)), actual_order(card)) for card in scored]
            for name, scorer in scorers.items()
        },
    )


def evaluate(data: pd.DataFrame, meeting: Meeting) -> dict[str, object]:
    """Walk-forward by year: each fold trains only on races before that year's first day."""
    past = data.loc[data["race_day"] < meeting.date]
    years = sorted({day[:4] for day in past["race_day"]})[-EVALUATION_FOLDS:]
    folds = [fold for year in years if (fold := evaluate_year(past, year)) is not None]
    if not folds:
        raise ValueError("No earlier edition has a known top five to evaluate")
    candidates = {
        name: summarize([race for fold in folds for race in fold.results[name]]) for name in CANDIDATES
    }
    return {
        "folds": [fold.summary for fold in folds],
        "candidates": candidates,
        "selected": max(CANDIDATES, key=lambda name: selection_key(candidates[name])),
    }


def predictions_payload(
    card: pd.DataFrame, scores: Sequence[float], *, model_version: str, generated_at: str
) -> list[dict[str, object]]:
    """The viewer's pred:fp:v1 body: lower predictedFinishNorm is better; no probabilities."""
    order = rank_order(card, scores)
    last = max(len(order) - 1, 1)
    by_horse = card.set_index("horse_id")["umaban"]
    return [
        {
            "confidenceTier": None,
            "horseNumber": str(int(str(by_horse[horse]))),
            "modelVersion": model_version,
            "predictedFinishNorm": rank / last,
            "predictedScoreStddev": None,
            "predictionGeneratedAt": generated_at,
            "showProbability": None,
            "winProbability": None,
        }
        for rank, horse in enumerate(order)
    ]


def predict_meeting(
    data: pd.DataFrame, meeting: Meeting, *, candidate: str, generated_at: str
) -> dict[str, list[dict[str, object]]]:
    scorer = CANDIDATES[candidate](data.loc[data["race_day"] < meeting.date])
    version = f"{MODEL_VERSION_PREFIX}-{candidate}-{meeting.date}-{meeting.venue}"
    return {
        f"pred:fp:v1:{meeting.date}:{meeting.venue}:{race}": predictions_payload(
            card, scorer(card), model_version=version, generated_at=generated_at
        )
        for race, card in data.loc[data["race_day"] == meeting.date].groupby("race_bango")
    }


def write_outputs(output: Path, report: Mapping[str, object], payloads: Mapping[str, object]) -> None:
    # Exclusive creation keeps earlier evidence; every KV body is a reviewed local file first.
    output.mkdir(parents=True, exist_ok=False)
    (output / "evaluation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "kv-bulk.json").write_text(
        json.dumps(
            [
                {"key": key, "value": json.dumps(body), "expiration_ttl": KV_TTL_SECONDS}
                for key, body in payloads.items()
            ]
        ),
        encoding="utf-8",
    )


def run(fetch: Fetch, meeting: Meeting, output: Path, now: datetime) -> dict[str, object]:
    data = runner_rows(load_sources(fetch, meeting))
    evaluation = evaluate(data, meeting)
    payloads = predict_meeting(
        data,
        meeting,
        candidate=str(evaluation["selected"]),
        generated_at=now.isoformat().replace("+00:00", "Z"),
    )
    report: dict[str, object] = {
        "meeting": f"{meeting.date}:{meeting.venue}",
        "features": FEATURES,
        "trainingRaces": int(data.loc[data["race_day"] < meeting.date, "race_key"].nunique()),
        "evaluation": evaluation,
        "selectionWasRetrospective": True,
        "predictedRaces": sorted(payloads),
        "metric": "exactSetHits: predicted top-k set equals actual top-k set, order ignored",
        "limitations": "Few past editions; partial source fields; no odds, popularity or gate features.",
    }
    write_outputs(output, report, payloads)
    return report


def connection_string(env: Mapping[str, str]) -> str:
    missing = [key for key in DSN_KEYS if not env.get(key)]
    if missing:
        raise ValueError(f"Missing local PostgreSQL settings: {', '.join(missing)}")
    return " ".join(f"{name}={env[key]}" for name, key in zip(DSN_NAMES, DSN_KEYS, strict=True))


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("date")
    parser.add_argument("venue")
    parser.add_argument("output", type=Path)
    args = parser.parse_args(argv)
    meeting = Meeting(args.date, args.venue)
    with psycopg.connect(connection_string(os.environ)) as connection, connection.cursor() as cursor:
        report = run(cursor_fetch(cursor), meeting, args.output, datetime.now(timezone.utc))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
