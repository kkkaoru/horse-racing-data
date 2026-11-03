"""Pooled JBC ranker trained on every past edition of the four JBC titles at any venue.

JBC moves between NAR venues (and JRA Kyoto in 2018), so the cohort is every past
race whose normalized title is one of the exact JBC titles, wherever it ran. The
domestic JV/NV rows are mapped onto the overseas event model's source shape so its
features, walk-forward folds, candidates and KV payload are reused unchanged.
History and people rates use only days strictly before each race. No odds or
popularity are read.

Inputs in DATA_DIR (dumped from local PG, columns as read below):
  races.parquet    jvd_ra/nvd_ra rows whose normalized kyosomei_hondai is in TITLES
  runners.parquet  their jvd_se/nvd_se rows; jockey/trainer/owner are normalized names
  history.parquet  every jvd_se/nvd_se start of those horses joined to its *_ra row
  people.parquet   per name and day starts/wins/shows over jvd_se+nvd_se since 2000
  generic_preds.parquet  stored race_finish_position_model_predictions for those races
  today.json       the day's runners from the production finish-prediction section

  uv run python src/scripts/jbc_event_model.py DATA_DIR 20261103 OUTPUT_DIR
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import overseas_event_model as event
from arc_finish_model import unordered_topk
from overseas_finish_features import NETKEIBA_SOURCE

TITLES: tuple[str, ...] = (
    "ＪＢＣレディスクラシック",
    "ＪＢＣスプリント",
    "ＪＢＣクラシック",
    "ＪＢＣ２歳優駿",
)
# JV/NV grade_code A/B/C are (J)G1-3; the suffix lets the shared grade patterns match.
GRADE_SUFFIX: Mapping[str, str] = {"A": " (G1)", "B": " (G2)", "C": " (G3)"}
DIRT_SURFACE: str = "ダ"
FIRST_EVALUATED_YEAR: str = "2011"
MODEL_VERSION_PREFIX: str = "jbc-event-v1"
PERSON_COLUMNS: tuple[str, ...] = ("jockey", "trainer", "owner")


@dataclass(frozen=True)
class Dumps:
    races: pd.DataFrame
    runners: pd.DataFrame
    history: pd.DataFrame
    people: pd.DataFrame


def race_id(venue: object, race: object) -> str:
    """Venue joins the race number so same-day JBC races at two venues stay distinct."""
    return f"{venue}{race}"


def to_sources(dumps: Dumps) -> event.Sources:
    races = dumps.races.loc[dumps.races["title"].isin(TITLES)]
    runners = dumps.runners
    history = dumps.history
    finish = pd.to_numeric(history["kakutei_chakujun"], errors="coerce")
    started = ~history["ijo_kubun_code"].astype(str).isin(event.NON_STARTER_CODES) & (finish > 0)
    history = history.loc[started]
    return event.Sources(
        races=pd.DataFrame(
            {
                "race_day": races["race_day"],
                "race_bango": [race_id(v, r) for v, r in zip(races["venue"], races["race_bango"], strict=True)],
                "title": races["title"],
                "kyori": races["kyori"],
                "track_code": races["track_code"],
            }
        ),
        runners=pd.DataFrame(
            {
                "race_day": runners["race_day"],
                "race_bango": [race_id(v, r) for v, r in zip(runners["venue"], runners["race_bango"], strict=True)],
                "umaban": runners["umaban"],
                "kakutei_chakujun": runners["kakutei_chakujun"],
                "ijo_kubun_code": runners["ijo_kubun_code"],
                "source_horse_id": runners["ketto_toroku_bango"],
                **{f"source_{kind}_id": runners[kind] for kind in PERSON_COLUMNS},
            }
        ),
        history=pd.DataFrame(
            {
                "source": NETKEIBA_SOURCE,
                "source_horse_id": history["ketto_toroku_bango"],
                "race_date": history["race_day"].map(event.iso_day),
                "race_day_sequence": pd.to_numeric(history["race_bango"]),
                "finish_position": finish.loc[started].astype(int),
                "finish_position_text": finish.loc[started].astype(int).astype(str),
                "distance_metres": pd.to_numeric(history["kyori"]),
                "race_name": history["race_name"] + history["grade_code"].map(GRADE_SUFFIX).fillna(""),
                "surface": [
                    event.TURF_SURFACE if str(code).startswith(event.TURF_TRACK_PREFIX) else DIRT_SURFACE
                    for code in history["track_code"]
                ],
            }
        ),
        people=dumps.people.assign(race_date=dumps.people["race_day"].map(event.iso_day)).loc[
            :, ["person_kind", "source_person_id", "race_date", "starts", "wins", "shows"]
        ],
    )


def evaluate(data: pd.DataFrame, day: str) -> dict[str, object]:
    """Every year from FIRST_EVALUATED_YEAR is one fold trained only on earlier days."""
    past = data.loc[data["race_day"] < day]
    years = sorted({d[:4] for d in past["race_day"] if d[:4] >= FIRST_EVALUATED_YEAR})
    folds = [fold for year in years if (fold := event.evaluate_year(past, year)) is not None]
    candidates = {
        name: event.summarize([race for fold in folds for race in fold.results[name]])
        for name in event.CANDIDATES
    }
    return {
        "folds": [fold.summary for fold in folds],
        "candidates": candidates,
        "selected": max(event.CANDIDATES, key=lambda name: event.selection_key(candidates[name])),
    }


def generic_summary(data: pd.DataFrame, generic: pd.DataFrame, races: Sequence[str]) -> dict[str, object]:
    """Stored generic-model ranks on the same evaluated races (missing races are skipped)."""
    results: list[list[dict[str, int | float | bool]]] = []
    for key in races:
        card = data.loc[data["race_key"] == key]
        rows = generic.loc[generic["race_key"] == key]
        if rows.empty or not event.evaluable(card):
            continue
        by_umaban = card.set_index(card["umaban"].astype(int))["horse_id"]
        ranked = rows.assign(umaban=rows["umaban"].astype(int)).sort_values(["predicted_rank", "umaban"])
        predicted = [by_umaban[u] for u in ranked["umaban"] if u in by_umaban.index]
        actual = [horse for horse in event.actual_order(card) if horse in set(predicted)]
        if actual[: event.TOP_K] == event.actual_order(card)[: event.TOP_K]:
            results.append(unordered_topk(predicted, actual))
    return {"races": len(results), "summary": event.summarize(results) if results else []}


def predict_day(
    data: pd.DataFrame, day: str, *, candidate: str, generated_at: str
) -> dict[str, list[dict[str, object]]]:
    scorer = event.CANDIDATES[candidate](data.loc[data["race_day"] < day])
    version = f"{MODEL_VERSION_PREFIX}-{candidate}-{day}"
    return {
        f"pred:fp:v1:{day}:{str(key)[:2]}:{str(key)[2:]}": event.predictions_payload(
            card, scorer(card), model_version=version, generated_at=generated_at
        )
        for key, card in data.loc[data["race_day"] == day].groupby("race_bango")
    }


def load_dumps(directory: Path, day: str) -> tuple[Dumps, pd.DataFrame]:
    today = pd.DataFrame(json.loads((directory / "today.json").read_text(encoding="utf-8")))
    races = pd.concat(
        [
            pd.read_parquet(directory / "races.parquet"),
            today.loc[:, ["race_day", "venue", "race_bango", "title", "kyori", "track_code"]].drop_duplicates(),
        ]
    )
    runners = pd.concat([pd.read_parquet(directory / "runners.parquet"), today.drop(columns=["title", "kyori", "track_code"])])
    generic = pd.read_parquet(directory / "generic_preds.parquet")
    dumps = Dumps(
        races=races.loc[races["race_day"] <= day],
        runners=runners.loc[runners["race_day"] <= day],
        history=pd.read_parquet(directory / "history.parquet"),
        people=pd.read_parquet(directory / "people.parquet"),
    )
    return dumps, generic


def run(dumps: Dumps, generic: pd.DataFrame, day: str, output: Path, now: datetime) -> dict[str, object]:
    data = event.runner_rows(to_sources(dumps))
    evaluation = evaluate(data, day)
    past = data.loc[(data["race_day"] < day) & (data["race_day"] >= FIRST_EVALUATED_YEAR)]
    generic_by_model = {
        str(version): generic_summary(
            data,
            rows.assign(race_key=rows["race_day"] + rows["venue"] + rows["race_bango"]),
            sorted(set(past["race_key"])),
        )
        for version, rows in generic.groupby("model_version")
    }
    payloads = predict_day(
        data,
        day,
        candidate=str(evaluation["selected"]),
        generated_at=now.isoformat().replace("+00:00", "Z"),
    )
    report: dict[str, object] = {
        "day": day,
        "titles": TITLES,
        "editions": sorted(
            f"{race.race_day}:{race.venue}:{race.race_bango}:{race.title}"
            for race in dumps.races.loc[dumps.races["title"].isin(TITLES)].itertuples(index=False)
        ),
        "features": event.FEATURES,
        "evaluation": evaluation,
        "genericStoredPredictions": generic_by_model,
        "selectionWasRetrospective": True,
        "predictedRaces": sorted(payloads),
        "limitations": "Generic stored predictions were generated in 2026 and may be in-sample.",
    }
    event.write_outputs(output, report, payloads)
    return report


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("day")
    parser.add_argument("output", type=Path)
    args = parser.parse_args(argv)
    if not event.MEETING_PATTERN.match(args.day):
        raise ValueError("day must be YYYYMMDD")
    dumps, generic = load_dumps(args.data, args.day)
    report = run(dumps, generic, args.day, args.output, datetime.now(timezone.utc))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
