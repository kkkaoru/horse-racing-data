"""Pooled overseas meeting ranker: sources, features, walk-forward selection and KV output."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg
import pytest

import overseas_event_model as model

DAYS: tuple[str, ...] = ("20201107", "20211106", "20221105", "20231104", "20261101")
HORSES: tuple[str, ...] = ("hA", "hB", "hC", "hD", "hE")


def races_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "race_day": list(DAYS),
            "race_bango": ["11", "11", "11", "11", "11"],
            "title": ["ターフ", "ターフ", "ターフ", "ターフ", "ターフ"],
            "kyori": ["2400", "2400", "2400", "2400", "2400"],
            "track_code": ["11", "11", "11", "11", "11"],
        }
    )


def starter(day: str, index: int, horse: str) -> dict[str, object]:
    return {
        "race_day": day,
        "race_bango": "11",
        "umaban": f"0{index + 1}",
        "kakutei_chakujun": "00" if day == "20261101" else f"0{index + 1}",
        "ijo_kubun_code": "0",
        "source_horse_id": horse,
        "source_jockey_id": f"j{index}",
        "source_trainer_id": None,
        "source_owner_id": float("nan"),
    }


def unidentified(day: str, umaban: str, code: str, horse: str | None) -> dict[str, object]:
    return {
        "race_day": day,
        "race_bango": "11",
        "umaban": umaban,
        "kakutei_chakujun": "00",
        "ijo_kubun_code": code,
        "source_horse_id": horse,
        "source_jockey_id": None,
        "source_trainer_id": None,
        "source_owner_id": None,
    }


def runners_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [starter(day, index, horse) for day in DAYS for index, horse in enumerate(HORSES)]
        + [unidentified(day, "06", "4", None) for day in DAYS]
        + [unidentified(day, "07", "1", "scratched") for day in DAYS]
    ).sort_values(["race_day", "umaban"], ignore_index=True)


def history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": ["netkeiba", "netkeiba", "netkeiba", "netkeiba"],
            "source_horse_id": ["hA", "hA", "hB", "hC"],
            "race_date": ["2019-09-01", "2021-09-01", "2019-09-01", "2019-09-01"],
            "race_day_sequence": [1, 1, 1, 1],
            "finish_position": [1, 1, 2, None],
            "finish_position_text": ["1", "1", "2", "中"],
            "distance_metres": [2400, 2000, 2400, 1200],
            "race_name": ["キングS(GI)", "トライアル(GII)", "キングS(GI)", None],
            "surface": ["芝", "芝", "芝", "ダ"],
        }
    )


def people_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "person_kind": ["jockey", "jockey"],
            "source_person_id": ["j0", "j1"],
            "race_date": ["2019-01-01", "2019-01-01"],
            "starts": [10, 10],
            "wins": [5, 1],
            "shows": [8, 3],
        }
    )


ANSWERS: Mapping[str, pd.DataFrame] = {
    model.RACES_SQL: races_frame(),
    model.RUNNERS_SQL: runners_frame(),
    model.HISTORY_SQL: history_frame(),
    model.PEOPLE_SQL: people_frame(),
}


class Recorder:
    def __init__(self, answers: Mapping[str, pd.DataFrame]) -> None:
        self.answers: Mapping[str, pd.DataFrame] = answers
        self.calls: list[Mapping[str, object]] = []

    def __call__(self, query: str, params: Mapping[str, object]) -> pd.DataFrame:
        self.calls.append(params)
        return self.answers[query]


def rows() -> pd.DataFrame:
    return model.runner_rows(model.load_sources(Recorder(ANSWERS), model.Meeting("20261101", "A4")))


def test_meeting_requires_a_date_and_an_overseas_venue_code() -> None:
    with pytest.raises(ValueError, match="overseas venue code"):
        model.Meeting("20261101", "05")
    with pytest.raises(ValueError, match="overseas venue code"):
        model.Meeting("2026-11-01", "A4")


def test_text_value_treats_null_and_nan_as_an_empty_id() -> None:
    assert model.text_value(None) == ""
    assert model.text_value(float("nan")) == ""
    assert model.text_value(" a1 ") == "a1"


def test_finish_value_ranks_unplaced_starters_last() -> None:
    assert model.finish_value("03", 8) == 3.0
    assert model.finish_value("00", 8) == 9.0
    assert model.finish_value(None, 8) == 9.0


def test_rate_is_missing_without_starts() -> None:
    assert np.isnan(model.rate(0.0, 0.0))
    assert model.rate(1.0, 4.0) == 0.25


def test_class_features_use_only_earlier_grade_surface_and_distance_records() -> None:
    history = history_frame().iloc[:2]
    assert model.class_features(history, race_date="2022-11-05", turf=True, distance=2400) == [
        1.0,
        1.0,
        2.0,
        2.0,
        1.0,
        1.0,
    ]
    before = model.class_features(history, race_date="2020-01-01", turf=False, distance=1200)
    assert before[:4] == [1.0, 1.0, 1.0, 0.0]
    assert np.isnan(before[4:]).all()


def test_load_sources_queries_the_meeting_and_its_past_edition_days() -> None:
    recorder = Recorder(ANSWERS)
    sources = model.load_sources(recorder, model.Meeting("20261101", "A4"))
    assert recorder.calls[0] == {
        "year": "2026",
        "mmdd": "1101",
        "venue": "A4",
        "date": "20261101",
        "min_year": "1996",
    }
    assert recorder.calls[1] == {"venue": "A4", "days": list(DAYS)}
    assert recorder.calls[2] == {"horses": ["hA", "hB", "hC", "hD", "hE", "scratched"]}
    assert recorder.calls[3] == {"people": ["j0", "j1", "j2", "j3", "j4"]}
    assert len(sources.runners) == 35


def test_load_sources_fails_without_the_meeting_itself() -> None:
    with pytest.raises(ValueError, match="No races are registered"):
        model.load_sources(Recorder({model.RACES_SQL: races_frame().iloc[:4]}), model.Meeting("20261101", "A4"))


def test_runner_rows_drop_non_starters_and_keep_unidentified_starters() -> None:
    data = rows()
    assert len(data) == 30
    assert data.loc[data["race_day"] == "20221105", "horse_id"].tolist() == [
        "hA",
        "hB",
        "hC",
        "hD",
        "hE",
        "2022110511:06",
    ]
    first = data.loc[(data["race_day"] == "20221105") & (data["horse_id"] == "hA")].iloc[0]
    assert first["horse_g1_starts"] == 1.0
    assert first["horse_starts"] == 2.0
    assert first["jockey_win_rate"] == 0.5
    assert first["race_turf"] == 1.0
    assert first["field_size"] == 6.0
    assert data.loc[data["horse_id"] == "2022110511:06", "finish"].tolist() == [7.0]


def test_percentile_score_ranks_missing_values_below_known_ones() -> None:
    card = pd.DataFrame({"a": [1.0, 3.0, float("nan")], "b": [2.0, 1.0, 3.0]})
    assert model.percentile_score(card, {"a": 1.0}) == [0.5, 1.0, 0.0]
    assert model.percentile_score(card, {"a": 1.0, "b": -1.0}) == [
        0.5 + 2 / 3,
        2.0,
        1 / 3,
    ]


def test_rank_order_breaks_ties_by_horse_and_rejects_nonfinite_scores() -> None:
    card = pd.DataFrame({"horse_id": ["b", "a", "c"]})
    assert model.rank_order(card, [1.0, 1.0, 2.0]) == ["c", "a", "b"]
    with pytest.raises(ValueError, match="finite"):
        model.rank_order(card, [1.0, float("inf"), 2.0])


def test_evaluable_requires_five_runners_with_known_top_five() -> None:
    assert model.evaluable(pd.DataFrame({"finish": [1.0, 2.0, 3.0, 4.0, 5.0, 7.0]}))
    assert not model.evaluable(pd.DataFrame({"finish": [1.0, 2.0, 3.0, 4.0]}))
    assert not model.evaluable(pd.DataFrame({"finish": [1.0, 2.0, 3.0, 4.0, 7.0, 7.0]}))


def test_fit_needs_two_earlier_races() -> None:
    data = rows()
    with pytest.raises(ValueError, match="two earlier races"):
        model.fit(data.loc[data["race_day"] == "20201107"], {"objective": "rank:pairwise"})


def test_evaluate_walks_forward_and_skips_years_without_enough_training() -> None:
    report = model.evaluate(rows(), model.Meeting("20261101", "A4"))
    # Round-trip through JSON: the report is the serialized evidence written to disk.
    candidates = json.loads(json.dumps(report))["candidates"]
    assert report["folds"] == [
        {"year": "2022", "trainingRaces": 2, "races": 1, "evaluatedRaces": ["ターフ"]},
        {"year": "2023", "trainingRaces": 3, "races": 1, "evaluatedRaces": ["ターフ"]},
    ]
    assert candidates["horseWinRate"] == [
        {"k": 1, "races": 2, "exactSetHits": 2, "meanPrecision": 1.0},
        {"k": 2, "races": 2, "exactSetHits": 2, "meanPrecision": 1.0},
        {"k": 3, "races": 2, "exactSetHits": 2, "meanPrecision": 1.0},
        {"k": 4, "races": 2, "exactSetHits": 0, "meanPrecision": 0.75},
        {"k": 5, "races": 2, "exactSetHits": 0, "meanPrecision": 0.8},
    ]
    assert len(candidates) == 16
    assert report["selected"] == "horseWinRate"


def test_evaluate_fails_without_an_evaluable_earlier_edition() -> None:
    data = rows()
    with pytest.raises(ValueError, match="known top five"):
        model.evaluate(data.loc[data["race_day"] >= "20231104"], model.Meeting("20261101", "A4"))


def test_selection_prefers_overlap_then_exact_hits() -> None:
    assert model.selection_key(
        [{"meanPrecision": 0.5, "exactSetHits": 1}, {"meanPrecision": 0.25, "exactSetHits": 2}]
    ) == (0.75, 3)


def test_predictions_payload_matches_the_viewer_contract() -> None:
    card = pd.DataFrame({"horse_id": ["x", "y", "z"], "umaban": ["01", "02", "10"]})
    assert model.predictions_payload(
        card, [0.1, 0.9, 0.5], model_version="v", generated_at="2026-10-31T00:00:00Z"
    ) == [
        {
            "confidenceTier": None,
            "horseNumber": "2",
            "modelVersion": "v",
            "predictedFinishNorm": 0.0,
            "predictedScoreStddev": None,
            "predictionGeneratedAt": "2026-10-31T00:00:00Z",
            "showProbability": None,
            "winProbability": None,
        },
        {
            "confidenceTier": None,
            "horseNumber": "10",
            "modelVersion": "v",
            "predictedFinishNorm": 0.5,
            "predictedScoreStddev": None,
            "predictionGeneratedAt": "2026-10-31T00:00:00Z",
            "showProbability": None,
            "winProbability": None,
        },
        {
            "confidenceTier": None,
            "horseNumber": "1",
            "modelVersion": "v",
            "predictedFinishNorm": 1.0,
            "predictedScoreStddev": None,
            "predictionGeneratedAt": "2026-10-31T00:00:00Z",
            "showProbability": None,
            "winProbability": None,
        },
    ]


def test_run_writes_evaluation_and_kv_bodies_for_the_meeting_only(tmp_path: Path) -> None:
    output = tmp_path / "out"
    report = model.run(
        Recorder(ANSWERS),
        model.Meeting("20261101", "A4"),
        output,
        datetime(2026, 10, 31, tzinfo=timezone.utc),
    )
    assert report["predictedRaces"] == ["pred:fp:v1:20261101:A4:11"]
    assert report["trainingRaces"] == 4
    bulk = json.loads((output / "kv-bulk.json").read_text(encoding="utf-8"))
    assert [entry["key"] for entry in bulk] == ["pred:fp:v1:20261101:A4:11"]
    assert bulk[0]["expiration_ttl"] == 259200
    body = json.loads(bulk[0]["value"])
    assert [row["horseNumber"] for row in body] == ["1", "2", "3", "6", "4", "5"]
    assert body[0]["modelVersion"] == "overseas-event-v1-horseWinRate-20261101-A4"
    assert json.loads((output / "evaluation.json").read_text(encoding="utf-8"))["meeting"] == "20261101:A4"
    with pytest.raises(FileExistsError):
        model.write_outputs(output, {}, {})


def test_connection_string_requires_every_local_setting() -> None:
    with pytest.raises(ValueError, match="POSTGRES_HOST, POSTGRES_DB"):
        model.connection_string(
            {"POSTGRES_PORT": "1", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"}
        )
    assert (
        model.connection_string(
            {
                "POSTGRES_HOST": "127.0.0.1",
                "POSTGRES_PORT": "15432",
                "POSTGRES_USER": "u",
                "POSTGRES_PASSWORD": "p",
                "POSTGRES_DB": "d",
            }
        )
        == "host=127.0.0.1 port=15432 user=u password=p dbname=d"
    )


class FakeDescription:
    def __init__(self, name: str) -> None:
        self.name: str = name


class FakePsycopgCursor:
    def __init__(self) -> None:
        self.current: pd.DataFrame = pd.DataFrame()

    def __enter__(self) -> FakePsycopgCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, query: bytes, _params: Mapping[str, object]) -> FakePsycopgCursor:
        self.current = ANSWERS[query.decode("utf-8")]
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self.current.itertuples(index=False, name=None))

    @property
    def description(self) -> list[FakeDescription]:
        return [FakeDescription(str(column)) for column in self.current.columns]


class FakeConnection:
    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self) -> FakePsycopgCursor:
        return FakePsycopgCursor()


def test_main_reads_local_postgresql_and_prints_the_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    connections: list[str] = []
    monkeypatch.setattr(psycopg, "connect", lambda dsn: connections.append(dsn) or FakeConnection())
    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", "15432")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_DB", "d")
    model.main(["20261101", "A4", str(tmp_path / "out")])
    assert connections == ["host=127.0.0.1 port=15432 user=u password=p dbname=d"]
    assert json.loads(capsys.readouterr().out)["predictedRaces"] == ["pred:fp:v1:20261101:A4:11"]
