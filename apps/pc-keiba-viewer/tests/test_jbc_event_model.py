"""Pooled JBC ranker: domestic source mapping, generic comparison and KV output."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

import jbc_event_model as model
import overseas_event_model as event

HORSES: tuple[str, ...] = ("h0", "h1", "h2", "h3", "h4")


def race(day: str, venue: str, bango: str, title: str) -> dict[str, str]:
    return {"race_day": day, "venue": venue, "race_bango": bango, "title": title, "kyori": "2000", "track_code": "24"}


def runner(day: str, venue: str, bango: str, index: int, *, finish: str, code: str = "0") -> dict[str, str]:
    return {
        "race_day": day,
        "venue": venue,
        "race_bango": bango,
        "umaban": f"0{index + 1}",
        "ketto_toroku_bango": HORSES[index % len(HORSES)],
        "kakutei_chakujun": finish,
        "ijo_kubun_code": code,
        "jockey": f"j{index}",
        "trainer": "t",
        "owner": "",
        "barei": "05",
        "seibetsu_code": "1",
        "futan_juryo": "570",
    }


def start(horse: str, day: str, finish: str, *, code: str = "0", track: str = "24", grade: str = "A") -> dict[str, str]:
    return {
        "src": "nvd",
        "ketto_toroku_bango": horse,
        "race_day": day,
        "venue": "44",
        "race_bango": "10",
        "kakutei_chakujun": finish,
        "ijo_kubun_code": code,
        "kyori": "2000",
        "track_code": track,
        "grade_code": grade,
        "race_name": "ＪＢＣクラシック",
        "kakutoku_honshokin": "0",
    }


def dumps() -> model.Dumps:
    past = ("20091103", "20101103", "20111103", "20121103")
    races = pd.DataFrame(
        [
            *[race(day, "44", "10", "ＪＢＣクラシック") for day in past],
            race("20111103", "44", "09", "ＪＢＣスプリント"),
            race("20121103", "30", "09", "ＪＢＣ２歳優駿"),
            race("20121003", "44", "10", "ＪＢＣカウントダウン"),
            race("20131103", "46", "09", "ＪＢＣクラシック"),
        ]
    )
    runners = pd.DataFrame(
        [
            *[runner(day, "44", "10", i, finish=f"0{i + 1}") for day in past for i in range(5)],
            *[runner("20111103", "44", "09", i, finish=f"0{5 - i}") for i in range(5)],
            *[runner("20121103", "30", "09", i, finish=f"0{i + 1}") for i in range(4)],
            runner("20121103", "44", "10", 5, finish="00", code="1"),
            *[runner("20131103", "46", "09", i, finish="00") for i in range(5)],
        ]
    )
    history = pd.DataFrame(
        [
            *[start(HORSES[i], day, f"0{i + 1}") for day in past for i in range(5)],
            start("h0", "20081103", "00"),
            start("h1", "20081103", "05", code="1"),
            start("h2", "20081003", "01", track="11", grade="D"),
        ]
    )
    people = pd.DataFrame(
        [
            {"person_kind": "jockey", "source_person_id": f"j{i}", "race_day": day, "starts": 2, "wins": 1 if i == 0 else 0, "shows": 1}
            for day in past
            for i in range(5)
        ]
    )
    return model.Dumps(races=races, runners=runners, history=history, people=people)


def test_to_sources_maps_domestic_rows_onto_the_event_source_shape() -> None:
    sources = model.to_sources(dumps())

    assert sources.races["race_bango"].tolist() == ["4410", "4410", "4410", "4410", "4409", "3009", "4609"]
    assert sources.runners.iloc[0].to_dict() == {
        "race_day": "20091103",
        "race_bango": "4410",
        "umaban": "01",
        "kakutei_chakujun": "01",
        "ijo_kubun_code": "0",
        "source_horse_id": "h0",
        "source_jockey_id": "j0",
        "source_trainer_id": "t",
        "source_owner_id": "",
    }
    assert len(sources.history) == 21
    assert sources.history.iloc[-1].to_dict() == {
        "source": "netkeiba",
        "source_horse_id": "h2",
        "race_date": "2008-10-03",
        "race_day_sequence": 10,
        "finish_position": 1,
        "finish_position_text": "1",
        "distance_metres": 2000,
        "race_name": "ＪＢＣクラシック",
        "surface": "芝",
    }
    assert sources.history.iloc[0]["race_name"] == "ＪＢＣクラシック (G1)"
    assert sources.history.iloc[0]["surface"] == "ダ"
    assert sources.people.columns.tolist() == ["person_kind", "source_person_id", "race_date", "starts", "wins", "shows"]
    assert sources.people.iloc[0]["race_date"] == "2009-11-03"


def test_run_evaluates_walk_forward_compares_generic_and_writes_kv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        event,
        "CANDIDATES",
        {
            "horseWinRate": event.baseline_preparer({"horse_win_rate": 1.0}),
            "xgb": event.model_preparer({"objective": "rank:pairwise", "max_depth": 1, "n_estimators": 5}),
        },
    )
    generic = pd.DataFrame(
        {
            "model_version": ["g"] * 9 + ["g"] * 4,
            "race_day": ["20111103"] * 5 + ["20121103"] * 4 + ["20121103"] * 4,
            "venue": ["44"] * 9 + ["30"] * 4,
            "race_bango": ["10"] * 9 + ["09"] * 4,
            "umaban": ["01", "02", "03", "04", "05", "01", "02", "03", "04", "01", "02", "03", "04"],
            "predicted_rank": [2, 1, 3, 4, 5, 1, 2, 3, 4, 1, 2, 3, 4],
        }
    )

    report = json.loads(
        json.dumps(model.run(dumps(), generic, "20131103", tmp_path / "out", datetime(2013, 11, 3, tzinfo=timezone.utc)))
    )

    evaluation = report["evaluation"]
    assert [fold["year"] for fold in evaluation["folds"]] == ["2011", "2012"]
    assert evaluation["candidates"]["horseWinRate"][0] == {"k": 1, "races": 3, "exactSetHits": 2, "meanPrecision": 2 / 3}
    assert report["genericStoredPredictions"] == {
        "g": {
            "races": 1,
            "summary": [
                {"k": 1, "races": 1, "exactSetHits": 0, "meanPrecision": 0.0},
                {"k": 2, "races": 1, "exactSetHits": 1, "meanPrecision": 1.0},
                {"k": 3, "races": 1, "exactSetHits": 1, "meanPrecision": 1.0},
                {"k": 4, "races": 1, "exactSetHits": 1, "meanPrecision": 1.0},
                {"k": 5, "races": 1, "exactSetHits": 1, "meanPrecision": 1.0},
            ],
        }
    }
    assert report["predictedRaces"] == ["pred:fp:v1:20131103:46:09"]
    bulk = json.loads((tmp_path / "out" / "kv-bulk.json").read_text(encoding="utf-8"))
    body = json.loads(bulk[0]["value"])
    assert bulk[0]["key"] == "pred:fp:v1:20131103:46:09"
    assert sorted(row["horseNumber"] for row in body) == ["1", "2", "3", "4", "5"]
    assert evaluation["selected"] == "xgb"
    assert body[0]["modelVersion"] == "jbc-event-v1-xgb-20131103"


def test_generic_summary_without_rows_reports_no_races() -> None:
    assert model.generic_summary(pd.DataFrame({"race_key": ["k"]}), pd.DataFrame({"race_key": ["x"]}), ["k"]) == {
        "races": 0,
        "summary": [],
    }


def test_main_reads_dumps_filters_later_days_and_prints_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(event, "CANDIDATES", {"horseWinRate": event.baseline_preparer({"horse_win_rate": 1.0})})
    source = dumps()
    today = source.runners.loc[source.runners["race_day"] == "20131103"].assign(
        title="ＪＢＣクラシック", kyori="2000", track_code="24"
    )
    later = runner("20141103", "46", "09", 0, finish="00") | {"title": "ＪＢＣクラシック", "kyori": "2000", "track_code": "24"}
    (tmp_path / "today.json").write_text(json.dumps([*today.to_dict("records"), later]), encoding="utf-8")
    source.races.loc[source.races["race_day"] < "20131103"].to_parquet(tmp_path / "races.parquet")
    source.runners.loc[source.runners["race_day"] < "20131103"].to_parquet(tmp_path / "runners.parquet")
    source.history.to_parquet(tmp_path / "history.parquet")
    source.people.to_parquet(tmp_path / "people.parquet")
    pd.DataFrame(
        {"model_version": ["g"], "race_day": ["20111103"], "venue": ["44"], "race_bango": ["10"], "umaban": ["01"], "predicted_rank": [1]}
    ).to_parquet(tmp_path / "generic_preds.parquet")

    model.main([str(tmp_path), "20131103", str(tmp_path / "out")])

    printed = json.loads(capsys.readouterr().out)
    assert printed["predictedRaces"] == ["pred:fp:v1:20131103:46:09"]
    assert printed["editions"][-1] == "20131103:46:09:ＪＢＣクラシック"
    assert len(printed["editions"]) == 7


def test_main_rejects_a_malformed_day(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="day must be YYYYMMDD"):
        model.main([str(tmp_path), "2013-11-03", str(tmp_path / "out")])
