"""Unit tests for the anonymized clef-flash request builders (run: uv run --with pytest pytest)."""

import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from clef_state import (  # noqa: E402
    build_request,
    build_request_v3,
    build_request_v4,
    build_request_v5,
    parse_answers,
    parse_answers_v3,
    parse_answers_v4,
    parse_answers_v5,
    runner_row,
)

RACE = {"surface": "T", "dist": 2000, "going": "1", "field": 2, "grade_c": 5, "joken": "999"}
TODAY = datetime.date(2026, 10, 18)
RUN = {"d": datetime.date(2026, 9, 18), "s": "T", "m": 1800, "g": "2", "n": 16, "f": 1, "t": -3, "c": 4, "l": 1, "k": "B"}


def runner(no, **extra):
    row = {"umaban": no, "brank": None, "frank": 2, "last1": 0.0, "last2": float("nan"), "last3": None,
           "days_since": 30.0, "career_raw": 5.0, "horse_top3": 0.31, "age": 3, "sex_c": 2.0, "futan": 55.0}
    row.update(extra)
    return row


def test_runner_row_rounds_rates_and_drops_missing_values():
    row = runner_row(runner(7))
    assert row["no"] == 7
    assert row["ht"] == 31
    assert row["l3"] == [0, None, None]
    assert row["br"] is None
    assert row["wt"] == 55


def test_build_request_sorts_runners_and_asks_winner_and_top3():
    req = build_request(RACE, [runner(2), runner(1)])
    assert [r["no"] for r in req["state"]["runners"]] == [1, 2]
    assert sorted(req["questions"]) == ["top3_1", "top3_2", "winner"]
    assert req["state"]["race"]["surface"] == "turf"
    assert req["state"]["race"]["going"] == "firm"


def test_parse_answers_reads_choice_and_noul():
    answers = {"winner": {"probabilities": {"1": 0.7, "2": 0.3}}, "top3_1": {"noul": 0.9}, "top3_2": {"noul": 0.4}}
    assert parse_answers(answers, [1, 2]) == {1: {"clef_win": 0.7, "clef_top3": 0.9}, 2: {"clef_win": 0.3, "clef_top3": 0.4}}


def test_v3_adds_pairwise_questions_and_sums_them():
    req = build_request_v3(RACE, [runner(1, last5=[RUN]), runner(2, last5=None)], TODAY)
    assert sorted(req["questions"]) == ["ahead_1_2", "top3_1", "top3_2", "winner"]
    assert req["state"]["candidates"][0]["runs"] == ["30|T1800|good|16|1|-3|4|1|B"]
    answers = {"winner": {"probabilities": {"1": 0.6, "2": 0.4}}, "top3_1": {"noul": 0.8}, "top3_2": {"noul": 0.5}, "ahead_1_2": {"noul": 0.75}}
    parsed = parse_answers_v3(answers, ["1", "2"])
    assert parsed[1]["clef_pair"] == 0.75
    assert parsed[2]["clef_pair"] == 0.25


def test_v4_asks_three_evidence_questions_per_runner():
    req = build_request_v4(RACE, [runner(1, last5=[RUN])], TODAY)
    assert sorted(req["questions"]) == ["cls_1", "fit_1", "form_1"]
    answers = {"cls_1": {"noul": 0.1}, "fit_1": {"noul": 0.2}, "form_1": {"noul": 0.3}}
    assert parse_answers_v4(answers, ["1"]) == {1: {"clef_form": 0.3, "clef_fit": 0.2, "clef_cls": 0.1}}


def test_v5_asks_winner_second_top3_and_top5():
    req = build_request_v5(RACE, [runner(1), runner(2)])
    assert sorted(req["questions"]) == ["second", "top3_1", "top3_2", "top5_1", "top5_2", "winner"]
    answers = {"winner": {"probabilities": {"1": 0.7, "2": 0.3}}, "second": {"probabilities": {"1": 0.2, "2": 0.8}},
               "top3_1": {"noul": 0.9}, "top3_2": {"noul": 0.6}, "top5_1": {"noul": 0.95}, "top5_2": {"noul": 0.8}}
    assert parse_answers_v5(answers, ["1", "2"]) == {
        1: {"clef_win": 0.7, "clef_second": 0.2, "clef_top3": 0.9, "clef_top5": 0.95},
        2: {"clef_win": 0.3, "clef_second": 0.8, "clef_top3": 0.6, "clef_top5": 0.8},
    }
