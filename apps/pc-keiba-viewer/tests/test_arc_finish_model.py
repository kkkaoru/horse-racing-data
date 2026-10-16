"""Arc metrics, event-time isolation, source validation and native model checks."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from arc_finish_model import (
    FEATURES,
    TrainingConfig,
    build_dataset,
    card_frame,
    entry_features,
    entry_people,
    person_features,
    predict_card,
    rerank_top_two,
    train_before,
    unordered_topk,
)


def test_unordered_topk_reports_overlap_and_exact_sets() -> None:
    assert unordered_topk(["b", "a", "c", "e", "d"], ["a", "b", "c", "d", "e"]) == [
        {"k": 1, "hits": 0, "precision": 0.0, "exact_set_hit": False},
        {"k": 2, "hits": 2, "precision": 1.0, "exact_set_hit": True},
        {"k": 3, "hits": 3, "precision": 1.0, "exact_set_hit": True},
        {"k": 4, "hits": 3, "precision": 0.75, "exact_set_hit": False},
        {"k": 5, "hits": 5, "precision": 1.0, "exact_set_hit": True},
    ]


@pytest.mark.parametrize(
    "predicted,actual",
    [
        (["a"], ["a"]),
        (["a", "a", "b", "c", "d"], ["a", "b", "c", "d", "e"]),
        (["a", "b", "c", "d", "e"], ["a", "b", "c", "d", "f"]),
    ],
)
def test_invalid_metric_fields(predicted: list[str], actual: list[str]) -> None:
    with pytest.raises(ValueError):
        unordered_topk(predicted, actual)


@pytest.fixture
def daily() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "person_kind": ["jockey", "jockey", "jockey"],
            "source_person_id": ["j", "j", "j"],
            "race_date": ["2022-01-01", "2023-10-01", "2024-01-01"],
            "starts": [10, 100, 100],
            "wins": [2, 100, 100],
            "shows": [4, 100, 100],
        }
    )


@pytest.fixture
def history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": ["netkeiba", "netkeiba", "netkeiba", "netkeiba"],
            "source_horse_id": ["h", "h", "h", "h"],
            "race_date": ["2023-09-01", "2023-09-20", "2023-10-01", "2024-01-01"],
            "finish_position": [2, None, 1, 1],
            "finish_position_text": ["2", "取", "1", "1"],
            "race_name": ["Trial(GII)", "Trial(GI)", "凱旋門賞(GI)", "Other(GI)"],
            "distance_metres": [2400, 2400, 2400, 2400],
        }
    )


def test_person_features_exclude_same_day_and_future(daily: pd.DataFrame) -> None:
    assert person_features(
        daily, person_id="j", kind="jockey", before="2023-10-01"
    ) == [10.0, 0.2, 0.4]


@pytest.mark.parametrize("person_id", ["", "missing"])
def test_missing_person_is_not_fabricated(daily: pd.DataFrame, person_id: str) -> None:
    result = person_features(
        daily, person_id=person_id, kind="jockey", before="2023-10-01"
    )
    assert result[0] == 0
    assert np.isnan(result[1:]).all()


def test_horse_features_exclude_target_future_and_withdrawals(
    history: pd.DataFrame, daily: pd.DataFrame
) -> None:
    result = entry_features(
        history,
        horse_id="h",
        race_date="2023-10-01",
        people_ids={"jockey": "j"},
        people_daily=daily,
    )
    assert result[:7] == [1.0, 0.0, 2.0, 2.0, 2.0, 30.0, 0.0]
    assert result[9:12] == [10.0, 0.2, 0.4]
    assert len(result) == 18


def test_missing_horse_history_remains_missing(
    history: pd.DataFrame, daily: pd.DataFrame
) -> None:
    result = entry_features(
        history,
        horse_id="unknown",
        race_date="2023-10-01",
        people_ids={},
        people_daily=daily,
    )
    assert result[:2] == [0.0, 0.0]
    assert np.isnan(result[2:9]).all()


def test_identity_conflict_fails() -> None:
    identities = pd.DataFrame(
        {
            "source_horse_id": ["h", "h"],
            "race_date": ["2023-10-01", "2023-10-01"],
            "person_kind": ["owner", "owner"],
            "source_person_id": ["a", "b"],
        }
    )
    with pytest.raises(ValueError, match="Ambiguous"):
        entry_people(identities, horse_id="h", race_date="2023-10-01")


def test_identity_is_scoped_to_race() -> None:
    identities = pd.DataFrame(
        {
            "source_horse_id": ["h", "h"],
            "race_date": ["2023-10-01", "2024-10-01"],
            "person_kind": ["owner", "owner"],
            "source_person_id": ["a", "b"],
        }
    )
    assert entry_people(identities, horse_id="h", race_date="2023-10-01") == {
        "owner": "a"
    }


def test_card_retains_dnf_runner(history: pd.DataFrame, daily: pd.DataFrame) -> None:
    card = pd.DataFrame(
        {
            "source_horse_id": ["h", "other"],
            "race_date": ["2023-10-01", "2023-10-01"],
            "finish_position": [1, None],
        }
    )
    identities = pd.DataFrame(
        columns=["source_horse_id", "race_date", "person_kind", "source_person_id"]
    )
    result = card_frame(
        card, history=history, identities=identities, people_daily=daily
    )
    assert result["finish"].tolist() == [1.0, 3.0]
    assert result["horse_id"].tolist() == ["h", "other"]


def test_dataset_rejects_duplicate_sources(
    history: pd.DataFrame, daily: pd.DataFrame
) -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        build_dataset(
            pd.concat([history, history]),
            identities=pd.DataFrame(),
            people_daily=daily,
            expected_fields={},
        )


def test_dataset_rejects_incomplete_fields(
    history: pd.DataFrame, daily: pd.DataFrame
) -> None:
    with pytest.raises(ValueError, match="No complete"):
        build_dataset(
            history, identities=pd.DataFrame(), people_daily=daily, expected_fields={}
        )


def test_win_focused_training_uses_only_prior_races() -> None:
    data = pd.DataFrame(np.zeros((6, len(FEATURES))), columns=FEATURES)
    data["horse_id"] = ["a", "b", "c", "a", "b", "c"]
    data["race_date"] = [
        "2021-10-01",
        "2021-10-01",
        "2021-10-01",
        "2022-10-01",
        "2022-10-01",
        "2022-10-01",
    ]
    data["finish"] = [1, 2, 3, 2, 1, 3]
    model = train_before(
        data, cutoff="2023-01-01", config=TrainingConfig(objective="reg:squarederror")
    )
    result = predict_card(model, data.iloc[:3])
    assert result["horse_id"].tolist() == ["a", "b", "c"]
    assert result["score"].round(6).tolist() == [0.333333, 0.333333, 0.333333]


def test_training_and_inference_are_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = pd.DataFrame(np.zeros((6, len(FEATURES))), columns=FEATURES)
    data["horse_id"] = ["a", "b", "c", "a", "b", "c"]
    data["race_date"] = [
        "2021-10-01",
        "2021-10-01",
        "2021-10-01",
        "2022-10-01",
        "2022-10-01",
        "2022-10-01",
    ]
    data["finish"] = [1, 2, 3, 2, 1, 3]
    model = train_before(data, cutoff="2023-01-01")
    card = data.iloc[:3]
    assert predict_card(model, card)["horse_id"].tolist() == ["a", "b", "c"]
    with pytest.raises(ValueError, match="two prior"):
        train_before(data, cutoff="2022-01-01")
    with pytest.raises(ValueError, match="one nonempty"):
        predict_card(model, data)
    monkeypatch.setattr(model, "predict", lambda frame: np.full(len(frame), np.nan))
    with pytest.raises(ValueError, match="nonfinite"):
        predict_card(model, card)


@pytest.fixture
def complete_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": "netkeiba",
            "source_horse_id": np.tile(["a", "b", "c", "d", "e"], 3),
            "race_date": np.repeat(["2023-10-01", "2024-10-06", "2025-10-05"], 5),
            "finish_position": np.tile([1, 2, 3, 4, 5], 3),
            "finish_position_text": np.tile(["1", "2", "3", "4", "5"], 3),
            "race_name": "凱旋門賞(GI)",
            "distance_metres": 2400,
        }
    )


def test_complete_dataset(complete_history: pd.DataFrame, daily: pd.DataFrame) -> None:
    identities = pd.DataFrame(
        columns=["source_horse_id", "race_date", "person_kind", "source_person_id"]
    )
    data, excluded = build_dataset(
        complete_history,
        identities=identities,
        people_daily=daily,
        expected_fields={"2023-10-01": 5, "2024-10-06": 5, "2025-10-05": 5},
    )
    assert len(data) == 15
    assert excluded == {}
    assert data["race_date"].unique().tolist() == [
        "2023-10-01",
        "2024-10-06",
        "2025-10-05",
    ]


def test_all_verified_years_are_used_without_a_date_floor(
    complete_history: pd.DataFrame,
    daily: pd.DataFrame,
) -> None:
    older = complete_history.iloc[:5].assign(race_date="2004-10-03")
    winner_only = complete_history.iloc[:1].assign(race_date="1920-10-03")
    identities = pd.DataFrame(
        columns=["source_horse_id", "race_date", "person_kind", "source_person_id"]
    )
    data, excluded = build_dataset(
        pd.concat([winner_only, older, complete_history]),
        identities=identities,
        people_daily=daily,
        expected_fields={
            "2004-10-03": 5,
            "2023-10-01": 5,
            "2024-10-06": 5,
            "2025-10-05": 5,
        },
    )
    assert len(data) == 20
    assert data["race_date"].min() == "2004-10-03"
    assert excluded == {"1920-10-03": "Incomplete field or top-five ground truth"}


def test_win_reranking_keeps_every_larger_set() -> None:
    base = pd.DataFrame(
        {"horse_id": ["a", "b", "c", "d", "e"], "score": [5, 4, 3, 2, 1]}
    )
    head = pd.DataFrame(
        {"horse_id": ["e", "d", "c", "b", "a"], "score": [9, 8, 7, 6, 5]}
    )
    result = rerank_top_two(base, head)
    assert result["horse_id"].tolist() == ["b", "a", "c", "d", "e"]
    assert set(result["horse_id"].iloc[:2]) == {"a", "b"}
    assert set(result["horse_id"].iloc[:3]) == {"a", "b", "c"}
    assert set(result["horse_id"].iloc[:4]) == {"a", "b", "c", "d"}
    assert set(result["horse_id"].iloc[:5]) == {"a", "b", "c", "d", "e"}


@pytest.mark.parametrize("ids", [["a"], ["a", "a"], ["x", "y"]])
def test_reranking_rejects_mismatched_or_duplicate_fields(ids: list[str]) -> None:
    with pytest.raises(ValueError, match="matching unique"):
        rerank_top_two(
            pd.DataFrame({"horse_id": ids, "score": 1}),
            pd.DataFrame({"horse_id": ["a", "b"], "score": [1, 2]}),
        )


def test_reranking_rejects_invalid_win_head() -> None:
    base = pd.DataFrame({"horse_id": ["a", "b"], "score": [2, 1]})
    with pytest.raises(ValueError, match="matching unique"):
        rerank_top_two(base, pd.DataFrame({"horse_id": ["a", "a"], "score": [1, 2]}))
    with pytest.raises(ValueError, match="nonfinite"):
        rerank_top_two(
            base, pd.DataFrame({"horse_id": ["a", "b"], "score": [1, float("nan")]})
        )


def test_missing_evaluation_year_is_fatal(
    complete_history: pd.DataFrame, daily: pd.DataFrame
) -> None:
    identities = pd.DataFrame(
        columns=["source_horse_id", "race_date", "person_kind", "source_person_id"]
    )
    with pytest.raises(ValueError, match="Every evaluation year"):
        build_dataset(
            complete_history,
            identities=identities,
            people_daily=daily,
            expected_fields={"2023-10-01": 5, "2024-10-06": 5},
        )
