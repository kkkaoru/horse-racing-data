from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import finish_position_lightgbm as subject


def test_to_relevance_top1():
    assert subject.to_relevance(1) == 3


def test_to_relevance_top2():
    assert subject.to_relevance(2) == 2


def test_to_relevance_top3():
    assert subject.to_relevance(3) == 1


def test_to_relevance_outside_top3():
    assert subject.to_relevance(4) == 0


def test_to_relevance_far_back():
    assert subject.to_relevance(15) == 0


def test_to_relevance_series_handles_nan():
    result = subject.to_relevance_series(pd.Series([1, 2, np.nan, 4]))
    assert result.tolist() == [3, 2, 0, 0]


def test_resolve_feature_columns_excludes_meta_and_labels():
    columns = [
        "source",
        "race_id",
        "umaban",
        "finish_position",
        "finish_norm",
        "speed_index_avg_5",
        "jockey_career_win_rate",
    ]
    feature_columns = subject.resolve_feature_columns(columns)
    assert feature_columns == ["speed_index_avg_5", "jockey_career_win_rate"]


def test_build_group_sizes_preserves_order():
    df = pd.DataFrame(
        {"race_id": ["r1", "r1", "r1", "r2", "r2"], "umaban": [1, 2, 3, 1, 2]}
    )
    assert subject.build_group_sizes(df) == [3, 2]


def test_rank_within_race_higher_score_is_better():
    df = pd.DataFrame(
        {
            "race_id": ["r1", "r1", "r1"],
            "predicted_score": [0.1, 0.7, 0.4],
        }
    )
    ranks = subject.rank_within_race(df)
    assert ranks.tolist() == [3, 1, 2]


def test_detect_categorical_features_returns_known_columns():
    feature_columns = ["track_code", "grade_code", "speed_index_avg_5"]
    assert subject.detect_categorical_features(feature_columns) == ["track_code", "grade_code"]


def test_detect_categorical_features_empty_when_absent():
    assert subject.detect_categorical_features(["speed_index_avg_5"]) == []


def test_encode_categoricals_converts_to_pandas_category():
    frame = pd.DataFrame({"track_code": ["10", "21"], "speed": [1.0, 2.0]})
    encoded = subject.encode_categoricals(frame, ["track_code"])
    assert str(encoded["track_code"].dtype) == "category"
    assert str(encoded["speed"].dtype) == "float64"


def test_sort_for_grouping_orders_by_race_then_umaban():
    df = pd.DataFrame(
        {"race_id": ["r2", "r1", "r1"], "umaban": [1, 2, 1], "value": ["a", "b", "c"]}
    )
    sorted_df = subject.sort_for_grouping(df)
    assert sorted_df["race_id"].tolist() == ["r1", "r1", "r2"]
    assert sorted_df["umaban"].tolist() == [1, 2, 1]


def test_select_feature_frame_returns_only_features():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    frame = subject.select_feature_frame(df, ["a", "c"])
    assert list(frame.columns) == ["a", "c"]


def test_training_params_from_args_casts_to_expected_types():
    args = subject.parse_args(
        [
            "train",
            "--train-csv",
            "x.csv",
            "--output-model",
            "x.lgb",
            "--num-iterations",
            "10",
            "--learning-rate",
            "0.1",
            "--num-leaves",
            "31",
            "--min-child-samples",
            "5",
            "--lambda-l2",
            "0.5",
        ]
    )
    params = subject.training_params_from_args(args)
    assert params == {
        "lambda_l2": 0.5,
        "learning_rate": 0.1,
        "min_child_samples": 5,
        "num_iterations": 10,
        "num_leaves": 31,
    }


def test_build_lightgbm_params_uses_lambdarank_with_ndcg_at_3():
    lgb_params = subject.build_lightgbm_params(
        {
            "lambda_l2": 0.0,
            "learning_rate": 0.05,
            "min_child_samples": 20,
            "num_iterations": 100,
            "num_leaves": 63,
        }
    )
    assert lgb_params["objective"] == "lambdarank"
    assert lgb_params["metric"] == "ndcg"
    assert lgb_params["eval_at"] == [3]


def make_synthetic_dataset(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    race_ids: list[str] = []
    umabans: list[int] = []
    finishes: list[int] = []
    horse_ids: list[str] = []
    features: list[list[float]] = []
    for race_index in range(8):
        race_id = f"race-{race_index}"
        n = 8
        ordering = rng.permutation(n)
        for slot in range(n):
            race_ids.append(race_id)
            umabans.append(slot + 1)
            horse_ids.append(f"h{race_index}-{slot}")
            finishes.append(int(ordering[slot]) + 1)
            features.append([float(rng.standard_normal()) for _ in range(5)])
    feature_columns = [f"feat_{idx}" for idx in range(5)]
    frame = pd.DataFrame(features, columns=feature_columns)
    frame["source"] = "jra"
    frame["race_date"] = "20260101"
    frame["kaisai_nen"] = "2026"
    frame["kaisai_tsukihi"] = "0101"
    frame["keibajo_code"] = "05"
    frame["race_bango"] = "01"
    frame["ketto_toroku_bango"] = horse_ids
    frame["umaban"] = umabans
    frame["category"] = "jra"
    frame["race_id"] = race_ids
    frame["finish_position"] = finishes
    frame["finish_norm"] = [f / 8 for f in finishes]
    frame["track_code"] = "10"
    frame["grade_code"] = " "
    return frame


def test_prepare_lgb_dataset_synthesises_train_bundle():
    df = make_synthetic_dataset()
    bundle = subject.prepare_lgb_dataset(df)
    assert len(bundle["feature_columns"]) >= 5
    assert "track_code" in bundle["categorical_features"]
    assert "grade_code" in bundle["categorical_features"]
    assert sum(bundle["group_sizes"]) == len(df)
    assert bundle["relevance"].dtype == np.int64
    assert int(bundle["relevance"].max()) == 3
    assert int(bundle["relevance"].min()) == 0


def test_train_lambdarank_returns_booster_and_metadata():
    df = make_synthetic_dataset()
    bundle = subject.prepare_lgb_dataset(df)
    booster, result = subject.train_lambdarank(
        bundle,
        None,
        {
            "lambda_l2": 0.0,
            "learning_rate": 0.1,
            "min_child_samples": 5,
            "num_iterations": 20,
            "num_leaves": 7,
        },
    )
    assert booster is not None
    assert result["train_rows"] == len(df)
    assert result["valid_rows"] == 0
    assert result["best_iteration"] >= 1


def test_score_dataset_returns_predictions_per_row():
    df = make_synthetic_dataset()
    bundle = subject.prepare_lgb_dataset(df)
    booster, _result = subject.train_lambdarank(
        bundle,
        None,
        {
            "lambda_l2": 0.0,
            "learning_rate": 0.1,
            "min_child_samples": 5,
            "num_iterations": 20,
            "num_leaves": 7,
        },
    )
    predictions = subject.score_dataset(booster, df)
    assert len(predictions) == len(df)
    assert list(predictions.columns) == [
        "race_id",
        "ketto_toroku_bango",
        "umaban",
        "predicted_score",
        "predicted_rank",
    ]
    assert predictions["predicted_rank"].min() == 1


def test_write_predictions_jsonl_writes_one_record_per_line(tmp_path: Path):
    predictions = pd.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["a", "b"],
            "umaban": [1, 2],
            "predicted_score": [0.4, 0.6],
            "predicted_rank": [2, 1],
        }
    )
    path = tmp_path / "out.jsonl"
    subject.write_predictions_jsonl(predictions, path)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["race_id"] == "r1"
    assert first["umaban"] == 1


def test_write_training_metadata_persists_json(tmp_path: Path):
    result: subject.TrainingResult = {
        "best_iteration": 10,
        "best_ndcg_at_3": 0.42,
        "elapsed_seconds": 1.5,
        "feature_columns": ["a", "b"],
        "train_rows": 100,
        "valid_rows": 0,
    }
    path = tmp_path / "meta.json"
    subject.write_training_metadata(result, path)
    parsed = json.loads(path.read_text(encoding="utf-8"))
    assert parsed["best_iteration"] == 10
    assert parsed["best_ndcg_at_3"] == 0.42


def test_save_and_load_booster_round_trip(tmp_path: Path):
    df = make_synthetic_dataset()
    bundle = subject.prepare_lgb_dataset(df)
    booster, _result = subject.train_lambdarank(
        bundle,
        None,
        {
            "lambda_l2": 0.0,
            "learning_rate": 0.1,
            "min_child_samples": 5,
            "num_iterations": 5,
            "num_leaves": 7,
        },
    )
    path = tmp_path / "model.lgb"
    subject.save_booster(booster, path)
    loaded = subject.load_booster(path)
    assert loaded is not None


def test_main_rejects_unknown_command():
    with pytest.raises(SystemExit):
        subject.main(["bogus"])


def test_run_train_command_writes_model_predictions_and_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    df = make_synthetic_dataset()
    train_csv = tmp_path / "train.csv"
    df.to_csv(train_csv, index=False)
    model_path = tmp_path / "model.lgb"
    predictions_path = tmp_path / "predictions.jsonl"
    metadata_path = tmp_path / "meta.json"
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    subject.run_train_command(
        subject.parse_args(
            [
                "train",
                "--train-csv",
                str(train_csv),
                "--output-model",
                str(model_path),
                "--output-predictions",
                str(predictions_path),
                "--output-metadata",
                str(metadata_path),
                "--num-iterations",
                "5",
                "--num-leaves",
                "7",
                "--min-child-samples",
                "5",
            ]
        )
    )
    assert model_path.exists()
    assert predictions_path.exists()
    assert metadata_path.exists()
    assert captured
