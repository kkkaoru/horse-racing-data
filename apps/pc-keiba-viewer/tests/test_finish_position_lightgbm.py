from __future__ import annotations

import json
from pathlib import Path
from typing import override

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


def test_parse_year_list_sorts_and_deduplicates():
    assert subject.parse_year_list("2023,2021,2023,2022") == [2021, 2022, 2023]


def test_parse_year_list_rejects_empty():
    with pytest.raises(ValueError, match="non-empty"):
        subject.parse_year_list(" , ,")


def test_filter_by_date_range_inclusive():
    df = pd.DataFrame({"race_date": ["20200101", "20210601", "20211231", "20220101"]})
    filtered = subject.filter_by_date_range(df, "20210101", "20211231")
    assert filtered["race_date"].tolist() == ["20210601", "20211231"]


def test_split_walk_forward_uses_year_window():
    df = pd.DataFrame(
        {
            "race_date": ["20191231", "20200101", "20201231", "20210101", "20210601", "20211231", "20220101"],
            "value": ["a", "b", "c", "d", "e", "f", "g"],
        }
    )
    fold = subject.split_walk_forward(df, "20160101", 2021)
    assert fold["valid_year"] == 2021
    assert fold["train_df"]["value"].tolist() == ["a", "b", "c"]
    assert fold["valid_df"]["value"].tolist() == ["d", "e", "f"]


def test_evaluate_predictions_computes_box_and_exact_hits():
    truth = pd.DataFrame(
        {
            "race_id": ["r1", "r1", "r1", "r2", "r2", "r2"],
            "ketto_toroku_bango": ["a", "b", "c", "d", "e", "f"],
            "finish_position": [1, 2, 3, 1, 2, 3],
        }
    )
    predictions = pd.DataFrame(
        {
            "race_id": ["r1", "r1", "r1", "r2", "r2", "r2"],
            "ketto_toroku_bango": ["a", "b", "c", "f", "e", "d"],
            "predicted_rank": [1, 2, 3, 1, 2, 3],
        }
    )
    metrics = subject.evaluate_predictions(predictions, truth)
    assert metrics["race_count"] == 2
    assert metrics["top1_accuracy"] == 0.5
    assert metrics["top3_box_accuracy"] == 1.0
    assert metrics["top3_exact_accuracy"] == 0.5


def test_aggregate_fold_metrics_averages():
    folds: list[subject.FoldMetrics] = [
        {
            "ndcg_at_3": 0.5,
            "race_count": 100,
            "top1_accuracy": 0.4,
            "top3_box_accuracy": 0.2,
            "top3_exact_accuracy": 0.05,
            "valid_rows": 1000,
            "valid_year": 2021,
        },
        {
            "ndcg_at_3": 0.7,
            "race_count": 110,
            "top1_accuracy": 0.6,
            "top3_box_accuracy": 0.3,
            "top3_exact_accuracy": 0.09,
            "valid_rows": 1100,
            "valid_year": 2022,
        },
    ]
    aggregate = subject.aggregate_fold_metrics(folds)
    assert aggregate["fold_count"] == 2
    assert aggregate["ndcg_at_3_mean"] == pytest.approx(0.6)
    assert aggregate["top1_accuracy_mean"] == pytest.approx(0.5)


def test_aggregate_fold_metrics_handles_empty():
    aggregate = subject.aggregate_fold_metrics([])
    assert aggregate["fold_count"] == 0
    assert aggregate["top1_accuracy_mean"] == 0.0


def test_write_walk_forward_report_writes_json(tmp_path: Path):
    output_path = tmp_path / "walk-forward.json"
    fold: subject.FoldMetrics = {
        "ndcg_at_3": 0.5,
        "race_count": 100,
        "top1_accuracy": 0.4,
        "top3_box_accuracy": 0.2,
        "top3_exact_accuracy": 0.05,
        "valid_rows": 1000,
        "valid_year": 2021,
    }
    aggregate = {"fold_count": 1, "ndcg_at_3_mean": 0.5}
    subject.write_walk_forward_report([fold], aggregate, output_path)
    parsed = json.loads(output_path.read_text(encoding="utf-8"))
    assert parsed["aggregate"]["fold_count"] == 1
    assert parsed["folds"][0]["valid_year"] == 2021


def make_walk_forward_dataset(seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    horses_per_race = 6
    for year in (2020, 2021):
        for race_index in range(8):
            race_id = f"{year}-r{race_index}"
            race_date = f"{year}{1 + race_index // 4:02d}{1 + race_index:02d}"
            ordering = rng.permutation(horses_per_race)
            for slot in range(horses_per_race):
                rows.append(
                    {
                        "source": "jra",
                        "race_date": race_date,
                        "kaisai_nen": str(year),
                        "kaisai_tsukihi": race_date[4:],
                        "keibajo_code": "05",
                        "race_bango": f"{race_index + 1:02d}",
                        "ketto_toroku_bango": f"{year}{race_index}{slot}",
                        "umaban": slot + 1,
                        "category": "jra",
                        "race_id": race_id,
                        "finish_position": int(ordering[slot]) + 1,
                        "finish_norm": (int(ordering[slot]) + 1) / horses_per_race,
                        "track_code": "10",
                        "grade_code": " ",
                        "feat_a": float(rng.standard_normal()),
                        "feat_b": float(rng.standard_normal()),
                        "feat_c": float(rng.standard_normal()),
                    }
                )
    return pd.DataFrame(rows)


def test_run_walk_forward_fold_returns_metrics():
    df = make_walk_forward_dataset()
    fold = subject.split_walk_forward(df, "20200101", 2021)
    _booster, predictions, metrics = subject.run_walk_forward_fold(
        fold,
        {
            "lambda_l2": 0.0,
            "learning_rate": 0.1,
            "min_child_samples": 2,
            "num_iterations": 10,
            "num_leaves": 7,
        },
    )
    assert metrics["valid_year"] == 2021
    assert metrics["race_count"] == 8
    assert len(predictions) == len(fold["valid_df"])


def test_run_walk_forward_command_writes_report_and_predictions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    df = make_walk_forward_dataset()
    csv_path = tmp_path / "full.csv"
    df.to_csv(csv_path, index=False)
    report_path = tmp_path / "report.json"
    predictions_dir = tmp_path / "predictions"
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    subject.run_walk_forward_command(
        subject.parse_args(
            [
                "walk-forward",
                "--csv",
                str(csv_path),
                "--train-start-date",
                "20200101",
                "--validation-years",
                "2021",
                "--output-report",
                str(report_path),
                "--output-predictions-dir",
                str(predictions_dir),
                "--num-iterations",
                "5",
                "--num-leaves",
                "7",
                "--min-child-samples",
                "2",
            ]
        )
    )
    assert report_path.exists()
    parsed = json.loads(report_path.read_text(encoding="utf-8"))
    assert parsed["aggregate"]["fold_count"] == 1
    assert (predictions_dir / "2021.jsonl").exists()
    assert captured


def test_suggest_hpo_params_uses_sampling_ranges():
    import optuna

    captured: dict[str, tuple[object, ...]] = {}

    class FakeTrial(optuna.trial.Trial):
        def __init__(self):
            pass

        @override
        def suggest_int(self, name: str, low: int, high: int, **kwargs: object) -> int:
            captured[name] = (low, high)
            return low

        @override
        def suggest_float(self, name: str, low: float, high: float, **kwargs: object) -> float:
            captured[name] = (low, high, bool(kwargs.get("log", False)))
            return low

    fake_trial = FakeTrial()
    params = subject.suggest_hpo_params(fake_trial, num_iterations=100)
    assert params["num_iterations"] == 100
    assert captured["num_leaves"] == (15, 255)
    assert captured["learning_rate"] == (0.01, 0.3, True)
    assert captured["min_child_samples"] == (5, 100)
    assert captured["lambda_l2"] == (0.0, 10.0, False)


def test_write_hpo_summary_persists_json(tmp_path: Path):
    summary: subject.HpoSummary = {
        "best_params": {
            "lambda_l2": 0.5,
            "learning_rate": 0.05,
            "min_child_samples": 10,
            "num_iterations": 100,
            "num_leaves": 31,
        },
        "best_value": 0.42,
        "n_trials": 5,
    }
    output_path = tmp_path / "best.json"
    subject.write_hpo_summary(summary, output_path)
    parsed = json.loads(output_path.read_text(encoding="utf-8"))
    assert parsed["best_value"] == 0.42
    assert parsed["best_params"]["num_leaves"] == 31


def test_run_hpo_command_writes_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    df = make_walk_forward_dataset()
    csv_path = tmp_path / "full.csv"
    df.to_csv(csv_path, index=False)
    output_path = tmp_path / "best.json"
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    summary = subject.run_hpo_command(
        subject.parse_args(
            [
                "hpo",
                "--csv",
                str(csv_path),
                "--train-start-date",
                "20200101",
                "--validation-years",
                "2021",
                "--output-best-params",
                str(output_path),
                "--n-trials",
                "2",
                "--num-iterations",
                "5",
            ]
        )
    )
    assert summary["n_trials"] == 2
    assert output_path.exists()
    parsed = json.loads(output_path.read_text(encoding="utf-8"))
    assert parsed["n_trials"] == 2
    assert "num_leaves" in parsed["best_params"]


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
