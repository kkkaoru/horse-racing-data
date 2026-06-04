from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import cast, final

import numpy as np
import pandas as pd
import pytest

import train_finish_position_stacking_meta as subject


FIXED_NOW: datetime = datetime(2026, 6, 4, 9, 0, 0, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_NOW


@final
class _FakeRegressor:
    feature_importances_: np.ndarray
    best_iteration_: int | None
    _n_features: int
    fit_calls: list[dict[str, object]]

    def __init__(self, *, n_features: int) -> None:
        self.feature_importances_ = np.array(
            [(i + 1) * 10 for i in range(n_features)], dtype=np.int64
        )
        self.best_iteration_ = 23
        self._n_features = n_features
        self.fit_calls = []

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        *,
        eval_set: list[tuple[np.ndarray, np.ndarray]] | None,
        callbacks: list[object] | None,
    ) -> None:
        self.fit_calls.append(
            {
                "X_shape": X.shape,
                "y_shape": y.shape,
                "eval_set_len": len(eval_set or []),
                "callback_count": len(callbacks or []),
            }
        )

    def predict(self, X: np.ndarray) -> np.ndarray:
        # Regression-style score: ascending row index = predicted earlier finish.
        return np.arange(X.shape[0], dtype=float)


def _build_dataset_frame(year_offsets: tuple[int, ...] = (0, 1, 2)) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    base_year = 2010
    for offset in year_offsets:
        year = base_year + offset
        for race_idx in range(160):
            for horse_idx in range(8):
                rows.append(
                    {
                        "race_id": f"jra:{year}:0101:05:{race_idx:02d}",
                        "ketto_toroku_bango": f"2010{race_idx:04d}{horse_idx:02d}",
                        "predicted_score": float((horse_idx * 1.5) - 4 + offset * 0.1),
                        "predicted_rank": horse_idx + 1,
                        "actual_finish_position": (horse_idx + 1) if horse_idx < 5 else 6,
                        "race_year": year,
                        "race_year_int": year,
                        "category": "jra",
                        "umaban": horse_idx + 1,
                        "futan_juryo": 55.0 + horse_idx * 0.5,
                        "horse_age": 4 + (horse_idx % 3),
                        "tansho_ninkijun": horse_idx + 1,
                        "kyori": 1200 + horse_idx * 100,
                        "track_code": "10" if horse_idx % 2 == 0 else "23",
                        "shusso_tosu": 8,
                        "horse_recent_kohan3f_avg5": 35.0 + horse_idx * 0.1,
                        "horse_recent_finish_position_avg5": float(horse_idx + 1),
                        "days_since_last_race": 30.0 + horse_idx,
                        "horse_career_track_win_rate": 0.1 * horse_idx,
                        "jockey_recent_30d_win_rate": 0.12,
                        "trainer_recent_30d_win_rate": 0.15,
                    }
                )
    return pd.DataFrame(rows)


def test_parse_args_train_mode_defaults() -> None:
    args = subject.parse_args(
        [
            "--mode",
            "train",
            "--cat",
            "jra",
            "--dataset-root",
            "tmp/ds",
            "--output-predictions-root",
            "tmp/out",
            "--output-model-dir",
            "tmp/model",
            "--model-version",
            "iter11-jra-cb+meta-v8",
        ]
    )
    norm = subject.normalize_train_args(args)
    assert norm["cat"] == "jra"
    assert norm["dataset_root"] == Path("tmp/ds")
    assert norm["output_predictions_root"] == Path("tmp/out")
    assert norm["output_model_dir"] == Path("tmp/model")
    assert norm["model_version"] == "iter11-jra-cb+meta-v8"
    assert norm["num_leaves"] == subject.DEFAULT_NUM_LEAVES
    assert norm["n_estimators"] == subject.DEFAULT_N_ESTIMATORS
    assert norm["learning_rate"] == subject.DEFAULT_LEARNING_RATE
    assert norm["random_state_base"] == subject.DEFAULT_RANDOM_STATE_BASE
    assert norm["val_fraction"] == subject.DEFAULT_VAL_FRACTION
    assert norm["fold_years"] is None


def test_parse_args_train_mode_with_fold_years() -> None:
    args = subject.parse_args(
        [
            "--mode",
            "train",
            "--cat",
            "nar",
            "--dataset-root",
            "tmp/ds",
            "--output-predictions-root",
            "tmp/out",
            "--output-model-dir",
            "tmp/model",
            "--model-version",
            "iter11-nar-xgb+meta-v8",
            "--fold-years",
            "2024,2025",
            "--val-fraction",
            "0.2",
        ]
    )
    norm = subject.normalize_train_args(args)
    assert norm["cat"] == "nar"
    assert norm["fold_years"] == (2024, 2025)
    assert norm["val_fraction"] == 0.2


def test_normalize_train_args_rejects_empty_fold_years() -> None:
    args = subject.parse_args(
        [
            "--mode",
            "train",
            "--cat",
            "jra",
            "--dataset-root",
            "x",
            "--output-predictions-root",
            "x",
            "--output-model-dir",
            "x",
            "--model-version",
            "v",
            "--fold-years",
            ",,",
        ]
    )
    with pytest.raises(ValueError, match="fold-years"):
        subject.normalize_train_args(args)


def test_normalize_train_args_rejects_bad_val_fraction() -> None:
    args = subject.parse_args(
        [
            "--mode",
            "train",
            "--cat",
            "jra",
            "--dataset-root",
            "x",
            "--output-predictions-root",
            "x",
            "--output-model-dir",
            "x",
            "--model-version",
            "v",
            "--val-fraction",
            "1.5",
        ]
    )
    with pytest.raises(ValueError, match="val-fraction"):
        subject.normalize_train_args(args)


def test_now_utc_returns_aware_datetime() -> None:
    value = subject.now_utc()
    assert value.tzinfo is not None


def test_format_iso_now_strips_microseconds() -> None:
    when = datetime(2026, 6, 4, 9, 0, 1, 555, tzinfo=timezone.utc)
    out = subject.format_iso_now(when)
    assert out == "2026-06-04T09:00:01Z"


def test_assign_distance_band_handles_all_branches() -> None:
    assert subject.assign_distance_band(None) == "intermediate"
    assert subject.assign_distance_band(float("nan")) == "intermediate"
    assert subject.assign_distance_band("not a number") == "intermediate"
    assert subject.assign_distance_band(1000) == "sprint"
    assert subject.assign_distance_band(1500) == "mile"
    assert subject.assign_distance_band(1800) == "intermediate"
    assert subject.assign_distance_band(2200) == "long"
    assert subject.assign_distance_band(3000) == "extended"


def test_assign_surface_dirt_flag_handles_branches() -> None:
    assert subject.assign_surface_dirt_flag(None) == 0
    assert subject.assign_surface_dirt_flag("") == 0
    assert subject.assign_surface_dirt_flag("23") == 1
    assert subject.assign_surface_dirt_flag("10") == 0


def test_encode_distance_band_one_hot_columns() -> None:
    bands = pd.Series(["sprint", "mile", "intermediate", "long", "extended"])
    out = subject.encode_distance_band_one_hot(bands)
    assert list(out.columns) == [
        "distance_band_sprint",
        "distance_band_mile",
        "distance_band_intermediate",
        "distance_band_long",
        "distance_band_extended",
    ]
    assert out["distance_band_sprint"].tolist() == [1, 0, 0, 0, 0]
    assert out["distance_band_extended"].tolist() == [0, 0, 0, 0, 1]


def test_add_within_race_normalised_produces_expected_columns() -> None:
    frame = _build_dataset_frame((0,)).head(16)
    out = subject.add_within_race_normalised(frame)
    expected = {
        "predicted_rank_norm",
        "score_z_in_race",
        "score_max_in_race_delta",
        "score_min_in_race_delta",
        "kohan3f_pct_rank_in_field",
        "finish_pos_avg5_z_in_race",
        "field_size_log",
    }
    assert expected.issubset(set(out.columns))
    assert float(out["score_max_in_race_delta"].iloc[0]) <= 0.0
    assert float(out["score_min_in_race_delta"].iloc[0]) >= 0.0
    assert float(out["field_size_log"].iloc[0]) > 0.0


def test_add_categorical_one_hots_adds_band_and_surface() -> None:
    frame = _build_dataset_frame((0,)).head(8)
    out = subject.add_categorical_one_hots(frame)
    assert "distance_band_sprint" in out.columns
    assert "surface_dirt_flag" in out.columns


def test_assemble_feature_frame_full_pipeline() -> None:
    frame = _build_dataset_frame((0,))
    out = subject.assemble_feature_frame(frame)
    cols = subject.feature_columns(out)
    assert "predicted_score" in cols
    assert "predicted_rank_norm" in cols
    assert "distance_band_sprint" in cols
    assert "surface_dirt_flag" in cols


def test_feature_columns_skips_missing() -> None:
    frame = pd.DataFrame({"predicted_score": [1.0, 2.0]})
    cols = subject.feature_columns(frame)
    assert cols == ["predicted_score"]


def test_rerank_within_race_ascends_score() -> None:
    frame = pd.DataFrame(
        {
            "race_id": ["r1", "r1", "r1", "r2", "r2"],
            "meta_score": [0.5, 2.1, -0.4, 1.0, 1.2],
        }
    )
    out = subject.rerank_within_race(frame, "meta_score")
    assert out["predicted_rank"].tolist() == [2, 3, 1, 1, 2]


def test_compute_oos_metrics_empty_frame_returns_zero() -> None:
    metrics = subject.compute_oos_metrics(pd.DataFrame())
    assert metrics == {
        "races": 0,
        "top1": 0.0,
        "place2": 0.0,
        "place3": 0.0,
        "top3_box": 0.0,
    }


def test_compute_oos_metrics_counts_hits() -> None:
    frame = pd.DataFrame(
        {
            "race_id": [
                "r1", "r1", "r1", "r1",
                "r2", "r2", "r2", "r2",
            ],
            "predicted_rank": [1, 2, 3, 4, 1, 2, 3, 4],
            "actual_finish_position": [1, 2, 3, 4, 2, 1, 3, 4],
        }
    )
    metrics = subject.compute_oos_metrics(frame)
    assert metrics["races"] == 2
    assert metrics["top1"] == pytest.approx(0.5)
    assert metrics["place2"] == pytest.approx(0.5)
    assert metrics["place3"] == pytest.approx(1.0)
    assert metrics["top3_box"] == pytest.approx(1.0)


def test_filter_to_fold_year_splits_correctly() -> None:
    frame = _build_dataset_frame((0, 1, 2))
    train, val = subject.filter_to_fold_year(frame, fold_year=2012)
    assert int(train["race_year"].max()) == 2011
    assert int(val["race_year"].min()) == 2012
    assert int(val["race_year"].max()) == 2012


def test_split_train_val_holds_out_trailing_races() -> None:
    frame = _build_dataset_frame((0, 1))
    train, val = subject.split_train_val(frame, val_fraction=0.5)
    assert not train.empty
    assert not val.empty
    assert train["race_id"].isin(val["race_id"]).sum() == 0


def test_split_train_val_caps_holdout_to_n_minus_one() -> None:
    frame = pd.DataFrame(
        {
            "race_id": ["r1", "r2"],
            "race_year": [2010, 2010],
        }
    )
    train, val = subject.split_train_val(frame, val_fraction=0.99)
    assert len(train) == 1
    assert len(val) == 1


def test_resolve_fold_years_returns_all_by_default() -> None:
    dataset = _build_dataset_frame((0, 1, 2))
    years = subject.resolve_fold_years(dataset, requested=None)
    assert years == (2010, 2011, 2012)


def test_resolve_fold_years_filters_requested() -> None:
    dataset = _build_dataset_frame((0, 1, 2))
    years = subject.resolve_fold_years(dataset, requested=(2011,))
    assert years == (2011,)


def test_resolve_fold_years_rejects_all_missing() -> None:
    dataset = _build_dataset_frame((0, 1))
    with pytest.raises(ValueError, match="none of the requested"):
        subject.resolve_fold_years(dataset, requested=(2099,))


def test_train_one_fold_skips_when_insufficient_train_rows() -> None:
    frame = _build_dataset_frame((0, 1))
    small = frame.head(5).copy()
    preds, meta = subject.train_one_fold(
        subject.assemble_feature_frame(small),
        cat="jra",
        fold_year=2011,
        model_version="v",
        num_leaves=8,
        n_estimators=5,
        learning_rate=0.1,
        min_child_samples=1,
        lambda_l2=0.0,
        early_stopping_rounds=2,
        random_state=43,
        val_fraction=0.2,
        regressor_factory=cast(
            subject.RegressorFactoryLike,
            lambda **kwargs: _FakeRegressor(n_features=1),
        ),
        early_stopping_factory=cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        now=FIXED_NOW,
    )
    assert preds.empty
    assert meta["skipped"] is True
    assert meta["skip_reason"] == "insufficient training rows"


def _large_dataset_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for race_idx in range(200):
        for horse_idx in range(8):
            rows.append(
                {
                    "race_id": f"jra:2010:0101:05:{race_idx:03d}",
                    "ketto_toroku_bango": f"2010{race_idx:04d}{horse_idx:02d}",
                    "predicted_score": float((horse_idx * 1.5) - 4),
                    "predicted_rank": horse_idx + 1,
                    "actual_finish_position": (horse_idx + 1) if horse_idx < 5 else 6,
                    "race_year": 2010,
                    "race_year_int": 2010,
                    "category": "jra",
                    "umaban": horse_idx + 1,
                    "futan_juryo": 55.0 + horse_idx * 0.5,
                    "horse_age": 4 + (horse_idx % 3),
                    "tansho_ninkijun": horse_idx + 1,
                    "kyori": 1200 + horse_idx * 100,
                    "track_code": "10" if horse_idx % 2 == 0 else "23",
                    "shusso_tosu": 8,
                    "horse_recent_kohan3f_avg5": 35.0 + horse_idx * 0.1,
                    "horse_recent_finish_position_avg5": float(horse_idx + 1),
                    "days_since_last_race": 30.0 + horse_idx,
                    "horse_career_track_win_rate": 0.1 * horse_idx,
                    "jockey_recent_30d_win_rate": 0.12,
                    "trainer_recent_30d_win_rate": 0.15,
                }
            )
    return pd.DataFrame(rows)


def test_train_one_fold_skips_when_no_oos_rows() -> None:
    frame = subject.assemble_feature_frame(_large_dataset_frame())
    preds, meta = subject.train_one_fold(
        frame,
        cat="jra",
        fold_year=2099,
        model_version="v",
        num_leaves=8,
        n_estimators=5,
        learning_rate=0.1,
        min_child_samples=1,
        lambda_l2=0.0,
        early_stopping_rounds=2,
        random_state=43,
        val_fraction=0.2,
        regressor_factory=cast(
            subject.RegressorFactoryLike,
            lambda **kwargs: _FakeRegressor(n_features=1),
        ),
        early_stopping_factory=cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        now=FIXED_NOW,
    )
    assert preds.empty
    assert meta["skipped"] is True
    assert meta["skip_reason"] == "no OOS rows for fold year"


def test_train_one_fold_runs_with_fake_regressor() -> None:
    frame = subject.assemble_feature_frame(_build_dataset_frame((0, 1, 2)))
    n_feats = len(subject.feature_columns(frame))

    def fake_factory(
        *,
        num_leaves: int,
        n_estimators: int,
        learning_rate: float,
        min_child_samples: int,
        lambda_l2: float,
        random_state: int,
    ) -> subject.LightGBMRegressorLike:
        _ = (
            num_leaves,
            n_estimators,
            learning_rate,
            min_child_samples,
            lambda_l2,
            random_state,
        )
        return cast(subject.LightGBMRegressorLike, _FakeRegressor(n_features=n_feats))

    preds, meta = subject.train_one_fold(
        frame,
        cat="jra",
        fold_year=2012,
        model_version="iter11-jra-cb+meta-v8",
        num_leaves=31,
        n_estimators=200,
        learning_rate=0.05,
        min_child_samples=100,
        lambda_l2=1.0,
        early_stopping_rounds=20,
        random_state=44,
        val_fraction=0.2,
        regressor_factory=cast(subject.RegressorFactoryLike, fake_factory),
        early_stopping_factory=cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        now=FIXED_NOW,
    )
    assert not preds.empty
    assert meta["skipped"] is False
    assert meta["fold_year"] == 2012
    assert meta["category"] == "jra"
    assert meta["model_version"] == "iter11-jra-cb+meta-v8"
    assert meta["random_state"] == 44
    assert meta["best_iteration"] == 23
    assert isinstance(meta["oos_metrics"], dict)
    assert isinstance(meta["feature_columns"], list)
    assert isinstance(meta["feature_importance_top10"], list)
    assert len(cast(list[object], meta["feature_importance_top10"])) <= 10
    assert "predicted_rank" in preds.columns


def test_write_fold_predictions_skips_empty_frame() -> None:
    calls: list[tuple[pd.DataFrame, Path]] = []
    subject.write_fold_predictions(
        pd.DataFrame(),
        Path("ignored"),
        cast(
            subject.PartitionedParquetWriterLike,
            lambda frame, output_dir: calls.append((frame, output_dir)),
        ),
    )
    assert calls == []


def test_write_fold_predictions_invokes_writer() -> None:
    calls: list[tuple[pd.DataFrame, Path]] = []
    frame = pd.DataFrame({"race_id": ["a"]})
    subject.write_fold_predictions(
        frame,
        Path("out"),
        cast(
            subject.PartitionedParquetWriterLike,
            lambda f, output_dir: calls.append((f, output_dir)),
        ),
    )
    assert len(calls) == 1
    assert calls[0][1] == Path("out")


def test_write_fold_metadata_writes_with_correct_path() -> None:
    captured: dict[str, Path] = {}

    def json_writer(payload: dict[str, object], path: Path) -> None:
        _ = payload
        captured["path"] = path

    subject.write_fold_metadata(
        {"fold_year": 2024},
        Path("/tmp/model"),
        2024,
        cast(subject.JsonWriterLike, json_writer),
    )
    assert captured["path"] == Path("/tmp/model/fold_2024.json")


def test_fold_already_complete_returns_false_when_missing(tmp_path: Path) -> None:
    assert subject.fold_already_complete(tmp_path, 2024) is False


def test_fold_already_complete_returns_true_when_present(tmp_path: Path) -> None:
    (tmp_path / "fold_2024.json").write_text(
        json.dumps({"skipped": False}), encoding="utf-8"
    )
    assert subject.fold_already_complete(tmp_path, 2024) is True


def test_fold_already_complete_handles_corrupt_json(tmp_path: Path) -> None:
    (tmp_path / "fold_2024.json").write_text("not json", encoding="utf-8")
    assert subject.fold_already_complete(tmp_path, 2024) is False


def test_load_fold_metadata_reads_payload(tmp_path: Path) -> None:
    payload: dict[str, object] = {"fold_year": 2024, "skipped": False}
    (tmp_path / "fold_2024.json").write_text(json.dumps(payload), encoding="utf-8")
    assert subject.load_fold_metadata(tmp_path, 2024) == payload


def test_coerce_partition_value_handles_year_and_default() -> None:
    assert subject.coerce_partition_value("race_year", "2024") == 2024
    assert subject.coerce_partition_value("race_year", "not-int") == "not-int"
    assert subject.coerce_partition_value("category", "jra") == "jra"


def test_default_read_parquet_dir_reads_file(tmp_path: Path) -> None:
    frame = pd.DataFrame({"x": [1, 2]})
    file_path = tmp_path / "a.parquet"
    frame.to_parquet(file_path)
    out = subject.default_read_parquet_dir(file_path)
    assert list(out["x"]) == [1, 2]


def test_default_read_parquet_dir_walks_partitions(tmp_path: Path) -> None:
    leaf = tmp_path / "category=jra" / "race_year=2024"
    leaf.mkdir(parents=True)
    pd.DataFrame({"x": [1]}).to_parquet(leaf / "p.parquet")
    out = subject.default_read_parquet_dir(tmp_path)
    assert "category" in out.columns
    assert int(out["race_year"].iloc[0]) == 2024


def test_default_read_parquet_dir_empty_returns_empty(tmp_path: Path) -> None:
    out = subject.default_read_parquet_dir(tmp_path)
    assert out.empty


def test_default_write_partitioned_parquet_round_trip(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "category": ["jra"],
            "race_year": [2024],
            "x": [1.0],
        }
    )
    subject.default_write_partitioned_parquet(frame, tmp_path / "out")
    assert (tmp_path / "out" / "category=jra" / "race_year=2024").exists()


def test_default_write_json_creates_parent(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "file.json"
    subject.default_write_json({"k": "v"}, target)
    assert target.exists()
    assert json.loads(target.read_text(encoding="utf-8")) == {"k": "v"}


def test_run_train_writes_metadata_summary_and_predictions(tmp_path: Path) -> None:
    dataset = subject.assemble_feature_frame(_build_dataset_frame((0, 1, 2)))
    n_feats = len(subject.feature_columns(dataset))

    captured_parquet: list[tuple[pd.DataFrame, Path]] = []
    captured_json: list[tuple[dict[str, object], Path]] = []

    def reader(path: Path) -> pd.DataFrame:
        _ = path
        return _build_dataset_frame((0, 1, 2))

    def writer(frame: pd.DataFrame, output_dir: Path) -> None:
        captured_parquet.append((frame, output_dir))

    def json_writer(payload: dict[str, object], path: Path) -> None:
        captured_json.append((payload, path))

    def regressor_factory(
        *,
        num_leaves: int,
        n_estimators: int,
        learning_rate: float,
        min_child_samples: int,
        lambda_l2: float,
        random_state: int,
    ) -> subject.LightGBMRegressorLike:
        _ = (
            num_leaves,
            n_estimators,
            learning_rate,
            min_child_samples,
            lambda_l2,
            random_state,
        )
        return cast(subject.LightGBMRegressorLike, _FakeRegressor(n_features=n_feats))

    args: subject.TrainArgs = {
        "mode": subject.MODE_TRAIN,
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "preds",
        "output_model_dir": tmp_path / "model",
        "model_version": "iter11-jra-cb+meta-v8",
        "num_leaves": 8,
        "n_estimators": 50,
        "learning_rate": 0.05,
        "min_child_samples": 5,
        "lambda_l2": 1.0,
        "early_stopping_rounds": 5,
        "random_state_base": 42,
        "fold_years": (2011, 2012),
        "val_fraction": 0.2,
    }
    deps: subject.TrainDeps = {
        "dataset_reader": cast(subject.ParquetDirReaderLike, reader),
        "parquet_writer": cast(subject.PartitionedParquetWriterLike, writer),
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "regressor_factory": cast(subject.RegressorFactoryLike, regressor_factory),
        "early_stopping_factory": cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        "now": cast(subject.NowFactoryLike, _fixed_now),
    }
    rc = subject.run_train(args, deps, resume=False)
    assert rc == 0
    assert len(captured_parquet) >= 2
    json_paths = [p for _payload, p in captured_json]
    assert tmp_path / "model" / "fold_2011.json" in json_paths
    assert tmp_path / "model" / "fold_2012.json" in json_paths
    assert tmp_path / "model" / "metadata.json" in json_paths
    summary_payload = next(
        payload for payload, path in captured_json if path.name == "metadata.json"
    )
    assert summary_payload["model_version"] == "iter11-jra-cb+meta-v8"
    assert isinstance(summary_payload["fold_results"], list)


def test_run_train_resumes_when_metadata_exists(tmp_path: Path) -> None:
    model_dir = tmp_path / "model"
    model_dir.mkdir(parents=True)
    (model_dir / "fold_2012.json").write_text(
        json.dumps({"fold_year": 2012, "skipped": False, "category": "jra"}),
        encoding="utf-8",
    )
    json_calls: list[Path] = []

    def reader(path: Path) -> pd.DataFrame:
        _ = path
        return _build_dataset_frame((0, 1, 2))

    def writer(frame: pd.DataFrame, output_dir: Path) -> None:
        _ = frame
        json_calls.append(output_dir)

    def json_writer(payload: dict[str, object], path: Path) -> None:
        _ = payload
        json_calls.append(path)

    def regressor_factory(
        *,
        num_leaves: int,
        n_estimators: int,
        learning_rate: float,
        min_child_samples: int,
        lambda_l2: float,
        random_state: int,
    ) -> subject.LightGBMRegressorLike:
        _ = (
            num_leaves,
            n_estimators,
            learning_rate,
            min_child_samples,
            lambda_l2,
            random_state,
        )
        return cast(subject.LightGBMRegressorLike, _FakeRegressor(n_features=1))

    args: subject.TrainArgs = {
        "mode": subject.MODE_TRAIN,
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "preds",
        "output_model_dir": model_dir,
        "model_version": "iter11-jra-cb+meta-v8",
        "num_leaves": 8,
        "n_estimators": 50,
        "learning_rate": 0.05,
        "min_child_samples": 5,
        "lambda_l2": 1.0,
        "early_stopping_rounds": 5,
        "random_state_base": 42,
        "fold_years": (2012,),
        "val_fraction": 0.2,
    }
    deps: subject.TrainDeps = {
        "dataset_reader": cast(subject.ParquetDirReaderLike, reader),
        "parquet_writer": cast(subject.PartitionedParquetWriterLike, writer),
        "json_writer": cast(subject.JsonWriterLike, json_writer),
        "regressor_factory": cast(subject.RegressorFactoryLike, regressor_factory),
        "early_stopping_factory": cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        "now": cast(subject.NowFactoryLike, _fixed_now),
    }
    rc = subject.run_train(args, deps, resume=True)
    assert rc == 0
    assert any(str(c).endswith("metadata.json") for c in json_calls)


def test_run_train_raises_on_empty_dataset(tmp_path: Path) -> None:
    args: subject.TrainArgs = {
        "mode": subject.MODE_TRAIN,
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "preds",
        "output_model_dir": tmp_path / "model",
        "model_version": "v",
        "num_leaves": 8,
        "n_estimators": 5,
        "learning_rate": 0.1,
        "min_child_samples": 1,
        "lambda_l2": 0.0,
        "early_stopping_rounds": 1,
        "random_state_base": 42,
        "fold_years": None,
        "val_fraction": 0.2,
    }
    deps: subject.TrainDeps = {
        "dataset_reader": cast(subject.ParquetDirReaderLike, lambda path: pd.DataFrame()),
        "parquet_writer": cast(
            subject.PartitionedParquetWriterLike,
            lambda frame, output_dir: None,
        ),
        "json_writer": cast(subject.JsonWriterLike, lambda payload, path: None),
        "regressor_factory": cast(
            subject.RegressorFactoryLike,
            lambda **kwargs: _FakeRegressor(n_features=1),
        ),
        "early_stopping_factory": cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        "now": cast(subject.NowFactoryLike, _fixed_now),
    }
    with pytest.raises(ValueError, match="empty"):
        subject.run_train(args, deps)


def test_run_train_raises_when_race_year_column_missing(tmp_path: Path) -> None:
    frame = _build_dataset_frame((0,)).drop(columns=["race_year"])
    args: subject.TrainArgs = {
        "mode": subject.MODE_TRAIN,
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "preds",
        "output_model_dir": tmp_path / "model",
        "model_version": "v",
        "num_leaves": 8,
        "n_estimators": 5,
        "learning_rate": 0.1,
        "min_child_samples": 1,
        "lambda_l2": 0.0,
        "early_stopping_rounds": 1,
        "random_state_base": 42,
        "fold_years": None,
        "val_fraction": 0.2,
    }
    deps: subject.TrainDeps = {
        "dataset_reader": cast(subject.ParquetDirReaderLike, lambda path: frame),
        "parquet_writer": cast(
            subject.PartitionedParquetWriterLike,
            lambda f, output_dir: None,
        ),
        "json_writer": cast(subject.JsonWriterLike, lambda payload, path: None),
        "regressor_factory": cast(
            subject.RegressorFactoryLike,
            lambda **kwargs: _FakeRegressor(n_features=1),
        ),
        "early_stopping_factory": cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        "now": cast(subject.NowFactoryLike, _fixed_now),
    }
    with pytest.raises(ValueError, match="race_year"):
        subject.run_train(args, deps)


def test_run_train_assigns_category_when_missing(tmp_path: Path) -> None:
    frame = _build_dataset_frame((0, 1, 2)).drop(columns=["category"])
    assembled = subject.assemble_feature_frame(frame.copy())
    n_feats = len(subject.feature_columns(assembled))
    json_paths: list[Path] = []

    def reader(path: Path) -> pd.DataFrame:
        _ = path
        return frame

    def regressor_factory(
        *,
        num_leaves: int,
        n_estimators: int,
        learning_rate: float,
        min_child_samples: int,
        lambda_l2: float,
        random_state: int,
    ) -> subject.LightGBMRegressorLike:
        _ = (
            num_leaves,
            n_estimators,
            learning_rate,
            min_child_samples,
            lambda_l2,
            random_state,
        )
        return cast(subject.LightGBMRegressorLike, _FakeRegressor(n_features=n_feats))

    args: subject.TrainArgs = {
        "mode": subject.MODE_TRAIN,
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "preds",
        "output_model_dir": tmp_path / "model",
        "model_version": "v",
        "num_leaves": 8,
        "n_estimators": 5,
        "learning_rate": 0.1,
        "min_child_samples": 1,
        "lambda_l2": 0.0,
        "early_stopping_rounds": 1,
        "random_state_base": 42,
        "fold_years": (2012,),
        "val_fraction": 0.2,
    }
    deps: subject.TrainDeps = {
        "dataset_reader": cast(subject.ParquetDirReaderLike, reader),
        "parquet_writer": cast(
            subject.PartitionedParquetWriterLike,
            lambda f, output_dir: None,
        ),
        "json_writer": cast(
            subject.JsonWriterLike, lambda payload, path: json_paths.append(path)
        ),
        "regressor_factory": cast(subject.RegressorFactoryLike, regressor_factory),
        "early_stopping_factory": cast(
            subject.EarlyStoppingFactoryLike, lambda stopping_rounds: object()
        ),
        "now": cast(subject.NowFactoryLike, _fixed_now),
    }
    rc = subject.run_train(args, deps, resume=False)
    assert rc == 0
    assert any(p.name == "metadata.json" for p in json_paths)


def test_main_invokes_run_train(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    def fake_run_train(args: subject.TrainArgs, deps: subject.TrainDeps) -> int:
        _ = deps
        captured["cat"] = args["cat"]
        captured["model_version"] = args["model_version"]
        return 0

    monkeypatch.setattr(subject, "run_train", fake_run_train)
    rc = subject.main(
        [
            "--mode",
            "train",
            "--cat",
            "jra",
            "--dataset-root",
            str(tmp_path / "ds"),
            "--output-predictions-root",
            str(tmp_path / "preds"),
            "--output-model-dir",
            str(tmp_path / "model"),
            "--model-version",
            "iter11-jra-cb+meta-v8",
        ]
    )
    assert rc == 0
    assert captured["cat"] == "jra"
    assert captured["model_version"] == "iter11-jra-cb+meta-v8"


def test_main_rejects_unknown_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    @final
    class _Args(argparse.Namespace):
        mode: str = "fit"

    monkeypatch.setattr(subject, "parse_args", lambda argv: _Args())
    with pytest.raises(ValueError, match="unknown mode"):
        subject.main([])


def test_default_regressor_factory_imports_lightgbm() -> None:
    regressor = subject.default_regressor_factory(
        num_leaves=4,
        n_estimators=2,
        learning_rate=0.1,
        min_child_samples=1,
        lambda_l2=0.0,
        random_state=42,
    )
    assert hasattr(regressor, "fit")
    assert hasattr(regressor, "predict")


def test_default_early_stopping_factory_returns_callback() -> None:
    cb = subject.default_early_stopping_factory(5)
    assert cb is not None
