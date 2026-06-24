from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import cast, override

import lightgbm as lgb
from lightgbm.basic import LightGBMError
import numpy as np
import polars as pl
import pytest

import corner_lightgbm as subject


class FakeBooster:
    def save_model(self, path: str) -> None:
        Path(path).write_text("fake model", encoding="utf-8")


class FakeRegressor:
    booster_: FakeBooster

    def __init__(self, **_kwargs: object) -> None:
        self.booster_ = FakeBooster()

    def fit(self, _features: object, _target: object, **_kwargs: object) -> FakeRegressor:
        return self

    def predict(self, features: object) -> np.ndarray:
        return np.linspace(0.15, 0.85, len(cast(pl.DataFrame, features)))


class FakeRanker(FakeRegressor):
    pass


class FakeClassifier(FakeRegressor):
    def predict_proba(self, features: object) -> np.ndarray:
        probabilities = np.linspace(0.25, 0.75, len(cast(pl.DataFrame, features)))
        return np.column_stack([1 - probabilities, probabilities])


class FakeGpuFallbackRegressor(FakeRegressor):
    fit_calls: int

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.fit_calls = 0

    def set_params(self, **_params: object) -> FakeGpuFallbackRegressor:
        return self

    @override
    def fit(self, _features: object, _target: object, **_kwargs: object) -> FakeGpuFallbackRegressor:
        self.fit_calls += 1
        if self.fit_calls == 1:
            raise LightGBMError("GPU Tree Learner was not enabled in this build.")
        return self


def make_model_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    feature_columns = set(subject.FEATURE_COLUMNS)
    raw_columns = {
        "babajotai_code_dirt",
        "babajotai_code_shiba",
        "barei",
        "finish_norm",
        "futan_juryo",
        "grade_code",
        "horse_key",
        "juryo_shubetsu_code",
        "keibajo_code",
        "kyori",
        "kyoso_joken_code",
        "kyoso_shubetsu_code",
        "race_bango",
        "race_date",
        "race_id",
        "seibetsu_code",
        "source",
        "shusso_tosu",
        "tansho_ninkijun",
        "tansho_odds",
        "track_code",
        "umaban",
    }
    columns = feature_columns | raw_columns | set(subject.TARGET_COLUMNS)
    for race_index, race_date in enumerate(["20250101", "20250101", "20260101", "20260101"]):
        for horse_offset in range(2):
            umaban = horse_offset + 1
            row: dict[str, object] = {
                column: 0.2 + horse_offset * 0.1 for column in columns
            }
            row.update(
                {
                    "horse_key": f"H{race_index}{horse_offset}",
                    "keibajo_code": "05",
                    "kyori": "1600",
                    "race_bango": "01",
                    "race_date": race_date,
                    "race_id": f"R{race_index}",
                    "source": "jra",
                    "shusso_tosu": "02",
                    "tansho_ninkijun": str(umaban),
                    "tansho_odds": str(10 + umaban),
                    "track_code": "12",
                    "umaban": str(umaban),
                },
            )
            for target_index, target_column in enumerate(subject.TARGET_COLUMNS):
                row[target_column] = 0.2 * target_index + horse_offset * 0.1
            rows.append(row)
    return pl.DataFrame(rows)


def test_normalize_date() -> None:
    assert subject.normalize_date("2026-05-14") == "20260514"


def test_load_dataset_adds_style_and_relative_features(tmp_path: Path) -> None:
    input_path = tmp_path / "dataset.csv"
    make_model_frame().write_csv(input_path)

    loaded = subject.load_dataset(str(input_path))

    assert "front_runner_score" in loaded.columns
    assert "horse_number_norm_race_rank" in loaded.columns
    assert loaded["race_date"].to_list() == [
        "20250101",
        "20250101",
        "20250101",
        "20250101",
        "20260101",
        "20260101",
        "20260101",
        "20260101",
    ]


def test_load_dataset_handles_optional_history_columns(tmp_path: Path) -> None:
    input_path = tmp_path / "dataset.csv"
    frame = make_model_frame().with_columns(
        pl.lit(12).alias("horse_odds_avg"),
        pl.lit(13).alias("horse_odds_recent_avg"),
        pl.lit(14).alias("horse_odds_last"),
        pl.lit(36).alias("horse_kohan_3f_avg"),
        pl.lit(37).alias("horse_kohan_3f_recent_avg"),
        pl.lit(38).alias("horse_kohan_3f_last"),
        pl.lit(30).alias("horse_days_since_last_start"),
    )
    frame.write_csv(input_path)

    loaded = subject.load_dataset(str(input_path))

    assert loaded["horse_log_odds_avg"].gt(0).all()
    assert loaded["horse_kohan_3f_norm_recent_avg"].gt(0).all()
    assert loaded["horse_days_since_last_start_norm"].is_between(0, 2).all()


def test_load_dataset_derives_vector_neighbor_features(tmp_path: Path) -> None:
    input_path = tmp_path / "dataset.csv"
    frame = make_model_frame().drop(subject.VECTOR_NEIGHBOR_FEATURE_COLUMNS)
    frame.write_csv(input_path)

    loaded = subject.load_dataset(str(input_path))
    future_rows = loaded.filter(pl.col("race_date") == "20260101")

    assert future_rows["vector_neighbor10_count"].gt(0).all()
    assert future_rows["vector_neighbor10_corner1_avg"].is_between(0, 1).all()


def test_load_dataset_derives_vector_neighbor_features_with_mlx(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    try:
        importlib.import_module("mlx.core")
    except (ImportError, OSError):
        pytest.skip("MLX requires Apple Silicon/macOS")
    input_path = tmp_path / "dataset.csv"
    frame = make_model_frame().drop(subject.VECTOR_NEIGHBOR_FEATURE_COLUMNS)
    frame.write_csv(input_path)
    monkeypatch.setenv("PC_KEIBA_VECTOR_BACKEND", "mlx")

    loaded = subject.load_dataset(str(input_path))
    future_rows = loaded.filter(pl.col("race_date") == "20260101")

    assert future_rows["vector_neighbor10_count"].gt(0).all()
    assert future_rows["vector_neighbor30_corner2_avg"].is_between(0, 1).all()


def test_load_dataset_falls_back_when_mlx_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "dataset.csv"
    frame = make_model_frame().drop(subject.VECTOR_NEIGHBOR_FEATURE_COLUMNS)
    frame.write_csv(input_path)
    def fake_import_mlx_core() -> object:
        raise ImportError("mlx.core")

    monkeypatch.setenv("PC_KEIBA_VECTOR_BACKEND", "mlx")
    monkeypatch.setattr(subject, "import_mlx_core", fake_import_mlx_core)

    loaded = subject.load_dataset(str(input_path))

    assert loaded["vector_neighbor10_count"].sum() > 0


def test_race_order_score_and_ranker_target() -> None:
    frame = make_model_frame().with_columns(
        pl.Series("prediction", [0.1, 0.2, 0.1, 0.2, 0.2, 0.1, 0.2, 0.1]),
    )

    score = subject.race_order_score(frame, "corner1_norm", "prediction")
    rank_target = subject.ranker_target(frame, "corner1_norm")

    assert 0 <= score <= 1
    assert rank_target.to_list() == [1, 0, 1, 0, 1, 0, 1, 0]


def test_pairwise_dataset_and_prediction() -> None:
    frame = make_model_frame()
    pair_features, labels = subject.build_pairwise_dataset(frame, "corner1_norm")
    prediction = subject.apply_pairwise_model(
        frame,
        cast(lgb.LGBMClassifier, FakeClassifier()),
        "corner1_norm",
    )

    assert pair_features.columns == subject.PAIRWISE_FEATURE_COLUMNS
    assert labels.to_list() == [1, 1, 1, 1]
    assert prediction.is_between(0, 1).all()


def test_pairwise_helpers_handle_single_horse_race() -> None:
    frame = make_model_frame().head(1)
    pair_features, labels = subject.build_pairwise_dataset(frame, "corner1_norm")
    prediction = subject.apply_pairwise_model(
        frame,
        cast(lgb.LGBMClassifier, FakeClassifier()),
        "corner1_norm",
    )

    assert pair_features.is_empty()
    assert labels.is_empty()
    assert prediction.to_list() == [0.5]


def test_training_helpers_with_fake_lightgbm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lgb, "LGBMRegressor", FakeRegressor)
    monkeypatch.setattr(lgb, "LGBMRanker", FakeRanker)
    monkeypatch.setattr(lgb, "LGBMClassifier", FakeClassifier)
    monkeypatch.setattr(subject, "train_lstm_model", lambda _features, _target: object())
    monkeypatch.setattr(subject, "train_transformer_model", lambda _features, _target: object())
    monkeypatch.setattr(
        subject,
        "predict_neural_corner_model",
        lambda _model, features: pl.Series(np.linspace(0.2, 0.8, len(features))),
    )
    frame = make_model_frame()

    model = subject.train_model(frame, "corner1_norm")
    ranker = subject.train_ranker(frame, "corner1_norm")
    classifier = subject.train_pairwise_model(frame, "corner1_norm")

    assert isinstance(model, FakeRegressor)
    assert isinstance(ranker, FakeRanker)
    assert isinstance(classifier, FakeClassifier)


def test_lightgbm_gpu_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PC_KEIBA_LIGHTGBM_DEVICE", "gpu")
    model = FakeGpuFallbackRegressor()
    frame = make_model_frame()

    subject.fit_regressor_with_device_fallback(
        cast(lgb.LGBMRegressor, model),
        frame[subject.FEATURE_COLUMNS],
        frame["corner1_norm"],
    )

    assert model.fit_calls == 2


def test_top_vector_neighbor_candidates_empty_input_returns_empty() -> None:
    empty_positions = np.array([], dtype=np.int64)
    empty_distances = np.array([], dtype=np.float64)

    positions, distances = subject.top_vector_neighbor_candidates(empty_positions, empty_distances)

    assert len(positions) == 0
    assert len(distances) == 0


def test_top_vector_neighbor_candidates_selects_closest() -> None:
    candidate_positions = np.array([0, 1, 2, 3], dtype=np.int64)
    squared_distances = np.array([9.0, 1.0, 4.0, 16.0], dtype=np.float64)

    positions, distances = subject.top_vector_neighbor_candidates(candidate_positions, squared_distances)

    assert positions[0] == 1
    assert abs(distances[0] - 1.0) < 1e-9


def test_choose_ensemble_prediction_returns_best_candidate() -> None:
    frame = make_model_frame().with_columns(
        pl.Series("regression_corner1_norm", [0.1, 0.2, 0.1, 0.2, 0.2, 0.1, 0.2, 0.1]),
        pl.Series("ranker_corner1_norm", [0.1, 0.2, 0.1, 0.2, 0.2, 0.1, 0.2, 0.1]),
        pl.Series("pairwise_corner1_norm", [0.1, 0.2, 0.1, 0.2, 0.2, 0.1, 0.2, 0.1]),
    )

    prediction, alpha, scores = subject.choose_ensemble_prediction(
        frame,
        "corner1_norm",
        "regression_corner1_norm",
        "ranker_corner1_norm",
        "pairwise_corner1_norm",
    )

    assert prediction.is_between(0, 1).all()
    assert alpha >= 0
    assert "0" in scores


def test_main_writes_metrics_and_predictions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PC_KEIBA_CORNER_FAST_GRID", "1")
    monkeypatch.setattr(lgb, "LGBMRegressor", FakeRegressor)
    monkeypatch.setattr(lgb, "LGBMRanker", FakeRanker)
    monkeypatch.setattr(lgb, "LGBMClassifier", FakeClassifier)
    input_path = tmp_path / "dataset.csv"
    model_dir = tmp_path / "models"
    predictions_path = tmp_path / "predictions.csv"
    metrics_path = tmp_path / "metrics.json"
    make_model_frame().write_csv(input_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "train-corner-lightgbm.py",
            "--input",
            str(input_path),
            "--train-to-date",
            "2025-12-31",
            "--test-from-date",
            "2026-01-01",
            "--test-to-date",
            "2026-12-31",
            "--model-output",
            str(model_dir),
            "--predictions-output",
            str(predictions_path),
            "--metrics-output",
            str(metrics_path),
        ],
    )

    subject.main()

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    predictions = pl.read_csv(predictions_path)
    assert metrics["train_rows"] == 4
    assert metrics["test_rows"] == 4
    assert predictions["predicted_corner1_norm"].is_between(0, 1).all()
    assert (model_dir / "corner1_norm.txt").exists()


def test_main_uses_neural_predictions_when_models_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PC_KEIBA_CORNER_FAST_GRID", "1")
    monkeypatch.setattr(lgb, "LGBMRegressor", FakeRegressor)
    monkeypatch.setattr(lgb, "LGBMRanker", FakeRanker)
    monkeypatch.setattr(lgb, "LGBMClassifier", FakeClassifier)
    fake_neural_model = object()
    monkeypatch.setattr(subject, "train_lstm_model", lambda _seq, _target: fake_neural_model)
    monkeypatch.setattr(subject, "train_transformer_model", lambda _seq, _target: fake_neural_model)
    monkeypatch.setattr(
        subject,
        "predict_neural_corner_model",
        lambda _model, features: pl.Series(np.linspace(0.1, 0.9, len(features))),
    )
    input_path = tmp_path / "dataset.csv"
    model_dir = tmp_path / "models"
    predictions_path = tmp_path / "predictions.csv"
    metrics_path = tmp_path / "metrics.json"
    make_model_frame().write_csv(input_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "train-corner-lightgbm.py",
            "--input",
            str(input_path),
            "--train-to-date",
            "2025-12-31",
            "--test-from-date",
            "2026-01-01",
            "--test-to-date",
            "2026-12-31",
            "--model-output",
            str(model_dir),
            "--predictions-output",
            str(predictions_path),
            "--metrics-output",
            str(metrics_path),
        ],
    )

    subject.main()

    predictions = pl.read_csv(predictions_path)
    assert predictions["predicted_corner1_norm"].is_between(0, 1).all()


def test_stacker_training_uses_pairwise_model_not_regression_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify pairwise_prediction_column is set by apply_pairwise_model during stacker training."""
    monkeypatch.setenv("PC_KEIBA_CORNER_FAST_GRID", "1")
    monkeypatch.setattr(lgb, "LGBMRegressor", FakeRegressor)
    monkeypatch.setattr(lgb, "LGBMRanker", FakeRanker)
    monkeypatch.setattr(lgb, "LGBMClassifier", FakeClassifier)
    monkeypatch.setattr(subject, "train_lstm_model", lambda _seq, _target: object())
    monkeypatch.setattr(subject, "train_transformer_model", lambda _seq, _target: object())
    monkeypatch.setattr(
        subject,
        "predict_neural_corner_model",
        lambda _model, features: pl.Series(np.linspace(0.2, 0.8, len(features))),
    )

    captured_stacking_frames: list[pl.DataFrame] = []
    original_train_stacking = subject.train_stacking_model

    def capturing_train_stacking(
        train: pl.DataFrame, target_column: str, stacking_features: pl.DataFrame,
    ) -> lgb.LGBMRegressor:
        captured_stacking_frames.append(stacking_features.clone())
        return original_train_stacking(train, target_column, stacking_features)

    monkeypatch.setattr(subject, "train_stacking_model", capturing_train_stacking)

    input_path = tmp_path / "dataset.csv"
    make_model_frame().write_csv(input_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "train-corner-lightgbm.py",
            "--input", str(input_path),
            "--train-to-date", "2025-12-31",
            "--test-from-date", "2026-01-01",
            "--test-to-date", "2026-12-31",
            "--model-output", str(tmp_path / "models"),
            "--predictions-output", str(tmp_path / "preds.csv"),
            "--metrics-output", str(tmp_path / "metrics.json"),
        ],
    )

    subject.main()

    # At least one stacking frame must have been captured
    assert len(captured_stacking_frames) > 0
    sf = captured_stacking_frames[0]
    # pairwise column must exist and must not equal the regression column
    assert "pairwise" in "".join(sf.columns)
    assert "regression" in "".join(sf.columns)
    pairwise_col = [c for c in sf.columns if c.startswith("pairwise")][0]
    regression_col = [c for c in sf.columns if c.startswith("regression")][0]
    # In-sample pairwise predictions from apply_pairwise_model differ from plain regression output
    assert not sf[pairwise_col].equals(sf[regression_col])
