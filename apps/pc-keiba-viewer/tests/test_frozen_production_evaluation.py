from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from catboost import CatBoost, Pool
from learning.frozen_production_evaluation import main, score_frozen_catboost
from numpy.typing import NDArray


@pytest.fixture
def model() -> CatBoost:
    fitted = CatBoost({"iterations": 2, "verbose": False, "allow_writing_files": False})
    fitted.fit(Pool([[1.0], [0.0]], label=[1.0, 0.0], feature_names=["x"]))
    return fitted


@pytest.fixture
def features() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source": ["nar", "nar"],
            "race_date": ["20260905", "20260905"],
            "keibajo_code": ["83", "83"],
            "race_bango": ["01", "01"],
            "ketto_toroku_bango": ["a", "b"],
            "umaban": [1, 2],
            "finish_position": [1, 2],
            "x": [1.0, None],
        }
    )


def test_reuses_serving_null_coercion(model: CatBoost, features: pl.DataFrame) -> None:
    result = score_frozen_catboost(features, model, training_end="20260518")
    assert result["race_id"].to_list() == ["nar-20260905-83-01", "nar-20260905-83-01"]
    assert result["predicted_rank"].to_list() == [1, 2]


@pytest.mark.parametrize("integer", [False, True])
def test_pandas_numeric_nulls_reach_model_as_nan(
    model: CatBoost,
    features: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
    integer: bool,
) -> None:
    recorded: list[list[float]] = []

    def predict(
        matrix: list[list[float]], *, prediction_type: str
    ) -> NDArray[np.float64]:
        assert prediction_type == "RawFormulaVal"
        recorded.extend(matrix)
        return np.array([1.0, 0.0])

    monkeypatch.setattr(model, "predict", predict)
    source = features.with_columns(pl.col("x").cast(pl.Int32)) if integer else features
    score_frozen_catboost(source, model, training_end="20260518", frame_loader="pandas")
    assert recorded[0] == [1.0]
    assert np.isnan(recorded[1][0])


def test_pandas_preserves_metadata_order_and_missingness(
    model: CatBoost, features: pl.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded: list[list[float]] = []

    def predict(
        matrix: list[list[float]], *, prediction_type: str
    ) -> NDArray[np.float64]:
        assert prediction_type == "RawFormulaVal"
        recorded.extend(matrix)
        return np.array([1.0, 0.0])

    monkeypatch.setattr(CatBoost, "feature_names_", property(lambda _: ["y", "x"]))
    monkeypatch.setattr(model, "predict", predict)
    source = features.with_columns(pl.Series("y", ["7.5", None]))
    score_frozen_catboost(source, model, training_end="20260518", frame_loader="pandas")
    assert recorded[0] == [7.5, 1.0]
    assert np.isnan(recorded[1][0])
    assert np.isnan(recorded[1][1])


def test_invalid_frame_loader(model: CatBoost, features: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="frame loader"):
        score_frozen_catboost(
            features, model, training_end="20260518", frame_loader="invalid"
        )


def test_unlabelled_inference_retains_all_entrants(
    model: CatBoost, features: pl.DataFrame
) -> None:
    unlabelled = features.with_columns(
        pl.lit(None).cast(pl.Int64).alias("finish_position")
    )
    result = score_frozen_catboost(
        unlabelled, model, training_end="20260518", observed_only=False
    )
    assert result.height == 2
    assert result["finish"].null_count() == 2
    assert result["predicted_rank"].to_list() == [1, 2]


def test_empty_inference_rejected(model: CatBoost, features: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="No runners"):
        score_frozen_catboost(
            features.head(0), model, training_end="20260518", observed_only=False
        )


def test_cli_inference_never_reports_accuracy(
    model: CatBoost, features: pl.DataFrame, tmp_path: Path
) -> None:
    model.save_model(tmp_path / "model.json", format="json")
    (tmp_path / "metadata.json").write_text(
        json.dumps(
            {
                "feature_names": ["x"],
                "model_version": "test",
                "train_date_range": ["20200101", "20260518"],
            }
        ),
        encoding="utf-8",
    )
    features.with_columns(
        pl.lit(None).cast(pl.Int64).alias("finish_position")
    ).write_parquet(tmp_path / "upcoming.parquet")
    main(
        [
            "--features",
            str(tmp_path / "upcoming.parquet"),
            "--model-dir",
            str(tmp_path),
            "--output",
            str(tmp_path / "result"),
            "--include-unlabelled",
            "--frame-loader",
            "pandas",
        ]
    )
    report = json.loads(
        (tmp_path / "result" / "report.json").read_text(encoding="utf-8")
    )
    assert report["metrics"] is None
    assert report["observed_labels"] == 0
    assert report["mode"] == "inference-only"
    assert report["frame_loader"] == "pandas"
    assert report["prophet_postprocessing_applied"] is False


def test_missing_feature_fails_closed(model: CatBoost, features: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="Missing model predictors"):
        score_frozen_catboost(features.drop("x"), model, training_end="20260518")


def test_unfitted_model_names_rejected(features: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="unique feature names"):
        score_frozen_catboost(features, CatBoost(), training_end="20260518")


def test_duplicate_feature_names_rejected(
    monkeypatch: pytest.MonkeyPatch,
    model: CatBoost,
    features: pl.DataFrame,
) -> None:
    monkeypatch.setattr(CatBoost, "feature_names_", property(lambda _: ["x", "x"]))
    with pytest.raises(ValueError, match="unique feature names"):
        score_frozen_catboost(features, model, training_end="20260518")


def test_empty_observations_rejected(model: CatBoost, features: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="No observed"):
        score_frozen_catboost(features.head(0), model, training_end="20260518")


@pytest.mark.parametrize("null_date", [True, False])
def test_invalid_date_type_rejected(
    model: CatBoost, features: pl.DataFrame, null_date: bool
) -> None:
    bad = features.with_columns(
        pl.lit(None if null_date else 20260905).alias("race_date")
    )
    with pytest.raises(ValueError, match="complete YYYYMMDD"):
        score_frozen_catboost(bad, model, training_end="20260518")


def test_noncanonical_training_date_rejected(
    model: CatBoost, features: pl.DataFrame
) -> None:
    with pytest.raises(ValueError, match="YYYYMMDD format"):
        score_frozen_catboost(features, model, training_end="2026-05-18")


def test_in_sample_evaluation_rejected(model: CatBoost, features: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="strictly follow"):
        score_frozen_catboost(features, model, training_end="20260905")


def test_duplicate_runner_rejected(model: CatBoost, features: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        score_frozen_catboost(
            pl.concat([features, features.head(1)]), model, training_end="20260518"
        )


@pytest.mark.parametrize("bad_score", [np.array([1.0]), np.array([1.0, np.nan])])
def test_invalid_scores_rejected(
    monkeypatch: pytest.MonkeyPatch,
    model: CatBoost,
    features: pl.DataFrame,
    bad_score: NDArray[np.float64],
) -> None:
    monkeypatch.setattr(model, "predict", lambda *_args, **_kwargs: bad_score)
    with pytest.raises(ValueError, match="finite score"):
        score_frozen_catboost(features, model, training_end="20260518")


@pytest.mark.parametrize("case", ["valid", "positional", "bad-date", "bad-order"])
def test_cli_metadata_and_persistence(
    tmp_path: Path,
    model: CatBoost,
    features: pl.DataFrame,
    case: str,
) -> None:
    if case == "positional":
        model = CatBoost(
            {"iterations": 2, "verbose": False, "allow_writing_files": False}
        )
        model.fit(Pool([[1.0], [0.0]], label=[1.0, 0.0]))
    model.save_model(tmp_path / "model.json", format="json")
    metadata = {
        "train_date_range": [
            "20260101",
            20260518 if case == "bad-date" else "20260518",
        ],
        "feature_names": ["y"] if case == "bad-order" else ["x"],
        "model_version": "fixture",
    }
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    features.write_parquet(tmp_path / "features.parquet")
    args = [
        "--features",
        str(tmp_path / "features.parquet"),
        "--model-dir",
        str(tmp_path),
        "--output",
        str(tmp_path / "result"),
    ]
    if case not in ("valid", "positional"):
        with pytest.raises((TypeError, ValueError), match="metadata|Metadata"):
            main(args)
        return
    main(args)
    report = json.loads(
        (tmp_path / "result" / "report.json").read_text(encoding="utf-8")
    )
    assert report["races"] == 1
    assert report["promotion_eligible"] is False


@pytest.mark.parametrize("declared_names", [[1], "x"])
def test_cli_rejects_malformed_predictor_metadata(
    tmp_path: Path,
    model: CatBoost,
    declared_names: list[int] | str,
) -> None:
    model.save_model(tmp_path / "model.json", format="json")
    metadata = {
        "train_date_range": ["20260101", "20260518"],
        "feature_names": declared_names,
    }
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(TypeError, match="string predictor names"):
        main(
            [
                "--features",
                "unused.parquet",
                "--model-dir",
                str(tmp_path),
                "--output",
                str(tmp_path / "result"),
            ]
        )
