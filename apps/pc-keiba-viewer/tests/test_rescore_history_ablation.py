from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from catboost import CatBoostRanker, Pool
from learning.rescore_history_ablation import (
    RescoreConfig,
    main,
    rescore_saved_ablation,
)


@pytest.fixture(params=["past_runs", "past_relative_speed_mean"])
def artifacts(tmp_path: Path, request: pytest.FixtureRequest) -> tuple[Path, Path]:
    feature = str(request.param)
    model = CatBoostRanker(
        iterations=2,
        loss_function="QuerySoftMax",
        verbose=False,
        allow_writing_files=False,
    )
    model.fit(
        Pool([[1.0], [0.0]], label=[1, 0], group_id=[0, 0], feature_names=[feature])
    )
    model.save_model(tmp_path / "model.cbm")
    metadata = {"training_end": "20251231", "venue": "83", "features": [feature]}
    (tmp_path / "report.json").write_text(json.dumps(metadata), encoding="utf-8")
    frame = pl.DataFrame(
        {
            "race_id": ["nar-20260905-83-01"] * 2,
            "race_date": ["20260905"] * 2,
            "horse_id": ["a", "b"],
            "horse_number": [1, 2],
            "finish": [1, 2],
            "venue": ["83", "83"],
            "past_runs": [1.0, None],
            "distance_m": [200.0, 200.0],
            "clock_seconds": [100.0, 200.0],
        }
    ).with_columns(
        pl.col("race_date").str.strptime(pl.Date, "%Y%m%d").alias("observed_date")
    )
    frame.write_parquet(tmp_path / "input.parquet")
    return tmp_path / "input.parquet", tmp_path


def test_rescore_does_not_refit(artifacts: tuple[Path, Path]) -> None:
    feature_path, model_dir = artifacts
    result = rescore_saved_ablation(
        feature_path,
        model_dir,
        model_dir / "rescored",
        config=RescoreConfig(venue="83", start=date(2026, 9, 1), end=date(2026, 9, 7)),
    )
    assert result["rows"] == 2
    assert result["refitted"] is False
    assert result["projection"] == "research-float32-preserve-NaN"


def test_overlap_rejected(artifacts: tuple[Path, Path]) -> None:
    feature_path, model_dir = artifacts
    with pytest.raises(ValueError, match="strictly after"):
        rescore_saved_ablation(
            feature_path,
            model_dir,
            model_dir / "rescored",
            config=RescoreConfig(
                venue="83", start=date(2025, 1, 1), end=date(2026, 1, 1)
            ),
        )


def test_reversed_range_rejected(artifacts: tuple[Path, Path]) -> None:
    feature_path, model_dir = artifacts
    with pytest.raises(ValueError, match="chronologically"):
        rescore_saved_ablation(
            feature_path,
            model_dir,
            model_dir / "rescored",
            config=RescoreConfig(
                venue="83", start=date(2026, 9, 7), end=date(2026, 1, 1)
            ),
        )


def test_wrong_venue_rejected(artifacts: tuple[Path, Path]) -> None:
    feature_path, model_dir = artifacts
    with pytest.raises(ValueError, match="venue differs"):
        rescore_saved_ablation(
            feature_path,
            model_dir,
            model_dir / "rescored",
            config=RescoreConfig(
                venue="54", start=date(2026, 1, 1), end=date(2026, 9, 7)
            ),
        )


def test_tampered_feature_contract_rejected(artifacts: tuple[Path, Path]) -> None:
    feature_path, model_dir = artifacts
    (model_dir / "report.json").write_text(
        json.dumps({"training_end": "20251231", "venue": "83", "features": ["finish"]}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="feature contract"):
        rescore_saved_ablation(
            feature_path,
            model_dir,
            model_dir / "rescored",
            config=RescoreConfig(
                venue="83", start=date(2026, 1, 1), end=date(2026, 9, 7)
            ),
        )


def test_cli_persists_report(
    artifacts: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    feature_path, model_dir = artifacts
    main(
        [
            "--features",
            str(feature_path),
            "--model-dir",
            str(model_dir),
            "--output",
            str(model_dir / "rescored"),
            "--venue",
            "83",
            "--from-date",
            "20260101",
            "--to-date",
            "20260907",
        ]
    )
    assert json.loads(capsys.readouterr().out)["promotion_eligible"] is False
