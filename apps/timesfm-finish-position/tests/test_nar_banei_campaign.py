from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest

from timesfm_finish_position import nar_banei_campaign as campaign
from timesfm_finish_position.domain import FloatArray


@dataclass
class FakeForecaster:
    checkpoint: str
    batch_size: int
    device: str
    checkpoint_revision: str = "fixture"

    @property
    def backend(self) -> str:
        return "test"

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        assert horizon == 1
        return tuple(context[:, -1:] for context in contexts)


@pytest.fixture
def arguments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    pl.DataFrame(
        {
            "horse_id": ["a"],
            "race_id": ["old"],
            "race_date": ["20190101"],
            "performance": [0.4],
            "relative_speed": [1.0],
            "day_speed": [0.2],
        }
    ).write_parquet(tmp_path / "observations.parquet")
    pl.DataFrame(
        {
            "horse_id": ["a"],
            "race_id": ["target"],
            "race_date": ["20200102"],
            "year": [2020],
            "cell_id": ["cell"],
        }
    ).write_parquet(tmp_path / "targets.parquet")
    monkeypatch.setattr(campaign, "TimesFm3Forecaster", FakeForecaster)
    return [
        "--input",
        str(tmp_path),
        "--output",
        str(tmp_path / "results"),
        "--profiles",
        "performance-full",
        "--device",
        "cpu",
        "--accept-non-commercial-license",
    ]


@pytest.mark.parametrize("profile", ["body-full", "performance-body-full"])
def test_optional_body_profiles(arguments: list[str], tmp_path: Path, profile: str) -> None:
    path = tmp_path / "observations.parquet"
    pl.read_parquet(path).with_columns(pl.lit(0.03).alias("body_weight")).write_parquet(path)
    campaign.main([*arguments, "--profiles", profile])
    result = pl.read_parquet(tmp_path / "results" / profile / "cell/2020/forecasts.parquet")
    assert result["timesfm_body_weight"].to_list() == [0.03]
    assert result["history_count_body_weight"].to_list() == [1]
    assert campaign.DEFAULT_PROFILES == (
        "performance-full",
        "performance-speed-full",
        "performance-32",
        "performance-frozen",
        "day-speed-full",
        "performance-day-speed-full",
    )


def test_cache_provenance_resume_and_selection(arguments: list[str], tmp_path: Path) -> None:
    campaign.main([*arguments, "--years", "2020", "--cells", "cell"])
    campaign.main(arguments)
    frame = pl.read_parquet(tmp_path / "results/performance-full/cell/2020/forecasts.parquet")
    assert frame["timesfm_performance"].to_list() == [0.4]
    observations = pl.read_parquet(tmp_path / "observations.parquet")
    observations.with_columns(pl.lit(0.6).alias("performance")).write_parquet(
        tmp_path / "observations.parquet"
    )
    with pytest.raises(ValueError, match="provenance changed"):
        campaign.main(arguments)


def test_day_speed_profiles(arguments: list[str], tmp_path: Path) -> None:
    campaign.main([*arguments, "--profiles", "day-speed-full", "performance-day-speed-full"])
    frame = pl.read_parquet(
        tmp_path / "results/performance-day-speed-full/cell/2020/forecasts.parquet"
    )
    assert frame["timesfm_day_speed"].to_list() == [0.2]
    assert frame["timesfm_performance"].to_list() == [0.4]


def test_empty_selection(arguments: list[str]) -> None:
    with pytest.raises(ValueError, match="No matching"):
        campaign.main([*arguments, "--years", "2025"])


def test_requires_license(arguments: list[str]) -> None:
    with pytest.raises(SystemExit, match="2"):
        campaign.main(arguments[:-1])


def test_cli_final_temporal_guard(arguments: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    def bad_forecast(*_args: object, **_kwargs: object) -> pl.DataFrame:
        return pl.DataFrame(
            {"race_date": ["20200102"], "latest_history_date": ["20200102"], "history_count": [1]}
        )

    monkeypatch.setattr(campaign, "forecast_targets", bad_forecast)
    with pytest.raises(ValueError, match="same-day or future"):
        campaign.main(arguments)
