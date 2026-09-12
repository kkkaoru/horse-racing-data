from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from timesfm_finish_position import nar_banei_search as search


@pytest.fixture
def arguments(tmp_path: Path) -> list[str]:
    frames: list[pl.DataFrame] = []
    for year in (2020, 2021):
        frame = pl.DataFrame(
            {
                "race_id": [f"r{year}"] * 5,
                "race_date": [f"{year}0901"] * 5,
                "horse_id": ["a", "b", "c", "d", "e"],
                "horse_number": [1, 2, 3, 4, 5],
                "finish": [1, 2, 3, 4, 5],
                "baseline_score": [0.75, 1.0, 0.5, 0.0, 0.25],
                "year": [year] * 5,
                "cell_id": ["cell"] * 5,
            }
        )
        frames.append(frame)
        forecast = frame.with_columns(
            pl.lit(8).alias("history_count"),
            pl.lit(f"{year - 1}1231").alias("latest_history_date"),
            *(
                pl.Series(f"{origin}_performance", [1.0, 0.75, 0.5, 0.25, 0.0])
                for origin in ("timesfm", "last", "mean5")
            ),
        )
        output = tmp_path / "cache/performance-full/cell" / str(year)
        output.mkdir(parents=True)
        forecast.write_parquet(output / "forecasts.parquet")
        (output / "report.json").write_text("{}", encoding="utf-8")
    pl.concat(frames).write_parquet(tmp_path / "targets.parquet")
    return [
        "--targets",
        str(tmp_path / "targets.parquet"),
        "--cache",
        str(tmp_path / "cache"),
        "--output",
        str(tmp_path / "search"),
        "--profiles",
        "performance-full",
        "--years",
        "2020",
        "2021",
        "2022",
        "--trials",
        "2",
    ]


def test_real_chronological_campaign_and_empty_final_year(
    arguments: list[str], tmp_path: Path
) -> None:
    search.main(
        [
            *arguments,
            "--cells",
            "cell",
            "--normalizations",
            "rank",
            "centered",
            "innovation",
            "--half-life-days",
            "0",
            "14",
        ]
    )
    root = tmp_path / "search/timesfm/cell"
    first = json.loads((root / "2020/report.json").read_text(encoding="utf-8"))
    second = json.loads((root / "2021/report.json").read_text(encoding="utf-8"))
    missing = json.loads((root / "2022/report.json").read_text(encoding="utf-8"))
    assert first["cold_start"] is True
    assert first["trials"] == 0
    assert second["development_years"] == [2020]
    assert second["trials"] == 2
    assert second["promotion_eligible"] is False
    assert missing["races"] == 0
    assert missing["exact_hits"] is None
    predictions = pl.read_parquet(root / "2021/predictions.parquet")
    assert sorted(predictions["predicted_rank"].to_list()) == [1, 2, 3, 4, 5]
    with pytest.raises(ValueError, match="already exists"):
        search.main(arguments)


def test_default_search_profiles_remain_compatible() -> None:
    assert search.DEFAULT_PROFILES == (
        "performance-full",
        "performance-speed-full",
        "performance-32",
        "performance-frozen",
        "day-speed-full",
        "performance-day-speed-full",
    )


def test_no_cells(arguments: list[str]) -> None:
    with pytest.raises(ValueError, match="No matching"):
        search.main([*arguments, "--cells", "absent"])


@pytest.mark.parametrize("invalid", [("--trials", "0"), ("--half-life-days", "-1")])
def test_invalid_budget(arguments: list[str], invalid: tuple[str, str]) -> None:
    with pytest.raises(SystemExit, match="2"):
        search.main([*arguments, *invalid])


def test_mismatched_and_missing_cache(arguments: list[str], tmp_path: Path) -> None:
    assert arguments
    targets = pl.read_parquet(tmp_path / "targets.parquet")
    with pytest.raises(FileNotFoundError):
        search.load_cache(targets, tmp_path / "cache", ["performance-32"])
    path = tmp_path / "cache/performance-full/cell/2020/forecasts.parquet"
    frame = pl.read_parquet(path)
    frame.with_columns(pl.lit(99.0).alias("baseline_score")).write_parquet(path)
    with pytest.raises(ValueError, match="complete expected cohort"):
        search.load_cache(targets, tmp_path / "cache", ["performance-full"])


def test_clock_resolution(
    arguments: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(search.time, "perf_counter", lambda: 1.0)
    search.main([*arguments, "--years", "2020", "--origins", "timesfm"])
    report = json.loads(
        (tmp_path / "search/timesfm/cell/2020/report.json").read_text(encoding="utf-8")
    )
    assert report["trials_per_second"] == 0.0
