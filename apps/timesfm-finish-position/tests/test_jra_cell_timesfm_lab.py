from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest

APP_ROOT = Path(__file__).resolve().parents[1]
PREDICTOR_SRC = APP_ROOT.parent / "finish-position-predict-container" / "src"
SCRIPT = APP_ROOT / "scripts" / "run_jra_cell_timesfm_lab.py"
sys.path.insert(0, str(PREDICTOR_SRC))
spec = importlib.util.spec_from_file_location("run_jra_cell_timesfm_lab", SCRIPT)
assert spec is not None and spec.loader is not None
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


def test_current_comparison_rejects_missing_rank() -> None:
    predictions = [
        {
            "race_id": "jra:2026:0830:06:01",
            "horse_id": "h1",
            "finish_position": 1,
            "predicted_rank": None,
        }
    ]
    with pytest.raises(ValueError, match="Expected an integer"):
        subject._current_metrics(
            predictions, {("jra:2026:0830:06:01", "h1"): 1}, current_through="20260830"
        )


def test_imputation_is_fit_only_on_prior_rows() -> None:
    values = np.asarray([[1.0], [np.nan], [999.0]], dtype=np.float64)
    result = subject._finite_values(
        values, fitting_mask=np.asarray([True, True, False], dtype=np.bool_)
    )

    assert result[:, 0].tolist() == [1.0, 1.0, 999.0]
    with pytest.raises(ValueError, match="must align"):
        subject._finite_values(values, fitting_mask=np.asarray([True], dtype=np.bool_))


def test_horse_normalization_uses_only_fitting_histories() -> None:
    values = np.asarray([[1.0], [3.0], [99.0], [10.0]], dtype=np.float64)
    horses = np.asarray(["h1", "h1", "h1", "h2"], dtype=np.str_)
    normalized, locations, scales = subject._normalize_horse_histories(
        values,
        horses,
        fitting_mask=np.asarray([True, True, False, True], dtype=np.bool_),
    )

    assert normalized[:2, 0].tolist() == [-1.0, 1.0]
    assert locations[2, 0] == 2.0
    assert scales[2, 0] == 1.0
    with pytest.raises(ValueError, match="requires fitting rows"):
        subject._normalize_horse_histories(values, horses, fitting_mask=np.zeros(4, dtype=np.bool_))


def test_ranking_is_race_local_and_has_stable_horse_ties() -> None:
    scores = np.asarray([1.0, 1.0, -1.0, 2.0], dtype=np.float64)
    races = np.asarray(["r1", "r1", "r2", "r2"], dtype=np.str_)
    horses = np.asarray(["h2", "h1", "h3", "h4"], dtype=np.str_)

    assert subject._ranks(scores, races, horses).tolist() == [2, 1, 2, 1]


def test_market_rating_and_residual_are_race_local() -> None:
    races = np.asarray(["r1", "r1", "r1", "r2", "r2"], dtype=np.str_)
    horses = np.asarray(["h1", "h2", "h3", "h4", "h5"], dtype=np.str_)
    odds = np.asarray([2.0, 5.0, np.nan, 7.0, 3.0], dtype=np.float64)

    ratings = subject._market_percentile_rating(races, horses, odds)

    assert ratings.tolist() == [1.0, 0.5, 0.0, 0.0, 1.0]


def test_current_comparison_stops_at_authoritative_cutoff() -> None:
    predictions = [
        {
            "race_id": "jra:2026:0830:06:01",
            "horse_id": "h1",
            "finish_position": 1,
            "predicted_rank": 1,
        },
        {
            "race_id": "jra:2026:0905:06:01",
            "horse_id": "h2",
            "finish_position": 1,
            "predicted_rank": 2,
        },
    ]

    model, current, complete = subject._current_metrics(
        predictions, {("jra:2026:0830:06:01", "h1"): 2}, current_through="20260830"
    )

    assert complete is True
    assert model is not None and model["top1_hits"] == 1
    assert current is not None and current["top1_hits"] == 0
    with pytest.raises(ValueError, match="canonical race ID"):
        subject._race_date_from_id("bad")


def test_cli_requires_explicit_license_acceptance(tmp_path: Path) -> None:
    required = [
        "--history",
        str(tmp_path / "history.parquet"),
        "--production-plan",
        str(tmp_path / "plan.json"),
        "--current",
        str(tmp_path / "current.parquet"),
        "--output",
        str(tmp_path / "report.json"),
        "--target-date",
        "2026-09-12",
        "--pg-url",
        "postgresql://local",
    ]
    with pytest.raises(SystemExit):
        subject.parse_args(required)
    with pytest.raises(SystemExit):
        subject.parse_args([*required, "--max-cells", "0", "--accept-non-commercial-license"])
    args = subject.parse_args(
        [
            *required,
            "--cell-id",
            "jra-cell-example",
            "--accept-non-commercial-license",
        ]
    )
    assert args.rustuna_trials == 10_000
    assert args.cell_ids == ["jra-cell-example"]
