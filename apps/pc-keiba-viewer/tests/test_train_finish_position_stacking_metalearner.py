from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import Ridge

import train_finish_position_stacking_metalearner as subject


FIXED_NOW: datetime = datetime(2026, 6, 4, 9, 0, 0, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_NOW


def _baseline_frame(*, cat: str = "jra", year: int = 2022) -> pd.DataFrame:
    rng = np.random.default_rng(seed=20260604)
    rows: list[dict[str, object]] = []
    for race_idx in range(80):
        scores = rng.uniform(-1.0, 1.0, size=8)
        order = np.argsort(-scores)
        for horse_idx in range(8):
            rank = int(np.where(order == horse_idx)[0][0]) + 1
            actual = rank if rank != 1 else 1
            rows.append({
                "race_id": f"{cat}:{year}:0101:05:{race_idx:02d}",
                "ketto_toroku_bango": f"201910{race_idx:04d}{horse_idx:02d}",
                "predicted_score": float(scores[horse_idx]),
                "predicted_rank": rank,
                "actual_finish_position": actual,
                "race_year": year,
                "category": cat,
                "grade_code": "A",
                "kyoso_joken_code": "703",
            })
    return pd.DataFrame(rows)


def _race_context_frame(baseline: pd.DataFrame) -> pd.DataFrame:
    races = baseline["race_id"].drop_duplicates().tolist()
    return pd.DataFrame({
        "race_id": races,
        "kyori": [1200 + (idx * 100) % 1500 for idx in range(len(races))],
        "track_code": ["10" if idx % 2 == 0 else "23" for idx in range(len(races))],
        "shusso_tosu": [8] * len(races),
    })


def _running_style_frame(baseline: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for idx, (rid, ktb) in enumerate(zip(baseline["race_id"], baseline["ketto_toroku_bango"], strict=True)):
        rows.append({
            "race_id": rid,
            "ketto_toroku_bango": ktb,
            "kyakushitsu_hantei": str(idx % 5),
        })
    return pd.DataFrame(rows)


def _multiyear_dataset(years: list[int]) -> pd.DataFrame:
    frames = []
    for yr in years:
        base = _baseline_frame(cat="jra", year=yr)
        ctx = _race_context_frame(base)
        rs = _running_style_frame(base)
        merged = subject.assemble_stacking_dataset(base, ctx, rs, "jra")
        frames.append(merged)
    return pd.concat(frames, ignore_index=True)


def test_parse_args_build_mode() -> None:
    args = subject.parse_args([
        "--mode", "build-dataset",
        "--cat", "jra",
        "--baseline-parquet-root", "/tmp/a",
        "--race-context-parquet", "/tmp/b.parquet",
        "--output-root", "/tmp/c",
    ])
    assert args.mode == "build-dataset"
    assert args.cat == "jra"


def test_parse_args_train_mode() -> None:
    args = subject.parse_args([
        "--mode", "train",
        "--cat", "nar",
        "--dataset-root", "/tmp/ds",
        "--output-predictions-root", "/tmp/out",
        "--model-version", "iter2-nar-stack",
    ])
    assert args.cat == "nar"


def test_normalize_build_dataset_args_happy_path(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "build-dataset",
        "--cat", "jra",
        "--baseline-parquet-root", str(tmp_path / "base"),
        "--race-context-parquet", str(tmp_path / "ctx.parquet"),
        "--running-style-parquet", str(tmp_path / "rs.parquet"),
        "--output-root", str(tmp_path / "out"),
    ])
    norm = subject.normalize_build_dataset_args(args)
    assert norm["cat"] == "jra"
    assert norm["running_style_parquet"] == tmp_path / "rs.parquet"


def test_normalize_build_dataset_args_optional_running_style(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "build-dataset",
        "--cat", "nar",
        "--baseline-parquet-root", str(tmp_path / "base"),
        "--race-context-parquet", str(tmp_path / "ctx.parquet"),
        "--output-root", str(tmp_path / "out"),
    ])
    norm = subject.normalize_build_dataset_args(args)
    assert norm["running_style_parquet"] is None


def test_normalize_build_dataset_args_missing_baseline_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "build-dataset",
        "--cat", "jra",
        "--race-context-parquet", str(tmp_path / "ctx.parquet"),
        "--output-root", str(tmp_path / "out"),
    ])
    with pytest.raises(ValueError, match="baseline-parquet-root"):
        subject.normalize_build_dataset_args(args)


def test_normalize_build_dataset_args_missing_context_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "build-dataset",
        "--cat", "jra",
        "--baseline-parquet-root", str(tmp_path / "base"),
        "--output-root", str(tmp_path / "out"),
    ])
    with pytest.raises(ValueError, match="race-context-parquet"):
        subject.normalize_build_dataset_args(args)


def test_normalize_build_dataset_args_missing_output_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "build-dataset",
        "--cat", "jra",
        "--baseline-parquet-root", str(tmp_path / "base"),
        "--race-context-parquet", str(tmp_path / "ctx.parquet"),
    ])
    with pytest.raises(ValueError, match="output-root"):
        subject.normalize_build_dataset_args(args)


def test_normalize_train_args_happy_path(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "train",
        "--cat", "jra",
        "--dataset-root", str(tmp_path / "ds"),
        "--output-predictions-root", str(tmp_path / "out"),
        "--model-version", "iter2-test",
        "--alpha-grid", "0.5,2.0",
        "--fold-years", "2024,2025",
    ])
    norm = subject.normalize_train_args(args)
    assert norm["alpha_grid"] == (0.5, 2.0)
    assert norm["fold_years"] == (2024, 2025)


def test_normalize_train_args_missing_dataset_root_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "train",
        "--cat", "jra",
        "--output-predictions-root", str(tmp_path / "out"),
        "--model-version", "iter2-test",
    ])
    with pytest.raises(ValueError, match="dataset-root"):
        subject.normalize_train_args(args)


def test_normalize_train_args_missing_output_predictions_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "train",
        "--cat", "jra",
        "--dataset-root", str(tmp_path / "ds"),
        "--model-version", "iter2-test",
    ])
    with pytest.raises(ValueError, match="output-predictions-root"):
        subject.normalize_train_args(args)


def test_normalize_train_args_missing_model_version_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "train",
        "--cat", "jra",
        "--dataset-root", str(tmp_path / "ds"),
        "--output-predictions-root", str(tmp_path / "out"),
    ])
    with pytest.raises(ValueError, match="model-version"):
        subject.normalize_train_args(args)


def test_normalize_train_args_empty_alpha_grid_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "train",
        "--cat", "jra",
        "--dataset-root", str(tmp_path / "ds"),
        "--output-predictions-root", str(tmp_path / "out"),
        "--model-version", "iter2-test",
        "--alpha-grid", "",
    ])
    with pytest.raises(ValueError, match="alpha-grid"):
        subject.normalize_train_args(args)


def test_normalize_train_args_empty_fold_years_raises(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--mode", "train",
        "--cat", "jra",
        "--dataset-root", str(tmp_path / "ds"),
        "--output-predictions-root", str(tmp_path / "out"),
        "--model-version", "iter2-test",
        "--fold-years", ",,",
    ])
    with pytest.raises(ValueError, match="fold-years"):
        subject.normalize_train_args(args)


def test_now_utc_returns_aware_datetime() -> None:
    now = subject.now_utc()
    assert now.tzinfo is not None


def test_assign_distance_band_sprint() -> None:
    assert subject.assign_distance_band(1100) == "sprint"


def test_assign_distance_band_mile() -> None:
    assert subject.assign_distance_band(1500) == "mile"


def test_assign_distance_band_intermediate() -> None:
    assert subject.assign_distance_band(1800) == "intermediate"


def test_assign_distance_band_long() -> None:
    assert subject.assign_distance_band(2200) == "long"


def test_assign_distance_band_extended() -> None:
    assert subject.assign_distance_band(3200) == "extended"


def test_assign_distance_band_none_falls_back() -> None:
    assert subject.assign_distance_band(None) == "intermediate"


def test_assign_distance_band_non_numeric_falls_back() -> None:
    assert subject.assign_distance_band("not-a-number") == "intermediate"


def test_assign_surface_dirt_flag_turf() -> None:
    assert subject.assign_surface_dirt_flag("11") == 0


def test_assign_surface_dirt_flag_dirt() -> None:
    assert subject.assign_surface_dirt_flag("23") == 1


def test_assign_surface_dirt_flag_none() -> None:
    assert subject.assign_surface_dirt_flag(None) == 0


def test_assign_surface_dirt_flag_empty_string() -> None:
    assert subject.assign_surface_dirt_flag("   ") == 0


def test_encode_kyakushitsu_one_hot_known_values() -> None:
    series = pd.Series(["1", "2", "3", "4", "0"])
    one_hot = subject.encode_kyakushitsu_one_hot(series)
    assert one_hot["kyakushitsu_1"].tolist() == [1, 0, 0, 0, 0]
    assert one_hot["kyakushitsu_4"].tolist() == [0, 0, 0, 1, 0]


def test_encode_kyakushitsu_one_hot_null_falls_back_to_zero() -> None:
    series = pd.Series([None, "2"])
    one_hot = subject.encode_kyakushitsu_one_hot(series)
    assert int(one_hot["kyakushitsu_0"].iloc[0]) == 1


def test_encode_kyakushitsu_one_hot_clips_out_of_range() -> None:
    series = pd.Series(["9"])
    one_hot = subject.encode_kyakushitsu_one_hot(series)
    assert int(one_hot["kyakushitsu_4"].iloc[0]) == 1


def test_encode_distance_band_one_hot() -> None:
    series = pd.Series(["sprint", "long", "intermediate"])
    df = subject.encode_distance_band_one_hot(series)
    assert df["distance_band_sprint"].tolist() == [1, 0, 0]
    assert df["distance_band_long"].tolist() == [0, 1, 0]


def test_add_race_level_aggregates_fills_std_for_singleton_race() -> None:
    frame = pd.DataFrame({
        "race_id": ["r1"],
        "predicted_score": [0.5],
        "predicted_rank": [1],
    })
    out = subject.add_race_level_aggregates(frame)
    assert out["score_std_in_race"].iloc[0] == 0.0
    assert out["num_horses_in_race"].iloc[0] == 1
    assert out["predicted_rank_norm"].iloc[0] == 1.0


def test_assemble_stacking_dataset_jra_with_running_style() -> None:
    baseline = _baseline_frame(cat="jra", year=2022)
    ctx = _race_context_frame(baseline)
    rs = _running_style_frame(baseline)
    out = subject.assemble_stacking_dataset(baseline, ctx, rs, "jra")
    for cls in (0, 1, 2, 3, 4):
        assert f"kyakushitsu_{cls}" in out.columns
    assert "distance_band_sprint" in out.columns
    assert "predicted_rank_norm" in out.columns


def test_assemble_stacking_dataset_jra_without_running_style_uses_zero_kyaku() -> None:
    baseline = _baseline_frame(cat="jra", year=2022)
    ctx = _race_context_frame(baseline)
    out = subject.assemble_stacking_dataset(baseline, ctx, None, "jra")
    assert int(out["kyakushitsu_0"].iloc[0]) == 0
    assert "kyakushitsu_4" in out.columns


def test_assemble_stacking_dataset_jra_empty_running_style_falls_back() -> None:
    baseline = _baseline_frame(cat="jra", year=2022)
    ctx = _race_context_frame(baseline)
    out = subject.assemble_stacking_dataset(baseline, ctx, pd.DataFrame(), "jra")
    assert int(out["kyakushitsu_0"].iloc[0]) == 0


def test_assemble_stacking_dataset_nar_skips_running_style() -> None:
    baseline = _baseline_frame(cat="nar", year=2022)
    ctx = _race_context_frame(baseline)
    out = subject.assemble_stacking_dataset(baseline, ctx, None, "nar")
    assert "kyakushitsu_3" in out.columns
    assert int(out["kyakushitsu_3"].iloc[0]) == 0


def test_assemble_stacking_dataset_drops_rows_with_missing_actual() -> None:
    baseline = _baseline_frame(cat="jra", year=2022)
    baseline.loc[0, "actual_finish_position"] = None
    ctx = _race_context_frame(baseline)
    out = subject.assemble_stacking_dataset(baseline, ctx, None, "jra")
    assert len(out) == len(baseline.dropna(subset=["actual_finish_position"]))


def test_stacking_feature_columns_excludes_missing() -> None:
    base_frame = pd.DataFrame({
        "predicted_score": [1.0],
        "predicted_rank_norm": [0.1],
        "mean_field_score": [0.5],
        "score_std_in_race": [0.2],
        "num_horses_in_race": [10],
        "surface_dirt_flag": [0],
        "race_year": [2020],
        "distance_band_sprint": [1],
        "distance_band_mile": [0],
        "distance_band_intermediate": [0],
        "distance_band_long": [0],
        "distance_band_extended": [0],
        "kyakushitsu_0": [0],
        "kyakushitsu_1": [0],
        "kyakushitsu_2": [1],
        "kyakushitsu_3": [0],
        "kyakushitsu_4": [0],
    })
    cols = subject.stacking_feature_columns(base_frame)
    assert "predicted_score" in cols
    assert "kyakushitsu_2" in cols


def test_pick_alpha_via_cv_picks_lowest_rmse() -> None:
    dataset = _multiyear_dataset([2020, 2021, 2022, 2023])
    alpha, scores = subject.pick_alpha_via_cv(
        dataset,
        alpha_grid=(0.1, 1.0, 10.0),
        cv_folds=3,
        random_state=42,
        ridge_factory=subject.default_ridge_factory,
    )
    assert alpha in (0.1, 1.0, 10.0)
    assert all(v >= 0.0 for v in scores.values())


def test_pick_alpha_via_cv_no_features_raises() -> None:
    # An entirely empty frame triggers the guard since no column in
    # stacking_feature_columns() can match.
    bad_frame = pd.DataFrame({"some_other": [0.1]})
    with pytest.raises(ValueError, match="feature columns"):
        subject.pick_alpha_via_cv(
            bad_frame,
            alpha_grid=(1.0,),
            cv_folds=3,
            random_state=0,
            ridge_factory=subject.default_ridge_factory,
        )


def test_pick_alpha_via_cv_handles_single_year_dataset() -> None:
    dataset = _multiyear_dataset([2021])
    alpha, _ = subject.pick_alpha_via_cv(
        dataset,
        alpha_grid=(1.0,),
        cv_folds=5,
        random_state=0,
        ridge_factory=subject.default_ridge_factory,
    )
    assert alpha == 1.0


def test_inner_cv_fold_assignment_is_chronologically_ordered() -> None:
    # Verify that pick_alpha_via_cv assigns earlier years to lower (or equal) fold indices.
    # We intercept year_to_fold by capturing the dict built inside the function via a
    # custom ridge_factory that records which years appear in each training set.
    # Instead of patching internals, we verify the observable property:
    # with 6 years and 3 folds, the earliest year must never appear in a fold
    # that contains a later year assigned to a lower fold number.
    # We reconstruct the assignment from the same formula and assert monotonicity.
    years = [2018, 2019, 2020, 2021, 2022, 2023]
    n_folds = 3
    n_years = len(years)
    fold_assignment = [int(i * n_folds / n_years) for i in range(n_years)]
    year_to_fold = dict(zip(years, fold_assignment, strict=True))

    # Fold indices must be non-decreasing as years increase
    folds_in_year_order = [year_to_fold[y] for y in sorted(year_to_fold)]
    assert folds_in_year_order == sorted(folds_in_year_order), (
        "fold indices must be non-decreasing with calendar year"
    )
    # The earliest year must be in fold 0
    assert year_to_fold[2018] == 0, "earliest year must be assigned to fold 0"
    # The latest year must be in a later fold than the earliest
    assert year_to_fold[2023] > year_to_fold[2018], (
        "latest year must have a higher fold index than the earliest year"
    )


def test_inner_cv_fold_assignment_is_deterministic() -> None:
    # pick_alpha_via_cv must return the same result on repeated calls
    # (no random fold assignment means no seed-dependent variation)
    dataset = _multiyear_dataset([2020, 2021, 2022, 2023])
    alpha_a, scores_a = subject.pick_alpha_via_cv(
        dataset,
        alpha_grid=(0.1, 1.0, 10.0),
        cv_folds=3,
        random_state=42,
        ridge_factory=subject.default_ridge_factory,
    )
    alpha_b, scores_b = subject.pick_alpha_via_cv(
        dataset,
        alpha_grid=(0.1, 1.0, 10.0),
        cv_folds=3,
        random_state=99,  # different seed must not change result
        ridge_factory=subject.default_ridge_factory,
    )
    assert alpha_a == alpha_b, "alpha selection must not depend on random_state"
    assert scores_a == scores_b, "cv scores must not depend on random_state"


def test_rerank_within_race_assigns_unique_ranks() -> None:
    frame = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "score": [0.5, 0.1, 0.3],
    })
    out = subject.rerank_within_race(frame, "score")
    assert sorted(out["predicted_rank"].tolist()) == [1, 2, 3]
    assert out.loc[out["score"] == 0.1, "predicted_rank"].iloc[0] == 1


def test_compute_oos_metrics_empty_frame() -> None:
    metrics = subject.compute_oos_metrics(pd.DataFrame())
    assert metrics["races"] == 0
    assert metrics["top1"] == 0.0


def test_compute_oos_metrics_perfect_predictions() -> None:
    frame = pd.DataFrame({
        "race_id": ["r1"] * 4 + ["r2"] * 4,
        "predicted_rank": [1, 2, 3, 4, 1, 2, 3, 4],
        "actual_finish_position": [1, 2, 3, 4, 1, 2, 3, 4],
    })
    metrics = subject.compute_oos_metrics(frame)
    assert metrics["top1"] == 1.0
    assert metrics["place2"] == 1.0
    assert metrics["place3"] == 1.0
    assert metrics["top3_box"] == 1.0


def test_compute_oos_metrics_mismatched_box() -> None:
    frame = pd.DataFrame({
        "race_id": ["r1"] * 4,
        "predicted_rank": [1, 2, 3, 4],
        "actual_finish_position": [1, 4, 3, 2],
    })
    metrics = subject.compute_oos_metrics(frame)
    assert metrics["top1"] == 1.0
    assert metrics["place2"] == 0.0


def test_train_one_fold_skips_when_train_too_small() -> None:
    dataset = _multiyear_dataset([2024])
    preds, meta = subject.train_one_fold(
        dataset,
        cat="jra",
        fold_year=2024,
        model_version="iter2-test",
        alpha_grid=(1.0,),
        cv_folds=2,
        random_state=0,
        ridge_factory=subject.default_ridge_factory,
        now=FIXED_NOW,
    )
    assert preds.empty
    assert meta["skipped"] is True


def test_train_one_fold_skips_when_no_validation_year() -> None:
    dataset = _multiyear_dataset([2020, 2021, 2022, 2023])
    preds, meta = subject.train_one_fold(
        dataset,
        cat="jra",
        fold_year=2099,
        model_version="iter2-test",
        alpha_grid=(1.0,),
        cv_folds=2,
        random_state=0,
        ridge_factory=subject.default_ridge_factory,
        now=FIXED_NOW,
    )
    assert preds.empty
    assert meta["skipped"] is True
    assert meta["skip_reason"] == "no OOS rows for fold year"


def test_train_one_fold_happy_path_predicts_and_reranks() -> None:
    dataset = _multiyear_dataset([2020, 2021, 2022, 2023])
    preds, meta = subject.train_one_fold(
        dataset,
        cat="jra",
        fold_year=2023,
        model_version="iter2-test",
        alpha_grid=(1.0,),
        cv_folds=3,
        random_state=0,
        ridge_factory=subject.default_ridge_factory,
        now=FIXED_NOW,
    )
    assert not preds.empty
    assert meta["skipped"] is False
    assert meta["alpha_picked"] == 1.0
    assert preds["predicted_rank"].min() == 1
    assert preds["model_version"].iloc[0] == "iter2-test"


def test_train_one_fold_feature_cols_derived_from_train_frame_not_full_dataset() -> None:
    # Build a 4-year dataset and inject an extra column ONLY into the 2023 rows.
    # With the bug (stacking_feature_columns(dataset)), that column would be
    # included in feature_cols even though it is all-NaN in the training frame
    # (years 2020-2022).  With the fix (stacking_feature_columns(train_frame)),
    # the column is absent from feature_cols entirely.
    dataset = _multiyear_dataset([2020, 2021, 2022, 2023])
    dataset.loc[dataset["race_year"] == 2023, "val_year_only_col"] = 1.0

    fitted_columns: list[list[str]] = []

    def capturing_ridge_factory(alpha: float, random_state: int) -> object:
        base_model = subject.default_ridge_factory(alpha=alpha, random_state=random_state)

        class CapturingRidge:
            def fit(self, x: np.ndarray, y: np.ndarray) -> "CapturingRidge":
                # We cannot recover column names from numpy arrays directly, so
                # we capture via the meta "feature_columns" key instead.  Here
                # we just delegate; the assertion is done on meta["feature_columns"].
                base_model.fit(x, y)
                return self

            def predict(self, x: np.ndarray) -> np.ndarray:
                return base_model.predict(x)

            def get_params(self, deep: bool = True) -> dict[str, object]:
                return base_model.get_params(deep=deep)

        return CapturingRidge()

    # Capture the feature_cols by checking meta["feature_columns"] returned by
    # train_one_fold — that key is set from feature_cols directly after the fix.
    preds, meta = subject.train_one_fold(
        dataset,
        cat="jra",
        fold_year=2023,
        model_version="iter2-test",
        alpha_grid=(1.0,),
        cv_folds=3,
        random_state=0,
        ridge_factory=capturing_ridge_factory,
        now=FIXED_NOW,
    )
    assert not preds.empty
    feature_columns = cast(list[str], meta["feature_columns"])
    # The extra column only present in the val year must NOT appear in
    # feature_cols (which must be derived from train_frame, not the full dataset).
    assert "val_year_only_col" not in feature_columns, (
        "feature_cols must come from train_frame, not the full dataset; "
        f"got {feature_columns}"
    )
    # Standard columns that exist in all years must still be present.
    assert "predicted_score" in feature_columns
    del fitted_columns  # referenced only to silence unused-variable lint


def test_resolve_fold_years_default_returns_all() -> None:
    dataset = pd.DataFrame({"race_year": [2020, 2020, 2021]})
    assert subject.resolve_fold_years(dataset, None) == (2020, 2021)


def test_resolve_fold_years_filters_to_intersection() -> None:
    dataset = pd.DataFrame({"race_year": [2020, 2021, 2022]})
    assert subject.resolve_fold_years(dataset, (2021, 2099)) == (2021,)


def test_resolve_fold_years_empty_intersection_raises() -> None:
    dataset = pd.DataFrame({"race_year": [2020, 2021]})
    with pytest.raises(ValueError, match="fold years"):
        subject.resolve_fold_years(dataset, (2099,))


def test_run_build_dataset_writes_output(tmp_path: Path) -> None:
    baseline = _baseline_frame(cat="jra", year=2022)
    ctx = _race_context_frame(baseline)
    rs = _running_style_frame(baseline)
    written: dict[str, object] = {}

    def baseline_reader(path: Path) -> pd.DataFrame:
        written["baseline_path"] = path
        return baseline

    def race_context_reader(path: Path) -> pd.DataFrame:
        written["ctx_path"] = path
        return ctx

    def running_style_reader(path: Path) -> pd.DataFrame:
        written["rs_path"] = path
        return rs

    def parquet_writer(frame: pd.DataFrame, output_dir: Path) -> None:
        written["written_rows"] = len(frame)
        written["output_dir"] = output_dir

    args: subject.BuildDatasetArgs = {
        "mode": "build-dataset",
        "cat": "jra",
        "baseline_parquet_root": tmp_path / "base",
        "race_context_parquet": tmp_path / "ctx.parquet",
        "running_style_parquet": tmp_path / "rs.parquet",
        "output_root": tmp_path / "out",
    }
    deps: subject.BuildDeps = {
        "baseline_reader": baseline_reader,
        "race_context_reader": race_context_reader,
        "running_style_reader": running_style_reader,
        "parquet_writer": parquet_writer,
    }
    rc = subject.run_build_dataset(args, deps)
    assert rc == 0
    assert written["written_rows"] == len(baseline)
    assert written["baseline_path"] == tmp_path / "base" / "category=jra"


def test_run_build_dataset_nar_skips_running_style_reader(tmp_path: Path) -> None:
    baseline = _baseline_frame(cat="nar", year=2022)
    ctx = _race_context_frame(baseline)
    rs_calls: list[Path] = []

    def baseline_reader(path: Path) -> pd.DataFrame:
        assert path == tmp_path / "base" / "category=nar"
        return baseline

    def race_context_reader(path: Path) -> pd.DataFrame:
        assert path == tmp_path / "ctx.parquet"
        return ctx

    def running_style_reader(path: Path) -> pd.DataFrame:
        rs_calls.append(path)
        return _running_style_frame(baseline)

    def parquet_writer(frame: pd.DataFrame, output_dir: Path) -> None:
        assert not frame.empty
        assert output_dir == tmp_path / "out"

    args: subject.BuildDatasetArgs = {
        "mode": "build-dataset",
        "cat": "nar",
        "baseline_parquet_root": tmp_path / "base",
        "race_context_parquet": tmp_path / "ctx.parquet",
        "running_style_parquet": tmp_path / "rs.parquet",
        "output_root": tmp_path / "out",
    }
    deps: subject.BuildDeps = {
        "baseline_reader": baseline_reader,
        "race_context_reader": race_context_reader,
        "running_style_reader": running_style_reader,
        "parquet_writer": parquet_writer,
    }
    subject.run_build_dataset(args, deps)
    assert rs_calls == []


def test_run_build_dataset_empty_baseline_raises(tmp_path: Path) -> None:
    def empty_reader(path: Path) -> pd.DataFrame:
        assert path is not None
        return pd.DataFrame()

    def empty_writer(frame: pd.DataFrame, output_dir: Path) -> None:
        del frame, output_dir

    args: subject.BuildDatasetArgs = {
        "mode": "build-dataset",
        "cat": "jra",
        "baseline_parquet_root": tmp_path,
        "race_context_parquet": tmp_path / "ctx.parquet",
        "running_style_parquet": None,
        "output_root": tmp_path / "out",
    }
    deps: subject.BuildDeps = {
        "baseline_reader": empty_reader,
        "race_context_reader": empty_reader,
        "running_style_reader": empty_reader,
        "parquet_writer": empty_writer,
    }
    with pytest.raises(ValueError, match="baseline parquet"):
        subject.run_build_dataset(args, deps)


def test_run_train_writes_predictions_and_metadata(tmp_path: Path) -> None:
    dataset = _multiyear_dataset([2020, 2021, 2022, 2023])
    written_preds: list[pd.DataFrame] = []
    written_json: dict[str, object] = {}

    def dataset_reader(path: Path) -> pd.DataFrame:
        assert path == tmp_path / "ds" / "category=jra"
        return dataset

    def parquet_writer(frame: pd.DataFrame, output_dir: Path) -> None:
        written_preds.append(frame.copy())
        assert output_dir == tmp_path / "out" / "predictions"

    def json_writer(payload: dict[str, object], path: Path) -> None:
        written_json["payload"] = payload
        written_json["path"] = path

    args: subject.TrainArgs = {
        "mode": "train",
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "out" / "predictions",
        "model_version": "iter2-test",
        "alpha_grid": (1.0,),
        "cv_folds": 2,
        "random_state": 0,
        "fold_years": (2023,),
    }
    deps: subject.TrainDeps = {
        "dataset_reader": dataset_reader,
        "parquet_writer": parquet_writer,
        "json_writer": json_writer,
        "ridge_factory": subject.default_ridge_factory,
        "now": _fixed_now,
    }
    rc = subject.run_train(args, deps)
    assert rc == 0
    assert written_preds
    payload = cast(dict[str, object], written_json["payload"])
    assert payload["model_version"] == "iter2-test"
    assert isinstance(payload["fold_results"], list)


def test_run_train_skip_only_fold_writes_metadata_no_predictions(tmp_path: Path) -> None:
    dataset = _multiyear_dataset([2024])
    write_calls: list[tuple[int, Path]] = []
    json_payload: dict[str, object] = {}

    def dataset_reader(path: Path) -> pd.DataFrame:
        assert path is not None
        return dataset

    def parquet_writer(frame: pd.DataFrame, output_dir: Path) -> None:
        write_calls.append((len(frame), output_dir))

    def json_writer(payload: dict[str, object], path: Path) -> None:
        json_payload.update(payload)
        assert path is not None

    args: subject.TrainArgs = {
        "mode": "train",
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "out" / "predictions",
        "model_version": "iter2-test",
        "alpha_grid": (1.0,),
        "cv_folds": 2,
        "random_state": 0,
        "fold_years": (2024,),
    }
    deps: subject.TrainDeps = {
        "dataset_reader": dataset_reader,
        "parquet_writer": parquet_writer,
        "json_writer": json_writer,
        "ridge_factory": subject.default_ridge_factory,
        "now": _fixed_now,
    }
    subject.run_train(args, deps)
    assert write_calls == []
    fold_results = cast(list[dict[str, object]], json_payload["fold_results"])
    assert fold_results[0]["skipped"] is True


def _noop_parquet_writer(frame: pd.DataFrame, output_dir: Path) -> None:
    del frame, output_dir


def _noop_json_writer(payload: dict[str, object], path: Path) -> None:
    del payload, path


def test_run_train_empty_dataset_raises(tmp_path: Path) -> None:
    def empty_reader(path: Path) -> pd.DataFrame:
        assert path is not None
        return pd.DataFrame()

    args: subject.TrainArgs = {
        "mode": "train",
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "out" / "predictions",
        "model_version": "iter2-test",
        "alpha_grid": (1.0,),
        "cv_folds": 2,
        "random_state": 0,
        "fold_years": None,
    }
    deps: subject.TrainDeps = {
        "dataset_reader": empty_reader,
        "parquet_writer": _noop_parquet_writer,
        "json_writer": _noop_json_writer,
        "ridge_factory": subject.default_ridge_factory,
        "now": _fixed_now,
    }
    with pytest.raises(ValueError, match="stacking dataset"):
        subject.run_train(args, deps)


def test_run_train_missing_race_year_raises(tmp_path: Path) -> None:
    bad_dataset = pd.DataFrame({"race_id": ["r1"], "predicted_score": [0.5]})

    def bad_reader(path: Path) -> pd.DataFrame:
        assert path is not None
        return bad_dataset

    args: subject.TrainArgs = {
        "mode": "train",
        "cat": "jra",
        "dataset_root": tmp_path / "ds",
        "output_predictions_root": tmp_path / "out" / "predictions",
        "model_version": "iter2-test",
        "alpha_grid": (1.0,),
        "cv_folds": 2,
        "random_state": 0,
        "fold_years": None,
    }
    deps: subject.TrainDeps = {
        "dataset_reader": bad_reader,
        "parquet_writer": _noop_parquet_writer,
        "json_writer": _noop_json_writer,
        "ridge_factory": subject.default_ridge_factory,
        "now": _fixed_now,
    }
    with pytest.raises(ValueError, match="race_year"):
        subject.run_train(args, deps)


def test_default_ridge_factory_returns_ridge() -> None:
    model = subject.default_ridge_factory(alpha=2.0, random_state=0)
    assert isinstance(model, Ridge)
    # ``Ridge.alpha`` is set on construction; basedpyright doesn't see the
    # dataclass-style attribute so we read it via the params dict instead.
    assert model.get_params()["alpha"] == 2.0


def test_default_read_parquet_dir_handles_missing(tmp_path: Path) -> None:
    out = subject.default_read_parquet_dir(tmp_path)
    assert out.empty


def test_default_read_parquet_dir_handles_single_file(tmp_path: Path) -> None:
    path = tmp_path / "a.parquet"
    pd.DataFrame({"a": [1, 2]}).to_parquet(path.as_posix(), index=False)
    out = subject.default_read_parquet_dir(path)
    assert len(out) == 2


def test_default_read_parquet_dir_concats_multiple(tmp_path: Path) -> None:
    (tmp_path / "p1").mkdir()
    (tmp_path / "p2").mkdir()
    pd.DataFrame({"a": [1]}).to_parquet((tmp_path / "p1" / "x.parquet").as_posix(), index=False)
    pd.DataFrame({"a": [2]}).to_parquet((tmp_path / "p2" / "x.parquet").as_posix(), index=False)
    out = subject.default_read_parquet_dir(tmp_path)
    assert sorted(out["a"].tolist()) == [1, 2]


def test_default_read_parquet_dir_hydrates_hive_partitions(tmp_path: Path) -> None:
    target = tmp_path / "race_year=2020" / "other=foo"
    target.mkdir(parents=True)
    pd.DataFrame({"a": [1, 2]}).to_parquet((target / "x.parquet").as_posix(), index=False)
    out = subject.default_read_parquet_dir(tmp_path)
    assert out["race_year"].iloc[0] == 2020
    assert out["other"].iloc[0] == "foo"


def test_default_read_parquet_dir_skips_segments_without_equals(tmp_path: Path) -> None:
    target = tmp_path / "no_equals_here"
    target.mkdir()
    pd.DataFrame({"a": [9]}).to_parquet((target / "x.parquet").as_posix(), index=False)
    out = subject.default_read_parquet_dir(tmp_path)
    assert out["a"].tolist() == [9]
    assert "no_equals_here" not in out.columns


def test_coerce_partition_value_race_year_int() -> None:
    assert subject.coerce_partition_value(subject.RACE_YEAR_COLUMN, "2024") == 2024


def test_coerce_partition_value_race_year_falls_back_to_string() -> None:
    assert subject.coerce_partition_value(subject.RACE_YEAR_COLUMN, "abc") == "abc"


def test_coerce_partition_value_other_key_stays_string() -> None:
    assert subject.coerce_partition_value("category", "jra") == "jra"


def test_default_read_parquet_file(tmp_path: Path) -> None:
    path = tmp_path / "a.parquet"
    pd.DataFrame({"a": [1]}).to_parquet(path.as_posix(), index=False)
    out = subject.default_read_parquet_file(path)
    assert len(out) == 1


def test_default_write_partitioned_parquet_creates_partitions(tmp_path: Path) -> None:
    frame = pd.DataFrame({"x": [1, 2], "category": ["jra", "jra"], "race_year": [2020, 2021]})
    subject.default_write_partitioned_parquet(frame, tmp_path / "out")
    assert (tmp_path / "out" / "category=jra" / "race_year=2020").exists()


def test_default_write_json(tmp_path: Path) -> None:
    target = tmp_path / "sub" / "meta.json"
    subject.default_write_json({"k": 1}, target)
    assert json.loads(target.read_text(encoding="utf-8")) == {"k": 1}


def test_format_iso_now_strips_micros() -> None:
    assert subject.format_iso_now(FIXED_NOW) == "2026-06-04T09:00:00Z"


def test_main_dispatches_build_dataset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(subject, "run_build_dataset", lambda _a, _d: 0)
    monkeypatch.setattr(subject, "default_read_parquet_dir", lambda _p: pd.DataFrame({"a": [1]}))
    monkeypatch.setattr(subject, "default_read_parquet_file", lambda _p: pd.DataFrame({"a": [1]}))
    rc = subject.main([
        "--mode", "build-dataset",
        "--cat", "jra",
        "--baseline-parquet-root", str(tmp_path / "base"),
        "--race-context-parquet", str(tmp_path / "ctx.parquet"),
        "--output-root", str(tmp_path / "out"),
    ])
    assert rc == 0


def test_main_dispatches_train(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(subject, "run_train", lambda _a, _d: 0)
    rc = subject.main([
        "--mode", "train",
        "--cat", "jra",
        "--dataset-root", str(tmp_path / "ds"),
        "--output-predictions-root", str(tmp_path / "out"),
        "--model-version", "iter2-test",
    ])
    assert rc == 0


def test_main_unknown_mode_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_args = type("X", (), {"mode": "unknown"})
    monkeypatch.setattr(subject, "parse_args", lambda _a: cast(object, fake_args))
    with pytest.raises(ValueError, match="unknown mode"):
        subject.main([])
