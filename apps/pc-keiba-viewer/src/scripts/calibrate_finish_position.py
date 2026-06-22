"""Iter 1 of the v8 iterative loop: L2 isotonic calibration per (cat x grade bucket).

Two CLI modes via ``--mode`` flag:

* ``--mode fit``     -> read OOS predictions parquet, fit one
  ``sklearn.isotonic.IsotonicRegression`` per (cat x grade bucket) for both
  ``top1`` (win) and ``top3_box`` (place3) targets, and write the resulting
  knot lists to JSON under
  ``apps/pc-keiba-viewer/finish-position/<cat>/v8-iter1-calibration/bucket_<key>/``.
* ``--mode apply``   -> read raw predictions parquet, look up the bucket's
  calibration JSON, transform ``predicted_top1_prob`` and
  ``predicted_top3_prob`` via ``numpy.interp``, re-rank per race, and write a
  new parquet partitioned by ``category`` / ``race_year``.

Buckets are formed from a categorical column on the predictions parquet
(``--bucket-dim {kyoso_joken,grade}``). A bucket with fewer than
``--min-bucket-samples`` races falls back to the cat-global curve so the
calibrator never overfits on tiny grade slices.

All file I/O is injected so unit tests can run fully mocked. Run with::

    uv run python src/scripts/calibrate_finish_position.py --mode fit ...
    uv run python src/scripts/calibrate_finish_position.py --mode apply ...
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, TypedDict, cast

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

MODE_FIT: str = "fit"
MODE_APPLY: str = "apply"
SUPPORTED_MODES: tuple[str, str] = (MODE_FIT, MODE_APPLY)

CATEGORY_JRA: str = "jra"
CATEGORY_NAR: str = "nar"
CATEGORY_BANEI: str = "ban-ei"
SUPPORTED_CATEGORIES: tuple[str, str, str] = (CATEGORY_JRA, CATEGORY_NAR, CATEGORY_BANEI)

BUCKET_DIM_KYOSO_JOKEN: str = "kyoso_joken"
BUCKET_DIM_GRADE: str = "grade"
SUPPORTED_BUCKET_DIMS: tuple[str, str] = (BUCKET_DIM_KYOSO_JOKEN, BUCKET_DIM_GRADE)

BUCKET_DIM_COLUMN: dict[str, str] = {
    BUCKET_DIM_KYOSO_JOKEN: "kyoso_joken_code",
    BUCKET_DIM_GRADE: "grade_code",
}

DEFAULT_MIN_BUCKET_SAMPLES: int = 500

CAT_GLOBAL_BUCKET_KEY: str = "_cat_global"

CALIBRATION_SCHEMA_VERSION: int = 1

ISO_TOP1_FILENAME: str = "iso.json"
ISO_TOP3_FILENAME: str = "iso_top3.json"

CALIBRATION_SOURCE_BUCKET: str = "bucket"
CALIBRATION_SOURCE_CAT_GLOBAL: str = "cat-global"
CALIBRATION_SOURCE_UNCALIBRATED: str = "uncalibrated"

RACE_ID_COLUMN: str = "race_id"
PREDICTED_TOP1_PROB_COLUMN: str = "predicted_top1_prob"
PREDICTED_TOP3_PROB_COLUMN: str = "predicted_top3_prob"
PREDICTED_RANK_COLUMN: str = "predicted_rank"
PREDICTED_SCORE_COLUMN: str = "predicted_score"
ACTUAL_FINISH_POSITION_COLUMN: str = "actual_finish_position"
CALIBRATION_SOURCE_COLUMN: str = "calibration_source"
PREDICTED_TOP1_PROB_CALIBRATED_COLUMN: str = "predicted_top1_prob_calibrated"
PREDICTED_TOP3_PROB_CALIBRATED_COLUMN: str = "predicted_top3_prob_calibrated"

TOP3_FINISH_THRESHOLD: int = 3


class FitArguments(TypedDict):
    mode: str
    cat: str
    predictions_root: Path
    output_dir: Path
    min_bucket_samples: int
    bucket_dim: str


class ApplyArguments(TypedDict):
    mode: str
    cat: str
    input_predictions_root: Path
    calibration_dir: Path
    output_predictions_root: Path


class CalibrationCurve(TypedDict):
    schema_version: int
    cat: str
    bucket_key: str
    target: str
    n_samples: int
    iso_x: list[float]
    iso_y: list[float]
    fit_at: str
    brier_score_before: float
    brier_score_after: float


class CurvePair(TypedDict):
    top1: CalibrationCurve
    top3: CalibrationCurve


class ParquetDirReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class ParquetWriterLike(Protocol):
    def __call__(self, frame: pd.DataFrame, output_dir: Path) -> None: ...


class JsonReaderLike(Protocol):
    def __call__(self, path: Path) -> CalibrationCurve: ...


class JsonWriterLike(Protocol):
    def __call__(self, payload: CalibrationCurve, path: Path) -> None: ...


class PathExistsLike(Protocol):
    def __call__(self, path: Path) -> bool: ...


class NowFactoryLike(Protocol):
    def __call__(self) -> datetime: ...


class FitDeps(TypedDict):
    parquet_reader: ParquetDirReaderLike
    json_writer: JsonWriterLike
    now: NowFactoryLike


class ApplyDeps(TypedDict):
    parquet_reader: ParquetDirReaderLike
    parquet_writer: ParquetWriterLike
    json_reader: JsonReaderLike
    path_exists: PathExistsLike


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="calibrate_finish_position")
    parser.add_argument("--mode", choices=list(SUPPORTED_MODES), required=True)
    parser.add_argument("--cat", choices=list(SUPPORTED_CATEGORIES), required=True)
    parser.add_argument("--predictions-root", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--min-bucket-samples", type=int, default=DEFAULT_MIN_BUCKET_SAMPLES)
    parser.add_argument(
        "--bucket-dim",
        choices=list(SUPPORTED_BUCKET_DIMS),
        default=BUCKET_DIM_KYOSO_JOKEN,
    )
    parser.add_argument("--input-predictions-root", type=Path, default=None)
    parser.add_argument("--calibration-dir", type=Path, default=None)
    parser.add_argument("--output-predictions-root", type=Path, default=None)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def normalize_fit_arguments(args: argparse.Namespace) -> FitArguments:
    if args.predictions_root is None:
        raise ValueError("--predictions-root is required for --mode fit")
    if args.output_dir is None:
        raise ValueError("--output-dir is required for --mode fit")
    return {
        "mode": MODE_FIT,
        "cat": cast(str, args.cat),
        "predictions_root": Path(args.predictions_root),
        "output_dir": Path(args.output_dir),
        "min_bucket_samples": int(args.min_bucket_samples),
        "bucket_dim": cast(str, args.bucket_dim),
    }


def normalize_apply_arguments(args: argparse.Namespace) -> ApplyArguments:
    if args.input_predictions_root is None:
        raise ValueError("--input-predictions-root is required for --mode apply")
    if args.calibration_dir is None:
        raise ValueError("--calibration-dir is required for --mode apply")
    if args.output_predictions_root is None:
        raise ValueError("--output-predictions-root is required for --mode apply")
    return {
        "mode": MODE_APPLY,
        "cat": cast(str, args.cat),
        "input_predictions_root": Path(args.input_predictions_root),
        "calibration_dir": Path(args.calibration_dir),
        "output_predictions_root": Path(args.output_predictions_root),
    }


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def format_fit_timestamp(now: datetime) -> str:
    return now.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_read_parquet_dir(path: Path) -> pd.DataFrame:
    """Read every parquet under ``path`` (recursive) into a single DataFrame.

    Both ``fit`` and ``apply`` modes consume Hive-partitioned parquet trees
    written by ``score_finish_position_walk_forward.py``, so we accept either a
    directory or a single file path and concatenate every parquet under it.
    """
    if path.is_file():
        return pd.read_parquet(path.as_posix())
    parts = sorted(path.rglob("*.parquet"))
    frames = [pd.read_parquet(p.as_posix()) for p in parts]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def default_write_parquet(frame: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(
        output_dir.as_posix(),
        partition_cols=["category", "race_year"],
        index=False,
    )


def default_read_calibration_json(path: Path) -> CalibrationCurve:
    return cast(CalibrationCurve, json.loads(path.read_text(encoding="utf-8")))


def default_write_calibration_json(payload: CalibrationCurve, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def default_path_exists(path: Path) -> bool:
    return path.exists()


def derive_top1_prob(frame: pd.DataFrame) -> pd.Series:
    """Return a ``predicted_top1_prob`` series, deriving it from per-race rank when absent.

    The walk-forward script leaves the column null for rank-only learners
    (CatBoost YetiRank / XGBoost rank:pairwise). We fall back to a rank-based
    proxy that decays linearly with the per-race rank so isotonic still has a
    monotonic signal to consume.
    """
    if PREDICTED_TOP1_PROB_COLUMN in frame.columns:
        column = frame[PREDICTED_TOP1_PROB_COLUMN]
        if not column.isna().all():
            return column.astype(float).fillna(0.0)
    return derive_prob_from_rank(frame, top_n=1)


def derive_top3_prob(frame: pd.DataFrame) -> pd.Series:
    if PREDICTED_TOP3_PROB_COLUMN in frame.columns:
        column = frame[PREDICTED_TOP3_PROB_COLUMN]
        if not column.isna().all():
            return column.astype(float).fillna(0.0)
    return derive_prob_from_rank(frame, top_n=TOP3_FINISH_THRESHOLD)


def derive_prob_from_rank(frame: pd.DataFrame, *, top_n: int) -> pd.Series:
    """Linear-decay proxy: 1.0 at rank 1, 0.0 below rank == race_size.

    ``top_n=1`` yields the win-proxy; ``top_n=3`` yields the box-proxy that
    sums to roughly 3.0 per race for top1 distributions.
    """
    if PREDICTED_RANK_COLUMN not in frame.columns:
        return pd.Series([0.0] * len(frame), index=frame.index)
    grouped = frame.groupby(RACE_ID_COLUMN)[PREDICTED_RANK_COLUMN]
    race_sizes = grouped.transform("count").astype(float)
    ranks = frame[PREDICTED_RANK_COLUMN].astype(float)
    if top_n == 1:
        proxy = (race_sizes - ranks + 1.0) / race_sizes
    else:
        proxy = ((race_sizes - ranks + 1.0) / race_sizes) * float(top_n)
    return proxy.clip(lower=0.0, upper=float(top_n)).fillna(0.0)


def win_indicator(frame: pd.DataFrame) -> pd.Series:
    actual = frame[ACTUAL_FINISH_POSITION_COLUMN].astype(float)
    return (actual == 1.0).astype(float)


def top3_indicator(frame: pd.DataFrame) -> pd.Series:
    actual = frame[ACTUAL_FINISH_POSITION_COLUMN].astype(float)
    return (actual <= float(TOP3_FINISH_THRESHOLD)).astype(float)


def normalize_bucket_key(value: object) -> str:
    """Stringify the raw bucket value into a filesystem-safe key.

    NaN / None values collapse to ``"_unknown"`` so the loader can still find
    a curve when the raw category column is missing for some rows.
    """
    if value is None:
        return "_unknown"
    if isinstance(value, float) and np.isnan(value):
        return "_unknown"
    text = str(value).strip()
    if not text:
        return "_unknown"
    return text


def fit_single_curve(
    probs: np.ndarray,
    targets: np.ndarray,
    *,
    cat: str,
    bucket_key: str,
    target_name: str,
    now: datetime,
) -> CalibrationCurve:
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(probs, targets)
    calibrated = iso.predict(probs)
    brier_before = float(np.mean((probs - targets) ** 2))
    brier_after = float(np.mean((calibrated - targets) ** 2))
    knots_x = iso.X_thresholds_
    knots_y = iso.y_thresholds_
    return {
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "cat": cat,
        "bucket_key": bucket_key,
        "target": target_name,
        "n_samples": int(probs.shape[0]),
        "iso_x": [float(value) for value in knots_x.tolist()],
        "iso_y": [float(value) for value in knots_y.tolist()],
        "fit_at": format_fit_timestamp(now),
        "brier_score_before": brier_before,
        "brier_score_after": brier_after,
    }


def fit_curves_for_frame(
    frame: pd.DataFrame,
    *,
    cat: str,
    bucket_key: str,
    now: datetime,
) -> CurvePair:
    # When using rank-based proxy (no direct probability column), exclude NaN-rank
    # rows before fitting so they don't create spurious calibration knots.
    if PREDICTED_RANK_COLUMN in frame.columns and (
        PREDICTED_TOP1_PROB_COLUMN not in frame.columns
        or frame[PREDICTED_TOP1_PROB_COLUMN].isna().all()
    ):
        frame = frame[frame[PREDICTED_RANK_COLUMN].notna()]
    if len(frame) == 0:
        raise ValueError(
            f"No valid rows for calibration fit (bucket_key={bucket_key!r}); "
            "all rows have NaN predicted_rank and no direct probability column"
        )
    top1_probs = derive_top1_prob(frame).to_numpy(dtype=float)
    top3_probs = derive_top3_prob(frame).to_numpy(dtype=float)
    top1_targets = win_indicator(frame).to_numpy(dtype=float)
    top3_targets = top3_indicator(frame).to_numpy(dtype=float)
    top1_curve = fit_single_curve(
        top1_probs,
        top1_targets,
        cat=cat,
        bucket_key=bucket_key,
        target_name="top1",
        now=now,
    )
    top3_curve = fit_single_curve(
        top3_probs,
        top3_targets,
        cat=cat,
        bucket_key=bucket_key,
        target_name="top3",
        now=now,
    )
    return {"top1": top1_curve, "top3": top3_curve}


def race_count_from_frame(frame: pd.DataFrame) -> int:
    if RACE_ID_COLUMN not in frame.columns:
        return 0
    return int(frame[RACE_ID_COLUMN].nunique())


def write_curve_pair(
    pair: CurvePair,
    *,
    output_dir: Path,
    bucket_key: str,
    json_writer: JsonWriterLike,
) -> None:
    bucket_dir = output_dir / f"bucket_{bucket_key}"
    json_writer(pair["top1"], bucket_dir / ISO_TOP1_FILENAME)
    json_writer(pair["top3"], bucket_dir / ISO_TOP3_FILENAME)


def fit_run(args: FitArguments, deps: FitDeps) -> dict[str, object]:
    frame = deps["parquet_reader"](args["predictions_root"])
    if frame.empty:
        sys.stderr.write(
            f"calibrate_finish_position: empty predictions parquet at {args['predictions_root']}; "
            "no calibration JSON written.\n",
        )
        return {"cat": args["cat"], "buckets_written": 0, "fallback_used": False, "race_count": 0}
    if ACTUAL_FINISH_POSITION_COLUMN not in frame.columns:
        raise ValueError(
            f"predictions parquet at {args['predictions_root']} is missing required column "
            f"{ACTUAL_FINISH_POSITION_COLUMN!r}; cannot fit isotonic calibration.",
        )
    column = BUCKET_DIM_COLUMN[args["bucket_dim"]]
    now = deps["now"]()
    buckets_written = 0
    fallback_used = False
    bucket_values: list[object] = (
        sorted(frame[column].dropna().unique().tolist()) if column in frame.columns else []
    )
    for raw_value in bucket_values:
        bucket_key = normalize_bucket_key(raw_value)
        bucket_frame = frame[frame[column] == raw_value]
        bucket_races = race_count_from_frame(bucket_frame)
        if bucket_races < args["min_bucket_samples"]:
            continue
        try:
            pair = fit_curves_for_frame(
                bucket_frame, cat=args["cat"], bucket_key=bucket_key, now=now,
            )
        except ValueError:
            continue
        write_curve_pair(
            pair,
            output_dir=args["output_dir"],
            bucket_key=bucket_key,
            json_writer=deps["json_writer"],
        )
        buckets_written += 1
    if buckets_written == 0:
        try:
            pair = fit_curves_for_frame(
                frame,
                cat=args["cat"],
                bucket_key=CAT_GLOBAL_BUCKET_KEY,
                now=now,
            )
        except ValueError:
            sys.stderr.write(
                "calibrate_finish_position: no valid rows for global fallback fit; "
                "no calibration JSON written.\n",
            )
            return {
                "cat": args["cat"],
                "buckets_written": 0,
                "fallback_used": False,
                "race_count": race_count_from_frame(frame),
            }
        write_curve_pair(
            pair,
            output_dir=args["output_dir"],
            bucket_key=CAT_GLOBAL_BUCKET_KEY,
            json_writer=deps["json_writer"],
        )
        fallback_used = True
        buckets_written = 1
    return {
        "cat": args["cat"],
        "buckets_written": buckets_written,
        "fallback_used": fallback_used,
        "race_count": race_count_from_frame(frame),
    }


def isotonic_transform(probs: pd.Series, curve: CalibrationCurve) -> pd.Series:
    schema_ver = cast("Mapping[str, object]", curve).get("schema_version")
    if schema_ver is not None and schema_ver != CALIBRATION_SCHEMA_VERSION:
        raise ValueError(
            f"calibration file schema_version={schema_ver!r} "
            f"!= expected {CALIBRATION_SCHEMA_VERSION}; re-run fit to regenerate"
        )
    xs = np.asarray(curve["iso_x"], dtype=float)
    ys = np.asarray(curve["iso_y"], dtype=float)
    if xs.size == 0:
        return probs.astype(float)
    raw = probs.astype(float).to_numpy()
    calibrated = np.interp(raw, xs, ys)
    return pd.Series(calibrated, index=probs.index)


def resolve_bucket_column(frame: pd.DataFrame) -> str | None:
    if BUCKET_DIM_COLUMN[BUCKET_DIM_KYOSO_JOKEN] in frame.columns:
        return BUCKET_DIM_COLUMN[BUCKET_DIM_KYOSO_JOKEN]
    if BUCKET_DIM_COLUMN[BUCKET_DIM_GRADE] in frame.columns:
        return BUCKET_DIM_COLUMN[BUCKET_DIM_GRADE]
    return None


def lookup_curve(
    *,
    calibration_dir: Path,
    bucket_key: str,
    filename: str,
    json_reader: JsonReaderLike,
    path_exists: PathExistsLike,
) -> tuple[CalibrationCurve | None, str]:
    bucket_path = calibration_dir / f"bucket_{bucket_key}" / filename
    if path_exists(bucket_path):
        return (json_reader(bucket_path), CALIBRATION_SOURCE_BUCKET)
    cat_global_path = calibration_dir / f"bucket_{CAT_GLOBAL_BUCKET_KEY}" / filename
    if path_exists(cat_global_path):
        return (json_reader(cat_global_path), CALIBRATION_SOURCE_CAT_GLOBAL)
    return (None, CALIBRATION_SOURCE_UNCALIBRATED)


def calibrated_series_for_target(
    frame: pd.DataFrame,
    *,
    raw_series: pd.Series,
    bucket_column: str | None,
    filename: str,
    deps: ApplyDeps,
    calibration_dir: Path,
) -> tuple[pd.Series, pd.Series]:
    if bucket_column is None:
        raw_bucket_keys = pd.Series(["_unknown"] * len(frame), index=frame.index)
    else:
        raw_bucket_keys = frame[bucket_column].map(normalize_bucket_key)
    calibrated_values = raw_series.clip(lower=0.0, upper=1.0).astype(float).copy()
    source_values = pd.Series([CALIBRATION_SOURCE_UNCALIBRATED] * len(frame), index=frame.index)
    unique_keys: list[str] = sorted(set(raw_bucket_keys.tolist()))
    for bucket_key in unique_keys:
        mask = raw_bucket_keys == bucket_key
        curve, source = lookup_curve(
            calibration_dir=calibration_dir,
            bucket_key=bucket_key,
            filename=filename,
            json_reader=deps["json_reader"],
            path_exists=deps["path_exists"],
        )
        source_values = source_values.where(~mask, source)
        if curve is None:
            continue
        transformed = isotonic_transform(raw_series[mask], curve)
        calibrated_values = calibrated_values.where(~mask, transformed)
    return calibrated_values, source_values


def re_rank_predictions(frame: pd.DataFrame, *, prob_column: str) -> pd.Series:
    return (
        frame.groupby(RACE_ID_COLUMN)[prob_column]
        .rank(method="first", ascending=False)
        .astype(int)
    )


def log_source_distribution(source_series: pd.Series, label: str) -> None:
    counts = source_series.value_counts(dropna=False)
    total = int(counts.sum())
    if total == 0:
        return
    bucket = int(counts.get(CALIBRATION_SOURCE_BUCKET, 0))
    cat_global = int(counts.get(CALIBRATION_SOURCE_CAT_GLOBAL, 0))
    uncalibrated = int(counts.get(CALIBRATION_SOURCE_UNCALIBRATED, 0))
    sys.stderr.write(
        f"calibrate_finish_position: calibration_source distribution [{label}]"
        f" bucket={bucket}/{total} cat-global={cat_global}/{total}"
        f" uncalibrated={uncalibrated}/{total}\n",
    )


def apply_run(args: ApplyArguments, deps: ApplyDeps) -> dict[str, object]:
    frame = deps["parquet_reader"](args["input_predictions_root"])
    if frame.empty:
        sys.stderr.write(
            "calibrate_finish_position: empty input predictions; no output parquet written.\n",
        )
        return {"cat": args["cat"], "rows_written": 0}
    bucket_column = resolve_bucket_column(frame)
    raw_top1 = derive_top1_prob(frame)
    raw_top3 = derive_top3_prob(frame)
    calibrated_top1, source_top1 = calibrated_series_for_target(
        frame,
        raw_series=raw_top1,
        bucket_column=bucket_column,
        filename=ISO_TOP1_FILENAME,
        deps=deps,
        calibration_dir=args["calibration_dir"],
    )
    calibrated_top3, source_top3 = calibrated_series_for_target(
        frame,
        raw_series=raw_top3,
        bucket_column=bucket_column,
        filename=ISO_TOP3_FILENAME,
        deps=deps,
        calibration_dir=args["calibration_dir"],
    )
    out = frame.copy()
    out[PREDICTED_TOP1_PROB_CALIBRATED_COLUMN] = calibrated_top1.astype(float)
    out[PREDICTED_TOP3_PROB_CALIBRATED_COLUMN] = calibrated_top3.astype(float)
    out[CALIBRATION_SOURCE_COLUMN] = source_top1.astype(str)
    out[PREDICTED_RANK_COLUMN] = re_rank_predictions(
        out, prob_column=PREDICTED_TOP1_PROB_CALIBRATED_COLUMN,
    )
    log_source_distribution(source_top1, label="top1")
    log_source_distribution(source_top3, label="top3")
    deps["parquet_writer"](out, args["output_predictions_root"])
    return {"cat": args["cat"], "rows_written": int(len(out))}


def build_default_fit_deps() -> FitDeps:
    return {
        "parquet_reader": default_read_parquet_dir,
        "json_writer": default_write_calibration_json,
        "now": now_utc,
    }


def build_default_apply_deps() -> ApplyDeps:
    return {
        "parquet_reader": default_read_parquet_dir,
        "parquet_writer": default_write_parquet,
        "json_reader": default_read_calibration_json,
        "path_exists": default_path_exists,
    }


def main(argv: list[str] | None = None) -> None:
    raw_args = parse_args(argv)
    if raw_args.mode == MODE_FIT:
        result = fit_run(normalize_fit_arguments(raw_args), build_default_fit_deps())
    else:
        result = apply_run(normalize_apply_arguments(raw_args), build_default_apply_deps())
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
