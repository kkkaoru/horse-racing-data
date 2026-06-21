"""Isotonic calibration for running-style v3 LightGBM model.

Provides load/apply utilities for OvR IsotonicRegression calibrators that are
exported as JSON piecewise-linear lookup tables.  The calibrators correct the
raw softmax probabilities emitted by the running-style model before downstream
use.

Ban-ei note: The running-style model is not applicable to Ban-ei races
(keibajo_code='83').  Ban-ei horses are characterised by futan_juryo (weight
load) rather than running style; this calibration module similarly does not
apply to Ban-ei predictions.

Usage:
    calibrators = load_calibrators("tmp/models/jra-running-style-lgbm-prod-v3/calibrators.json")
    probs_calibrated = apply_calibration(probs_raw, calibrators)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict, cast

import numpy as np

# Class order matches running_style_lightgbm.CLASS_LABELS and PROBABILITY_COLUMNS
CLASS_NAMES: tuple[str, str, str, str] = ("nige", "senkou", "sashi", "oikomi")
CALIBRATORS_FILENAME: str = "calibrators.json"
MODELS_DIR_NAME: str = "tmp/models"


class CalibrationTable(TypedDict):
    """Piecewise-linear calibration table for a single class (OvR)."""

    x: list[float]
    y: list[float]


class RunningStyleCalibrators(TypedDict):
    """Full calibrator set for one category (jra or nar)."""

    category: str
    fit_year: int
    classes: list[str]
    calibrators: dict[str, CalibrationTable]


def load_calibrators(calibrators_path: str) -> RunningStyleCalibrators:
    """Load a RunningStyleCalibrators payload from a JSON file.

    Raises FileNotFoundError when the file does not exist.
    Raises ValueError when the JSON structure is invalid.
    """
    path = Path(calibrators_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Calibrators JSON not found at {calibrators_path}. "
            "Run validate_calibration_robustness.py to generate it."
        )
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    validate_calibrators_payload(raw, calibrators_path)
    assert isinstance(raw, dict)
    raw_dict = cast(dict[str, object], raw)
    calibrators_obj = raw_dict["calibrators"]
    assert isinstance(calibrators_obj, dict)
    calibrators_dict = cast(dict[str, object], calibrators_obj)
    return RunningStyleCalibrators(
        category=str(raw_dict["category"]),
        fit_year=int(cast(int, raw_dict["fit_year"])),
        classes=[str(c) for c in cast(list[object], raw_dict["classes"])],
        calibrators={
            cls_name: _build_calibration_table(table, cls_name, calibrators_path)
            for cls_name, table in calibrators_dict.items()
        },
    )


def _build_calibration_table(table: object, cls_name: str, source: str) -> CalibrationTable:
    assert isinstance(table, dict), f"table for {cls_name} must be dict in {source}"
    table_dict = cast(dict[str, object], table)
    x_val = table_dict["x"]
    y_val = table_dict["y"]
    assert isinstance(x_val, list), f"x for {cls_name} must be list in {source}"
    assert isinstance(y_val, list), f"y for {cls_name} must be list in {source}"
    return CalibrationTable(
        x=[float(cast(float, v)) for v in x_val],
        y=[float(cast(float, v)) for v in y_val],
    )


def validate_calibrators_payload(raw: object, source: str) -> None:
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict at top level in {source}")
    raw_dict = cast(dict[str, object], raw)
    for required_key in ("category", "fit_year", "classes", "calibrators"):
        if required_key not in raw_dict:
            raise ValueError(f"Missing key '{required_key}' in {source}")
    calibrators = raw_dict["calibrators"]
    if not isinstance(calibrators, dict):
        raise ValueError(f"'calibrators' must be a dict in {source}")
    calibrators_dict = cast(dict[str, object], calibrators)
    for cls_name, table in calibrators_dict.items():
        if not isinstance(table, dict):
            raise ValueError(f"Calibration table for '{cls_name}' must be a dict in {source}")
        table_dict = cast(dict[str, object], table)
        if "x" not in table_dict or "y" not in table_dict:
            raise ValueError(f"Calibration table for '{cls_name}' missing 'x' or 'y' in {source}")
        x_val = table_dict["x"]
        y_val = table_dict["y"]
        if not isinstance(x_val, list) or not isinstance(y_val, list):
            raise ValueError(f"Calibration table for '{cls_name}' x/y must be lists in {source}")
        if len(x_val) != len(y_val):
            raise ValueError(
                f"Calibration table for '{cls_name}' has mismatched x/y lengths in {source}"
            )
    for required_class in ("nige", "senkou", "sashi", "oikomi"):
        if required_class not in calibrators_dict:
            raise ValueError(
                f"Calibrators JSON is missing required class '{required_class}' in {source}"
            )


def apply_calibration(
    probabilities: np.ndarray,
    calibrators: RunningStyleCalibrators,
) -> np.ndarray:
    """Apply OvR isotonic calibration to raw softmax probabilities.

    Args:
        probabilities: Shape (N, 4) float64 array in class order
                       [nige, senkou, sashi, oikomi].
        calibrators:   RunningStyleCalibrators loaded via load_calibrators().

    Returns:
        Calibrated and renormalized probabilities, same shape (N, 4).
        Row sums are guaranteed to equal 1.0 (within floating-point precision).
    """
    if probabilities.ndim != 2 or probabilities.shape[1] != len(CLASS_NAMES):
        raise ValueError(
            f"probabilities must have shape (N, 4); got {probabilities.shape}"
        )
    calibrated_cols: list[np.ndarray] = []
    for class_idx, class_name in enumerate(CLASS_NAMES):
        table = calibrators["calibrators"][class_name]
        x_knots = np.asarray(table["x"], dtype=np.float64)
        y_knots = np.asarray(table["y"], dtype=np.float64)
        col_raw = probabilities[:, class_idx]
        col_cal = np.interp(col_raw, x_knots, y_knots)
        calibrated_cols.append(col_cal)
    calibrated = np.stack(calibrated_cols, axis=1)
    row_sums = calibrated.sum(axis=1, keepdims=True)
    # Guard against degenerate all-zero rows
    row_sums = np.where(row_sums == 0.0, 1.0, row_sums)
    return (calibrated / row_sums).astype(np.float64)


def _repo_root() -> Path:
    """Return the repository root (four levels up from this file)."""
    return Path(__file__).resolve().parents[4]


def calibrators_path_for_model_version(model_version: str) -> str:
    """Return the conventional calibrators.json path for a model version.

    The returned path is relative to the repository root, e.g.:
        tmp/models/jra-running-style-lgbm-prod-v3/calibrators.json
    """
    return str(
        _repo_root() / MODELS_DIR_NAME / model_version / CALIBRATORS_FILENAME
    )
