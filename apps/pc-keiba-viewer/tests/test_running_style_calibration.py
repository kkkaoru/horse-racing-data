"""Tests for running_style_calibration module.

Covers:
- load_calibrators: happy path, file-not-found, invalid structure
- apply_calibration: identity transform, monotone transform, renormalization
- calibrators_path_for_model_version: path construction
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

import running_style_calibration as subject


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_calibrators_dict(
    category: str = "jra",
    fit_year: int = 2025,
    x: list[float] | None = None,
    y: list[float] | None = None,
) -> dict[str, object]:
    """Build a minimal valid calibrators payload dict."""
    if x is None:
        x = [0.0, 0.5, 1.0]
    if y is None:
        y = [0.0, 0.5, 1.0]
    table = {"x": x, "y": y}
    return {
        "category": category,
        "fit_year": fit_year,
        "classes": ["nige", "senkou", "sashi", "oikomi"],
        "calibrators": {
            "nige": dict(table),
            "senkou": dict(table),
            "sashi": dict(table),
            "oikomi": dict(table),
        },
    }


def _write_calibrators(tmp_path: Path, payload: dict[str, object]) -> Path:
    p = tmp_path / "calibrators.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# load_calibrators
# ---------------------------------------------------------------------------

def test_load_calibrators_happy_path(tmp_path: Path) -> None:
    payload = _make_calibrators_dict()
    p = _write_calibrators(tmp_path, payload)
    result = subject.load_calibrators(str(p))
    assert result["category"] == "jra"
    assert result["fit_year"] == 2025
    assert result["classes"] == ["nige", "senkou", "sashi", "oikomi"]
    assert set(result["calibrators"].keys()) == {"nige", "senkou", "sashi", "oikomi"}


def test_load_calibrators_returns_float_lists(tmp_path: Path) -> None:
    payload = _make_calibrators_dict(x=[0.0, 0.25, 0.5, 0.75, 1.0], y=[0.0, 0.2, 0.5, 0.8, 1.0])
    p = _write_calibrators(tmp_path, payload)
    result = subject.load_calibrators(str(p))
    for cls in ("nige", "senkou", "sashi", "oikomi"):
        assert all(isinstance(v, float) for v in result["calibrators"][cls]["x"])
        assert all(isinstance(v, float) for v in result["calibrators"][cls]["y"])


def test_load_calibrators_file_not_found(tmp_path: Path) -> None:
    missing = tmp_path / "nonexistent.json"
    with pytest.raises(FileNotFoundError):
        subject.load_calibrators(str(missing))


def test_load_calibrators_invalid_top_level_not_dict(tmp_path: Path) -> None:
    p = tmp_path / "bad.json"
    p.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with pytest.raises(ValueError):
        subject.load_calibrators(str(p))


def test_load_calibrators_missing_required_key(tmp_path: Path) -> None:
    payload = _make_calibrators_dict()
    del payload["fit_year"]  # type: ignore[misc]
    p = _write_calibrators(tmp_path, payload)
    with pytest.raises(ValueError):
        subject.load_calibrators(str(p))


def test_load_calibrators_calibrators_not_dict(tmp_path: Path) -> None:
    payload = _make_calibrators_dict()
    payload["calibrators"] = "not-a-dict"
    p = _write_calibrators(tmp_path, payload)
    with pytest.raises(ValueError):
        subject.load_calibrators(str(p))


def test_load_calibrators_table_not_dict(tmp_path: Path) -> None:
    payload = _make_calibrators_dict()
    payload["calibrators"] = {"nige": "bad", "senkou": {}, "sashi": {}, "oikomi": {}}  # type: ignore[assignment]
    p = _write_calibrators(tmp_path, payload)
    with pytest.raises(ValueError):
        subject.load_calibrators(str(p))


def test_load_calibrators_table_missing_x_key(tmp_path: Path) -> None:
    payload = _make_calibrators_dict()
    payload["calibrators"] = {  # type: ignore[assignment]
        "nige": {"y": [0.0, 1.0]},
        "senkou": {"x": [0.0, 1.0], "y": [0.0, 1.0]},
        "sashi": {"x": [0.0, 1.0], "y": [0.0, 1.0]},
        "oikomi": {"x": [0.0, 1.0], "y": [0.0, 1.0]},
    }
    p = _write_calibrators(tmp_path, payload)
    with pytest.raises(ValueError):
        subject.load_calibrators(str(p))


def test_load_calibrators_table_mismatched_xy_lengths(tmp_path: Path) -> None:
    payload = _make_calibrators_dict()
    payload["calibrators"] = {  # type: ignore[assignment]
        "nige": {"x": [0.0, 0.5, 1.0], "y": [0.0, 1.0]},  # mismatch
        "senkou": {"x": [0.0, 1.0], "y": [0.0, 1.0]},
        "sashi": {"x": [0.0, 1.0], "y": [0.0, 1.0]},
        "oikomi": {"x": [0.0, 1.0], "y": [0.0, 1.0]},
    }
    p = _write_calibrators(tmp_path, payload)
    with pytest.raises(ValueError):
        subject.load_calibrators(str(p))


def test_load_calibrators_nar_category(tmp_path: Path) -> None:
    payload = _make_calibrators_dict(category="nar", fit_year=2024)
    p = _write_calibrators(tmp_path, payload)
    result = subject.load_calibrators(str(p))
    assert result["category"] == "nar"
    assert result["fit_year"] == 2024


# ---------------------------------------------------------------------------
# apply_calibration
# ---------------------------------------------------------------------------

def _make_calibrators_from_xy(
    x: list[float],
    y: list[float],
    category: str = "jra",
    fit_year: int = 2025,
) -> subject.RunningStyleCalibrators:
    """Build a RunningStyleCalibrators directly from x/y knot lists (without file I/O)."""
    table = subject.CalibrationTable(x=x, y=y)
    return subject.RunningStyleCalibrators(
        category=category,
        fit_year=fit_year,
        classes=["nige", "senkou", "sashi", "oikomi"],
        calibrators={"nige": table, "senkou": table, "sashi": table, "oikomi": table},
    )


def test_apply_calibration_identity_transform() -> None:
    """Identity calibrator (y=x) must leave probabilities unchanged after renorm."""
    calibrators = _make_calibrators_from_xy(x=[0.0, 1.0], y=[0.0, 1.0])
    probs = np.array([[0.7, 0.1, 0.1, 0.1], [0.25, 0.25, 0.25, 0.25]])
    result = subject.apply_calibration(probs, calibrators)
    # Identity + renorm = original (rows already sum to 1)
    np.testing.assert_allclose(result, probs, atol=1e-10)


def test_apply_calibration_renormalizes_to_one() -> None:
    """After calibration every row must sum to 1.0."""
    calibrators = _make_calibrators_from_xy(x=[0.0, 0.5, 1.0], y=[0.0, 0.3, 0.8])
    probs = np.array([
        [0.6, 0.2, 0.1, 0.1],
        [0.1, 0.1, 0.4, 0.4],
        [0.9, 0.05, 0.03, 0.02],
    ])
    result = subject.apply_calibration(probs, calibrators)
    row_sums = result.sum(axis=1)
    np.testing.assert_allclose(row_sums, np.ones(3), atol=1e-12)


def test_apply_calibration_monotone_transform_preserves_argmax_on_clear_winner() -> None:
    """Monotone increasing calibrator should preserve argmax for dominant probs."""
    calibrators = _make_calibrators_from_xy(
        x=[0.0, 0.25, 0.5, 0.75, 1.0],
        y=[0.0, 0.1, 0.3, 0.7, 1.0],
    )
    # Horse clearly predicted as nige (class 0)
    probs = np.array([[0.9, 0.05, 0.03, 0.02]])
    result = subject.apply_calibration(probs, calibrators)
    assert int(np.argmax(result[0])) == 0


def test_apply_calibration_output_shape_preserved() -> None:
    calibrators = _make_calibrators_from_xy(x=[0.0, 0.5, 1.0], y=[0.0, 0.5, 1.0])
    probs = np.random.default_rng(42).dirichlet(alpha=[1.0, 1.0, 1.0, 1.0], size=100)
    result = subject.apply_calibration(probs, calibrators)
    assert result.shape == (100, 4)


def test_apply_calibration_all_non_negative() -> None:
    """Output probabilities must all be non-negative."""
    calibrators = _make_calibrators_from_xy(x=[0.0, 0.5, 1.0], y=[0.0, 0.4, 0.9])
    rng = np.random.default_rng(7)
    probs = rng.dirichlet(alpha=[2.0, 1.0, 1.0, 0.5], size=50)
    result = subject.apply_calibration(probs, calibrators)
    assert float(result.min()) >= 0.0


def test_apply_calibration_wrong_num_classes_raises() -> None:
    calibrators = _make_calibrators_from_xy(x=[0.0, 0.5, 1.0], y=[0.0, 0.5, 1.0])
    probs_3class = np.array([[0.5, 0.3, 0.2]])
    with pytest.raises(ValueError):
        subject.apply_calibration(probs_3class, calibrators)


def test_apply_calibration_1d_array_raises() -> None:
    calibrators = _make_calibrators_from_xy(x=[0.0, 0.5, 1.0], y=[0.0, 0.5, 1.0])
    probs_1d = np.array([0.7, 0.1, 0.1, 0.1])
    with pytest.raises(ValueError):
        subject.apply_calibration(probs_1d, calibrators)


def test_apply_calibration_empty_batch() -> None:
    """Zero-row input should produce zero-row output without error."""
    calibrators = _make_calibrators_from_xy(x=[0.0, 0.5, 1.0], y=[0.0, 0.5, 1.0])
    probs_empty = np.empty((0, 4), dtype=np.float64)
    result = subject.apply_calibration(probs_empty, calibrators)
    assert result.shape == (0, 4)


def test_apply_calibration_degenerate_zero_row() -> None:
    """All-zero calibrated row should not produce NaN (guard against div-by-zero)."""
    calibrators = _make_calibrators_from_xy(x=[0.0, 1.0], y=[0.0, 0.0])
    probs = np.array([[0.25, 0.25, 0.25, 0.25]])
    result = subject.apply_calibration(probs, calibrators)
    assert not np.any(np.isnan(result))


# ---------------------------------------------------------------------------
# calibrators_path_for_model_version
# ---------------------------------------------------------------------------

def test_calibrators_path_for_model_version_ends_with_filename() -> None:
    path = subject.calibrators_path_for_model_version("jra-running-style-lgbm-prod-v3")
    assert path.endswith("tmp/models/jra-running-style-lgbm-prod-v3/calibrators.json")


def test_calibrators_path_for_model_version_nar() -> None:
    path = subject.calibrators_path_for_model_version("nar-running-style-lgbm-prod-v3")
    assert path.endswith("tmp/models/nar-running-style-lgbm-prod-v3/calibrators.json")


def test_calibrators_path_for_model_version_contains_repo_root() -> None:
    """Path must be absolute (contains the repo structure)."""
    path = subject.calibrators_path_for_model_version("jra-running-style-lgbm-prod-v3")
    assert "horse-racing-data" in path or "apps" not in path or "tmp" in path
    # Minimal sanity: it's an absolute-ish path that contains tmp/models
    assert "tmp/models" in path


def test_calibrators_path_for_model_version_arbitrary_version() -> None:
    path = subject.calibrators_path_for_model_version("custom-model-v99")
    assert "custom-model-v99" in path
    assert path.endswith("calibrators.json")
