"""Tests for score_running_style_local (Phase B, X5: raw probabilities only)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest

import score_running_style_local as subject
from running_style_calibration import RunningStyleCalibrators, CalibrationTable


def test_parse_args_minimum_required() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
        "--output-parquet", "/tmp/out.parquet",
        "--running-style-feature-version", "v1",
        "--category", "jra",
    ])
    assert args.model_version == "jra-running-style-lgbm-prod-v1.5"


def test_parse_args_accepts_nar_category() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--model-version", "nar-running-style-lgbm-prod-v1",
        "--output-parquet", "/tmp/out.parquet",
        "--running-style-feature-version", "v1",
        "--category", "nar",
    ])
    assert args.category == "nar"


def test_parse_args_rejects_ban_ei_category() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--model-version", "m",
            "--output-parquet", "/tmp/out.parquet",
            "--running-style-feature-version", "v1",
            "--category", "ban-ei",
        ])


def test_parse_args_rejects_bogus_category() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--model-version", "m",
            "--output-parquet", "/tmp/out.parquet",
            "--running-style-feature-version", "v1",
            "--category", "bogus",
        ])


def test_parse_args_requires_model_version() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--running-style-feature-version", "v1",
            "--category", "jra",
        ])


def test_parse_args_requires_output_parquet() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--model-version", "m",
            "--running-style-feature-version", "v1",
            "--category", "jra",
        ])


def test_parse_args_requires_feature_version() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--model-version", "m",
            "--output-parquet", "/tmp/out.parquet",
            "--category", "jra",
        ])


def test_repo_root_returns_horse_racing_data_directory() -> None:
    root = subject.repo_root()
    # When running from a git worktree under .claude/worktrees/<name>/, the
    # directory name is the worktree id rather than the canonical
    # "horse-racing-data". The invariant we actually rely on (repo_root is the
    # top of the checkout that contains apps/pc-keiba-viewer/...) holds in both
    # the canonical checkout AND every worktree.
    assert (root / "apps" / "pc-keiba-viewer").is_dir()


def test_resolve_artifact_path_uses_tmp_models_convention() -> None:
    artifact = subject.resolve_artifact_path("jra-running-style-lgbm-prod-v1.5")
    assert artifact.endswith("tmp/models/jra-running-style-lgbm-prod-v1.5/model.txt")


def test_resolve_artifact_path_handles_nar_version() -> None:
    artifact = subject.resolve_artifact_path("nar-running-style-lgbm-prod-v1")
    assert artifact.endswith("tmp/models/nar-running-style-lgbm-prod-v1/model.txt")


def test_assert_artifact_exists_returns_path_when_present() -> None:
    path_exists = MagicMock(return_value=True)
    assert (
        subject.assert_artifact_exists("/tmp/models/x/model.txt", path_exists=path_exists)
        == "/tmp/models/x/model.txt"
    )
    path_exists.assert_called_once_with("/tmp/models/x/model.txt")


def test_assert_artifact_exists_raises_when_missing() -> None:
    path_exists = MagicMock(return_value=False)
    with pytest.raises(FileNotFoundError):
        subject.assert_artifact_exists("/tmp/models/missing/model.txt", path_exists=path_exists)


def test_default_path_exists_true_for_existing_file(tmp_path: Path) -> None:
    target = tmp_path / "exists.txt"
    target.write_text("ok", encoding="utf-8")
    assert subject.default_path_exists(target.as_posix()) is True


def test_default_path_exists_false_for_missing_file(tmp_path: Path) -> None:
    target = tmp_path / "missing.txt"
    assert subject.default_path_exists(target.as_posix()) is False


def test_select_race_key_frame_returns_only_race_key_columns() -> None:
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    result = subject.select_race_key_frame(frame)
    assert list(result.columns) == [
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
    ]


def test_select_race_key_frame_skips_missing_columns() -> None:
    frame = pl.DataFrame({"source": ["jra"], "race_bango": ["01"]})
    result = subject.select_race_key_frame(frame)
    assert list(result.columns) == ["source", "race_bango"]


def test_build_probability_frame_writes_four_columns() -> None:
    probs = np.array([[0.4, 0.3, 0.2, 0.1], [0.1, 0.2, 0.3, 0.4]])
    result = subject.build_probability_frame(probs)
    assert list(result.columns) == ["p_nige", "p_senkou", "p_sashi", "p_oikomi"]
    assert result["p_nige"].to_list() == [0.4, 0.1]
    assert result["p_oikomi"].to_list() == [0.1, 0.4]


def test_attach_version_columns_sets_metadata_columns() -> None:
    frame = pl.DataFrame({"p_nige": [0.6]})
    result = subject.attach_version_columns(
        frame, feature_version="v1", model_version="jra-running-style-lgbm-prod-v1.5",
    )
    assert result["running_style_feature_version"].to_list() == ["v1"]
    assert result["model_version"].to_list() == ["jra-running-style-lgbm-prod-v1.5"]


def test_score_frame_writes_race_key_probability_and_version_columns() -> None:
    booster = MagicMock()
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
        "target_running_style_class": [0],
    })
    with patch.object(
        subject, "predict_softmax",
        return_value=np.array([[0.7, 0.1, 0.1, 0.1]]),
    ):
        result = subject.score_frame(
            booster=booster, frame=frame, feature_version="v1", model_version="m1",
        )
    assert list(result.columns) == [
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "p_nige",
        "p_senkou",
        "p_sashi",
        "p_oikomi",
        "running_style_feature_version",
        "model_version",
    ]


def test_score_frame_omits_predicted_class_columns() -> None:
    booster = MagicMock()
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    with patch.object(
        subject, "predict_softmax",
        return_value=np.array([[0.6, 0.2, 0.1, 0.1]]),
    ):
        result = subject.score_frame(
            booster=booster, frame=frame, feature_version="v1", model_version="m1",
        )
    assert "predicted_class" not in result.columns
    assert "second_predicted_class" not in result.columns
    assert "predicted_label" not in result.columns


def test_score_frame_with_empty_frame_returns_empty_output() -> None:
    booster = MagicMock()
    frame = pl.DataFrame(schema={
        "source": pl.Utf8,
        "kaisai_nen": pl.Utf8,
        "kaisai_tsukihi": pl.Utf8,
        "keibajo_code": pl.Utf8,
        "race_bango": pl.Utf8,
        "ketto_toroku_bango": pl.Utf8,
        "umaban": pl.Int64,
        "speed_index_avg_5": pl.Float64,
    })
    with patch.object(
        subject, "predict_softmax",
        return_value=np.empty((0, 4), dtype=np.float64),
    ):
        result = subject.score_frame(
            booster=booster, frame=frame, feature_version="v1", model_version="m1",
        )
    assert len(result) == 0
    assert "p_nige" in result.columns
    assert "running_style_feature_version" in result.columns


def test_write_logits_parquet_creates_parent_dir(tmp_path: Path) -> None:
    output_path = (tmp_path / "logits" / "category=jra" / "race_year=2006" / "data_0.parquet").as_posix()
    frame = MagicMock(spec=pl.DataFrame)
    subject.write_logits_parquet(frame, output_path)
    frame.write_parquet.assert_called_once_with(output_path)
    assert (tmp_path / "logits" / "category=jra" / "race_year=2006").is_dir()


def test_run_loads_artifact_from_convention_path_and_writes_logits() -> None:
    booster_loader = MagicMock(return_value=MagicMock())
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    pandas_reader = MagicMock(return_value=frame)
    path_exists = MagicMock(return_value=True)
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
        "--output-parquet", "/tmp/out.parquet",
        "--running-style-feature-version", "v1",
        "--category", "jra",
    ])
    with patch.object(
        subject, "predict_softmax",
        return_value=np.array([[0.6, 0.2, 0.1, 0.1]]),
    ):
        with patch.object(subject, "write_logits_parquet") as write_mock:
            with patch.object(subject, "try_load_calibrators", return_value=None):
                subject.run(
                    args,
                    booster_loader=booster_loader,
                    pandas_reader=pandas_reader,
                    path_exists=path_exists,
                )
    invoked_path = booster_loader.call_args.kwargs["model_file"]
    assert invoked_path.endswith("tmp/models/jra-running-style-lgbm-prod-v1.5/model.txt")
    assert write_mock.called


def test_run_raises_when_artifact_missing() -> None:
    booster_loader = MagicMock()
    pandas_reader = MagicMock()
    path_exists = MagicMock(return_value=False)
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--model-version", "nonexistent-version",
        "--output-parquet", "/tmp/out.parquet",
        "--running-style-feature-version", "v1",
        "--category", "jra",
    ])
    with pytest.raises(FileNotFoundError):
        subject.run(
            args,
            booster_loader=booster_loader,
            pandas_reader=pandas_reader,
            path_exists=path_exists,
        )
    booster_loader.assert_not_called()
    pandas_reader.assert_not_called()


def _make_calibrators() -> RunningStyleCalibrators:
    table = CalibrationTable(x=[0.0, 1.0], y=[0.0, 1.0])
    return RunningStyleCalibrators(
        category="jra",
        fit_year=2025,
        classes=["nige", "senkou", "sashi", "oikomi"],
        calibrators={"nige": table, "senkou": table, "sashi": table, "oikomi": table},
    )


def test_score_frame_with_calibrators_applies_calibration() -> None:
    """score_frame with calibrators should pass probabilities through apply_calibration."""
    booster = MagicMock()
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    calibrated_probs = np.array([[0.6, 0.2, 0.1, 0.1]])
    with patch.object(
        subject, "predict_softmax",
        return_value=np.array([[0.7, 0.1, 0.1, 0.1]]),
    ):
        with patch.object(
            subject, "apply_calibration",
            return_value=calibrated_probs,
        ) as apply_mock:
            result = subject.score_frame(
                booster=booster,
                frame=frame,
                feature_version="v3",
                model_version="jra-running-style-lgbm-prod-v3",
                calibrators=_make_calibrators(),
            )
    apply_mock.assert_called_once()
    assert float(result["p_nige"][0]) == pytest.approx(0.6)


def test_score_frame_without_calibrators_skips_calibration() -> None:
    """score_frame with calibrators=None must not call apply_calibration."""
    booster = MagicMock()
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    with patch.object(
        subject, "predict_softmax",
        return_value=np.array([[0.7, 0.1, 0.1, 0.1]]),
    ):
        with patch.object(subject, "apply_calibration") as apply_mock:
            subject.score_frame(
                booster=booster,
                frame=frame,
                feature_version="v3",
                model_version="jra-running-style-lgbm-prod-v3",
                calibrators=None,
            )
    apply_mock.assert_not_called()


def testtry_load_calibrators_returns_none_when_file_absent() -> None:
    path_exists = MagicMock(return_value=False)
    result = subject.try_load_calibrators("jra-running-style-lgbm-prod-v3", path_exists=path_exists)
    assert result is None


def testtry_load_calibrators_returns_calibrators_when_present(tmp_path: Path) -> None:
    # Write a real calibrators file
    payload = {
        "category": "jra",
        "fit_year": 2025,
        "classes": ["nige", "senkou", "sashi", "oikomi"],
        "calibrators": {
            cls: {"x": [0.0, 1.0], "y": [0.0, 1.0]}
            for cls in ("nige", "senkou", "sashi", "oikomi")
        },
    }
    calib_file = tmp_path / "calibrators.json"
    calib_file.write_text(json.dumps(payload), encoding="utf-8")

    path_exists_real = MagicMock(return_value=True)
    with patch.object(
        subject, "calibrators_path_for_model_version",
        return_value=str(calib_file),
    ):
        result = subject.try_load_calibrators(
            "jra-running-style-lgbm-prod-v3",
            path_exists=path_exists_real,
        )
    assert result is not None
    assert result["category"] == "jra"


def test_run_passes_calibrators_when_present(tmp_path: Path) -> None:
    """run() should attempt to load calibrators and pass them to score_frame."""
    booster_loader = MagicMock(return_value=MagicMock())
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    pandas_reader = MagicMock(return_value=frame)
    path_exists = MagicMock(return_value=True)
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--model-version", "jra-running-style-lgbm-prod-v3",
        "--output-parquet", "/tmp/out.parquet",
        "--running-style-feature-version", "v3",
        "--category", "jra",
    ])
    dummy_calibrators = _make_calibrators()
    with patch.object(subject, "predict_softmax", return_value=np.array([[0.6, 0.2, 0.1, 0.1]])):
        with patch.object(subject, "write_logits_parquet"):
            with patch.object(subject, "try_load_calibrators", return_value=dummy_calibrators) as calib_mock:
                subject.run(
                    args,
                    booster_loader=booster_loader,
                    pandas_reader=pandas_reader,
                    path_exists=path_exists,
                )
    calib_mock.assert_called_once_with("jra-running-style-lgbm-prod-v3", path_exists=path_exists)


def test_run_passes_none_calibrators_when_absent() -> None:
    """run() should pass calibrators=None to score_frame when file does not exist."""
    booster_loader = MagicMock(return_value=MagicMock())
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    pandas_reader = MagicMock(return_value=frame)
    path_exists = MagicMock(return_value=True)
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--model-version", "jra-running-style-lgbm-prod-v3",
        "--output-parquet", "/tmp/out.parquet",
        "--running-style-feature-version", "v3",
        "--category", "jra",
    ])
    with patch.object(subject, "predict_softmax", return_value=np.array([[0.6, 0.2, 0.1, 0.1]])):
        with patch.object(subject, "write_logits_parquet"):
            with patch.object(subject, "try_load_calibrators", return_value=None):
                with patch.object(subject, "apply_calibration") as apply_mock:
                    subject.run(
                        args,
                        booster_loader=booster_loader,
                        pandas_reader=pandas_reader,
                        path_exists=path_exists,
                    )
    apply_mock.assert_not_called()


def test_score_frame_passes_detected_categoricals_to_predict_softmax() -> None:
    """score_frame must call detect_categorical_features and pass the result to predict_softmax."""
    booster = MagicMock()
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2024"],
        "kaisai_tsukihi": ["0101"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "track_code": ["A"],
        "speed_index_avg_5": [50.0],
    })
    detected_cats = ["track_code"]
    with patch.object(
        subject, "detect_categorical_features",
        return_value=detected_cats,
    ) as detect_mock:
        with patch.object(
            subject, "predict_softmax",
            return_value=np.array([[0.7, 0.1, 0.1, 0.1]]),
        ) as predict_mock:
            subject.score_frame(
                booster=booster, frame=frame, feature_version="v1", model_version="m1",
            )
    detect_mock.assert_called_once()
    called_categoricals = predict_mock.call_args[0][3]
    assert called_categoricals == detected_cats


def test_main_uses_lightgbm_booster_and_polars_read_parquet() -> None:
    fake_booster = MagicMock()
    argv = [
        "--features-parquet", "/tmp/feat.parquet",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
        "--output-parquet", "/tmp/out.parquet",
        "--running-style-feature-version", "v1",
        "--category", "jra",
    ]
    frame = pl.DataFrame({"x": [1]})
    with patch("lightgbm.Booster", return_value=fake_booster) as booster_mock:
        with patch.object(subject, "default_path_exists", return_value=True):
            with patch.object(subject, "try_load_calibrators", return_value=None):
                with patch.object(subject, "score_frame", return_value=frame):
                    with patch.object(subject, "write_logits_parquet"):
                        with patch.object(pl, "read_parquet", return_value=frame):
                            subject.main(argv)
    invoked_path = booster_mock.call_args.kwargs["model_file"]
    assert invoked_path.endswith("tmp/models/jra-running-style-lgbm-prod-v1.5/model.txt")
