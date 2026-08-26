from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from predict_lib.jra_joint_alternate_scorer import (
    MODEL_FILE_NAME,
    MODEL_VERSION,
    JraJointAlternateScorer,
)


def _arrays(feature_count: int = 1) -> dict[str, np.ndarray]:
    return {
        "runner.layers.0.weight": np.zeros((128, feature_count + 2), np.float32),
        "runner.layers.0.bias": np.zeros(128, np.float32),
        "runner.layers.2.weight": np.zeros((64, 128), np.float32),
        "runner.layers.2.bias": np.zeros(64, np.float32),
        "residual.layers.0.weight": np.zeros((64, 128), np.float32),
        "residual.layers.0.bias": np.zeros(64, np.float32),
        "residual.layers.2.weight": np.zeros((1, 64), np.float32),
        "residual.layers.2.bias": np.zeros(1, np.float32),
        "defer.layers.0.weight": np.zeros((32, 64), np.float32),
        "defer.layers.0.bias": np.zeros(32, np.float32),
        "defer.layers.2.weight": np.zeros((1, 32), np.float32),
        "defer.layers.2.bias": np.zeros(1, np.float32),
        "mean": np.zeros(feature_count, np.float32),
        "std": np.ones(feature_count, np.float32),
    }


def _artifact(tmp_path: Path, *, margin: float = 0.1) -> Path:
    metadata = {
        "model_version": MODEL_VERSION,
        "feature_names": ["feature"],
        "feature_count": 1,
        "shadow_only": True,
        "changes_primary_rank": False,
        "minimum_candidate_margin": margin,
    }
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    with (tmp_path / MODEL_FILE_NAME).open("wb") as output:
        np.savez_compressed(output, allow_pickle=False, **_arrays())
    return tmp_path


def _entries(count: int = 7) -> list[dict[str, object]]:
    return [
        {
            "ketto_toroku_bango": f"H{index}",
            "feature": "missing" if index == 7 else float(index),
        }
        for index in range(1, count + 1)
    ]


def test_load_and_select_high_margin_candidate_without_changing_top5(tmp_path: Path) -> None:
    scorer = JraJointAlternateScorer.load(_artifact(tmp_path))
    result = scorer.select_candidate(
        _entries(),
        [7, 6, 5, 4, 3, 2, 1],
        [1, 1, 1, 1, 1, 1, 1],
        ["H1", "H2", "H3", "H4", "H5"],
    )

    assert result.horse_id == "H6"
    assert result.emitted is True
    assert result.margin is not None and result.margin > 0.1
    assert result.defer_probability == pytest.approx(0.5)
    assert result.reason == "joint-margin-pass"


def test_candidate_abstains_for_small_fields_low_margin_and_no_outside(tmp_path: Path) -> None:
    scorer = JraJointAlternateScorer.load(_artifact(tmp_path, margin=100.0))
    assert scorer.select_candidate(_entries(5), [5, 4, 3, 2, 1], [1] * 5, []).reason == (
        "field-size-le5"
    )
    assert (
        scorer.select_candidate(
            _entries(), [7, 6, 5, 4, 3, 2, 1], [1] * 7, ["H1", "H2", "H3", "H4", "H5"]
        ).reason
        == "low-margin-abstain"
    )

    ordinary = JraJointAlternateScorer.load(_artifact(tmp_path, margin=0.0))
    assert (
        ordinary.select_candidate(
            _entries(), [7, 6, 5, 4, 3, 2, 1], [1] * 7, [f"H{i}" for i in range(1, 8)]
        ).reason
        == "no-outside-runner"
    )


def test_candidate_validates_lengths_scores_and_horse_identities(tmp_path: Path) -> None:
    scorer = JraJointAlternateScorer.load(_artifact(tmp_path))
    with pytest.raises(ValueError, match="lengths differ"):
        scorer.select_candidate(_entries(), [1], [1] * 7, [])
    with pytest.raises(ValueError, match="finite vectors"):
        scorer.select_candidate(_entries(), [1, 2, 3, 4, 5, 6, float("nan")], [1] * 7, [])
    duplicated = _entries()
    duplicated[-1]["ketto_toroku_bango"] = "H1"
    with pytest.raises(ValueError, match="missing or duplicated"):
        scorer.select_candidate(duplicated, [7, 6, 5, 4, 3, 2, 1], [1] * 7, [])


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"feature_names": []}, "feature_names invalid"),
        ({"feature_count": 2}, "feature_count mismatch"),
        ({"shadow_only": False}, "must be shadow-only"),
        ({"minimum_candidate_margin": True}, "minimum_candidate_margin invalid"),
        ({"minimum_candidate_margin": -1}, "minimum_candidate_margin invalid"),
    ],
)
def test_load_rejects_invalid_metadata(
    tmp_path: Path, updates: dict[str, object], message: str
) -> None:
    path = _artifact(tmp_path)
    metadata_path = path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.update(updates)
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        JraJointAlternateScorer.load(path)


def test_load_rejects_metadata_and_array_contract_violations(tmp_path: Path) -> None:
    path = _artifact(tmp_path)
    metadata_path = path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["model_version"] = "wrong"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="identity"):
        JraJointAlternateScorer.load(path)

    _artifact(tmp_path)
    with (path / MODEL_FILE_NAME).open("wb") as output:
        arrays = _arrays()
        arrays.pop("mean")
        np.savez_compressed(output, allow_pickle=False, **arrays)
    with pytest.raises(ValueError, match="arrays mismatch"):
        JraJointAlternateScorer.load(path)

    _artifact(tmp_path)
    with (path / MODEL_FILE_NAME).open("wb") as output:
        arrays = _arrays()
        arrays["mean"] = np.zeros(2, np.float32)
        np.savez_compressed(output, allow_pickle=False, **arrays)
    with pytest.raises(ValueError, match="array invalid: mean"):
        JraJointAlternateScorer.load(path)

    _artifact(tmp_path)
    with (path / MODEL_FILE_NAME).open("wb") as output:
        arrays = _arrays()
        arrays["std"][0] = 0
        np.savez_compressed(output, allow_pickle=False, **arrays)
    with pytest.raises(ValueError, match="std must be positive"):
        JraJointAlternateScorer.load(path)
