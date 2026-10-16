"""Reproducibility and immutable-output checks for the Arc training CLI."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import train_arc_finish_model as training
from arc_finish_model import FEATURES


def test_hash_mismatch_prevents_training(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    source.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        training.load_dataset(
            source=source, races=tmp_path / "missing", source_sha256="invalid"
        )


def test_invalid_snapshot_shape(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    source.write_text("[]", encoding="utf-8")
    races = tmp_path / "races.json"
    races.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON objects"):
        training.load_dataset(
            source=source, races=races, source_sha256=hashlib.sha256(b"[]").hexdigest()
        )


def test_source_to_dataset_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps({"history": [{"horse": "h"}], "identities": [], "peopleDaily": []}),
        encoding="utf-8",
    )
    races = tmp_path / "races.json"
    races.write_text(
        json.dumps(
            {
                "races": [
                    {
                        "kaisai_nen": "2023",
                        "kaisai_tsukihi": "1001",
                        "shusso_tosu": "15",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        training, "build_dataset", lambda history, **kwargs: (history, {})
    )
    data, excluded = training.load_dataset(
        source=source,
        races=races,
        source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
    )
    assert data.to_dict("records") == [{"horse": "h"}]
    assert excluded == {}


def test_reproducible_artifacts_and_output_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = pd.DataFrame(np.zeros((25, len(FEATURES))), columns=FEATURES)
    data["horse_id"] = np.tile(["a", "b", "c", "d", "e"], 5)
    data["race_date"] = np.repeat(
        ["2021-10-01", "2022-10-01", "2023-10-01", "2024-10-01", "2025-10-01"], 5
    )
    data["finish"] = np.tile([1, 2, 3, 4, 5], 5)
    monkeypatch.setattr(training, "load_dataset", lambda **kwargs: (data, {}))
    races = tmp_path / "races.json"
    races.write_text("{}", encoding="utf-8")
    report = training.train_snapshot(
        source=tmp_path / "source",
        races=races,
        source_sha256="pinned",
        cutoff="2026-10-04",
        output=tmp_path / "model",
    )
    assert report["productionTrainingMaxDate"] == "2025-10-01"
    assert (tmp_path / "model" / "arc-production.json").is_file()
    result = json.loads(
        (tmp_path / "model" / "evaluation.json").read_text(encoding="utf-8")
    )
    assert result["folds"][0]["trainingMaxDate"] == "2022-10-01"
    assert result["folds"][2]["trainingRaces"] == 4
    with pytest.raises(FileExistsError):
        training.train_snapshot(
            source=tmp_path / "source",
            races=races,
            source_sha256="pinned",
            cutoff="2026-10-04",
            output=tmp_path / "model",
        )
    with pytest.raises(ValueError, match="Missing evaluation"):
        training.evaluate_fold(data, year=2020, output=tmp_path)


@pytest.mark.parametrize("value", [{}, [None], [{1: "invalid-key"}]])
def test_invalid_snapshot_table(value: object) -> None:
    with pytest.raises(ValueError):
        training.snapshot_frame(value)


def test_main(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "train_arc_finish_model",
            "--source",
            "source.json",
            "--races",
            "races.json",
            "--source-sha256",
            "abc",
            "--cutoff",
            "2026-10-04",
            "--output",
            "new-output",
        ],
    )
    monkeypatch.setattr(training, "train_snapshot", lambda **kwargs: {"ok": True})
    training.main()
    assert capsys.readouterr().out == '{"ok": true}\n'
