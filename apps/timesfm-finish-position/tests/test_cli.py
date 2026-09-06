from __future__ import annotations

from pathlib import Path

import pytest

import timesfm_finish_position.cli as subject
from timesfm_finish_position.cli import main, parse_args


def test_parse_args_has_local_defaults() -> None:
    args = parse_args(["--accept-non-commercial-license"])
    assert args.checkpoint == "google/timesfm-3.0-pytorch"
    assert args.context_length == 512
    assert args.batch_size == 4
    assert args.accept_non_commercial_license is True


def test_main_requires_noncommercial_acknowledgement() -> None:
    with pytest.raises(SystemExit, match="non-commercial and non-production"):
        main([])


def test_main_runs_and_writes_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "report.json"
    observed: dict[str, object] = {}

    def fake_run(config: object) -> dict[str, object]:
        observed["config"] = config
        return {"schema": "test"}

    def fake_write(report: dict[str, object], path: Path) -> None:
        observed["report"] = report
        observed["path"] = path

    monkeypatch.setattr(subject, "run_experiment", fake_run)
    monkeypatch.setattr(subject, "write_report", fake_write)
    main(
        [
            "--accept-non-commercial-license",
            "--input",
            str(tmp_path / "input.parquet"),
            "--output",
            str(output),
            "--device",
            "cpu",
            "--bootstrap-repetitions",
            "10",
        ]
    )
    assert observed["report"] == {"schema": "test"}
    assert observed["path"] == output
