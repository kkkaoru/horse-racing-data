"""Exercise local baseline wrapper arguments without touching a database."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("kind", ["base", "layers"])
def test_single_race_output_is_separate_and_local(tmp_path: Path, kind: str) -> None:
    name = f"build-local-banei-production-{kind}.sh"
    script = tmp_path / "repo/apps/pc-keiba-viewer/scripts" / name
    script.parent.mkdir(parents=True)
    shutil.copyfile(Path(__file__).resolve().parents[1] / "scripts" / name, script)
    binaries = tmp_path / "bin"
    binaries.mkdir()
    uv = binaries / "uv"
    uv.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n', encoding="utf-8")
    uv.chmod(0o755)
    env = {
        "PATH": f"{binaries}{os.pathsep}{os.defpath}",
        "PG_URL": "postgresql://test:test@localhost:5432/horse_racing",
        "FEATURE_RUN": "production-upcoming-single-race",
        "TARGET_DATE": "20260912",
        "TARGET_RACE": "83:01",
    }
    result = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, check=True
    )
    assert "production-upcoming-single-race/83" in result.stdout
    if kind == "base":
        assert "--target-race\n83:01\n" in result.stdout
        assert "--target-date\n20260912\n" in result.stdout
        env.pop("TARGET_RACE")
        historical = subprocess.run(
            ["bash", str(script)], env=env, capture_output=True, text=True, check=True
        )
        assert "--target-race" not in historical.stdout
    else:
        assert result.stdout.count("--input-dir") == 6
    env["PG_URL"] = "postgresql://test:test@remote.example:5432/horse_racing"
    rejected = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, check=False
    )
    assert rejected.returncode == 1
    assert "local" in rejected.stderr
