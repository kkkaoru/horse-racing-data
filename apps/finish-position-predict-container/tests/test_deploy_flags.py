"""Tests for the tracked deploy-flags declaration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from predict_lib.deploy_flags import (
    DEPLOY_FLAGS_PATH,
    DeployFlags,
    DeployFlagsValidationError,
    load_deploy_flags,
)


def _write(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "deploy_flags.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_tracked_deploy_flags_load_and_default_to_enabled() -> None:
    flags = load_deploy_flags()

    assert flags == DeployFlags(nar_transformer_blend_enabled=True)


def test_load_deploy_flags_accepts_false(tmp_path: Path) -> None:
    path = _write(tmp_path, {"nar_transformer_blend_enabled": False})

    flags = load_deploy_flags(path)

    assert flags == DeployFlags(nar_transformer_blend_enabled=False)


def test_load_deploy_flags_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(DeployFlagsValidationError, match="not found"):
        load_deploy_flags(tmp_path / "missing.json")


def test_load_deploy_flags_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "deploy_flags.json"
    path.write_text("{", encoding="utf-8")

    with pytest.raises(DeployFlagsValidationError, match="invalid deploy_flags"):
        load_deploy_flags(path)


def test_load_deploy_flags_rejects_non_object(tmp_path: Path) -> None:
    path = _write(tmp_path, [])

    with pytest.raises(DeployFlagsValidationError, match="must be an object"):
        load_deploy_flags(path)


def test_load_deploy_flags_rejects_missing_key(tmp_path: Path) -> None:
    path = _write(tmp_path, {})

    with pytest.raises(DeployFlagsValidationError, match="keys differ"):
        load_deploy_flags(path)


def test_load_deploy_flags_rejects_unexpected_key(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {"nar_transformer_blend_enabled": True, "extra_flag": False},
    )

    with pytest.raises(DeployFlagsValidationError, match="keys differ"):
        load_deploy_flags(path)


def test_load_deploy_flags_rejects_non_boolean_value(tmp_path: Path) -> None:
    path = _write(tmp_path, {"nar_transformer_blend_enabled": "true"})

    with pytest.raises(DeployFlagsValidationError, match="must be a boolean"):
        load_deploy_flags(path)


def test_deploy_flags_path_points_at_tracked_file() -> None:
    assert DEPLOY_FLAGS_PATH.name == "deploy_flags.json"
    assert DEPLOY_FLAGS_PATH.exists()
