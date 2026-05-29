"""Tests for running_style_feature_version SSoT loader."""

from __future__ import annotations

from pathlib import Path

import running_style_feature_version as subject


def test_running_style_feature_version_equals_v1() -> None:
    assert subject.RUNNING_STYLE_FEATURE_VERSION == "v1"


def test_running_style_feature_version_desc_is_non_empty() -> None:
    assert len(subject.RUNNING_STYLE_FEATURE_VERSION_DESC) > 0


def test_running_style_feature_version_json_path_exists() -> None:
    json_path: Path = (
        Path(subject.__file__).parent
        / "finish-position-features"
        / "running-style-feature-version.json"
    )
    assert json_path.is_file()
