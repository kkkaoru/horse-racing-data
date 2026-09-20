"""Prevent annotation syntax from hiding executable functions from coverage."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "line",
    [
        "def values() -> tuple[int, ...]:",
        "    support: tuple[int, ...]",
        "def nested() -> tuple[tuple[int, ...], tuple[int, ...]]:",
        '    text = "..."',
    ],
)
def test_coverage_does_not_exclude_annotations_or_string_values(line: str) -> None:
    config = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    patterns = config["tool"]["coverage"]["report"]["exclude_lines"]
    assert not any(re.search(pattern, line) for pattern in patterns)


@pytest.mark.parametrize(
    "line", ["    ...", "    def method(self) -> None: ...", "    ...  # Protocol stub"]
)
def test_coverage_still_excludes_only_unexecutable_ellipsis_stubs(line: str) -> None:
    config = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    patterns = config["tool"]["coverage"]["report"]["exclude_lines"]
    assert any(re.search(pattern, line) for pattern in patterns)
