"""Tests for mlflow_tracking.__main__.

Only the module-import path is exercised here; the ``if __name__ ==
"__main__":`` guard itself is excluded from coverage via the approved
pragma in pyproject.toml (mirrors the finish-position-predict-container
CLI-wrapper convention).
"""

from __future__ import annotations

import mlflow_tracking.__main__ as main_module


def test_main_module_exposes_main_function() -> None:
    assert callable(main_module.main)
