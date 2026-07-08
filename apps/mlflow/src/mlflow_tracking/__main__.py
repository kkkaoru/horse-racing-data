"""Entry point for ``uv run python -m mlflow_tracking``."""

from __future__ import annotations

import sys

from mlflow_tracking.cli import main

if __name__ == "__main__":
    sys.exit(main())
