"""Running-style feature version SSoT loader (reads JSON shared with TS)."""

from __future__ import annotations

import json
from pathlib import Path

_JSON_PATH: Path = (
    Path(__file__).parent / "finish-position-features" / "running-style-feature-version.json"
)
_DATA: dict[str, str] = json.loads(_JSON_PATH.read_text(encoding="utf-8"))

RUNNING_STYLE_FEATURE_VERSION: str = _DATA["version"]
RUNNING_STYLE_FEATURE_VERSION_DESC: str = _DATA["description"]
