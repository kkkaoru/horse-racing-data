"""Single source of truth for FINISH_POSITION_VERSION (Agent G).

Reads ``finish-position-features/finish-position-version.json`` so that the
TypeScript and Python sides cannot drift. Any rename must update both this
module and the TS module that consumes the same JSON file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict

SSOT_PATH: Path = (
    Path(__file__).resolve().parent / "finish-position-features" / "finish-position-version.json"
)


class FinishPositionVersionFile(TypedDict):
    version: str
    description: str


def parse_finish_position_version_payload(raw: str) -> FinishPositionVersionFile:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("finish-position-version.json must be a JSON object")
    version = parsed.get("version")
    description = parsed.get("description")
    if not isinstance(version, str) or version == "":
        raise ValueError("finish-position-version.json is missing string field 'version'")
    if not isinstance(description, str):
        raise ValueError("finish-position-version.json is missing string field 'description'")
    return {"version": version, "description": description}


def read_finish_position_version_file(path: Path) -> FinishPositionVersionFile:
    return parse_finish_position_version_payload(path.read_text(encoding="utf-8"))


_FILE = read_finish_position_version_file(SSOT_PATH)

FINISH_POSITION_VERSION: str = _FILE["version"]
FINISH_POSITION_VERSION_DESCRIPTION: str = _FILE["description"]
