from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-workout-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_workout_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_workout_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "10:02",
        ]
    )
    assert args.target_race == "10:02"


def test_stage_workout_raw_focused_filters_to_base_parquet_horses() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_workout_raw(FakeConn(), "20230101", focused_target=True)
    body = " ".join(captured)
    assert "from pg.jvd_hc" in body
    assert "ketto_toroku_bango in (select distinct ketto_toroku_bango from base_parquet)" in body
    assert "chokyo_nengappi >=" in body


def test_stage_workout_raw_default_does_not_require_base_parquet() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_workout_raw(FakeConn(), "20230101")
    body = " ".join(captured)
    assert "base_parquet" not in body

