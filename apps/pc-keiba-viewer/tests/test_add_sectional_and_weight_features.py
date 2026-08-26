from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-sectional-and-weight-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_sectional_and_weight_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_sectional_and_weight_features"] = subject
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


def test_stage_history_focused_filters_bataiju_and_rec_to_base_horses() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history(FakeConn(), "20230101", "20231231", focused_target=True)
    body = " ".join(captured)
    assert "from pg.jvd_se" in body
    assert "from pg.race_entry_corner_features rec" in body
    assert "ketto_toroku_bango in (select distinct ketto_toroku_bango from base_v3)" in body
    assert "rec.ketto_toroku_bango in (select distinct ketto_toroku_bango from base_v3)" in body


def test_stage_history_default_does_not_require_base_v3() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history(FakeConn(), "20230101", "20231231")
    body = " ".join(captured)
    assert "base_v3" not in body


def test_stage_history_pushes_literal_horses_and_year_partitions() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history(
        FakeConn(),
        "20230101",
        "20241231",
        horse_literals="'horse_a', 'horse_b'",
    )
    body = " ".join(captured)
    assert "ketto_toroku_bango in ('horse_a', 'horse_b')" in body
    assert "rec.ketto_toroku_bango in ('horse_a', 'horse_b')" in body
    assert "kaisai_nen between '2023' and '2024'" in body
    assert "rec.kaisai_nen between '2023' and '2024'" in body


def test_target_horse_literals_escapes_and_handles_empty() -> None:
    class Result:
        rows: list[tuple[str]]

        def __init__(self, rows: list[tuple[str]]) -> None:
            self.rows = rows

        def fetchall(self) -> list[tuple[str]]:
            return self.rows

    class FakeConn:
        rows: list[tuple[str]]

        def __init__(self, rows: list[tuple[str]]) -> None:
            self.rows = rows

        def execute(self, _sql: str) -> Result:
            return Result(self.rows)

    assert subject.target_horse_literals(FakeConn([("a'b",), ("horse_c",)])) == "'a''b', 'horse_c'"
    assert subject.target_horse_literals(FakeConn([])) == "null"
