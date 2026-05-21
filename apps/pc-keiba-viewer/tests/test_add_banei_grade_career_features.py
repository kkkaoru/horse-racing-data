from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-banei-grade-career-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_banei_grade_career_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_banei_grade_career_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_banei_keibajo_constant() -> None:
    assert subject.BAN_EI_KEIBAJO == "83"


def test_grade_rank_sql_orders_p_highest() -> None:
    sql = subject.GRADE_RANK_SQL
    assert "'P' then 6" in sql
    assert "'Q' then 5" in sql
    assert "'E' then 1" in sql


def test_append_features_sql_pivots_six_grades() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    for letter in ("E", "T", "S", "R", "Q", "P"):
        assert f"horse_grade_{letter}_career_starts" in sql
        assert f"horse_grade_{letter}_career_win_rate" in sql


def test_append_features_sql_includes_current_grade_aggregates() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "current_race_grade_letter" in sql
    assert "horse_current_grade_career_win_rate" in sql
    assert "horse_current_grade_career_starts" in sql
    assert "horse_career_starts_minus_field" in sql
    assert "field_avg_career_starts" in sql
