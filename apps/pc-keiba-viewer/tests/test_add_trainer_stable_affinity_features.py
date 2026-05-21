from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-trainer-stable-affinity-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_trainer_stable_affinity_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_trainer_stable_affinity_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.category == "jra"


def test_parse_args_supports_nar_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out"), "--category", "nar"]
    )
    assert args.category == "nar"


def test_append_features_sql_jra_uses_jvd_se() -> None:
    sql = subject.append_features_sql("dummy.parquet", "jra")
    assert "pg.jvd_se" in sql
    assert "pg.nvd_se" not in sql


def test_append_features_sql_nar_uses_nvd_se() -> None:
    sql = subject.append_features_sql("dummy.parquet", "nar")
    assert "pg.nvd_se" in sql
    assert "pg.jvd_se" not in sql


def test_append_features_sql_contains_trainer_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "trainer_grade_career_starts" in sql
    assert "trainer_grade_win_rate" in sql
    assert "trainer_grade_top3_rate" in sql
    assert "trainer_target_race_career_count" in sql
    assert "trainer_target_race_win_count" in sql
    assert "trainer_target_race_top3_count" in sql
    assert "trainer_target_race_has_history" in sql


def test_append_features_sql_joins_grade_and_target_cumul() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join trainer_grade_cumul" in sql
    assert "left join trainer_target_cumul" in sql
    assert "tg.chokyoshi_code = bwt.chokyoshi_code" in sql
    assert "tg.grade_code = bwt.grade_code" in sql
