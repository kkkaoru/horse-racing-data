from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-baba-pedigree-affinity-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_baba_pedigree_affinity_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_baba_pedigree_affinity_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_append_features_sql_contains_baba_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "current_baba_condition" in sql
    assert "horse_baba_win_rate" in sql
    assert "horse_baba_career_starts" in sql
    assert "sire_baba_win_rate" in sql
    assert "damsire_baba_win_rate" in sql
    assert "sire_horse_baba_combined_score" in sql


def test_append_features_sql_joins_pedigree_and_baba() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join horse_pedigree" in sql
    assert "left join horse_baba_cumul" in sql
    assert "left join sire_baba_cumul" in sql
    assert "left join damsire_baba_cumul" in sql
