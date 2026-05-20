from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-head-to-head-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_head_to_head_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_head_to_head_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_append_features_sql_contains_h2h_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "h2h_encounter_count" in sql
    assert "h2h_win_count_vs_field" in sql
    assert "h2h_loss_count_vs_field" in sql
    assert "h2h_win_rate_vs_field" in sql
    assert "h2h_avg_finish_diff_vs_field" in sql
    assert "h2h_unique_rivals_count" in sql


def test_append_features_sql_left_joins_summary() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join h2h_horse_summary" in sql
    assert "s.self_horse = b.ketto_toroku_bango" in sql


def test_race_partition_constant() -> None:
    assert subject.RACE_PARTITION == "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
