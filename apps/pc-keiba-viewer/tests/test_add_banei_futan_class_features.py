from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-banei-futan-class-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_banei_futan_class_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_banei_futan_class_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_banei_keibajo_constant() -> None:
    assert subject.BAN_EI_KEIBAJO == "83"


def test_futan_hex_parse_includes_hex_prefix() -> None:
    assert "'0x'" in subject.FUTAN_HEX_PARSE
    assert "as integer" in subject.FUTAN_HEX_PARSE


def test_futan_bucket_sql_has_7_buckets() -> None:
    sql = subject.FUTAN_BUCKET_SQL
    # Buckets 0..5 use `then <n>`; bucket 6 is `else 6` (catch-all)
    for bucket_val in ("0", "1", "2", "3", "4", "5"):
        assert f"then {bucket_val}" in sql, f"missing bucket {bucket_val}"
    assert "else 6" in sql, "missing catch-all bucket 6"


def test_append_features_sql_contains_futan_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "current_futan_class" in sql
    assert "horse_futan_class_career_starts" in sql
    assert "horse_futan_class_career_win_rate" in sql
    assert "horse_futan_class_career_top3_rate" in sql
    assert "sire_futan_class_win_rate" in sql
    assert "damsire_futan_class_win_rate" in sql
    assert "field_futan_class_avg" in sql
    assert "self_futan_minus_field_avg" in sql
