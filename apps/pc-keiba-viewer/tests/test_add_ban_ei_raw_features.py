from __future__ import annotations

import importlib.util
import inspect
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-ban-ei-raw-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_ban_ei_raw_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_ban_ei_raw_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_ban_ei_keibajo_constant() -> None:
    assert subject.BAN_EI_KEIBAJO == "83"


def test_hex_weight_missing_sentinel_constant() -> None:
    assert subject.HEX_WEIGHT_MISSING_SENTINEL == "FFF"


def test_hex_kg_sql_uses_hex_prefix_and_integer_cast() -> None:
    sql = subject.hex_kg_sql("bataiju")
    assert "'0x'" in sql
    assert "as integer" in sql


def test_hex_kg_sql_nulls_missing_sentinel() -> None:
    sql = subject.hex_kg_sql("bataiju")
    assert "'FFF'" in sql


def test_hex_kg_sql_references_requested_column() -> None:
    sql = subject.hex_kg_sql("futan_juryo")
    assert "futan_juryo" in sql


def test_stage_nvd_se_source_uses_hex_decode_not_decimal() -> None:
    src = inspect.getsource(subject.stage_nvd_se)
    assert "hex_kg_sql" in src
    assert "trim(futan_juryo), '') as double" not in src
    assert "trim(bataiju), '') as int" not in src


def test_hex_decode_bataiju_values_to_kg() -> None:
    con = duckdb.connect()
    sql = f"select {subject.hex_kg_sql('w')} as kg from (values ('3E8')) as t(w)"
    row = con.execute(sql).fetchone()
    assert row is not None
    assert row[0] == 1000


def test_hex_decode_bataiju_digit_only_hex_is_not_decimal() -> None:
    con = duckdb.connect()
    sql = f"select {subject.hex_kg_sql('w')} as kg from (values ('410')) as t(w)"
    row = con.execute(sql).fetchone()
    assert row is not None
    assert row[0] == 1040


def test_hex_decode_futan_value_to_kg() -> None:
    con = duckdb.connect()
    sql = f"select {subject.hex_kg_sql('w')} as kg from (values ('26C')) as t(w)"
    row = con.execute(sql).fetchone()
    assert row is not None
    assert row[0] == 620


def test_hex_decode_missing_sentinel_is_null() -> None:
    con = duckdb.connect()
    sql = f"select {subject.hex_kg_sql('w')} as kg from (values ('FFF')) as t(w)"
    row = con.execute(sql).fetchone()
    assert row is not None
    assert row[0] is None


def test_hex_decode_empty_string_is_null() -> None:
    con = duckdb.connect()
    sql = f"select {subject.hex_kg_sql('w')} as kg from (values ('')) as t(w)"
    row = con.execute(sql).fetchone()
    assert row is not None
    assert row[0] is None


def test_append_features_sql_contains_weight_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "futan_juryo_bucket" in sql
    assert "bataiju_diff_from_avg5" in sql
