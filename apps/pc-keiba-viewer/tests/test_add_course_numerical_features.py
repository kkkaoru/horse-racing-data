from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-course-numerical-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_course_numerical_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_course_numerical_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output_and_lookup(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--course-lookup",
            str(tmp_path / "lookup.parquet"),
        ]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.course_lookup == tmp_path / "lookup.parquet"


def test_course_feature_names_count_is_seven() -> None:
    assert len(subject.COURSE_FEATURE_NAMES) == 7


def test_course_feature_names_exact_set() -> None:
    assert subject.COURSE_FEATURE_NAMES == (
        "course_elevation_diff_m",
        "course_final_straight_m",
        "course_dist_to_first_corner_m",
        "course_corner_count",
        "course_full_gate_count",
        "course_good_track_nige_rentai_rate_pct",
        "course_heavy_track_nige_rentai_rate_pct",
    )


def test_join_keys_are_keibajo_kyori_track() -> None:
    assert subject.JOIN_KEYS == ("keibajo_code", "kyori", "track_code")


def test_append_features_sql_emits_all_seven_course_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "course_elevation_diff_m" in sql
    assert "course_final_straight_m" in sql
    assert "course_dist_to_first_corner_m" in sql
    assert "course_corner_count" in sql
    assert "course_full_gate_count" in sql
    assert "course_good_track_nige_rentai_rate_pct" in sql
    assert "course_heavy_track_nige_rentai_rate_pct" in sql


def test_append_features_sql_left_joins_lookup_on_typed_keys() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join lookup_typed lt" in sql
    assert "lt.join_keibajo_code = bt.join_keibajo_code" in sql
    assert "lt.join_kyori = bt.join_kyori" in sql
    assert "lt.join_track_code = bt.join_track_code" in sql


def test_append_features_sql_casts_keys_to_canonical_types() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "cast(b.keibajo_code as varchar)" in sql
    assert "cast(b.kyori as integer)" in sql
    assert "cast(b.track_code as varchar)" in sql


def test_stage_course_lookup_does_not_use_ignore_nulls_modifier(tmp_path: Path) -> None:
    """Regression: DuckDB rejects ``any_value(... ignore nulls)`` as a parser
    error because RESPECT/IGNORE NULLS is only valid on window functions.
    The stage_course_lookup SQL must therefore not emit that modifier."""
    lookup = tmp_path / "lookup.parquet"
    table = pa.table(
        {
            "keibajo_code": pa.array(["01", "01"], type=pa.string()),
            "kyori": pa.array([1200, 1200], type=pa.int32()),
            "track_code": pa.array(["11", "11"], type=pa.string()),
            "course_elevation_diff_m": pa.array([None, 1.5], type=pa.float64()),
            "course_final_straight_m": pa.array([300.0, None], type=pa.float64()),
            "course_dist_to_first_corner_m": pa.array([None, 250.0], type=pa.float64()),
            "course_corner_count": pa.array([4, None], type=pa.int32()),
            "course_full_gate_count": pa.array([None, 16], type=pa.int32()),
            "course_good_track_nige_rentai_rate_pct": pa.array(
                [None, 12.5], type=pa.float64()
            ),
            "course_heavy_track_nige_rentai_rate_pct": pa.array(
                [8.25, None], type=pa.float64()
            ),
        }
    )
    pq.write_table(table, lookup)
    con = duckdb.connect(":memory:")
    subject.stage_course_lookup(con, lookup)
    rows = con.execute(
        "select keibajo_code, kyori, track_code, course_elevation_diff_m, "
        "course_final_straight_m, course_dist_to_first_corner_m, course_corner_count, "
        "course_full_gate_count, course_good_track_nige_rentai_rate_pct, "
        "course_heavy_track_nige_rentai_rate_pct from course_lookup"
    ).fetchall()
    con.close()
    assert rows == [("01", 1200, "11", 1.5, 300.0, 250.0, 4, 16, 12.5, 8.25)]
