from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-sire-venue-bias-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_sire_venue_bias_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_sire_venue_bias_features"] = subject
_spec.loader.exec_module(subject)


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)


def test_parse_args_requires_input_output_with_default_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.category == "jra"


def test_parse_args_accepts_ban_ei_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "ban-ei",
        ]
    )
    assert args.category == "ban-ei"


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "44:08",
        ]
    )
    assert args.target_race == "44:08"


def test_category_predicates_jra_uses_source_jra_and_no_keibajo_filter() -> None:
    assert subject._category_predicates("jra") == ("jra", "true")


def test_category_predicates_nar_excludes_ban_ei_keibajo() -> None:
    assert subject._category_predicates("nar") == (
        "nar",
        "(h.keibajo_code is null or h.keibajo_code <> '83')",
    )


def test_category_predicates_ban_ei_keeps_only_keibajo_83() -> None:
    assert subject._category_predicates("ban-ei") == ("nar", "h.keibajo_code = '83'")


def test_surface_sql_maps_turf_dirt_other() -> None:
    sql = subject._surface_sql("h.track_code")
    assert "when h.track_code like '1%' then 'turf'" in sql
    assert "when h.track_code like '2%' then 'dirt'" in sql
    assert "else 'other' end" in sql


def test_sire_history_focus_filter_sql_false_is_empty() -> None:
    assert subject.sire_history_focus_filter_sql(False) == ""


def test_sire_history_focus_filter_sql_true_uses_target_sires() -> None:
    sql = subject.sire_history_focus_filter_sql(True)
    assert "target_sires" in sql
    assert "ts.sire_id = p.sire_id" in sql


def test_stage_target_sires_reads_input_parquet_and_pedigree() -> None:
    conn = FakeConn()
    subject.stage_target_sires(conn, "/tmp/in/race_year=*/*.parquet")
    body = " ".join(conn.statements)
    assert "create or replace temp table target_sires" in body
    assert "read_parquet('/tmp/in/race_year=*/*.parquet'" in body
    assert "join horse_pedigree hp" in body


def test_stage_sire_race_history_focused_filters_to_target_sires() -> None:
    conn = FakeConn()
    subject.stage_sire_race_history(conn, "20200101", "jra", focused_target=True)
    body = " ".join(conn.statements)
    assert "target_sires" in body
    assert "h.race_date >= '20200101'" in body


def test_append_features_sql_contains_all_five_feature_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "sire_venue_surface_dist_win_rate" in sql
    assert "sire_venue_surface_dist_place_rate" in sql
    assert "sire_venue_surface_dist_runs" in sql
    assert "sire_venue_surface_win_rate" in sql
    assert "sire_venue_surface_place_rate" in sql


def test_append_features_sql_joins_pedigree_and_cumul_tables() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join horse_pedigree" in sql
    assert "left join sire_svsd_cumul" in sql
    assert "left join sire_svs_cumul" in sql
