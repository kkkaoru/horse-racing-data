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
