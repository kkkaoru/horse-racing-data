from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-pacestyle-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_pacestyle_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_pacestyle_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output_and_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "jra",
        ]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.category == "jra"


def test_parse_args_accepts_nar_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "nar",
        ]
    )
    assert args.category == "nar"


def test_parse_args_rejects_invalid_category(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--input-dir",
                str(tmp_path / "in"),
                "--output-dir",
                str(tmp_path / "out"),
                "--category",
                "ban-ei",
            ]
        )


def test_build_version_filter_sql_jra_includes_all_known_years() -> None:
    sql = subject.build_version_filter_sql("jra")
    assert "kaisai_nen = '2024'" in sql
    assert "kaisai_nen = '2025'" in sql
    assert "kaisai_nen = '2026'" in sql
    assert "jra-running-style-ens-lgbm-trans-v1.3" in sql
    assert "jra-running-style-lgbm-prod-v1.5" in sql


def test_build_version_filter_sql_nar_uses_nar_model_versions() -> None:
    sql = subject.build_version_filter_sql("nar")
    assert "nar-running-style-trans-v1.4" in sql
    assert "nar-running-style-lgbm-prod-v1.5" in sql


def test_build_version_filter_sql_unknown_category_returns_false() -> None:
    sql = subject.build_version_filter_sql("ban-ei")
    assert sql == "false"


def test_append_features_sql_emits_all_ten_pacestyle_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet", "jra")
    assert "past_style_x_field_pace_match" in sql
    assert "sire_x_field_pace_score" in sql
    assert "rs_p_nige" in sql
    assert "rs_p_senkou" in sql
    assert "rs_p_sashi" in sql
    assert "rs_p_oikomi" in sql
    assert "rs_predicted_class" in sql
    assert "rs_confidence_entropy" in sql
    assert "rs_p_nige_x_field_pace" in sql
    assert "rs_sire_style_match" in sql


def test_append_features_sql_left_joins_rs_preds_by_race_id() -> None:
    sql = subject.append_features_sql("dummy.parquet", "nar")
    assert "left join rs_preds" in sql
    assert "rs.race_id" in sql
    assert "rs.ketto_toroku_bango = b.ketto_toroku_bango" in sql


def test_append_features_sql_race_id_prefix_matches_category() -> None:
    jra_sql = subject.append_features_sql("dummy.parquet", "jra")
    nar_sql = subject.append_features_sql("dummy.parquet", "nar")
    assert "'jra:' || b.kaisai_nen" in jra_sql
    assert "'nar:' || b.kaisai_nen" in nar_sql


def test_rs_version_pref_keys_cover_both_categories() -> None:
    assert "jra" in subject.RS_VERSION_PREF
    assert "nar" in subject.RS_VERSION_PREF
    assert 2026 in subject.RS_VERSION_PREF["jra"]
    assert 2026 in subject.RS_VERSION_PREF["nar"]
