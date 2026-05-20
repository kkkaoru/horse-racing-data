from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-grade-race-lineage-features.py"
JRA_CONFIG = SCRIPTS_DIR / "lineage-races" / "jra.json"

# scripts dir 内の _resource_defaults を import 解決するため sys.path に追加
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_grade_race_lineage_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_grade_race_lineage_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output_and_config(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--config",
            str(JRA_CONFIG),
        ]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.config == JRA_CONFIG


def test_load_jra_config_has_target_races() -> None:
    cfg = subject.load_config(JRA_CONFIG)
    assert cfg["category"] == "jra"
    assert isinstance(cfg["target_races"], list)
    assert len(cfg["target_races"]) >= 20


def test_load_config_rejects_missing_target_races(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"version": 1}))
    try:
        subject.load_config(bad)
    except ValueError as e:
        assert "target_races" in str(e)
        return
    raise AssertionError("expected ValueError")


def test_build_target_classify_sql_contains_all_target_ids() -> None:
    cfg = subject.load_config(JRA_CONFIG)
    sql = subject.build_target_classify_sql(cfg)
    assert sql.startswith("case ")
    assert sql.endswith("end")
    for tr in cfg["target_races"]:
        assert f"'{tr['id']}'" in sql, f"missing target id {tr['id']}"


def test_build_target_classify_sql_uses_normalized_kyosomei() -> None:
    cfg = {
        "target_races": [
            {
                "id": "test_g1",
                "match": {"kyosomei_equals": "テストG1", "keibajo_code": "05", "kyori": 2000},
            }
        ]
    }
    sql = subject.build_target_classify_sql(cfg)
    assert "kyosomei_norm = 'テストG1'" in sql
    assert "keibajo_code = '05'" in sql
    assert "kyori_int = 2000" in sql


def test_build_target_classify_sql_supports_kyosomei_contains() -> None:
    cfg = {
        "target_races": [
            {
                "id": "test_g2",
                "match": {"kyosomei_contains": "天皇賞（春）", "kyori": 3200},
            }
        ]
    }
    sql = subject.build_target_classify_sql(cfg)
    assert "kyosomei_norm like '%天皇賞（春）%'" in sql


def test_build_target_classify_sql_escapes_quotes() -> None:
    cfg = {
        "target_races": [
            {
                "id": "with'quote",
                "match": {"kyosomei_equals": "Tom's Race"},
            }
        ]
    }
    sql = subject.build_target_classify_sql(cfg)
    assert "with''quote" in sql
    assert "Tom''s Race" in sql


def test_build_target_classify_sql_raises_when_no_branches() -> None:
    try:
        subject.build_target_classify_sql({"target_races": [{"id": "x", "match": {}}]})
    except ValueError as e:
        assert "No target race classifications" in str(e)
        return
    raise AssertionError("expected ValueError when no branches")


def test_build_trial_defs_values_includes_lookback_and_type() -> None:
    cfg = {
        "target_races": [
            {
                "id": "race_a",
                "match": {"kyosomei_equals": "Race A"},
                "trials": [
                    {"name": "trial 1", "match": {"kyosomei_equals": "Trial One"}, "lookback_days": 45},
                    {"name": "trial 2", "match": {"kyosomei_contains": "Trial"}, "lookback_days": 90},
                ],
            }
        ]
    }
    values = subject.build_trial_defs_values(cfg)
    assert "'race_a'" in values
    assert "'trial 1'" in values
    assert "'equals'" in values
    assert "'Trial One'" in values
    assert "45" in values
    assert "'contains'" in values
    assert "90" in values


def test_build_trial_defs_values_skips_trials_without_match() -> None:
    cfg = {
        "target_races": [
            {
                "id": "race_a",
                "match": {"kyosomei_equals": "A"},
                "trials": [
                    {"name": "no_match_trial", "match": {}, "lookback_days": 30},
                    {"name": "ok_trial", "match": {"kyosomei_equals": "OK"}, "lookback_days": 30},
                ],
            }
        ]
    }
    values = subject.build_trial_defs_values(cfg)
    assert "ok_trial" in values
    assert "no_match_trial" not in values


def test_build_trial_defs_values_raises_when_empty() -> None:
    try:
        subject.build_trial_defs_values({"target_races": [{"id": "x", "match": {}, "trials": []}]})
    except ValueError as e:
        assert "trial definitions" in str(e)
        return
    raise AssertionError("expected ValueError when no trial defs")


def test_append_features_sql_includes_target_grade_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "target_race_id" in sql
    assert "target_grade_trial_count" in sql
    assert "target_grade_trial_top1_count" in sql
    assert "target_grade_trial_top3_count" in sql
    assert "target_grade_trial_best_finish" in sql
    assert "target_grade_trial_avg_top2_margin_decisec" in sql
    assert "target_grade_has_trial_history" in sql


def test_jra_json_targets_match_g1_count() -> None:
    cfg = subject.load_config(JRA_CONFIG)
    ids = {tr["id"] for tr in cfg["target_races"]}
    expected_min = {
        "tokyo_yushun_derby",
        "yushun_himba_oaks",
        "satsuki_sho",
        "kikkasho",
        "arima_kinen",
        "japan_cup",
        "ouka_sho",
        "tenno_sho_spring",
        "tenno_sho_autumn",
        "takarazuka_kinen",
    }
    missing = expected_min - ids
    assert not missing, f"missing key G1 races: {missing}"
