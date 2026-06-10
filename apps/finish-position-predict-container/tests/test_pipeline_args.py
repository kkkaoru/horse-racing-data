"""Tests for the v8 feature-pipeline subprocess argv builders."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.pipeline_args import (
    COURSE_LOOKUP_PATH,
    HISTORY_FROM_DATE,
    RELATIONSHIP_CATEGORY_BY_CATEGORY,
    RELATIONSHIP_SCRIPT,
    SCRIPTS_WITH_PG_URL,
    build_base_argv,
    build_layer_argv,
    layer_chain_for,
)

BUILDER = Path("/app/pipeline/finish_position_features_duckdb.py")
LAYER_DIR = Path("/app/pipeline/finish-position-features")
URL = "postgresql://u:p@h/db"


def test_build_base_argv_uses_target_date_mode_for_jra() -> None:
    argv = build_base_argv(BUILDER, "jra", "20260603", 0, URL, Path("/tmp/base"))
    assert argv == [
        "python",
        "/app/pipeline/finish_position_features_duckdb.py",
        "--category",
        "jra",
        "--target-date",
        "20260603",
        "--days-ahead",
        "0",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--output-dir",
        "/tmp/base",
        "--allow-empty-targets",
    ]


def test_build_base_argv_passes_days_ahead_for_nar() -> None:
    argv = build_base_argv(BUILDER, "nar", "20260603", 2, URL, Path("/tmp/base"))
    assert argv == [
        "python",
        "/app/pipeline/finish_position_features_duckdb.py",
        "--category",
        "nar",
        "--target-date",
        "20260603",
        "--days-ahead",
        "2",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--output-dir",
        "/tmp/base",
        "--allow-empty-targets",
    ]


def test_build_base_argv_includes_allow_empty_targets_flag() -> None:
    argv = build_base_argv(BUILDER, "ban-ei", "20260603", 1, URL, Path("/tmp/base"))
    assert argv[-1] == "--allow-empty-targets"


def test_build_layer_argv_race_internal_has_no_pg_url_or_from_date() -> None:
    argv = build_layer_argv(
        "add-race-internal-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-race-internal-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
    ]


def test_build_layer_argv_market_signal_has_pg_url_and_from_date() -> None:
    argv = build_layer_argv(
        "add-market-signal-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-market-signal-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
    ]


def test_build_layer_argv_near_miss_has_pg_url_and_from_date() -> None:
    argv = build_layer_argv(
        "add-near-miss-features.py",
        "nar",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-near-miss-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
    ]


def test_build_layer_argv_sectional_weight_has_pg_url_and_from_date() -> None:
    argv = build_layer_argv(
        "add-sectional-and-weight-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-4] == "--pg-url"
    assert argv[-3] == "postgresql://u:p@h/db"
    assert argv[-2] == "--from-date"
    assert argv[-1] == HISTORY_FROM_DATE


def test_build_layer_argv_futan_juryo_has_pg_url_and_from_date() -> None:
    argv = build_layer_argv(
        "add-futan-juryo-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-4] == "--pg-url"
    assert argv[-3] == "postgresql://u:p@h/db"
    assert argv[-2] == "--from-date"
    assert argv[-1] == HISTORY_FROM_DATE


def test_build_layer_argv_workout_has_pg_url_and_from_date() -> None:
    argv = build_layer_argv(
        "add-workout-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-4] == "--pg-url"
    assert argv[-3] == "postgresql://u:p@h/db"
    assert argv[-2] == "--from-date"
    assert argv[-1] == HISTORY_FROM_DATE


def test_build_layer_argv_lineage_passes_config_for_jra() -> None:
    argv = build_layer_argv(
        "add-grade-race-lineage-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-grade-race-lineage-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
        "--config",
        "/app/pipeline/finish-position-features/lineage-races/jra.json",
    ]


def test_build_layer_argv_lineage_passes_config_for_nar() -> None:
    argv = build_layer_argv(
        "add-grade-race-lineage-features.py",
        "nar",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-2] == "--config"
    assert argv[-1] == "/app/pipeline/finish-position-features/lineage-races/nar.json"


def test_build_layer_argv_lineage_passes_config_for_ban_ei() -> None:
    argv = build_layer_argv(
        "add-grade-race-lineage-features.py",
        "ban-ei",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-2] == "--config"
    assert argv[-1] == "/app/pipeline/finish-position-features/lineage-races/ban-ei.json"


def test_build_layer_argv_trainer_passes_category_for_jra() -> None:
    argv = build_layer_argv(
        "add-trainer-stable-affinity-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-2] == "--category"
    assert argv[-1] == "jra"


def test_build_layer_argv_trainer_passes_category_for_nar() -> None:
    argv = build_layer_argv(
        "add-trainer-stable-affinity-features.py",
        "nar",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-2] == "--category"
    assert argv[-1] == "nar"


def test_build_layer_argv_h2h_has_pg_url_and_from_date_only() -> None:
    argv = build_layer_argv(
        "add-head-to-head-features.py",
        "nar",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-head-to-head-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
    ]


def test_build_layer_argv_baba_has_pg_url_and_from_date_only() -> None:
    argv = build_layer_argv(
        "add-baba-pedigree-affinity-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-baba-pedigree-affinity-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
    ]


def test_build_layer_argv_banei_futan_class_has_pg_url_and_from_date_only() -> None:
    argv = build_layer_argv(
        "add-banei-futan-class-features.py",
        "ban-ei",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-banei-futan-class-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
    ]


def test_build_layer_argv_banei_grade_career_has_pg_url_and_from_date_only() -> None:
    argv = build_layer_argv(
        "add-banei-grade-career-features.py",
        "ban-ei",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-banei-grade-career-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
    ]


def test_layer_chain_jra_is_full_v6_plus_v7_with_trainer_plus_v8() -> None:
    chain = layer_chain_for("jra")
    assert list(chain) == [
        "add-race-internal-features.py",
        "add-market-signal-features.py",
        "add-sectional-and-weight-features.py",
        "add-futan-juryo-features.py",
        "add-workout-features.py",
        "add-near-miss-features.py",
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        "add-trainer-stable-affinity-features.py",
        "add-pacestyle-features.py",
        "add-course-numerical-features.py",
        "add-relationship-r1-features.py",
    ]


def test_layer_chain_nar_is_light_v6_plus_v7_plus_trainer_plus_pacestyle() -> None:
    chain = layer_chain_for("nar")
    assert list(chain) == [
        "add-race-internal-features.py",
        "add-near-miss-features.py",
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        "add-trainer-stable-affinity-features.py",
        "add-pacestyle-features.py",
        "add-relationship-r1-features.py",
    ]


def test_layer_chain_ban_ei_appends_futan_and_grade_career() -> None:
    chain = layer_chain_for("ban-ei")
    assert list(chain) == [
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
        "add-banei-futan-class-features.py",
        "add-banei-grade-career-features.py",
    ]


def test_build_layer_argv_pacestyle_passes_pg_url_and_category_for_jra() -> None:
    argv = build_layer_argv(
        "add-pacestyle-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-pacestyle-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
        "--category",
        "jra",
    ]


def test_build_layer_argv_pacestyle_passes_category_for_nar() -> None:
    argv = build_layer_argv(
        "add-pacestyle-features.py",
        "nar",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv[-2] == "--category"
    assert argv[-1] == "nar"


def test_build_layer_argv_course_numerical_passes_lookup_path() -> None:
    argv = build_layer_argv(
        "add-course-numerical-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-course-numerical-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--course-lookup",
        str(COURSE_LOOKUP_PATH),
    ]


def test_course_lookup_path_is_baked_image_location() -> None:
    assert Path("/app/lookups/course-numerical-features.parquet") == COURSE_LOOKUP_PATH


def test_build_layer_argv_relationship_passes_pg_url_from_date_and_category_for_jra() -> None:
    argv = build_layer_argv(
        RELATIONSHIP_SCRIPT,
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    # The relationship layer reads PG history, so it must receive ``--pg-url``
    # with the supplied URL (the script has a default pg-url, so a wiring
    # omission would be SILENT and score against the wrong DB), ``--from-date``
    # to bound the scan, and ``--category`` with the mapped source filter.
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-relationship-r1-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--pg-url",
        "postgresql://u:p@h/db",
        "--from-date",
        HISTORY_FROM_DATE,
        "--category",
        "jra",
    ]


def test_build_layer_argv_relationship_passes_category_for_nar() -> None:
    argv = build_layer_argv(
        RELATIONSHIP_SCRIPT,
        "nar",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert "--pg-url" in argv
    assert argv[argv.index("--pg-url") + 1] == URL
    assert argv[-2] == "--category"
    assert argv[-1] == "nar"


def test_build_layer_argv_non_relationship_script_omits_relationship_category() -> None:
    argv = build_layer_argv(
        "add-course-numerical-features.py",
        "jra",
        LAYER_DIR,
        Path("/tmp/in"),
        Path("/tmp/out"),
        URL,
    )
    assert argv == [
        "python",
        "/app/pipeline/finish-position-features/add-course-numerical-features.py",
        "--input-dir",
        "/tmp/in",
        "--output-dir",
        "/tmp/out",
        "--course-lookup",
        str(COURSE_LOOKUP_PATH),
    ]


def test_relationship_script_is_in_scripts_with_pg_url() -> None:
    assert RELATIONSHIP_SCRIPT in SCRIPTS_WITH_PG_URL


def test_relationship_category_map_has_exactly_jra_and_nar_keys() -> None:
    assert set(RELATIONSHIP_CATEGORY_BY_CATEGORY.keys()) == {"jra", "nar"}


# ---------------------------------------------------------------------------
# build_base_argv — realtime-odds optional arg
# ---------------------------------------------------------------------------


def test_build_base_argv_without_realtime_odds_omits_flag() -> None:
    argv = build_base_argv(BUILDER, "nar", "20260610", 0, URL, Path("/tmp/base"))
    assert "--realtime-odds" not in argv


def test_build_base_argv_with_realtime_odds_path_appends_flag() -> None:
    argv = build_base_argv(
        BUILDER, "nar", "20260610", 0, URL, Path("/tmp/base"),
        realtime_odds_path=Path("/tmp/predict-upcoming/realtime-odds-nar.parquet"),
    )
    assert "--realtime-odds" in argv
    assert argv[argv.index("--realtime-odds") + 1] == (
        "/tmp/predict-upcoming/realtime-odds-nar.parquet"
    )


def test_build_base_argv_with_realtime_odds_still_ends_with_allow_empty_targets() -> None:
    argv = build_base_argv(
        BUILDER, "jra", "20260610", 2, URL, Path("/tmp/base"),
        realtime_odds_path=Path("/tmp/predict-upcoming/realtime-odds-jra.parquet"),
    )
    assert "--allow-empty-targets" in argv


def test_build_base_argv_realtime_odds_none_produces_same_argv_as_before() -> None:
    # Passing None explicitly must produce the same result as omitting the arg
    # so existing callers are unaffected.
    argv_implicit = build_base_argv(BUILDER, "jra", "20260610", 0, URL, Path("/tmp/base"))
    argv_explicit_none = build_base_argv(
        BUILDER, "jra", "20260610", 0, URL, Path("/tmp/base"), realtime_odds_path=None
    )
    assert argv_implicit == argv_explicit_none


def test_build_base_argv_with_realtime_odds_for_ban_ei_appends_flag() -> None:
    argv = build_base_argv(
        BUILDER, "ban-ei", "20260610", 0, URL, Path("/tmp/base"),
        realtime_odds_path=Path("/tmp/predict-upcoming/realtime-odds-ban-ei.parquet"),
    )
    assert "--realtime-odds" in argv
    assert argv[argv.index("--realtime-odds") + 1] == (
        "/tmp/predict-upcoming/realtime-odds-ban-ei.parquet"
    )
