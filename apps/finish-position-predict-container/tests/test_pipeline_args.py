"""Tests for the v7-lineage feature-pipeline subprocess argv builders."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.pipeline_args import (
    HISTORY_FROM_DATE,
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


def test_layer_chain_jra_is_full_v6_plus_v7_with_trainer() -> None:
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
    ]


def test_layer_chain_nar_is_light_v6_plus_v7_without_trainer() -> None:
    chain = layer_chain_for("nar")
    assert list(chain) == [
        "add-race-internal-features.py",
        "add-near-miss-features.py",
        "add-grade-race-lineage-features.py",
        "add-head-to-head-features.py",
        "add-baba-pedigree-affinity-features.py",
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
