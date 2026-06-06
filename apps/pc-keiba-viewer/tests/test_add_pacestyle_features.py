from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

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


def test_parse_args_rs_source_default_is_auto(tmp_path: Path) -> None:
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
    assert args.rs_source == "auto"


def test_parse_args_rs_source_accepts_r2(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "jra",
            "--rs-source",
            "r2",
        ]
    )
    assert args.rs_source == "r2"


def test_parse_args_rs_source_accepts_pg(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "jra",
            "--rs-source",
            "pg",
        ]
    )
    assert args.rs_source == "pg"


def test_parse_args_rs_source_rejects_invalid_value(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--input-dir",
                str(tmp_path / "in"),
                "--output-dir",
                str(tmp_path / "out"),
                "--category",
                "jra",
                "--rs-source",
                "neon",
            ]
        )


def test_parse_args_run_date_default_is_none(tmp_path: Path) -> None:
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
    assert args.run_date is None


def test_parse_args_run_date_accepts_yyyymmdd(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "nar",
            "--run-date",
            "20260607",
        ]
    )
    assert args.run_date == "20260607"


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


def test_rs_source_choices_constants_are_exposed() -> None:
    assert subject.RS_SOURCE_CHOICES == ("r2", "pg", "auto")
    assert subject.R2_BUCKET_DEFAULT == "pc-keiba-features-archive"
    assert subject.R2_PREDICTIONS_PREFIX == "running-style/predictions/by-day"


def test_setup_r2_duckdb_secret_installs_and_loads_httpfs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("R2_ACCOUNT_ID", "acc123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key456")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "sec789")
    con = MagicMock()
    subject.setup_r2_duckdb_secret(con)
    first_call_sql = con.execute.call_args_list[0].args[0]
    assert first_call_sql == "install httpfs; load httpfs;"


def test_setup_r2_duckdb_secret_uses_endpoint_template(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("R2_ACCOUNT_ID", "acc123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key456")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "sec789")
    con = MagicMock()
    subject.setup_r2_duckdb_secret(con)
    secret_sql = con.execute.call_args_list[1].args[0]
    assert "ENDPOINT 'acc123.r2.cloudflarestorage.com'" in secret_sql
    assert "KEY_ID 'key456'" in secret_sql
    assert "SECRET 'sec789'" in secret_sql
    assert "REGION 'auto'" in secret_sql
    assert "URL_STYLE 'path'" in secret_sql
    assert "TYPE S3" in secret_sql
    assert "create or replace secret r2_secret" in secret_sql


def test_setup_r2_duckdb_secret_raises_when_account_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key456")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "sec789")
    con = MagicMock()
    with pytest.raises(KeyError):
        subject.setup_r2_duckdb_secret(con)


def test_stage_rs_predictions_from_r2_builds_glob_for_jra() -> None:
    con = MagicMock()
    subject.stage_rs_predictions_from_r2(con, "jra", "20260607", "pc-keiba-features-archive")
    create_sql = con.execute.call_args_list[0].args[0]
    assert (
        "s3://pc-keiba-features-archive/running-style/predictions/by-day/"
        "2026/06/07/jra/*.parquet"
    ) in create_sql
    assert "'jra:' || kaisai_nen || ':' || kaisai_tsukihi" in create_sql


def test_stage_rs_predictions_from_r2_builds_glob_for_nar_custom_bucket() -> None:
    con = MagicMock()
    subject.stage_rs_predictions_from_r2(con, "nar", "20240115", "other-bucket")
    create_sql = con.execute.call_args_list[0].args[0]
    assert (
        "s3://other-bucket/running-style/predictions/by-day/2024/01/15/nar/*.parquet"
    ) in create_sql
    assert "'nar:' || kaisai_nen || ':' || kaisai_tsukihi" in create_sql


def test_stage_rs_predictions_from_r2_creates_index() -> None:
    con = MagicMock()
    subject.stage_rs_predictions_from_r2(con, "jra", "20260607", "pc-keiba-features-archive")
    index_sql = con.execute.call_args_list[1].args[0]
    assert index_sql == "create index rs_preds_idx on rs_preds (race_id, ketto_toroku_bango)"


def test_stage_rs_predictions_from_pg_uses_pg_attach_table() -> None:
    con = MagicMock()
    subject.stage_rs_predictions_from_pg(con, "jra")
    create_sql = con.execute.call_args_list[0].args[0]
    assert "from pg.race_running_style_model_predictions" in create_sql
    assert "where source = 'jra'" in create_sql


def test_stage_rs_predictions_pg_mode_skips_r2_and_attaches_pg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_calls: list[str] = []
    con = MagicMock()

    def fake_install(c: object, url: str) -> None:
        install_calls.append(url)

    def fake_r2_setup(c: object) -> None:
        install_calls.append("R2_SETUP_CALLED")

    monkeypatch.setattr(subject, "install_and_attach_pg", fake_install)
    monkeypatch.setattr(subject, "setup_r2_duckdb_secret", fake_r2_setup)
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_pg",
        lambda c, cat: install_calls.append(f"pg:{cat}"),
    )
    args = argparse.Namespace(
        rs_source="pg",
        run_date=None,
        category="jra",
        pg_url="postgresql://x",
    )
    subject.stage_rs_predictions(con, args)
    assert install_calls == ["postgresql://x", "pg:jra"]


def test_stage_rs_predictions_r2_mode_calls_setup_and_from_r2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("R2_BUCKET", raising=False)
    order: list[str] = []
    con = MagicMock()
    monkeypatch.setattr(
        subject,
        "setup_r2_duckdb_secret",
        lambda c: order.append("setup"),
    )
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_r2",
        lambda c, cat, rd, bk: order.append(f"r2:{cat}:{rd}:{bk}"),
    )
    monkeypatch.setattr(
        subject,
        "install_and_attach_pg",
        lambda c, url: order.append("pg_attach"),
    )
    args = argparse.Namespace(
        rs_source="r2",
        run_date="20260607",
        category="nar",
        pg_url="postgresql://x",
    )
    subject.stage_rs_predictions(con, args)
    assert order == ["setup", "r2:nar:20260607:pc-keiba-features-archive"]


def test_stage_rs_predictions_r2_mode_uses_custom_bucket_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("R2_BUCKET", "alt-bucket")
    captured: list[str] = []
    con = MagicMock()
    monkeypatch.setattr(subject, "setup_r2_duckdb_secret", lambda c: None)
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_r2",
        lambda c, cat, rd, bk: captured.append(bk),
    )
    args = argparse.Namespace(
        rs_source="r2",
        run_date="20260607",
        category="jra",
        pg_url="postgresql://x",
    )
    subject.stage_rs_predictions(con, args)
    assert captured == ["alt-bucket"]


def test_stage_rs_predictions_r2_mode_raises_when_run_date_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    con = MagicMock()
    monkeypatch.setattr(
        subject,
        "setup_r2_duckdb_secret",
        lambda c: None,
    )
    args = argparse.Namespace(
        rs_source="r2",
        run_date=None,
        category="jra",
        pg_url="postgresql://x",
    )
    with pytest.raises(ValueError):
        subject.stage_rs_predictions(con, args)


def test_stage_rs_predictions_r2_strict_propagates_setup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    con = MagicMock()

    def boom(c: object) -> None:
        raise KeyError("R2_ACCOUNT_ID")

    monkeypatch.setattr(subject, "setup_r2_duckdb_secret", boom)
    monkeypatch.setattr(
        subject,
        "install_and_attach_pg",
        lambda c, url: pytest.fail("PG must NOT be touched in r2 strict mode"),
    )
    args = argparse.Namespace(
        rs_source="r2",
        run_date="20260607",
        category="jra",
        pg_url="postgresql://x",
    )
    with pytest.raises(KeyError):
        subject.stage_rs_predictions(con, args)


def test_stage_rs_predictions_auto_falls_back_to_pg_when_setup_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    con = MagicMock()

    def boom(c: object) -> None:
        raise KeyError("R2_ACCOUNT_ID")

    monkeypatch.setattr(subject, "setup_r2_duckdb_secret", boom)
    monkeypatch.setattr(
        subject,
        "install_and_attach_pg",
        lambda c, url: order.append(f"pg_attach:{url}"),
    )
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_pg",
        lambda c, cat: order.append(f"pg:{cat}"),
    )
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_r2",
        lambda c, cat, rd, bk: pytest.fail("r2 loader must NOT run after setup raised"),
    )
    args = argparse.Namespace(
        rs_source="auto",
        run_date="20260607",
        category="jra",
        pg_url="postgresql://x",
    )
    subject.stage_rs_predictions(con, args)
    assert order == ["pg_attach:postgresql://x", "pg:jra"]


def test_stage_rs_predictions_auto_falls_back_to_pg_when_run_date_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    con = MagicMock()
    monkeypatch.setattr(
        subject,
        "setup_r2_duckdb_secret",
        lambda c: pytest.fail("setup must NOT run when run_date is missing"),
    )
    monkeypatch.setattr(
        subject,
        "install_and_attach_pg",
        lambda c, url: order.append("pg_attach"),
    )
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_pg",
        lambda c, cat: order.append(f"pg:{cat}"),
    )
    args = argparse.Namespace(
        rs_source="auto",
        run_date=None,
        category="nar",
        pg_url="postgresql://x",
    )
    subject.stage_rs_predictions(con, args)
    assert order == ["pg_attach", "pg:nar"]


def test_stage_rs_predictions_auto_uses_r2_when_setup_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    con = MagicMock()
    monkeypatch.setattr(subject, "setup_r2_duckdb_secret", lambda c: order.append("setup"))
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_r2",
        lambda c, cat, rd, bk: order.append(f"r2:{cat}:{rd}"),
    )
    monkeypatch.setattr(
        subject,
        "install_and_attach_pg",
        lambda c, url: pytest.fail("PG must NOT be touched when R2 succeeds"),
    )
    args = argparse.Namespace(
        rs_source="auto",
        run_date="20260607",
        category="jra",
        pg_url="postgresql://x",
    )
    subject.stage_rs_predictions(con, args)
    assert order == ["setup", "r2:jra:20260607"]


def test_install_and_attach_pg_runs_install_load_attach_in_order() -> None:
    con = MagicMock()
    subject.install_and_attach_pg(con, "postgresql://user:pass@host:5432/db")
    first_sql = con.execute.call_args_list[0].args[0]
    second_sql = con.execute.call_args_list[1].args[0]
    third_sql = con.execute.call_args_list[2].args[0]
    assert first_sql == "install postgres"
    assert second_sql == "load postgres"
    assert third_sql == (
        "attach 'postgresql://user:pass@host:5432/db' as pg (type postgres, read_only)"
    )


def test_write_partitioned_removes_existing_output_dir(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    (out / "stale.parquet").write_text("stale")
    con = MagicMock()
    subject.write_partitioned(con, "select 1 as race_year", out)
    assert out.exists()
    assert not (out / "stale.parquet").exists()
    copy_sql = con.execute.call_args_list[0].args[0]
    assert "copy (select 1 as race_year) to '" in copy_sql
    assert "partition_by (race_year)" in copy_sql


def test_write_partitioned_creates_dir_when_absent(tmp_path: Path) -> None:
    out = tmp_path / "missing"
    con = MagicMock()
    subject.write_partitioned(con, "select 1 as race_year", out)
    assert out.exists()
    assert con.execute.call_count == 1


def test_main_invokes_stage_rs_predictions_with_args(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_con = MagicMock()
    monkeypatch.setattr(subject.duckdb, "connect", lambda _path: fake_con)
    monkeypatch.setattr(subject, "apply_to_connection", lambda c, t, m: None)
    seen: dict[str, object] = {}

    def fake_stage(c: object, a: argparse.Namespace) -> None:
        seen["category"] = a.category
        seen["rs_source"] = a.rs_source
        seen["run_date"] = a.run_date

    monkeypatch.setattr(subject, "stage_rs_predictions", fake_stage)
    monkeypatch.setattr(
        subject,
        "write_partitioned",
        lambda c, sql, out: seen.update({"wrote": True}),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add-pacestyle-features.py",
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "jra",
            "--rs-source",
            "auto",
            "--run-date",
            "20260607",
        ],
    )
    subject.main()
    assert seen["category"] == "jra"
    assert seen["rs_source"] == "auto"
    assert seen["run_date"] == "20260607"
    assert seen["wrote"] is True
