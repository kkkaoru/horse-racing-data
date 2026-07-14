from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import polars as pl
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


def test_parse_args_rejects_removed_rs_source_option(tmp_path: Path) -> None:
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
                "auto",
            ]
        )


def test_parse_args_accepts_raw_catalog_prediction_run_date(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "jra",
            "--run-date",
            "20260715",
        ]
    )
    assert args.run_date == "20260715"


def test_parse_args_target_race_accepts_focused_scope(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "jra",
            "--target-race",
            "05:11",
        ]
    )
    assert args.target_race == "05:11"


def test_build_version_filter_sql_jra_includes_all_known_years() -> None:
    sql = subject.build_version_filter_sql("jra")
    assert "kaisai_nen = '2024'" in sql
    assert "kaisai_nen = '2025'" in sql
    assert "kaisai_nen = '2026'" in sql
    assert "jra-running-style-ens-lgbm-trans-v1.3" in sql
    assert "jra-running-style-lgbm-prod-v3" in sql


def test_build_version_filter_sql_nar_uses_nar_model_versions() -> None:
    sql = subject.build_version_filter_sql("nar")
    assert "nar-running-style-trans-v1.4" in sql
    assert "nar-running-style-lgbm-prod-v3" in sql


def test_build_version_filter_sql_unknown_category_returns_false() -> None:
    sql = subject.build_version_filter_sql("ban-ei")
    assert sql == "false"


def test_build_version_filter_sql_from_pairs_empty_returns_false() -> None:
    assert subject.build_version_filter_sql_from_pairs({}) == "false"


def test_build_version_filter_sql_from_pairs_builds_or_clauses() -> None:
    sql = subject.build_version_filter_sql_from_pairs(
        {2026: "jra-running-style-lgbm-prod-v3"}
    )
    assert (
        sql
        == "(kaisai_nen = '2026' and model_version = 'jra-running-style-lgbm-prod-v3')"
    )


def test_preferred_version_has_rows_true_when_row_found() -> None:
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (1,)
    result = subject._preferred_version_has_rows(
        con, "jra", 2026, "jra-running-style-lgbm-prod-v3"
    )
    assert result is True
    sql = con.execute.call_args_list[0].args[0]
    assert "model_version = 'jra-running-style-lgbm-prod-v3'" in sql
    assert "kaisai_nen = '2026'" in sql
    assert "source = 'jra'" in sql


def test_preferred_version_has_rows_false_when_no_row() -> None:
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = None
    result = subject._preferred_version_has_rows(
        con, "nar", 2026, "nar-running-style-lgbm-prod-v1.5"
    )
    assert result is False


def test_latest_available_model_version_returns_top_row() -> None:
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = ("jra-running-style-lgbm-prod-v3",)
    result = subject._latest_available_model_version(con, "jra", 2026)
    assert result == "jra-running-style-lgbm-prod-v3"
    sql = con.execute.call_args_list[0].args[0]
    assert "group by model_version" in sql
    assert "order by max(kaisai_tsukihi) desc, model_version desc" in sql


def test_latest_available_model_version_returns_none_when_no_rows() -> None:
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = None
    result = subject._latest_available_model_version(con, "jra", 2099)
    assert result is None


def test_resolve_version_pref_keeps_preferred_when_rows_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    con = MagicMock()
    monkeypatch.setattr(
        subject, "_preferred_version_has_rows", lambda c, cat, year, ver: True
    )
    monkeypatch.setattr(
        subject,
        "_latest_available_model_version",
        lambda c, cat, year: pytest.fail(
            "fallback lookup must not run when the preference already has rows"
        ),
    )
    resolved = subject.resolve_version_pref(con, "jra")
    assert resolved == subject.RS_VERSION_PREF["jra"]


def test_resolve_version_pref_falls_back_when_preferred_has_zero_rows(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    con = MagicMock()
    monkeypatch.setattr(
        subject, "_preferred_version_has_rows", lambda c, cat, year, ver: year != 2026
    )
    monkeypatch.setattr(
        subject,
        "_latest_available_model_version",
        lambda c, cat, year: "jra-running-style-lgbm-prod-v3" if year == 2026 else None,
    )
    resolved = subject.resolve_version_pref(con, "jra")
    assert resolved[2026] == "jra-running-style-lgbm-prod-v3"
    assert resolved[2024] == subject.RS_VERSION_PREF["jra"][2024]
    captured = capsys.readouterr()
    assert "RS_VERSION_PREF stale for jra 2026" in captured.err


def test_resolve_version_pref_keeps_preferred_when_no_pg_rows_at_all(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    con = MagicMock()
    monkeypatch.setattr(
        subject, "_preferred_version_has_rows", lambda c, cat, year, ver: False
    )
    monkeypatch.setattr(
        subject, "_latest_available_model_version", lambda c, cat, year: None
    )
    resolved = subject.resolve_version_pref(con, "nar")
    assert resolved == subject.RS_VERSION_PREF["nar"]
    captured = capsys.readouterr()
    assert captured.err == ""


def test_resolve_version_pref_unknown_category_returns_empty_dict() -> None:
    con = MagicMock()
    resolved = subject.resolve_version_pref(con, "ban-ei")
    assert resolved == {}


def test_append_features_sql_emits_all_thirteen_pacestyle_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet", "jra")
    assert "past_style_x_field_pace_match" in sql
    assert "sire_x_field_pace_score" in sql
    assert "rs_predicted_corner_front_score" in sql
    assert "rs_predicted_corner_rank" in sql
    assert "rs_predicted_corner_rank_pct" in sql


def test_append_features_sql_pace_cross_terms_use_case_when_not_coalesce() -> None:
    """NULL past_nige_rate_self/sire_nige_rate must propagate NULL, not 0.

    The cross-term columns must use CASE WHEN IS NOT NULL instead of coalesce(, 0)
    so that horses with no style history produce NULL (unknown) rather than 0.0
    (falsely implying zero style-pace interaction).
    """
    sql = subject.append_features_sql("dummy.parquet", "jra")
    assert "CASE WHEN b.past_nige_rate_self IS NOT NULL" in sql
    assert "CASE WHEN b.sire_nige_rate IS NOT NULL" in sql
    assert "coalesce(b.past_nige_rate_self, 0)" not in sql
    # coalesce(b.sire_nige_rate, 0) appears inside rs_sire_style_match where it is already
    # guarded by CASE WHEN rs.rs_p_nige IS NOT NULL — verify the unguarded form is absent
    assert "coalesce(b.past_oikomi_rate_self, 0)" not in sql
    assert "rs_p_nige" in sql
    assert "rs_p_senkou" in sql
    assert "rs_p_sashi" in sql
    assert "rs_p_oikomi" in sql
    assert "rs_predicted_class" in sql
    assert "rs_predicted_corner_front_score" in sql
    assert "rs_predicted_corner_rank" in sql
    assert "rs_predicted_corner_rank_pct" in sql
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


def test_target_race_ids_filter_sql_false_is_empty() -> None:
    assert subject.target_race_ids_filter_sql(False) == ""


def test_target_race_ids_filter_sql_true_uses_staged_ids() -> None:
    sql = subject.target_race_ids_filter_sql(True).format(category="jra")
    assert "in (select race_id from target_race_ids)" in sql
    assert "'jra:' || kaisai_nen" in sql


def test_stage_rs_predictions_from_catalog_uses_attached_raw_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    con = MagicMock()
    monkeypatch.setattr(
        subject, "resolve_version_pref", lambda c, cat: subject.RS_VERSION_PREF[cat]
    )
    subject.stage_rs_predictions_from_catalog(con, "jra")
    create_sql = con.execute.call_args_list[0].args[0]
    assert "from pg.race_running_style_model_predictions" in create_sql
    assert "where source = 'jra'" in create_sql
    assert "cast(cell_model_key as varchar) as rs_cell_model_key" in create_sql
    assert "cast(cell_variant_id as varchar) as rs_cell_variant_id" in create_sql
    assert "rs_p_senkou + 2 * rs_p_sashi + 3 * rs_p_oikomi" in create_sql
    assert "as rs_predicted_corner_front_score" in create_sql
    assert "row_number() over" in create_sql
    assert "partition by race_id" in create_sql
    assert (
        "order by rs_predicted_corner_front_score asc, rs_p_nige desc, "
        "ketto_toroku_bango asc"
    ) in create_sql
    assert "read_parquet" not in create_sql
    assert "daily_race_entries" not in create_sql
    assert "features-archive" not in create_sql


def test_stage_rs_predictions_from_catalog_focused_filters_to_target_race_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    con = MagicMock()
    monkeypatch.setattr(
        subject, "resolve_version_pref", lambda c, cat: subject.RS_VERSION_PREF[cat]
    )
    subject.stage_rs_predictions_from_catalog(con, "nar", focused_target=True)
    create_sql = con.execute.call_args_list[0].args[0]
    assert "where source = 'nar'" in create_sql
    assert "target_race_ids" in create_sql


def test_stage_rs_predictions_from_catalog_uses_resolved_version_pref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catalog staging must use the version resolved from that same catalog."""
    con = MagicMock()
    monkeypatch.setattr(
        subject,
        "resolve_version_pref",
        lambda c, cat: {2099: "catalog-version"},
    )
    subject.stage_rs_predictions_from_catalog(con, "jra")
    create_sql = con.execute.call_args_list[0].args[0]
    assert "kaisai_nen = '2099'" in create_sql
    assert "model_version = 'catalog-version'" in create_sql
    assert "jra-running-style-lgbm-prod-v3" not in create_sql


def test_stage_rs_predictions_reads_only_versioned_r2_output_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    con = MagicMock()
    monkeypatch.setattr(
        subject,
        "setup_r2_duckdb_secret",
        lambda c: calls.append("secret"),
    )
    monkeypatch.setattr(
        subject,
        "stage_rs_predictions_from_r2",
        lambda c, cat, date, bucket, focused=False: calls.append(
            ("r2", cat, date, bucket, focused)
        ),
    )
    args = argparse.Namespace(
        category="nar",
        pg_url="r2-catalog://",
        run_date="20260715",
        target_race="44:08",
    )
    subject.stage_rs_predictions(con, args)
    assert calls == [
        "secret",
        ("r2", "nar", "20260715", "pc-keiba-features-archive", True),
    ]


def test_stage_rs_predictions_rejects_catalog_without_generation_date() -> None:
    args = argparse.Namespace(
        category="jra",
        pg_url="r2-catalog://",
    )
    with pytest.raises(ValueError, match="--run-date"):
        subject.stage_rs_predictions(MagicMock(), args)


def test_setup_r2_duckdb_secret_uses_required_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("R2_ACCOUNT_ID", "account")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret")
    con = MagicMock()
    subject.setup_r2_duckdb_secret(con)
    assert con.execute.call_args_list[0].args[0] == "install httpfs; load httpfs;"
    secret_sql = con.execute.call_args_list[1].args[0]
    assert "endpoint 'account.r2.cloudflarestorage.com'" in secret_sql
    assert "key_id 'key'" in secret_sql


def test_stage_rs_predictions_from_r2_uses_raw_catalog_generation() -> None:
    con = MagicMock()
    subject.stage_rs_predictions_from_r2(
        con,
        "jra",
        "20260715",
        "archive",
        focused_target=True,
    )
    create_sql = con.execute.call_args_list[0].args[0]
    assert (
        "s3://archive/running-style/predictions/by-day/raw-iceberg-v1/"
        "2026/07/15/jra/*.parquet"
    ) in create_sql
    assert "target_race_ids" in create_sql
    assert "race_running_style_model_predictions" not in create_sql


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
            "--run-date",
            "20260607",
        ],
    )
    subject.main()
    assert seen["category"] == "jra"
    assert seen["run_date"] == "20260607"
    assert seen["wrote"] is True


def test_stage_target_race_ids_extracts_input_race_ids(tmp_path: Path) -> None:
    input_glob = _base_parquet(tmp_path)
    con = duckdb.connect(":memory:")
    subject.stage_target_race_ids(con, input_glob, "jra")
    rows = con.execute("select race_id from target_race_ids").fetchall()
    con.close()
    assert rows == [("jra:2024:0101:01:1",)]


# ---------------------------------------------------------------------------
# SQL correctness: rs_confidence_entropy / cross-terms must be NULL for
# horses with no RS prediction (LEFT JOIN miss), not 0.
# ---------------------------------------------------------------------------


def _base_parquet(tmp_path: Path) -> str:
    """Write a minimal base parquet and return the file glob string."""
    nullable_float = [None, None]
    df = pl.DataFrame(
        {
            "kaisai_nen": ["2024", "2024"],
            "kaisai_tsukihi": ["0101", "0101"],
            "keibajo_code": ["01", "01"],
            "race_bango": ["1", "1"],
            "ketto_toroku_bango": ["a", "b"],
            "shusso_tosu": [2, 2],
            # base_extra referenced columns — all nullable, set to NULL for simplicity
            "past_nige_rate_self": nullable_float,
            "past_senkou_rate_self": nullable_float,
            "past_sashi_rate_self": nullable_float,
            "past_oikomi_rate_self": nullable_float,
            "field_nige_pressure": nullable_float,
            "field_senkou_pressure": nullable_float,
            "field_sashi_pressure": nullable_float,
            "field_oikomi_pressure": nullable_float,
            "sire_nige_rate": nullable_float,
            "sire_senkou_rate": nullable_float,
            "sire_sashi_rate": nullable_float,
            "sire_oikomi_rate": nullable_float,
            "field_pace_index": nullable_float,
            "race_year": [2024, 2024],
        },
        schema_overrides={
            "past_nige_rate_self": pl.Float64,
            "past_senkou_rate_self": pl.Float64,
            "past_sashi_rate_self": pl.Float64,
            "past_oikomi_rate_self": pl.Float64,
            "field_nige_pressure": pl.Float64,
            "field_senkou_pressure": pl.Float64,
            "field_sashi_pressure": pl.Float64,
            "field_oikomi_pressure": pl.Float64,
            "sire_nige_rate": pl.Float64,
            "sire_senkou_rate": pl.Float64,
            "sire_sashi_rate": pl.Float64,
            "sire_oikomi_rate": pl.Float64,
            "field_pace_index": pl.Float64,
        },
    )
    out = tmp_path / "race_year=2024" / "base.parquet"
    out.parent.mkdir(parents=True)
    df.write_parquet(out.as_posix())
    return str(tmp_path / "race_year=*" / "*.parquet")


def _rs_preds_df() -> pl.DataFrame:
    """RS predictions only for horse 'a' — horse 'b' is unscored."""
    return pl.DataFrame(
        {
            "race_id": ["jra:2024:0101:01:1"],
            "ketto_toroku_bango": ["a"],
            "rs_p_nige": [0.6],
            "rs_p_senkou": [0.2],
            "rs_p_sashi": [0.1],
            "rs_p_oikomi": [0.1],
            "rs_predicted_class": ["nige"],
            "rs_predicted_corner_front_score": [0.7],
            "rs_predicted_corner_rank": [1],
            "model_version": ["v1.0"],
            "race_date": ["20240101"],
        }
    )


def test_append_features_sql_uses_case_when_for_rs_columns():
    """SQL string must use CASE WHEN guard instead of coalesce-to-zero for rs_p_* columns."""
    sql = subject.append_features_sql("dummy.parquet", "jra")
    assert "CASE WHEN rs.rs_p_nige IS NOT NULL" in sql, (
        "rs_confidence_entropy and cross-terms must be guarded with CASE WHEN IS NOT NULL"
    )


def test_append_features_sql_rs_confidence_entropy_null_for_unscored_horse(
    tmp_path: Path,
):
    """Horse with no RS prediction must get NULL rs_confidence_entropy, not 0."""
    input_glob = _base_parquet(tmp_path)
    con = duckdb.connect()
    con.register("rs_preds", _rs_preds_df())
    sql = subject.append_features_sql(input_glob, "jra")
    result = con.execute(sql).pl()

    horse_a = result.filter(pl.col("ketto_toroku_bango") == "a").row(0, named=True)
    horse_b = result.filter(pl.col("ketto_toroku_bango") == "b").row(0, named=True)

    assert horse_a["rs_confidence_entropy"] is not None, (
        "scored horse must have non-NULL entropy"
    )
    assert horse_b["rs_confidence_entropy"] is None, (
        "unscored horse must have NULL entropy, not 0 (0 would falsely imply max confidence)"
    )


def test_append_features_sql_corner_rank_pct_values(tmp_path: Path):
    input_glob = _base_parquet(tmp_path)
    con = duckdb.connect()
    con.register(
        "rs_preds",
        pl.DataFrame(
            {
                "race_id": ["jra:2024:0101:01:1", "jra:2024:0101:01:1"],
                "ketto_toroku_bango": ["a", "b"],
                "rs_p_nige": [0.6, 0.1],
                "rs_p_senkou": [0.2, 0.4],
                "rs_p_sashi": [0.1, 0.3],
                "rs_p_oikomi": [0.1, 0.2],
                "rs_predicted_class": [0, 1],
                "rs_predicted_corner_front_score": [0.7, 1.6],
                "rs_predicted_corner_rank": [1, 2],
            }
        ),
    )
    result = con.execute(subject.append_features_sql(input_glob, "jra")).pl()

    horse_a = result.filter(pl.col("ketto_toroku_bango") == "a").row(0, named=True)
    horse_b = result.filter(pl.col("ketto_toroku_bango") == "b").row(0, named=True)
    assert horse_a["rs_predicted_corner_front_score"] == pytest.approx(0.7)
    assert horse_a["rs_predicted_corner_rank"] == 1
    assert horse_a["rs_predicted_corner_rank_pct"] == pytest.approx(0.0)
    assert horse_b["rs_predicted_corner_front_score"] == pytest.approx(1.6)
    assert horse_b["rs_predicted_corner_rank"] == 2
    assert horse_b["rs_predicted_corner_rank_pct"] == pytest.approx(1.0)


def test_append_features_sql_corner_rank_pct_null_for_unscored_horse(tmp_path: Path):
    input_glob = _base_parquet(tmp_path)
    con = duckdb.connect()
    con.register("rs_preds", _rs_preds_df())
    result = con.execute(subject.append_features_sql(input_glob, "jra")).pl()

    horse_b = result.filter(pl.col("ketto_toroku_bango") == "b").row(0, named=True)
    assert horse_b["rs_predicted_corner_front_score"] is None
    assert horse_b["rs_predicted_corner_rank"] is None
    assert horse_b["rs_predicted_corner_rank_pct"] is None


def test_append_features_sql_cross_terms_null_for_unscored_horse(tmp_path: Path):
    """rs_p_nige_x_field_pace and rs_sire_style_match must be NULL for unscored horses."""
    input_glob = _base_parquet(tmp_path)
    con = duckdb.connect()
    con.register("rs_preds", _rs_preds_df())
    sql = subject.append_features_sql(input_glob, "jra")
    result = con.execute(sql).pl()

    horse_b = result.filter(pl.col("ketto_toroku_bango") == "b").row(0, named=True)
    assert horse_b["rs_p_nige_x_field_pace"] is None, (
        "unscored horse rs_p_nige_x_field_pace must be NULL"
    )
    assert horse_b["rs_sire_style_match"] is None, (
        "unscored horse rs_sire_style_match must be NULL"
    )


def test_append_features_sql_cross_terms_non_null_for_scored_horse(tmp_path: Path):
    """rs_p_nige_x_field_pace and rs_sire_style_match must be non-NULL for scored horses."""
    input_glob = _base_parquet(tmp_path)
    con = duckdb.connect()
    con.register("rs_preds", _rs_preds_df())
    sql = subject.append_features_sql(input_glob, "jra")
    result = con.execute(sql).pl()

    horse_a = result.filter(pl.col("ketto_toroku_bango") == "a").row(0, named=True)
    # With all base columns NULL (0 from coalesce), cross-terms should be 0.0 (not NULL)
    assert horse_a["rs_p_nige_x_field_pace"] is not None
    assert horse_a["rs_sire_style_match"] is not None
