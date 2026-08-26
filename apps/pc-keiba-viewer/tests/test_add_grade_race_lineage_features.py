from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

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


def test_install_and_attach_pg_delegates_to_catalog_attach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attach_mock = MagicMock()
    connection = MagicMock()
    monkeypatch.setattr(subject, "attach_source_catalog", attach_mock)

    subject.install_and_attach_pg(connection, "postgresql://catalog")

    attach_mock.assert_called_once_with(connection, "postgresql://catalog")


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


def test_config_category_and_max_lookback_use_configured_trial_scope() -> None:
    config = {
        "category": "nar",
        "target_races": [
            {
                "id": "target",
                "trials": [
                    {"match": {"kyosomei_equals": "A"}, "lookback_days": 30},
                    {"match": {"kyosomei_equals": "B"}},
                ],
            }
        ],
    }

    assert subject.config_category(config) == "nar"
    assert subject.max_trial_lookback_days(config) == 90


def test_config_category_and_lookback_reject_invalid_config() -> None:
    try:
        subject.config_category({"category": "overseas", "target_races": []})
    except ValueError as error:
        assert str(error) == "Config category must be one of: jra, nar, ban-ei"
    else:
        raise AssertionError("expected invalid category error")

    try:
        subject.max_trial_lookback_days({"category": "nar", "target_races": []})
    except ValueError as error:
        assert str(error) == "No trial lookback definitions built"
        return
    raise AssertionError("expected missing lookback error")


def test_category_predicates_split_jra_nar_and_ban_ei() -> None:
    assert subject.category_predicate("jra", "r") == "r.source = 'jra'"
    assert subject.category_predicate("ban-ei", "r") == (
        "r.source = 'nar' and r.keibajo_code = '83'"
    )
    assert subject.category_predicate("nar", "r") == (
        "r.source = 'nar' and (r.keibajo_code is null or r.keibajo_code <> '83')"
    )
    assert subject.category_venue_predicate("jra", "r") == "true"
    assert subject.category_venue_predicate("ban-ei", "r") == "r.keibajo_code = '83'"
    assert subject.category_venue_predicate("nar", "r") == (
        "(r.keibajo_code is null or r.keibajo_code <> '83')"
    )

    try:
        subject.category_predicate("overseas", "r")
    except ValueError as error:
        assert str(error) == "Unsupported category: overseas"
    else:
        raise AssertionError("expected unsupported category error")

    try:
        subject.category_venue_predicate("overseas", "r")
    except ValueError as error:
        assert str(error) == "Unsupported category: overseas"
        return
    raise AssertionError("expected unsupported venue category error")


def test_target_horse_predicate_scopes_source_and_escapes_literals() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table lineage_target_horses (
          source varchar,
          ketto_toroku_bango varchar
        )
        """
    )
    con.execute(
        """
        insert into lineage_target_horses values
          ('nar', 'horse-2'),
          ('jra', 'horse-1'),
          ('nar', 'horse''3'),
          ('nar', ''),
          ('nar', null),
          (null, 'horse-4')
        """
    )

    assert subject.target_horse_predicate(con, "nar", "se") == (
        "se.ketto_toroku_bango in ('', 'horse''3', 'horse-2')"
    )
    con.close()


def test_target_horse_predicate_fails_closed_when_no_joinable_horses() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table lineage_target_horses (
          source varchar,
          ketto_toroku_bango varchar
        )
        """
    )
    con.execute("insert into lineage_target_horses values ('nar', null), (null, 'horse-1')")

    assert subject.target_horse_predicate(con, "jra", "se") == "false"
    con.close()


def test_jra_scoped_race_history_reads_raw_partitioned_tables() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create temp table lineage_target_horses (
          source varchar,
          ketto_toroku_bango varchar
        )
        """
    )
    con.execute("insert into lineage_target_horses values ('jra', 'horse-1')")
    con.execute(
        """
        create table pg.jvd_ra (
          kaisai_nen varchar,
          kaisai_tsukihi varchar,
          keibajo_code varchar,
          race_bango varchar,
          kyori varchar
        )
        """
    )
    con.execute("insert into pg.jvd_ra values ('2026', '0810', '05', '01', '1800')")
    con.execute(
        """
        create table pg.jvd_se (
          kaisai_nen varchar,
          kaisai_tsukihi varchar,
          keibajo_code varchar,
          race_bango varchar,
          ketto_toroku_bango varchar,
          umaban varchar,
          kakutei_chakujun varchar,
          time_sa varchar
        )
        """
    )
    con.execute(
        """
        insert into pg.jvd_se values
          ('2026', '0810', '05', '01', 'horse-1', '01', '02', '0015'),
          ('2026', '0810', '05', '01', 'horse-2', '02', '01', '0000')
        """
    )

    subject.stage_race_history(
        con,
        "20100101",
        subject.LineageScope(
            category="jra",
            history_from_date="20260701",
            target_to_date="20260825",
        ),
    )

    assert con.execute(
        """
        select source, race_date, ketto_toroku_bango, finish_position, time_sa
        from race_history
        """
    ).fetchall() == [("jra", "20260810", "horse-1", 2, 1.5)]
    con.close()


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
                "match": {
                    "kyosomei_equals": "テストG1",
                    "keibajo_code": "05",
                    "kyori": 2000,
                    "month": 5,
                    "grade_code": "A",
                },
            }
        ]
    }
    sql = subject.build_target_classify_sql(cfg)
    assert "kyosomei_norm = 'テストG1'" in sql
    assert "keibajo_code = '05'" in sql
    assert "kyori_int = 2000" in sql
    assert "month = 5" in sql
    assert "grade_code = 'A'" in sql


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


def test_nar_scoped_staging_matches_full_features_without_scanning_jra(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "input" / "race_year=2026"
    input_dir.mkdir(parents=True)
    input_file = input_dir / "part.parquet"
    con = duckdb.connect(":memory:")
    con.execute(
        f"""
        copy (
          select
            'nar'::varchar as source,
            '2026'::varchar as kaisai_nen,
            '0825'::varchar as kaisai_tsukihi,
            '43'::varchar as keibajo_code,
            '01'::varchar as race_bango,
            'horse-1'::varchar as ketto_toroku_bango
        ) to '{input_file.as_posix()}' (format parquet)
        """
    )
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.nvd_ra (
          kaisai_nen varchar,
          kaisai_tsukihi varchar,
          keibajo_code varchar,
          race_bango varchar,
          kyosomei_hondai varchar,
          kyori varchar,
          grade_code varchar
        )
        """
    )
    con.execute(
        """
        insert into pg.nvd_ra values
          ('2026', '0825', '43', '01', 'Target Stakes', '1800', 'A'),
          ('2026', '0810', '43', '02', 'Trial Stakes', '1800', 'B'),
          ('2026', '0701', '43', '03', 'Trial Stakes', '1800', 'B'),
          ('2026', '0810', '83', '04', 'Other Stakes', '0200', 'P')
        """
    )
    con.execute(
        """
        create table pg.nvd_se (
          kaisai_nen varchar,
          kaisai_tsukihi varchar,
          keibajo_code varchar,
          race_bango varchar,
          ketto_toroku_bango varchar,
          umaban varchar,
          kakutei_chakujun varchar,
          time_sa varchar
        )
        """
    )
    con.execute(
        """
        insert into pg.nvd_se values
          ('2026', '0825', '43', '01', 'horse-1', '01', '00', '0000'),
          ('2026', '0810', '43', '02', 'horse-1', '01', '01', '-020'),
          ('2026', '0701', '43', '03', 'horse-1', '01', '01', '-010'),
          ('2026', '0810', '43', '02', 'horse-2', '02', '01', '-030'),
          ('2026', '0810', '83', '04', 'horse-1', '01', '01', '-040')
        """
    )
    con.execute(
        """
        create table pg.race_entry_corner_features (
          source varchar,
          race_date varchar,
          kaisai_nen varchar,
          kaisai_tsukihi varchar,
          keibajo_code varchar,
          race_bango varchar,
          ketto_toroku_bango varchar,
          finish_position integer,
          time_sa double
        )
        """
    )
    con.execute(
        """
        insert into pg.race_entry_corner_features values
          ('nar', '20260825', '2026', '0825', '43', '01', 'horse-1', 5, 4.0),
          ('nar', '20260810', '2026', '0810', '43', '02', 'horse-1', 1, -2.0),
          ('nar', '20260701', '2026', '0701', '43', '03', 'horse-1', 1, -1.0),
          ('nar', '20260810', '2026', '0810', '43', '02', 'horse-2', 1, -3.0),
          ('nar', '20260810', '2026', '0810', '83', '04', 'horse-1', 1, -4.0),
          ('jra', '20260810', '2026', '0810', '05', '05', 'horse-1', 1, -5.0)
        """
    )
    config = {
        "category": "nar",
        "target_races": [
            {
                "id": "target",
                "match": {"kyosomei_equals": "TargetStakes"},
                "trials": [
                    {
                        "name": "trial",
                        "match": {"kyosomei_equals": "TrialStakes"},
                        "lookback_days": 30,
                    }
                ],
            }
        ],
    }
    input_glob = f"{(tmp_path / 'input').as_posix()}/race_year=*/*.parquet"
    classify_sql = subject.build_target_classify_sql(config)
    trial_values = subject.build_trial_defs_values(config)
    scope = subject.stage_target_scope(con, input_glob, "nar", 30)

    assert scope == subject.LineageScope(
        category="nar", history_from_date="20260726", target_to_date="20260825"
    )
    subject.stage_race_meta(con, "20100101", scope)
    assert con.execute("select count(*) from race_meta").fetchone() == (2,)
    subject.stage_target_classifications(con, classify_sql, scoped=True)
    subject.stage_trial_definitions(con, trial_values)
    subject.stage_race_serves_as_trial(con)
    subject.stage_race_history(con, "20100101", scope)
    assert con.execute("select count(*) from race_history").fetchone() == (1,)
    subject.stage_horse_trial_history(con)
    subject.stage_horse_target_race_trial_summary(con)
    con.execute(
        f"create temp table scoped_result as {subject.append_features_sql(input_glob)}"
    )

    con.execute(
        """
        create table pg.jvd_ra as
        select * from pg.nvd_ra where false
        """
    )
    con.execute(
        """
        insert into pg.jvd_ra values
          ('2026', '0810', '05', '05', 'Trial Stakes', '1800', 'B')
        """
    )
    subject.stage_race_meta(con, "20100101")
    subject.stage_target_classifications(con, classify_sql)
    subject.stage_trial_definitions(con, trial_values)
    subject.stage_race_serves_as_trial(con)
    subject.stage_race_history(con, "20100101")
    subject.stage_horse_trial_history(con)
    subject.stage_horse_target_race_trial_summary(con)
    con.execute(f"create temp table full_result as {subject.append_features_sql(input_glob)}")

    assert con.execute(
        """
        select
          target_race_id,
          target_grade_trial_count,
          target_grade_trial_top1_count,
          target_grade_trial_top3_count,
          target_grade_trial_best_finish,
          target_grade_trial_avg_top2_margin_decisec,
          target_grade_has_trial_history
        from scoped_result
        """
    ).fetchall() == [("target", 1, 1, 1, 1, -2.0, 1)]
    assert con.execute(
        """
        select * from scoped_result
        except all
        select * from full_result
        """
    ).fetchall() == []
    assert con.execute(
        """
        select * from full_result
        except all
        select * from scoped_result
        """
    ).fetchall() == []
    con.close()


def test_main_runs_scoped_pipeline_with_config_max_lookback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = MagicMock()
    config = {
        "category": "nar",
        "target_races": [
            {
                "id": "target",
                "match": {"kyosomei_equals": "Target"},
                "trials": [
                    {
                        "name": "Trial",
                        "match": {"kyosomei_equals": "Trial"},
                        "lookback_days": 180,
                    }
                ],
            }
        ],
    }
    parse_args_mock = MagicMock(
        return_value=subject.argparse.Namespace(
            config=tmp_path / "nar.json",
            from_date="20100101",
            input_dir=tmp_path / "input",
            memory_limit="4GB",
            output_dir=tmp_path / "output",
            pg_url="postgresql://catalog",
            threads=2,
            to_date="20991231",
        )
    )
    load_config_mock = MagicMock(return_value=config)
    apply_mock = MagicMock()
    attach_mock = MagicMock()
    target_scope_mock = MagicMock(
        return_value=subject.LineageScope(
            category="nar", history_from_date="20260226", target_to_date="20260825"
        )
    )
    race_meta_mock = MagicMock()
    target_classification_mock = MagicMock()
    trial_definitions_mock = MagicMock()
    serves_as_trial_mock = MagicMock()
    race_history_mock = MagicMock()
    horse_history_mock = MagicMock()
    summary_mock = MagicMock()
    write_mock = MagicMock()
    monkeypatch.setattr(subject, "parse_args", parse_args_mock)
    monkeypatch.setattr(subject, "load_config", load_config_mock)
    monkeypatch.setattr(subject.duckdb, "connect", MagicMock(return_value=connection))
    monkeypatch.setattr(subject, "apply_to_connection", apply_mock)
    monkeypatch.setattr(subject, "install_and_attach_pg", attach_mock)
    monkeypatch.setattr(subject, "stage_target_scope", target_scope_mock)
    monkeypatch.setattr(subject, "stage_race_meta", race_meta_mock)
    monkeypatch.setattr(subject, "stage_target_classifications", target_classification_mock)
    monkeypatch.setattr(subject, "stage_trial_definitions", trial_definitions_mock)
    monkeypatch.setattr(subject, "stage_race_serves_as_trial", serves_as_trial_mock)
    monkeypatch.setattr(subject, "stage_race_history", race_history_mock)
    monkeypatch.setattr(subject, "stage_horse_trial_history", horse_history_mock)
    monkeypatch.setattr(subject, "stage_horse_target_race_trial_summary", summary_mock)
    monkeypatch.setattr(subject, "write_partitioned", write_mock)

    subject.main()

    target_scope_mock.assert_called_once_with(
        connection,
        f"{tmp_path.as_posix()}/input/race_year=*/*.parquet",
        "nar",
        180,
    )
    race_meta_mock.assert_called_once_with(
        connection,
        "20100101",
        subject.LineageScope(
            category="nar", history_from_date="20260226", target_to_date="20260825"
        ),
    )
    target_classification_mock.assert_called_once_with(
        connection,
        "case when kyosomei_norm = 'Target' then 'target' else null end",
        scoped=True,
    )
    race_history_mock.assert_called_once_with(
        connection,
        "20100101",
        subject.LineageScope(
            category="nar", history_from_date="20260226", target_to_date="20260825"
        ),
    )
    connection.close.assert_called_once_with()


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
