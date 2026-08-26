from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from unittest.mock import Mock

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-trainer-stable-affinity-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "add_trainer_stable_affinity_features", MODULE_PATH
)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_trainer_stable_affinity_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.category == "jra"


def test_parse_args_supports_nar_category(tmp_path: Path) -> None:
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


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "10:02",
        ]
    )
    assert args.target_race == "10:02"


def test_append_features_sql_jra_uses_jvd_se() -> None:
    sql = subject.append_features_sql("dummy.parquet", "jra")
    assert "pg.jvd_se" in sql
    assert "pg.nvd_se" not in sql


def test_append_features_sql_nar_uses_nvd_se() -> None:
    sql = subject.append_features_sql("dummy.parquet", "nar")
    assert "pg.nvd_se" in sql
    assert "pg.jvd_se" not in sql


def test_append_features_sql_contains_trainer_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "trainer_grade_career_starts" in sql
    assert "trainer_grade_win_rate" in sql
    assert "trainer_grade_top3_rate" in sql
    assert "trainer_target_race_career_count" in sql
    assert "trainer_target_race_win_count" in sql
    assert "trainer_target_race_top3_count" in sql
    assert "trainer_target_race_has_history" in sql


def test_append_features_sql_joins_grade_and_target_cumul() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "asof left join trainer_grade_cumul" in sql
    assert "asof left join trainer_target_cumul" in sql
    assert "tg.chokyoshi_code = bwt.chokyoshi_code" in sql
    assert "tg.grade_code = bwt.grade_code" in sql
    assert "tg.race_date < bwt.race_date" in sql
    assert "tt.race_date < bwt.race_date" in sql


def test_trainer_cumulative_tables_include_each_history_day() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, query: str) -> None:
            self.sql.append(query)

    conn = FakeConn()
    subject.stage_trainer_grade_cumul(conn)
    subject.stage_trainer_target_race_cumul(conn)
    joined = "\n".join(conn.sql)
    assert joined.count("rows between unbounded preceding and current row") == 2
    assert "rows between unbounded preceding and 1 preceding" not in joined


def test_stage_target_trainers_reads_trainers_from_base_input() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, query: str) -> FakeConn:
            self.sql.append(query)
            return self

        def fetchall(self) -> list[tuple[str, str, str, str, str, str]]:
            return [("jra", "2026", "0826", "05", "01", "HORSE01")]

    conn = FakeConn()
    subject.stage_target_trainers(conn, "jra")
    joined = "\n".join(conn.sql)
    assert "create or replace temp table target_trainers" in joined
    assert "from pg.jvd_se se" in joined
    assert "se.kaisai_nen = '2026'" in joined
    assert "se.kaisai_tsukihi = '0826'" in joined
    assert "se.keibajo_code = '05'" in joined
    assert "se.race_bango = '01'" in joined
    assert "se.ketto_toroku_bango in ('HORSE01')" in joined
    assert "se.chokyoshi_code" in joined


def test_stage_race_history_always_filters_to_input_trainers() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, query: str) -> FakeConn:
            self.sql.append(query)
            return self

        def fetchall(self) -> list[tuple[str]]:
            return [("TRAINER01",)]

    conn = FakeConn()
    subject.stage_race_history_with_trainer(conn, "20100101", "jra")
    joined = "\n".join(conn.sql)
    assert "from pg.jvd_se se" in joined
    assert "inner join pg.jvd_ra ra" in joined
    assert "se.chokyoshi_code in ('TRAINER01')" in joined
    assert "se.kaisai_nen || se.kaisai_tsukihi >= '20100101'" in joined
    assert "se.kaisai_nen >= '2010'" in joined
    assert "try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)" in joined
    assert "try_cast(nullif(trim(se.umaban), '') as integer) is not null" in joined


def test_target_race_entry_filter_scopes_source_and_escapes_literals() -> None:
    class FakeConn:
        def execute(self, _query: str) -> FakeConn:
            return self

        def fetchall(self) -> list[tuple[str, str, str, str, str, str]]:
            return [
                ("jra", "2026", "0826", "05", "01", "O'HARE"),
                ("nar", "2026", "0826", "43", "01", "NAR01"),
            ]

    assert subject.target_race_entry_filter_sql(FakeConn(), "jra") == (
        "(se.kaisai_nen = '2026' and se.kaisai_tsukihi = '0826' "
        "and se.keibajo_code = '05' and se.race_bango = '01' "
        "and se.ketto_toroku_bango in ('O''HARE'))"
    )


def test_target_race_entry_filter_returns_false_without_category_rows() -> None:
    class FakeConn:
        def execute(self, _query: str) -> FakeConn:
            return self

        def fetchall(self) -> list[tuple[str, str, str, str, str, str]]:
            return [("nar", "2026", "0826", "43", "01", "NAR01")]

    assert subject.target_race_entry_filter_sql(FakeConn(), "jra") == "false"


def test_target_trainer_filter_deduplicates_and_escapes_literals() -> None:
    class FakeConn:
        def execute(self, _query: str) -> FakeConn:
            return self

        def fetchall(self) -> list[tuple[str]]:
            return [("O'NEIL",), ("TRAINER01",), ("TRAINER01",)]

    assert subject.target_trainer_filter_sql(FakeConn()) == (
        "se.chokyoshi_code in ('O''NEIL', 'TRAINER01')"
    )


def test_target_trainer_filter_returns_false_without_trainers() -> None:
    class FakeConn:
        def execute(self, _query: str) -> FakeConn:
            return self

        def fetchall(self) -> list[tuple[str]]:
            return []

    assert subject.target_trainer_filter_sql(FakeConn()) == "false"


def test_stage_race_history_nar_uses_raw_nar_tables() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, query: str) -> FakeConn:
            self.sql.append(query)
            return self

        def fetchall(self) -> list[tuple[str]]:
            return [("TRAINER43",)]

    conn = FakeConn()
    subject.stage_race_history_with_trainer(conn, "20200101", "nar")
    joined = "\n".join(conn.sql)
    assert "'nar' as source" in joined
    assert "from pg.nvd_se se" in joined
    assert "inner join pg.nvd_ra ra" in joined
    assert "pg.race_entry_corner_features" not in joined


def test_stage_race_history_preserves_compatibility_view_row_validity() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("create schema pg")
    conn.execute(
        """
        create table pg.jvd_se (
          kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
          race_bango varchar, ketto_toroku_bango varchar, kakutei_chakujun varchar,
          umaban varchar, chokyoshi_code varchar
        );
        create table pg.jvd_ra (
          kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
          race_bango varchar, grade_code varchar
        );
        create temp table target_trainers (chokyoshi_code varchar);
        insert into target_trainers values ('TRAINER01');
        insert into pg.jvd_ra values ('2026', '0101', '05', '01', 'C');
        insert into pg.jvd_se values
          ('2026', '0101', '05', '01', 'HORSE01', '01', '01', 'TRAINER01'),
          ('2026', '0101', '05', '01', 'HORSE02', '00', '02', 'TRAINER01'),
          ('2026', '0101', '05', '01', 'HORSE03', '03', '', 'TRAINER01'),
          ('2026', '0101', '05', '01', 'HORSE04', '04', '04', 'TRAINER02');
        """
    )

    subject.stage_race_history_with_trainer(conn, "20200101", "jra")

    assert conn.execute(
        """
        select source, race_date, ketto_toroku_bango, finish_position,
               grade_code, chokyoshi_code
        from race_history
        """
    ).fetchall() == [("jra", "20260101", "HORSE01", 1, "C", "TRAINER01")]
    conn.close()


def test_main_stages_target_trainers_for_whole_day_input(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = argparse.Namespace(
        category="nar",
        from_date="20100101",
        input_dir=tmp_path / "input",
        memory_limit="1GB",
        output_dir=tmp_path / "output",
        pg_url="postgresql://example",
        target_race=None,
        threads=1,
    )
    connection = Mock()
    stage_trainers = Mock()
    stage_history = Mock()
    monkeypatch.setattr(subject, "parse_args", Mock(return_value=args))
    monkeypatch.setattr(subject.duckdb, "connect", Mock(return_value=connection))
    monkeypatch.setattr(subject, "apply_to_connection", Mock())
    monkeypatch.setattr(subject, "install_and_attach_pg", Mock())
    monkeypatch.setattr(subject, "stage_base_input", Mock())
    monkeypatch.setattr(subject, "stage_target_trainers", stage_trainers)
    monkeypatch.setattr(subject, "stage_race_history_with_trainer", stage_history)
    monkeypatch.setattr(subject, "stage_trainer_grade_cumul", Mock())
    monkeypatch.setattr(subject, "stage_trainer_target_race_cumul", Mock())
    monkeypatch.setattr(subject, "append_features_sql", Mock(return_value="select 1"))
    monkeypatch.setattr(subject, "write_partitioned", Mock())

    subject.main()

    stage_trainers.assert_called_once_with(connection, "nar")
    stage_history.assert_called_once_with(connection, "20100101", "nar")
