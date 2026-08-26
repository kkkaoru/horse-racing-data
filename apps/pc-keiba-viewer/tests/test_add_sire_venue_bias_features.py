from __future__ import annotations

import importlib.util
import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-sire-venue-bias-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "add_sire_venue_bias_features", MODULE_PATH
)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_sire_venue_bias_features"] = subject
_spec.loader.exec_module(subject)


class FakeConn:
    def __init__(self, rows: list[tuple[object, ...]] | None = None) -> None:
        self.statements: list[str] = []
        self.rows: list[tuple[object, ...]] = [] if rows is None else rows

    def execute(self, sql: str) -> FakeConn:
        self.statements.append(sql)
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    def fetchone(self) -> tuple[object, ...] | None:
        return self.rows[0] if self.rows else None

    def close(self) -> None:
        self.statements.append("closed")


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


def test_category_predicates_uses_requested_alias() -> None:
    assert subject._category_predicates("nar", alias="se") == (
        "nar",
        "(se.keibajo_code is null or se.keibajo_code <> '83')",
    )


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


def test_sql_literal_escapes_quotes() -> None:
    assert subject.sql_literal("horse'01") == "'horse''01'"


def test_target_sire_horse_filter_sql_returns_sorted_unique_literals() -> None:
    conn = FakeConn([("horse'02",), ("horse01",), ("horse01",)])
    assert subject.target_sire_horse_filter_sql(conn) == (
        "se.ketto_toroku_bango in ('horse''02', 'horse01')"
    )
    assert "inner join target_sires" in conn.statements[0]


def test_target_sire_horse_filter_sql_handles_empty_cohort() -> None:
    conn = FakeConn()
    assert subject.target_sire_horse_filter_sql(conn) == "false"


def test_target_history_upper_bound_handles_value_and_empty() -> None:
    assert subject.target_history_upper_bound(FakeConn([("20260826",)])) == "20260826"
    assert subject.target_history_upper_bound(FakeConn([(None,)])) is None
    assert subject.target_history_upper_bound(FakeConn()) is None


def test_stage_target_sires_reads_input_parquet_and_pedigree() -> None:
    conn = FakeConn()
    subject.stage_target_sires(conn, "/tmp/in/race_year=*/*.parquet")
    body = " ".join(conn.statements)
    assert "create or replace temp table target_sires" in body
    assert "read_parquet('/tmp/in/race_year=*/*.parquet'" in body
    assert "join horse_pedigree hp" in body


def test_stage_target_sire_cells_keeps_only_input_cells(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create temp table horse_pedigree as select * from (values
          ('target01', 'sire01', 'damsire01'),
          ('target02', null, 'damsire02')
        ) pedigree(ketto_toroku_bango, sire_id, damsire_id)
        """
    )
    partition_dir = tmp_path / "race_year=2024"
    partition_dir.mkdir()
    conn.execute(
        f"""
        copy (select * from (values
          ('target01', '01', '10', 1200, '20240102'),
          ('target02', '02', '20', 1400, '20240103')
        ) b(ketto_toroku_bango, keibajo_code, track_code, kyori, race_date))
        to '{(partition_dir / "part.parquet").as_posix()}' (format parquet)
        """
    )

    subject.stage_target_sire_cells(
        conn, f"{tmp_path.as_posix()}/race_year=*/*.parquet"
    )
    assert conn.execute(
        """
        select sire_id, keibajo_code, surface_type, kyori, race_date
        from target_sire_cells order by sire_id
        """
    ).fetchall() == [("sire01", "01", "turf", 1200, "20240102")]
    conn.close()


def test_stage_sire_race_history_focused_filters_to_target_sires() -> None:
    conn = FakeConn([("horse01",)])
    subject.stage_sire_race_history(conn, "20200101", "jra", focused_target=True)
    body = " ".join(conn.statements)
    assert "target_sires" in body
    assert "from pg.jvd_se se" in body
    assert "inner join pg.jvd_ra ra" in body
    assert "se.kaisai_nen >= '2020'" in body
    assert "se.kaisai_nen || se.kaisai_tsukihi >= '20200101'" in body
    assert "se.ketto_toroku_bango in ('horse01')" in body
    assert "pg.race_entry_corner_features" not in body


def test_stage_sire_race_history_nar_excludes_ban_ei_in_raw_branch() -> None:
    conn = FakeConn([("horse01",)])
    subject.stage_sire_race_history(conn, "20100101", "nar", focused_target=True)
    body = " ".join(conn.statements)
    assert "from pg.nvd_se se" in body
    assert "inner join pg.nvd_ra ra" in body
    assert "(se.keibajo_code is null or se.keibajo_code <> '83')" in body


def test_stage_sire_race_history_pushes_target_date_upper_bound() -> None:
    conn = FakeConn([("horse01",)])
    subject.stage_sire_race_history(
        conn, "20100101", "nar", focused_target=True, to_date="20260826"
    )
    body = " ".join(conn.statements)
    assert "se.kaisai_nen <= '2026'" in body
    assert "se.kaisai_nen || se.kaisai_tsukihi < '20260826'" in body


def test_stage_sire_race_history_ban_ei_uses_venue_83() -> None:
    conn = FakeConn([("horse01",)])
    subject.stage_sire_race_history(conn, "20100101", "ban-ei", focused_target=True)
    body = " ".join(conn.statements)
    assert "from pg.nvd_se se" in body
    assert "se.keibajo_code = '83'" in body


def test_stage_sire_race_history_unfocused_keeps_full_raw_history() -> None:
    conn = FakeConn()
    subject.stage_sire_race_history(conn, "20100101", "jra")
    body = " ".join(conn.statements)
    assert "and (true)" in body
    assert "target_sires" not in body


def test_stage_sire_race_history_raw_scan_preserves_normalized_filters() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("create schema pg")
    conn.execute(
        """
        create table pg.jvd_se (
          kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
          race_bango varchar, ketto_toroku_bango varchar, umaban varchar,
          kakutei_chakujun varchar
        )
        """
    )
    conn.execute(
        """
        create table pg.jvd_ra (
          kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
          race_bango varchar, track_code varchar, kyori varchar
        )
        """
    )
    conn.execute(
        """
        insert into pg.jvd_ra values
          ('2024', '0101', '01', '01', '10', '1200'),
          ('2019', '1231', '01', '01', '20', '1400')
        """
    )
    conn.execute(
        """
        insert into pg.jvd_se values
          ('2024', '0101', '01', '01', 'horse01', '01', '01'),
          ('2024', '0101', '01', '01', 'horse02', '02', '02'),
          ('2024', '0101', '01', '01', 'horse03', '03', '00'),
          ('2024', '0101', '01', '01', 'horse04', '', '03'),
          ('2019', '1231', '01', '01', 'horse01', '01', '01')
        """
    )
    conn.execute(
        """
        create temp table horse_pedigree as select * from (values
          ('horse01', 'sire01'), ('horse02', 'sire02'),
          ('horse03', 'sire01'), ('horse04', 'sire01')
        ) pedigree(ketto_toroku_bango, sire_id)
        """
    )
    conn.execute("create temp table target_sires as select 'sire01' as sire_id")

    subject.stage_sire_race_history(conn, "20200101", "jra", focused_target=True)

    assert conn.execute(
        """
        select sire_id, keibajo_code, surface_type, kyori, race_date,
          finish_position
        from sire_race_history
        """
    ).fetchall() == [("sire01", "01", "turf", 1200, "20240101", 1)]
    conn.close()


def test_stage_svs_cumul_uses_strict_prior_target_join() -> None:
    conn = FakeConn()
    subject.stage_svs_cumul(conn)
    body = " ".join(conn.statements)
    assert "from target_sire_cells" in body
    assert "left join sire_race_history" in body
    assert "h.race_date < t.race_date" in body


def test_stage_svs_cumul_preserves_cross_distance_daily_totals() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create temp table target_sire_cells as select * from (values
          ('sire01', '01', 'turf', 1600, '20240201')
        ) target(sire_id, keibajo_code, surface_type, kyori, race_date)
        """
    )
    conn.execute(
        """
        create temp table sire_race_history as select * from (values
          ('sire01', '01', 'turf', 1200, '20240101', 1),
          ('sire01', '01', 'turf', 1400, '20240101', 3),
          ('sire01', '01', 'turf', 1600, '20240201', 5),
          ('sire01', '01', 'turf', 1800, '20240301', 1)
        ) history(sire_id, keibajo_code, surface_type, kyori, race_date,
                  finish_position)
        """
    )

    subject.stage_svsd_cumul(conn)
    subject.stage_svs_cumul(conn)

    assert conn.execute(
        """
        select past_starts, past_wins, past_places from sire_svs_cumul
        where race_date = '20240201'
        """
    ).fetchall() == [(2, 1, 2)]
    conn.close()


def test_upcoming_target_gets_strictly_prior_non_null_features(
    tmp_path: Path,
) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create temp table horse_pedigree as select * from (values
          ('target01', 'sire01', 'damsire01')
        ) pedigree(ketto_toroku_bango, sire_id, damsire_id)
        """
    )
    partition_dir = tmp_path / "race_year=2024"
    partition_dir.mkdir()
    conn.execute(
        f"""
        copy (select 'target01' ketto_toroku_bango, '01' keibajo_code,
          '10' track_code, 1600 kyori, '20240201' race_date, 2024 race_year)
        to '{(partition_dir / "part.parquet").as_posix()}' (format parquet)
        """
    )
    input_glob = f"{tmp_path.as_posix()}/race_year=*/*.parquet"
    subject.stage_target_sire_cells(conn, input_glob)
    conn.execute(
        """
        create temp table sire_race_history as select * from (values
          ('sire01', '01', 'turf', 1600, '20240101', 1),
          ('sire01', '01', 'turf', 1600, '20240201', 9),
          ('sire01', '01', 'turf', 1600, '20240301', 1),
          ('sire01', '01', 'turf', 1400, '20240115', 2)
        ) history(sire_id, keibajo_code, surface_type, kyori, race_date,
                  finish_position)
        """
    )

    subject.stage_svsd_cumul(conn)
    subject.stage_svs_cumul(conn)

    assert conn.execute(
        f"""
        select sire_venue_surface_dist_runs,
          sire_venue_surface_dist_win_rate,
          sire_venue_surface_dist_place_rate,
          sire_venue_surface_win_rate,
          sire_venue_surface_place_rate
        from ({subject.append_features_sql(input_glob)})
        """
    ).fetchall() == [(1, 1.0, 1.0, 0.5, 1.0)]
    conn.close()


def test_main_scopes_whole_day_history_to_input_sires(tmp_path: Path) -> None:
    conn = FakeConn()
    with (
        patch.object(
            subject,
            "parse_args",
            return_value=Namespace(
                category="nar",
                from_date="20100101",
                input_dir=tmp_path / "in",
                memory_limit="1GB",
                output_dir=tmp_path / "out",
                pg_url="r2-catalog://pc-keiba",
                target_race=None,
                threads=1,
            ),
        ),
        patch.object(subject.duckdb, "connect", return_value=conn),
        patch.object(subject, "apply_to_connection"),
        patch.object(subject, "install_and_attach_pg"),
        patch.object(subject, "stage_horse_pedigree") as stage_pedigree,
        patch.object(subject, "stage_target_sires") as stage_target_sires,
        patch.object(subject, "stage_target_sire_cells") as stage_target_cells,
        patch.object(subject, "target_history_upper_bound", return_value="20260826"),
        patch.object(subject, "stage_sire_race_history") as stage_sire_race_history,
        patch.object(subject, "stage_svsd_cumul"),
        patch.object(subject, "stage_svs_cumul"),
        patch.object(subject, "append_features_sql", return_value="select 1"),
        patch.object(subject, "write_partitioned"),
    ):
        subject.main()

    stage_target_sires.assert_called_once_with(
        conn, f"{(tmp_path / 'in').as_posix()}/race_year=*/*.parquet"
    )
    stage_pedigree.assert_called_once_with(conn)
    stage_target_cells.assert_called_once_with(
        conn, f"{(tmp_path / 'in').as_posix()}/race_year=*/*.parquet"
    )
    stage_sire_race_history.assert_called_once_with(
        conn, "20100101", "nar", focused_target=True, to_date="20260826"
    )


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
