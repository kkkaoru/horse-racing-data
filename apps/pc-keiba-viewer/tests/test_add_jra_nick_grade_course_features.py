from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-jra-nick-grade-course-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_jra_nick_grade_course_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_jra_nick_grade_course_features"] = subject
_spec.loader.exec_module(subject)

# 2024-01-01 G1 (turf 1600 at 05): H1 wins in 1:33.0, H2 2nd, H4 3rd, H5 4th.
# 2024-02-01 race 01 (1勝, same course) is the target: H1 and the debutant H3.
# 2024-02-01 race 11 (G1, same course) finishes the same day and must not leak.
PG_FIXTURE_SQL = """
create table pg.jvd_ra (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
  race_bango varchar, kyori varchar, track_code varchar, grade_code varchar,
  kyoso_joken_code varchar, shusso_tosu varchar, babajotai_code_shiba varchar,
  babajotai_code_dirt varchar);
insert into pg.jvd_ra values
  ('2024','0101','05','11','1600','11','A','999','04','1','0'),
  ('2024','0201','05','01','1600','11','','005','02','1','0'),
  ('2024','0201','05','11','1600','11','A','999','01','1','0');
create table pg.jvd_se (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
  race_bango varchar, ketto_toroku_bango varchar, kakutei_chakujun varchar,
  soha_time varchar, corner_4 varchar);
insert into pg.jvd_se values
  ('2024','0101','05','11','H1','01','1330','01'),
  ('2024','0101','05','11','H2','02','1335','02'),
  ('2024','0101','05','11','H4','03','1336','03'),
  ('2024','0101','05','11','H5','04','1340','04'),
  ('2024','0201','05','01','H1','00','0000','00'),
  ('2024','0201','05','01','H3','00','0000','00'),
  ('2024','0201','05','11','H2','01','1200','01');
create table pg.jvd_um (ketto_toroku_bango varchar, ketto_joho_01a varchar,
  ketto_joho_01b varchar, ketto_joho_05a varchar, ketto_joho_05b varchar);
insert into pg.jvd_um values
  ('H1','S1','Sire One','B1','Bms One'),
  ('H2','S1','Sire One','B2','Bms Two'),
  ('H3','S1','Sire One','B1','Bms One'),
  ('H4','S2','Sire Two','B2','Bms Two'),
  ('H5','S1','Sire One','B2','Bms Two');
create table pg.jvd_hn (bamei varchar, hanshoku_toroku_bango varchar);
insert into pg.jvd_hn values ('Bms One','HB1');
create table pg.jvd_bt (hanshoku_toroku_bango varchar, keito_id varchar);
insert into pg.jvd_bt values ('HB1','K1');
"""


def _prepare(tmp_path: Path) -> tuple[duckdb.DuckDBPyConnection, str]:
    con = duckdb.connect(":memory:")
    con.execute("attach ':memory:' as pg")
    con.execute(PG_FIXTURE_SQL)
    (tmp_path / "in" / "race_year=2024").mkdir(parents=True)
    con.execute(
        "copy (select * from (values ('2024','0201','05',1,'H1'),('2024','0201','05',1,'H3'))"
        " t(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango))"
        f" to '{(tmp_path / 'in' / 'race_year=2024' / 'p.parquet').as_posix()}' (format parquet)"
    )
    return con, f"{(tmp_path / 'in').as_posix()}/race_year=*/*.parquet"


def test_parse_args_defaults(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert (args.input_dir, args.output_dir, args.from_date) == (
        tmp_path / "in",
        tmp_path / "out",
        "20080101",
    )


def test_build_uses_only_earlier_days(tmp_path: Path) -> None:
    con, input_glob = _prepare(tmp_path)
    subject.build(con, 2008, input_glob)
    rows = con.execute(
        "select ketto_toroku_bango, round(nick_win_eb, 6), round(nick_top3_eb, 6),"
        " round(nick_top3_lift, 6), round(nick_logn, 6), round(nick_surface_top3_eb, 6),"
        " round(nick_dist_surface_top3_eb, 6), round(sire_bms_keito_surface_top3_lift, 6),"
        " nick_top3_lift_rank_in_race, round(hg_course_logn, 6),"
        " round(hg_sire_course_top3_eb, 6), round(hg_sire_course_top3_lift, 6),"
        " hg_time_gap, hg_style_gap, hg_time_gap_rank_in_race"
        " from jra_nick_grade_course_features order by ketto_toroku_bango"
    ).fetchall()
    assert rows == [
        ("H1", 0.365079, 0.68254, 0.015873, 0.693147, 0.697657, 0.712054, 0.00813,
         1.5, 0.693147, 0.666667, 0.0, 0.0, 0.25, 1.0),
        ("H3", 0.365079, 0.68254, 0.015873, 0.693147, 0.697657, 0.712054, 0.00813,
         1.5, 0.693147, 0.666667, 0.0, None, None, None),
    ]


def test_par_gap_measures_past_runs_against_open_class_winners(tmp_path: Path) -> None:
    con, input_glob = _prepare(tmp_path)
    # Three earlier open-class wins on the same course and going set a 1:33.0 par;
    # a heavy-going OP win must not enter it.
    con.execute(
        "insert into pg.jvd_ra values"
        " ('2023','1101','05','11','1600','11','','999','01','1','0'),"
        " ('2023','1102','05','11','1600','11','','999','01','1','0'),"
        " ('2023','1103','05','11','1600','11','','999','01','1','0'),"
        " ('2023','1104','05','11','1600','11','','999','01','4','0')"
    )
    con.execute(
        "insert into pg.jvd_se values"
        " ('2023','1101','05','11','P1','01','1330','01'),"
        " ('2023','1102','05','11','P2','01','1330','01'),"
        " ('2023','1103','05','11','P3','01','1330','01'),"
        " ('2023','1104','05','11','P4','01','1500','01')"
    )
    con.execute(
        "insert into pg.jvd_um values ('P1','S9','x','B9','y'), ('P2','S9','x','B9','y'),"
        " ('P3','S9','x','B9','y'), ('P4','S9','x','B9','y')"
    )
    subject.build(con, 2008, input_glob)
    assert con.execute(
        "select ketto_toroku_bango, hg_par_gap_mean, hg_par_gap_best, hg_par_gap_best_rank_in_race"
        " from jra_nick_grade_course_features order by ketto_toroku_bango"
    ).fetchall() == [("H1", 0.0, 0.0, 1.0), ("H3", None, None, None)]


def test_top_class_race_has_no_higher_grade_features(tmp_path: Path) -> None:
    con, input_glob = _prepare(tmp_path)
    con.execute("update pg.jvd_ra set grade_code = 'A' where race_bango = '01'")
    subject.build(con, 2008, input_glob)
    assert con.execute(
        "select count(hg_course_logn), count(hg_sire_course_top3_eb), count(nick_win_eb)"
        " from jra_nick_grade_course_features"
    ).fetchall() == [(0, 0, 2)]


def test_main_appends_features_partitioned_by_year(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare(tmp_path)[0].close()
    pg_file = tmp_path / "pg.duckdb"
    seed = duckdb.connect(str(pg_file))
    seed.execute(PG_FIXTURE_SQL)
    seed.close()

    def fake_attach(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
        assert pg_url == "postgresql://example/db"
        con.execute(f"attach '{pg_file.as_posix()}' as pg (read_only)")

    monkeypatch.setattr(subject, "attach_source_catalog", fake_attach)
    out = tmp_path / "out"
    (out / "stale").mkdir(parents=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(out),
            "--pg-url",
            "postgresql://example/db",
            "--threads",
            "1",
        ],
    )
    subject.main()
    assert sorted(p.name for p in out.iterdir()) == ["race_year=2024"]
    assert duckdb.sql(
        f"select ketto_toroku_bango, hg_time_gap from read_parquet('{out.as_posix()}/*/*.parquet')"
        " order by 1"
    ).fetchall() == [("H1", 0.0), ("H3", None)]
