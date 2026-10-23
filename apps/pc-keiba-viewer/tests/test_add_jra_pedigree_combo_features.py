from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-jra-pedigree-combo-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_jra_pedigree_combo_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_jra_pedigree_combo_features"] = subject
_spec.loader.exec_module(subject)

# 2024-01-01 turf 1600 at 05: H1 (S1 x B1) wins, H2 (S1 x B2) 2nd, H4 (S2 x B2) 3rd, H5 4th.
# 2024-02-01 race 01 is the target: H1 again and its full sibling H3 (same dam D1).
# 2024-02-01 race 11 finishes on the target day and must not leak.
# Lineage: S1 -> SS1 (sire's sire) is in jvd_bt as 'K1aa01'; B1's sire BS1 is 'K1aa02'.
PG_FIXTURE_SQL = """
create table pg.jvd_ra (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
  race_bango varchar, kyori varchar, track_code varchar);
insert into pg.jvd_ra values
  ('2024','0101','05','11','1600','11'),
  ('2024','0201','05','01','1600','11'),
  ('2024','0201','05','11','1600','11');
create table pg.jvd_se (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
  race_bango varchar, ketto_toroku_bango varchar, kakutei_chakujun varchar);
insert into pg.jvd_se values
  ('2024','0101','05','11','H1','01'),
  ('2024','0101','05','11','H2','02'),
  ('2024','0101','05','11','H4','03'),
  ('2024','0101','05','11','H5','04'),
  ('2024','0201','05','01','H1','00'),
  ('2024','0201','05','01','H3','00'),
  ('2024','0201','05','11','H2','01');
create table pg.jvd_um (ketto_toroku_bango varchar,
  ketto_joho_01a varchar, ketto_joho_02a varchar, ketto_joho_03a varchar,
  ketto_joho_04a varchar, ketto_joho_05a varchar, ketto_joho_06a varchar,
  ketto_joho_07a varchar, ketto_joho_08a varchar, ketto_joho_09a varchar,
  ketto_joho_10a varchar, ketto_joho_11a varchar, ketto_joho_12a varchar,
  ketto_joho_13a varchar, ketto_joho_14a varchar);
insert into pg.jvd_um values
  ('H1','S1','D1','SS1','SD1','B1','BD1','SSS','X1','X2','X3','BS1','X4','X5','X6'),
  ('H2','S1','D2','SS1','SD1','B2','BD2','SSS','X1','X2','X3','BS2','X4','X5','X6'),
  ('H3','S1','D1','SS1','SD1','B1','BD1','SSS','X1','X2','X3','BS1','X4','X5','SSS'),
  ('H4','S2','D3','SS2','SD2','B2','BD2','SSX','X1','X2','X3','BS2','X4','X5','X6'),
  ('H5','S1','D4','SS1','SD1','B2','BD2','SSS','X1','X2','X3','BS2','X4','X5','X6');
create table pg.jvd_hn (hanshoku_toroku_bango varchar, ketto_joho_01a varchar);
insert into pg.jvd_hn values ('S1','SS1'), ('SS1','SSS'), ('B1','BS1'), ('BS1',' ');
create table pg.jvd_bt (hanshoku_toroku_bango varchar, keito_id varchar);
insert into pg.jvd_bt values ('SS1','K1aa01  '), ('BS1','K1aa02'), ('B2','K1aa01');
"""
SELECTED = (
    "select ketto_toroku_bango, round(ped_s_bms_dist_top3_lift, 6),"
    " round(ped_ss_bms_surf_top3_lift, 6), round(ped_lines_surf_top3_lift, 6),"
    " round(ped_pair_logn, 6), round(ped_dam_sib_top3_eb, 6), round(ped_dam_sib_logn, 6),"
    " ped_inbreed_dups, ped_same_broad_line, ped_s_bms_dist_top3_lift_rank_in_race"
    " from jra_pedigree_combo_features where kaisai_tsukihi = '0201' and race_bango = '01'"
    " order by ketto_toroku_bango"
)


def _prepare(tmp_path: Path, rows: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    con = duckdb.connect(":memory:")
    con.execute("attach ':memory:' as pg")
    con.execute(PG_FIXTURE_SQL)
    (tmp_path / "in" / "race_year=2024").mkdir(parents=True)
    con.execute(
        f"copy (select * from (values {rows})"
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
    con, input_glob = _prepare(
        tmp_path, "('2024','0201','05',1,'H1'),('2024','0201','05',1,'H3')"
    )
    subject.build(con, 2008, input_glob)
    # S1 alone: 3 runs, 2 top-3 -> (2 + 30*.212)/33 = .253333; B1 alone: (1 + 6.36)/31.
    # Expected .278753; pair S1 x B1 (1 run, 1 top-3) -> (1 + 30*.278753)/31 - .278753.
    # H1's dam D1 has no other finished runner; H3's sibling H1 ran once (won).
    # Lines K1aa01 vs K1aa02 differ at the broad (6-char) level.
    assert con.execute(SELECTED).fetchall() == [
        ("H1", 0.023266, 0.023266, 0.023266, 0.693147, 0.212, 0.0, 0.0, 0.0, 1.5),
        ("H3", 0.023266, 0.023266, 0.023266, 0.693147, 0.237419, 0.693147, 1.0, 0.0, 1.5),
    ]


def test_one_day_card_matches_training_values(tmp_path: Path) -> None:
    serve, serve_glob = _prepare(
        tmp_path / "serve", "('2024','0201','05',1,'H1'),('2024','0201','05',1,'H3')"
    )
    subject.build(serve, 2008, serve_glob)
    train, train_glob = _prepare(
        tmp_path / "train",
        "('2024','0101','05',11,'H1'),('2024','0101','05',11,'H2'),"
        "('2024','0101','05',11,'H4'),('2024','0101','05',11,'H5'),"
        "('2024','0201','05',1,'H1'),('2024','0201','05',1,'H3'),('2024','0201','05',11,'H2')",
    )
    subject.build(train, 2008, train_glob)
    assert serve.execute(SELECTED).fetchall() == train.execute(SELECTED).fetchall()


def test_lineage_walks_paternal_line_to_nearest_recorded_stallion(tmp_path: Path) -> None:
    con, input_glob = _prepare(tmp_path, "('2024','0201','05',1,'H1')")
    subject.build(con, 2008, input_glob)
    assert con.execute("select id, keito from lineage order by id").fetchall() == [
        ("B1", "K1aa02"),
        ("B2", "K1aa01"),
        ("S1", "K1aa01"),
        ("SS1", "K1aa01"),
    ]


def test_main_appends_features_partitioned_by_year(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare(tmp_path, "('2024','0201','05',1,'H1'),('2024','0201','05',1,'H3')")[0].close()
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
        "select ketto_toroku_bango, round(ped_dam_sib_logn, 6)"
        f" from read_parquet('{out.as_posix()}/*/*.parquet') order by 1"
    ).fetchall() == [("H1", 0.0), ("H3", 0.693147)]
