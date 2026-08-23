from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import cast

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-jra-jockey-pedigree-cell-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "add_jra_jockey_pedigree_cell_features", MODULE_PATH
)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_jra_jockey_pedigree_cell_features"] = subject
_spec.loader.exec_module(subject)


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, query: str) -> object:
        self.statements.append(query)
        return None


def test_parse_args_defaults_match_gate(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.from_date == "20080101"


def test_parse_args_accepts_resource_args(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--threads",
            "4",
            "--memory-limit",
            "6GB",
        ]
    )
    assert args.threads == 4
    assert args.memory_limit == "6GB"


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "02:01",
        ]
    )
    assert args.target_race == "02:01"


def test_stage_jockey_features_uses_race_grain_prior_windows() -> None:
    conn = FakeConn()
    subject.stage_jockey_features(conn, 2008, "/tmp/in/race_year=*/*.parquet")
    sql = conn.statements[0]
    assert "race_seq" in sql
    assert "jo_race" in sql
    assert "jvn_race" in sql
    assert "rows between unbounded preceding and 1 preceding" in sql
    assert "count(is_win) as n" in sql
    assert "lpad(cast(cast(se.umaban as int)" not in sql


def test_stage_pedigree_features_uses_race_grain_prior_windows() -> None:
    conn = FakeConn()
    subject.stage_pedigree_features(conn, 2008, "/tmp/in/race_year=*/*.parquet")
    sql = " ".join(conn.statements)
    assert "go_race" in sql
    assert "gds_race" in sql
    assert "scs_race" in sql
    assert "rows between unbounded preceding and 1 preceding" in sql
    assert "count(is_win) as n" in sql
    assert "lpad(cast(cast(se.umaban as int)" not in sql


def test_stage_jockey_features_focused_adds_unfinished_target_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConn()

    def fake_fetch(_: object, sql: str) -> list[object]:
        assert "jra_jockey_cell_target_rows" in sql
        return cast(list[object], ["05339"])

    monkeypatch.setattr(subject, "_fetch_scalar_values", fake_fetch)
    subject.stage_jockey_features(
        conn, 2008, "/tmp/in/race_year=*/*.parquet", focused_target=True
    )

    sql = " ".join(conn.statements)
    assert "jra_jockey_cell_target_rows" in sql
    assert "cast(null as double) as is_win" in sql
    assert "se.kishu_code in ('05339')" in sql
    assert "left join history h" in sql
    assert "h.race_seq < t.race_seq" in sql
    assert "group by all" in sql
    assert "rows between unbounded preceding and 1 preceding" not in sql


def test_stage_pedigree_features_focused_filters_target_pedigree_entities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConn()
    responses = iter(
        [
            cast(list[object], ["sire-a"]),
            cast(list[object], ["gsire-b"]),
            cast(list[object], ["1"]),
        ]
    )

    def fake_fetch(_: object, sql: str) -> list[object]:
        assert "jra_pedigree_cell_target_rows" in sql
        return next(responses)

    monkeypatch.setattr(subject, "_fetch_scalar_values", fake_fetch)
    subject.stage_pedigree_features(
        conn, 2008, "/tmp/in/race_year=*/*.parquet", focused_target=True
    )

    sql = " ".join(conn.statements)
    assert "jra_pedigree_cell_target_rows" in sql
    assert "cast(null as double) as is_win" in sql
    assert "sire in ('sire-a')" in sql
    assert "gsire in ('gsire-b')" in sql
    assert "keito_id in ('1')" in sql
    assert "union all" in sql
    assert "rows between unbounded preceding and 1 preceding" in sql


def test_append_features_sql_dedupes_feature_tables_before_join() -> None:
    sql = subject.append_features_sql("/tmp/in/race_year=*/*.parquet")
    assert "from jra_jockey_cell_features" in sql
    assert "from jra_pedigree_cell_features" in sql
    assert (
        "group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango"
        in sql
    )
    assert "left join j\n" in sql
    assert "left join p\n" in sql


def test_append_features_sql_contains_candidate_feature_names() -> None:
    sql = subject.append_features_sql("/tmp/in/race_year=*/*.parquet")
    for name in [
        "jk_venue_nichime_win_eb",
        "jk_fullcell_edge",
        "gsire_dist_surface_win_eb",
        "sire_class_surface_edge",
        "keito_dist_surface_win_eb",
        "gsire_dist_surface_edge_rank_in_race",
    ]:
        assert name in sql


def test_stage_features_keep_rank_nulls_for_missing_priors() -> None:
    conn = FakeConn()
    subject.stage_jockey_features(conn, 2008, "/tmp/in/race_year=*/*.parquet")
    subject.stage_pedigree_features(conn, 2008, "/tmp/in/race_year=*/*.parquet")
    sql = " ".join(conn.statements)
    assert "case when jk_venue_nichime_win_eb is null then null else" in sql
    assert "case when jk_venue_nichime_edge is null then null else" in sql
    assert "case when gsire_dist_surface_win_eb is null then null else" in sql
    assert "case when gsire_dist_surface_edge is null then null else" in sql


def test_focused_pedigree_features_match_bulk_target(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("create schema pg")
    conn.execute(
        "create table pg.jvd_hn (bamei varchar, hanshoku_toroku_bango varchar)"
    )
    conn.execute(
        "create table pg.jvd_bt (hanshoku_toroku_bango varchar, keito_id integer)"
    )
    conn.execute(
        "create table pg.jvd_se (kaisai_nen varchar, kaisai_tsukihi varchar, "
        "keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar, "
        "kakutei_chakujun varchar)"
    )
    conn.execute(
        "create table pg.jvd_ra (kaisai_nen varchar, kaisai_tsukihi varchar, "
        "keibajo_code varchar, race_bango varchar, kyoso_joken_code varchar, "
        "kyori integer, track_code varchar)"
    )
    conn.execute(
        "create table pg.jvd_um (ketto_toroku_bango varchar, ketto_joho_01b varchar, "
        "ketto_joho_03b varchar)"
    )
    conn.execute("insert into pg.jvd_hn values ('SIRE', 'HS')")
    conn.execute("insert into pg.jvd_bt values ('HS', 1)")
    conn.execute(
        "insert into pg.jvd_se values "
        "('2025','0101','01','01','H1','01'),"
        "('2025','0201','02','02','H2','04'),"
        "('2026','0823','07','12','HT','02')"
    )
    conn.execute(
        "insert into pg.jvd_ra values "
        "('2025','0101','01','01','A',1600,'10'),"
        "('2025','0201','02','02','A',1600,'10'),"
        "('2026','0823','07','12','A',1600,'10')"
    )
    conn.execute(
        "insert into pg.jvd_um values "
        "('H1','SIRE','GSIRE'),('H2','SIRE','GSIRE'),('HT','SIRE','GSIRE')"
    )

    partition_dir = tmp_path / "race_year=2026"
    partition_dir.mkdir()
    target_path = (partition_dir / "target.parquet").as_posix().replace("'", "''")
    conn.execute(
        "copy (select '2026' as kaisai_nen, '0823' as kaisai_tsukihi, "
        "'07' as keibajo_code, '12' as race_bango, 'HT' as ketto_toroku_bango) "
        f"to '{target_path}' (format parquet)"
    )
    input_glob = f"{tmp_path.as_posix()}/race_year=*/*.parquet"
    feature_names = """
      gsire_dist_surface_win_eb, gsire_dist_surface_top3_eb,
      gsire_venue_win_eb, gsire_dist_surface_edge, gsire_dist_surface_logn,
      sire_class_surface_win_eb, sire_class_surface_edge,
      keito_dist_surface_win_eb, gsire_dist_surface_win_rank_in_race,
      gsire_dist_surface_edge_rank_in_race
    """

    subject.stage_pedigree_features(conn, 2008, input_glob)
    bulk = conn.execute(
        f"select {feature_names} from jra_pedigree_cell_features "
        "where kaisai_nen='2026' and kaisai_tsukihi='0823' "
        "and keibajo_code='07' and race_bango='12' and ketto_toroku_bango='HT'"
    ).fetchone()

    subject.stage_pedigree_features(conn, 2008, input_glob, focused_target=True)
    focused = conn.execute(
        f"select {feature_names} from jra_pedigree_cell_features "
        "where ketto_toroku_bango='HT'"
    ).fetchone()

    assert bulk is not None
    assert focused == pytest.approx(bulk)
    conn.close()
