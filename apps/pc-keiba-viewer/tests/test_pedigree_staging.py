from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "pedigree_staging", SCRIPTS_DIR / "pedigree_staging.py"
)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["pedigree_staging"] = subject
_spec.loader.exec_module(subject)


def _make_con_with_mock_pg(
    jvd_rows: list[tuple[str | None, str, str]],
    nvd_rows: list[tuple[str | None, str, str]],
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with pg.jvd_um and pg.nvd_um mocked.

    Each row is (ketto_toroku_bango, ketto_joho_01a, ketto_joho_04a).
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    # jvd_um mock
    con.execute(
        """
        create table pg.jvd_um (
          ketto_toroku_bango varchar,
          ketto_joho_01a varchar,
          ketto_joho_04a varchar
        )
        """
    )
    for row in jvd_rows:
        con.execute(
            "insert into pg.jvd_um values (?, ?, ?)",
            list(row),
        )
    # nvd_um mock
    con.execute(
        """
        create table pg.nvd_um (
          ketto_toroku_bango varchar,
          ketto_joho_01a varchar,
          ketto_joho_04a varchar
        )
        """
    )
    for row in nvd_rows:
        con.execute(
            "insert into pg.nvd_um values (?, ?, ?)",
            list(row),
        )
    return con


def test_jra_horse_gets_sire_from_jvd_um() -> None:
    """A horse that exists only in jvd_um (JRA) gets sire_id / damsire_id populated."""
    con = _make_con_with_mock_pg(
        jvd_rows=[("JRA_HORSE_1", "SIRE_JRA", "DAMSIRE_JRA")],
        nvd_rows=[],
    )
    subject.stage_horse_pedigree(con)
    rows = con.execute(
        "select ketto_toroku_bango, sire_id, damsire_id from horse_pedigree"
    ).fetchall()
    con.close()
    assert rows == [("JRA_HORSE_1", "SIRE_JRA", "DAMSIRE_JRA")]


def test_nar_horse_gets_sire_from_nvd_um() -> None:
    """A horse that exists only in nvd_um (NAR/Ban-ei) gets non-NULL sire_id.

    This is the bug fix: previously only jvd_um was queried, so NAR horses
    silently received sire_id = NULL.
    """
    con = _make_con_with_mock_pg(
        jvd_rows=[],
        nvd_rows=[("NAR_HORSE_1", "SIRE_NAR", "DAMSIRE_NAR")],
    )
    subject.stage_horse_pedigree(con)
    rows = con.execute(
        "select ketto_toroku_bango, sire_id, damsire_id from horse_pedigree"
    ).fetchall()
    con.close()
    assert rows == [("NAR_HORSE_1", "SIRE_NAR", "DAMSIRE_NAR")]


def test_horse_in_both_tables_jvd_um_wins() -> None:
    """When a ketto_toroku_bango appears in both jvd_um and nvd_um, jvd_um wins."""
    con = _make_con_with_mock_pg(
        jvd_rows=[("DUAL_HORSE", "SIRE_JVD", "DAMSIRE_JVD")],
        nvd_rows=[("DUAL_HORSE", "SIRE_NVD", "DAMSIRE_NVD")],
    )
    subject.stage_horse_pedigree(con)
    rows = con.execute(
        "select ketto_toroku_bango, sire_id, damsire_id from horse_pedigree"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0] == ("DUAL_HORSE", "SIRE_JVD", "DAMSIRE_JVD")


def test_blank_sire_id_becomes_null() -> None:
    """Blank or whitespace-only ketto_joho_01a / ketto_joho_04a becomes NULL."""
    con = _make_con_with_mock_pg(
        jvd_rows=[("HORSE_BLANK_SIRE", "   ", "")],
        nvd_rows=[],
    )
    subject.stage_horse_pedigree(con)
    rows = con.execute(
        "select sire_id, damsire_id from horse_pedigree"
    ).fetchall()
    con.close()
    assert rows == [(None, None)]


def test_null_ketto_toroku_bango_excluded() -> None:
    """Rows with NULL ketto_toroku_bango are excluded from the staging table."""
    con = _make_con_with_mock_pg(
        jvd_rows=[(None, "SOME_SIRE", "SOME_DAMSIRE")],
        nvd_rows=[(None, "ANOTHER_SIRE", "ANOTHER_DAMSIRE")],
    )
    subject.stage_horse_pedigree(con)
    count = con.execute("select count(*) from horse_pedigree").fetchone()
    con.close()
    assert count is not None
    assert count[0] == 0


def test_mixed_jra_and_nar_horses_both_populated() -> None:
    """JRA and NAR horses in the same batch both get non-NULL sire_id."""
    con = _make_con_with_mock_pg(
        jvd_rows=[("JRA_A", "SIRE_A", "DAMSIRE_A")],
        nvd_rows=[("NAR_B", "SIRE_B", "DAMSIRE_B")],
    )
    subject.stage_horse_pedigree(con)
    rows = con.execute(
        "select ketto_toroku_bango, sire_id, damsire_id from horse_pedigree order by ketto_toroku_bango"
    ).fetchall()
    con.close()
    assert ("JRA_A", "SIRE_A", "DAMSIRE_A") in rows
    assert ("NAR_B", "SIRE_B", "DAMSIRE_B") in rows
    assert len(rows) == 2
