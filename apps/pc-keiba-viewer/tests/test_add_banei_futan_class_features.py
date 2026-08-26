from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-banei-futan-class-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_banei_futan_class_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_banei_futan_class_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_banei_keibajo_constant() -> None:
    assert subject.BAN_EI_KEIBAJO == "83"


def test_futan_hex_parse_includes_hex_prefix() -> None:
    assert "'0x'" in subject.FUTAN_HEX_PARSE
    assert "as integer" in subject.FUTAN_HEX_PARSE


def test_futan_bucket_sql_has_7_buckets() -> None:
    sql = subject.FUTAN_BUCKET_SQL
    # Buckets 0..5 use `then <n>`; bucket 6 is `else 6` (catch-all)
    for bucket_val in ("0", "1", "2", "3", "4", "5"):
        assert f"then {bucket_val}" in sql, f"missing bucket {bucket_val}"
    assert "else 6" in sql, "missing catch-all bucket 6"


def test_stage_horse_pedigree_uses_ketto_joho_05a_for_damsire() -> None:
    """damsire_id must come from ketto_joho_05a (damsire), not ketto_joho_04a (dam)."""
    import inspect

    src = inspect.getsource(subject.stage_horse_pedigree)
    assert "ketto_joho_05a" in src
    assert "ketto_joho_04a" not in src


def test_stage_horse_pedigree_falls_back_to_nvd_nu_and_scopes_lineage() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        "create table pg.nvd_um (ketto_toroku_bango varchar, ketto_joho_01a varchar, ketto_joho_05a varchar)"
    )
    con.execute(
        "create table pg.nvd_nu (ketto_toroku_bango varchar, ketto_joho_01a varchar, ketto_joho_05a varchar)"
    )
    con.execute(
        """
        create table banei_targets as
        select 'TARGET'::varchar as ketto_toroku_bango
        """
    )
    con.execute("insert into pg.nvd_nu values ('TARGET', 'SIRE-1', 'DAMSIRE-1')")
    con.execute("insert into pg.nvd_um values ('PROGENY', 'SIRE-1', 'OTHER')")
    con.execute("insert into pg.nvd_um values ('UNRELATED', 'SIRE-X', 'DAMSIRE-X')")

    subject.stage_horse_pedigree(con)

    rows = con.execute(
        "select ketto_toroku_bango, sire_id, damsire_id from banei_pedigree order by 1"
    ).fetchall()
    assert rows == [
        ("PROGENY", "SIRE-1", "OTHER"),
        ("TARGET", "SIRE-1", "DAMSIRE-1"),
    ]
    con.close()


def test_append_features_sql_contains_futan_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "current_futan_class" in sql
    assert "horse_futan_class_career_starts" in sql
    assert "horse_futan_class_career_win_rate" in sql
    assert "horse_futan_class_career_top3_rate" in sql
    assert "sire_futan_class_win_rate" in sql
    assert "damsire_futan_class_win_rate" in sql
    assert "field_futan_class_avg" in sql
    assert "self_futan_minus_field_avg" in sql


def test_append_features_uses_latest_strictly_prior_cumulative_row(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    input_path = tmp_path / "input.parquet"
    con.execute(
        f"""
        copy (
          select 'nar'::varchar as source, '2026'::varchar as kaisai_nen,
            '0826'::varchar as kaisai_tsukihi, '83'::varchar as keibajo_code,
            '01'::varchar as race_bango, 'HORSE'::varchar as ketto_toroku_bango,
            '20260826'::varchar as race_date
        ) to '{input_path.as_posix()}' (format parquet)
        """
    )
    con.execute(
        """
        create table current_race_futan as
        select 'nar'::varchar as source, '2026'::varchar as kaisai_nen,
          '0826'::varchar as kaisai_tsukihi, '83'::varchar as keibajo_code,
          '01'::varchar as race_bango, 'HORSE'::varchar as ketto_toroku_bango,
          5::integer as futan_class, 4.5::double as field_futan_class_avg
        """
    )
    con.execute(
        "create table banei_pedigree as select 'HORSE'::varchar as ketto_toroku_bango, 'SIRE'::varchar as sire_id, 'DAMSIRE'::varchar as damsire_id"
    )
    con.execute(
        """
        create table horse_futan_cumul as
        select 'nar'::varchar as source, 'HORSE'::varchar as ketto_toroku_bango,
          5::integer as futan_class, '20260820'::varchar as race_date,
          4::bigint as past_starts, 2::hugeint as past_wins, 3::hugeint as past_top3
        """
    )
    con.execute(
        "create table sire_futan_cumul as select 'SIRE'::varchar as sire_id, 5::integer as futan_class, '20260819'::varchar as race_date, 8::bigint as past_starts, 2::hugeint as past_wins"
    )
    con.execute(
        "create table damsire_futan_cumul as select 'DAMSIRE'::varchar as damsire_id, 5::integer as futan_class, '20260818'::varchar as race_date, 10::bigint as past_starts, 1::hugeint as past_wins"
    )

    row = con.execute(subject.append_features_sql(input_path.as_posix())).fetchone()

    assert row is not None
    names = [item[0] for item in con.description]
    result = dict(zip(names, row, strict=True))
    assert result["horse_futan_class_career_starts"] == 4
    assert result["horse_futan_class_career_win_rate"] == 0.5
    assert result["horse_futan_class_career_top3_rate"] == 0.75
    assert result["sire_futan_class_starts"] == 8
    assert result["damsire_futan_class_starts"] == 10
    con.close()


def test_futan_staging_scopes_history_and_reads_upcoming_runner() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        "create table pg.nvd_um (ketto_toroku_bango varchar, ketto_joho_01a varchar, ketto_joho_05a varchar)"
    )
    con.execute(
        "create table pg.nvd_nu (ketto_toroku_bango varchar, ketto_joho_01a varchar, ketto_joho_05a varchar)"
    )
    con.execute(
        "create table pg.nvd_se (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar, futan_juryo varchar, kakutei_chakujun varchar)"
    )
    con.execute(
        """
        create table banei_targets as
        select 'nar'::varchar as source, '2026'::varchar as kaisai_nen,
          '0826'::varchar as kaisai_tsukihi, '83'::varchar as keibajo_code,
          '01'::varchar as race_bango, 'HORSE'::varchar as ketto_toroku_bango,
          '20260826'::varchar as race_date
        """
    )
    con.execute("insert into pg.nvd_nu values ('HORSE', 'SIRE', 'DAMSIRE')")
    con.execute(
        "insert into pg.nvd_se values ('2026', '0820', '83', '01', 'HORSE', '26C', '01')"
    )
    con.execute(
        "insert into pg.nvd_se values ('2026', '0826', '83', '01', 'HORSE', '26C', '')"
    )
    con.execute(
        "insert into pg.nvd_se values ('2026', '0820', '83', '01', 'OTHER', '26C', '01')"
    )

    subject.stage_horse_pedigree(con)
    subject.stage_banei_history(con, "20200101")
    subject.stage_horse_futan_cumul(con)
    subject.stage_sire_futan_cumul(con)
    subject.stage_damsire_futan_cumul(con)
    subject.stage_current_race_futan(con)

    history_rows = con.execute(
        "select ketto_toroku_bango, futan_kg, finish_position from banei_history"
    ).fetchall()
    current_rows = con.execute(
        "select ketto_toroku_bango, futan_kg, futan_class from current_race_futan"
    ).fetchall()
    assert history_rows == [("HORSE", 620, 1)]
    assert current_rows == [("HORSE", 620, 3)]
    con.close()
