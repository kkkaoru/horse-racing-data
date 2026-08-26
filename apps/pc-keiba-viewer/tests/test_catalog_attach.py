from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from _catalog_attach import _RAW_RACE_ENTRY_VIEW_SQL, attach_source_catalog


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, query: str) -> object:
        self.statements.append(query)
        return None


def test_attach_source_catalog_preserves_postgres_statements() -> None:
    con = FakeConn()

    attach_source_catalog(con, "postgresql://stub/horse_racing")

    assert con.statements == [
        "install postgres",
        "load postgres",
        "attach 'postgresql://stub/horse_racing' as pg (type postgres, read_only)",
    ]


def test_attach_source_catalog_attaches_local_duckdb_read_only() -> None:
    con = FakeConn()

    attach_source_catalog(con, "duckdb:///tmp/nar-history.duckdb")

    assert con.statements == [
        "attach '/tmp/nar-history.duckdb' as pg (type duckdb, read_only)"
    ]


def test_attach_source_catalog_rejects_relative_duckdb_path() -> None:
    with pytest.raises(ValueError, match="must be absolute"):
        attach_source_catalog(FakeConn(), "duckdb://relative.duckdb")


def test_attach_source_catalog_reads_real_duckdb_snapshot_read_only(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "source.duckdb"
    writer = duckdb.connect(str(snapshot_path))
    writer.execute("create table sample(value integer)")
    writer.execute("insert into sample values (7)")
    writer.close()
    reader = duckdb.connect(":memory:")

    attach_source_catalog(reader, f"duckdb://{snapshot_path}")

    assert reader.execute("select value from pg.sample").fetchone() == (7,)
    with pytest.raises(duckdb.Error):
        reader.execute("insert into pg.sample values (8)")


def test_attach_source_catalog_attaches_r2_catalog_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "warehouse")
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example.test")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "catalog-token")
    con = FakeConn()

    attach_source_catalog(con, "r2-catalog://")

    assert con.statements[:7] == [
        "install iceberg",
        "load iceberg",
        "install httpfs",
        "load httpfs",
        "create or replace temporary secret finish_position_r2_catalog "
        "(type iceberg, token 'catalog-token')",
        "attach 'warehouse' as catalog_raw (type iceberg, "
        "endpoint 'https://catalog.example.test', "
        "secret finish_position_r2_catalog, default_schema 'pc_keiba', read_only)",
        "create schema pg",
    ]
    assert "create view pg.jvd_se as select * from catalog_raw.jvd_se" in con.statements
    assert "create view pg.jvd_wc as select * from catalog_raw.jvd_wc" in con.statements
    assert (
        "create view pg.netkeiba_training_workouts as select * from "
        "catalog_raw.netkeiba_training_workouts"
        in con.statements
    )
    assert "create view pg.nvd_ra as select * from catalog_raw.nvd_ra" in con.statements
    assert "create view pg.race_entry_corner_features as" in con.statements[-2]
    assert "from pg.jvd_se se" in con.statements[-2]
    assert "from pg.nvd_se se" in con.statements[-2]
    assert "race_counts" not in con.statements[-2]
    assert con.statements[-2].count("from raw_rows") == 1
    assert (
        "count(*) over (\n"
        "        partition by source, kaisai_nen, kaisai_tsukihi, "
        "keibajo_code, race_bango\n"
        "      )" in con.statements[-2]
    )
    assert (
        "try_cast(nullif(trim(raw.umaban), '') as integer) is not null"
        in con.statements[-2]
    )
    assert "nullif(trim(raw.shusso_tosu), '00')" in con.statements[-2]
    assert (
        "create view pg.race_running_style_model_predictions as" in con.statements[-1]
    )
    assert "where false" in con.statements[-1]


def test_raw_race_entry_view_counts_only_valid_runners() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    entry_columns = """
        kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
        race_bango varchar, ketto_toroku_bango varchar, umaban varchar,
        bamei varchar, seibetsu_code varchar, barei varchar, futan_juryo varchar,
        kishumei_ryakusho varchar, chokyoshimei_ryakusho varchar, banushimei varchar,
        kakutei_chakujun varchar, tansho_ninkijun varchar, tansho_odds varchar,
        soha_time varchar, time_sa varchar, kohan_3f varchar,
        corner_1 varchar, corner_2 varchar, corner_3 varchar, corner_4 varchar
    """
    race_columns = """
        kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
        race_bango varchar, track_code varchar, grade_code varchar,
        kyoso_shubetsu_code varchar, juryo_shubetsu_code varchar,
        kyoso_joken_code varchar, babajotai_code_shiba varchar,
        babajotai_code_dirt varchar, kyori varchar, shusso_tosu varchar
    """
    for table in ("jvd_se", "nvd_se"):
        con.execute(f"create table pg.{table} ({entry_columns})")
    for table in ("jvd_ra", "nvd_ra"):
        con.execute(f"create table pg.{table} ({race_columns})")
    con.execute(
        """
        insert into pg.nvd_ra values
          ('2026', '0826', '35', '01', '24', '', '', '', '', '', '', '1200', '00')
        """
    )
    con.execute(
        """
        insert into pg.nvd_se values
          ('2026', '0826', '35', '01', 'horse-1', '01', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''),
          ('2026', '0826', '35', '01', 'horse-2', '02', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''),
          ('2026', '0826', '35', '01', 'invalid-number', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''),
          ('2026', '0826', '35', '01', '', '03', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '')
        """
    )

    con.execute(_RAW_RACE_ENTRY_VIEW_SQL)

    assert con.execute(
        "select ketto_toroku_bango, shusso_tosu from pg.race_entry_corner_features "
        "order by ketto_toroku_bango"
    ).fetchall() == [("horse-1", 2), ("horse-2", 2)]


def test_attach_source_catalog_reports_all_missing_r2_environment_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("R2_CATALOG_WAREHOUSE", raising=False)
    monkeypatch.setenv("R2_CATALOG_URI", "")
    monkeypatch.delenv("R2_CATALOG_TOKEN", raising=False)

    with pytest.raises(
        RuntimeError,
        match=(
            "R2 catalog source requires environment variables: "
            "R2_CATALOG_WAREHOUSE, R2_CATALOG_URI, R2_CATALOG_TOKEN"
        ),
    ):
        attach_source_catalog(FakeConn(), "r2-catalog://")


def test_attach_source_catalog_escapes_catalog_sql_literals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "ware'house")
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example.test/a'b")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "tok'en")
    con = FakeConn()

    attach_source_catalog(con, "r2-catalog://production")

    assert con.statements[4] == (
        "create or replace temporary secret finish_position_r2_catalog "
        "(type iceberg, token 'tok''en')"
    )
    assert con.statements[5] == (
        "attach 'ware''house' as catalog_raw (type iceberg, "
        "endpoint 'https://catalog.example.test/a''b', "
        "secret finish_position_r2_catalog, default_schema 'pc_keiba', read_only)"
    )
