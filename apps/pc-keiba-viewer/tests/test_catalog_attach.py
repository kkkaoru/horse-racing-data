from __future__ import annotations

import pytest

from _catalog_attach import attach_source_catalog


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
    assert "counts.entry_count" in con.statements[-2]
    assert "nullif(trim(raw.shusso_tosu), '00')" in con.statements[-2]
    assert (
        "create view pg.race_running_style_model_predictions as" in con.statements[-1]
    )
    assert "where false" in con.statements[-1]


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
