from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import timesfm_finish_position.history_export as subject
from timesfm_finish_position.history_export import (
    HISTORY_SELECT_SQL,
    duckdb_literal,
    encoded_race_time_seconds_sql,
    export_history_parquet,
    history_select_sql,
)


class FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str] | None]] = []
        self.closed = False

    def execute(self, query: str, parameters: list[str] | None = None) -> object:
        self.calls.append((query, parameters))
        return self

    def close(self) -> None:
        self.closed = True


def test_history_select_is_point_in_time_source_and_has_required_outcomes() -> None:
    assert "nvd_se" in HISTORY_SELECT_SQL
    assert "performance_rating" in HISTORY_SELECT_SQL
    assert "speed_figure" in HISTORY_SELECT_SQL
    assert "final_3f_rating" in HISTORY_SELECT_SQL
    assert "pace_rating" in HISTORY_SELECT_SQL
    assert "order by race_date, race_id, horse_id" in HISTORY_SELECT_SQL


def test_history_select_supports_jra_and_validates_scope() -> None:
    sql = history_select_sql("jra", from_year=2000, to_year=2026)
    assert "concat('jra:', se.kaisai_nen, ':', se.kaisai_tsukihi, ':'" in sql
    assert "from pg.jvd_se" in sql
    assert "from pg.jvd_ra" in sql
    assert "group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango" in sql
    assert "between '2000' and '2026'" in sql
    with pytest.raises(ValueError, match="category"):
        history_select_sql("banei")
    with pytest.raises(ValueError, match="from_year"):
        history_select_sql("jra", from_year=2026, to_year=2025)


def test_encoded_race_time_seconds_sql_decodes_mssd_positions() -> None:
    sql = encoded_race_time_seconds_sql("encoded")
    assert "(try_cast(trim(encoded) as bigint) // 1000) * 600" in sql
    assert "((try_cast(trim(encoded) as bigint) // 10) % 100) * 10" in sql
    assert "(try_cast(trim(encoded) as bigint) % 10)" in sql
    assert "((try_cast(trim(encoded) as bigint) // 10) % 100) < 60" in sql


def test_duckdb_literal_escapes_quotes_and_rejects_nul() -> None:
    assert duckdb_literal("local'url") == "'local''url'"
    with pytest.raises(ValueError, match="must not contain NUL"):
        duckdb_literal("bad\x00value")


def test_export_history_parquet_uses_injected_local_connection(tmp_path: Path) -> None:
    connection = FakeConnection()
    output = tmp_path / "nested" / "history.parquet"
    export_history_parquet(
        output,
        database_url="postgresql://local",
        connection=connection,
        category="jra",
        from_year=2000,
    )
    assert output.parent.is_dir()
    assert connection.calls[0] == ("INSTALL postgres", None)
    assert connection.calls[1] == ("LOAD postgres", None)
    assert connection.calls[2] == (
        "ATTACH 'postgresql://local' AS pg (TYPE POSTGRES, READ_ONLY)",
        None,
    )
    assert str(output) in connection.calls[3][0]
    assert "from pg.jvd_se" in connection.calls[3][0]
    assert "between '2000' and '2026'" in connection.calls[3][0]
    assert "FORMAT PARQUET" in connection.calls[3][0]
    assert connection.closed is False


def test_export_history_parquet_owns_dynamic_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    connection = FakeConnection()

    def fake_import_module(_name: str) -> SimpleNamespace:
        return SimpleNamespace(connect=lambda: connection)

    monkeypatch.setattr(subject.importlib, "import_module", fake_import_module)
    monkeypatch.setenv(subject.DATABASE_URL_ENV, "postgresql://env-local")
    export_history_parquet(tmp_path / "history.parquet")
    assert "'postgresql://env-local'" in connection.calls[2][0]
    assert connection.closed is True
