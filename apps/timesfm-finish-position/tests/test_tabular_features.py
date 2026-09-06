from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import timesfm_finish_position.tabular_features as subject
from timesfm_finish_position.tabular_features import (
    HISTORY_METRICS,
    ROLLING_WINDOWS,
    build_tabular_feature_parquet,
    build_tabular_feature_sql,
    rolling_feature_expressions,
)


class FakeConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.closed = False

    def execute(self, query: str) -> object:
        self.queries.append(query)
        return self

    def close(self) -> None:
        self.closed = True


def test_rolling_features_always_end_one_day_row_before_target() -> None:
    expressions = rolling_feature_expressions()
    assert len(expressions) == 2 + len(HISTORY_METRICS) * (1 + 2 * len(ROLLING_WINDOWS))
    assert all(
        "1 preceding" in expression
        for expression in expressions
        if expression.startswith(("avg(", "stddev_pop("))
    )
    assert any(expression.startswith("lag(performance_rating)") for expression in expressions)


def test_build_tabular_feature_sql_aggregates_same_day_before_lagging(tmp_path: Path) -> None:
    query = build_tabular_feature_sql(
        tmp_path / "history'input.parquet", tmp_path / "features.parquet"
    )
    assert "group by horse_id, race_day" in query
    assert "rows between unbounded preceding and 1 preceding" in query
    assert "history''input.parquet" in query
    assert "order by source.race_date, source.race_id, source.horse_id" in query


def test_build_tabular_feature_parquet_uses_injected_connection(tmp_path: Path) -> None:
    connection = FakeConnection()
    output = tmp_path / "nested" / "features.parquet"
    build_tabular_feature_parquet(tmp_path / "history.parquet", output, connection=connection)
    assert output.parent.is_dir()
    assert len(connection.queries) == 1
    assert str(output) in connection.queries[0]
    assert connection.closed is False


def test_build_tabular_feature_parquet_closes_owned_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    connection = FakeConnection()

    def fake_import_module(_name: str) -> SimpleNamespace:
        return SimpleNamespace(connect=lambda: connection)

    monkeypatch.setattr(subject.importlib, "import_module", fake_import_module)
    build_tabular_feature_parquet(tmp_path / "history.parquet", tmp_path / "features.parquet")
    assert connection.closed is True
