"""Tests for mlflow_tracking.db.

Never calls psycopg2.connect for real: db.psycopg2.connect is always
monkeypatched to a fake that records its arguments and returns a
unittest.mock.MagicMock, so these tests never touch the network (Neon or the
local PostgreSQL replica).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mlflow_tracking import config, db


def test_connect_racing_neon_connects_with_resolved_dsn_and_sets_readonly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_NEON_RACING_URL, "postgresql://user:pass@neon.example/racing")
    fake_conn = MagicMock()
    calls: list[str] = []

    def fake_connect(dsn: str) -> MagicMock:
        calls.append(dsn)
        return fake_conn

    monkeypatch.setattr(db.psycopg2, "connect", fake_connect)

    result = db.connect_racing_neon()

    assert calls == ["postgresql://user:pass@neon.example/racing"]
    fake_conn.set_session.assert_called_once_with(readonly=True, autocommit=True)
    assert result is fake_conn


def test_connect_local_replica_connects_with_resolved_dsn_and_sets_readonly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(config.ENV_LOCAL_REPLICA_URL, "postgresql://custom:custom@127.0.0.1:5432/db")
    fake_conn = MagicMock()
    calls: list[str] = []

    def fake_connect(dsn: str) -> MagicMock:
        calls.append(dsn)
        return fake_conn

    monkeypatch.setattr(db.psycopg2, "connect", fake_connect)

    result = db.connect_local_replica()

    assert calls == ["postgresql://custom:custom@127.0.0.1:5432/db"]
    fake_conn.set_session.assert_called_once_with(readonly=True, autocommit=True)
    assert result is fake_conn


def test_connect_local_replica_uses_default_dsn_when_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = MagicMock()
    calls: list[str] = []

    def fake_connect(dsn: str) -> MagicMock:
        calls.append(dsn)
        return fake_conn

    monkeypatch.setattr(db.psycopg2, "connect", fake_connect)

    db.connect_local_replica()

    assert calls == [config.DEFAULT_LOCAL_REPLICA_URL]
