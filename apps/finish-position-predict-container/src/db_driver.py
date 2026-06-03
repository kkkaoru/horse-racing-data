"""Neon / Postgres connection boundary.

Isolates the dynamically-imported psycopg driver behind a minimal typed
``ConnectionLike`` protocol so the rest of the predictor stays strict, using the
same importlib idiom as the viewer's ``default_psycopg_connect``: resolving
``psycopg.connect`` dynamically keeps the connection typed as ``Any`` (assignable
to our protocol) without pulling psycopg's concrete Connection stub, whose
overloaded ``cursor()`` over-constrains our minimal surface. I/O boundary — not
unit-tested; exercised at deploy time per DEPLOY.md.
"""

from __future__ import annotations

import importlib
from typing import Protocol


class CursorLike(Protocol):
    def execute(self, query: str, params: object = ...) -> object: ...


class ConnectionLike(Protocol):
    def cursor(self) -> CursorLike: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


def connect_postgres(database_url: str) -> ConnectionLike:
    """Open a psycopg connection typed as the minimal ``ConnectionLike``."""
    module = importlib.import_module("psycopg")
    connect_fn = module.connect
    connection: ConnectionLike = connect_fn(database_url)
    return connection
