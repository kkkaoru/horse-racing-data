"""Read-only network DB access for production-usage sync + champion cell eval.

A deliberate, narrow exception to this package's file-based-only ingestion
design (see ingest_eval.py's module docstring): tracing genuinely-served
predictions and joining them against finalized race results requires two live
Postgres sources this repo has no file export of:

  - the racing Neon database (config.get_racing_neon_dsn(), NEON_PRIMARY_URL)
    for race_finish_position_model_predictions / race_running_style_model_predictions
  - the local PostgreSQL replica (config.get_local_replica_dsn(),
    127.0.0.1:15432 by default) for jvd_se/jvd_ra/nvd_se/nvd_ra finalized
    results and race metadata

Every query issued by this package against either connection is a SELECT --
NEVER INSERT/UPDATE/DELETE against any racing table (see the repo's
never-delete-racing-data rule). Connections opened here are READ-ONLY
(`set_session(readonly=True, autocommit=True)`) as defense in depth on top of
that code-review discipline.

Every caller-facing function elsewhere in this package (sync_production.py,
champion_cell_eval.py, serve_eval.py) takes an INJECTED connection (or a
zero-argument factory defaulting to the real functions below), never
constructing one internally -- this keeps `pytest` fully hermetic (see
tests/conftest.py): tests always pass a fake connection/cursor and never call
psycopg2.connect for real, so pytest never touches Neon or the local replica.
"""

from __future__ import annotations

from typing import Protocol, cast

import psycopg2

from mlflow_tracking import config


class CursorLike(Protocol):
    def execute(self, query: str, params: object = None) -> object: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class ConnectionLike(Protocol):
    def cursor(self) -> CursorLike: ...

    def close(self) -> None: ...


def connect_racing_neon() -> ConnectionLike:
    """Open a READ-ONLY connection to the racing Neon database.

    Resolves the DSN via config.get_racing_neon_dsn() (raises ValueError,
    naming only the missing env var, if NEON_PRIMARY_URL is unset -- never
    the DSN value itself). set_session(readonly=True, autocommit=True) is
    applied before returning so every statement issued on this connection is
    rejected by Postgres itself if it is ever anything other than a SELECT,
    as defense in depth on top of this package's own SELECT-only discipline.
    """
    conn = psycopg2.connect(config.get_racing_neon_dsn())
    conn.set_session(readonly=True, autocommit=True)
    return cast(ConnectionLike, conn)


def connect_local_replica() -> ConnectionLike:
    """Open a READ-ONLY connection to the local PostgreSQL replica.

    Resolves the DSN via config.get_local_replica_dsn() (defaults to the
    standard 127.0.0.1:15432 local-replica endpoint when
    HORSE_RACING_LOCAL_PG_URL is unset). set_session(readonly=True,
    autocommit=True) is applied before returning, same rationale as
    connect_racing_neon() above.
    """
    conn = psycopg2.connect(config.get_local_replica_dsn())
    conn.set_session(readonly=True, autocommit=True)
    return cast(ConnectionLike, conn)
