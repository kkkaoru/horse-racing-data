#!/usr/bin/env python3
"""Execute race-chain layer entrypoints in one Python/DuckDB process.

Each legacy layer still owns its SQL contract and Parquet boundary. Keeping one
interpreter and one resettable DuckDB connection removes repeated Python,
extension, and catalog-client startup while preserving byte-for-byte layer
ordering. The parent Container retains the hard process timeout and can kill
this helper safely.
"""

from __future__ import annotations

import argparse
import json
import runpy
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Final, Protocol, cast, final

import duckdb

MAX_LAYERS: Final[int] = 8


class _MutableDuckDbModule(Protocol):
    connect: Callable[..., object]


@dataclass(frozen=True, slots=True)
class LayerCommand:
    script: Path
    arguments: tuple[str, ...]


@final
class _SharedConnection:
    """DuckDB proxy whose layer-local ``close`` leaves the shared engine alive."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._connection = connection

    def close(self) -> None:
        """Layer ownership ends here; the runner closes the engine once."""

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)


def _parse_plan(path: Path, allowed_root: Path) -> tuple[LayerCommand, ...]:
    value: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not 0 < len(value) <= MAX_LAYERS:
        raise ValueError("race-chain plan must contain 1..8 layers")
    commands: list[LayerCommand] = []
    root = allowed_root.resolve()
    for item in value:
        if (
            not isinstance(item, list)
            or len(item) < 2
            or not all(isinstance(token, str) for token in item)
        ):
            raise ValueError("race-chain command is invalid")
        if item[0] != "python":
            raise ValueError("race-chain command must use the Python entrypoint")
        script = Path(item[1]).resolve()
        if not script.is_relative_to(root) or script.suffix != ".py" or not script.is_file():
            raise ValueError("race-chain script is outside the allowed layer root")
        commands.append(LayerCommand(script=script, arguments=tuple(item[2:])))
    return tuple(commands)


def _quote_identifier(value: object) -> str:
    return f'"{str(value).replace(chr(34), chr(34) * 2)}"'


def _reset_connection(connection: duckdb.DuckDBPyConnection) -> None:
    """Remove one layer's transient catalog objects before the next layer."""
    views = connection.execute(
        "select schema_name, view_name from duckdb_views() "
        "where not internal and schema_name not in ('information_schema', 'pg_catalog')"
    ).fetchall()
    for schema_name, view_name in views:
        connection.execute(
            f"drop view if exists {_quote_identifier(schema_name)}."
            f"{_quote_identifier(view_name)} cascade"
        )
    tables = connection.execute(
        "select schema_name, table_name from duckdb_tables() "
        "where not internal and schema_name not in ('information_schema', 'pg_catalog')"
    ).fetchall()
    for schema_name, table_name in tables:
        connection.execute(
            f"drop table if exists {_quote_identifier(schema_name)}."
            f"{_quote_identifier(table_name)} cascade"
        )
    databases = connection.execute("pragma database_list").fetchall()
    for _, name, _ in databases:
        if str(name) not in {"memory", "system", "temp"}:
            connection.execute(f"detach {_quote_identifier(name)}")
    schemas = connection.execute(
        "select schema_name from duckdb_schemas() where not internal and database_name = 'memory'"
    ).fetchall()
    for (schema_name,) in schemas:
        connection.execute(f"drop schema if exists {_quote_identifier(schema_name)} cascade")


def run_plan(plan_path: Path, allowed_root: Path, timings_path: Path) -> int:
    commands = _parse_plan(plan_path, allowed_root)
    connection = duckdb.connect(":memory:")
    proxy = _SharedConnection(connection)
    mutable_duckdb = cast(_MutableDuckDbModule, duckdb)
    original_connect = mutable_duckdb.connect
    original_argv = sys.argv
    timings: list[dict[str, object]] = []

    def _shared_connect(*_args: object, **_kwargs: object) -> _SharedConnection:
        return proxy

    try:
        mutable_duckdb.connect = _shared_connect
        for index, command in enumerate(commands, start=1):
            started = perf_counter()
            status = "done"
            try:
                sys.argv = [str(command.script), *command.arguments]
                runpy.run_path(str(command.script), run_name="__main__")
            except SystemExit as error:
                if error.code not in (None, 0):
                    status = "failed"
                    raise RuntimeError(
                        f"race-chain layer exited script={command.script.name} code={error.code}"
                    ) from None
            except BaseException:
                status = "failed"
                raise
            finally:
                timings.append(
                    {
                        "elapsedSeconds": perf_counter() - started,
                        "index": index,
                        "script": command.script.name,
                        "status": status,
                        "total": len(commands),
                    }
                )
            if index < len(commands):
                _reset_connection(connection)
        return 0
    finally:
        timings_path.write_text(json.dumps(timings, separators=(",", ":")), encoding="utf-8")
        sys.argv = original_argv
        mutable_duckdb.connect = original_connect
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--allowed-root", type=Path, required=True)
    parser.add_argument("--timings", type=Path, required=True)
    args = parser.parse_args(argv)
    return run_plan(args.plan, args.allowed_root, args.timings)


if __name__ == "__main__":
    raise SystemExit(main())
