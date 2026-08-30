"""Tests for the fused race-chain process runner."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from race_chain_runner import run_plan


def test_run_plan_reuses_one_engine_and_resets_layer_objects(tmp_path: Path) -> None:
    layer_dir = tmp_path / "layers"
    layer_dir.mkdir()
    first = layer_dir / "first.py"
    second = layer_dir / "second.py"
    marker = tmp_path / "marker.txt"
    first.write_text(
        "import duckdb\n"
        "connection = duckdb.connect(':memory:')\n"
        "connection.execute('create schema pg')\n"
        "connection.execute('create table pg.marker(value integer)')\n"
        "connection.close()\n",
        encoding="utf-8",
    )
    second.write_text(
        "import duckdb, pathlib, sys\n"
        "connection = duckdb.connect(':memory:')\n"
        "connection.execute('create schema pg')\n"
        'count = connection.execute("select count(*) from duckdb_tables() '
        "where table_name = 'marker'\").fetchone()[0]\n"
        "pathlib.Path(sys.argv[1]).write_text(str(count))\n"
        "connection.close()\n",
        encoding="utf-8",
    )
    plan = tmp_path / "plan.json"
    timings = tmp_path / "timings.json"
    plan.write_text(
        json.dumps(
            [
                ["python", str(first)],
                ["python", str(second), str(marker)],
            ]
        ),
        encoding="utf-8",
    )

    assert run_plan(plan, layer_dir, timings) == 0
    assert marker.read_text(encoding="utf-8") == "0"
    rows = json.loads(timings.read_text(encoding="utf-8"))
    assert [row["script"] for row in rows] == ["first.py", "second.py"]
    assert [row["status"] for row in rows] == ["done", "done"]


def test_run_plan_rejects_commands_outside_the_layer_root(tmp_path: Path) -> None:
    layer_dir = tmp_path / "layers"
    layer_dir.mkdir()
    outside = tmp_path / "outside.py"
    outside.write_text("pass\n", encoding="utf-8")
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps([["python", str(outside)]]), encoding="utf-8")

    with pytest.raises(ValueError, match="outside the allowed layer root"):
        run_plan(plan, layer_dir, tmp_path / "timings.json")
