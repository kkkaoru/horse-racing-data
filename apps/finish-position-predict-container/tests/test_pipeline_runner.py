"""Tests for the pipeline-runner helpers (URL masking + stderr surfacing).

``pipeline_runner`` is the I/O glue that shells out to the bundled feature
scripts; it is intentionally NOT in the coverage gate (the subprocess paths are
exercised at deploy time per ``DEPLOY.md``). These tests cover only the pure,
deterministic helpers that protect operations: ``mask_pg_url`` (defensive
credential redaction before any argv ever reaches a log) and ``_run`` (must
surface the child's stderr tail so silent ``exit 1`` from the feature pipeline
becomes diagnosable instead of an opaque ``CalledProcessError``).
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from time import perf_counter
from typing import cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeline_runner
from pipeline_runner import has_parquet_output, mask_pg_url, run_with_stderr_capture

_TIMEOUT_SECONDS_ATTR = "_pipeline_subprocess_timeout_seconds"
_pipeline_subprocess_timeout_seconds = cast(
    Callable[[], float], getattr(pipeline_runner, _TIMEOUT_SECONDS_ATTR)
)

# ``_day_base_dir`` / ``_reset_category_work_dirs`` are module-private (leading
# underscore); accessed via getattr + cast (same pattern as
# ``test_predict_upcoming.py`` / ``test_model_meta.py``'s ``_env_flag``
# helper) so this is a dynamic attribute lookup, not a static
# ``pipeline_runner._day_base_dir`` expression -- strict basedpyright does not
# flag ``reportPrivateUsage`` on this, and the attribute name is read from a
# variable (not a string literal) so ruff's B009 does not fire either.
_DAY_BASE_DIR_ATTR = "_day_base_dir"
_RESET_CATEGORY_WORK_DIRS_ATTR = "_reset_category_work_dirs"
_day_base_dir = cast(
    Callable[[str, str], Path],
    getattr(pipeline_runner, _DAY_BASE_DIR_ATTR),
)
_reset_category_work_dirs = cast(
    Callable[[str, Path], None],
    getattr(pipeline_runner, _RESET_CATEGORY_WORK_DIRS_ATTR),
)



def test_mask_pg_url_redacts_userinfo():
    masked = mask_pg_url("postgresql://user:secret@host/db")
    assert masked == "postgresql://<redacted>@host/db"


def test_mask_pg_url_redacts_neon_style_token():
    masked = mask_pg_url(
        "postgresql://neondb_owner:npg_VERYSECRET@ep-foo.aws.neon.tech/neondb?sslmode=require"
    )
    assert "npg_VERYSECRET" not in masked
    assert masked.startswith("postgresql://<redacted>@ep-foo.aws.neon.tech/neondb")


def test_mask_pg_url_passthrough_when_no_userinfo():
    assert mask_pg_url("python") == "python"
    assert mask_pg_url("/app/pipeline/foo.py") == "/app/pipeline/foo.py"


def test_run_succeeds_on_zero_exit():
    run_with_stderr_capture(["python", "-c", "print('ok')"])


def test_run_suppresses_child_stdout_without_debug(capfd: pytest.CaptureFixture[str]):
    run_with_stderr_capture(
        [
            "python",
            "-c",
            "import sys; sys.stdout.write('hello-child-stdout\\n'); sys.stdout.flush()",
        ]
    )
    captured = capfd.readouterr()
    assert "hello-child-stdout" not in captured.out


def test_run_suppresses_child_stderr_without_debug(
    capfd: pytest.CaptureFixture[str],
):
    run_with_stderr_capture(
        [
            "python",
            "-c",
            "import sys; sys.stderr.write('child-progress-log\\n'); sys.stderr.flush()",
        ]
    )
    captured = capfd.readouterr()
    assert "child-progress-log" not in captured.err


def test_run_streams_child_output_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
):
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    run_with_stderr_capture(
        [
            "python",
            "-c",
            "import sys; sys.stdout.write('debug-out\\n'); sys.stderr.write('debug-err\\n')",
        ]
    )
    captured = capfd.readouterr()
    assert "debug-out" in captured.out
    assert "debug-err" in captured.err


def test_run_raises_runtime_error_with_stderr_tail_on_failure():
    with pytest.raises(RuntimeError) as exc_info:
        run_with_stderr_capture(
            [
                "python",
                "-c",
                "import sys; sys.stderr.write('boom from child\\n'); sys.exit(7)",
            ]
        )
    message = str(exc_info.value)
    assert "exit 7" in message
    assert "boom from child" in message


def test_run_masks_pg_url_in_error_message():
    with pytest.raises(RuntimeError) as exc_info:
        run_with_stderr_capture(
            [
                "python",
                "-c",
                "import sys; sys.exit(1)",
                "--pg-url",
                "postgresql://u:hunter2@h/db",
            ]
        )
    message = str(exc_info.value)
    assert "hunter2" not in message
    assert "<redacted>" in message


def test_pipeline_subprocess_timeout_seconds_defaults_when_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, raising=False)
    assert (
        _pipeline_subprocess_timeout_seconds()
        == pipeline_runner.DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    )


def test_pipeline_subprocess_timeout_seconds_defaults_when_blank(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, "   ")
    assert (
        _pipeline_subprocess_timeout_seconds()
        == pipeline_runner.DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    )


def test_pipeline_subprocess_timeout_seconds_defaults_when_non_numeric(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, "not-a-number")
    assert (
        _pipeline_subprocess_timeout_seconds()
        == pipeline_runner.DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    )


def test_pipeline_subprocess_timeout_seconds_defaults_when_non_positive(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, "0")
    assert (
        _pipeline_subprocess_timeout_seconds()
        == pipeline_runner.DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    )
    monkeypatch.setenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, "-5")
    assert (
        _pipeline_subprocess_timeout_seconds()
        == pipeline_runner.DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    )


def test_pipeline_subprocess_timeout_seconds_honours_valid_override(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, "42.5")
    assert _pipeline_subprocess_timeout_seconds() == 42.5


def test_run_kills_hanging_subprocess_at_short_timeout(monkeypatch: pytest.MonkeyPatch):
    """A subprocess that never exits on its own must be killed, not hung on forever.

    Sets a short override (0.2s) and launches a child that sleeps far longer
    (30s) than that. If the kill did not actually happen, this test would take
    ~30s (or longer) instead of finishing in well under a second -- the
    ``elapsed`` bound below is the proxy for "the process group was really
    killed", since a unit test cannot otherwise observe the reaped pid.
    """
    monkeypatch.setenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, "0.2")
    started = perf_counter()
    with pytest.raises(RuntimeError) as exc_info:
        run_with_stderr_capture(["python", "-c", "import time; time.sleep(30)"])
    elapsed = perf_counter() - started
    message = str(exc_info.value)
    assert "timed out after" in message
    assert elapsed < 10.0, f"hanging subprocess was not killed promptly (took {elapsed:.1f}s)"


def test_run_kills_hanging_subprocess_reports_stderr_tail_from_before_timeout(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(pipeline_runner.PIPELINE_SUBPROCESS_TIMEOUT_ENV, "0.3")
    with pytest.raises(RuntimeError) as exc_info:
        run_with_stderr_capture(
            [
                "python",
                "-c",
                (
                    "import sys, time; "
                    "sys.stderr.write('hung-before-timeout\\n'); sys.stderr.flush(); "
                    "time.sleep(30)"
                ),
            ]
        )
    message = str(exc_info.value)
    assert "hung-before-timeout" in message


def test_has_parquet_output_false_for_missing_dir(tmp_path: Path):
    missing = tmp_path / "does-not-exist"
    assert has_parquet_output(missing) is False


def test_has_parquet_output_false_for_empty_dir(tmp_path: Path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    assert has_parquet_output(empty_dir) is False


def test_has_parquet_output_true_when_partitioned_parquet_exists(tmp_path: Path):
    base = tmp_path / "feat"
    partition = base / "race_year=2026"
    partition.mkdir(parents=True)
    (partition / "data.parquet").write_bytes(b"PAR1")
    assert has_parquet_output(base) is True


def test_build_pipeline_signature_accepts_venue_weather_dir():
    import inspect

    from pipeline_runner import build_pipeline

    assert "venue_weather_dir" in inspect.signature(build_pipeline).parameters


def test_build_pipeline_signature_accepts_target_race():
    import inspect

    from pipeline_runner import build_pipeline

    param = inspect.signature(build_pipeline).parameters.get("target_race")
    assert param is not None
    assert param.default is None


def test_build_upcoming_feature_rows_signature_accepts_target_race():
    import inspect

    from pipeline_runner import build_upcoming_feature_rows

    param = inspect.signature(build_upcoming_feature_rows).parameters.get("target_race")
    assert param is not None
    assert param.default is None


def test_fetch_venue_weather_dir_is_importable_from_weather_fetcher():
    from weather_fetcher import fetch_venue_weather_dir

    assert callable(fetch_venue_weather_dir)


def test_query_upcoming_race_keys_filters_to_target_race(monkeypatch: pytest.MonkeyPatch):
    import db_driver
    import realtime_odds_fetcher
    import weather_fetcher

    captured_sql = ""
    connection_closed = False
    captured_race_keys: list[tuple[str, str]] | None = None
    captured_target_race: str | None = None

    class FakeCursor:
        def execute(self, sql: str) -> None:
            nonlocal captured_sql
            captured_sql = sql

        def fetchall(self) -> list[tuple[str, str]]:
            return [("44", "08")]

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            nonlocal connection_closed
            connection_closed = True

    def fake_connect_postgres(_url: str) -> FakeConn:
        return FakeConn()

    def fake_fetch_realtime_odds_parquet(
        category: str,
        target_date: str,
        work_dir: Path,
        race_keys: list[tuple[str, str]] | None = None,
    ) -> None:
        nonlocal captured_race_keys
        assert category == "nar"
        assert target_date == "20260629"
        assert work_dir == pipeline_runner.WORK_DIR
        captured_race_keys = race_keys

    def fake_fetch_venue_weather_dir(_target_date: str, _work_dir: Path) -> None:
        return None

    def fake_build_pipeline(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        final_dir: Path,
        realtime_odds_path: Path | None = None,
        venue_weather_dir: Path | None = None,
        target_race: str | None = None,
    ) -> bool:
        nonlocal captured_target_race
        assert category == "nar"
        assert target_date == "20260629"
        assert days_ahead == 0
        assert database_url == "postgresql://u:p@h/db"
        assert final_dir == pipeline_runner.WORK_DIR / "feat-nar-v7-final"
        assert realtime_odds_path is None
        assert venue_weather_dir is None
        captured_target_race = target_race
        return False

    monkeypatch.setattr(db_driver, "connect_postgres", fake_connect_postgres)
    monkeypatch.setattr(
        realtime_odds_fetcher,
        "fetch_realtime_odds_parquet",
        fake_fetch_realtime_odds_parquet,
    )
    monkeypatch.setattr(weather_fetcher, "fetch_venue_weather_dir", fake_fetch_venue_weather_dir)
    monkeypatch.setattr(pipeline_runner, "build_pipeline", fake_build_pipeline)

    rows = pipeline_runner.build_upcoming_feature_rows(
        "nar",
        "20260629",
        0,
        "postgresql://u:p@h/db",
        target_race="44:08",
    )

    assert rows == {}
    assert captured_race_keys == [("44", "08")]
    assert captured_target_race == "44:08"
    assert "and keibajo_code = '44'" in captured_sql
    assert "and race_bango = '08'" in captured_sql
    assert connection_closed is True


def test_build_pipeline_logs_layer_elapsed_seconds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "layer_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    # The debug-timing writer is diagnostic-only I/O (real Postgres connect) —
    # stub it out so this test of the layer-timing LOG behavior never touches
    # the network. Its own behavior is covered by the
    # test_record_layer_timing_row_* tests below.
    recorded_calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        pipeline_runner,
        "record_layer_timing_row",
        lambda *args: recorded_calls.append(args),
    )
    captured_temp_dir: Path | None = None

    def fake_base_argv(*args: object, **kwargs: object) -> list[str]:
        nonlocal captured_temp_dir
        output_dir = args[5]
        temp_dir = kwargs.get("temp_dir")
        assert isinstance(temp_dir, Path)
        captured_temp_dir = temp_dir
        return ["base", str(output_dir)]

    def fake_layer_argv(*args: object, **kwargs: object) -> list[str]:
        output_dir = args[4]
        return ["layer", str(output_dir)]

    def fake_run(args: list[str]) -> None:
        Path(args[-1]).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline_runner, "build_base_argv", fake_base_argv)
    monkeypatch.setattr(pipeline_runner, "build_layer_argv", fake_layer_argv)
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run)

    final_dir = tmp_path / "final"
    built = pipeline_runner.build_pipeline(
        "jra", "20260629", 0, "postgresql://u:p@h/db", final_dir, target_race="05:11"
    )

    captured = capsys.readouterr()
    assert built is True
    assert "step=layer index=1/1 status=start" in captured.err
    assert "step=layer index=1/1 status=done" in captured.err
    assert "script=script-a.py" in captured.err
    assert "target_race=05:11" in captured.err
    assert "elapsed_seconds=" in captured.err
    assert captured_temp_dir == work_dir / "duckdb-spill" / "jra-20260629-05-11"
    assert captured_temp_dir is not None
    assert captured_temp_dir.exists()
    # One debug-timing call for the base build (layer_index=0) + one for the
    # single layer in the fake chain (layer_index=1), both status="done".
    # Positional args: (database_url, run_id, category, run_date, target_race,
    # layer_index, layer_total, layer_script, status, elapsed, cumulative).
    assert len(recorded_calls) == 2
    base_call, layer_call = recorded_calls
    assert (base_call[5], base_call[6], base_call[7], base_call[8]) == (
        0,
        1,
        "__base_build__",
        "done",
    )
    assert (layer_call[5], layer_call[6], layer_call[7], layer_call[8]) == (
        1,
        1,
        "script-a.py",
        "done",
    )


def test_build_pipeline_resets_stale_category_work_dirs_before_rename(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """A 2nd same-category race in one process must reset the category-scoped
    work dirs left by the 1st race so ``current.rename(final_dir)`` lands on a
    clean target (the production write-gap: rename onto a non-empty ``final_dir``
    fails with ENOTEMPTY after every layer already logged "done").

    Exercises the cleanup through the PUBLIC ``build_pipeline`` API only: a
    stale ``final_dir`` (non-empty), a stale base dir, and a stale layer dir are
    pre-created, then ``build_pipeline`` must succeed and the stale markers must
    be gone (final_dir reset then repopulated by the layer rename).
    """
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "layer_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)

    # Simulate the 1st race's leftovers: a populated final_dir + base + layer dir.
    final_dir = work_dir / "feat-jra-v7-final"
    final_dir.mkdir()
    (final_dir / "stale.parquet").write_bytes(b"PAR1-stale")
    stale_base = work_dir / "feat-jra-base"
    stale_base.mkdir()
    (stale_base / "stale_base.parquet").write_bytes(b"PAR1-stale-base")
    stale_layer = work_dir / "feat-jra-layer-0"
    stale_layer.mkdir()
    (stale_layer / "stale_layer.parquet").write_bytes(b"PAR1-stale-layer")

    def fake_base_argv(*args: object, **kwargs: object) -> list[str]:
        output_dir = args[5]
        return ["base", str(output_dir)]

    def fake_layer_argv(*args: object, **kwargs: object) -> list[str]:
        output_dir = args[4]
        return ["layer", str(output_dir)]

    def fake_run(args: list[str]) -> None:
        Path(args[-1]).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline_runner, "build_base_argv", fake_base_argv)
    monkeypatch.setattr(pipeline_runner, "build_layer_argv", fake_layer_argv)
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run)

    built = pipeline_runner.build_pipeline(
        "jra", "20260702", 0, "postgresql://u:p@h/db", final_dir, target_race="05:11"
    )

    assert built is True
    # final_dir was reset (stale.parquet gone) then repopulated by the rename.
    assert final_dir.exists()
    assert not (final_dir / "stale.parquet").exists()
    # The stale base marker is gone (base dir was cleared before the base build).
    assert not (stale_base / "stale_base.parquet").exists()


def test_record_layer_timing_row_writes_row_via_mocked_connection(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    import psycopg

    executed_sql: list[str] = []
    inserted_params: list[tuple[object, ...]] = []
    state = {"committed": False, "closed": False}

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            executed_sql.append(sql)
            if params is not None:
                inserted_params.append(params)

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            state["committed"] = True

        def close(self) -> None:
            state["closed"] = True

    captured_connect_kwargs: dict[str, object] = {}

    def fake_connect(url: str, **kwargs: object) -> FakeConn:
        captured_connect_kwargs["url"] = url
        captured_connect_kwargs.update(kwargs)
        return FakeConn()

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    pipeline_runner.record_layer_timing_row(
        "postgresql://u:p@h/db",
        "jra:20260702:05:11:abcd1234",
        "jra",
        "20260702",
        "05:11",
        1,
        3,
        "add-race-internal-features.py",
        "done",
        1.5,
        2.5,
    )

    assert captured_connect_kwargs["url"] == "postgresql://u:p@h/db"
    assert captured_connect_kwargs["connect_timeout"] == 5
    assert any(
        "CREATE TABLE IF NOT EXISTS _debug_finish_position_layer_timing" in sql
        for sql in executed_sql
    )
    assert any("INSERT INTO _debug_finish_position_layer_timing" in sql for sql in executed_sql)
    assert inserted_params == [
        (
            "jra:20260702:05:11:abcd1234",
            "jra",
            "20260702",
            "05",
            "11",
            1,
            3,
            "add-race-internal-features.py",
            "done",
            1.5,
            2.5,
        )
    ]
    assert state["committed"] is True
    assert state["closed"] is True


def test_record_layer_timing_row_no_target_race_leaves_keys_none(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    import psycopg

    inserted_params: list[tuple[object, ...]] = []

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            if params is not None:
                inserted_params.append(params)

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    pipeline_runner.record_layer_timing_row(
        "postgresql://u:p@h/db",
        "jra:20260702:all:abcd1234",
        "jra",
        "20260702",
        None,
        0,
        3,
        "__base_build__",
        "done",
        2.0,
        2.0,
    )

    assert inserted_params[0][3] is None  # keibajo_code
    assert inserted_params[0][4] is None  # race_bango


def test_record_layer_timing_row_swallows_connect_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    import psycopg

    def fake_connect_raises(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom-connect")

    monkeypatch.setattr(psycopg, "connect", fake_connect_raises)

    pipeline_runner.record_layer_timing_row(
        "postgresql://u:p@h/db",
        "jra:20260702:all:abcd1234",
        "jra",
        "20260702",
        None,
        0,
        3,
        "__base_build__",
        "failed",
        2.0,
        2.0,
    )

    captured = capsys.readouterr()
    assert "debug-timing write failed" in captured.err
    assert "boom-connect" in captured.err


def test_record_layer_timing_row_swallows_execute_error_and_still_closes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    import psycopg

    state = {"closed": False}

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            raise RuntimeError("boom-execute")

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            pass

        def close(self) -> None:
            state["closed"] = True

    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    pipeline_runner.record_layer_timing_row(
        "postgresql://u:p@h/db",
        "jra:20260702:all:abcd1234",
        "jra",
        "20260702",
        None,
        0,
        3,
        "__base_build__",
        "done",
        1.0,
        1.0,
    )

    captured = capsys.readouterr()
    assert "debug-timing write failed" in captured.err
    assert "boom-execute" in captured.err
    assert state["closed"] is True


# ---------------------------------------------------------------------------
# DAY_CHAIN / RACE_CHAIN split — _day_base_dir naming + reset exclusion
# ---------------------------------------------------------------------------


def test_day_base_dir_naming_does_not_match_feat_glob():
    day_dir = _day_base_dir("jra", "20260712")
    assert day_dir == pipeline_runner.WORK_DIR / "daybase-jra-20260712"
    assert not day_dir.name.startswith("feat-")


def test_reset_category_work_dirs_does_not_delete_day_base_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Regression guard for the 'excluded by construction' invariant: the
    per-race stale-work-dir sweep must never evict the per-category+day
    day-base cache other races of the same day rely on."""
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)

    day_base_dir = _day_base_dir("jra", "20260712")
    day_base_dir.mkdir(parents=True)
    (day_base_dir / "marker.txt").write_text("keep-me")

    final_dir = work_dir / "feat-jra-v7-final"
    _reset_category_work_dirs("jra", final_dir)

    assert day_base_dir.exists()
    assert (day_base_dir / "marker.txt").read_text() == "keep-me"


# ---------------------------------------------------------------------------
# build_day_base
# ---------------------------------------------------------------------------


def test_build_day_base_returns_final_dir_on_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(
        pipeline_runner, "day_chain_for", lambda _category: ("script-a.py", "script-b.py")
    )
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)

    captured_target_race: list[object] = []

    def fake_base_argv(*args: object, **kwargs: object) -> list[str]:
        output_dir = args[5]
        captured_target_race.append(args[8])
        return ["base", str(output_dir)]

    def fake_layer_argv(*args: object, **kwargs: object) -> list[str]:
        output_dir = args[4]
        return ["layer", str(output_dir)]

    def fake_run(args: list[str]) -> None:
        Path(args[-1]).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline_runner, "build_base_argv", fake_base_argv)
    monkeypatch.setattr(pipeline_runner, "build_layer_argv", fake_layer_argv)
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run)

    result = pipeline_runner.build_day_base("jra", "20260712", 2, "postgresql://u:p@h/db")

    assert result == _day_base_dir("jra", "20260712") / "final"
    assert result is not None
    assert result.exists()
    # Whole-day scope: base build must NOT receive --target-race.
    assert captured_target_race == [None]


def test_build_day_base_returns_none_when_base_build_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: False)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)
    monkeypatch.setattr(pipeline_runner, "build_base_argv", lambda *args, **kwargs: ["base"])
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", lambda args: None)

    result = pipeline_runner.build_day_base("nar", "20260712", 0, "postgresql://u:p@h/db")

    assert result is None


def test_build_day_base_resets_stale_day_dir_before_building(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)

    stale_day_dir = _day_base_dir("jra", "20260712")
    stale_final = stale_day_dir / "final"
    stale_final.mkdir(parents=True)
    (stale_final / "stale.parquet").write_bytes(b"STALE")

    def fake_base_argv(*args: object, **kwargs: object) -> list[str]:
        return ["base", str(args[5])]

    def fake_layer_argv(*args: object, **kwargs: object) -> list[str]:
        return ["layer", str(args[4])]

    def fake_run(args: list[str]) -> None:
        Path(args[-1]).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline_runner, "build_base_argv", fake_base_argv)
    monkeypatch.setattr(pipeline_runner, "build_layer_argv", fake_layer_argv)
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run)

    result = pipeline_runner.build_day_base("jra", "20260712", 0, "postgresql://u:p@h/db")

    assert result is not None
    assert result.exists()
    assert not (result / "stale.parquet").exists()


def test_build_day_base_propagates_base_build_failure_and_records_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ())
    recorded: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        pipeline_runner, "record_layer_timing_row", lambda *args: recorded.append(args)
    )
    monkeypatch.setattr(pipeline_runner, "build_base_argv", lambda *args, **kwargs: ["base"])

    def fake_run_raises(args: list[str]) -> None:
        raise RuntimeError("base build boom")

    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run_raises)

    with pytest.raises(RuntimeError, match="base build boom"):
        pipeline_runner.build_day_base("jra", "20260712", 0, "postgresql://u:p@h/db")

    assert len(recorded) == 1
    # Positional args: (database_url, run_id, category, run_date, target_race,
    # layer_index, layer_total, layer_script, status, elapsed, cumulative).
    assert recorded[0][7] == "__daybase_base__"
    assert recorded[0][8] == "failed"


def test_build_day_base_signature_accepts_realtime_and_weather():
    import inspect

    sig = inspect.signature(pipeline_runner.build_day_base)
    assert "realtime_odds_path" in sig.parameters
    assert "venue_weather_dir" in sig.parameters
    assert sig.parameters["realtime_odds_path"].default is None
    assert sig.parameters["venue_weather_dir"].default is None


# ---------------------------------------------------------------------------
# ensure_day_base
# ---------------------------------------------------------------------------


def test_ensure_day_base_local_disk_hit_skips_r2(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    final_dir = _day_base_dir("jra", "20260712") / "final"
    partition = final_dir / "race_year=2026"
    partition.mkdir(parents=True)
    (partition / "data.parquet").write_bytes(b"PAR1")

    called: list[bool] = []
    monkeypatch.setattr(
        pipeline_runner, "r2_get_parquet", lambda *args, **kwargs: called.append(True) or True
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "postgresql://u:p@h/db", None)

    assert result == final_dir
    assert called == []


def test_ensure_day_base_r2_hit_when_local_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")
    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", lambda *args, **kwargs: True)

    result = pipeline_runner.ensure_day_base("nar", "20260712", 0, "postgresql://u:p@h/db", r2)

    assert result == _day_base_dir("nar", "20260712") / "final"


def test_ensure_day_base_r2_miss_returns_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")
    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", lambda *args, **kwargs: False)

    result = pipeline_runner.ensure_day_base("ban-ei", "20260712", 0, "postgresql://u:p@h/db", r2)

    assert result is None


def test_ensure_day_base_no_local_no_r2_config_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "postgresql://u:p@h/db", None)

    assert result is None


def test_ensure_day_base_r2_exception_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    def raiser(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("network boom")

    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", raiser)

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "postgresql://u:p@h/db", r2)

    assert result is None
    captured = capsys.readouterr()
    assert "network boom" in captured.err


# ---------------------------------------------------------------------------
# day_base_covers_entry_list
# ---------------------------------------------------------------------------


def _write_day_base_parquet(day_base_dir: Path, rows: list[tuple[str, str]]) -> None:
    import pandas as pd

    day_base_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["race_id", "ketto_toroku_bango"])
    frame.to_parquet(day_base_dir / "data.parquet")


def test_day_base_covers_entry_list_true_when_all_current_horses_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import db_driver

    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(
        day_base_dir,
        [
            ("jra:2026:0712:05:11", "H1"),
            ("jra:2026:0712:05:11", "H2"),
            ("jra:2026:0712:05:12", "H3"),
        ],
    )

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            pass

        def fetchall(self) -> list[tuple[str]]:
            return [("H1",), ("H2",)]

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            pass

    monkeypatch.setattr(db_driver, "connect_postgres", lambda _url: FakeConn())

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "05:11", "postgresql://u:p@h/db"
    )

    assert result is True


def test_day_base_covers_entry_list_false_when_current_horse_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import db_driver

    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("jra:2026:0712:05:11", "H1")])

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            pass

        def fetchall(self) -> list[tuple[str]]:
            return [("H1",), ("H2",)]  # H2 scratched-in / late add, not in day-base

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            pass

    monkeypatch.setattr(db_driver, "connect_postgres", lambda _url: FakeConn())

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False


def test_day_base_covers_entry_list_only_matches_target_race_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """A horse present elsewhere in the day-base but NOT under the target
    race's own keibajo/bango must not count as coverage — proves the SQL
    filter is scoped to the target race, not the whole day-base."""
    import db_driver

    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(
        day_base_dir,
        [
            ("jra:2026:0712:05:11", "H1"),
            ("jra:2026:0712:06:11", "H9"),
        ],
    )

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            pass

        def fetchall(self) -> list[tuple[str]]:
            return [("H9",)]

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            pass

    monkeypatch.setattr(db_driver, "connect_postgres", lambda _url: FakeConn())

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False


def test_day_base_covers_entry_list_false_when_no_current_entrants(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import db_driver

    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("jra:2026:0712:05:11", "H1")])

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            pass

        def fetchall(self) -> list[tuple[str]]:
            return []

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            pass

    monkeypatch.setattr(db_driver, "connect_postgres", lambda _url: FakeConn())

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False


def test_day_base_covers_entry_list_false_on_pg_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    import db_driver

    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("jra:2026:0712:05:11", "H1")])

    def raiser(_url: str) -> None:
        raise RuntimeError("connect boom")

    monkeypatch.setattr(db_driver, "connect_postgres", raiser)

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False
    captured = capsys.readouterr()
    assert "connect boom" in captured.err


def test_day_base_covers_entry_list_uses_nvd_se_for_nar(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import db_driver

    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("nar:2026:0712:30:03", "H1")])

    captured_sql: list[str] = []

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            captured_sql.append(sql)

        def fetchall(self) -> list[tuple[str]]:
            return [("H1",)]

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            pass

    monkeypatch.setattr(db_driver, "connect_postgres", lambda _url: FakeConn())

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "nar", "30:03", "postgresql://u:p@h/db"
    )

    assert result is True
    assert any("nvd_se" in sql for sql in captured_sql)


# ---------------------------------------------------------------------------
# build_pipeline_from_day_base
# ---------------------------------------------------------------------------


def test_build_pipeline_from_day_base_runs_race_chain_only_from_day_base_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(
        pipeline_runner, "race_chain_for", lambda _category: ("script-a.py", "script-b.py")
    )
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)

    captured_inputs: list[Path] = []
    captured_target_races: list[str | None] = []

    def fake_layer_argv(
        script: str,
        category: str,
        layer_dir: Path,
        input_dir: Path,
        output_dir: Path,
        database_url: str,
        target_date: str | None = None,
        target_race: str | None = None,
    ) -> list[str]:
        captured_inputs.append(input_dir)
        captured_target_races.append(target_race)
        return ["layer", str(output_dir)]

    def fake_run(args: list[str]) -> None:
        Path(args[-1]).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline_runner, "build_layer_argv", fake_layer_argv)
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run)

    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()
    final_dir = work_dir / "feat-jra-v7-final"

    result = pipeline_runner.build_pipeline_from_day_base(
        "jra", "20260712", 0, "postgresql://u:p@h/db", day_base_dir, final_dir, "05:11"
    )

    assert result is True
    assert final_dir.exists()
    assert captured_inputs[0] == day_base_dir
    assert captured_target_races == ["05:11", "05:11"]


def test_build_pipeline_from_day_base_signature_accepts_extra_params():
    import inspect

    sig = inspect.signature(pipeline_runner.build_pipeline_from_day_base)
    assert "realtime_odds_path" in sig.parameters
    assert "venue_weather_dir" in sig.parameters
    assert sig.parameters["realtime_odds_path"].default is None
    assert sig.parameters["venue_weather_dir"].default is None
    assert "days_ahead" in sig.parameters


# ---------------------------------------------------------------------------
# build_upcoming_feature_rows_split
# ---------------------------------------------------------------------------


def test_build_upcoming_feature_rows_split_returns_none_when_day_base_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path / "work")
    monkeypatch.setattr(pipeline_runner, "ensure_day_base", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline_runner, "build_day_base", lambda *args, **kwargs: None)

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
    )

    assert result is None


def test_build_upcoming_feature_rows_split_returns_none_on_entry_list_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path / "work")
    monkeypatch.setattr(pipeline_runner, "ensure_day_base", lambda *args, **kwargs: day_base_dir)
    monkeypatch.setattr(
        pipeline_runner, "day_base_covers_entry_list", lambda *args, **kwargs: False
    )
    called: list[bool] = []
    monkeypatch.setattr(
        pipeline_runner,
        "build_pipeline_from_day_base",
        lambda *args, **kwargs: called.append(True) or True,
    )

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
    )

    assert result is None
    assert called == []


def test_build_upcoming_feature_rows_split_returns_none_when_race_chain_build_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path / "work")
    monkeypatch.setattr(pipeline_runner, "ensure_day_base", lambda *args, **kwargs: day_base_dir)
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        pipeline_runner, "build_pipeline_from_day_base", lambda *args, **kwargs: False
    )

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
    )

    assert result is None


def test_build_upcoming_feature_rows_split_success_reads_grouped_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import pandas as pd

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()
    monkeypatch.setattr(pipeline_runner, "ensure_day_base", lambda *args, **kwargs: day_base_dir)
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", lambda *args, **kwargs: True)

    def fake_build_pipeline_from_day_base(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        day_base_dir_arg: Path,
        final_dir: Path,
        target_race: str,
        realtime_odds_path: Path | None = None,
        venue_weather_dir: Path | None = None,
    ) -> bool:
        final_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(
            {"race_id": ["jra:2026:0712:05:11", "jra:2026:0712:05:11"], "umaban": [1, 2]}
        )
        frame.to_parquet(final_dir / "data.parquet")
        return True

    monkeypatch.setattr(
        pipeline_runner, "build_pipeline_from_day_base", fake_build_pipeline_from_day_base
    )

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
    )

    assert result is not None
    assert "jra:2026:0712:05:11" in result
    assert len(result["jra:2026:0712:05:11"]) == 2


def test_build_upcoming_feature_rows_split_falls_back_to_inline_build_day_base(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import pandas as pd

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()
    monkeypatch.setattr(pipeline_runner, "ensure_day_base", lambda *args, **kwargs: None)

    called: list[tuple[object, ...]] = []

    def fake_build_day_base(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        realtime_odds_path: Path | None = None,
        venue_weather_dir: Path | None = None,
    ) -> Path:
        called.append((category, target_date, days_ahead, database_url))
        return day_base_dir

    monkeypatch.setattr(pipeline_runner, "build_day_base", fake_build_day_base)
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", lambda *args, **kwargs: True)

    def fake_build_pipeline_from_day_base(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        day_base_dir_arg: Path,
        final_dir: Path,
        target_race: str,
        realtime_odds_path: Path | None = None,
        venue_weather_dir: Path | None = None,
    ) -> bool:
        final_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"race_id": ["jra:2026:0712:05:11"], "umaban": [1]}).to_parquet(
            final_dir / "data.parquet"
        )
        return True

    monkeypatch.setattr(
        pipeline_runner, "build_pipeline_from_day_base", fake_build_pipeline_from_day_base
    )

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
    )

    assert result is not None
    assert called == [("jra", "20260712", 0, "postgresql://u:p@h/db")]


def test_build_upcoming_feature_rows_split_returns_none_on_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path / "work")

    def raiser(*args: object, **kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(pipeline_runner, "ensure_day_base", raiser)

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
    )

    assert result is None
