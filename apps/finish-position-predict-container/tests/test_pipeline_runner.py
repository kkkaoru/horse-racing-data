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
from collections.abc import Callable, Sequence
from pathlib import Path
from time import perf_counter
from typing import cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeline_runner
from pipeline_runner import (
    PipelineDeadlineExceededError,
    SourceWatermarkOutcome,
    has_parquet_output,
    mask_pg_url,
    pipeline_execution_scope,
    r2_day_base_dest_path,
    run_with_stderr_capture,
)

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
_COMPUTE_SOURCE_WATERMARK_ATTR = "_compute_source_watermark"
_COMPUTE_SOURCE_WATERMARK_OUTCOME_ATTR = "_compute_source_watermark_outcome"
_COMPUTE_RS_WATERMARK_ATTR = "_compute_rs_watermark"
_WRITE_WATERMARK_ATTR = "_write_watermark"
_READ_WATERMARK_ATTR = "_read_watermark"
_READ_WATERMARK_REASON_ATTR = "_read_watermark_reason"
_compute_source_watermark = cast(
    "Callable[[str, str, str], tuple[str, int] | None]",
    getattr(pipeline_runner, _COMPUTE_SOURCE_WATERMARK_ATTR),
)
_compute_source_watermark_outcome = cast(
    "Callable[[str, str, str], SourceWatermarkOutcome]",
    getattr(pipeline_runner, _COMPUTE_SOURCE_WATERMARK_OUTCOME_ATTR),
)
_read_watermark_reason = cast(
    "Callable[[Path], str | None]",
    getattr(pipeline_runner, _READ_WATERMARK_REASON_ATTR),
)
_compute_rs_watermark = cast(
    "Callable[[str, str, object], tuple[str, int] | None]",
    getattr(pipeline_runner, _COMPUTE_RS_WATERMARK_ATTR),
)
_write_watermark = cast(
    "Callable[[Path, tuple[str, int, str, int]], None]",
    getattr(pipeline_runner, _WRITE_WATERMARK_ATTR),
)
_read_watermark = cast(
    "Callable[[Path], tuple[str, int, str, int] | None]",
    getattr(pipeline_runner, _READ_WATERMARK_ATTR),
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


def test_pipeline_execution_scope_bounds_the_whole_child_chain() -> None:
    started = perf_counter()
    with (
        pipeline_execution_scope(total_timeout_seconds=0.2),
        pytest.raises(PipelineDeadlineExceededError) as exc_info,
    ):
        run_with_stderr_capture(["python", "-c", "import time; time.sleep(30)"])
    elapsed = perf_counter() - started
    assert "pipeline total deadline exceeded" in str(exc_info.value)
    assert elapsed < 10.0


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


def test_has_parquet_output_false_for_flat_features_parquet(tmp_path: Path):
    base = tmp_path / "feat"
    base.mkdir()
    (base / "features.parquet").write_bytes(b"FLAT")
    assert has_parquet_output(base) is False


def test_has_parquet_output_true_for_hive_features_parquet(tmp_path: Path):
    base = tmp_path / "feat"
    partition = base / "race_year=2026"
    partition.mkdir(parents=True)
    (partition / "features.parquet").write_bytes(b"HIVE")
    assert has_parquet_output(base) is True


def test_r2_day_base_dest_path_uses_target_date_year() -> None:
    assert r2_day_base_dest_path(Path("/tmp/final"), "20260816") == Path(
        "/tmp/final/race_year=2026/features.parquet"
    )


def test_r2_day_base_dest_path_jra_run_date() -> None:
    assert r2_day_base_dest_path(Path("/tmp/final"), "20260712") == Path(
        "/tmp/final/race_year=2026/features.parquet"
    )


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
    import realtime_odds_fetcher
    import weather_fetcher

    captured_sql = ""
    captured_params: list[object] = []
    captured_race_keys: list[tuple[str, str]] | None = None
    captured_target_race: str | None = None

    def fake_query_source_rows(_url: str, sql: str, params: list[object]) -> list[tuple[str, str]]:
        nonlocal captured_sql, captured_params
        captured_sql = sql
        captured_params = params
        return [("44", "08")]

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

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)
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
    assert "and keibajo_code = ?" in captured_sql
    assert "and race_bango = ?" in captured_sql
    assert captured_params == ["20260629", "20260629", "44", "08"]


def test_build_upcoming_feature_rows_skips_pipeline_when_target_race_has_no_upcoming_rows(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    """Presence guard: target_race set + zero source rows -> build_pipeline never runs."""
    import realtime_odds_fetcher
    import weather_fetcher

    build_pipeline_called = False
    realtime_odds_called = False
    venue_weather_called = False

    def fake_query_source_rows(
        _url: str, _sql: str, _params: list[object]
    ) -> list[tuple[str, str]]:
        return []

    def fake_fetch_realtime_odds_parquet(
        category: str,
        target_date: str,
        work_dir: Path,
        race_keys: list[tuple[str, str]] | None = None,
    ) -> None:
        nonlocal realtime_odds_called
        realtime_odds_called = True
        return None

    def fake_fetch_venue_weather_dir(_target_date: str, _work_dir: Path) -> None:
        nonlocal venue_weather_called
        venue_weather_called = True
        return None

    def fake_build_pipeline(*_args: object, **_kwargs: object) -> bool:
        nonlocal build_pipeline_called
        build_pipeline_called = True
        return False

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)
    monkeypatch.setattr(
        realtime_odds_fetcher,
        "fetch_realtime_odds_parquet",
        fake_fetch_realtime_odds_parquet,
    )
    monkeypatch.setattr(weather_fetcher, "fetch_venue_weather_dir", fake_fetch_venue_weather_dir)
    monkeypatch.setattr(pipeline_runner, "build_pipeline", fake_build_pipeline)

    rows = pipeline_runner.build_upcoming_feature_rows(
        "jra",
        "20260718",
        0,
        "postgresql://u:p@h/db",
        target_race="01:11",
    )

    assert rows == {}
    assert build_pipeline_called is False
    assert realtime_odds_called is False
    assert venue_weather_called is False
    assert capsys.readouterr().err == ""


def test_build_upcoming_feature_rows_runs_pipeline_when_target_race_has_upcoming_rows(
    monkeypatch: pytest.MonkeyPatch,
):
    """Presence guard does not fire when target_race has >=1 source row (unchanged path)."""
    import realtime_odds_fetcher
    import weather_fetcher

    build_pipeline_called = False

    def fake_query_source_rows(
        _url: str, _sql: str, _params: list[object]
    ) -> list[tuple[str, str]]:
        return [("01", "11")]

    def fake_fetch_realtime_odds_parquet(
        category: str,
        target_date: str,
        work_dir: Path,
        race_keys: list[tuple[str, str]] | None = None,
    ) -> None:
        return None

    def fake_fetch_venue_weather_dir(_target_date: str, _work_dir: Path) -> None:
        return None

    def fake_build_pipeline(*_args: object, **_kwargs: object) -> bool:
        nonlocal build_pipeline_called
        build_pipeline_called = True
        return False

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)
    monkeypatch.setattr(
        realtime_odds_fetcher,
        "fetch_realtime_odds_parquet",
        fake_fetch_realtime_odds_parquet,
    )
    monkeypatch.setattr(weather_fetcher, "fetch_venue_weather_dir", fake_fetch_venue_weather_dir)
    monkeypatch.setattr(pipeline_runner, "build_pipeline", fake_build_pipeline)

    rows = pipeline_runner.build_upcoming_feature_rows(
        "jra",
        "20260718",
        0,
        "postgresql://u:p@h/db",
        target_race="01:11",
    )

    assert rows == {}
    assert build_pipeline_called is True


def test_build_upcoming_feature_rows_runs_pipeline_when_no_target_race_even_if_keys_empty(
    monkeypatch: pytest.MonkeyPatch,
):
    """Presence guard is scoped to target_race only -- whole-day calls are unaffected
    even when the upcoming-race-keys query happens to return zero rows."""
    import realtime_odds_fetcher
    import weather_fetcher

    build_pipeline_called = False

    def fake_query_source_rows(
        _url: str, _sql: str, _params: list[object]
    ) -> list[tuple[str, str]]:
        return []

    def fake_fetch_realtime_odds_parquet(
        category: str,
        target_date: str,
        work_dir: Path,
        race_keys: list[tuple[str, str]] | None = None,
    ) -> None:
        return None

    def fake_fetch_venue_weather_dir(_target_date: str, _work_dir: Path) -> None:
        return None

    def fake_build_pipeline(*_args: object, **_kwargs: object) -> bool:
        nonlocal build_pipeline_called
        build_pipeline_called = True
        return False

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)
    monkeypatch.setattr(
        realtime_odds_fetcher,
        "fetch_realtime_odds_parquet",
        fake_fetch_realtime_odds_parquet,
    )
    monkeypatch.setattr(weather_fetcher, "fetch_venue_weather_dir", fake_fetch_venue_weather_dir)
    monkeypatch.setattr(pipeline_runner, "build_pipeline", fake_build_pipeline)

    rows = pipeline_runner.build_upcoming_feature_rows(
        "jra",
        "20260718",
        1,
        "postgresql://u:p@h/db",
        target_race=None,
    )

    assert rows == {}
    assert build_pipeline_called is True


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
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    import psycopg

    executed_sql: list[str] = []
    inserted_params: list[tuple[object, ...]] = []
    state = {"committed": False, "closed": False}

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            executed_sql.append(sql)
            if params is not None:
                inserted_params.append(params)

        def fetchone(self) -> tuple[object, ...] | None:
            return ("off",)

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            state["committed"] = True

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            state["closed"] = True

    captured_connect_kwargs: dict[str, object] = {}

    def fake_connect(url: str, **kwargs: object) -> FakeConn:
        captured_connect_kwargs["url"] = url
        captured_connect_kwargs.update(kwargs)
        return FakeConn()

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    wrote = pipeline_runner.record_layer_timing_row(
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
    assert wrote is True

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
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    import psycopg

    inserted_params: list[tuple[object, ...]] = []

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            if params is not None:
                inserted_params.append(params)

        def fetchone(self) -> tuple[object, ...] | None:
            return ("off",)

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            return None

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
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    import psycopg

    def fake_connect_raises(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom-connect")

    monkeypatch.setattr(psycopg, "connect", fake_connect_raises)

    wrote = pipeline_runner.record_layer_timing_row(
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
    assert wrote is False

    captured = capsys.readouterr()
    assert captured.err == ""


def test_record_layer_timing_row_logs_connect_error_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    import psycopg

    def fake_connect_raises(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom-connect")

    monkeypatch.setattr(psycopg, "connect", fake_connect_raises)

    wrote = pipeline_runner.record_layer_timing_row(
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
    assert wrote is False
    captured = capsys.readouterr()
    assert "debug-timing write failed" in captured.err
    assert "boom-connect" in captured.err


def test_record_layer_timing_row_swallows_execute_error_and_still_closes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
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

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            state["closed"] = True

    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    wrote = pipeline_runner.record_layer_timing_row(
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
    assert wrote is False

    captured = capsys.readouterr()
    assert captured.err == ""
    assert state["closed"] is True


def test_record_layer_timing_row_refuses_read_only_transaction(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    import psycopg

    state = {"committed": False, "rolled_back": False, "closed": False, "inserted": False}

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            if params is not None:
                state["inserted"] = True

        def fetchone(self) -> tuple[object, ...] | None:
            return ("on",)

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            state["committed"] = True

        def rollback(self) -> None:
            state["rolled_back"] = True

        def close(self) -> None:
            state["closed"] = True

    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    wrote = pipeline_runner.record_layer_timing_row(
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
    assert wrote is False
    assert state["inserted"] is False
    assert state["committed"] is False
    assert state["rolled_back"] is True
    assert state["closed"] is True
    captured = capsys.readouterr()
    assert captured.err == ""


def test_record_layer_timing_row_refuses_missing_read_only_row(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    import psycopg

    state = {"committed": False, "inserted": False}

    class FakeCursor:
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            if params is not None:
                state["inserted"] = True

        def fetchone(self) -> tuple[object, ...] | None:
            return None

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            state["committed"] = True

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    wrote = pipeline_runner.record_layer_timing_row(
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
    assert wrote is False
    assert state["inserted"] is False
    assert state["committed"] is False
    captured = capsys.readouterr()
    assert captured.err == ""


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


def test_build_day_base_commits_running_style_foundation_before_day_layers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)
    monkeypatch.setattr(
        pipeline_runner, "_query_source_rows", lambda *_args, **_kwargs: [("20260822", 477)]
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    events: list[str] = []

    def fake_base_argv(*args: object, **kwargs: object) -> list[str]:
        return ["base", str(args[5])]

    def fake_layer_argv(*args: object, **kwargs: object) -> list[str]:
        return ["layer", str(args[4])]

    def fake_run(args: list[str]) -> None:
        Path(args[-1]).mkdir(parents=True, exist_ok=True)
        events.append(args[0])

    def commit(category: str, target_date: str, base_dir: Path, watermark: tuple[str, int]) -> None:
        assert base_dir == _day_base_dir("jra", "20260822") / "base"
        assert (category, target_date, watermark) == ("jra", "20260822", ("20260822", 477))
        events.append("foundation")

    monkeypatch.setattr(pipeline_runner, "build_base_argv", fake_base_argv)
    monkeypatch.setattr(pipeline_runner, "build_layer_argv", fake_layer_argv)
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run)

    result = pipeline_runner.build_day_base(
        "jra",
        "20260822",
        0,
        "r2-catalog://pc-keiba",
        running_style_foundation_commit_fn=commit,
    )

    assert result is not None
    assert events == ["base", "foundation", "layer"]


def test_build_day_base_keeps_building_when_foundation_commit_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path / "work")
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ())
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)
    monkeypatch.setattr(
        pipeline_runner, "_query_source_rows", lambda *_args, **_kwargs: [("20260822", 477)]
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner, "build_base_argv", lambda *args, **_kwargs: ["base", str(args[5])]
    )
    monkeypatch.setattr(
        pipeline_runner,
        "run_with_stderr_capture",
        lambda args: Path(args[-1]).mkdir(parents=True, exist_ok=True),
    )

    def fail_commit(*_args: object) -> None:
        raise RuntimeError("R2 handoff failed")

    result = pipeline_runner.build_day_base(
        "jra",
        "20260822",
        0,
        "r2-catalog://pc-keiba",
        running_style_foundation_commit_fn=fail_commit,
    )

    assert result is not None


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


def test_build_day_base_writes_watermark_for_catalog_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Offline (``postgresql://``) builds never write a watermark -- only
    catalog-source (``r2-catalog://``) builds do, since only catalog-source
    reads consult one in ``ensure_day_base``."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)
    monkeypatch.setattr(
        pipeline_runner, "build_base_argv", lambda *args, **kwargs: ["base", str(args[5])]
    )
    monkeypatch.setattr(
        pipeline_runner, "build_layer_argv", lambda *args, **kwargs: ["layer", str(args[4])]
    )
    monkeypatch.setattr(
        pipeline_runner,
        "run_with_stderr_capture",
        lambda a: Path(a[-1]).mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(
        pipeline_runner, "_query_source_rows", lambda *_args, **_kwargs: [("20260712", 1200)]
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.build_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba")

    assert result is not None
    day_dir = _day_base_dir("jra", "20260712")
    assert _read_watermark(day_dir) == ("20260712", 1200, "none", 0)


def test_build_day_base_skips_watermark_when_source_count_is_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """A 0-row concat query is fail-closed: parquet may exist, but no sidecar
    is written. A frozen empty pair would hide later SE corrections."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)
    monkeypatch.setattr(
        pipeline_runner, "build_base_argv", lambda *args, **kwargs: ["base", str(args[5])]
    )
    monkeypatch.setattr(
        pipeline_runner, "build_layer_argv", lambda *args, **kwargs: ["layer", str(args[4])]
    )
    monkeypatch.setattr(
        pipeline_runner,
        "run_with_stderr_capture",
        lambda a: Path(a[-1]).mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(
        pipeline_runner, "_query_source_rows", lambda *_args, **_kwargs: [(None, 0)]
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.build_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba")

    assert result is not None
    day_dir = _day_base_dir("ban-ei", "20260816")
    assert _read_watermark(day_dir) is None
    assert _read_watermark_reason(day_dir) == "watermark count is 0"


def test_build_day_base_watermark_reason_silent_without_debug(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)
    monkeypatch.setattr(
        pipeline_runner, "build_base_argv", lambda *args, **kwargs: ["base", str(args[5])]
    )
    monkeypatch.setattr(
        pipeline_runner, "build_layer_argv", lambda *args, **kwargs: ["layer", str(args[4])]
    )
    monkeypatch.setattr(
        pipeline_runner,
        "run_with_stderr_capture",
        lambda a: Path(a[-1]).mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(
        pipeline_runner, "_query_source_rows", lambda *_args, **_kwargs: [(None, 0)]
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    pipeline_runner.build_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba")

    assert capsys.readouterr().err == ""


def test_build_day_base_watermark_reason_logs_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "day_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)
    monkeypatch.setattr(
        pipeline_runner, "build_base_argv", lambda *args, **kwargs: ["base", str(args[5])]
    )
    monkeypatch.setattr(
        pipeline_runner, "build_layer_argv", lambda *args, **kwargs: ["layer", str(args[4])]
    )
    monkeypatch.setattr(
        pipeline_runner,
        "run_with_stderr_capture",
        lambda a: Path(a[-1]).mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(
        pipeline_runner, "_query_source_rows", lambda *_args, **_kwargs: [(None, 0)]
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    pipeline_runner.build_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba")

    assert (
        "[day-base] watermark absent category=ban-ei target_date=20260816 "
        "reason=watermark count is 0" in capsys.readouterr().err
    )


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
# _compute_source_watermark / _write_watermark / _read_watermark
# ---------------------------------------------------------------------------


def test_compute_source_watermark_returns_max_updated_and_row_count(
    monkeypatch: pytest.MonkeyPatch,
):
    captured_sql: list[str] = []

    def fake_query_source_rows(
        _url: str, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[object, ...]]:
        captured_sql.append(sql)
        assert list(params) == []
        return [("20260712", 946)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    result = _compute_source_watermark("jra", "20260712", "r2-catalog://pc-keiba")

    assert result == ("20260712", 946)
    assert "jvd_se" in captured_sql[0]
    assert "data_sakusei_nengappi" in captured_sql[0]
    assert "?" not in captured_sql[0]
    assert "(kaisai_nen || kaisai_tsukihi) between '20260712' and '20260712'" in captured_sql[0]
    assert "kaisai_nen between '2026' and '2026'" in captured_sql[0]


def test_compute_source_watermark_uses_nvd_se_for_nar_and_banei(monkeypatch: pytest.MonkeyPatch):
    captured_sql: list[str] = []
    captured_params: list[list[object]] = []

    def fake_query_source_rows(
        _url: str, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[object, ...]]:
        captured_sql.append(sql)
        captured_params.append(list(params))
        return [("20260712", 500)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    _compute_source_watermark("nar", "20260712", "r2-catalog://pc-keiba")
    _compute_source_watermark("ban-ei", "20260712", "r2-catalog://pc-keiba")

    assert "nvd_se" in captured_sql[0]
    assert "keibajo_code <> '83'" in captured_sql[0]
    assert "?" not in captured_sql[0]
    assert "(kaisai_nen || kaisai_tsukihi) between '20260712' and '20260712'" in captured_sql[0]
    assert "nvd_se" in captured_sql[1]
    assert "keibajo_code = '83'" in captured_sql[1]
    assert "?" not in captured_sql[1]
    assert "(kaisai_nen || kaisai_tsukihi) between '20260712' and '20260712'" in captured_sql[1]
    assert captured_params[0] == []
    assert captured_params[1] == []


def test_compute_source_watermark_returns_none_on_query_exception(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    def raiser(*_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        raise RuntimeError("attach failed")

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", raiser)

    result = _compute_source_watermark("jra", "20260712", "r2-catalog://pc-keiba")

    assert result is None
    assert capsys.readouterr().err == ""
    outcome = _compute_source_watermark_outcome("jra", "20260712", "r2-catalog://pc-keiba")
    assert outcome.value is None
    assert outcome.reason == "watermark query failed: attach failed"


def test_compute_source_watermark_returns_none_when_zero_rows(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(pipeline_runner, "_query_source_rows", lambda *_a, **_k: [(None, 0)])

    result = _compute_source_watermark("jra", "20260712", "r2-catalog://pc-keiba")

    assert result is None
    outcome = _compute_source_watermark_outcome("jra", "20260712", "r2-catalog://pc-keiba")
    assert outcome.value is None
    assert outcome.reason == "watermark count is 0"


def test_compute_source_watermark_rejects_non_ymd_date_without_query(
    monkeypatch: pytest.MonkeyPatch,
):
    called: list[bool] = []

    def fake_query_source_rows(
        _url: str, _sql: str, _params: Sequence[object] = ()
    ) -> list[tuple[object, ...]]:
        called.append(True)
        return [("20260712", 1)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    result = _compute_source_watermark("ban-ei", "2026-0816", "r2-catalog://pc-keiba")

    assert result is None
    assert called == []
    outcome = _compute_source_watermark_outcome("ban-ei", "2026081", "r2-catalog://pc-keiba")
    assert outcome.value is None
    assert outcome.reason == "watermark target_date is not YYYYMMDD"


def test_compute_source_watermark_returns_none_when_query_returns_no_rows(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(pipeline_runner, "_query_source_rows", lambda *_a, **_k: [])

    result = _compute_source_watermark("ban-ei", "20260816", "r2-catalog://pc-keiba")

    assert result is None


def test_compute_source_watermark_banei_zero_count_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
):
    captured_sql: list[str] = []

    def fake_query_source_rows(
        _url: str, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[object, ...]]:
        captured_sql.append(sql)
        assert list(params) == []
        return [(None, 0)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    result = _compute_source_watermark("ban-ei", "20260816", "r2-catalog://pc-keiba")

    assert result is None
    assert "nvd_se" in captured_sql[0]
    assert "keibajo_code = '83'" in captured_sql[0]
    assert "?" not in captured_sql[0]
    assert "(kaisai_nen || kaisai_tsukihi) between '20260816' and '20260816'" in captured_sql[0]


def test_compute_source_watermark_banei_concat_date_returns_count(
    monkeypatch: pytest.MonkeyPatch,
):
    captured_sql: list[str] = []

    def fake_query_source_rows(
        _url: str, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[object, ...]]:
        captured_sql.append(sql)
        assert list(params) == []
        return [("20260816", 80)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    result = _compute_source_watermark("ban-ei", "20260816", "r2-catalog://pc-keiba")

    assert result == ("20260816", 80)
    assert "nvd_se" in captured_sql[0]
    assert "keibajo_code = '83'" in captured_sql[0]
    assert "?" not in captured_sql[0]
    assert "(kaisai_nen || kaisai_tsukihi) between '20260816' and '20260816'" in captured_sql[0]
    assert "kaisai_nen between '2026' and '2026'" in captured_sql[0]


def test_compute_source_watermark_coerces_string_row_count(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        pipeline_runner, "_query_source_rows", lambda *_a, **_k: [("20260816", "80")]
    )

    result = _compute_source_watermark("ban-ei", "20260816", "r2-catalog://pc-keiba")

    assert result == ("20260816", 80)


def test_compute_source_watermark_null_max_with_nonzero_count_uses_none_token(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(pipeline_runner, "_query_source_rows", lambda *_a, **_k: [(None, 80)])

    result = _compute_source_watermark("ban-ei", "20260816", "r2-catalog://pc-keiba")

    assert result == ("none", 80)


def test_compute_source_watermark_blank_max_with_nonzero_count_uses_none_token(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(pipeline_runner, "_query_source_rows", lambda *_a, **_k: [("   ", 80)])

    result = _compute_source_watermark("ban-ei", "20260816", "r2-catalog://pc-keiba")

    assert result == ("none", 80)


def test_compute_rs_watermark_banei_returns_none_token():
    result = _compute_rs_watermark("ban-ei", "20260816", None)

    assert result == ("none", 0)


def test_compute_rs_watermark_jra_without_r2_returns_none():
    result = _compute_rs_watermark("jra", "20260816", None)

    assert result is None


def test_write_then_read_watermark_round_trips(tmp_path: Path):
    day_dir = tmp_path / "daybase-jra-20260712"
    day_dir.mkdir(parents=True)

    _write_watermark(day_dir, ("20260712", 946, "2026-07-18T09:00:00", 12))

    assert _read_watermark(day_dir) == ("20260712", 946, "2026-07-18T09:00:00", 12)


def test_read_watermark_returns_none_when_file_missing(tmp_path: Path):
    day_dir = tmp_path / "daybase-jra-20260712"
    day_dir.mkdir(parents=True)

    assert _read_watermark(day_dir) is None


def test_read_watermark_returns_none_on_malformed_json(tmp_path: Path):
    day_dir = tmp_path / "daybase-jra-20260712"
    day_dir.mkdir(parents=True)
    (day_dir / "watermark.json").write_text("not json", encoding="utf-8")

    assert _read_watermark(day_dir) is None


def test_write_watermark_failure_is_best_effort(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    """A watermark write to a path that cannot be created must not raise --
    it degrades the NEXT ensure_day_base call to a safe rebuild, never this
    build's own success (see build_day_base's call site)."""
    unwritable_dir = tmp_path / "not-a-real-parent" / "nested" / "too-deep"

    _write_watermark(unwritable_dir, ("20260712", 946, "none", 0))

    assert capsys.readouterr().err == ""


# ---------------------------------------------------------------------------
# ensure_day_base
# ---------------------------------------------------------------------------


def test_ensure_day_base_local_disk_hit_skips_r2(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
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


def test_ensure_day_base_catalog_source_watermark_match_returns_local_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Replaces the old ``test_ensure_day_base_catalog_source_rejects_existing_local_and_r2``:
    the catalog-source path is no longer an unconditional bypass (e6111ca6's
    original fix) -- it now trusts a local-disk day-base when a FRESH
    watermark matches the one recorded alongside it. R2 is still never
    attempted for catalog sources (deliberately out of scope for this
    change -- see ``ensure_day_base``'s own docstring)."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    hive_dir = final_dir / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"TRUSTED")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    r2_calls: list[bool] = []
    monkeypatch.setattr(
        pipeline_runner, "r2_get_parquet", lambda *args, **kwargs: r2_calls.append(True) or True
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result == final_dir
    assert r2_calls == []


def test_ensure_day_base_catalog_source_banei_none_rs_match_returns_local_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """HIT path for Ban-ei: concat source rows plus the absent-RS token
    ``("none", 0)`` match the sidecar build wrote."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    hive_dir = final_dir / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"TRUSTED-BANEI")
    _write_watermark(day_dir, ("20260816", 80, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260816", 80)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result == final_dir


def test_ensure_day_base_catalog_source_zero_count_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """A 0-row concat query must not HIT even if a sidecar was left behind."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"UNTRUSTED-ZERO")
    _write_watermark(day_dir, ("20260816", 80, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [(None, 0)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result is None


def test_ensure_day_base_hit_local_silent_without_debug(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    hive_dir = final_dir / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"TRUSTED")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result == final_dir
    assert capsys.readouterr().err == ""


def test_ensure_day_base_hit_local_logs_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    hive_dir = final_dir / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"TRUSTED")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result == final_dir
    assert (
        "[day-base] HIT local category=jra target_date=20260712 reason=watermark-match"
        in capsys.readouterr().err
    )


def test_ensure_day_base_hit_records_daybase_hit_step(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.debug_log import drain_debug_progress

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    hive_dir = final_dir / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"TRUSTED")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result == final_dir
    assert drain_debug_progress() == [
        "step=daybase-hit source=local category=jra target_date=20260712 reason=watermark-match"
    ]


def test_ensure_day_base_miss_records_daybase_miss_step(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.debug_log import drain_debug_progress

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result is None
    assert drain_debug_progress() == [
        "step=daybase-miss category=jra target_date=20260712 reason=no-local-cache"
    ]


def test_ensure_day_base_hit_silent_step_queue_without_debug(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.debug_log import drain_debug_progress

    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    hive_dir = final_dir / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"TRUSTED")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result == final_dir
    assert drain_debug_progress() == []


def test_ensure_day_base_zero_count_logs_miss_not_hit_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"UNTRUSTED-ZERO")
    _write_watermark(day_dir, ("20260816", 80, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [(None, 0)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result is None
    captured = capsys.readouterr()
    assert (
        "[day-base] MISS category=ban-ei target_date=20260816 reason=watermark count is 0"
        in captured.err
    )
    assert "HIT" not in captured.err


def test_ensure_day_base_zero_count_silent_without_debug(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"UNTRUSTED-ZERO")
    _write_watermark(day_dir, ("20260816", 80, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [(None, 0)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result is None
    assert capsys.readouterr().err == ""


def test_presence_guard_logs_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    import realtime_odds_fetcher
    import weather_fetcher

    def fake_query_source_rows(
        _url: str, _sql: str, _params: list[object]
    ) -> list[tuple[str, str]]:
        return []

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)
    monkeypatch.setattr(
        realtime_odds_fetcher, "fetch_realtime_odds_parquet", lambda *_a, **_k: None
    )
    monkeypatch.setattr(weather_fetcher, "fetch_venue_weather_dir", lambda *_a, **_k: None)
    monkeypatch.setattr(pipeline_runner, "build_pipeline", lambda *_a, **_k: False)

    rows = pipeline_runner.build_upcoming_feature_rows(
        "jra",
        "20260718",
        0,
        "postgresql://u:p@h/db",
        target_race="01:11",
    )

    assert rows == {}
    assert (
        "[pipeline] presence-guard: target_race=01:11 category=jra "
        "has zero upcoming rows in source catalog -> skipping feature build"
        in capsys.readouterr().err
    )


def test_ensure_day_base_catalog_source_watermark_mismatch_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"STALE")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))
    # A late correction/scratch bumped the row count -- the current source
    # no longer matches what this cached day-base was built from.
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1201)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result is None


def test_ensure_day_base_catalog_source_watermark_mismatch_on_rs_side_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """The entrant side can match while the RS side has moved on (new
    running-style predictions landed since this day-base was built) -- either
    side mismatching must force a rebuild, not just the entrant side."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"STALE-RS")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    # Running-style predictions have since been written for this day.
    monkeypatch.setattr(
        pipeline_runner,
        "_compute_rs_watermark",
        lambda *_args, **_kwargs: ("2026-07-18T09:00:00", 177),
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result is None


def test_ensure_day_base_catalog_source_no_watermark_file_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """A local-disk day-base with no watermark sidecar (e.g. left over from
    before this change, or a write failure at build time) is untrusted even
    though the parquet itself is present."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    final_dir = _day_base_dir("jra", "20260712") / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"NO-WATERMARK")
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result is None


def test_ensure_day_base_catalog_source_watermark_query_fails_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("jra", "20260712")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"UNVERIFIABLE")
    _write_watermark(day_dir, ("20260712", 1200, "none", 0))

    def raiser(*_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
        raise RuntimeError("catalog attach failed")

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", raiser)
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    r2 = None

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", r2)

    assert result is None


def test_ensure_day_base_catalog_source_r2_watermark_match_fetches_and_returns_final_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Task #32 -- cross-process reuse: this shard never built (or downloaded)
    a local day-base for this category+day, but a DIFFERENT process (another
    ``RACE_SHARDED_DO`` shard, or the ``/prewarm-day-base`` job) already put
    one in R2 with matching watermark metadata. A signed HEAD confirms
    freshness before the (more expensive) GET pays for the body."""
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    head_calls: list[str] = []

    def fake_r2_head_watermark(_r2: R2Config, object_key: str) -> tuple[str, int, str, int] | None:
        head_calls.append(object_key)
        return ("20260712", 1200, "none", 0)

    get_calls: list[str] = []

    def fake_r2_get_parquet(_r2: R2Config, object_key: str, dest: Path) -> bool:
        get_calls.append(object_key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"R2-DAY-BASE")
        return True

    monkeypatch.setattr(pipeline_runner, "r2_head_watermark", fake_r2_head_watermark)
    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", fake_r2_get_parquet)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", r2)

    final_dir = _day_base_dir("jra", "20260712") / "final"
    assert result == final_dir
    assert head_calls == ["feat-daybase/catalog-v1/jra/20260712/features.parquet"]
    assert get_calls == ["feat-daybase/catalog-v1/jra/20260712/features.parquet"]
    assert (final_dir / "race_year=2026" / "features.parquet").read_bytes() == b"R2-DAY-BASE"


def test_ensure_day_base_r2_hit_logs_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from predict_lib.serve import R2Config

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_r2, **_kwargs: ("20260712", 1200, "none", 0),
    )

    def fake_r2_get_parquet(_r2: object, _key: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"R2-DAY-BASE")
        return True

    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", fake_r2_get_parquet)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", r2)

    assert result is not None
    assert (result / "race_year=2026" / "features.parquet").read_bytes() == b"R2-DAY-BASE"
    assert (
        "[day-base] HIT r2 category=jra target_date=20260712 reason=watermark-match"
        in capsys.readouterr().err
    )


def test_ensure_day_base_catalog_source_r2_watermark_mismatch_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    from predict_lib.debug_log import drain_debug_progress
    from predict_lib.serve import R2Config

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: ("20260711", 1199, "none", 0),
    )
    get_calls: list[str] = []
    monkeypatch.setattr(
        pipeline_runner,
        "r2_get_parquet",
        lambda _r2, object_key, _dest: get_calls.append(object_key) or True,
    )
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", r2)

    assert result is None
    assert get_calls == []
    assert drain_debug_progress() == [
        "step=daybase-miss category=jra target_date=20260712 reason=r2-watermark-mismatch"
    ]


def test_ensure_day_base_banei_20260817_r2_missing_object_logs_r2_missing_object(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.debug_log import drain_debug_progress
    from predict_lib.serve import R2Config

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260815", 118)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: None,
    )
    get_calls: list[str] = []
    monkeypatch.setattr(
        pipeline_runner,
        "r2_get_parquet",
        lambda _r2, object_key, _dest: get_calls.append(object_key) or True,
    )
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("ban-ei", "20260817", 0, "r2-catalog://pc-keiba", r2)

    assert result is None
    assert get_calls == []
    assert drain_debug_progress() == [
        "step=daybase-miss category=ban-ei target_date=20260817 reason=r2-missing-object"
    ]


def test_ensure_day_base_banei_20260817_r2_watermark_mismatch_logs_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.debug_log import drain_debug_progress
    from predict_lib.serve import R2Config

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260815", 118)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: ("20260814", 117, "none", 0),
    )
    get_calls: list[str] = []
    monkeypatch.setattr(
        pipeline_runner,
        "r2_get_parquet",
        lambda _r2, object_key, _dest: get_calls.append(object_key) or True,
    )
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("ban-ei", "20260817", 0, "r2-catalog://pc-keiba", r2)

    assert result is None
    assert get_calls == []
    assert drain_debug_progress() == [
        "step=daybase-miss category=ban-ei target_date=20260817 reason=r2-watermark-mismatch"
    ]


def test_ensure_day_base_banei_20260817_r2_watermark_match_hits_hive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.debug_log import drain_debug_progress
    from predict_lib.serve import R2Config

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260815", 118)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: ("20260815", 118, "none", 0),
    )

    def fake_r2_get_parquet(_r2: object, _key: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"BANEI-20260817-R2-DAY-BASE")
        return True

    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", fake_r2_get_parquet)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("ban-ei", "20260817", 0, "r2-catalog://pc-keiba", r2)

    final_dir = _day_base_dir("ban-ei", "20260817") / "final"
    assert result == final_dir
    assert (final_dir / "race_year=2026" / "features.parquet").read_bytes() == (
        b"BANEI-20260817-R2-DAY-BASE"
    )
    assert drain_debug_progress() == [
        "step=daybase-hit source=r2 category=ban-ei target_date=20260817 reason=watermark-match"
    ]


def test_ensure_day_base_catalog_source_r2_watermark_match_but_get_fails_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: ("20260712", 1200, "none", 0),
    )
    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", lambda *_args, **_kwargs: False)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", r2)

    assert result is None


def test_ensure_day_base_catalog_source_r2_head_exception_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    def raiser(*_args: object, **_kwargs: object) -> tuple[str, int, str, int] | None:
        raise RuntimeError("head request boom")

    monkeypatch.setattr(pipeline_runner, "r2_head_watermark", raiser)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", r2)

    assert result is None
    assert capsys.readouterr().err == ""


def test_ensure_day_base_catalog_source_no_r2_config_skips_r2_branch_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Regression guard: when no R2 credentials are configured (the offline
    single-shard case), the catalog-source path must fall straight through to
    ``None`` on a local-disk miss without ever attempting the R2 HEAD --
    unchanged behavior for callers that never pass ``r2_config``."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260712", 1200)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    head_calls: list[bool] = []
    monkeypatch.setattr(
        pipeline_runner, "r2_head_watermark", lambda *_args, **_kwargs: head_calls.append(True)
    )

    result = pipeline_runner.ensure_day_base("jra", "20260712", 0, "r2-catalog://pc-keiba", None)

    assert result is None
    assert head_calls == []


def test_ensure_day_base_r2_hit_when_local_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")
    captured_keys: list[str] = []

    def fake_r2_get_parquet(_r2: R2Config, object_key: str, dest: Path) -> bool:
        captured_keys.append(object_key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"OFFLINE-R2")
        return True

    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", fake_r2_get_parquet)

    result = pipeline_runner.ensure_day_base("nar", "20260712", 0, "postgresql://u:p@h/db", r2)

    final_dir = _day_base_dir("nar", "20260712") / "final"
    assert result == final_dir
    assert captured_keys == ["feat-daybase/catalog-v1/nar/20260712/features.parquet"]
    assert (final_dir / "race_year=2026" / "features.parquet").read_bytes() == b"OFFLINE-R2"


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
    assert captured.err == ""


def test_ensure_day_base_r2_get_true_without_hive_file_is_miss(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A True GET that does not land race_year=*/*.parquet must not HIT."""
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260814", 117)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: ("20260814", 117, "none", 0),
    )
    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", lambda *_args, **_kwargs: True)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", r2)

    final_dir = _day_base_dir("ban-ei", "20260816") / "final"
    assert result is None
    assert list(final_dir.glob("race_year=*/*.parquet")) == []


def test_ensure_day_base_local_flat_parquet_is_not_a_hit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Leftover flat final/features.parquet + matching watermark is not a HIT."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"FLAT-LEFTOVER")
    _write_watermark(day_dir, ("20260814", 117, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260814", 117)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result is None
    assert (final_dir / "features.parquet").read_bytes() == b"FLAT-LEFTOVER"
    assert list(final_dir.glob("race_year=*/*.parquet")) == []


def test_ensure_day_base_r2_hit_materializes_hive_for_banei_20260816(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Production Ban-ei 20260816 watermark: R2 GET must restore hive layout."""
    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260814", 117)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: ("20260814", 117, "none", 0),
    )

    def fake_r2_get_parquet(_r2: object, _key: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"BANEI-R2-DAY-BASE")
        return True

    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", fake_r2_get_parquet)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", r2)

    final_dir = _day_base_dir("ban-ei", "20260816") / "final"
    assert result == final_dir
    assert (final_dir / "race_year=2026" / "features.parquet").read_bytes() == b"BANEI-R2-DAY-BASE"
    assert list(final_dir.glob("race_year=*/*.parquet")) == [
        final_dir / "race_year=2026" / "features.parquet"
    ]


def test_build_upcoming_feature_rows_split_r2_hit_does_not_rebuild_banei_day_base(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """R2 HIT + race-chain must not fall back to build_day_base / step=base."""
    import pandas as pd

    from predict_lib.serve import R2Config

    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260814", 117)],
    )
    monkeypatch.setattr(
        pipeline_runner, "_compute_rs_watermark", lambda *_args, **_kwargs: ("none", 0)
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: ("20260814", 117, "none", 0),
    )

    def fake_r2_get_parquet(_r2: object, _key: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"BANEI-R2-DAY-BASE")
        return True

    monkeypatch.setattr(pipeline_runner, "r2_get_parquet", fake_r2_get_parquet)

    def refuse_rebuild(*_args: object, **_kwargs: object) -> Path:
        raise AssertionError("build_day_base must not run on a watermark HIT")

    monkeypatch.setattr(pipeline_runner, "build_day_base", refuse_rebuild)
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", lambda *_a, **_k: True)

    race_chain_calls: list[tuple[Path, str]] = []

    def fake_race_chain(
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
        race_chain_calls.append((day_base_dir_arg, target_race))
        final_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "race_id": ["ban-ei:2026:0816:83:03"],
                "umaban": [1],
            }
        ).to_parquet(final_dir / "data.parquet")
        return True

    monkeypatch.setattr(pipeline_runner, "build_pipeline_from_day_base", fake_race_chain)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "ban-ei", "20260816", 0, "r2-catalog://pc-keiba", "83:03", r2_config=r2
    )

    day_base_final = _day_base_dir("ban-ei", "20260816") / "final"
    assert result == {
        "ban-ei:2026:0816:83:03": [
            {"race_id": "ban-ei:2026:0816:83:03", "umaban": 1},
        ]
    }
    assert race_chain_calls == [(day_base_final, "83:03")]
    assert (day_base_final / "race_year=2026" / "features.parquet").read_bytes() == (
        b"BANEI-R2-DAY-BASE"
    )


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
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(
        day_base_dir,
        [
            ("jra:2026:0712:05:11", "H1"),
            ("jra:2026:0712:05:11", "H2"),
            ("jra:2026:0712:05:12", "H3"),
        ],
    )

    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda _url, _sql, _params: [("H1",), ("H2",)],
    )

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "20260712", "05:11", "postgresql://u:p@h/db"
    )

    assert result is True


def test_day_base_covers_entry_list_false_when_current_horse_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("jra:2026:0712:05:11", "H1")])

    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda _url, _sql, _params: [("H1",), ("H2",)],
    )

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "20260712", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False


def test_day_base_covers_entry_list_only_matches_target_race_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """A horse present elsewhere in the day-base but NOT under the target
    race's own keibajo/bango must not count as coverage — proves the SQL
    filter is scoped to the target race, not the whole day-base."""
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(
        day_base_dir,
        [
            ("jra:2026:0712:05:11", "H1"),
            ("jra:2026:0712:06:11", "H9"),
        ],
    )

    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda _url, _sql, _params: [("H9",)],
    )

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "20260712", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False


def test_day_base_covers_entry_list_false_when_no_current_entrants(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("jra:2026:0712:05:11", "H1")])

    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda _url, _sql, _params: [],
    )

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "20260712", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False


def test_day_base_covers_entry_list_false_on_source_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("jra:2026:0712:05:11", "H1")])

    def raiser(_url: str, _sql: str, _params: tuple[object, ...]) -> None:
        raise RuntimeError("connect boom")

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", raiser)

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "20260712", "05:11", "postgresql://u:p@h/db"
    )

    assert result is False
    captured = capsys.readouterr()
    assert captured.err == ""


def test_day_base_covers_entry_list_uses_nvd_se_for_nar(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("nar:2026:0712:30:03", "H1")])

    captured_sql: list[str] = []
    captured_params: list[tuple[object, ...]] = []

    def fake_query_source_rows(
        _url: str, sql: str, _params: tuple[object, ...]
    ) -> list[tuple[str]]:
        captured_sql.append(sql)
        captured_params.append(_params)
        return [("H1",)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "nar", "20260712", "30:03", "postgresql://u:p@h/db"
    )

    assert result is True
    assert captured_sql == [
        """
            select distinct ketto_toroku_bango
            from pg.nvd_se
            where kaisai_nen between '2026' and '2026'
              and (kaisai_nen || kaisai_tsukihi) between '20260712' and '20260712'
              and keibajo_code = ? and race_bango = ?
              and ketto_toroku_bango is not null
              and coalesce(trim(ijo_kubun_code), '0')
                  not in ('1', '2')
            """
    ]
    assert captured_params == [("30", "03")]


def test_day_base_covers_entry_list_interpolates_jra_date_and_excludes_scratch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("jra:2026:0820:05:11", "H1")])

    captured_sql: list[str] = []
    captured_params: list[tuple[object, ...]] = []

    def fake_query_source_rows(
        _url: str, sql: str, _params: tuple[object, ...]
    ) -> list[tuple[str]]:
        captured_sql.append(sql)
        captured_params.append(_params)
        return [("H1",)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "jra", "20260820", "05:11", "r2-catalog://pc-keiba"
    )

    assert result is True
    assert captured_sql == [
        """
            select distinct ketto_toroku_bango
            from pg.jvd_se
            where kaisai_nen between '2026' and '2026'
              and (kaisai_nen || kaisai_tsukihi) between '20260820' and '20260820'
              and keibajo_code = ? and race_bango = ?
              and ketto_toroku_bango is not null
              and coalesce(trim(ijo_kubun_code), '0')
                  not in ('1', '2')
            """
    ]
    assert captured_params == [("05", "11")]


def test_day_base_covers_entry_list_false_for_non_ymd_target_date(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day_base_dir = tmp_path / "daybase"
    _write_day_base_parquet(day_base_dir, [("nar:2026:0820:30:07", "H1")])
    query_calls: list[str] = []

    def fake_query_source_rows(
        _url: str, sql: str, _params: tuple[object, ...]
    ) -> list[tuple[str]]:
        query_calls.append(sql)
        return [("H1",)]

    monkeypatch.setattr(pipeline_runner, "_query_source_rows", fake_query_source_rows)

    result = pipeline_runner.day_base_covers_entry_list(
        day_base_dir, "nar", "2026-08-20", "30:07", "r2-catalog://pc-keiba"
    )

    assert result is False
    assert query_calls == []


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


def test_build_pipeline_from_day_base_records_racechain_layer_step(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.debug_log import drain_debug_progress

    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")
    drain_debug_progress()
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "race_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "record_layer_timing_row", lambda *args: None)

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
        return ["layer", str(output_dir)]

    def fake_run(args: list[str]) -> None:
        Path(args[-1]).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline_runner, "build_layer_argv", fake_layer_argv)
    monkeypatch.setattr(pipeline_runner, "run_with_stderr_capture", fake_run)

    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()
    final_dir = work_dir / "feat-jra-v7-final"

    result = pipeline_runner.build_pipeline_from_day_base(
        "jra", "20260712", 0, "postgresql://u:p@h/db", day_base_dir, final_dir, "83:03"
    )

    assert result is True
    messages = drain_debug_progress()
    assert messages[0] == (
        "step=racechain-layer index=1/1 status=start category=jra script=script-a.py "
        "target_race=83:03 elapsed_seconds=0.000"
    )
    assert "step=racechain-layer index=1/1 status=done" in messages[1]
    assert "step=daybase-base" not in messages[0]
    assert "step=daybase-base" not in messages[1]
    assert "step=daybase-base" not in messages[2]


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
            {
                "race_id": [
                    "jra:2026:0712:05:11",
                    "jra:2026:0712:05:11",
                    "jra:2026:0712:05:12",
                ],
                "umaban": [1, 2, 3],
            }
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
    assert list(result.keys()) == ["jra:2026:0712:05:11"]
    assert len(result["jra:2026:0712:05:11"]) == 2


def test_build_upcoming_feature_rows_split_falls_back_to_inline_build_day_base(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import pandas as pd

    monkeypatch.setenv("PREDICT_CONTAINER_ROLE", "unknown-role")
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
        r2_config: object | None = None,
    ) -> Path:
        called.append((category, target_date, days_ahead, database_url, r2_config))
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
    assert called == [("jra", "20260712", 0, "postgresql://u:p@h/db", None)]


def test_build_upcoming_feature_rows_split_race_chain_miss_requires_day_base_without_building(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.container_role import DayBaseRequiredError

    monkeypatch.setenv("PREDICT_CONTAINER_ROLE", "race-chain")
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path / "work")
    monkeypatch.setattr(pipeline_runner, "ensure_day_base", lambda *args, **kwargs: None)

    def unexpected_build(*args: object, **kwargs: object) -> None:
        raise AssertionError("race-chain role must not build a day base inline")

    monkeypatch.setattr(pipeline_runner, "build_day_base", unexpected_build)

    with pytest.raises(
        DayBaseRequiredError,
        match=("DAY_BASE_REQUIRED: day-base unavailable category=jra target_date=20260712"),
    ):
        pipeline_runner.build_upcoming_feature_rows_split(
            "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
        )


def test_build_upcoming_feature_rows_split_race_chain_hit_matches_legacy_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import pandas as pd

    monkeypatch.setenv("PREDICT_CONTAINER_ROLE", "race-chain")
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
        pd.DataFrame(
            {
                "race_id": ["jra:2026:0712:05:11", "jra:2026:0712:05:12"],
                "umaban": [1, 2],
            }
        ).to_parquet(final_dir / "data.parquet")
        return True

    monkeypatch.setattr(
        pipeline_runner, "build_pipeline_from_day_base", fake_build_pipeline_from_day_base
    )

    result = pipeline_runner.build_upcoming_feature_rows_split(
        "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
    )

    assert result == {"jra:2026:0712:05:11": [{"race_id": "jra:2026:0712:05:11", "umaban": 1}]}


def test_build_upcoming_feature_rows_split_race_chain_wraps_unexpected_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from predict_lib.container_role import DayBaseRequiredError

    monkeypatch.setenv("PREDICT_CONTAINER_ROLE", "race-chain")
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path / "work")

    def raiser(*args: object, **kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(pipeline_runner, "ensure_day_base", raiser)

    with pytest.raises(
        DayBaseRequiredError,
        match=(
            "DAY_BASE_REQUIRED: race-chain error category=jra target_date=20260712 "
            "target_race=05:11: RuntimeError"
        ),
    ) as error_info:
        pipeline_runner.build_upcoming_feature_rows_split(
            "jra", "20260712", 0, "postgresql://u:p@h/db", "05:11"
        )

    assert isinstance(error_info.value.__cause__, RuntimeError)


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


# ---------------------------------------------------------------------------
# _group_parquet_rows
# ---------------------------------------------------------------------------

_GROUP_PARQUET_ROWS_ATTR = "_group_parquet_rows"
_group_parquet_rows = cast(
    Callable[..., dict[str, list[dict[str, object]]]],
    getattr(pipeline_runner, _GROUP_PARQUET_ROWS_ATTR),
)


def test_group_parquet_rows_without_target_race_keeps_all() -> None:
    import pandas as pd

    frame = pd.DataFrame(
        {
            "race_id": ["jra:2026:0712:05:11", "jra:2026:0712:05:12"],
            "umaban": [1, 2],
        }
    )
    grouped = _group_parquet_rows(frame)
    assert sorted(grouped.keys()) == ["jra:2026:0712:05:11", "jra:2026:0712:05:12"]


def test_group_parquet_rows_with_target_race_keeps_only_that_race() -> None:
    import pandas as pd

    frame = pd.DataFrame(
        {
            "race_id": ["jra:2026:0712:05:11", "jra:2026:0712:05:12", "jra:2026:0712:06:01"],
            "umaban": [1, 2, 3],
        }
    )
    grouped = _group_parquet_rows(frame, target_race="05:11")
    assert list(grouped.keys()) == ["jra:2026:0712:05:11"]
    assert grouped["jra:2026:0712:05:11"] == [
        {"race_id": "jra:2026:0712:05:11", "umaban": 1},
    ]


def test_group_parquet_rows_rejects_non_dataframe() -> None:
    with pytest.raises(TypeError, match=r"pandas\.DataFrame"):
        _group_parquet_rows([{"race_id": "jra:2026:0712:05:11"}])
