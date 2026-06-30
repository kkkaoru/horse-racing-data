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
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeline_runner
from pipeline_runner import has_parquet_output, mask_pg_url, run_with_stderr_capture


def test_mask_pg_url_redacts_userinfo():
    masked = mask_pg_url("postgresql://user:secret@host/db")
    assert masked == "postgresql://<redacted>@host/db"


def test_mask_pg_url_redacts_neon_style_token():
    masked = mask_pg_url("postgresql://neondb_owner:npg_VERYSECRET@ep-foo.aws.neon.tech/neondb?sslmode=require")
    assert "npg_VERYSECRET" not in masked
    assert masked.startswith("postgresql://<redacted>@ep-foo.aws.neon.tech/neondb")


def test_mask_pg_url_passthrough_when_no_userinfo():
    assert mask_pg_url("python") == "python"
    assert mask_pg_url("/app/pipeline/foo.py") == "/app/pipeline/foo.py"


def test_run_succeeds_on_zero_exit():
    run_with_stderr_capture(["python", "-c", "print('ok')"])


def test_run_streams_child_stdout_to_parent_stdout(capfd: pytest.CaptureFixture[str]):
    run_with_stderr_capture([
        "python",
        "-c",
        "import sys; sys.stdout.write('hello-child-stdout\\n'); sys.stdout.flush()",
    ])
    captured = capfd.readouterr()
    assert "hello-child-stdout" in captured.out


def test_run_streams_child_stderr_to_parent_stderr_on_success(
    capfd: pytest.CaptureFixture[str],
):
    run_with_stderr_capture([
        "python",
        "-c",
        "import sys; sys.stderr.write('child-progress-log\\n'); sys.stderr.flush()",
    ])
    captured = capfd.readouterr()
    assert "child-progress-log" in captured.err


def test_run_raises_runtime_error_with_stderr_tail_on_failure():
    with pytest.raises(RuntimeError) as exc_info:
        run_with_stderr_capture([
            "python",
            "-c",
            "import sys; sys.stderr.write('boom from child\\n'); sys.exit(7)",
        ])
    message = str(exc_info.value)
    assert "exit 7" in message
    assert "boom from child" in message


def test_run_masks_pg_url_in_error_message():
    with pytest.raises(RuntimeError) as exc_info:
        run_with_stderr_capture([
            "python",
            "-c",
            "import sys; sys.exit(1)",
            "--pg-url",
            "postgresql://u:hunter2@h/db",
        ])
    message = str(exc_info.value)
    assert "hunter2" not in message
    assert "<redacted>" in message


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
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    monkeypatch.setattr(pipeline_runner, "DUCKDB_BUILDER", tmp_path / "builder.py")
    monkeypatch.setattr(pipeline_runner, "LAYER_DIR", tmp_path / "layers")
    monkeypatch.setattr(pipeline_runner, "layer_chain_for", lambda _category: ("script-a.py",))
    monkeypatch.setattr(pipeline_runner, "has_parquet_output", lambda _path: True)
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
