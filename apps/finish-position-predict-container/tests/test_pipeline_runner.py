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


def test_fetch_venue_weather_dir_is_importable_from_weather_fetcher():
    from weather_fetcher import fetch_venue_weather_dir

    assert callable(fetch_venue_weather_dir)
