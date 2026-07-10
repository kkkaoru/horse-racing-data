"""Tests for mlflow_tracking.backfill_serve_timeline."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest
from mlflow import MlflowClient
from mlflow.entities.trace_state import TraceState

from mlflow_tracking import backfill_serve_timeline, config, timeline, trace_emit


def _payload_for(date_str: str) -> dict[str, object]:
    return {
        "finish_position": {
            "date_str": date_str,
            "category": "jra",
            "era": "POST_FIX",
            "top1_pct": 44.5,
            "place2_pct": 24.0,
            "place3_pct": 15.0,
            "fukusho_2p_pct": 74.0,
            "top3_box_pct": 30.0,
            "model_version_counts": {"jra-cb-v9-sim-2013-clean": 100},
            "subgroups": [],
        },
        "running_style": {
            "date_str": date_str,
            "category": "jra",
            "era": "POST_FIX",
            "overall_accuracy_pct": 55.0,
            "macro_f1_pct": 50.0,
            "model_version": "rs-v3",
        },
    }


def _completed(
    stdout: str, *, returncode: int = 0, stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


class _FakeRunner:
    """Records every call and returns a canned CompletedProcess per date."""

    responses: dict[str, subprocess.CompletedProcess[str]]
    calls: list[tuple[tuple[str, ...], Path]]

    def __init__(self, responses: dict[str, subprocess.CompletedProcess[str]]) -> None:
        self.responses = responses
        self.calls = []

    def __call__(self, cmd: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        cmd_tuple = tuple(cmd)
        self.calls.append((cmd_tuple, cwd))
        date_index = cmd_tuple.index("--date") + 1
        date_str = cmd_tuple[date_index]
        return self.responses[date_str]


def test_backfill_serve_timeline_happy_path_ingests_every_date(
    client: MlflowClient, tmp_path: Path
) -> None:
    responses = {
        "20260601": _completed(json.dumps(_payload_for("20260601"))),
        "20260602": _completed(json.dumps(_payload_for("20260602"))),
    }
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260602", viewer_root=tmp_path, runner=runner
    )
    assert summary.dates_total == 2
    assert summary.dates_ingested == 2
    assert summary.dates_skipped_existing == 0
    assert summary.dates_skipped_no_data == 0
    assert summary.errors == []
    assert len(runner.calls) == 2
    fp_dates = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_top1_pct")
    assert fp_dates == {timeline.step_for_date("20260601"), timeline.step_for_date("20260602")}


def test_backfill_serve_timeline_builds_expected_command(
    client: MlflowClient, tmp_path: Path
) -> None:
    responses = {"20260601": _completed(json.dumps(_payload_for("20260601")))}
    runner = _FakeRunner(responses)
    backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260601", viewer_root=tmp_path, runner=runner
    )
    cmd, cwd = runner.calls[0]
    assert cmd == (
        "uv",
        "run",
        "--project",
        str(tmp_path),
        "python",
        "src/scripts/serve_accuracy_report.py",
        "--date",
        "20260601",
        "--category",
        "jra",
        "--json",
    )
    assert cwd == tmp_path


def test_backfill_serve_timeline_gets_job_traces_only_via_ingest_serve_accuracy_delegation(
    client: MlflowClient, tmp_path: Path
) -> None:
    """job_trace wiring (2026-07-11): `backfill_serve_timeline` does NOT
    open a job_trace of its own -- see this module's own docstring for why
    (a first implementation that ALSO wrapped the whole date-range loop in
    its own `timelines` job_trace double-counted against the per-date
    traces `ingest_eval.ingest_serve_accuracy` already emits via
    delegation, caught by this exact test). Each of the 2 ingested dates
    here gets its OWN primary trace (into finish-position/serve-accuracy)
    AND its own nested `timelines` trace, entirely through `ingest_eval.
    ingest_serve_accuracy`'s own wiring."""
    responses = {
        "20260601": _completed(json.dumps(_payload_for("20260601"))),
        "20260602": _completed(json.dumps(_payload_for("20260602"))),
    }
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260602", viewer_root=tmp_path, runner=runner
    )
    assert summary.dates_ingested == 2

    tracing_client = trace_emit.build_tracing_client(client)
    serve_accuracy_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_SERVE_ACCURACY)
    assert serve_accuracy_experiment is not None
    ingest_traces = tracing_client.search_traces(
        experiment_ids=[serve_accuracy_experiment.experiment_id], max_results=10
    )
    assert len(ingest_traces) == 2  # one per ingested date, via ingest_serve_accuracy

    timelines_experiment = client.get_experiment_by_name(config.EXPERIMENT_TIMELINES)
    assert timelines_experiment is not None
    timeline_traces = tracing_client.search_traces(
        experiment_ids=[timelines_experiment.experiment_id], max_results=10
    )
    assert len(timeline_traces) == 2  # one per ingested date's nested upsert-point trace
    for trace_summary in timeline_traces:
        trace = tracing_client.get_trace(trace_summary.info.trace_id)
        assert trace.info.state == TraceState.OK
        assessments_by_name = {a.name: a for a in trace.info.assessments}
        points_feedback = assessments_by_name["points_appended"].feedback
        assert points_feedback is not None
        assert points_feedback.value == 2.0  # both fp+rs sections present in _payload_for


def test_backfill_serve_timeline_no_data_only_range_creates_no_traces_at_all(
    client: MlflowClient, tmp_path: Path
) -> None:
    """A call whose only date is `no_data` never calls `ingest_serve_
    accuracy` at all -- neither the primary nor the nested `timelines`
    trace is ever created, since there is nothing to delegate to."""
    responses = {
        "20260601": _completed(
            json.dumps({"error": "no_data", "date": "20260601", "category": "jra"}),
            returncode=1,
        )
    }
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260601", viewer_root=tmp_path, runner=runner
    )
    assert summary.dates_ingested == 0
    assert client.get_experiment_by_name(config.EXPERIMENT_FP_SERVE_ACCURACY) is None
    assert client.get_experiment_by_name(config.EXPERIMENT_TIMELINES) is None


def test_backfill_serve_timeline_no_data_date_is_skipped(
    client: MlflowClient, tmp_path: Path
) -> None:
    responses = {
        "20260601": _completed(
            json.dumps({"error": "no_data", "date": "20260601", "category": "jra"}),
            returncode=1,
        )
    }
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260601", viewer_root=tmp_path, runner=runner
    )
    assert summary.dates_ingested == 0
    assert summary.dates_skipped_no_data == 1
    assert summary.errors == []


def test_backfill_serve_timeline_empty_stdout_is_an_error(
    client: MlflowClient, tmp_path: Path
) -> None:
    responses = {"20260601": _completed("", returncode=1, stderr="boom")}
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260601", viewer_root=tmp_path, runner=runner
    )
    assert summary.dates_ingested == 0
    assert len(summary.errors) == 1
    assert "20260601" in summary.errors[0]
    assert "boom" in summary.errors[0]


def test_backfill_serve_timeline_invalid_json_stdout_is_an_error(
    client: MlflowClient, tmp_path: Path
) -> None:
    responses = {"20260601": _completed("{not valid json")}
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260601", viewer_root=tmp_path, runner=runner
    )
    assert summary.dates_ingested == 0
    assert len(summary.errors) == 1
    assert "invalid JSON" in summary.errors[0]


def test_backfill_serve_timeline_bad_date_does_not_abort_the_rest(
    client: MlflowClient, tmp_path: Path
) -> None:
    responses = {
        "20260601": _completed("", returncode=1, stderr="boom"),
        "20260602": _completed(json.dumps(_payload_for("20260602"))),
    }
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260602", viewer_root=tmp_path, runner=runner
    )
    assert summary.dates_total == 2
    assert summary.dates_ingested == 1
    assert len(summary.errors) == 1


def test_backfill_serve_timeline_skip_existing_skips_seeded_date_without_calling_runner(
    client: MlflowClient, tmp_path: Path
) -> None:
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    timeline.upsert_timeline_point(
        client, "running-style", "jra", "20260601", {"rs_overall_accuracy_pct": 55.0}
    )
    responses = {"20260602": _completed(json.dumps(_payload_for("20260602")))}
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client,
        "jra",
        "20260601",
        "20260602",
        skip_existing=True,
        viewer_root=tmp_path,
        runner=runner,
    )
    assert summary.dates_skipped_existing == 1
    assert summary.dates_ingested == 1
    assert len(runner.calls) == 1
    called_date_index = runner.calls[0][0].index("--date") + 1
    assert runner.calls[0][0][called_date_index] == "20260602"


def test_backfill_serve_timeline_skip_existing_does_not_skip_when_only_one_task_has_the_date(
    client: MlflowClient, tmp_path: Path
) -> None:
    # Only finish-position has 20260601's point; running-style does not, so
    # the date must still be processed (both must have it to skip).
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    responses = {"20260601": _completed(json.dumps(_payload_for("20260601")))}
    runner = _FakeRunner(responses)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client,
        "jra",
        "20260601",
        "20260601",
        skip_existing=True,
        viewer_root=tmp_path,
        runner=runner,
    )
    assert summary.dates_skipped_existing == 0
    assert summary.dates_ingested == 1
    assert len(runner.calls) == 1


def test_backfill_serve_timeline_rejects_banei_category(
    client: MlflowClient, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="Ban-ei"):
        backfill_serve_timeline.backfill_serve_timeline(
            client, "banei", "20260601", "20260601", viewer_root=tmp_path
        )


def test_backfill_serve_timeline_rejects_unknown_category(
    client: MlflowClient, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="jra"):
        backfill_serve_timeline.backfill_serve_timeline(
            client, "keirin", "20260601", "20260601", viewer_root=tmp_path
        )


def test_backfill_serve_timeline_rejects_invalid_date_from(
    client: MlflowClient, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="8-digit"):
        backfill_serve_timeline.backfill_serve_timeline(
            client, "jra", "not-a-date", "20260601", viewer_root=tmp_path
        )


def test_backfill_serve_timeline_rejects_invalid_date_to(
    client: MlflowClient, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="8-digit"):
        backfill_serve_timeline.backfill_serve_timeline(
            client, "jra", "20260601", "not-a-date", viewer_root=tmp_path
        )


def test_backfill_serve_timeline_rejects_inverted_date_range(
    client: MlflowClient, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="must not be before"):
        backfill_serve_timeline.backfill_serve_timeline(
            client, "jra", "20260602", "20260601", viewer_root=tmp_path
        )


def test_backfill_serve_timeline_defaults_viewer_root_when_omitted(client: MlflowClient) -> None:
    responses = {"20260601": _completed(json.dumps(_payload_for("20260601")))}
    runner = _FakeRunner(responses)
    backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260601", runner=runner
    )
    _, cwd = runner.calls[0]
    assert cwd == backfill_serve_timeline.config.REPO_ROOT / "apps" / "pc-keiba-viewer"


def test_default_subprocess_runner_forces_mlflow_disabled_env(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exercises `_default_subprocess_runner` through the PUBLIC
    `backfill_serve_timeline` API (by omitting `runner=`, its default), with
    only the real `subprocess.run` monkeypatched -- never calling the
    private function directly -- so its own env-forcing body/wiring is
    covered without ever actually shelling out.
    """
    captured_env: dict[str, dict[str, str]] = {}

    def _fake_run(
        cmd: object,
        cwd: object = None,
        capture_output: bool = False,
        text: bool = False,
        check: bool = False,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        captured_env["env"] = env or {}
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr(subprocess, "run", _fake_run)
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client, "jra", "20260601", "20260601", viewer_root=tmp_path
    )
    assert captured_env["env"]["HORSE_RACING_MLFLOW_ENABLED"] == "0"
    assert len(summary.errors) == 1
