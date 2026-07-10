"""Tests for mlflow_tracking.migrate_timeline_v2 (the one-time v1 -> v2
timeline migration), run entirely against the isolated sqlite tmp_path store
the `client` fixture provides -- never the real Neon-backed store."""

from __future__ import annotations

from datetime import UTC, datetime

from mlflow import MlflowClient

from mlflow_tracking import config, migrate_timeline_v2, timeline
from mlflow_tracking.logging_api import get_or_create_experiment

# Real v1 points were timestamped at noon JST (== 03:00 UTC, same calendar
# date) on their own date -- never wall-clock "now" -- by the pre-migration
# `upsert_timeline_point` (see `timeline.py`'s `_JST_NOON_AS_UTC_HOUR`).
# Duplicated here (rather than reaching into that private helper from a test)
# so `_create_v1_run` below produces realistically-shaped v1 data: without
# this, `migrate_v1_run`'s "timestamp carries over unchanged" contract would
# be tested against an artifact of this helper's own creation time instead.
_JST_NOON_AS_UTC_HOUR = 3


def _timestamp_ms_for_date(date_yyyymmdd: str) -> int:
    year, month, day = int(date_yyyymmdd[:4]), int(date_yyyymmdd[4:6]), int(date_yyyymmdd[6:8])
    moment = datetime(year, month, day, _JST_NOON_AS_UTC_HOUR, 0, 0, tzinfo=UTC)
    return int(moment.timestamp() * 1000)


def _create_v1_run(
    client: MlflowClient,
    experiment_id: str,
    task: str,
    category: str,
    *,
    points: dict[str, list[tuple[str, float]]] | None = None,
    extra_tags: dict[str, str] | None = None,
) -> str:
    """Create a run shaped exactly like the pre-migration `upsert_timeline_point`
    would have: tagged with the legacy `timeline.TIMELINE_KEY_TAG`, and
    (optionally) metric points logged the old way (step = int(date_yyyymmdd)).

    `points` maps metric_key -> list of (date_yyyymmdd, value) pairs.
    """
    tags = {
        timeline.TIMELINE_KEY_TAG: f"{task}:{category}",
        "task": task,
        "category": category,
        "eval_regime": "serve",
        **(extra_tags or {}),
    }
    run = client.create_run(experiment_id, run_name=f"timeline-{task}-{category}", tags=tags)
    run_id = run.info.run_id
    for metric_key, date_value_pairs in (points or {}).items():
        for date_yyyymmdd, value in date_value_pairs:
            client.log_metric(
                run_id,
                metric_key,
                value,
                timestamp=_timestamp_ms_for_date(date_yyyymmdd),
                step=int(date_yyyymmdd),
            )
    return run_id


def test_find_v1_timeline_runs_finds_real_run(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    v1_run_id = _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "jra",
        points={"fp_top1_pct": [("20260601", 44.5)]},
    )
    found = migrate_timeline_v2.find_v1_timeline_runs(client, experiment_id)
    assert [run.info.run_id for run in found] == [v1_run_id]


def test_find_v1_timeline_runs_excludes_already_deprecated_run(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "jra",
        points={"fp_top1_pct": [("20260601", 44.5)]},
        extra_tags={migrate_timeline_v2.DEPRECATED_TAG: migrate_timeline_v2.TRUE_STR},
    )
    found = migrate_timeline_v2.find_v1_timeline_runs(client, experiment_id)
    assert found == []


def test_find_v1_timeline_runs_excludes_junk_and_diagnostic_runs(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "jra",
        points={"fp_top1_pct": [("20260601", 44.5)]},
        extra_tags={migrate_timeline_v2.JUNK_TAG: migrate_timeline_v2.TRUE_STR},
    )
    _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "nar",
        points={"fp_top1_pct": [("20260601", 30.0)]},
        extra_tags={migrate_timeline_v2.DIAGNOSTIC_TAG: migrate_timeline_v2.TRUE_STR},
    )
    found = migrate_timeline_v2.find_v1_timeline_runs(client, experiment_id)
    assert found == []


def test_find_v1_timeline_runs_excludes_v2_runs(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    # A run created by the current (v2) upsert_timeline_point carries only
    # TIMELINE_KEY_TAG_V2, never the legacy TIMELINE_KEY_TAG.
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    found = migrate_timeline_v2.find_v1_timeline_runs(client, experiment_id)
    assert found == []


def test_migrate_v1_run_copies_every_point_with_recomputed_step(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    old_run_id = _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "jra",
        points={
            "fp_top1_pct": [("20260601", 44.5), ("20260602", 45.0)],
            "fp_place2_pct": [("20260601", 24.0)],
        },
    )
    old_run = client.get_run(old_run_id)

    result = migrate_timeline_v2.migrate_v1_run(client, old_run)

    assert result.old_run_id == old_run_id
    assert result.task == "finish-position"
    assert result.category == "jra"
    assert result.points_migrated == 3
    assert result.new_run_id != old_run_id

    new_run = client.get_run(result.new_run_id)
    assert new_run.data.tags["timeline_key_v2"] == "finish-position:jra"
    assert new_run.info.run_name == "timeline-finish-position-jra-v2"

    top1_history = {
        point.step: point.value
        for point in client.get_metric_history(result.new_run_id, "fp_top1_pct")
    }
    assert top1_history == {
        timeline.step_for_date("20260601"): 44.5,
        timeline.step_for_date("20260602"): 45.0,
    }
    place2_history = client.get_metric_history(result.new_run_id, "fp_place2_pct")
    assert len(place2_history) == 1
    assert place2_history[0].step == timeline.step_for_date("20260601")
    assert place2_history[0].value == 24.0

    old_run_after = client.get_run(old_run_id)
    assert old_run_after.data.tags[migrate_timeline_v2.DEPRECATED_TAG] == "true"
    assert old_run_after.data.tags[migrate_timeline_v2.SUPERSEDED_BY_TAG] == result.new_run_id


def test_migrate_v1_run_preserves_original_timestamp(client: MlflowClient) -> None:
    """The migrated point's timestamp_ms must equal the original point's --
    only the step numbering changes (see migrate_v1_run's own docstring)."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    old_run_id = _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "jra",
        points={"fp_top1_pct": [("20260601", 44.5)]},
    )
    old_history = client.get_metric_history(old_run_id, "fp_top1_pct")
    old_timestamp = old_history[0].timestamp

    old_run = client.get_run(old_run_id)
    result = migrate_timeline_v2.migrate_v1_run(client, old_run)

    new_history = client.get_metric_history(result.new_run_id, "fp_top1_pct")
    assert new_history[0].timestamp == old_timestamp


def test_migrate_v1_run_raises_when_task_or_category_tag_missing(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    run = client.create_run(
        experiment_id, tags={timeline.TIMELINE_KEY_TAG: "finish-position:jra"}
    )
    client.log_metric(run.info.run_id, "fp_top1_pct", 44.5, step=20260601)
    stale_run = client.get_run(run.info.run_id)
    try:
        migrate_timeline_v2.migrate_v1_run(client, stale_run)
    except ValueError as exc:
        assert "task/category" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing task/category tags")


def test_migrate_v1_run_raises_when_no_metric_points(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    run = client.create_run(
        experiment_id,
        tags={
            timeline.TIMELINE_KEY_TAG: "finish-position:jra",
            "task": "finish-position",
            "category": "jra",
        },
    )
    try:
        migrate_timeline_v2.migrate_v1_run(client, run)
    except ValueError as exc:
        assert "no metric points" in str(exc)
    else:
        raise AssertionError("expected ValueError for a run with no metric points")


def test_migrate_all_v1_timeline_runs_empty_when_experiment_does_not_exist(
    client: MlflowClient,
) -> None:
    summary = migrate_timeline_v2.migrate_all_v1_timeline_runs(client)
    assert summary.results == []


def test_migrate_all_v1_timeline_runs_migrates_every_real_v1_run(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "jra",
        points={"fp_top1_pct": [("20260601", 44.5)]},
    )
    _create_v1_run(
        client,
        experiment_id,
        "running-style",
        "nar",
        points={"rs_overall_accuracy_pct": [("20260602", 55.0)]},
    )
    # A diagnostic run must be left alone (not migrated, not deprecated).
    diagnostic_run_id = _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "banei",
        points={"fp_top1_pct": [("20260603", 20.0)]},
        extra_tags={migrate_timeline_v2.DIAGNOSTIC_TAG: migrate_timeline_v2.TRUE_STR},
    )

    summary = migrate_timeline_v2.migrate_all_v1_timeline_runs(client)

    assert len(summary.results) == 2
    migrated_pairs = {(result.task, result.category) for result in summary.results}
    assert migrated_pairs == {("finish-position", "jra"), ("running-style", "nar")}

    diagnostic_run = client.get_run(diagnostic_run_id)
    assert migrate_timeline_v2.DEPRECATED_TAG not in diagnostic_run.data.tags

    fp_dates = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_top1_pct")
    assert fp_dates == {timeline.step_for_date("20260601")}


def test_migrate_all_v1_timeline_runs_is_safe_to_call_twice(client: MlflowClient) -> None:
    """A second call must not re-migrate an already-deprecated run (it is
    excluded by find_v1_timeline_runs), so the metric history on the v2 run
    is not duplicated."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    _create_v1_run(
        client,
        experiment_id,
        "finish-position",
        "jra",
        points={"fp_top1_pct": [("20260601", 44.5)]},
    )
    first = migrate_timeline_v2.migrate_all_v1_timeline_runs(client)
    second = migrate_timeline_v2.migrate_all_v1_timeline_runs(client)
    assert len(first.results) == 1
    assert len(second.results) == 0
    new_run_id = first.results[0].new_run_id
    history = client.get_metric_history(new_run_id, "fp_top1_pct")
    assert len(history) == 1
