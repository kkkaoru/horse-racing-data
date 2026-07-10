"""Tests for mlflow_tracking.timeline."""

from __future__ import annotations

import datetime as dt

import pytest
from mlflow import MlflowClient

from mlflow_tracking import config, timeline
from mlflow_tracking.logging_api import get_or_create_experiment


def test_upsert_timeline_point_creates_run_on_first_call(client: MlflowClient) -> None:
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    run = client.get_run(run_id)
    assert run.info.run_name == "timeline-finish-position-jra-v2"
    assert run.data.tags["timeline_key_v2"] == "finish-position:jra"
    assert "timeline_key" not in run.data.tags
    assert run.data.tags["task"] == "finish-position"
    assert run.data.tags["category"] == "jra"
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == config.EXPERIMENT_TIMELINES


def test_upsert_timeline_point_tags_creation_with_eval_regime_serve(
    client: MlflowClient,
) -> None:
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    run = client.get_run(run_id)
    assert run.data.tags["eval_regime"] == "serve"


def test_upsert_timeline_point_eval_regime_tag_survives_second_call(
    client: MlflowClient,
) -> None:
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260602", {"fp_top1_pct": 45.0}
    )
    run = client.get_run(run_id)
    assert run.data.tags["eval_regime"] == "serve"


def test_upsert_timeline_point_end_time_is_not_the_historical_metric_date(
    client: MlflowClient,
) -> None:
    """Regression test for a bug where `set_terminated` was passed the
    metric's own historical `timestamp_ms` as `end_time`, making a
    backfill for a long-past `date_yyyymmdd` produce `end_time <
    start_time` (a negative Duration in the UI). `end_time` must instead
    default to this call's actual wall-clock time, which can never be
    earlier than `start_time` (also wall-clock, set by `create_run`).
    """
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20200101", {"fp_top1_pct": 44.5}
    )
    run = client.get_run(run_id)
    historical_end_time = int(dt.datetime(2020, 1, 1, 3, 0, 0, tzinfo=dt.UTC).timestamp() * 1000)
    assert run.info.end_time != historical_end_time
    assert run.info.end_time >= run.info.start_time


def test_upsert_timeline_point_reuses_same_run_across_dates(client: MlflowClient) -> None:
    run_id_1 = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    run_id_2 = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260602", {"fp_top1_pct": 45.0}
    )
    assert run_id_1 == run_id_2
    history = client.get_metric_history(run_id_1, "fp_top1_pct")
    steps = {point.step: point.value for point in history}
    assert steps == {
        timeline.step_for_date("20260601"): 44.5,
        timeline.step_for_date("20260602"): 45.0,
    }


def test_upsert_timeline_point_metric_timestamp_is_noon_jst_as_utc(client: MlflowClient) -> None:
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    history = client.get_metric_history(run_id, "fp_top1_pct")
    assert len(history) == 1
    expected_ms = int(dt.datetime(2026, 6, 1, 3, 0, 0, tzinfo=dt.UTC).timestamp() * 1000)
    assert history[0].timestamp == expected_ms


def test_upsert_timeline_point_does_not_duplicate_same_step(client: MlflowClient) -> None:
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 99.9}
    )
    history = client.get_metric_history(run_id, "fp_top1_pct")
    assert len(history) == 1
    assert history[0].value == 44.5


def test_upsert_timeline_point_new_metric_key_on_existing_step_still_logs(
    client: MlflowClient,
) -> None:
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_place2_pct": 24.0}
    )
    place2_history = client.get_metric_history(run_id, "fp_place2_pct")
    assert len(place2_history) == 1
    assert place2_history[0].step == timeline.step_for_date("20260601")
    top1_history = client.get_metric_history(run_id, "fp_top1_pct")
    assert len(top1_history) == 1


def test_upsert_timeline_point_applies_tags(client: MlflowClient) -> None:
    run_id = timeline.upsert_timeline_point(
        client,
        "finish-position",
        "jra",
        "20260601",
        {"fp_top1_pct": 44.5},
        tags={"note": "backfilled"},
    )
    run = client.get_run(run_id)
    assert run.data.tags["note"] == "backfilled"


def test_upsert_timeline_point_updates_tags_on_existing_run(client: MlflowClient) -> None:
    run_id = timeline.upsert_timeline_point(
        client,
        "finish-position",
        "jra",
        "20260601",
        {"fp_top1_pct": 44.5},
        tags={"note": "first"},
    )
    timeline.upsert_timeline_point(
        client,
        "finish-position",
        "jra",
        "20260602",
        {"fp_top1_pct": 45.0},
        tags={"note": "second"},
    )
    run = client.get_run(run_id)
    assert run.data.tags["note"] == "second"


def test_upsert_timeline_point_run_finished_after_each_call(client: MlflowClient) -> None:
    run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    assert client.get_run(run_id).info.status == "FINISHED"
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260602", {"fp_top1_pct": 45.0}
    )
    assert client.get_run(run_id).info.status == "FINISHED"


def test_upsert_timeline_point_different_categories_get_different_runs(
    client: MlflowClient,
) -> None:
    jra_run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    nar_run_id = timeline.upsert_timeline_point(
        client, "finish-position", "nar", "20260601", {"fp_top1_pct": 30.0}
    )
    assert jra_run_id != nar_run_id


def test_upsert_timeline_point_rejects_invalid_date(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="8-digit"):
        timeline.upsert_timeline_point(client, "finish-position", "jra", "not-a-date", {})


def test_validate_yyyymmdd_accepts_valid_date() -> None:
    timeline.validate_yyyymmdd("20260614")


def test_validate_yyyymmdd_rejects_wrong_length() -> None:
    with pytest.raises(ValueError, match="8-digit"):
        timeline.validate_yyyymmdd("2026061")


def test_validate_yyyymmdd_rejects_non_numeric() -> None:
    with pytest.raises(ValueError, match="8-digit"):
        timeline.validate_yyyymmdd("2026abcd")


def test_validate_yyyymmdd_rejects_invalid_calendar_date() -> None:
    with pytest.raises(ValueError, match="not a real calendar date"):
        timeline.validate_yyyymmdd("20260231")


def test_fp_metrics_for_timeline_extracts_known_keys() -> None:
    result = timeline.fp_metrics_for_timeline(
        {
            "top1_pct": 44.5,
            "place2_pct": 24.0,
            "place3_pct": 15.0,
            "place4_pct": 10.0,
            "place5_pct": 8.0,
            "place6_pct": 6.0,
            "fukusho_2p_pct": 74.0,
            "top3_box_pct": 30.0,
            "races": 12,
            "horses": 144,
        }
    )
    assert result == {
        "fp_top1_pct": 44.5,
        "fp_place2_pct": 24.0,
        "fp_place3_pct": 15.0,
        "fp_place4_pct": 10.0,
        "fp_place5_pct": 8.0,
        "fp_place6_pct": 6.0,
        "fp_fukusho_2p_pct": 74.0,
        "fp_top3_box_pct": 30.0,
        "fp_races": 12.0,
    }


def test_fp_metrics_for_timeline_skips_non_numeric_values() -> None:
    result = timeline.fp_metrics_for_timeline({"top1_pct": "not-a-number"})
    assert result == {}


def test_fp_metrics_for_timeline_skips_none_place456_pct() -> None:
    """place4_pct/place5_pct/place6_pct may be None (a day/cell where every
    race had too small a field for that rank, see
    serve_eval.aggregate_fp_day_metrics's own docstring) -- these must be
    silently skipped, never logged as a fabricated 0.0, while other present
    keys still come through."""
    result = timeline.fp_metrics_for_timeline(
        {
            "top1_pct": 44.5,
            "place4_pct": None,
            "place5_pct": None,
            "place6_pct": None,
        }
    )
    assert result == {"fp_top1_pct": 44.5}


def test_fp_metrics_for_timeline_empty_dict_yields_empty_result() -> None:
    assert timeline.fp_metrics_for_timeline({}) == {}


def test_fp_metrics_for_timeline_extracts_races_scheduled_and_coverage_pct() -> None:
    """Regression coverage for the 2026-07-11 coverage-ratio addition:
    sync_production._sync_fp_eval merges races_scheduled/coverage_pct into
    the SAME day_metrics dict this function reads, so they must compose
    through this exact map/isinstance-guarded path like every other key."""
    result = timeline.fp_metrics_for_timeline(
        {"top1_pct": 44.5, "races_scheduled": 485.0, "coverage_pct": 2.27}
    )
    assert result == {
        "fp_top1_pct": 44.5,
        "fp_races_scheduled": 485.0,
        "fp_coverage_pct": 2.27,
    }


def test_fp_metrics_for_timeline_skips_none_coverage_pct() -> None:
    """coverage_pct is None when zero races were scheduled that day (see
    sync_production._coverage_pct) -- never a fabricated 0.0."""
    result = timeline.fp_metrics_for_timeline(
        {"top1_pct": 44.5, "races_scheduled": 0.0, "coverage_pct": None}
    )
    assert result == {"fp_top1_pct": 44.5, "fp_races_scheduled": 0.0}


def test_rs_metrics_for_timeline_extracts_overall_and_macro_f1() -> None:
    result = timeline.rs_metrics_for_timeline({"overall_accuracy_pct": 55.0, "macro_f1_pct": 50.0})
    assert result == {"rs_overall_accuracy_pct": 55.0, "rs_macro_f1_pct": 50.0}


def test_rs_metrics_for_timeline_skips_none_macro_f1() -> None:
    result = timeline.rs_metrics_for_timeline({"overall_accuracy_pct": 55.0, "macro_f1_pct": None})
    assert result == {"rs_overall_accuracy_pct": 55.0}


def test_rs_metrics_for_timeline_skips_non_numeric_overall_accuracy() -> None:
    result = timeline.rs_metrics_for_timeline({"overall_accuracy_pct": "not-a-number"})
    assert result == {}


def test_rs_metrics_for_timeline_extracts_per_class_f1() -> None:
    result = timeline.rs_metrics_for_timeline(
        {
            "per_class": [
                {"label": "nige", "f1_pct": 40.0},
                {"label": "oikomi", "f1_pct": None},
                {"label": "senkou"},
                "not-a-dict",
                {"f1_pct": 30.0},
            ]
        }
    )
    assert result == {"rs_f1_nige_pct": 40.0}


def test_rs_metrics_for_timeline_ignores_non_list_per_class() -> None:
    result = timeline.rs_metrics_for_timeline({"per_class": "not-a-list"})
    assert result == {}


def test_rs_metrics_for_timeline_empty_dict_yields_empty_result() -> None:
    assert timeline.rs_metrics_for_timeline({}) == {}


def test_timeline_dates_present_empty_when_experiment_does_not_exist(
    client: MlflowClient,
) -> None:
    assert timeline.timeline_dates_present(client, "finish-position", "jra", "fp_top1_pct") == set()


def test_timeline_dates_present_empty_when_run_does_not_exist_but_experiment_does(
    client: MlflowClient,
) -> None:
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    result = timeline.timeline_dates_present(
        client, "running-style", "jra", "rs_overall_accuracy_pct"
    )
    assert result == set()


def test_timeline_dates_present_returns_logged_steps(client: MlflowClient) -> None:
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260602", {"fp_top1_pct": 45.0}
    )
    result = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_top1_pct")
    assert result == {timeline.step_for_date("20260601"), timeline.step_for_date("20260602")}


def test_timeline_dates_present_empty_for_unknown_metric_key(client: MlflowClient) -> None:
    timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )
    result = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_unknown_key")
    assert result == set()


def test_step_for_date_epoch_is_step_zero() -> None:
    assert timeline.step_for_date("20200101") == 0


def test_step_for_date_day_after_epoch_is_step_one() -> None:
    assert timeline.step_for_date("20200102") == 1


def test_step_for_date_matches_known_spot_checked_values() -> None:
    # Spot-checked independently via `(date(y, m, d) - date(2020, 1, 1)).days`.
    assert timeline.step_for_date("20260601") == 2343
    assert timeline.step_for_date("20261231") == 2556


def test_step_for_date_recovers_original_date_via_documented_formula() -> None:
    """The docstring's own recovery formula (TIMELINE_STEP_EPOCH + N days)
    must round-trip back to the original date for an arbitrary date."""
    date_str = "20260516"
    step = timeline.step_for_date(date_str)
    recovered = timeline.TIMELINE_STEP_EPOCH + dt.timedelta(days=step)
    assert recovered == dt.date(2026, 5, 16)


def test_upsert_timeline_point_never_matches_or_extends_a_legacy_v1_run(
    client: MlflowClient,
) -> None:
    """A pre-migration v1 run (tagged with the legacy `TIMELINE_KEY_TAG`, not
    `TIMELINE_KEY_TAG_V2`) must never be found or appended to by
    `upsert_timeline_point` -- it always creates/uses a distinct v2 run
    instead. Regression test for the v1/v2 tag-KEY split (not just a
    different tag value)."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    v1_run = client.create_run(
        experiment_id,
        run_name="timeline-finish-position-jra",
        tags={
            timeline.TIMELINE_KEY_TAG: "finish-position:jra",
            "task": "finish-position",
            "category": "jra",
            "eval_regime": "serve",
        },
    )
    client.log_metric(v1_run.info.run_id, "fp_top1_pct", 10.0, step=20260601)

    v2_run_id = timeline.upsert_timeline_point(
        client, "finish-position", "jra", "20260601", {"fp_top1_pct": 44.5}
    )

    assert v2_run_id != v1_run.info.run_id
    v2_run = client.get_run(v2_run_id)
    assert v2_run.data.tags["timeline_key_v2"] == "finish-position:jra"
    assert v2_run.info.run_name == "timeline-finish-position-jra-v2"
    # The legacy v1 run's own history is completely untouched.
    v1_history = client.get_metric_history(v1_run.info.run_id, "fp_top1_pct")
    assert len(v1_history) == 1
    assert v1_history[0].step == 20260601
    assert v1_history[0].value == 10.0

