"""Hermetic tests for forward dynamic-market shadow evaluation."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from mlflow import MlflowClient
from psycopg2 import OperationalError

from mlflow_tracking import config
from mlflow_tracking import dynamic_market_shadow_eval as shadow_eval

DATE = "20260824"
ROUTER = shadow_eval.SHADOW_ROUTER_VERSION


class FakeCursor:
    connection: FakeConnection

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.pending: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> None:
        self.connection.queries.append((query, params))
        if self.connection.error is not None:
            raise self.connection.error
        self.pending = self.connection.rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.pending


class FakeConnection:
    rows: list[tuple[object, ...]]
    error: OperationalError | None
    queries: list[tuple[str, object]]
    closed: bool

    def __init__(
        self,
        rows: list[tuple[object, ...]] | None = None,
        error: OperationalError | None = None,
    ) -> None:
        self.rows = rows or []
        self.error = error
        self.queries = []
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


def _prediction(
    race: str, baseline: str, shadow: str, *, active: bool = True
) -> shadow_eval.ShadowPrediction:
    return shadow_eval.ShadowPrediction(
        race_id=f"jra:2026:0824:06:{race}",
        router_version=ROUTER,
        baseline_top1=baseline,
        shadow_top1=shadow,
        upset_probability=0.6,
        market_weight=0.98,
        active=active,
    )


def _result(finish: int | None, popularity: int | None) -> shadow_eval.FinalizedHorse:
    return shadow_eval.FinalizedHorse(finish=finish, market_rank=popularity)


def test_fetch_shadow_predictions_validates_top5_arrays() -> None:
    generated_at = datetime(2026, 8, 24, tzinfo=UTC)
    connection = FakeConnection(
        [
            ("jra:2026:0824:06:01", ROUTER, ["a"], ["b"], 0.6, 0.98, True, generated_at),
            ("bad", ROUTER, "a", ["b"], 0.4, 1.0, False, generated_at),
            ("bad", ROUTER, ["a"], "b", 0.4, 1.0, False, generated_at),
            ("bad", ROUTER, [], ["b"], 0.4, 1.0, False, generated_at),
        ]
    )

    predictions = shadow_eval.fetch_shadow_predictions(connection, DATE)

    assert len(predictions) == 1
    assert predictions[0].baseline_top1 == "a"
    assert connection.queries[0][1] == (ROUTER, DATE)


def test_fetch_finalized_results_parses_positive_values_only() -> None:
    connection = FakeConnection(
        [
            ("06", "01", "a", "1", "03"),
            ("06", "01", "b", "中止", "00"),
            ("06", "01", "c", 0, None),
        ]
    )

    results = shadow_eval.fetch_finalized_results(connection, DATE)

    assert results[("06", "01", "a")] == _result(1, 3)
    assert results[("06", "01", "b")] == _result(None, None)
    assert results[("06", "01", "c")] == _result(None, None)
    assert connection.queries[0][1] == ("2026", "0824")


def test_evaluate_day_reports_top1_through_top5_and_tied_winner_rule() -> None:
    predictions = [
        _prediction("01", "a", "b"),
        _prediction("02", "c", "d", active=False),
    ]
    results = {
        ("06", "01", "a"): _result(4, 5),
        ("06", "01", "b"): _result(2, 2),
        ("06", "01", "winner1"): _result(1, 1),
        ("06", "02", "c"): _result(1, 1),
        ("06", "02", "d"): _result(5, 5),
        ("06", "02", "winner2a"): _result(1, 4),
        ("06", "02", "winner2b"): _result(1, 2),
    }

    evaluation = shadow_eval.evaluate_day(DATE, predictions, results)

    assert evaluation.evaluated_races == 2
    assert evaluation.metrics["all_baseline_top1_pct"] == 50.0
    assert evaluation.metrics["all_shadow_top5_pct"] == 100.0
    assert evaluation.metrics["all_delta_top3_pt"] == 0.0
    assert evaluation.metrics["favorite_driven_races"] == 2.0
    assert "upset_races" not in evaluation.metrics
    assert evaluation.metrics["active_races"] == 1.0
    assert all(f"all_delta_top{cutoff}_pt" in evaluation.metrics for cutoff in range(1, 6))


def test_evaluate_day_counts_missing_and_upset_segments() -> None:
    malformed = shadow_eval.ShadowPrediction(
        race_id="malformed",
        router_version=ROUTER,
        baseline_top1="g",
        shadow_top1="h",
        upset_probability=0.5,
        market_weight=1.0,
        active=False,
    )
    predictions = [
        _prediction("01", "a", "b"),
        _prediction("02", "missing", "d"),
        _prediction("03", "e", "f"),
        malformed,
    ]
    results = {
        ("06", "01", "a"): _result(6, 1),
        ("06", "01", "b"): _result(1, 4),
        ("06", "01", "winner"): _result(1, 5),
        ("06", "02", "d"): _result(1, 1),
        ("06", "02", "winner"): _result(1, 1),
        ("06", "03", "e"): _result(2, 1),
        ("06", "03", "f"): _result(3, 2),
        ("06", "03", "winner"): _result(1, None),
    }

    evaluation = shadow_eval.evaluate_day(DATE, predictions, results)

    assert evaluation.evaluated_races == 1
    assert evaluation.missing_final_result == 2
    assert evaluation.missing_final_popularity == 1
    assert evaluation.metrics["upset_races"] == 1.0
    assert "favorite_driven_races" not in evaluation.metrics


def test_log_day_evaluation_reuses_run(client: MlflowClient) -> None:
    evaluation = shadow_eval.evaluate_day(
        DATE,
        [_prediction("01", "a", "b")],
        {
            ("06", "01", "a"): _result(2, 2),
            ("06", "01", "b"): _result(1, 1),
            ("06", "01", "winner"): _result(1, 1),
        },
    )

    first = shadow_eval.log_day_evaluation(client, evaluation)
    second = shadow_eval.log_day_evaluation(client, evaluation)

    assert second == first
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_DYNAMIC_MARKET_SHADOW)
    assert experiment is not None
    assert len(client.search_runs([experiment.experiment_id])) == 1
    run = client.get_run(first)
    assert run.data.metrics["all_shadow_top1_pct"] == 100.0
    assert run.data.tags["evaluation_complete"] == "true"
    assert client.download_artifacts(first, shadow_eval.METRICS_ARTIFACT)


def _day_with_outcomes(
    date_str: str, outcomes: tuple[shadow_eval.ShadowOutcome, ...]
) -> shadow_eval.DayEvaluation:
    return shadow_eval.DayEvaluation(
        date=date_str,
        router_version=ROUTER,
        shadow_rows=len(outcomes),
        evaluated_races=len(outcomes),
        missing_final_result=0,
        missing_final_popularity=0,
        metrics={},
        outcomes=outcomes,
    )


def test_cumulative_evaluation_bootstraps_dates_and_passes_strict_gate() -> None:
    days: list[shadow_eval.DayEvaluation] = []
    for day_number in range(1, 9):
        date_str = f"202609{day_number:02d}"
        days.append(
            _day_with_outcomes(
                date_str,
                (
                    shadow_eval.ShadowOutcome(date_str, 5, 1, 1),
                    shadow_eval.ShadowOutcome(date_str, 5, 1, 4),
                ),
            )
        )

    evaluation = shadow_eval.evaluate_cumulative(
        "20260901", "20260908", days, bootstrap_samples=30, bootstrap_seed=7
    )

    assert evaluation.evaluated_races == 16
    assert evaluation.cluster_dates == 8
    assert evaluation.metrics["all_delta_top1_3_mean_pt"] == 100.0
    assert evaluation.metrics["favorite_driven_delta_top1_ci95_low_pt"] == 100.0
    assert evaluation.metrics["upset_delta_top3_ci95_high_pt"] == 100.0
    assert evaluation.metrics["promotion_point_gate"] == 1.0
    assert evaluation.metrics["promotion_confidence_gate"] == 1.0
    assert evaluation.metrics["promotion_ready"] == 1.0


def test_cumulative_evaluation_with_one_date_omits_invalid_interval() -> None:
    day = _day_with_outcomes(
        DATE,
        (shadow_eval.ShadowOutcome(DATE, 2, 1, 1),),
    )

    evaluation = shadow_eval.evaluate_cumulative(DATE, DATE, [day], bootstrap_samples=0)

    assert evaluation.metrics["cluster_dates"] == 1.0
    assert "all_delta_top1_ci95_low_pt" not in evaluation.metrics
    assert evaluation.metrics["promotion_point_gate"] == 0.0
    assert evaluation.metrics["promotion_confidence_gate"] == 0.0
    assert evaluation.metrics["promotion_ready"] == 0.0


def test_cluster_bootstrap_omits_segment_interval_when_resamples_can_be_empty() -> None:
    favorite_date = "20260901"
    upset_date = "20260902"
    days = [
        _day_with_outcomes(
            favorite_date,
            (shadow_eval.ShadowOutcome(favorite_date, 2, 1, 1),),
        ),
        _day_with_outcomes(
            upset_date,
            (shadow_eval.ShadowOutcome(upset_date, 2, 1, 4),),
        ),
    ]

    evaluation = shadow_eval.evaluate_cumulative(
        favorite_date,
        upset_date,
        days,
        bootstrap_samples=20,
        bootstrap_seed=2,
    )

    assert "all_delta_top1_ci95_low_pt" in evaluation.metrics
    assert "favorite_driven_delta_top1_ci95_low_pt" not in evaluation.metrics
    assert "upset_delta_top1_ci95_low_pt" not in evaluation.metrics


def test_log_cumulative_evaluation_reuses_run(client: MlflowClient) -> None:
    evaluation = shadow_eval.evaluate_cumulative(DATE, DATE, [])

    first = shadow_eval.log_cumulative_evaluation(client, evaluation)
    second = shadow_eval.log_cumulative_evaluation(client, evaluation)

    assert second == first
    run = client.get_run(first)
    assert run.data.tags["promotion_ready"] == "false"
    assert run.data.metrics["evaluated_races"] == 0.0


def test_evaluate_range_isolates_error_and_closes(client: MlflowClient) -> None:
    neon = FakeConnection(error=OperationalError("temporary"))
    local = FakeConnection()

    summary = shadow_eval.evaluate_range(
        client,
        DATE,
        DATE,
        neon_factory=lambda: neon,
        local_factory=lambda: local,
    )

    assert summary.dates_processed == 1
    assert summary.dates_logged == 0
    assert summary.errors == [f"{DATE}: temporary"]
    assert neon.closed is True
    assert local.closed is True


def test_evaluate_range_logs_results(client: MlflowClient) -> None:
    shadow_row = (
        "jra:2026:0824:06:01",
        ROUTER,
        ["a"],
        ["b"],
        0.6,
        0.98,
        True,
        datetime(2026, 8, 24, tzinfo=UTC),
    )
    neon = FakeConnection([shadow_row])
    local = FakeConnection([("06", "01", "a", 2, 2), ("06", "01", "b", 1, 1)])

    summary = shadow_eval.evaluate_range(
        client,
        DATE,
        DATE,
        neon_factory=lambda: neon,
        local_factory=lambda: local,
    )

    assert summary.dates_logged == 1
    assert summary.dates_without_shadow == 0
    assert summary.cumulative_logged is True
    assert summary.errors == []


def test_evaluate_range_isolates_cumulative_logging_failure(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    shadow_row = (
        "jra:2026:0824:06:01",
        ROUTER,
        ["a"],
        ["b"],
        0.6,
        0.98,
        True,
        datetime(2026, 8, 24, tzinfo=UTC),
    )
    neon = FakeConnection([shadow_row])
    local = FakeConnection([("06", "01", "a", 2, 2), ("06", "01", "b", 1, 1)])
    monkeypatch.setattr(
        shadow_eval,
        "log_cumulative_evaluation",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("tracking unavailable")),
    )

    summary = shadow_eval.evaluate_range(
        client,
        DATE,
        DATE,
        neon_factory=lambda: neon,
        local_factory=lambda: local,
    )

    assert summary.dates_logged == 1
    assert summary.cumulative_logged is False
    assert summary.errors == [f"cumulative:{DATE}:{DATE}: tracking unavailable"]
    assert neon.closed is True
    assert local.closed is True


def test_evaluate_range_skips_no_shadow_and_rejects_inverted_range(
    client: MlflowClient,
) -> None:
    empty_neon = FakeConnection()
    empty_local = FakeConnection()
    summary = shadow_eval.evaluate_range(
        client,
        DATE,
        DATE,
        neon_factory=lambda: empty_neon,
        local_factory=lambda: empty_local,
    )
    assert summary.dates_without_shadow == 1

    neon = FakeConnection()
    local = FakeConnection()
    with pytest.raises(ValueError, match="date_from"):
        shadow_eval.evaluate_range(
            client,
            "20260825",
            DATE,
            neon_factory=lambda: neon,
            local_factory=lambda: local,
        )
    assert neon.closed is True
    assert local.closed is True


def test_main_prints_summary_and_returns_error_status(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        shadow_eval,
        "evaluate_range",
        lambda *_args, **_kwargs: shadow_eval.RangeSummary(dates_processed=1, errors=["boom"]),
    )
    monkeypatch.setattr(config, "get_tracking_uri", lambda: "sqlite:///:memory:")

    status = shadow_eval.main(["--date-from", DATE, "--date-to", DATE])

    captured = capsys.readouterr()
    assert status == 1
    assert "dates processed: 1" in captured.out
    assert "cumulative logged: false" in captured.out
    assert "error: boom" in captured.err
