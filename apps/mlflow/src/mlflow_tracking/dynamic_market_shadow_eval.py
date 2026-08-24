"""Forward evaluation for the JRA dynamic-market shadow router.

The serving container writes one race-level shadow row to Neon without
changing the ranking returned to users.  This module joins those immutable
predictions to finalized JVD results and records cumulative Top1 through Top5
for the baseline and shadow Top1 picks.  Metrics are always split into all,
favorite-driven, and upset races; a segment with no observations is omitted
rather than logged as a fabricated zero.
"""

from __future__ import annotations

import argparse
import random
import re
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Final, cast

from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag
from psycopg2 import Error as PsycopgError

from mlflow_tracking import config, db
from mlflow_tracking.logging_api import (
    get_or_create_experiment,
    log_batch_chunked,
    log_json_artifact,
)

SHADOW_ROUTER_VERSION: Final[str] = "jra-dynamic-market-shadow-loop43-2026"
SHADOW_TABLE: Final[str] = "finish_position_dynamic_market_shadow_predictions"
SHADOW_EVAL_KEY_TAG: Final[str] = "dynamic_market_shadow_eval_key"
UPSET_MIN_WINNER_MARKET_RANK: Final[int] = 4
METRICS_ARTIFACT: Final[str] = "dynamic_market_shadow_metrics.json"
BOOTSTRAP_SAMPLES: Final[int] = 2_000
BOOTSTRAP_SEED: Final[int] = 43
MIN_PROMOTION_CLUSTER_DATES: Final[int] = 8

_RACE_ID = re.compile(
    r"^jra:(?P<year>[0-9]{4}):(?P<month_day>[0-9]{4}):"
    r"(?P<venue>[^:]+):(?P<race>[^:]+)$"
)
_SHADOW_SQL: Final[str] = f"""
    SELECT race_id, router_version, baseline_top5, shadow_top5,
           upset_probability, market_weight, active, generated_at
    FROM {SHADOW_TABLE}
    WHERE router_version = %s
      AND split_part(race_id, ':', 2) || split_part(race_id, ':', 3) = %s
"""
_RESULT_SQL: Final[str] = """
    SELECT keibajo_code, race_bango, ketto_toroku_bango,
           kakutei_chakujun, tansho_ninkijun
    FROM jvd_se
    WHERE kaisai_nen = %s AND kaisai_tsukihi = %s
"""


@dataclass(frozen=True, slots=True)
class ShadowPrediction:
    race_id: str
    router_version: str
    baseline_top1: str
    shadow_top1: str
    upset_probability: float
    market_weight: float
    active: bool


@dataclass(frozen=True, slots=True)
class FinalizedHorse:
    finish: int | None
    market_rank: int | None


@dataclass(frozen=True, slots=True)
class ShadowOutcome:
    race_date: str
    baseline_finish: int
    shadow_finish: int
    winner_market_rank: int

    @property
    def segment(self) -> str:
        if self.winner_market_rank >= UPSET_MIN_WINNER_MARKET_RANK:
            return "upset"
        return "favorite_driven"


@dataclass(frozen=True, slots=True)
class DayEvaluation:
    date: str
    router_version: str
    shadow_rows: int
    evaluated_races: int
    missing_final_result: int
    missing_final_popularity: int
    metrics: dict[str, float]
    outcomes: tuple[ShadowOutcome, ...]


@dataclass(frozen=True, slots=True)
class CumulativeEvaluation:
    date_from: str
    date_to: str
    router_version: str
    evaluated_races: int
    cluster_dates: int
    metrics: dict[str, float]


@dataclass(slots=True)
class RangeSummary:
    dates_processed: int = 0
    dates_logged: int = 0
    dates_without_shadow: int = 0
    cumulative_logged: bool = False
    errors: list[str] = field(default_factory=list)


ConnectionFactory = Callable[[], db.ConnectionLike]


def _positive_int(value: object) -> int | None:
    text = str(value or "").strip()
    if not text.isascii() or not text.isdigit():
        return None
    parsed = int(text)
    return parsed if parsed > 0 else None


def _parse_race_id(race_id: str) -> tuple[str, str, str] | None:
    match = _RACE_ID.fullmatch(race_id)
    if match is None:
        return None
    return (
        match.group("year") + match.group("month_day"),
        match.group("venue"),
        match.group("race"),
    )


def fetch_shadow_predictions(
    conn: db.ConnectionLike, date_str: str, router_version: str = SHADOW_ROUTER_VERSION
) -> list[ShadowPrediction]:
    cursor = conn.cursor()
    cursor.execute(_SHADOW_SQL, (router_version, date_str))
    predictions: list[ShadowPrediction] = []
    for row in cursor.fetchall():
        baseline_top5 = row[2]
        shadow_top5 = row[3]
        if not isinstance(baseline_top5, Sequence) or isinstance(baseline_top5, str):
            continue
        if not isinstance(shadow_top5, Sequence) or isinstance(shadow_top5, str):
            continue
        if not baseline_top5 or not shadow_top5:
            continue
        predictions.append(
            ShadowPrediction(
                race_id=str(row[0]),
                router_version=str(row[1]),
                baseline_top1=str(baseline_top5[0]),
                shadow_top1=str(shadow_top5[0]),
                upset_probability=float(cast("int | float | str", row[4])),
                market_weight=float(cast("int | float | str", row[5])),
                active=bool(row[6]),
            )
        )
    return predictions


def fetch_finalized_results(
    conn: db.ConnectionLike, date_str: str
) -> dict[tuple[str, str, str], FinalizedHorse]:
    cursor = conn.cursor()
    cursor.execute(_RESULT_SQL, (date_str[:4], date_str[4:]))
    return {
        (str(row[0]), str(row[1]), str(row[2])): FinalizedHorse(
            finish=_positive_int(row[3]), market_rank=_positive_int(row[4])
        )
        for row in cursor.fetchall()
    }


def _segment_outcomes(outcomes: Sequence[ShadowOutcome], segment: str) -> list[ShadowOutcome]:
    if segment == "all":
        return list(outcomes)
    return [row for row in outcomes if row.segment == segment]


def _delta_at_cutoff(outcomes: Sequence[ShadowOutcome], cutoff: int) -> float:
    denominator = len(outcomes)
    baseline_hits = sum(row.baseline_finish <= cutoff for row in outcomes)
    shadow_hits = sum(row.shadow_finish <= cutoff for row in outcomes)
    return 100.0 * (shadow_hits - baseline_hits) / denominator


def _cumulative_metrics(outcomes: Sequence[ShadowOutcome], prefix: str) -> dict[str, float]:
    if not outcomes:
        return {}
    denominator = len(outcomes)
    metrics: dict[str, float] = {f"{prefix}_races": float(denominator)}
    for cutoff in range(1, 6):
        baseline = 100.0 * sum(row.baseline_finish <= cutoff for row in outcomes) / denominator
        shadow = 100.0 * sum(row.shadow_finish <= cutoff for row in outcomes) / denominator
        metrics[f"{prefix}_baseline_top{cutoff}_pct"] = baseline
        metrics[f"{prefix}_shadow_top{cutoff}_pct"] = shadow
        metrics[f"{prefix}_delta_top{cutoff}_pt"] = shadow - baseline
    metrics[f"{prefix}_delta_top1_3_mean_pt"] = (
        sum(metrics[f"{prefix}_delta_top{cutoff}_pt"] for cutoff in range(1, 4)) / 3.0
    )
    return metrics


def _percentile(sorted_values: Sequence[float], probability: float) -> float:
    position = probability * (len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction


def _cluster_bootstrap_metrics(
    outcomes: Sequence[ShadowOutcome], *, samples: int, seed: int
) -> dict[str, float]:
    dates = sorted({row.race_date for row in outcomes})
    if len(dates) < 2 or samples < 1:
        return {}
    by_date = {
        date_str: [row for row in outcomes if row.race_date == date_str] for date_str in dates
    }
    rng = random.Random(seed)
    distributions: dict[str, list[float]] = {}
    for _ in range(samples):
        sampled: list[ShadowOutcome] = []
        for date_str in rng.choices(dates, k=len(dates)):
            sampled.extend(by_date[date_str])
        for segment in ("all", "favorite_driven", "upset"):
            selected = _segment_outcomes(sampled, segment)
            if not selected:
                continue
            deltas = [_delta_at_cutoff(selected, cutoff) for cutoff in range(1, 6)]
            for cutoff, delta in enumerate(deltas, start=1):
                distributions.setdefault(f"{segment}_delta_top{cutoff}", []).append(delta)
            distributions.setdefault(f"{segment}_delta_top1_3_mean", []).append(
                sum(deltas[:3]) / 3.0
            )
    metrics: dict[str, float] = {}
    for name, values in distributions.items():
        if len(values) != samples:
            continue
        ordered = sorted(values)
        metrics[f"{name}_ci95_low_pt"] = _percentile(ordered, 0.025)
        metrics[f"{name}_ci95_high_pt"] = _percentile(ordered, 0.975)
    return metrics


def evaluate_cumulative(
    date_from: str,
    date_to: str,
    day_evaluations: Sequence[DayEvaluation],
    *,
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
    bootstrap_seed: int = BOOTSTRAP_SEED,
) -> CumulativeEvaluation:
    """Pool finalized forward outcomes and estimate date-cluster uncertainty."""
    outcomes = [outcome for evaluation in day_evaluations for outcome in evaluation.outcomes]
    metrics: dict[str, float] = {}
    for segment in ("all", "favorite_driven", "upset"):
        metrics.update(_cumulative_metrics(_segment_outcomes(outcomes, segment), segment))
    cluster_dates = len({outcome.race_date for outcome in outcomes})
    metrics["cluster_dates"] = float(cluster_dates)
    metrics["evaluated_races"] = float(len(outcomes))
    metrics.update(
        _cluster_bootstrap_metrics(
            outcomes,
            samples=bootstrap_samples,
            seed=bootstrap_seed,
        )
    )

    all_top5_nonnegative = all(
        metrics.get(f"all_delta_top{cutoff}_pt", -1.0) >= 0.0 for cutoff in range(1, 6)
    )
    segment_means_positive = all(
        metrics.get(f"{segment}_delta_top1_3_mean_pt", 0.0) > 0.0
        for segment in ("all", "favorite_driven", "upset")
    )
    confidence_positive = cluster_dates >= MIN_PROMOTION_CLUSTER_DATES and all(
        metrics.get(f"{segment}_delta_top1_3_mean_ci95_low_pt", 0.0) > 0.0
        for segment in ("all", "favorite_driven", "upset")
    )
    metrics["promotion_point_gate"] = float(all_top5_nonnegative and segment_means_positive)
    metrics["promotion_confidence_gate"] = float(confidence_positive)
    metrics["promotion_ready"] = float(
        all_top5_nonnegative and segment_means_positive and confidence_positive
    )
    return CumulativeEvaluation(
        date_from=date_from,
        date_to=date_to,
        router_version=SHADOW_ROUTER_VERSION,
        evaluated_races=len(outcomes),
        cluster_dates=cluster_dates,
        metrics=metrics,
    )


def evaluate_day(
    date_str: str,
    predictions: Sequence[ShadowPrediction],
    results: Mapping[tuple[str, str, str], FinalizedHorse],
) -> DayEvaluation:
    outcomes: list[ShadowOutcome] = []
    missing_final_result = 0
    missing_final_popularity = 0
    for prediction in predictions:
        parsed = _parse_race_id(prediction.race_id)
        if parsed is None or parsed[0] != date_str:
            missing_final_result += 1
            continue
        _, venue, race_bango = parsed
        baseline = results.get((venue, race_bango, prediction.baseline_top1))
        shadow = results.get((venue, race_bango, prediction.shadow_top1))
        if baseline is None or shadow is None or baseline.finish is None or shadow.finish is None:
            missing_final_result += 1
            continue
        winners = [
            result.market_rank
            for (result_venue, result_race, _), result in results.items()
            if result_venue == venue
            and result_race == race_bango
            and result.finish == 1
            and result.market_rank is not None
        ]
        if not winners:
            missing_final_popularity += 1
            continue
        outcomes.append(
            ShadowOutcome(
                race_date=date_str,
                baseline_finish=baseline.finish,
                shadow_finish=shadow.finish,
                winner_market_rank=min(winners),
            )
        )

    metrics: dict[str, float] = {}
    segments = {
        "all": outcomes,
        "favorite_driven": [row for row in outcomes if row.segment == "favorite_driven"],
        "upset": [row for row in outcomes if row.segment == "upset"],
    }
    for segment, rows in segments.items():
        metrics.update(_cumulative_metrics(rows, segment))
    metrics["shadow_rows"] = float(len(predictions))
    metrics["evaluated_races"] = float(len(outcomes))
    metrics["missing_final_result"] = float(missing_final_result)
    metrics["missing_final_popularity"] = float(missing_final_popularity)
    metrics["active_races"] = float(sum(prediction.active for prediction in predictions))
    return DayEvaluation(
        date=date_str,
        router_version=SHADOW_ROUTER_VERSION,
        shadow_rows=len(predictions),
        evaluated_races=len(outcomes),
        missing_final_result=missing_final_result,
        missing_final_popularity=missing_final_popularity,
        metrics=metrics,
        outcomes=tuple(outcomes),
    )


def _find_run(client: MlflowClient, experiment_id: str, key: str) -> str | None:
    runs = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{SHADOW_EVAL_KEY_TAG} = '{key}'",
        max_results=1,
    )
    return runs[0].info.run_id if runs else None


def log_day_evaluation(client: MlflowClient, evaluation: DayEvaluation) -> str:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_DYNAMIC_MARKET_SHADOW)
    key = f"{evaluation.date}:{evaluation.router_version}"
    run_id = _find_run(client, experiment_id, key)
    if run_id is None:
        run = client.create_run(
            experiment_id,
            run_name=f"{evaluation.date} JRA dynamic-market shadow",
            tags={
                SHADOW_EVAL_KEY_TAG: key,
                "date": evaluation.date,
                "category": "jra",
                "task": "finish-position",
                "router_version": evaluation.router_version,
                "evaluation_regime": "forward-shadow",
            },
        )
        run_id = run.info.run_id
    timestamp = int(time.time() * 1000)
    log_batch_chunked(
        client,
        run_id,
        metrics=[
            Metric(name, value, timestamp, 0) for name, value in sorted(evaluation.metrics.items())
        ],
        tags=[RunTag("evaluation_complete", str(evaluation.evaluated_races > 0).lower())],
    )
    log_json_artifact(client, run_id, METRICS_ARTIFACT, asdict(evaluation))
    client.set_terminated(run_id, status="FINISHED")
    return run_id


def log_cumulative_evaluation(client: MlflowClient, evaluation: CumulativeEvaluation) -> str:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_DYNAMIC_MARKET_SHADOW)
    key = f"cumulative:{evaluation.date_from}:{evaluation.date_to}:{evaluation.router_version}"
    run_id = _find_run(client, experiment_id, key)
    if run_id is None:
        run = client.create_run(
            experiment_id,
            run_name=f"{evaluation.date_to} JRA dynamic-market cumulative",
            tags={
                SHADOW_EVAL_KEY_TAG: key,
                "date_from": evaluation.date_from,
                "date_to": evaluation.date_to,
                "category": "jra",
                "task": "finish-position",
                "router_version": evaluation.router_version,
                "evaluation_regime": "forward-shadow-cumulative",
            },
        )
        run_id = run.info.run_id
    timestamp = int(time.time() * 1000)
    log_batch_chunked(
        client,
        run_id,
        metrics=[
            Metric(name, value, timestamp, 0) for name, value in sorted(evaluation.metrics.items())
        ],
        tags=[RunTag("promotion_ready", str(bool(evaluation.metrics["promotion_ready"])).lower())],
    )
    log_json_artifact(client, run_id, METRICS_ARTIFACT, asdict(evaluation))
    client.set_terminated(run_id, status="FINISHED")
    return run_id


def _date_range(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(f"{date_from[:4]}-{date_from[4:6]}-{date_from[6:]}")
    end = date.fromisoformat(f"{date_to[:4]}-{date_to[4:6]}-{date_to[6:]}")
    if start > end:
        raise ValueError("date_from must be on or before date_to")
    return [
        (start + timedelta(days=offset)).strftime("%Y%m%d")
        for offset in range((end - start).days + 1)
    ]


def evaluate_range(
    client: MlflowClient,
    date_from: str,
    date_to: str,
    *,
    neon_factory: ConnectionFactory = db.connect_racing_neon,
    local_factory: ConnectionFactory = db.connect_local_replica,
) -> RangeSummary:
    summary = RangeSummary()
    neon_conn = neon_factory()
    local_conn = local_factory()
    evaluations: list[DayEvaluation] = []
    try:
        for date_str in _date_range(date_from, date_to):
            summary.dates_processed += 1
            try:
                predictions = fetch_shadow_predictions(neon_conn, date_str)
                if not predictions:
                    summary.dates_without_shadow += 1
                    continue
                results = fetch_finalized_results(local_conn, date_str)
                evaluation = evaluate_day(date_str, predictions, results)
                log_day_evaluation(client, evaluation)
                evaluations.append(evaluation)
                summary.dates_logged += 1
            except (OSError, PsycopgError, RuntimeError, ValueError) as exc:
                summary.errors.append(f"{date_str}: {exc}")
        if evaluations:
            try:
                cumulative = evaluate_cumulative(date_from, date_to, evaluations)
                log_cumulative_evaluation(client, cumulative)
                summary.cumulative_logged = True
            except (OSError, RuntimeError, ValueError) as exc:
                summary.errors.append(f"cumulative:{date_from}:{date_to}: {exc}")
    finally:
        neon_conn.close()
        local_conn.close()
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    config.load_dotenv_local()
    config.load_repo_root_env_fallback()
    parser = argparse.ArgumentParser(description="Evaluate JRA dynamic-market shadow predictions")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    args = parser.parse_args(argv)
    client = MlflowClient(tracking_uri=config.get_tracking_uri())
    summary = evaluate_range(client, args.date_from, args.date_to)
    print(
        f"dates processed: {summary.dates_processed}\n"
        f"dates logged: {summary.dates_logged}\n"
        f"dates without shadow: {summary.dates_without_shadow}\n"
        f"cumulative logged: {str(summary.cumulative_logged).lower()}\n"
        f"errors: {len(summary.errors)}"
    )
    for error in summary.errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if summary.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
