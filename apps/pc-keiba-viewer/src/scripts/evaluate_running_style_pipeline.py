"""Aggregate running-style bucket-eval rows and emit a Markdown report.

Run with::

  uv --project apps/pc-keiba-viewer run \\
    python apps/pc-keiba-viewer/src/scripts/evaluate_running_style_pipeline.py \\
      --pg-url postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing \\
      --running-style-feature-version v1 \\
      --model-version-jra jra-running-style-lgbm-prod-v2 \\
      --model-version-nar nar-running-style-lgbm-prod-v1.5 \\
      --output report.md

The script reads aggregated rows from ``running_style_model_bucket_evaluations``
(see ``finish-position-features/evaluate-running-style-bucket-sql.ts`` for the
schema), computes per-class precision/recall/F1/log-loss/top-2/accuracy per
category, splits the metrics into train (2016-2025) vs OOS (pre-2016 + 2026) to
expose train leakage, and writes a Markdown report that downstream GitHub
Actions workflows can ship to Discord or PR comments. Exits with status 1 when
a critical anti-pattern is detected (e.g. >=20pp train/holdout accuracy gap).
"""

from __future__ import annotations

import argparse
import decimal
import importlib
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Callable, Sequence

CLASS_LABELS: tuple[str, ...] = ("nige", "senkou", "sashi", "oikomi")

TRAIN_YEAR_FROM: int = 2016
TRAIN_YEAR_TO: int = 2025

CRITICAL_ACC_GAP_PP: float = 20.0
DRIFT_PRECISION_RECALL_GAP_PP: float = 30.0
NIGE_PRECISION_LEAK_THRESHOLD: float = 0.9
NIGE_PRECISION_RECALL_GAP_LEAK_PP: float = 40.0

LOW_PRECISION_THRESHOLD: float = 0.5
LOW_PRECISION_MIN_SAMPLES: int = 100
TOP_N_WEAK_BUCKETS: int = 10

PERIOD_ALL: str = "all"
PERIOD_OOS_ONLY: str = "oos-only"
PERIOD_CHOICES: tuple[str, str] = (PERIOD_ALL, PERIOD_OOS_ONLY)

CATEGORY_JRA: str = "jra"
CATEGORY_NAR: str = "nar"

EXIT_CRITICAL: int = 1

PERIOD_LABEL_BY_KEY: dict[str, str] = {
    "all": "All",
    "train": "Train (2016-2025)",
    "oos": "OOS (pre-2016 + 2026+)",
}

SELECT_BUCKET_ROWS_SQL: str = """
    select
      category,
      evaluation_window_from,
      keibajo_code,
      kyori,
      kyoso_shubetsu_code,
      kyoso_joken_code,
      track_code,
      grade_code,
      race_name,
      race_count,
      prediction_count,
      cm_actual_nige_pred_nige_count,
      cm_actual_nige_pred_senkou_count,
      cm_actual_nige_pred_sashi_count,
      cm_actual_nige_pred_oikomi_count,
      cm_actual_senkou_pred_nige_count,
      cm_actual_senkou_pred_senkou_count,
      cm_actual_senkou_pred_sashi_count,
      cm_actual_senkou_pred_oikomi_count,
      cm_actual_sashi_pred_nige_count,
      cm_actual_sashi_pred_senkou_count,
      cm_actual_sashi_pred_sashi_count,
      cm_actual_sashi_pred_oikomi_count,
      cm_actual_oikomi_pred_nige_count,
      cm_actual_oikomi_pred_senkou_count,
      cm_actual_oikomi_pred_sashi_count,
      cm_actual_oikomi_pred_oikomi_count,
      log_loss_nige_sum,
      log_loss_nige_count,
      log_loss_senkou_sum,
      log_loss_senkou_count,
      log_loss_sashi_sum,
      log_loss_sashi_count,
      log_loss_oikomi_sum,
      log_loss_oikomi_count,
      top2_hit_count
    from running_style_model_bucket_evaluations
    where running_style_feature_version = %s
      and (
            (category = %s and model_version = %s)
         or (category = %s and model_version = %s)
      )
"""


@dataclass(frozen=True)
class BucketDimensions:
    category: str
    window_from: str
    keibajo_code: str
    kyori: int
    kyoso_shubetsu_code: str
    kyoso_joken_code: str | None
    track_code: str | None
    grade_code: str | None
    race_name: str | None


@dataclass(frozen=True)
class BucketMetrics:
    race_count: int
    prediction_count: int
    confusion: dict[tuple[str, str], int]
    log_loss_sum: dict[str, float]
    log_loss_count: dict[str, int]
    top2_hit_count: int


@dataclass(frozen=True)
class BucketRecord:
    dims: BucketDimensions
    metrics: BucketMetrics


@dataclass(frozen=True)
class PerClassMetrics:
    precision: float
    recall: float
    f1: float
    actual_count: int
    predicted_count: int


@dataclass(frozen=True)
class AggregateMetrics:
    prediction_count: int
    race_count: int
    accuracy: float
    top2_accuracy: float
    log_loss: float
    per_class: dict[str, PerClassMetrics]


@dataclass(frozen=True)
class YearMetrics:
    year: int
    metrics: AggregateMetrics


@dataclass(frozen=True)
class YearChange:
    year_from: int
    year_to: int
    delta_pp: float


@dataclass(frozen=True)
class WeakBucket:
    category: str
    keibajo_code: str
    kyori: int
    kyoso_shubetsu_code: str
    prediction_count: int
    precision: float
    class_label: str


@dataclass(frozen=True)
class CategoryReport:
    category: str
    model_version: str
    all_metrics: AggregateMetrics
    train_metrics: AggregateMetrics
    oos_metrics: AggregateMetrics
    per_year: list[YearMetrics]
    drift_years: list[YearChange]
    drift_warnings: list[str]
    weak_buckets: list[WeakBucket]


@dataclass
class ReportContext:
    feature_version: str
    period: str
    model_version_jra: str
    model_version_nar: str
    categories: list[CategoryReport] = field(default_factory=list)
    critical: bool = False


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="evaluate_running_style_pipeline")
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
    parser.add_argument("--model-version-jra", type=str, required=True)
    parser.add_argument("--model-version-nar", type=str, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--period",
        type=str,
        choices=list(PERIOD_CHOICES),
        default=PERIOD_ALL,
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def to_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, decimal.Decimal):
        return int(value)
    if isinstance(value, str) and value:
        return int(float(value))
    return 0


def to_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, str) and value:
        return float(value)
    return 0.0


def to_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def confusion_key(actual: str, predicted: str) -> tuple[str, str]:
    return (actual, predicted)


def build_confusion_from_row(row: Sequence[object]) -> dict[tuple[str, str], int]:
    offset = 11
    confusion: dict[tuple[str, str], int] = {}
    index = 0
    for actual in CLASS_LABELS:
        for predicted in CLASS_LABELS:
            confusion[confusion_key(actual, predicted)] = to_int(row[offset + index])
            index += 1
    return confusion


def build_log_loss_from_row(row: Sequence[object]) -> tuple[dict[str, float], dict[str, int]]:
    offset = 27
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    for i, label in enumerate(CLASS_LABELS):
        sums[label] = to_float(row[offset + i * 2])
        counts[label] = to_int(row[offset + i * 2 + 1])
    return sums, counts


def parse_db_row(row: Sequence[object]) -> BucketRecord:
    dims = BucketDimensions(
        category=str(row[0]),
        window_from=str(row[1]),
        keibajo_code=str(row[2]),
        kyori=to_int(row[3]),
        kyoso_shubetsu_code=str(row[4]),
        kyoso_joken_code=to_optional_str(row[5]),
        track_code=to_optional_str(row[6]),
        grade_code=to_optional_str(row[7]),
        race_name=to_optional_str(row[8]),
    )
    log_sum, log_count = build_log_loss_from_row(row)
    metrics = BucketMetrics(
        race_count=to_int(row[9]),
        prediction_count=to_int(row[10]),
        confusion=build_confusion_from_row(row),
        log_loss_sum=log_sum,
        log_loss_count=log_count,
        top2_hit_count=to_int(row[35]),
    )
    return BucketRecord(dims=dims, metrics=metrics)


def window_from_to_year(window_from: str) -> int:
    return int(window_from[:4])


def is_train_year(year: int) -> bool:
    return TRAIN_YEAR_FROM <= year <= TRAIN_YEAR_TO


def filter_by_period(records: list[BucketRecord], period: str) -> list[BucketRecord]:
    if period == PERIOD_ALL:
        return records
    return [record for record in records if not is_train_year(window_from_to_year(record.dims.window_from))]


def safe_divide(numer: float, denom: float) -> float:
    if denom == 0:
        return 0.0
    return numer / denom


def compute_per_class_metrics(confusion: dict[tuple[str, str], int]) -> dict[str, PerClassMetrics]:
    metrics: dict[str, PerClassMetrics] = {}
    for label in CLASS_LABELS:
        tp = confusion.get(confusion_key(label, label), 0)
        actual_total = sum(confusion.get(confusion_key(label, predicted), 0) for predicted in CLASS_LABELS)
        predicted_total = sum(confusion.get(confusion_key(actual, label), 0) for actual in CLASS_LABELS)
        precision = safe_divide(tp, predicted_total)
        recall = safe_divide(tp, actual_total)
        denom = precision + recall
        f1 = safe_divide(2 * precision * recall, denom) if denom > 0 else 0.0
        metrics[label] = PerClassMetrics(
            precision=precision,
            recall=recall,
            f1=f1,
            actual_count=actual_total,
            predicted_count=predicted_total,
        )
    return metrics


def aggregate_metrics(records: list[BucketRecord]) -> AggregateMetrics:
    if not records:
        return AggregateMetrics(
            prediction_count=0,
            race_count=0,
            accuracy=0.0,
            top2_accuracy=0.0,
            log_loss=0.0,
            per_class={label: PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in CLASS_LABELS},
        )
    confusion: dict[tuple[str, str], int] = {confusion_key(a, p): 0 for a in CLASS_LABELS for p in CLASS_LABELS}
    log_sum_total = 0.0
    log_count_total = 0
    race_total = 0
    pred_total = 0
    top2_total = 0
    for record in records:
        for key, val in record.metrics.confusion.items():
            confusion[key] += val
        for label in CLASS_LABELS:
            log_sum_total += record.metrics.log_loss_sum[label]
            log_count_total += record.metrics.log_loss_count[label]
        race_total += record.metrics.race_count
        pred_total += record.metrics.prediction_count
        top2_total += record.metrics.top2_hit_count
    diagonal = sum(confusion[confusion_key(label, label)] for label in CLASS_LABELS)
    total = sum(confusion.values())
    return AggregateMetrics(
        prediction_count=pred_total,
        race_count=race_total,
        accuracy=safe_divide(diagonal, total),
        top2_accuracy=safe_divide(top2_total, pred_total),
        log_loss=safe_divide(log_sum_total, log_count_total),
        per_class=compute_per_class_metrics(confusion),
    )


def group_records_by_year(records: list[BucketRecord]) -> dict[int, list[BucketRecord]]:
    grouped: dict[int, list[BucketRecord]] = {}
    for record in records:
        year = window_from_to_year(record.dims.window_from)
        grouped.setdefault(year, []).append(record)
    return grouped


def compute_per_year_metrics(records: list[BucketRecord]) -> list[YearMetrics]:
    grouped = group_records_by_year(records)
    return [YearMetrics(year=year, metrics=aggregate_metrics(grouped[year])) for year in sorted(grouped)]


def detect_year_changes(per_year: list[YearMetrics]) -> list[YearChange]:
    changes: list[YearChange] = []
    for prev, curr in zip(per_year, per_year[1:]):
        delta_pp = abs(curr.metrics.accuracy - prev.metrics.accuracy) * 100.0
        if delta_pp > CRITICAL_ACC_GAP_PP:
            changes.append(YearChange(year_from=prev.year, year_to=curr.year, delta_pp=delta_pp))
    return changes


def detect_drift_warnings(per_year: list[YearMetrics]) -> list[str]:
    warnings: list[str] = []
    for label in CLASS_LABELS:
        max_p = 0.0
        max_r = 0.0
        for ym in per_year:
            cls = ym.metrics.per_class[label]
            if cls.precision > max_p:
                max_p = cls.precision
            if cls.recall > max_r:
                max_r = cls.recall
        gap_pp = (max_p - max_r) * 100.0
        if gap_pp >= DRIFT_PRECISION_RECALL_GAP_PP:
            warnings.append(
                f"{label}: precision-recall gap reached {gap_pp:.1f}pp (max precision {max_p:.3f}, max recall {max_r:.3f}) — inflate suspected"
            )
    return warnings


def detect_train_leakage(train: AggregateMetrics, oos: AggregateMetrics) -> bool:
    if train.prediction_count == 0 or oos.prediction_count == 0:
        return False
    return (train.accuracy - oos.accuracy) * 100.0 >= CRITICAL_ACC_GAP_PP


def detect_nige_leakage(train: AggregateMetrics) -> bool:
    if train.prediction_count == 0:
        return False
    nige = train.per_class["nige"]
    gap_pp = (nige.precision - nige.recall) * 100.0
    return nige.precision >= NIGE_PRECISION_LEAK_THRESHOLD and gap_pp >= NIGE_PRECISION_RECALL_GAP_LEAK_PP


def collect_weak_buckets(records: list[BucketRecord]) -> list[WeakBucket]:
    candidates: list[WeakBucket] = []
    for record in records:
        if record.metrics.prediction_count < LOW_PRECISION_MIN_SAMPLES:
            continue
        per_class = compute_per_class_metrics(record.metrics.confusion)
        for label, cls in per_class.items():
            if cls.predicted_count == 0:
                continue
            if cls.precision < LOW_PRECISION_THRESHOLD:
                candidates.append(
                    WeakBucket(
                        category=record.dims.category,
                        keibajo_code=record.dims.keibajo_code,
                        kyori=record.dims.kyori,
                        kyoso_shubetsu_code=record.dims.kyoso_shubetsu_code,
                        prediction_count=record.metrics.prediction_count,
                        precision=cls.precision,
                        class_label=label,
                    )
                )
    return sorted(candidates, key=lambda bucket: (bucket.precision, -bucket.prediction_count))[:TOP_N_WEAK_BUCKETS]


def build_category_report(category: str, model_version: str, records: list[BucketRecord]) -> CategoryReport:
    train_records = [record for record in records if is_train_year(window_from_to_year(record.dims.window_from))]
    oos_records = [record for record in records if not is_train_year(window_from_to_year(record.dims.window_from))]
    all_metrics = aggregate_metrics(records)
    train_metrics = aggregate_metrics(train_records)
    oos_metrics = aggregate_metrics(oos_records)
    per_year = compute_per_year_metrics(records)
    return CategoryReport(
        category=category,
        model_version=model_version,
        all_metrics=all_metrics,
        train_metrics=train_metrics,
        oos_metrics=oos_metrics,
        per_year=per_year,
        drift_years=detect_year_changes(per_year),
        drift_warnings=detect_drift_warnings(per_year),
        weak_buckets=collect_weak_buckets(records),
    )


def filter_records_for_category(records: list[BucketRecord], category: str) -> list[BucketRecord]:
    return [record for record in records if record.dims.category == category]


def format_percent(value: float) -> str:
    return f"{value * 100.0:.2f}%"


def format_float(value: float) -> str:
    return f"{value:.4f}"


def format_int(value: int) -> str:
    return f"{value:,}"


def render_section_overall_table(metrics: AggregateMetrics, label: str) -> list[str]:
    lines: list[str] = []
    lines.append(f"#### {label}")
    lines.append("")
    lines.append("| Class | Precision | Recall | F1 | Actual | Predicted |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for cls_label in CLASS_LABELS:
        cls = metrics.per_class[cls_label]
        lines.append(
            f"| {cls_label} | {format_float(cls.precision)} | {format_float(cls.recall)} | {format_float(cls.f1)} | {format_int(cls.actual_count)} | {format_int(cls.predicted_count)} |"
        )
    lines.append("")
    lines.append(
        f"- Accuracy: {format_percent(metrics.accuracy)} / Top-2 accuracy: {format_percent(metrics.top2_accuracy)} / Log loss: {format_float(metrics.log_loss)}"
    )
    lines.append(
        f"- Race count: {format_int(metrics.race_count)} / Prediction count: {format_int(metrics.prediction_count)}"
    )
    lines.append("")
    return lines


def render_section_overall(report: CategoryReport) -> list[str]:
    lines: list[str] = []
    lines.append(f"### Section 1: {report.category.upper()} overall metrics ({report.model_version})")
    lines.append("")
    lines.extend(render_section_overall_table(report.all_metrics, PERIOD_LABEL_BY_KEY["all"]))
    lines.extend(render_section_overall_table(report.train_metrics, PERIOD_LABEL_BY_KEY["train"]))
    lines.extend(render_section_overall_table(report.oos_metrics, PERIOD_LABEL_BY_KEY["oos"]))
    return lines


def render_section_train_oos_gap(report: CategoryReport) -> list[str]:
    lines: list[str] = []
    lines.append(f"### Section 2: {report.category.upper()} train/holdout gap analysis")
    lines.append("")
    train_acc_pp = report.train_metrics.accuracy * 100.0
    oos_acc_pp = report.oos_metrics.accuracy * 100.0
    gap_pp = train_acc_pp - oos_acc_pp
    lines.append(f"- Train avg accuracy: {format_percent(report.train_metrics.accuracy)}")
    lines.append(f"- OOS avg accuracy: {format_percent(report.oos_metrics.accuracy)}")
    lines.append(f"- Train - OOS gap: {gap_pp:+.2f}pp")
    lines.append("")
    lines.append("| Year | Predictions | Accuracy | Top-2 | Log loss |")
    lines.append("| ---: | ---: | ---: | ---: | ---: |")
    for ym in report.per_year:
        lines.append(
            f"| {ym.year} | {format_int(ym.metrics.prediction_count)} | {format_percent(ym.metrics.accuracy)} | {format_percent(ym.metrics.top2_accuracy)} | {format_float(ym.metrics.log_loss)} |"
        )
    lines.append("")
    if report.drift_years:
        lines.append("**Year-over-year accuracy shifts > 20pp detected:**")
        lines.append("")
        for change in report.drift_years:
            lines.append(f"- {change.year_from} -> {change.year_to}: {change.delta_pp:.2f}pp")
        lines.append("")
    else:
        lines.append("- No year-over-year accuracy shift exceeding 20pp.")
        lines.append("")
    return lines


def render_section_per_class_drift(report: CategoryReport) -> list[str]:
    lines: list[str] = []
    lines.append(f"### Section 3: {report.category.upper()} per-class drift")
    lines.append("")
    lines.append("| Year | nige P/R | senkou P/R | sashi P/R | oikomi P/R |")
    lines.append("| ---: | --- | --- | --- | --- |")
    for ym in report.per_year:
        cells: list[str] = []
        for label in CLASS_LABELS:
            cls = ym.metrics.per_class[label]
            cells.append(f"{format_float(cls.precision)} / {format_float(cls.recall)}")
        lines.append(f"| {ym.year} | {cells[0]} | {cells[1]} | {cells[2]} | {cells[3]} |")
    lines.append("")
    if report.drift_warnings:
        lines.append("**Warnings:**")
        lines.append("")
        for warning in report.drift_warnings:
            lines.append(f"- {warning}")
        lines.append("")
    else:
        lines.append("- No precision/recall inflate signal detected.")
        lines.append("")
    return lines


def render_section_weak_buckets(report: CategoryReport) -> list[str]:
    lines: list[str] = []
    lines.append(f"### Section 4: {report.category.upper()} dimension-specific weak points")
    lines.append("")
    if not report.weak_buckets:
        lines.append("- No weak bucket exceeded the threshold (n >= 100, precision < 0.5).")
        lines.append("")
        return lines
    lines.append("| keibajo | kyori | shubetsu | class | Precision | Predictions |")
    lines.append("| --- | ---: | --- | --- | ---: | ---: |")
    for bucket in report.weak_buckets:
        lines.append(
            f"| {bucket.keibajo_code} | {bucket.kyori} | {bucket.kyoso_shubetsu_code} | {bucket.class_label} | {format_float(bucket.precision)} | {format_int(bucket.prediction_count)} |"
        )
    lines.append("")
    return lines


def category_recommendations(report: CategoryReport) -> list[str]:
    recs: list[str] = []
    if detect_train_leakage(report.train_metrics, report.oos_metrics):
        recs.append(
            f"P4b: {report.category.upper()} train ({TRAIN_YEAR_FROM}-{TRAIN_YEAR_TO}) accuracy outruns OOS by >= 20pp — narrow train window or rebuild holdout split"
        )
    if detect_nige_leakage(report.train_metrics):
        nige = report.train_metrics.per_class["nige"]
        recs.append(
            f"P4c: {report.category.upper()} train nige precision {nige.precision:.3f} >> recall {nige.recall:.3f} — short-window self-rate feature suspected as leak"
        )
    return recs


def render_section_recommendations(reports: list[CategoryReport]) -> list[str]:
    lines: list[str] = []
    lines.append("### Section 5: improvement recommendations")
    lines.append("")
    recs: list[str] = []
    for report in reports:
        recs.extend(category_recommendations(report))
    if not recs:
        lines.append("- No critical anti-pattern detected.")
        lines.append("")
        return lines
    for rec in recs:
        lines.append(f"- {rec}")
    lines.append("")
    return lines


def detect_critical(reports: list[CategoryReport]) -> bool:
    for report in reports:
        if detect_train_leakage(report.train_metrics, report.oos_metrics):
            return True
    return False


def render_report(ctx: ReportContext) -> str:
    lines: list[str] = []
    lines.append("# Running-style model evaluation report")
    lines.append("")
    lines.append(f"- Running-style feature version: `{ctx.feature_version}`")
    lines.append(f"- Period filter: `{ctx.period}`")
    lines.append(f"- JRA model: `{ctx.model_version_jra}`")
    lines.append(f"- NAR model: `{ctx.model_version_nar}`")
    lines.append("")
    for report in ctx.categories:
        lines.extend(render_section_overall(report))
        lines.extend(render_section_train_oos_gap(report))
        lines.extend(render_section_per_class_drift(report))
        lines.extend(render_section_weak_buckets(report))
    lines.extend(render_section_recommendations(ctx.categories))
    if ctx.critical:
        lines.append("> Critical anti-pattern detected — exit code 1.")
        lines.append("")
    return "\n".join(lines)


def summary_line(report: CategoryReport) -> str:
    gap_pp = (report.train_metrics.accuracy - report.oos_metrics.accuracy) * 100.0
    return (
        f"[{report.category}] model={report.model_version} "
        f"all_acc={format_percent(report.all_metrics.accuracy)} "
        f"train_acc={format_percent(report.train_metrics.accuracy)} "
        f"oos_acc={format_percent(report.oos_metrics.accuracy)} "
        f"gap={gap_pp:+.2f}pp"
    )


def print_summary(ctx: ReportContext, stream: IO[str]) -> None:
    for report in ctx.categories:
        print(summary_line(report), file=stream)
    if ctx.critical:
        print("CRITICAL: train leakage anti-pattern detected", file=stream)


QueryFn = Callable[[str, str, tuple[object, ...]], list[tuple[object, ...]]]


def default_psycopg_query(pg_url: str, sql: str, params: tuple[object, ...]) -> list[tuple[object, ...]]:
    module = importlib.import_module("psycopg")
    with module.connect(pg_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.encode("utf-8"), params)
            return list(cursor.fetchall())


def fetch_bucket_rows(
    pg_url: str,
    feature_version: str,
    model_version_jra: str,
    model_version_nar: str,
    query: QueryFn = default_psycopg_query,
) -> list[BucketRecord]:
    params: tuple[object, ...] = (
        feature_version,
        CATEGORY_JRA,
        model_version_jra,
        CATEGORY_NAR,
        model_version_nar,
    )
    rows = query(pg_url, SELECT_BUCKET_ROWS_SQL, params)
    return [parse_db_row(row) for row in rows]


def build_report_context(
    feature_version: str,
    period: str,
    model_version_jra: str,
    model_version_nar: str,
    records: list[BucketRecord],
) -> ReportContext:
    filtered = filter_by_period(records, period)
    jra_records = filter_records_for_category(filtered, CATEGORY_JRA)
    nar_records = filter_records_for_category(filtered, CATEGORY_NAR)
    reports: list[CategoryReport] = []
    if jra_records:
        reports.append(build_category_report(CATEGORY_JRA, model_version_jra, jra_records))
    if nar_records:
        reports.append(build_category_report(CATEGORY_NAR, model_version_nar, nar_records))
    return ReportContext(
        feature_version=feature_version,
        period=period,
        model_version_jra=model_version_jra,
        model_version_nar=model_version_nar,
        categories=reports,
        critical=detect_critical(reports),
    )


def run_pipeline(
    pg_url: str,
    feature_version: str,
    period: str,
    model_version_jra: str,
    model_version_nar: str,
    output_path: Path,
    query: QueryFn = default_psycopg_query,
    stdout: IO[str] | None = None,
) -> int:
    records = fetch_bucket_rows(
        pg_url=pg_url,
        feature_version=feature_version,
        model_version_jra=model_version_jra,
        model_version_nar=model_version_nar,
        query=query,
    )
    ctx = build_report_context(
        feature_version=feature_version,
        period=period,
        model_version_jra=model_version_jra,
        model_version_nar=model_version_nar,
        records=records,
    )
    output_path.write_text(render_report(ctx), encoding="utf-8")
    print_summary(ctx, stdout if stdout is not None else sys.stdout)
    return EXIT_CRITICAL if ctx.critical else 0


def main() -> None:
    args = parse_args()
    exit_code = run_pipeline(
        pg_url=args.pg_url,
        feature_version=args.running_style_feature_version,
        period=args.period,
        model_version_jra=args.model_version_jra,
        model_version_nar=args.model_version_nar,
        output_path=args.output,
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
