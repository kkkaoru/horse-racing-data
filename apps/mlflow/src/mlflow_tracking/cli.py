"""Command-line interface for the horse-racing MLflow tracking helper.

Run via: uv run python -m mlflow_tracking.cli <subcommand> ...
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import chdir
from datetime import date
from pathlib import Path

from mlflow import MlflowClient

from mlflow_tracking import (
    backfill_finish_position,
    backfill_running_style,
    backfill_serve_timeline,
    backfill_traces,
    cell_eval_runs,
    champion_cell_eval,
    config,
    export_production,
    ingest_eval,
    ingest_local_pg_history,
    refresh_eval_metrics,
    registry,
    sync_production,
    timeline,
    training_run,
)
from mlflow_tracking.logging_api import get_or_create_experiment


def build_client() -> MlflowClient:
    """Construct the MlflowClient for the configured tracking URI."""
    return MlflowClient(tracking_uri=config.get_tracking_uri())


def _parse_categories(raw: str) -> tuple[str, ...]:
    """Parse a comma-separated --categories/--category value into a tuple, stripping whitespace."""
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def cmd_init(_args: argparse.Namespace) -> int:
    """Create the data directory, switch the sqlite db to WAL mode, and
    ensure every canonical experiment exists. Warns (does not fail) when R2
    mode is configured but the bucket is unreachable."""
    data_dir = config.get_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    config.ensure_wal(config.db_path_for(data_dir))

    # A brand-new sqlite tracking store auto-creates mlflow's own "Default"
    # experiment (id=0) the first time any store method touches it, with an
    # artifact_location resolved as "<cwd>/mlruns" -- there is no public,
    # non-server MlflowClient/env-var API to override this. `init` is the
    # documented first command against a fresh store, so running that first
    # touch with cwd=data_dir keeps the stray directory inside the
    # already-gitignored apps/mlflow/data/ instead of leaking to the package
    # root regardless of the caller's actual working directory.
    with chdir(data_dir):
        client = build_client()
        for experiment_name in config.ALL_EXPERIMENT_NAMES:
            get_or_create_experiment(client, experiment_name)

    if config.get_artifacts_mode() == "r2":
        config.apply_r2_env()
        bucket = config.get_r2_bucket()
        if not config.check_r2_bucket_reachable(bucket):
            print(
                f"warning: R2 bucket {bucket!r} is not reachable (check R2 credentials)",
                file=sys.stderr,
            )

    print(f"initialized tracking store at {data_dir}")
    return 0


def cmd_backfill_finish_position_flat(args: argparse.Namespace) -> int:
    """Register HISTORICAL finish-position versions from a FLAT staging
    directory (--models-root), never touching champion aliases."""
    client = build_client()
    summary = backfill_finish_position.backfill_finish_position_flat(
        client, Path(args.models_root)
    )
    print(
        f"versions registered: {summary['versions_registered']}\n"
        f"skipped (already registered): {len(summary['skipped_existing'])}"
    )
    for error in summary["errors"]:
        print(f"error: {error}", file=sys.stderr)
    return 1 if summary["errors"] else 0


def cmd_backfill_finish_position(args: argparse.Namespace) -> int:
    if args.models_root is not None:
        return cmd_backfill_finish_position_flat(args)

    client = build_client()
    summary = backfill_finish_position.backfill_finish_position(client)
    print(
        f"base versions registered: {summary['base_versions_registered']}\n"
        f"per-class versions registered: {summary['per_class_versions_registered']}\n"
        f"champion sync: {summary['champion_sync']}\n"
        f"cell routing run: {summary['cell_routing_run_id']}"
    )
    for error in summary["errors"]:
        print(f"error: {error}", file=sys.stderr)

    # A "skipped"/failed champion-sync entry means a production category has
    # no champion alias pointing at any version -- that must never pass
    # silently as a dict string in stdout, so it is surfaced loudly on
    # stderr and, unless explicitly overridden, fails the command.
    incomplete = {
        category: status
        for category, status in summary["champion_sync"].items()
        if status != "ok"
    }
    for category, status in incomplete.items():
        print(f"warning: champion sync incomplete for {category!r}: {status}", file=sys.stderr)
    if incomplete and not args.allow_missing_champion:
        print(
            "error: champion sync incomplete for "
            f"{sorted(incomplete)}; pass --allow-missing-champion to proceed anyway",
            file=sys.stderr,
        )
        return 1
    return 1 if summary["errors"] else 0


def cmd_backfill_running_style(args: argparse.Namespace) -> int:
    client = build_client()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else backfill_running_style.DEFAULT_ARTIFACT_ROOT
    )
    summary = backfill_running_style.backfill_running_style(client, artifact_root)
    print(
        f"versions registered: {summary['versions_registered']}\n"
        f"champion sync: {summary['champion_sync']}"
    )
    for error in summary["errors"]:
        print(f"error: {error}", file=sys.stderr)
    return 1 if summary["errors"] else 0


def cmd_ingest_trial_registry(args: argparse.Namespace) -> int:
    client = build_client()
    run_ids = ingest_eval.ingest_trial_registry(client, Path(args.path))
    print(f"logged {len(run_ids)} run(s): {', '.join(run_ids)}")
    return 0


def cmd_ingest_serve_accuracy(args: argparse.Namespace) -> int:
    client = build_client()
    run_id = ingest_eval.ingest_serve_accuracy(
        client, Path(args.path), eval_regime=args.eval_regime, experiment_name=args.experiment
    )
    print(f"logged run: {run_id}")
    return 0


def _warn_if_run_name_already_exists(
    client: MlflowClient, experiment_name: str, run_name: str
) -> None:
    """Print a non-blocking stderr HINT when a run named `run_name` already
    exists in `experiment_name` -- a "did you mean to pass --run-key?" nudge
    for the (default) --run-key-omitted `log-eval` path, which always
    creates a new run and would otherwise silently accumulate same-named
    duplicates on every repeated call. Never raises and never affects the
    command's exit code -- a missing/not-yet-created experiment simply has
    no runs to find, which is not itself worth warning about."""
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        return
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"attributes.run_name = '{run_name}'",
        max_results=1,
    )
    if matches:
        print(
            f"hint: a run named {run_name!r} already exists in {experiment_name!r} "
            f"(run_id={matches[0].info.run_id}) -- pass --run-key to reuse it instead of "
            "creating a duplicate",
            file=sys.stderr,
        )


def cmd_log_eval(args: argparse.Namespace) -> int:
    client = build_client()
    if args.run_key is None:
        resolved_run_name = args.run_name if args.run_name is not None else Path(args.path).stem
        _warn_if_run_name_already_exists(client, args.experiment, resolved_run_name)
    run_id = ingest_eval.ingest_cell_report(
        client,
        Path(args.path),
        eval_regime=args.eval_regime,
        experiment_name=args.experiment,
        run_name=args.run_name,
        run_key=args.run_key,
    )
    print(f"logged run: {run_id}")
    return 0


def cmd_ingest_local_pg_model_evaluations(args: argparse.Namespace) -> int:
    client = build_client()
    run_ids = ingest_local_pg_history.ingest_model_prediction_evaluations(
        client, Path(args.path), eval_regime=args.eval_regime
    )
    print(f"logged {len(run_ids)} run(s): {', '.join(run_ids)}")
    return 0


def cmd_ingest_local_pg_running_style_buckets(args: argparse.Namespace) -> int:
    client = build_client()
    run_ids = ingest_local_pg_history.ingest_running_style_bucket_evaluations(
        client, Path(args.path), eval_regime=args.eval_regime
    )
    print(f"logged {len(run_ids)} run(s): {', '.join(run_ids)}")
    return 0


def cmd_log_training_run(args: argparse.Namespace) -> int:
    client = build_client()
    try:
        run_id = training_run.log_training_run(client, Path(args.manifest_path))
    except (ValueError, OSError, KeyError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"logged run: {run_id}")
    return 0


def cmd_export_cell_routing(args: argparse.Namespace) -> int:
    client = build_client()
    output_path = Path(args.output) if args.output else None
    try:
        result_path = export_production.export_cell_routing(
            client, args.category, output_path, upload_r2=args.upload_r2
        )
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"exported cell routing to: {result_path}")
    return 0


def cmd_export_active_models(args: argparse.Namespace) -> int:
    client = build_client()
    output_path = Path(args.output) if args.output else None
    try:
        result_path = export_production.export_active_models(
            client, output_path, upload_r2=args.upload_r2, allow_missing=args.allow_missing
        )
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"exported active models to: {result_path}")
    return 0


def cmd_backfill_serve_timeline(args: argparse.Namespace) -> int:
    client = build_client()
    summary = backfill_serve_timeline.backfill_serve_timeline(
        client,
        args.category,
        args.date_from,
        args.date_to,
        skip_existing=args.skip_existing,
    )
    print(
        f"dates total: {summary.dates_total}\n"
        f"dates ingested: {summary.dates_ingested}\n"
        f"dates skipped (existing): {summary.dates_skipped_existing}\n"
        f"dates skipped (no data): {summary.dates_skipped_no_data}"
    )
    for error in summary.errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if summary.errors else 0


def cmd_sync_production(args: argparse.Namespace) -> int:
    client = build_client()
    categories = _parse_categories(args.categories)
    summary = sync_production.sync_production_range(
        client,
        args.date_from,
        args.date_to,
        categories=categories,
        emit_traces=not args.no_traces,
    )
    print(
        f"dates processed: {summary.dates_processed}\n"
        f"fp runs created: {summary.fp_runs_created}\n"
        f"fp runs reused: {summary.fp_runs_reused}\n"
        f"fp eval logged: {summary.fp_eval_logged}\n"
        f"fp eval skipped (no results): {summary.fp_eval_skipped_no_results}\n"
        f"rs runs created: {summary.rs_runs_created}\n"
        f"rs runs reused: {summary.rs_runs_reused}\n"
        f"rs eval logged: {summary.rs_eval_logged}\n"
        f"rs eval skipped (no results): {summary.rs_eval_skipped_no_results}\n"
        f"serving gaps detected: {summary.serving_gaps_detected}\n"
        f"champion gaps detected: {summary.champion_gaps_detected}\n"
        f"traces created: {summary.traces_created}\n"
        f"traces already existed (idempotent no-op): {summary.traces_already_existed}"
    )
    for error in summary.errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if summary.errors else 0


def cmd_refresh_eval_metrics(_args: argparse.Namespace) -> int:
    """One-time metrics-only enrichment: backfill fp_place4_pct/fp_place5_pct/
    fp_place6_pct onto every already-evaluated finish-position production-usage
    run (and the corresponding v2 finish-position timeline run), without
    re-logging any existing metric/table/artifact. See
    refresh_eval_metrics.py's own module docstring for the full rationale."""
    client = build_client()
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(client)
    print(
        f"runs scanned: {summary.runs_scanned}\n"
        f"runs updated: {summary.runs_updated}\n"
        f"runs skipped (already enriched): {summary.runs_skipped_already_enriched}\n"
        f"runs skipped (not applicable -- no eligible races for rank 4/5/6): "
        f"{summary.runs_skipped_not_applicable}\n"
        f"metric points appended: {summary.metric_points_appended}\n"
        f"timeline points appended: {summary.timeline_points_appended}"
    )
    for error in summary.errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if summary.errors else 0


def cmd_backfill_traces(args: argparse.Namespace) -> int:
    """One-time (but safely re-runnable) historical backfill: emit MLflow
    traces (+ Feedback assessments) for every already-evaluated
    (date, category, model_version) run in BOTH the finish-position and
    running-style production-usage experiments. See backfill_traces.py's
    own module docstring for the full idempotency/design rationale."""
    client = build_client()
    for bound_name, bound_value in (("--date-from", args.date_from), ("--date-to", args.date_to)):
        if bound_value is not None:
            try:
                timeline.validate_yyyymmdd(bound_value)
            except ValueError as exc:
                print(f"error: {bound_name}: {exc}", file=sys.stderr)
                return 1
    summary = backfill_traces.backfill_traces(
        client, date_from=args.date_from, date_to=args.date_to
    )
    print(
        f"fp runs scanned: {summary.fp_runs_scanned}\n"
        f"fp traces created: {summary.fp_traces_created}\n"
        f"fp traces already existed (idempotent no-op): {summary.fp_traces_already_existed}\n"
        f"rs runs scanned: {summary.rs_runs_scanned}\n"
        f"rs traces created: {summary.rs_traces_created}\n"
        f"rs traces already existed (idempotent no-op): {summary.rs_traces_already_existed}"
    )
    for error in summary.errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if summary.errors else 0


def cmd_eval_champion_cells(args: argparse.Namespace) -> int:
    client = build_client()
    categories = _parse_categories(args.category)
    as_of_date: date | None = None
    if args.as_of:
        try:
            timeline.validate_yyyymmdd(args.as_of)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        as_of_date = date(int(args.as_of[:4]), int(args.as_of[4:6]), int(args.as_of[6:8]))

    results = champion_cell_eval.eval_champion_cells(
        client, categories=categories, window_days=args.window_days, as_of=as_of_date
    )
    for result in results:
        print(
            f"category: {result.category}  task: {result.task}  "
            f"champion_model_version: {result.champion_model_version}  "
            f"unit_count: {result.unit_count}  "
            f"unit_count_champion_only: {result.unit_count_champion_only}  "
            f"cell_count: {result.cell_count}  "
            f"low_n_cell_count: {result.low_n_cell_count}  "
            f"has_champion_coverage: {result.has_champion_coverage}  "
            f"run_id: {result.run_id}"
        )
    # Per-(category, task) failures are isolated and warned about inside
    # eval_champion_cells itself (via warnings.warn), not surfaced as
    # returned errors -- there is no summary-object error list to check here,
    # unlike cmd_sync_production above, so this command always exits 0.
    return 0


def cmd_eval_cells(args: argparse.Namespace) -> int:
    client = build_client()
    categories = _parse_categories(args.category)
    summaries = cell_eval_runs.eval_cells(
        client, categories=categories, window_days=args.window_days, min_races=args.min_races
    )
    for summary in summaries:
        print(
            f"category: {summary.category}  task: {summary.task}  "
            f"window_days: {summary.window_days}  "
            f"min_races_requested: {summary.min_races_requested}  "
            f"min_races_used: {summary.min_races_used}  "
            f"volume_guard_triggered: {summary.volume_guard_triggered}  "
            f"champion_model_version: {summary.champion_model_version}  "
            f"runs_created: {summary.runs_created}  "
            f"runs_reused: {summary.runs_reused}  "
            f"cells_skipped_low_volume: {summary.cells_skipped_low_volume}"
        )
    # Same rationale as cmd_eval_champion_cells above: per-(category, task)
    # failures are isolated and warned about inside eval_cells itself, so
    # this command always exits 0.
    return 0


def cmd_set_champion(args: argparse.Namespace) -> int:
    client = build_client()
    registry.set_champion(client, args.model, args.version)
    print(f"champion alias set: {args.model} -> version {args.version}")
    return 0


def cmd_set_challenger(args: argparse.Namespace) -> int:
    client = build_client()
    registry.set_challenger(client, args.model, args.version)
    print(f"challenger alias set: {args.model} -> version {args.version}")
    return 0


def cmd_list_models(_args: argparse.Namespace) -> int:
    client = build_client()
    for model in client.search_registered_models():
        aliases = ", ".join(f"{alias}={version}" for alias, version in model.aliases.items())
        print(f"{model.name}  [{aliases}]" if aliases else model.name)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mlflow_tracking", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "init", help="Create data dir, WAL mode, and canonical experiments"
    ).set_defaults(func=cmd_init)

    backfill_fp_parser = subparsers.add_parser(
        "backfill-finish-position", help="Backfill the finish-position model registry from disk"
    )
    backfill_fp_parser.add_argument(
        "--allow-missing-champion",
        action="store_true",
        help="Exit 0 even if one or more categories' champion alias failed to sync",
    )
    backfill_fp_parser.add_argument(
        "--models-root",
        default=None,
        help="Register HISTORICAL versions from a FLAT {version}/metadata.json staging "
        "directory (e.g. apps/pc-keiba-viewer/tmp/models/) instead of running the full "
        "production-tree backfill. Never touches champion aliases or cell_routing.json.",
    )
    backfill_fp_parser.set_defaults(func=cmd_backfill_finish_position)

    rs_parser = subparsers.add_parser(
        "backfill-running-style", help="Backfill the running-style model registry from a directory"
    )
    rs_parser.add_argument(
        "artifact_root",
        nargs="?",
        default=None,
        help="Directory to scan for running-style model artifacts "
        "(default: apps/pc-keiba-viewer/tmp/models/)",
    )
    rs_parser.set_defaults(func=cmd_backfill_running_style)

    trial_parser = subparsers.add_parser(
        "ingest-trial-registry", help="Ingest a trial_registry_{category}.duckdb file"
    )
    trial_parser.add_argument("path", help="Path to the trial_registry duckdb file")
    trial_parser.set_defaults(func=cmd_ingest_trial_registry)

    serve_parser = subparsers.add_parser(
        "ingest-serve-accuracy", help="Ingest a serve_accuracy_report.py --json payload"
    )
    serve_parser.add_argument("path", help="Path to the serve-accuracy JSON file")
    serve_parser.add_argument(
        "--eval-regime",
        required=True,
        help="Evaluation regime (e.g. 'serve', 'oos', 'self-consistency', 'unspecified')",
    )
    serve_parser.add_argument(
        "--experiment",
        default=None,
        help="Experiment name override (default: auto-detect finish-position vs running-style)",
    )
    serve_parser.set_defaults(func=cmd_ingest_serve_accuracy)

    log_parser = subparsers.add_parser(
        "log-eval", help="Ingest a generic cell-metrics table (parquet/JSON)"
    )
    log_parser.add_argument("path", help="Path to the cell-metrics parquet or JSON file")
    log_parser.add_argument(
        "--eval-regime",
        required=True,
        help="Evaluation regime (e.g. 'serve', 'oos', 'self-consistency', 'unspecified')",
    )
    log_parser.add_argument(
        "--experiment", default=config.EXPERIMENT_FP_WF_EVAL, help="Experiment name"
    )
    log_parser.add_argument("--run-name", default=None, help="Optional run name override")
    log_parser.add_argument(
        "--run-key",
        default=None,
        help="Optional idempotency key: when given, find-or-reuse an existing run tagged "
        "with this run_key in --experiment instead of always creating a new run. Omitted "
        "(default): unchanged behavior, always creates a new run.",
    )
    log_parser.set_defaults(func=cmd_log_eval)

    pg_model_evals_parser = subparsers.add_parser(
        "ingest-local-pg-model-evaluations",
        help="Ingest a model_prediction_evaluations local-PG-replica export (parquet/JSON)",
    )
    pg_model_evals_parser.add_argument("path", help="Path to the exported parquet/JSON file")
    pg_model_evals_parser.add_argument(
        "--eval-regime",
        default="wf",
        help="Evaluation regime (default: 'wf' -- this table is written by the offline "
        "walk-forward bucket-eval harness, never a live-serve path)",
    )
    pg_model_evals_parser.set_defaults(func=cmd_ingest_local_pg_model_evaluations)

    pg_rs_buckets_parser = subparsers.add_parser(
        "ingest-local-pg-running-style-buckets",
        help="Ingest a running_style_model_bucket_evaluations local-PG-replica export "
        "(parquet/JSON)",
    )
    pg_rs_buckets_parser.add_argument("path", help="Path to the exported parquet/JSON file")
    pg_rs_buckets_parser.add_argument(
        "--eval-regime",
        default="wf",
        help="Evaluation regime (default: 'wf' -- this table is written by the offline "
        "walk-forward bucket-eval harness, never a live-serve path)",
    )
    pg_rs_buckets_parser.set_defaults(func=cmd_ingest_local_pg_running_style_buckets)

    training_run_parser = subparsers.add_parser(
        "log-training-run",
        help="Ingest a hr-mlflow-training-run/v1 manifest emitted by a trainer hook",
    )
    training_run_parser.add_argument("manifest_path", help="Path to the training-run manifest JSON")
    training_run_parser.set_defaults(func=cmd_log_training_run)

    export_routing_parser = subparsers.add_parser(
        "export-cell-routing",
        help="Export a cell_routing.json-compatible fragment from the registry",
    )
    export_routing_parser.add_argument(
        "--category", required=True, help="jra, nar, or banei/ban-ei"
    )
    export_routing_parser.add_argument(
        "--output", default=None, help="Output path (default: apps/mlflow/data/exports/)"
    )
    export_routing_parser.add_argument(
        "--upload-r2", default=None, help="Optional s3://bucket/key to also upload the export to"
    )
    export_routing_parser.set_defaults(func=cmd_export_cell_routing)

    export_active_parser = subparsers.add_parser(
        "export-active-models",
        help="Export {model_versions, subclass_overrides} from registry champion aliases",
    )
    export_active_parser.add_argument(
        "--output", default=None, help="Output path (default: apps/mlflow/data/exports/)"
    )
    export_active_parser.add_argument(
        "--upload-r2", default=None, help="Optional s3://bucket/key to also upload the export to"
    )
    export_active_parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Write a partial export even if one or more categories have no champion alias",
    )
    export_active_parser.set_defaults(func=cmd_export_active_models)

    backfill_timeline_parser = subparsers.add_parser(
        "backfill-serve-timeline",
        help="Backfill timelines/{finish-position,running-style} points from "
        "serve_accuracy_report.py over a historical date range (jra/nar only)",
    )
    backfill_timeline_parser.add_argument(
        "--category", choices=["jra", "nar"], required=True, help="jra or nar (no Ban-ei support)"
    )
    backfill_timeline_parser.add_argument(
        "--date-from", required=True, help="Start date (YYYYMMDD), inclusive"
    )
    backfill_timeline_parser.add_argument(
        "--date-to", required=True, help="End date (YYYYMMDD), inclusive"
    )
    backfill_timeline_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip a date when both timelines already have a point there",
    )
    backfill_timeline_parser.set_defaults(func=cmd_backfill_serve_timeline)

    sync_production_parser = subparsers.add_parser(
        "sync-production",
        help="Sync genuinely-served production predictions into MLflow (production-usage "
        "tracking, prediction-vs-result evaluation, and timelines dense points)",
    )
    sync_production_parser.add_argument(
        "--date-from", required=True, help="Start date (YYYYMMDD), inclusive"
    )
    sync_production_parser.add_argument(
        "--date-to", required=True, help="End date (YYYYMMDD), inclusive"
    )
    sync_production_parser.add_argument(
        "--categories",
        default="jra,nar,banei",
        help="Comma-separated categories to sync (default: jra,nar,banei)",
    )
    sync_production_parser.add_argument(
        "--no-traces",
        action="store_true",
        help="Skip MLflow trace/assessment emission for this call (base tracking and eval "
        "metrics/tables are unaffected) -- see trace_emit.py's module docstring",
    )
    sync_production_parser.set_defaults(func=cmd_sync_production)

    subparsers.add_parser(
        "refresh-eval-metrics",
        help="One-time metrics-only backfill: append fp_place4_pct/fp_place5_pct/fp_place6_pct "
        "to every already-evaluated finish-position production-usage run (and its v2 timeline "
        "run), without re-logging any existing metric/table/artifact",
    ).set_defaults(func=cmd_refresh_eval_metrics)

    backfill_traces_parser = subparsers.add_parser(
        "backfill-traces",
        help="One-time (but safely re-runnable) historical backfill: emit MLflow traces + "
        "Feedback assessments for every already-evaluated (date, category, model_version) run "
        "in BOTH the finish-position and running-style production-usage experiments -- see "
        "backfill_traces.py's own module docstring",
    )
    backfill_traces_parser.add_argument(
        "--date-from",
        default=None,
        help="Optional start date (YYYYMMDD), inclusive -- restrict the backfill to runs on or "
        "after this date (default: no lower bound, every run ever found)",
    )
    backfill_traces_parser.add_argument(
        "--date-to",
        default=None,
        help="Optional end date (YYYYMMDD), inclusive -- restrict the backfill to runs on or "
        "before this date (default: no upper bound)",
    )
    backfill_traces_parser.set_defaults(func=cmd_backfill_traces)

    champion_cells_parser = subparsers.add_parser(
        "eval-champion-cells",
        help="Evaluate each category's current champion model at CELL granularity over a "
        "trailing window of genuinely-served, finalized-result predictions",
    )
    champion_cells_parser.add_argument(
        "--window-days",
        type=int,
        default=champion_cell_eval.DEFAULT_WINDOW_DAYS,
        help=f"Trailing window size in days (default: {champion_cell_eval.DEFAULT_WINDOW_DAYS})",
    )
    champion_cells_parser.add_argument(
        "--category",
        default="jra,nar,banei",
        help="Comma-separated categories to evaluate (default: jra,nar,banei)",
    )
    champion_cells_parser.add_argument(
        "--as-of",
        default=None,
        help="Override the as-of date (YYYYMMDD, default: today) -- mainly for reproducible "
        "re-runs",
    )
    champion_cells_parser.set_defaults(func=cmd_eval_champion_cells)

    cell_eval_runs_parser = subparsers.add_parser(
        "eval-cells",
        help="Score EVERY served model_version (champion + variants + any other version with "
        "enough volume) at CELL granularity, logging one PERSISTENT, drillable MLflow run per "
        "(category, cell, model_version) with a trend of daily points -- see cell_eval_runs.py",
    )
    cell_eval_runs_parser.add_argument(
        "--category",
        default="jra,nar,banei",
        help="Comma-separated categories to evaluate, from {jra, nar, banei} "
        "(default: jra,nar,banei)",
    )
    cell_eval_runs_parser.add_argument(
        "--window-days",
        type=int,
        default=cell_eval_runs.DEFAULT_WINDOW_DAYS,
        help=f"Trailing window size in days (default: {cell_eval_runs.DEFAULT_WINDOW_DAYS})",
    )
    cell_eval_runs_parser.add_argument(
        "--min-races",
        type=int,
        default=cell_eval_runs.DEFAULT_MIN_RACES,
        help="Minimum races/horses in-window for a (model_version, cell) group to get its own "
        f"run (default: {cell_eval_runs.DEFAULT_MIN_RACES}); automatically raised, with a "
        "warning, if it would otherwise produce more than "
        f"{cell_eval_runs.MAX_RUNS_PER_CATEGORY_TASK} runs for one category/task",
    )
    cell_eval_runs_parser.set_defaults(func=cmd_eval_cells)

    champion_parser = subparsers.add_parser(
        "set-champion", help="Set the champion alias on a registered model"
    )
    champion_parser.add_argument("model", help="Registered model name")
    champion_parser.add_argument("version", help="MLflow model version number")
    champion_parser.set_defaults(func=cmd_set_champion)

    challenger_parser = subparsers.add_parser(
        "set-challenger", help="Set the challenger alias on a registered model"
    )
    challenger_parser.add_argument("model", help="Registered model name")
    challenger_parser.add_argument("version", help="MLflow model version number")
    challenger_parser.set_defaults(func=cmd_set_challenger)

    subparsers.add_parser(
        "list-models", help="List registered models and their aliases"
    ).set_defaults(func=cmd_list_models)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    # Best-effort load of apps/mlflow/.env.local before anything else reads
    # os.environ, so every subcommand (not just tests that call cmd_*
    # directly) sees the same backend URI / R2 settings even when invoked
    # without direnv (cron, launchd, subprocess). load_repo_root_env_fallback()
    # runs immediately after as a defense-in-depth fallback: it only ever
    # imports a narrow allow-list of keys from the repo-root .env, and its
    # setdefault semantics mean any key already loaded from .env.local above
    # (or already present in the process env) wins.
    config.load_dotenv_local()
    config.load_repo_root_env_fallback()
    parser = build_parser()
    args = parser.parse_args(argv)
    func = args.func
    return func(args)


if __name__ == "__main__":
    # Allows `python -m mlflow_tracking.cli ...` (documented in README.md) to
    # work directly, in addition to `python -m mlflow_tracking ...` via
    # __main__.py -- `python -m pkg.module` runs that module as __main__ and
    # never reaches __main__.py's own guard, so cli.py needs its own.
    raise SystemExit(main())
