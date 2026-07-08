"""Command-line interface for the horse-racing MLflow tracking helper.

Run via: uv run python -m mlflow_tracking.cli <subcommand> ...
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import chdir
from pathlib import Path

from mlflow import MlflowClient

from mlflow_tracking import (
    backfill_finish_position,
    backfill_running_style,
    config,
    export_production,
    ingest_eval,
    registry,
    training_run,
)
from mlflow_tracking.logging_api import get_or_create_experiment


def build_client() -> MlflowClient:
    """Construct the MlflowClient for the configured tracking URI."""
    return MlflowClient(tracking_uri=config.get_tracking_uri())


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


def cmd_backfill_finish_position(args: argparse.Namespace) -> int:
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


def cmd_log_eval(args: argparse.Namespace) -> int:
    client = build_client()
    run_id = ingest_eval.ingest_cell_report(
        client,
        Path(args.path),
        eval_regime=args.eval_regime,
        experiment_name=args.experiment,
        run_name=args.run_name,
    )
    print(f"logged run: {run_id}")
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


def cmd_set_champion(args: argparse.Namespace) -> int:
    client = build_client()
    registry.set_champion(client, args.model, args.version)
    print(f"champion alias set: {args.model} -> version {args.version}")
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
    log_parser.set_defaults(func=cmd_log_eval)

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

    champion_parser = subparsers.add_parser(
        "set-champion", help="Set the champion alias on a registered model"
    )
    champion_parser.add_argument("model", help="Registered model name")
    champion_parser.add_argument("version", help="MLflow model version number")
    champion_parser.set_defaults(func=cmd_set_champion)

    subparsers.add_parser(
        "list-models", help="List registered models and their aliases"
    ).set_defaults(func=cmd_list_models)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
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
