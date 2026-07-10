"""Ingest a single training-run manifest into MLflow.

Consumed by env-gated hooks in the pc-keiba-viewer trainers, which call:
``uv run --project <repo>/apps/mlflow python -m mlflow_tracking.cli
log-training-run <manifest-path>``. The manifest schema
(``hr-mlflow-training-run/v1``) is a fixed cross-repo contract — do not change
its field names/shape without also updating the trainer-side hook.

Manifest shape (``?`` = optional, defaults noted)::

    {
      "schema": "hr-mlflow-training-run/v1",
      "task": "finish-position" | "running-style",
      "category": "jra" | "nar" | "banei",
      "model_version": "<string>",
      "artifact_dir": "<abs path containing metadata.json>",   ?
      "eval_regime": "wf" | "oos" | "serve" | "self-consistency" | "unspecified",
      "experiment": "<explicit experiment name override>",     ?
      "cell_report": "<abs path to a parquet|json cell table>", ?
      "aggregate_metrics": {"<metric>": <number>},              ?
      "params": {...},                                          ?
      "tags": {...},                                             ?
      "register": false,   ? (default false)
      "champion": false    ? (default false)
    }

`register`/`champion` are best-effort side effects on top of the logged run;
everything else is pure logging. The caller (hook) treats a non-zero CLI exit
as a non-fatal warning, so this module never leaves a run half-open on error
paths that occur after run creation — it always terminates what it started.

★ Why this generic training-run ingester also has serve-accuracy-specific
timeline logic (see `_maybe_log_serve_timeline_point` below): `ingest_eval.py`
exposes its own `ingest_serve_accuracy` (and CLI subcommand
`ingest-serve-accuracy`) as what looks like THE ingestion path for
`serve_accuracy_report.py --json` output, and it does also upsert a timeline
point. But empirically (grepped across this repo), that function is never
invoked automatically anywhere — it is a manual/CLI-only path. The ACTUAL
live path, every time `serve_accuracy_report.py` runs for real, is:
`serve_accuracy_report.py::run()` -> `emit_mlflow_serve_accuracy()` ->
`mlflow_hook.safe_emit_training_run()` -> a subprocess call to THIS module's
CLI entry point (`log-training-run`) -> `log_training_run()` (this
function). Confirmed against the real Neon-backed tracking store: the live
`finish-position/serve-accuracy` experiment's existing runs carry
UNPREFIXED metric keys (`top1_pct`, `place2_pct`, ...) matching this
module's own `aggregate_metrics` handling (built from
`mlflow_hook.flatten_numeric_metrics(fp_dict)`), not `ingest_eval.py`'s
`fp_`-prefixed keys. `serve_accuracy_report.py` itself is also not
cron/launchd-scheduled anywhere in this repo (it is run manually today), but
if/when it is automated, THIS is the code path that will actually fire — so
the timeline hook belongs here, not only in `ingest_eval.py`, or the
`timelines` experiment would silently never receive live-serve points.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final, cast

from mlflow import MlflowClient
from mlflow.entities import Metric, Param, RunTag
from mlflow.tracing.client import TracingClient

from mlflow_tracking import config, ingest_eval, logging_api, registry, timeline, trace_emit
from mlflow_tracking.logging_api import (
    get_or_create_experiment,
    log_batch_chunked,
    log_json_artifact,
    route_json_fields,
)

MANIFEST_SCHEMA: Final[str] = "hr-mlflow-training-run/v1"

# Best-effort key list for the SERVE-mode job_trace `races` Feedback (see
# trace_emit.job_trace) -- mirrors ingest_eval._RACES_LIKE_KEYS/_races_
# feedback_value's identical rationale, kept as a SEPARATE small duplicate
# here (not imported) since it operates on a different payload shape: a
# single flattened `aggregate_metrics` dict for ONE task, not a (fp, rs)
# section pair.
_RACES_LIKE_KEYS: Final[tuple[str, ...]] = ("races", "race_count", "total_horses", "horse_count")

VALID_TASKS: Final[frozenset[str]] = frozenset({"finish-position", "running-style"})

# Generic (family-agnostic) metadata.json routing: the union of
# finish-position's and running-style's identity/flatten keys, since
# artifact_dir's metadata.json could be either shape and this ingestion path
# has no other signal to tell which.
GENERIC_IDENTITY_TAG_KEYS: Final[frozenset[str]] = frozenset(
    {
        "model_version",
        "architecture",
        "category",
        "note",
        "feature_schema_version",
        "class_weight_scheme",
    }
)
GENERIC_FLATTEN_PARAM_KEYS: Final[frozenset[str]] = frozenset(
    {"hyperparams", "hyperparameters", "scale_and_bias"}
)


def _resolve_experiment(task: str, eval_regime: str) -> str:
    """Pick the experiment by task+eval_regime (running-style has one eval
    experiment regardless of regime; finish-position splits wf vs serve)."""
    if task == "running-style":
        return config.EXPERIMENT_RS_EVAL
    if eval_regime == "serve":
        return config.EXPERIMENT_FP_SERVE_ACCURACY
    return config.EXPERIMENT_FP_WF_EVAL


def _require_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"manifest '{key}' must be a non-empty string")
    return value


def _optional_str(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"manifest '{key}' must be a non-empty string when present")
    return value


def _optional_dict(payload: dict[str, object], key: str) -> dict[str, object]:
    value = payload.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"manifest '{key}' must be an object when present")
    return cast(dict[str, object], value)


def _optional_bool(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key, False)
    return bool(value)


def _ingest_artifact_dir(client: MlflowClient, run_id: str, artifact_dir: Path) -> None:
    """Ingest artifact_dir's metadata.json the same way the backfill_*
    modules do for their own metadata.json (family-agnostic key routing)."""
    metadata_path = artifact_dir / "metadata.json"
    if not metadata_path.is_file():
        return
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        return
    params, tags, artifact_payloads = route_json_fields(
        metadata,
        identity_tag_keys=GENERIC_IDENTITY_TAG_KEYS,
        flatten_param_keys=GENERIC_FLATTEN_PARAM_KEYS,
    )
    log_batch_chunked(
        client,
        run_id,
        params=[Param(k, v) for k, v in params.items()],
        tags=[RunTag(k, v) for k, v in tags.items()],
    )
    for artifact_key, payload in artifact_payloads.items():
        log_json_artifact(client, run_id, f"{artifact_key}.json", payload)


def _ingest_cell_report(client: MlflowClient, run_id: str, cell_report_path: Path) -> None:
    """Ingest cell_report through the same normalize + log_table + parquet
    path as ingest_eval.ingest_cell_report, but into an already-open run
    rather than creating a new one."""
    cell_df = ingest_eval.normalize_cell_dataframe(ingest_eval.read_cell_table(cell_report_path))
    headline = logging_api.select_headline_metrics(cell_df)
    if headline:
        headline_items = [Metric(k, v, 0, 0) for k, v in headline.items()]
        log_batch_chunked(client, run_id, metrics=headline_items)
    logging_api.log_cell_table(client, run_id, cell_df)


def _register_and_promote(
    client: MlflowClient,
    run_id: str,
    task: registry.Task,
    category: registry.Category,
    model_version_label: str,
    artifact_dir: str | None,
    should_set_champion: bool,
) -> None:
    registered_name = registry.registered_model_name(category, task)
    source_uri = (
        f"file://{Path(artifact_dir).resolve()}" if artifact_dir is not None else f"runs:/{run_id}"
    )
    version_tags = registry.version_tags(category=category, task=task)
    version_tags["model_version"] = model_version_label
    version = registry.register_version(
        client, registered_name, source_uri, run_id=run_id, tags=version_tags
    )
    if should_set_champion:
        registry.set_champion(client, registered_name, version.version)


def _races_feedback_value(aggregate_metrics: Mapping[str, object]) -> float:
    """Best-effort extraction of a "how many races/horses did this
    training-run manifest's `aggregate_metrics` represent" numeric value,
    for the SERVE-mode job_trace `races` Feedback (see `_RACES_LIKE_KEYS`'s
    own comment). Returns 0.0 when none of these keys are present -- a
    deliberately honest "unknown", never a fabricated nonzero guess."""
    for key in _RACES_LIKE_KEYS:
        value = aggregate_metrics.get(key)
        if isinstance(value, int | float):
            return float(value)
    return 0.0


def _maybe_log_serve_timeline_point(
    client: MlflowClient,
    task: registry.Task,
    category: registry.Category,
    eval_regime: str,
    manifest_tags: dict[str, object],
    aggregate_metrics: dict[str, object],
    *,
    tracing_client: TracingClient,
) -> str | None:
    """Upsert a `timelines` point for a live-serve manifest, or no-op.

    Fires only when BOTH: `eval_regime == "serve"` (this manifest represents
    a real serve-accuracy check, not a wf/oos/self-consistency/unspecified
    eval) AND the manifest carries a non-empty string `date` tag (without a
    date, there is no calendar position to plot a point at). See this
    module's own docstring for why this hook lives here rather than only in
    ingest_eval.py.

    Returns the timeline run_id when a point was actually logged (so the
    caller can tag the per-day run with it), or None when the hook did not
    fire (wrong regime, no date tag, or the extracted metric subset was
    empty — e.g. an `aggregate_metrics` that happens to carry no
    timeline-relevant keys).

    ★ Job-execution trace (2026-07-11): `tracing_client` is REQUIRED
    (unlike a "None disables tracing" optional design) since this module's
    ONLY call site (`log_training_run` below) always has one built already
    -- there is no other real or test caller that needs a no-tracing
    variant (this package's own convention -- see `sync_production.py`'s
    `emit_traces`-gated `tracing_client` -- is to make tracing OPTIONAL only
    where a caller genuinely needs to skip it, e.g. `--no-traces`; no such
    flag exists for `log-training-run`). When this hook is ABOUT to
    actually upsert a point, that REAL `timeline.upsert_timeline_point`
    call is wrapped in its OWN nested `trace_emit.job_trace` landing in
    `config.EXPERIMENT_TIMELINES` (Feedback `points_appended`, always 1.0
    here -- this hook upserts at most one point per call, unlike
    `ingest_eval.ingest_serve_accuracy`'s up-to-two) -- mirrors that
    function's identical nested-job-trace pattern (see its own docstring).
    Never opened when this hook does not fire.
    """
    if eval_regime != "serve":
        return None
    date_tag = manifest_tags.get("date")
    if not isinstance(date_tag, str) or not date_tag:
        return None
    timeline_metrics = (
        timeline.fp_metrics_for_timeline(aggregate_metrics)
        if task == "finish-position"
        else timeline.rs_metrics_for_timeline(aggregate_metrics)
    )
    if not timeline_metrics:
        return None

    timelines_experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    with trace_emit.job_trace(
        tracing_client,
        timelines_experiment_id,
        "log-training-run",
        attributes={"task": task, "category": category},
    ) as t:
        timeline_run_id = timeline.upsert_timeline_point(
            client, task, category, date_tag, timeline_metrics
        )
        t.feedback("points_appended", 1.0)
    return timeline_run_id


def log_training_run(client: MlflowClient, manifest_path: Path) -> str:
    """Ingest one training-run manifest, returning the created run_id.

    Raises ValueError/OSError/json.JSONDecodeError on a malformed manifest —
    callers (the CLI) are expected to catch these and exit non-zero with a
    clear message, per the fixed manifest contract's error-handling policy.

    ★ Job-execution trace (2026-07-11): one `trace_emit.job_trace` per call
    into this SAME `resolved_experiment`. Two distinct SHAPES, chosen by
    `eval_regime == "serve"` (mirrors `_resolve_experiment`'s own serve/non-
    serve split, though the two are independent decisions -- an explicit
    `experiment` override can route a serve-regime manifest anywhere):
      - SERVE mode (job_name `log-training-run-serve`): Feedback `races`
        (numeric, see `_races_feedback_value`) -- no named steps (this
        package's per-race/per-horse serve-usage signal already lives in
        `finish-position/production-usage`/`running-style/production-usage`
        via the prior wave's traces; this job trace is about "did THIS
        ingestion run", not a second usage signal).
      - WF mode (job_name `log-training-run-wf`, every OTHER eval_regime --
        wf/oos/self-consistency/unspecified): steps `parse-manifest`
        (extracting params/tags/aggregate_metrics from the payload),
        `log-metrics` (the `log_batch_chunked` call), `ingest-metadata`
        (the optional `artifact_dir`/`cell_report` ingestion -- entered
        even when neither is present, a trivial no-op span in that case).
        Feedback `ingestion_ok` (bool): True unless `artifact_dir` was
        given but its `metadata.json` was missing (`_ingest_artifact_dir`'s
        own silent no-op path -- see that function's docstring) -- the one
        real "specified but nothing there" soft-failure this function can
        observe without raising.
    `eval_regime`/`era` (era from the manifest's own `tags.era`, when
    present -- this manifest schema has no dedicated top-level `era` field)
    are span ATTRIBUTES on EITHER shape, never a Feedback.
    """
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"manifest must be a JSON object: {manifest_path}")

    schema = payload.get("schema")
    if schema != MANIFEST_SCHEMA:
        raise ValueError(f"unsupported manifest schema: {schema!r} (expected {MANIFEST_SCHEMA!r})")

    task_raw = _require_str(payload, "task")
    if task_raw not in VALID_TASKS:
        raise ValueError(f"manifest 'task' must be one of {sorted(VALID_TASKS)}, got: {task_raw!r}")
    task: registry.Task = "finish-position" if task_raw == "finish-position" else "running-style"

    category = registry.normalize_category(_require_str(payload, "category"))
    if task == "running-style" and category == "banei":
        raise ValueError("running-style does not support category=banei (Ban-ei is out of scope)")

    model_version_label = _require_str(payload, "model_version")
    eval_regime = _require_str(payload, "eval_regime")
    ingest_eval.validate_eval_regime(eval_regime)

    experiment_override = _optional_str(payload, "experiment")
    resolved_experiment = (
        experiment_override
        if experiment_override is not None
        else _resolve_experiment(task, eval_regime)
    )
    experiment_id = get_or_create_experiment(client, resolved_experiment)

    # A cheap, direct peek at the manifest's own (not-yet-validated) "tags"
    # object purely to surface "era" as a job-trace ATTRIBUTE if present --
    # done here (before the `with` block below even opens) rather than
    # inside the "parse-manifest" step, since job_trace's `attributes` are
    # fixed at construction time; the real, validated parse still happens
    # in "parse-manifest" below via `_optional_dict`.
    raw_manifest_tags = payload.get("tags")
    era_hint = raw_manifest_tags.get("era") if isinstance(raw_manifest_tags, dict) else None

    tracing_client = trace_emit.build_tracing_client(client)
    is_serve_mode = eval_regime == "serve"
    job_name = "log-training-run-serve" if is_serve_mode else "log-training-run-wf"
    job_attributes: dict[str, object] = {
        "task": task,
        "category": category,
        "model_version": model_version_label,
        "eval_regime": eval_regime,
    }
    if isinstance(era_hint, str) and era_hint:
        job_attributes["era"] = era_hint

    with trace_emit.job_trace(
        tracing_client, experiment_id, job_name, attributes=job_attributes
    ) as t:
        with t.step("parse-manifest"):
            manifest_params = _optional_dict(payload, "params")
            manifest_tags = _optional_dict(payload, "tags")
            aggregate_metrics = _optional_dict(payload, "aggregate_metrics")

        run = client.create_run(experiment_id, run_name=model_version_label)
        run_id = run.info.run_id

        tags: dict[str, str] = {
            "eval_regime": eval_regime,
            "task": task,
            "category": category,
            "model_version": model_version_label,
        }
        tags.update({str(k): str(v) for k, v in manifest_tags.items()})

        # See this module's docstring for why a generic training-run ingester
        # also upserts a `timelines` point: this is the empirically-real,
        # automatic live-serve path (ingest_eval.py's own timeline wiring is
        # manual/CLI-only). No-ops for every non-serve eval_regime and every
        # manifest without a `date` tag. Threading `tracing_client` through
        # makes the ACTUAL upsert (when it fires) get its own nested job
        # trace into `timelines` -- see that function's own docstring.
        timeline_run_id = _maybe_log_serve_timeline_point(
            client,
            task,
            category,
            eval_regime,
            manifest_tags,
            aggregate_metrics,
            tracing_client=tracing_client,
        )
        if timeline_run_id is not None:
            tags[f"timeline_run_id:{task}"] = timeline_run_id

        with t.step("log-metrics"):
            params: dict[str, str] = {str(k): str(v) for k, v in manifest_params.items()}
            metric_items = [
                Metric(str(k), float(v), 0, 0)
                for k, v in aggregate_metrics.items()
                if isinstance(v, int | float)
            ]
            log_batch_chunked(
                client,
                run_id,
                metrics=metric_items,
                params=[Param(k, v) for k, v in params.items()],
                tags=[RunTag(k, v) for k, v in tags.items()],
            )

        artifact_dir = _optional_str(payload, "artifact_dir")
        # True unless `artifact_dir` was given but its metadata.json is
        # missing (`_ingest_artifact_dir`'s own silent no-op path) -- the
        # WF-mode `ingestion_ok` Feedback's basis (see this function's own
        # docstring). Nothing requested at all is trivially "ok".
        metadata_ingested = True
        with t.step("ingest-metadata"):
            if artifact_dir is not None:
                _ingest_artifact_dir(client, run_id, Path(artifact_dir))
                metadata_ingested = (Path(artifact_dir) / "metadata.json").is_file()

            cell_report = _optional_str(payload, "cell_report")
            if cell_report is not None:
                _ingest_cell_report(client, run_id, Path(cell_report))

        client.set_terminated(run_id, status="FINISHED")

        if _optional_bool(payload, "register"):
            _register_and_promote(
                client,
                run_id,
                task,
                category,
                model_version_label,
                artifact_dir,
                _optional_bool(payload, "champion"),
            )

        if is_serve_mode:
            t.feedback("races", _races_feedback_value(aggregate_metrics))
        else:
            t.feedback("ingestion_ok", metadata_ingested)

    return run_id
