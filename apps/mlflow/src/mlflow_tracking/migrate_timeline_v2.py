"""ONE-SHOT migration: v1 timeline runs (step = `int(date_yyyymmdd)`) -> v2
timeline runs (step = `timeline.step_for_date`, days-since-2020-01-01).

★ Why this exists: MLflow 3.14's GenAI/Eval run-detail chart-tile renderer
spins forever on a Model-metrics chart whose step magnitude reaches
~20 million (every existing timeline run's step, since `step =
int(date_yyyymmdd)` e.g. 20260516) -- see `timeline.py`'s module docstring
and `timeline.step_for_date`'s docstring for the full empirical root-cause.
The fix is a new step scheme; this module carries the one-time data
migration that copies every pre-existing (v1) timeline run's full metric
history into a new v2 run under the new scheme, then marks the old run
`deprecated=true` / `superseded_by=<new_run_id>` -- never deleting or
mutating it otherwise (this package forbids data deletion).

This module is deliberately NOT wired into `cli.py`'s subcommand list: it is
a single, one-time data-shape migration meant to run against the real
production tracking store exactly once (though it IS safe to invoke more
than once -- see `migrate_v1_run`'s docstring -- there is simply no reason
to, once the real runs have all been migrated). Exposing it as a routine CLI
subcommand would misleadingly suggest it is meant to be re-run regularly,
the way `sync-production`/`backfill-serve-timeline` are.

To run this for real against the production store, load the same two env
files `cli.py`'s `main()` loads (this module deliberately does NOT do this
itself -- see `config.load_dotenv_local`/`load_repo_root_env_fallback`'s own
docstrings for why only the CLI entry point does), then call
`migrate_all_v1_timeline_runs`:

    from mlflow import MlflowClient
    from mlflow_tracking import config, migrate_timeline_v2

    config.load_dotenv_local()
    config.load_repo_root_env_fallback()
    client = MlflowClient(tracking_uri=config.get_tracking_uri())
    summary = migrate_timeline_v2.migrate_all_v1_timeline_runs(client)
    print(f"runs migrated: {len(summary.results)}")
    for result in summary.results:
        print(result)

Never print or log `config.get_tracking_uri()`'s return value itself (the
resolved DSN) -- only this module's own summary objects, which carry run
ids/task/category/point counts, never a connection string.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from mlflow import MlflowClient
from mlflow.entities import Run

from mlflow_tracking import config, timeline

# Tags identifying the 3 throwaway diagnostic runs used to root-cause the
# GenAI/Eval chart-rendering bug (see timeline.py's module docstring) --
# deliberately left in the real `timelines` experiment as a record of that
# investigation, never migrated (they are not real serve-accuracy data) and
# never deleted.
JUNK_TAG: Final[str] = "junk"
DIAGNOSTIC_TAG: Final[str] = "diagnostic"

DEPRECATED_TAG: Final[str] = "deprecated"
SUPERSEDED_BY_TAG: Final[str] = "superseded_by"
TRUE_STR: Final[str] = "true"

# Comfortably above the realistic run count in this experiment (one run per
# (task, category) pair, ever -- a handful at most), so a single
# `search_runs` call always sees every run in one page.
_MAX_RUNS_PER_EXPERIMENT: Final[int] = 1000


@dataclass
class MigratedRunResult:
    """Outcome of migrating one v1 run to its v2 counterpart."""

    old_run_id: str
    new_run_id: str
    task: str
    category: str
    points_migrated: int


@dataclass
class MigrationSummary:
    """Outcome of one `migrate_all_v1_timeline_runs` call."""

    results: list[MigratedRunResult]


def find_v1_timeline_runs(client: MlflowClient, experiment_id: str) -> list[Run]:
    """Return every REAL (non-deprecated, non-diagnostic) v1 timeline run in
    `experiment_id`.

    A "real v1 run" carries the legacy `timeline.TIMELINE_KEY_TAG` tag (the
    pre-migration run-finding key -- `upsert_timeline_point` no longer
    writes this tag on anything it creates, so its presence alone identifies
    a run that predates this migration) and excludes two families:
    - Already-migrated runs (`deprecated=true`, set by a previous
      `migrate_v1_run` call) -- so re-running this function/the migration is
      idempotent rather than re-migrating (and double-counting points onto)
      the same v2 run.
    - The 3 throwaway diagnostic runs (`junk=true` or `diagnostic=true`) used
      to root-cause the original GenAI/Eval chart-rendering bug -- these are
      synthetic copies/variants, not real serve-accuracy data, and are
      deliberately left untouched (not migrated, not deleted) as a record of
      that investigation.

    Fetches every run in the experiment via a single unfiltered
    `search_runs` call rather than expressing this 3-way tag-presence/
    tag-value exclusion as an MLflow filter-string (which has no clean way
    to express "tag key is present at all", only "tag equals this value"),
    then applies the exclusion logic in plain Python.
    """
    runs = client.search_runs([experiment_id], max_results=_MAX_RUNS_PER_EXPERIMENT)
    result: list[Run] = []
    for run in runs:
        tags = run.data.tags
        if timeline.TIMELINE_KEY_TAG not in tags:
            continue
        if tags.get(DEPRECATED_TAG) == TRUE_STR:
            continue
        if tags.get(JUNK_TAG) == TRUE_STR or tags.get(DIAGNOSTIC_TAG) == TRUE_STR:
            continue
        result.append(run)
    return result


def migrate_v1_run(client: MlflowClient, run: Run) -> MigratedRunResult:
    """Migrate one v1 timeline run's full metric history into its v2
    counterpart, then tag the old run `deprecated=true` /
    `superseded_by=<new_run_id>`.

    `task`/`category` are read from the OLD run's own `task`/`category` tags
    (set at v1 creation time by the pre-migration `upsert_timeline_point`,
    never touched by this migration) rather than parsed back out of the
    `timeline_key` tag's colon-joined value -- reading the two fields
    directly is more robust than re-splitting a string this function did not
    itself construct.

    For every metric key already logged on the old run (enumerated from
    `run.data.metrics`, a dict of key -> LATEST value -- only its KEYS are
    used here, to know which metric families exist on this run; the actual
    per-point values come from `client.get_metric_history` below, not this
    dict), reads that key's full point history and re-logs each point into
    the v2 run via `timeline.upsert_timeline_point`, so dedup-by-step and
    FINISHED-status behavior are reused rather than reimplemented. The old
    point's `step` field IS the original YYYYMMDD integer (that is exactly
    what pre-migration `upsert_timeline_point` logged as `step`), so
    `str(old_point.step)` is exactly the `date_yyyymmdd` string
    `upsert_timeline_point` expects -- it recomputes both the v2 step (via
    `timeline.step_for_date`) and the timestamp_ms (via
    `_timestamp_ms_for_date`, deterministic from that same date string) from
    it, so the migrated point's timestamp_ms lands on the exact same value
    the original point already had; only the step numbering changes, and the
    value carries over completely unchanged.

    Idempotent: calling this again for the same old run after it has already
    been migrated re-runs every `upsert_timeline_point` call, which no-ops
    on every already-logged (key, step) pair (see that function's own
    dedup-by-step docstring) and simply re-applies the same `deprecated`/
    `superseded_by` tags -- harmless, though `find_v1_timeline_runs` already
    excludes an already-`deprecated` run from ever reaching this function
    via the normal `migrate_all_v1_timeline_runs` entry point.

    Raises ValueError if the old run is missing its `task`/`category` tags,
    or if it has zero metric points to migrate -- both would mean `run` was
    never actually created by the pre-migration `upsert_timeline_point` (and
    therefore should be structurally impossible for anything
    `find_v1_timeline_runs` returns), so this is a loud failure rather than a
    silent no-op that would otherwise leave the old run un-deprecated with
    no explanation.
    """
    old_run_id = run.info.run_id
    task = run.data.tags.get("task")
    category = run.data.tags.get("category")
    if not task or not category:
        raise ValueError(f"v1 timeline run {old_run_id!r} is missing its task/category tags")

    new_run_id: str | None = None
    points_migrated = 0
    for metric_key in run.data.metrics:
        for point in client.get_metric_history(old_run_id, metric_key):
            date_yyyymmdd = str(point.step)
            new_run_id = timeline.upsert_timeline_point(
                client, task, category, date_yyyymmdd, {metric_key: point.value}
            )
            points_migrated += 1

    if new_run_id is None:
        raise ValueError(f"v1 timeline run {old_run_id!r} has no metric points to migrate")

    client.set_tag(old_run_id, DEPRECATED_TAG, TRUE_STR)
    client.set_tag(old_run_id, SUPERSEDED_BY_TAG, new_run_id)

    return MigratedRunResult(
        old_run_id=old_run_id,
        new_run_id=new_run_id,
        task=task,
        category=category,
        points_migrated=points_migrated,
    )


def migrate_all_v1_timeline_runs(client: MlflowClient) -> MigrationSummary:
    """Migrate every real v1 timeline run in the `timelines` experiment to
    its v2 counterpart (see `find_v1_timeline_runs`/`migrate_v1_run`).

    Returns an empty summary (no error) when the `timelines` experiment does
    not exist at all yet -- nothing to migrate.
    """
    experiment = client.get_experiment_by_name(config.EXPERIMENT_TIMELINES)
    if experiment is None:
        return MigrationSummary(results=[])
    v1_runs = find_v1_timeline_runs(client, experiment.experiment_id)
    results = [migrate_v1_run(client, run) for run in v1_runs]
    return MigrationSummary(results=results)
