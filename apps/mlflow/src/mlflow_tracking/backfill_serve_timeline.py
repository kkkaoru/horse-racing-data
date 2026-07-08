"""Backfill the `timelines` experiment's finish-position/running-style serve-
accuracy points over a historical date range.

Re-invokes `apps/pc-keiba-viewer/src/scripts/serve_accuracy_report.py --json`
once per date and feeds its payload through
`ingest_eval.ingest_serve_accuracy` (which itself upserts the timeline point
-- see ingest_eval.py's wiring), rather than reimplementing the PG query or
the timeline-point logic here. This module owns only: the date-range walk,
subprocess orchestration, and per-date error isolation.

jra/nar ONLY -- there is no Ban-ei support to backfill. serve_accuracy_report.py's
own `VALID_CATEGORIES = ("jra", "nar")` has no Ban-ei entry at all (its own
argparse `--category` would reject "banei" outright), so covering Ban-ei here
would require a separate, out-of-scope change to that script first. This
module defensively re-validates the category itself (not just via the CLI's
argparse `choices`) so a direct programmatic call gets the same guardrail.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final, Literal

from mlflow import MlflowClient

from mlflow_tracking import config, ingest_eval, timeline

SUPPORTED_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar"})

SERVE_ACCURACY_SCRIPT_RELATIVE_PATH: Final[str] = "src/scripts/serve_accuracy_report.py"

# The env var apps/pc-keiba-viewer/src/scripts/mlflow_hook.py checks (see its
# own module docstring) before shelling out to `log-training-run` at all.
# _default_subprocess_runner forces this to "0" unconditionally on every
# invocation it makes -- see that function's own docstring for why.
MLFLOW_ENABLED_ENV_VAR: Final[str] = "HORSE_RACING_MLFLOW_ENABLED"
MLFLOW_DISABLED_VALUE: Final[str] = "0"

# Both jra and nar support running-style (unlike finish-position, running-
# style is never Ban-ei-eligible at all -- see registry.py's
# `VALID_TASKS`/`running-style does not support category=banei` rule in
# training_run.py), so there is no category branching needed when checking
# "does this date already have both timelines' points" for --skip-existing.
FP_SKIP_EXISTING_METRIC_KEY: Final[str] = "fp_top1_pct"
RS_SKIP_EXISTING_METRIC_KEY: Final[str] = "rs_overall_accuracy_pct"


@dataclass
class BackfillTimelineSummary:
    dates_total: int
    dates_ingested: int
    dates_skipped_existing: int
    dates_skipped_no_data: int
    errors: list[str]


SubprocessRunner = Callable[[Sequence[str], Path], subprocess.CompletedProcess[str]]


def _default_subprocess_runner(cmd: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run `cmd` for real via `subprocess.run`, forcing
    HORSE_RACING_MLFLOW_ENABLED=0 in the child's environment.

    `serve_accuracy_report.py` ALSO calls `mlflow_hook.safe_emit_training_run()`
    internally on every invocation (see its own `emit_mlflow_serve_accuracy()`),
    which would otherwise create a SECOND, differently-keyed per-day run in
    the same experiment via the training_run.py live-serve path for every
    single backfilled date, duplicating/cluttering the leaderboard on top of
    the run `backfill_serve_timeline` itself creates via
    `ingest_eval.ingest_serve_accuracy`. Disabling the child's own MLflow
    hook unconditionally (rather than requiring every caller of the
    injectable `runner` parameter to remember to do this) is the simplest
    fix: this runner exists purely for backfill, so the live hook should
    never be active for it regardless of the ambient environment's own
    HORSE_RACING_MLFLOW_ENABLED setting.
    """
    env = {**os.environ, MLFLOW_ENABLED_ENV_VAR: MLFLOW_DISABLED_VALUE}
    return subprocess.run(list(cmd), cwd=cwd, capture_output=True, text=True, check=False, env=env)


def _date_range_yyyymmdd(date_from: str, date_to: str) -> list[str]:
    """Return every YYYYMMDD string from `date_from` to `date_to`, inclusive."""
    start = date(int(date_from[:4]), int(date_from[4:6]), int(date_from[6:8]))
    end = date(int(date_to[:4]), int(date_to[4:6]), int(date_to[6:8]))
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def _timeline_point_exists_for_date(client: MlflowClient, category: str, date_str: str) -> bool:
    """True when BOTH the finish-position and running-style timelines for
    `category` already have a point at `date_str`'s step."""
    date_int = int(date_str)
    fp_dates = timeline.timeline_dates_present(
        client, "finish-position", category, FP_SKIP_EXISTING_METRIC_KEY
    )
    rs_dates = timeline.timeline_dates_present(
        client, "running-style", category, RS_SKIP_EXISTING_METRIC_KEY
    )
    return date_int in fp_dates and date_int in rs_dates


DateOutcome = Literal["ingested", "skipped_no_data", "error"]


def _run_one_date(
    client: MlflowClient,
    category: str,
    date_str: str,
    viewer_root: Path,
    runner: SubprocessRunner,
) -> tuple[DateOutcome, str]:
    """Invoke serve_accuracy_report.py for one date and ingest its payload.

    Returns (outcome, error_message) -- error_message is the empty string
    for every outcome other than "error" (kept as a plain `str`, not
    `str | None`, specifically so the caller never needs an `x or
    "fallback"` expression whose fallback arm would be structurally
    unreachable and therefore uncoverable). A three-way tagged outcome
    (rather than independent booleans) is used deliberately:
    "ingested"/"skipped_no_data"/"error" are mutually exclusive by
    construction, so the caller's if/elif/else is exhaustive with no
    unreachable branch for branch coverage to complain about. Never raises
    for a single bad date -- a malformed/empty subprocess result is
    reported via the error_message slot so one bad date cannot abort the
    whole range.
    """
    cmd = [
        "uv",
        "run",
        "--project",
        str(viewer_root),
        "python",
        SERVE_ACCURACY_SCRIPT_RELATIVE_PATH,
        "--date",
        date_str,
        "--category",
        category,
        "--json",
    ]
    result = runner(cmd, viewer_root)
    stdout = result.stdout.strip()
    if not stdout:
        stderr_tail = result.stderr.strip()[-500:]
        return (
            "error",
            f"{date_str}: empty stdout (exit code {result.returncode}); stderr: {stderr_tail}",
        )
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        return "error", f"{date_str}: invalid JSON stdout: {exc}"

    if isinstance(payload, dict) and payload.get("error") == "no_data":
        return "skipped_no_data", ""

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / f"serve_accuracy_{date_str}.json"
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        ingest_eval.ingest_serve_accuracy(client, tmp_path, eval_regime="serve")
    return "ingested", ""


def backfill_serve_timeline(
    client: MlflowClient,
    category: str,
    date_from: str,
    date_to: str,
    *,
    skip_existing: bool = False,
    viewer_root: Path | None = None,
    runner: SubprocessRunner | None = None,
) -> BackfillTimelineSummary:
    """Backfill `timelines` points for `category` over [date_from, date_to].

    For each date in the (inclusive) range: optionally skip it if
    `skip_existing` and both the finish-position and running-style timelines
    already have a point there; otherwise shell out to
    `serve_accuracy_report.py --date <date> --category <category> --json`
    and, on success, ingest its stdout payload via
    `ingest_eval.ingest_serve_accuracy` (which itself upserts the timeline
    point(s) -- see that function's docstring). A "no data for this date"
    response, an empty-stdout subprocess result, and an invalid-JSON stdout
    are all recorded (in `dates_skipped_no_data` or `errors`) rather than
    raised, so one bad date never aborts the rest of the range.

    `category` is restricted to {"jra", "nar"} -- see this module's own
    docstring for why Ban-ei has no timeline coverage. Raises ValueError for
    any other category, and for an invalid date or an inverted range
    (date_to < date_from), so a caller invoking this function directly (not
    just through the CLI's `choices=["jra", "nar"]` argparse guard) still
    gets the same guardrail.

    `runner` defaults to `_default_subprocess_runner` (real subprocess,
    HORSE_RACING_MLFLOW_ENABLED forced off) -- tests always inject a fake
    one so this function never shells out or touches Neon/local-PG.
    """
    if category not in SUPPORTED_CATEGORIES:
        raise ValueError(
            f"backfill-serve-timeline only supports category in "
            f"{sorted(SUPPORTED_CATEGORIES)} (got: {category!r}); "
            "serve_accuracy_report.py itself has no Ban-ei support"
        )
    timeline.validate_yyyymmdd(date_from)
    timeline.validate_yyyymmdd(date_to)
    if date_to < date_from:
        raise ValueError(f"date_to ({date_to!r}) must not be before date_from ({date_from!r})")

    resolved_viewer_root = (
        viewer_root if viewer_root is not None else config.REPO_ROOT / "apps" / "pc-keiba-viewer"
    )
    resolved_runner = runner if runner is not None else _default_subprocess_runner

    dates = _date_range_yyyymmdd(date_from, date_to)
    dates_ingested = 0
    dates_skipped_existing = 0
    dates_skipped_no_data = 0
    errors: list[str] = []

    for date_str in dates:
        if skip_existing and _timeline_point_exists_for_date(client, category, date_str):
            dates_skipped_existing += 1
            continue
        outcome, error = _run_one_date(
            client, category, date_str, resolved_viewer_root, resolved_runner
        )
        if outcome == "error":
            errors.append(error)
        elif outcome == "skipped_no_data":
            dates_skipped_no_data += 1
        else:
            dates_ingested += 1

    return BackfillTimelineSummary(
        dates_total=len(dates),
        dates_ingested=dates_ingested,
        dates_skipped_existing=dates_skipped_existing,
        dates_skipped_no_data=dates_skipped_no_data,
        errors=errors,
    )
