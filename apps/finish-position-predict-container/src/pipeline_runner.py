"""Build the v7-lineage feature parquet for TODAY's races and load it.

Deploy-time I/O glue (not unit-tested; verified per DEPLOY.md). It reuses the
unchanged repo feature pipeline that already produced the training parquet:

  1. ``finish_position_features_duckdb.py`` builds the base feature parquet from
     the raw Iceberg Catalog in ``--target-date`` mode for the requested category over
     [target_date, target_date + days_ahead]. That mode emits feature rows for
     the day's races INCLUDING UPCOMING ones (``finish_position`` still NULL):
     historical aggregates are computed from prior races only
     (``h.race_date < t.race_date``), so the target race's own outcome never
     leaks in and the vector is computable before the race is run.
  2. The FULL per-category layer chain (``pipeline_args.LAYER_CHAIN``) appends
     the v6 base layers (race-internal / market-signal / sectional-and-weight /
     futan-juryo / workout / near-miss, as applicable per category) and the v7
     layers (lineage / head-to-head / baba-pedigree / trainer (JRA) / ban-ei),
     reproducing the exact feature set each model was trained on (226 JRA / 175
     NAR / 111 Ban-ei) per
     docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md
     sections 4 / 8 / 9 and
     docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V6_STACKED.md
     section 2. Each layer preserves
     UPCOMING rows via LEFT JOIN (history side filtered to finish_position NOT
     NULL), so today's races survive with NULL/0 history features and no
     missing-layer zero-fill at score time.
  3. The final parquet is read into per-race ordered feature dicts keyed by the
     canonical ``race_id`` so ``predict_upcoming`` can score each race.

The scripts live at ``/app/pipeline`` in the image (see Dockerfile). This module
only wires arguments + reads the result; all feature logic stays in the reused
scripts so there is a single source of truth. The argv vectors themselves are
built by the pure, unit-tested ``predict_lib.pipeline_args`` builders.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from time import perf_counter
from typing import IO, Final

from predict_lib.conn_url import is_catalog_source_url
from predict_lib.model_meta import Category
from predict_lib.pipeline_args import (
    build_base_argv,
    build_layer_argv,
    day_chain_for,
    layer_chain_for,
    race_chain_for,
)
from predict_lib.r2_client import r2_get_parquet, r2_head_watermark
from predict_lib.rescore import RaceScope, race_matches_scope
from predict_lib.serve import R2Config, build_r2_day_base_key

PIPELINE_DIR: Final[Path] = Path("/app/pipeline")
DUCKDB_BUILDER: Final[Path] = PIPELINE_DIR / "finish_position_features_duckdb.py"
LAYER_DIR: Final[Path] = PIPELINE_DIR / "finish-position-features"
WORK_DIR: Final[Path] = Path("/tmp/predict-upcoming")
RACE_ID_FIELD: Final[str] = "race_id"


def _group_parquet_rows(
    frame: object, target_race: str | None = None
) -> dict[str, list[Mapping[str, object]]]:
    """Group a feature parquet frame by ``race_id``, optionally to one race.

    When ``target_race`` (``keibajo:bango``) is set, extra races that leaked
    into the category work dir / day-base output are dropped before scoring.
    """
    import pandas as pd

    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"expected pandas.DataFrame, got {type(frame)!r}")
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for race_id, race_frame in frame.groupby(RACE_ID_FIELD):
        grouped[str(race_id)] = list(race_frame.to_dict(orient="records"))
    if target_race is None:
        return grouped
    keibajo_code, race_bango = target_race.split(":", 1)
    scope = RaceScope(keibajo_code=keibajo_code, race_bango=race_bango)
    return {
        race_id: entries
        for race_id, entries in grouped.items()
        if race_matches_scope(race_id, scope)
    }


STDERR_TAIL_BYTES: Final[int] = 4000
PG_URL_USERINFO_RE: Final[re.Pattern[str]] = re.compile(r"(postgresql://)[^@]+@")
PG_URL_REDACTED: Final[str] = r"\1<redacted>@"
PREDICT_DEBUG_LOGS_ENV: Final[str] = "PREDICT_DEBUG_LOGS"
TRUE_ENV_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on", "debug"})

PIPELINE_SUBPROCESS_TIMEOUT_ENV: Final[str] = "PIPELINE_SUBPROCESS_TIMEOUT_SECONDS"
"""Env var overriding :data:`DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS`."""

DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS: Final[float] = 35 * 60
"""Default ceiling on a single feature-pipeline subprocess (DuckDB base build
or one v7 layer script).

Without a bound, a subprocess that hangs (a stuck DuckDB spill, a wedged
``psql`` connection) never returns from ``Popen.wait()``, permanently
occupying the single per-process focused-full pipeline slot (or, for a batch
request, the process-wide ``_PIPELINE_EXEC_LOCK`` in ``predict_lib.serve``)
and requiring a manual container restart to clear -- the exact failure mode
that wedged JRA/NAR serving twice on 2026-07-11/12. 35 minutes is chosen to
comfortably exceed the worst observed/extrapolated single LAYER's duration
(the whole chain, all layers combined, has been observed taking up to
~25-27 minutes for JRA) while still being far below the Worker's ~40-minute
per-message retry budget (``FOCUSED_FULL_RETRY_DELAY_SECONDS`` x
``max_retries`` in ``finish-position-cron/src/queue-consumer.ts``), so a
genuine timeout still gets converted into a DLQ-visible error within that
budget rather than silently exhausting it. Env-overridable so an
unusually large backfill window (multi-day ``daysAhead``) can raise it
without a code change.
"""


def _pipeline_subprocess_timeout_seconds() -> float:
    """Read :data:`PIPELINE_SUBPROCESS_TIMEOUT_ENV`, falling back to the default.

    A missing, blank, non-numeric, or non-positive value all fall back to
    :data:`DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS` -- a malformed
    override must never silently disable the timeout by producing e.g. a
    zero or negative ``Popen.wait(timeout=...)`` bound.
    """
    raw = os.environ.get(PIPELINE_SUBPROCESS_TIMEOUT_ENV)
    if raw is None or not raw.strip():
        return DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    if value <= 0:
        return DEFAULT_PIPELINE_SUBPROCESS_TIMEOUT_SECONDS
    return value


def _kill_process_group(process: subprocess.Popen[str]) -> None:
    """Best-effort SIGKILL of *process*'s entire process group.

    ``run_with_stderr_capture`` starts the child with ``start_new_session=True``,
    making it its own process group leader, so killing the group (not just the
    direct child pid) also reaps any grandchild the feature-pipeline script
    spawned (e.g. a DuckDB/psql subprocess of its own) that would otherwise
    survive as an orphan still holding the category work directories open.
    Never raises: a process that already exited between the timeout firing and
    this call (``ProcessLookupError``) is expected, not an error.
    """
    try:
        pgid = os.getpgid(process.pid)
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError as exc:
        print(
            f"[pipeline] failed to kill process group pid={process.pid}: {exc}",
            file=sys.stderr,
            flush=True,
        )


# --- TEMPORARY diagnostic instrumentation (added 2026-07-02) ---------------
# Investigating a live production hang: the Cloudflare Queue consumer
# (finish-position-cron/src/queue-consumer.ts) holds a stub.fetch() call to
# this Container's /predict endpoint for ~15-17 minutes, gets killed/
# redelivered by the platform, retries twice more, then dead-letters with
# ZERO rows ever written to race_finish_position_model_predictions. Container
# health checks show no crashes, so the platform is killing the connection
# from OUTSIDE. We have no dashboard/SSH access this session, so per-layer
# timing (already logged to this process's stderr via ``_log_pipeline_progress``)
# never reaches us: wrangler tail only shows the parent Worker's own
# console.log (further thinned by head_sampling_rate). ``DEBUG_LAYER_TIMING_TABLE``
# writes the same timing straight to Neon so it can be read via direct SQL.
# Remove this table name, ``record_layer_timing_row``, and its call sites
# once the timeout root cause is found and fixed.
DEBUG_LAYER_TIMING_TABLE: Final[str] = "_debug_finish_position_layer_timing"
DEBUG_LAYER_TIMING_CONNECT_TIMEOUT_SECONDS: Final[int] = 5


def record_layer_timing_row(
    database_url: str,
    run_id: str,
    category: Category,
    run_date: str,
    target_race: str | None,
    layer_index: int,
    layer_total: int,
    layer_script: str,
    status: str,
    elapsed_seconds: float,
    cumulative_elapsed_seconds: float,
) -> None:
    """Best-effort write of one layer-timing row to a TEMPORARY debug table.

    See the module-level comment above ``DEBUG_LAYER_TIMING_TABLE`` for why
    this exists. This function must NEVER raise and must NEVER meaningfully
    slow down the real pipeline: it opens a short-lived connection with a
    short ``connect_timeout``, writes one row, and closes — any failure
    (including the ``CREATE TABLE IF NOT EXISTS``) is swallowed and only
    best-effort logged to stderr.
    """
    if not debug_logs_enabled():
        return
    keibajo_code: str | None = None
    race_bango: str | None = None
    if target_race is not None and ":" in target_race:
        keibajo_code, race_bango = target_race.split(":", 1)
    try:
        import psycopg

        conn = psycopg.connect(
            database_url, connect_timeout=DEBUG_LAYER_TIMING_CONNECT_TIMEOUT_SECONDS
        )
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DEBUG_LAYER_TIMING_TABLE} (
                  id BIGSERIAL PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  category TEXT NOT NULL,
                  run_date TEXT NOT NULL,
                  keibajo_code TEXT,
                  race_bango TEXT,
                  layer_index INTEGER NOT NULL,
                  layer_total INTEGER NOT NULL,
                  layer_script TEXT NOT NULL,
                  status TEXT NOT NULL,
                  elapsed_seconds DOUBLE PRECISION NOT NULL,
                  cumulative_elapsed_seconds DOUBLE PRECISION NOT NULL,
                  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                f"""
                INSERT INTO {DEBUG_LAYER_TIMING_TABLE} (
                  run_id, category, run_date, keibajo_code, race_bango,
                  layer_index, layer_total, layer_script, status,
                  elapsed_seconds, cumulative_elapsed_seconds
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    category,
                    run_date,
                    keibajo_code,
                    race_bango,
                    layer_index,
                    layer_total,
                    layer_script,
                    status,
                    elapsed_seconds,
                    cumulative_elapsed_seconds,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:  # best-effort diagnostic only, never re-raise
        print(
            f"[pipeline] debug-timing write failed run_id={run_id} "
            f"layer_index={layer_index} status={status} error={exc!r}",
            file=sys.stderr,
            flush=True,
        )


# --- end TEMPORARY diagnostic instrumentation ------------------------------


def mask_pg_url(text: str) -> str:
    """Replace ``user:pass@`` in any ``postgresql://`` URL with ``<redacted>@``.

    Defensive: subprocess argv carries the Neon URL with the password, and we
    want to be able to log argv on failure without ever leaking the secret.
    """
    return PG_URL_USERINFO_RE.sub(PG_URL_REDACTED, text)


def _capture_stream(src: IO[str], buffer: list[str], *, sink: IO[str] | None = None) -> None:
    """Collect a subprocess stream and optionally forward it for debug output."""
    for line in src:
        if sink is not None:
            sink.write(line)
            sink.flush()
        buffer.append(line)


def debug_logs_enabled() -> bool:
    return os.environ.get(PREDICT_DEBUG_LOGS_ENV, "").strip().lower() in TRUE_ENV_VALUES


def run_with_stderr_capture(args: Sequence[str]) -> None:
    """Run a subprocess, capturing stderr tail and streaming only in debug mode.

    Without this wrapper ``subprocess.run(check=True)`` raises
    ``CalledProcessError`` but does NOT include the child's stderr in the
    message, so a silent ``exit 1`` from the feature pipeline turns into an
    opaque parent-side traceback. We:

    * capture the child's stdout/stderr and only forward it to parent streams
      when debug logs are explicitly enabled;
    * keep the stderr tail in memory so it can be attached to the RuntimeError
      on failure;
    * mask any ``--pg-url`` in argv before logging so the Neon password never
      reaches logs (defensive);
    * bound the wait with :func:`_pipeline_subprocess_timeout_seconds` -- on
      expiry, SIGKILL the whole process group (see :func:`_kill_process_group`)
      and raise ``RuntimeError`` instead of hanging forever. ``args`` already
      carries the category / target-date / target-race identity as CLI flags
      (``build_base_argv`` / ``build_layer_argv``), so the same masked argv
      used in the exit-code failure message below also serves as the "which
      race" identity for the timeout log line.
    """
    timeout_seconds = _pipeline_subprocess_timeout_seconds()
    safe_args = [mask_pg_url(arg) for arg in args]
    process = subprocess.Popen(
        list(args),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    stdout_buffer: list[str] = []
    stderr_buffer: list[str] = []
    assert process.stdout is not None
    assert process.stderr is not None
    stream_debug = debug_logs_enabled()
    stdout_thread = threading.Thread(
        target=_capture_stream,
        args=(process.stdout, stdout_buffer),
        kwargs={"sink": sys.stdout if stream_debug else None},
    )
    stderr_thread = threading.Thread(
        target=_capture_stream,
        args=(process.stderr, stderr_buffer),
        kwargs={"sink": sys.stderr if stream_debug else None},
    )
    stdout_thread.start()
    stderr_thread.start()
    try:
        returncode = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        print(
            f"[pipeline] SUBPROCESS TIMEOUT after {timeout_seconds:.0f}s -- "
            f"killing process group: {safe_args}",
            file=sys.stderr,
            flush=True,
        )
        _kill_process_group(process)
        process.wait()  # reap now that the group has been signaled
        stdout_thread.join()
        stderr_thread.join()
        stderr_text = "".join(stderr_buffer)
        stderr_tail = stderr_text[-STDERR_TAIL_BYTES:]
        message = (
            f"subprocess timed out after {timeout_seconds:.0f}s and was killed: {safe_args}\n"
            f"stderr (last {STDERR_TAIL_BYTES} bytes):\n{stderr_tail}"
        )
        raise RuntimeError(message) from None
    stdout_thread.join()
    stderr_thread.join()
    if returncode == 0:
        return
    stderr_text = "".join(stderr_buffer)
    stderr_tail = stderr_text[-STDERR_TAIL_BYTES:]
    message = (
        f"subprocess failed (exit {returncode}): {safe_args}\n"
        f"stderr (last {STDERR_TAIL_BYTES} bytes):\n{stderr_tail}"
    )
    raise RuntimeError(message)


def _final_parquet_dir(category: Category) -> Path:
    return WORK_DIR / f"feat-{category}-v7-final"


def _duckdb_temp_dir(category: Category, target_date: str, target_race: str | None) -> Path:
    target_label = target_race.replace(":", "-") if target_race is not None else "all"
    return WORK_DIR / "duckdb-spill" / f"{category}-{target_date}-{target_label}"


def _day_base_dir(category: Category, target_date: str) -> Path:
    """Return the per-category+day work dir for the DAY_CHAIN-only day-base build.

    Deliberately named ``daybase-{category}-{target_date}`` (NOT
    ``feat-{category}-...``) so it never matches the ``feat-{category}-*``
    glob that :func:`_reset_category_work_dirs` sweeps between sequential
    same-category focused-full races -- see that function's docstring for the
    "excluded by construction" invariant this naming choice guarantees, and
    ``tests/test_pipeline_runner.py::test_reset_category_work_dirs_does_not_delete_day_base_dir``
    for the regression test pinning it.
    """
    return WORK_DIR / f"daybase-{category}-{target_date}"


def _log_pipeline_progress(message: str) -> None:
    if debug_logs_enabled():
        print(f"[pipeline] {message}", file=sys.stderr, flush=True)


def _query_source_rows(
    database_url: str,
    sql: str,
    params: Sequence[object] = (),
) -> list[tuple[object, ...]]:
    """Run a read-only query against the configured feature source.

    Production passes ``r2-catalog://pc-keiba``. The shared feature-pipeline
    adapter attaches the Cloudflare Iceberg REST catalog as ``pg``, preserving
    the existing ``pg.<table>`` SQL contract without opening Postgres. The
    Postgres branch remains available for offline training and parity checks.
    """
    import duckdb

    layer_dir = str(LAYER_DIR)
    if layer_dir not in sys.path:
        sys.path.insert(0, layer_dir)
    from _catalog_attach import attach_source_catalog

    connection = duckdb.connect(":memory:")
    try:
        attach_source_catalog(connection, database_url)
        return connection.execute(sql, list(params)).fetchall()
    finally:
        connection.close()


def _se_table_and_filter(category: Category) -> tuple[str, str]:
    """Return the (table, keibajo_code filter) pair used to scope a
    category's raw entrant/result rows -- mirrors :func:`_query_upcoming_race_keys`'s
    own per-category split so the watermark below reads the identical row
    set that the day-base build itself will read."""
    if category == "jra":
        return "jvd_se", "keibajo_code in ('01','02','03','04','05','06','07','08','09','10')"
    if category == "nar":
        return "nvd_se", "keibajo_code <> '83'"
    return "nvd_se", "keibajo_code = '83'"


def _compute_source_watermark(
    category: Category,
    target_date: str,
    database_url: str,
) -> tuple[str, int] | None:
    """Return ``(max_data_sakusei_nengappi, row_count)`` for category+day's raw
    entrant/result source (``jvd_se`` / ``nvd_se``), or ``None`` on any failure.

    This is the day-base freshness signal: recorded alongside a freshly-built
    day-base (see :func:`build_day_base`) and recomputed fresh on every
    :func:`ensure_day_base` call. A mismatch -- or inability to compute either
    side -- means the underlying source data for this category+day has
    changed since the cached day-base was built (a correction, a late
    scratch, a late add, a backfill), or the freshness signal itself could
    not be verified. Either way the caller must not trust the cache and must
    rebuild -- ``None`` is the fail-closed sentinel throughout this module's
    existing freshness-resolution functions (mirrors
    :func:`day_base_covers_entry_list`'s own "return False on ANY exception"
    contract).

    ``data_sakusei_nengappi`` (JV-Data's per-record "data creation date"
    field) is used rather than a generic "updated_at" column because
    ``jvd_se`` / ``nvd_se`` do not carry one -- this is the closest existing
    per-row freshness signal, confirmed present on both tables by direct
    schema read.

    Deliberately narrow scope: this does NOT attempt to catch staleness in
    the 7 late-binding odds/weight columns :func:`build_day_base`'s own
    docstring already explains are handled by the independent ``mode=rescore``
    freshness layer regardless of this watermark. It exists to catch changes
    to the day's ENTRANT/RESULT source data itself, as a defense-in-depth
    layer alongside (not a replacement for) the entry-list-membership check
    :func:`day_base_covers_entry_list` already runs unconditionally on every
    call to :func:`build_upcoming_feature_rows_split`, regardless of which
    branch produced ``day_base_dir``.
    """
    se_table, keibajo_filter = _se_table_and_filter(category)
    try:
        rows = _query_source_rows(
            database_url,
            f"""
                select max(data_sakusei_nengappi), count(*)
                from pg.{se_table}
                where kaisai_nen = ? and kaisai_tsukihi = ? and {keibajo_filter}
            """,
            [target_date[:4], target_date[4:]],
        )
    except Exception as exc:
        print(
            f"[day-base] watermark query failed category={category} "
            f"target_date={target_date} error={exc}",
            file=sys.stderr,
        )
        return None
    if not rows or not rows[0][1]:
        return None
    max_updated, row_count = rows[0]
    count_value = row_count if isinstance(row_count, int) else int(str(row_count))
    return (str(max_updated).strip() if max_updated is not None else "", count_value)


_RS_PREDICTIONS_R2_PREFIX: Final[str] = "running-style/predictions/by-day/raw-iceberg-v1"
"""Object-key prefix for the by-day running-style prediction shards this
watermark checks for freshness. Must stay in sync with ``R2_PREDICTIONS_PREFIX``
in ``apps/pc-keiba-viewer/src/scripts/finish-position-features/
add-pacestyle-features.py`` -- that script's own R2 read
(``stage_rs_predictions_from_r2``) and this watermark must agree on where the
by-day shards live, or this watermark could report "fresh" while the actual
feature build reads a shard from a different location."""

_RS_WATERMARK_CATEGORIES: Final[frozenset[Category]] = frozenset({"jra", "nar"})
"""Categories with a by-day running-style R2 shard (mirrors
``PACESTYLE_CATEGORY_BY_CATEGORY``'s keys in ``predict_lib.pipeline_args``) --
``ban-ei`` predictions are not split out by day the same way, so its day-base
has no RS freshness signal to check."""

DayBaseWatermark = tuple[str, int, str, int]
"""``(max_data_sakusei_nengappi, entrant_row_count, rs_predicted_at_max,
rs_row_count)`` -- the combined freshness signal :func:`ensure_day_base`
trusts a cached day-base against."""


def _compute_rs_watermark(
    category: Category, target_date: str, r2_config: R2Config | None
) -> tuple[str, int] | None:
    """Freshness signal for the day's running-style predictions in R2.

    Returns ``("", 0)`` -- a STABLE, valid watermark value, never the
    fail-closed ``None`` sentinel -- both when RS predictions don't apply to
    ``category`` (no by-day shard, e.g. ``ban-ei``) and when the day's shard
    simply has not been written yet (a normal, expected state early in the
    day). Either case is a real "no RS yet" state: a day-base built while it
    holds has no RS features to offer, which is correct, and the watermark
    only changes -- forcing a rebuild -- once real predictions with a higher
    row count / later ``predicted_at`` actually appear. This deliberately
    avoids rebuilding on every single call while genuinely waiting for RS to
    land for the day.

    Returns ``None`` (matching :func:`_compute_source_watermark`'s
    fail-closed contract) on any OTHER error -- missing R2 credentials,
    network failure, unexpected schema -- so the caller cannot verify
    freshness and must not trust the cache.
    """
    if category not in _RS_WATERMARK_CATEGORIES:
        return ("", 0)
    if r2_config is None:
        return None
    import duckdb

    yyyy, mm, dd = target_date[:4], target_date[4:6], target_date[6:8]
    glob = (
        f"s3://{r2_config.bucket}/{_RS_PREDICTIONS_R2_PREFIX}/"
        f"{yyyy}/{mm}/{dd}/{category}/*.parquet"
    )
    try:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute("install httpfs; load httpfs;")
            connection.execute(
                f"""
                create or replace secret r2_secret (
                  type s3,
                  key_id '{r2_config.access_key_id}',
                  secret '{r2_config.secret_access_key}',
                  endpoint '{r2_config.account_id}.r2.cloudflarestorage.com',
                  region 'auto',
                  url_style 'path'
                )
                """
            )
            row = connection.execute(
                f"select max(predicted_at), count(*) from read_parquet('{glob}')"
            ).fetchone()
        finally:
            connection.close()
    except duckdb.IOException as exc:
        if "No files found that match" not in str(exc):
            print(
                f"[day-base] rs watermark query failed category={category} "
                f"target_date={target_date} error={exc}",
                file=sys.stderr,
            )
            return None
        return ("", 0)
    except Exception as exc:
        print(
            f"[day-base] rs watermark setup failed category={category} "
            f"target_date={target_date} error={exc}",
            file=sys.stderr,
        )
        return None
    if not row or not row[1]:
        return ("", 0)
    max_predicted_at, row_count = row
    count_value = row_count if isinstance(row_count, int) else int(str(row_count))
    predicted_at_value = str(max_predicted_at).strip() if max_predicted_at is not None else ""
    return (predicted_at_value, count_value)


def _combine_watermarks(
    entrant: tuple[str, int] | None, rs: tuple[str, int] | None
) -> DayBaseWatermark | None:
    """Merge the entrant + RS freshness signals into one comparable value.

    ``None`` propagates from either side -- if EITHER signal cannot be
    verified, the combined watermark is unverifiable too, matching both
    components' own fail-closed contract.
    """
    if entrant is None or rs is None:
        return None
    return (entrant[0], entrant[1], rs[0], rs[1])


def _watermark_path(day_dir: Path) -> Path:
    return day_dir / "watermark.json"


def _write_watermark(day_dir: Path, watermark: DayBaseWatermark) -> None:
    payload = {
        "max_data_sakusei_nengappi": watermark[0],
        "row_count": watermark[1],
        "rs_predicted_at_max": watermark[2],
        "rs_row_count": watermark[3],
    }
    try:
        _watermark_path(day_dir).write_text(json.dumps(payload), encoding="utf-8")
    except OSError as exc:
        # Best-effort: a write failure here must not fail the day-base build
        # itself -- it just means the NEXT ensure_day_base call for this
        # category+day will find no watermark file, treat it as a mismatch,
        # and safely rebuild (fail-closed), same outcome as today.
        print(f"[day-base] watermark write failed dir={day_dir} error={exc}", file=sys.stderr)


def _read_watermark(day_dir: Path) -> DayBaseWatermark | None:
    try:
        payload = json.loads(_watermark_path(day_dir).read_text(encoding="utf-8"))
        return (
            str(payload["max_data_sakusei_nengappi"]),
            int(payload["row_count"]),
            str(payload["rs_predicted_at_max"]),
            int(payload["rs_row_count"]),
        )
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _query_upcoming_race_keys(
    database_url: str,
    target_date: str,
    days_ahead: int,
    category: Category,
    target_race: str | None = None,
) -> list[tuple[str, str]]:
    """Query upcoming race keys from the configured Iceberg feature source.

    Used to drive the per-race realtime-odds fetch so only races that will be
    predicted receive a GET request. Returns an empty list on any error so the
    caller falls back to the NULL-odds path gracefully.

    The query reads ``nvd_se`` / ``jvd_se`` for the target window and returns
    DISTINCT (keibajo_code, race_bango) pairs whose ``kakutei_chakujun`` is
    blank (UPCOMING). The DuckDB feature build derives ``finish_position`` from
    the same tables so the race set is consistent.
    """
    from datetime import UTC, datetime, timedelta

    try:
        from_dt = datetime.strptime(target_date, "%Y%m%d").replace(tzinfo=UTC)
        to_dt = from_dt + timedelta(days=days_ahead)
        target_from = target_date
        target_to = to_dt.strftime("%Y%m%d")
        if category == "jra":
            se_table = "jvd_se"
            keibajo_filter = "keibajo_code in ('01','02','03','04','05','06','07','08','09','10')"
        elif category == "nar":
            se_table = "nvd_se"
            keibajo_filter = "keibajo_code <> '83'"
        else:
            se_table = "nvd_se"
            keibajo_filter = "keibajo_code = '83'"
        target_race_filter = ""
        params: list[object] = [target_from, target_to]
        if target_race is not None:
            keibajo_code, race_bango = target_race.split(":", 1)
            target_race_filter = "and keibajo_code = ? and race_bango = ?"
            params.extend((keibajo_code, race_bango))

        sql = f"""
            select distinct keibajo_code, race_bango
            from pg.{se_table}
            where kaisai_nen between '{target_from[:4]}' and '{target_to[:4]}'
              and (kaisai_nen || kaisai_tsukihi) between ? and ?
              and {keibajo_filter}
              {target_race_filter}
              and ketto_toroku_bango is not null
              and (kakutei_chakujun is null or trim(kakutei_chakujun) in ('', '00'))
            order by keibajo_code, race_bango
        """
        rows = _query_source_rows(database_url, sql, params)
        return [(str(r[0]).strip(), str(r[1]).strip()) for r in rows if r[0] and r[1]]
    except Exception as exc:
        print(
            f"[realtime-odds] race-key query failed category={category} error={exc}",
            file=sys.stderr,
        )
        return []


def build_upcoming_feature_rows(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    target_race: str | None = None,
) -> Mapping[str, list[Mapping[str, object]]]:
    """Run the pipeline and return ``race_id`` -> ordered entry feature dicts.

    Returns an empty mapping when the base build emits zero target rows (e.g.
    JRA on a NAR-only weekday). In that case the per-category layer chain is
    skipped — there is nothing to score — and the caller continues with the
    next category without raising.

    Presence guard (2026-07-17, latency profile task): when ``target_race``
    is set, :func:`_query_upcoming_race_keys` already runs first (originally
    added only to scope the realtime-odds fetch) against the SAME
    ``jvd_se``/``nvd_se`` source tables and the SAME target-race/unsettled
    filter the DuckDB base build itself applies. If it comes back empty for
    that one race, the base build is guaranteed to also find zero target rows
    for it, which already makes this function return ``{}`` below (the
    existing ``if not built: return {}`` path, unchanged) — but only after
    paying the full ~2-9 minute base build + full layer chain first. Skipping
    straight to that same ``{}`` return here changes only *when* the
    already-existing empty-mapping outcome is reached for this one case,
    never *whether* it is reached, so it carries no accuracy risk: it cannot
    cause a race that would otherwise have been scored to go unscored, and it
    reuses a query that already ran on every call before this change — no new
    I/O is added. Deliberately scoped to ``target_race is not None`` only
    (the per-race latency this was written to address); a whole-day/
    whole-category call (``target_race is None``) is unchanged.

    A realtime-odds fetch is attempted before the base build; on failure (HTTP
    error, timeout, empty response) the fetch is skipped gracefully and the
    build falls back to the existing NULL-odds path so the prediction always
    completes even when the hot worker is unavailable. A venue-weather fetch is
    attempted alongside it (materialized as a DuckDB sidecar directory) and
    falls back to the NULL-weather path with the same graceful semantics when
    the venue-weather worker is unavailable.
    """
    import pandas as pd

    from realtime_odds_fetcher import fetch_realtime_odds_parquet  # bundled in image
    from weather_fetcher import fetch_venue_weather_dir  # bundled in image

    final_dir = _final_parquet_dir(category)
    race_keys = _query_upcoming_race_keys(
        database_url, target_date, days_ahead, category, target_race
    )
    if target_race is not None and not race_keys:
        print(
            f"[pipeline] presence-guard: target_race={target_race} category={category} "
            "has zero upcoming rows in source catalog -> skipping feature build",
            file=sys.stderr,
        )
        return {}
    realtime_odds_path = fetch_realtime_odds_parquet(category, target_date, WORK_DIR, race_keys)
    venue_weather_dir = fetch_venue_weather_dir(target_date, WORK_DIR)
    built = build_pipeline(
        category,
        target_date,
        days_ahead,
        database_url,
        final_dir,
        realtime_odds_path,
        venue_weather_dir,
        target_race,
    )
    if not built:
        return {}
    frame = pd.read_parquet(final_dir)
    return _group_parquet_rows(frame, target_race=target_race)


def has_parquet_output(directory: Path) -> bool:
    """True when ``directory`` contains at least one ``.parquet`` file.

    The DuckDB base build writes partitioned output (``race_year=YYYY/*.parquet``)
    when target rows exist, and an empty directory when ``--allow-empty-targets``
    is set and the target window has no races. The layer chain expects at least
    one parquet file, so we treat a parquet-less directory as "no work to do".
    """
    if not directory.exists():
        return False
    return any(directory.rglob("*.parquet"))


def _reset_category_work_dirs(category: Category, final_dir: Path) -> None:
    """Remove the CATEGORY-scoped work dirs left by a prior race in this process.

    ``build_pipeline`` writes intermediate feature parquet to work directories
    keyed by ``category`` only -- the base build dir (``feat-{category}-base``),
    each layer dir (``feat-{category}-layer-{index}``), and the final dir
    (``final_dir`` == ``feat-{category}-v7-final``) -- and finishes with
    ``current.rename(final_dir)``.

    The focused-full single-slot dispatch now runs multiple same-category races
    SEQUENTIALLY in one long-lived container process. Without this reset the
    2nd+ race would find ``final_dir`` (and the base / layer dirs) already
    populated by the 1st race, so ``current.rename(final_dir)`` fails with
    ``OSError`` (ENOTEMPTY) AFTER every layer already logged "done" -- the
    pipeline completes with no scoring and no Neon write (the production
    write-gap this fixes). Clearing these dirs at the start of each race makes
    the post-layer rename land on a clean target every time.

    The race-scoped ``duckdb-spill`` dir is intentionally NOT removed here: it
    is already keyed per race (category + target_date + target_race) and cleaned
    separately, so it never collides across sequential same-category races.

    The per-category+day ``daybase-{category}-{target_date}`` dir (see
    :func:`_day_base_dir`) is ALSO intentionally excluded from this sweep --
    by construction, not by an extra guard clause: its name does not match the
    ``feat-{category}-*`` glob below, so a stale-work-dir reset for a focused
    per-race full request never evicts the day-base cache that
    :func:`ensure_day_base` / :func:`build_pipeline_from_day_base` rely on for
    every OTHER race of the same category+day.

    ``shutil.rmtree(..., ignore_errors=True)`` makes a missing dir (the first
    race in a process) a no-op.
    """
    shutil.rmtree(WORK_DIR / f"feat-{category}-base", ignore_errors=True)
    shutil.rmtree(final_dir, ignore_errors=True)
    for layer_dir in WORK_DIR.glob(f"feat-{category}-layer-*"):
        shutil.rmtree(layer_dir, ignore_errors=True)


def build_pipeline(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    final_dir: Path,
    realtime_odds_path: Path | None = None,
    venue_weather_dir: Path | None = None,
    target_race: str | None = None,
) -> bool:
    """Run the DuckDB base build then each v7 layer into ``final_dir``.

    Returns ``True`` when a populated ``final_dir`` was produced, ``False`` when
    the base build emitted zero target rows (in which case the layer chain is
    skipped because layer scripts cannot read an empty parquet directory).

    When ``realtime_odds_path`` is provided it is forwarded to the DuckDB base
    build via ``--realtime-odds`` so real-time tansho odds from the hot worker
    flow into ``odds_score`` / ``popularity_score``. When ``venue_weather_dir``
    is provided it is forwarded via ``--venue-weather-dir`` so the per-year
    ``venue_weather_{year}.duckdb`` files supply hourly weather features.

    When ``target_race`` (``keibajo_code:race_bango``) is provided it is
    forwarded to the base build via ``--target-race`` so only that single race
    is built instead of every race on ``target_date``.
    """
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    # Reset the category-scoped work dirs left by any prior race in this
    # long-lived process (sequential same-category focused-full races) so the
    # post-layer ``current.rename(final_dir)`` lands on a clean target instead
    # of failing with ENOTEMPTY. See ``_reset_category_work_dirs``.
    _reset_category_work_dirs(category, final_dir)
    base_dir = WORK_DIR / f"feat-{category}-base"
    duckdb_temp_dir = _duckdb_temp_dir(category, target_date, target_race)
    duckdb_temp_dir.mkdir(parents=True, exist_ok=True)
    target_label = target_race if target_race is not None else "all"
    chain = layer_chain_for(category)
    # TEMPORARY (2026-07-02): unique id per /predict invocation of build_pipeline
    # so debug-timing rows for this run can be grouped/ordered in Neon. See
    # DEBUG_LAYER_TIMING_TABLE comment above.
    run_id = f"{category}:{target_date}:{target_label}:{uuid.uuid4().hex[:8]}"
    base_start = perf_counter()
    _log_pipeline_progress(
        f"step=base index=0 status=start category={category} "
        f"target_date={target_date} days_ahead={days_ahead} "
        f"target_race={target_label} elapsed_seconds=0.000"
    )
    try:
        run_with_stderr_capture(
            build_base_argv(
                DUCKDB_BUILDER,
                category,
                target_date,
                days_ahead,
                database_url,
                base_dir,
                realtime_odds_path,
                venue_weather_dir,
                target_race,
                temp_dir=duckdb_temp_dir,
            )
        )
    except Exception:
        base_elapsed = perf_counter() - base_start
        _log_pipeline_progress(
            f"step=base index=0 status=failed category={category} "
            f"target_race={target_label} elapsed_seconds={base_elapsed:.3f}"
        )
        record_layer_timing_row(
            database_url,
            run_id,
            category,
            target_date,
            target_race,
            0,
            len(chain),
            "__base_build__",
            "failed",
            base_elapsed,
            base_elapsed,
        )
        raise
    base_elapsed = perf_counter() - base_start
    _log_pipeline_progress(
        f"step=base index=0 status=done category={category} "
        f"target_race={target_label} elapsed_seconds={base_elapsed:.3f}"
    )
    record_layer_timing_row(
        database_url,
        run_id,
        category,
        target_date,
        target_race,
        0,
        len(chain),
        "__base_build__",
        "done",
        base_elapsed,
        base_elapsed,
    )
    if not has_parquet_output(base_dir):
        _log_pipeline_progress(
            f"step=layers index=0 status=skipped category={category} "
            f"target_race={target_label} reason=no-parquet elapsed_seconds=0.000"
        )
        return False
    current = base_dir
    for index, script in enumerate(chain):
        nxt = WORK_DIR / f"feat-{category}-layer-{index}"
        layer_start = perf_counter()
        _log_pipeline_progress(
            f"step=layer index={index + 1}/{len(chain)} status=start "
            f"category={category} script={script} target_race={target_label} "
            f"elapsed_seconds=0.000"
        )
        try:
            run_with_stderr_capture(
                build_layer_argv(
                    script,
                    category,
                    LAYER_DIR,
                    current,
                    nxt,
                    database_url,
                    target_date=target_date,
                    target_race=target_race,
                )
            )
        except Exception:
            layer_elapsed = perf_counter() - layer_start
            _log_pipeline_progress(
                f"step=layer index={index + 1}/{len(chain)} status=failed "
                f"category={category} script={script} target_race={target_label} "
                f"elapsed_seconds={layer_elapsed:.3f}"
            )
            record_layer_timing_row(
                database_url,
                run_id,
                category,
                target_date,
                target_race,
                index + 1,
                len(chain),
                script,
                "failed",
                layer_elapsed,
                perf_counter() - base_start,
            )
            raise
        layer_elapsed = perf_counter() - layer_start
        _log_pipeline_progress(
            f"step=layer index={index + 1}/{len(chain)} status=done "
            f"category={category} script={script} target_race={target_label} "
            f"elapsed_seconds={layer_elapsed:.3f}"
        )
        record_layer_timing_row(
            database_url,
            run_id,
            category,
            target_date,
            target_race,
            index + 1,
            len(chain),
            script,
            "done",
            layer_elapsed,
            perf_counter() - base_start,
        )
        current = nxt
    current.rename(final_dir)
    _log_pipeline_progress(
        f"done pipeline category={category} target_race={target_label} output={final_dir}"
    )
    return True


# ---------------------------------------------------------------------------
# DAY_CHAIN / RACE_CHAIN split (per-race rebuild speedup)
#
# docs/cf-only-serving-architecture.md §2-3 +
# docs/finish-position-cloudflare-container/08-per-race-rebuild-plan.md.
# ---------------------------------------------------------------------------

# race_id format emitted by the DuckDB base build:
# "source:kaisai_nen:kaisai_tsukihi:keibajo_code:race_bango" (see
# finish_position_features_duckdb.py). ``day_base_covers_entry_list`` matches
# the day-base's own rows for one race by splitting this string rather than
# reconstructing it (mirrors ``predict_upcoming._split_parquet_by_race``'s
# identical split-based matching -- the constants are duplicated rather than
# imported because ``predict_upcoming`` already imports FROM this module and
# a reverse import would be circular).
_RACE_ID_KEIBAJO_SPLIT_PART: Final[int] = 4  # DuckDB split_part is 1-indexed
_RACE_ID_BANGO_SPLIT_PART: Final[int] = 5


def build_day_base(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    realtime_odds_path: Path | None = None,
    venue_weather_dir: Path | None = None,
    r2_config: R2Config | None = None,
) -> Path | None:
    """Run the DuckDB base build + DAY_CHAIN layers once per category+day.

    Returns the day-base's ``final`` parquet directory on success, or ``None``
    when the base build emits zero target rows for ``target_date`` (mirrors
    :func:`build_pipeline`'s ``has_parquet_output`` contract).

    Why the base build belongs HERE (day-base, whole-day scope, no
    ``--target-race``) and not in the per-race ``RACE_CHAIN`` path
    -------------------------------------------------------------------
    The DuckDB base build (``finish_position_features_duckdb.py``) emits a
    handful of columns straight off the target row: ``tansho_odds``,
    ``tansho_ninkijun``, ``popularity_score``, ``odds_score``,
    ``current_bataiju``, ``target_zogen_sa``, and
    ``babajotai_code_shiba`` / ``babajotai_code_dirt``. All seven are
    RACE-FRESH in principle -- odds and weight move up to post time -- so it
    is reasonable to ask whether freezing them at day-base build time
    (~09:30 JST, once per category+day) rather than re-fetching them per race
    silently stales the served vector. It does not, for three independently
    sufficient reasons:

    1. Verified via the actual SQL, not assumed: none of these seven columns
       are RE-FETCHED by any ``RACE_CHAIN`` layer script. For example,
       ``add-baba-pedigree-affinity-features.py``'s ``current_baba_condition``
       (~line 331) is `rb.baba_cond` sourced from the INPUT PARQUET's own
       ``babajotai_code_shiba`` / ``_dirt`` columns (a CTE over the base
       build's own output) -- not an independent live PG query. So whether the
       base build ran at day-base time or at old per-race time, the RACE_CHAIN
       layers consume whatever the base build already wrote; splitting the
       base build out of the per-race path changes WHEN these 7 columns are
       computed, not WHAT downstream layers see.
    2. The already-deployed ``mode=rescore`` path
       (``predict_lib.rescore.apply_fresh_snapshots``, wired in
       ``predict_upcoming._make_rescore_fn``) already re-fetches live odds /
       weight and recomputes exactly these late-binding columns close to race
       post time, INDEPENDENT of and unaffected by this split. It is a
       separate freshness layer that runs on TOP of whatever
       ``mode=full`` / day-base produced, both before and after this change.
    3. For the mandatory byte-parity acceptance test
       (``tests/test_day_base_parity.py``) against HISTORICAL dates, this is a
       non-issue: for a past, already-settled race, ``tansho_odds`` /
       ``tansho_ninkijun`` / ``babajotai_code_*`` / ``bataiju`` in
       ``jvd_se`` / ``nvd_se`` are FIXED (no live odds movement), so the OLD
       (full rebuild) and NEW (day-base + race-chain) paths read identical
       values regardless of when each subprocess ran within the same test.

    This also exactly matches the reference architecture this design ports
    from: the Mac-batch pipeline builds its base once per day and relies on a
    separate freshness layer for late-binding columns, the same shape as here.

    ``realtime_odds_path`` / ``venue_weather_dir`` are forwarded to the base
    build exactly as :func:`build_pipeline` forwards them -- when the caller
    (the ``/prewarm-day-base`` HTTP path) chooses to fetch them before this
    call, the day-base's frozen odds/weather snapshot reflects that fetch;
    when omitted (``None``), the base build falls back to its existing
    NULL-odds / NULL-weather path unchanged, same as always.
    """
    day_dir = _day_base_dir(category, target_date)
    # A retried/failed prior day-base build for the SAME category+day must not
    # leave partial base/layer dirs behind -- reset unconditionally before
    # building (mirrors _reset_category_work_dirs's rationale for the
    # per-race path, scoped to this day-base's own isolated dir tree).
    shutil.rmtree(day_dir, ignore_errors=True)
    day_dir.mkdir(parents=True, exist_ok=True)
    base_dir = day_dir / "base"
    duckdb_temp_dir = day_dir / "duckdb-spill"
    duckdb_temp_dir.mkdir(parents=True, exist_ok=True)
    chain = day_chain_for(category)
    run_id = f"{category}:{target_date}:daybase:{uuid.uuid4().hex[:8]}"
    base_start = perf_counter()
    _log_pipeline_progress(
        f"step=daybase-base index=0 status=start category={category} "
        f"target_date={target_date} days_ahead={days_ahead} elapsed_seconds=0.000"
    )
    try:
        run_with_stderr_capture(
            build_base_argv(
                DUCKDB_BUILDER,
                category,
                target_date,
                days_ahead,
                database_url,
                base_dir,
                realtime_odds_path,
                venue_weather_dir,
                None,  # target_race: whole-day scope -- every race of the day
                temp_dir=duckdb_temp_dir,
            )
        )
    except Exception:
        base_elapsed = perf_counter() - base_start
        _log_pipeline_progress(
            f"step=daybase-base index=0 status=failed category={category} "
            f"elapsed_seconds={base_elapsed:.3f}"
        )
        record_layer_timing_row(
            database_url,
            run_id,
            category,
            target_date,
            None,
            0,
            len(chain),
            "__daybase_base__",
            "failed",
            base_elapsed,
            base_elapsed,
        )
        raise
    base_elapsed = perf_counter() - base_start
    _log_pipeline_progress(
        f"step=daybase-base index=0 status=done category={category} "
        f"elapsed_seconds={base_elapsed:.3f}"
    )
    record_layer_timing_row(
        database_url,
        run_id,
        category,
        target_date,
        None,
        0,
        len(chain),
        "__daybase_base__",
        "done",
        base_elapsed,
        base_elapsed,
    )
    if not has_parquet_output(base_dir):
        _log_pipeline_progress(
            f"step=daybase-layers index=0 status=skipped category={category} "
            f"reason=no-parquet elapsed_seconds=0.000"
        )
        return None
    current = base_dir
    for index, script in enumerate(chain):
        nxt = day_dir / f"layer-{index}"
        layer_start = perf_counter()
        _log_pipeline_progress(
            f"step=daybase-layer index={index + 1}/{len(chain)} status=start "
            f"category={category} script={script} elapsed_seconds=0.000"
        )
        try:
            run_with_stderr_capture(
                build_layer_argv(
                    script,
                    category,
                    LAYER_DIR,
                    current,
                    nxt,
                    database_url,
                    target_date=target_date,
                    target_race=None,
                )
            )
        except Exception:
            layer_elapsed = perf_counter() - layer_start
            _log_pipeline_progress(
                f"step=daybase-layer index={index + 1}/{len(chain)} status=failed "
                f"category={category} script={script} elapsed_seconds={layer_elapsed:.3f}"
            )
            record_layer_timing_row(
                database_url,
                run_id,
                category,
                target_date,
                None,
                index + 1,
                len(chain),
                script,
                "failed",
                layer_elapsed,
                perf_counter() - base_start,
            )
            raise
        layer_elapsed = perf_counter() - layer_start
        _log_pipeline_progress(
            f"step=daybase-layer index={index + 1}/{len(chain)} status=done "
            f"category={category} script={script} elapsed_seconds={layer_elapsed:.3f}"
        )
        record_layer_timing_row(
            database_url,
            run_id,
            category,
            target_date,
            None,
            index + 1,
            len(chain),
            script,
            "done",
            layer_elapsed,
            perf_counter() - base_start,
        )
        current = nxt
    final_dir = day_dir / "final"
    shutil.rmtree(final_dir, ignore_errors=True)
    current.rename(final_dir)
    if is_catalog_source_url(database_url):
        # Record the freshness signal THIS build's source data had at
        # completion time, so a later ensure_day_base call (same process,
        # next race of the day, OR a different shard reading this build's
        # R2 upload -- task #32) can verify the source hasn't changed before
        # trusting this day-base instead of rebuilding. Best-effort: a
        # failed watermark compute/write here degrades the NEXT call to
        # "no watermark found" (safe fail-closed rebuild), never this
        # build's own success.
        watermark = _combine_watermarks(
            _compute_source_watermark(category, target_date, database_url),
            _compute_rs_watermark(category, target_date, r2_config),
        )
        if watermark is not None:
            _write_watermark(day_dir, watermark)
    _log_pipeline_progress(
        f"done daybase category={category} target_date={target_date} output={final_dir}"
    )
    return final_dir


def ensure_day_base(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    r2_config: R2Config | None,
) -> Path | None:
    """Resolve the cached day-base parquet dir for category+day, or ``None``.

    Never raises and never builds -- pure freshness/fallback resolution
    mirroring ``weather_fetcher.fetch_venue_weather_dir``'s
    fetch/materialize/fallback shape:

    1. Catalog source watermark gate -- production ``r2-catalog://`` reads
       first try a LOCAL-DISK day-base (this same container process already
       built it for an earlier race of the same category+day) when a FRESH
       watermark (:func:`_compute_source_watermark`: max
       ``data_sakusei_nengappi`` + row count of the day's raw ``jvd_se`` /
       ``nvd_se`` rows) matches the watermark :func:`build_day_base` wrote
       alongside it. A watermark that cannot be computed means the caller
       must not trust ANY cache (local or R2) and rebuilds from current raw
       Catalog data -- this is a REPLACEMENT for the previous unconditional
       bypass (every catalog-source call always returned ``None`` here,
       forcing a full day-base rebuild for literally every race), not a
       relaxation of its fail-closed contract: any ambiguity still returns
       ``None``.

       R2 cross-process reuse (task #32, 2026-07-19: a day-base built by a
       DIFFERENT container process -- another ``RACE_SHARDED_DO`` shard --
       or by the ``/prewarm-day-base`` job) is attempted NEXT, when the
       local-disk check misses and a watermark WAS computable: a cheap
       signed HEAD (:func:`predict_lib.r2_client.r2_head_watermark`) checks
       the R2 object's custom metadata against the freshly-computed
       watermark BEFORE paying for the full parquet GET. A mismatch,
       missing metadata (e.g. an object written before this metadata
       existed), or any HEAD failure all resolve to "untrusted" -- fall
       through to the existing ``return None`` unchanged, same fail-closed
       contract as every other branch here. This lets a shard's first race
       skip the day-base build entirely when the 09:30 JST prewarm already
       populated R2 for this category+day, instead of every shard paying
       the full rebuild cost on its own first race.
    2. Offline local disk fast path -- if this container process already built (or
       downloaded) the day-base for this category+day, return it immediately.
       This is the common case for the 2nd+ race of the day served by the same
       long-lived container process.
    3. Offline R2 fast path -- when ``r2_config`` is provided, GET
       ``build_r2_day_base_key(category, target_date)`` (the prewarm job's
       upload target) into the local day-base dir and return it on success.
       Unconditional (no watermark check) -- this branch only runs for
       NON-catalog (offline/Postgres) sources, which never exercise the
       live-staleness concern the catalog-source watermark gate above
       guards against.
    4. Otherwise return ``None`` -- the caller decides whether to build the
       day-base synchronously via :func:`build_day_base` or fall back to the
       full :func:`build_pipeline` / ``LAYER_CHAIN`` path for this race. This
       function itself never blocks on a multi-minute build.
    """
    day_dir = _day_base_dir(category, target_date)
    final_dir = day_dir / "final"
    if is_catalog_source_url(database_url):
        watermark = _combine_watermarks(
            _compute_source_watermark(category, target_date, database_url),
            _compute_rs_watermark(category, target_date, r2_config),
        )
        if watermark is None:
            return None
        if has_parquet_output(final_dir) and _read_watermark(day_dir) == watermark:
            return final_dir
        if r2_config is not None:
            object_key = build_r2_day_base_key(category, target_date)
            try:
                r2_watermark = r2_head_watermark(r2_config, object_key)
                if r2_watermark == watermark:
                    dest_path = final_dir / "features.parquet"
                    if r2_get_parquet(r2_config, object_key, dest_path):
                        return final_dir
            except Exception as exc:
                print(
                    f"[day-base] ensure_day_base r2 watermark fetch failed category={category} "
                    f"target_date={target_date} key={object_key} error={exc}",
                    file=sys.stderr,
                )
        return None
    if has_parquet_output(final_dir):
        return final_dir
    if r2_config is None:
        return None
    object_key = build_r2_day_base_key(category, target_date)
    dest_path = final_dir / "features.parquet"
    try:
        if r2_get_parquet(r2_config, object_key, dest_path):
            return final_dir
    except Exception as exc:
        print(
            f"[day-base] ensure_day_base r2 fetch failed category={category} "
            f"target_date={target_date} key={object_key} error={exc}",
            file=sys.stderr,
        )
    return None


def day_base_covers_entry_list(
    day_base_dir: Path,
    category: Category,
    target_date: str,
    target_race: str,
    database_url: str,
) -> bool:
    """True when every CURRENT entrant of ``target_race`` appears in the day-base.

    Guards against entry-list drift between the day-base build (prewarm,
    ~09:30 JST) and a later per-race request: a late scratch or a late add
    changes the CURRENT entry list in Postgres without changing the already
    frozen day-base parquet. When any current ``ketto_toroku_bango`` from
    ``jvd_se`` / ``nvd_se`` is missing from the day-base's rows for this race,
    the day-base is stale for this race.

    ``target_race`` is ``"keibajo_code:race_bango"``. The day-base's own rows
    for the race are matched by splitting its ``race_id`` column
    (``source:kaisai_nen:kaisai_tsukihi:keibajo_code:race_bango``) rather than
    reconstructing the full ``race_id`` string, since the exact
    ``source`` / ``kaisai_nen`` / ``kaisai_tsukihi`` tokens the day-base build
    used are not assumed here.

    Returns ``False`` on ANY exception (missing dir, PG error, malformed
    parquet, ...) -- fail toward the safe fallback (a fresh build / the full
    ``LAYER_CHAIN`` path), never toward trusting a possibly-stale cache.
    """
    try:
        import duckdb

        keibajo_code, race_bango = target_race.split(":", 1)
        se_table = "jvd_se" if category == "jra" else "nvd_se"
        source_rows = _query_source_rows(
            database_url,
            f"""
            select distinct ketto_toroku_bango
            from pg.{se_table}
            where kaisai_nen = ? and kaisai_tsukihi = ?
              and keibajo_code = ? and race_bango = ?
              and ketto_toroku_bango is not null
            """,
            (target_date[:4], target_date[4:], keibajo_code, race_bango),
        )
        current_horses = {str(row[0]).strip() for row in source_rows if row[0]}
        if not current_horses:
            return False
        glob_path = str(day_base_dir / "**" / "*.parquet")
        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                "SELECT DISTINCT ketto_toroku_bango FROM "
                "read_parquet(?, hive_partitioning = false) "
                "WHERE split_part(race_id, ':', ?) = ? AND split_part(race_id, ':', ?) = ?",
                [
                    glob_path,
                    _RACE_ID_KEIBAJO_SPLIT_PART,
                    keibajo_code,
                    _RACE_ID_BANGO_SPLIT_PART,
                    race_bango,
                ],
            ).fetchall()
        finally:
            con.close()
        day_base_horses = {str(row[0]).strip() for row in rows if row[0]}
        return current_horses.issubset(day_base_horses)
    except Exception as exc:
        print(
            f"[day-base] entry-list drift check failed target_race={target_race} error={exc}",
            file=sys.stderr,
        )
        return False


def build_pipeline_from_day_base(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    day_base_dir: Path,
    final_dir: Path,
    target_race: str,
    realtime_odds_path: Path | None = None,
    venue_weather_dir: Path | None = None,
) -> bool:
    """Run only the RACE_CHAIN layers against a pre-built day-base, into ``final_dir``.

    Mirrors :func:`build_pipeline`'s per-layer loop (including the timing
    instrumentation) but SKIPS the base build entirely: ``current`` starts at
    ``day_base_dir`` and only :func:`predict_lib.pipeline_args.race_chain_for`
    runs, with ``target_race`` forwarded to every RACE_CHAIN script's
    ``build_layer_argv`` call (all RACE_CHAIN scripts already declare
    ``SCRIPTS_WITH_TARGET_RACE_SCOPE`` support). The final layer's output is
    renamed into ``final_dir`` using the SAME ``feat-{category}-v7-final``
    contract :func:`build_pipeline` already uses, so
    ``predict_upcoming.py``'s downstream scoring / R2-payload code needs ZERO
    changes to consume either path's output.

    ``days_ahead`` / ``realtime_odds_path`` / ``venue_weather_dir`` are
    accepted for signature parity with :func:`build_pipeline` /
    :func:`build_day_base` (the top-level orchestrator
    :func:`build_upcoming_feature_rows_split` forwards the same argument set
    to every stage) but are not currently consumed here: no RACE_CHAIN script
    declares a ``--realtime-odds`` / ``--venue-weather-dir`` flag (only the
    base build does, and this function never runs a base build), and the
    RACE_CHAIN loop needs no day-window bound beyond ``target_race`` itself.
    Kept in the signature so a future RACE_CHAIN script that DOES need
    race-time-fresh odds/weather can be wired without changing every caller.
    """
    del days_ahead, realtime_odds_path, venue_weather_dir
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    _reset_category_work_dirs(category, final_dir)
    chain = race_chain_for(category)
    run_id = f"{category}:{target_date}:{target_race}:racechain:{uuid.uuid4().hex[:8]}"
    run_start = perf_counter()
    current = day_base_dir
    for index, script in enumerate(chain):
        nxt = WORK_DIR / f"feat-{category}-layer-{index}"
        layer_start = perf_counter()
        _log_pipeline_progress(
            f"step=racechain-layer index={index + 1}/{len(chain)} status=start "
            f"category={category} script={script} target_race={target_race} "
            f"elapsed_seconds=0.000"
        )
        try:
            run_with_stderr_capture(
                build_layer_argv(
                    script,
                    category,
                    LAYER_DIR,
                    current,
                    nxt,
                    database_url,
                    target_date=target_date,
                    target_race=target_race,
                )
            )
        except Exception:
            layer_elapsed = perf_counter() - layer_start
            _log_pipeline_progress(
                f"step=racechain-layer index={index + 1}/{len(chain)} status=failed "
                f"category={category} script={script} target_race={target_race} "
                f"elapsed_seconds={layer_elapsed:.3f}"
            )
            record_layer_timing_row(
                database_url,
                run_id,
                category,
                target_date,
                target_race,
                index + 1,
                len(chain),
                script,
                "failed",
                layer_elapsed,
                perf_counter() - run_start,
            )
            raise
        layer_elapsed = perf_counter() - layer_start
        _log_pipeline_progress(
            f"step=racechain-layer index={index + 1}/{len(chain)} status=done "
            f"category={category} script={script} target_race={target_race} "
            f"elapsed_seconds={layer_elapsed:.3f}"
        )
        record_layer_timing_row(
            database_url,
            run_id,
            category,
            target_date,
            target_race,
            index + 1,
            len(chain),
            script,
            "done",
            layer_elapsed,
            perf_counter() - run_start,
        )
        current = nxt
    current.rename(final_dir)
    _log_pipeline_progress(
        f"done racechain category={category} target_race={target_race} output={final_dir}"
    )
    return True


def build_upcoming_feature_rows_split(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
    target_race: str,
    r2_config: R2Config | None = None,
) -> Mapping[str, list[Mapping[str, object]]] | None:
    """Focused per-race build via the day-base + RACE_CHAIN split path.

    Returns ``None`` on ANY gap -- day-base unavailable and could not be built
    inline, entry-list drift, or any exception -- so the caller falls back to
    the existing, unmodified :func:`build_upcoming_feature_rows` (full
    ``LAYER_CHAIN``) for this race. This preserves the mandatory
    fail-safe / never-fail-closed contract: a day-base problem degrades to the
    slower-but-correct full path rather than serving a stale or incomplete
    feature vector.

    ``target_race`` (``"keibajo_code:race_bango"``) is REQUIRED here -- this
    function is for the focused per-race full path
    (``predict_lib.serve.is_focused_full_request``) only, never for
    whole-category / whole-day dispatch.

    Resolution order: :func:`ensure_day_base` (local-disk / R2 fast paths)
    first; if that returns ``None``, build the day-base synchronously via
    :func:`build_day_base` (slow, but still saves work for the NEXT race of
    the same category+day via the day-base's on-disk cache). Realtime-odds /
    venue-weather are intentionally NOT fetched on this path (``None`` /
    ``None``) -- the day-base is expected to be pre-warmed once per day via
    ``GET /prewarm-day-base`` before genuine odds volatility begins, and the
    already-deployed ``mode=rescore`` path is the freshness layer for
    late-binding odds/weight columns (see :func:`build_day_base`'s docstring).
    """
    try:
        day_base_dir = ensure_day_base(category, target_date, days_ahead, database_url, r2_config)
        if day_base_dir is None:
            day_base_dir = build_day_base(
                category,
                target_date,
                days_ahead,
                database_url,
                r2_config=r2_config,
            )
        if day_base_dir is None:
            return None
        if not day_base_covers_entry_list(
            day_base_dir, category, target_date, target_race, database_url
        ):
            return None
        final_dir = _final_parquet_dir(category)
        built = build_pipeline_from_day_base(
            category,
            target_date,
            days_ahead,
            database_url,
            day_base_dir,
            final_dir,
            target_race,
        )
        if not built:
            return None
        import pandas as pd

        frame = pd.read_parquet(final_dir)
        return _group_parquet_rows(frame, target_race=target_race)
    except Exception as exc:
        _log_pipeline_progress(
            f"build_upcoming_feature_rows_split failed category={category} "
            f"target_race={target_race} error={exc}"
        )
        return None
