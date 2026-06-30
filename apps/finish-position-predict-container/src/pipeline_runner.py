"""Build the v7-lineage feature parquet for TODAY's races and load it.

Deploy-time I/O glue (not unit-tested; verified per DEPLOY.md). It reuses the
unchanged repo feature pipeline that already produced the training parquet:

  1. ``finish_position_features_duckdb.py`` builds the base feature parquet from
     Postgres in ``--target-date`` mode for the requested category over
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

import re
import subprocess
import sys
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from time import perf_counter
from typing import IO, Final

from predict_lib.model_meta import Category
from predict_lib.pipeline_args import (
    build_base_argv,
    build_layer_argv,
    layer_chain_for,
)

PIPELINE_DIR: Final[Path] = Path("/app/pipeline")
DUCKDB_BUILDER: Final[Path] = PIPELINE_DIR / "finish_position_features_duckdb.py"
LAYER_DIR: Final[Path] = PIPELINE_DIR / "finish-position-features"
WORK_DIR: Final[Path] = Path("/tmp/predict-upcoming")
RACE_ID_FIELD: Final[str] = "race_id"
STDERR_TAIL_BYTES: Final[int] = 4000
PG_URL_USERINFO_RE: Final[re.Pattern[str]] = re.compile(r"(postgresql://)[^@]+@")
PG_URL_REDACTED: Final[str] = r"\1<redacted>@"


def mask_pg_url(text: str) -> str:
    """Replace ``user:pass@`` in any ``postgresql://`` URL with ``<redacted>@``.

    Defensive: subprocess argv carries the Neon URL with the password, and we
    want to be able to log argv on failure without ever leaking the secret.
    """
    return PG_URL_USERINFO_RE.sub(PG_URL_REDACTED, text)


def _tee_stream(src: IO[str], sink: IO[str], buffer: list[str]) -> None:
    """Forward ``src`` -> ``sink`` line-by-line while also collecting into buffer.

    Used so the child's stderr (heartbeat / progress logs from the bundled
    feature scripts) keeps streaming through to the parent's stderr in real
    time AND we still have the tail available to attach to a RuntimeError on
    non-zero exit. Without this tee, ``subprocess.run(capture_output=True)``
    would silence the child for the entire (10-30 min) feature build and only
    surface anything after the child exited — fatal for live observability.
    """
    for line in src:
        sink.write(line)
        sink.flush()
        buffer.append(line)


def run_with_stderr_capture(args: Sequence[str]) -> None:
    """Run a subprocess, streaming output through and capturing stderr tail.

    Without this wrapper ``subprocess.run(check=True)`` raises
    ``CalledProcessError`` but does NOT include the child's stderr in the
    message, so a silent ``exit 1`` from the feature pipeline turns into an
    opaque parent-side traceback. We:

    * stream the child's stdout to the parent's stdout line-by-line so the
      feature build's JSON heartbeat keeps reaching container logs live;
    * stream the child's stderr to the parent's stderr line-by-line AND keep
      the tail in memory so it can be attached to the RuntimeError on failure;
    * mask any ``--pg-url`` in argv before logging so the Neon password never
      reaches logs (defensive).
    """
    process = subprocess.Popen(
        list(args),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    stdout_buffer: list[str] = []
    stderr_buffer: list[str] = []
    assert process.stdout is not None
    assert process.stderr is not None
    stdout_thread = threading.Thread(
        target=_tee_stream, args=(process.stdout, sys.stdout, stdout_buffer)
    )
    stderr_thread = threading.Thread(
        target=_tee_stream, args=(process.stderr, sys.stderr, stderr_buffer)
    )
    stdout_thread.start()
    stderr_thread.start()
    returncode = process.wait()
    stdout_thread.join()
    stderr_thread.join()
    if returncode == 0:
        return
    safe_args = [mask_pg_url(arg) for arg in args]
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


def _log_pipeline_progress(message: str) -> None:
    print(f"[pipeline] {message}", file=sys.stderr, flush=True)


def _query_upcoming_race_keys(
    database_url: str,
    target_date: str,
    days_ahead: int,
    category: Category,
    target_race: str | None = None,
) -> list[tuple[str, str]]:
    """Query (keibajo_code, race_bango) for upcoming races from Neon.

    Used to drive the per-race realtime-odds fetch so only races that will be
    predicted receive a GET request. Returns an empty list on any error so the
    caller falls back to the NULL-odds path gracefully.

    The query reads ``nvd_se`` / ``jvd_se`` for the target window and returns
    DISTINCT (keibajo_code, race_bango) pairs whose ``kakutei_chakujun`` is
    blank (UPCOMING). The DuckDB feature build derives ``finish_position`` from
    the same tables so the race set is consistent.
    """
    from datetime import UTC, datetime, timedelta

    from db_driver import connect_postgres

    try:
        from_dt = datetime.strptime(target_date, "%Y%m%d").replace(tzinfo=UTC)
        to_dt = from_dt + timedelta(days=days_ahead)
        target_from = target_date
        target_to = to_dt.strftime("%Y%m%d")
        if category == "jra":
            se_table = "jvd_se"
            keibajo_filter = (
                "keibajo_code in ('01','02','03','04','05','06','07','08','09','10')"
            )
        elif category == "nar":
            se_table = "nvd_se"
            keibajo_filter = "keibajo_code <> '83'"
        else:
            se_table = "nvd_se"
            keibajo_filter = "keibajo_code = '83'"
        target_race_filter = ""
        if target_race is not None:
            keibajo_code, race_bango = target_race.split(":", 1)
            target_race_filter = (
                f"and keibajo_code = '{keibajo_code}' "
                f"and race_bango = '{race_bango}'"
            )

        sql = f"""
            select distinct keibajo_code, race_bango
            from {se_table}
            where kaisai_nen between '{target_from[:4]}' and '{target_to[:4]}'
              and (kaisai_nen || kaisai_tsukihi) between '{target_from}' and '{target_to}'
              and {keibajo_filter}
              {target_race_filter}
              and ketto_toroku_bango is not null
              and (kakutei_chakujun is null or trim(kakutei_chakujun) in ('', '00'))
            order by keibajo_code, race_bango
        """
        conn = connect_postgres(database_url)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.close()
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
    realtime_odds_path = fetch_realtime_odds_parquet(
        category, target_date, WORK_DIR, race_keys
    )
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
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for race_id, race_frame in frame.groupby(RACE_ID_FIELD):
        grouped[str(race_id)] = list(race_frame.to_dict(orient="records"))
    return grouped


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
    base_dir = WORK_DIR / f"feat-{category}-base"
    duckdb_temp_dir = _duckdb_temp_dir(category, target_date, target_race)
    duckdb_temp_dir.mkdir(parents=True, exist_ok=True)
    target_label = target_race if target_race is not None else "all"
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
        _log_pipeline_progress(
            f"step=base index=0 status=failed category={category} "
            f"target_race={target_label} elapsed_seconds={perf_counter() - base_start:.3f}"
        )
        raise
    _log_pipeline_progress(
        f"step=base index=0 status=done category={category} "
        f"target_race={target_label} elapsed_seconds={perf_counter() - base_start:.3f}"
    )
    if not has_parquet_output(base_dir):
        _log_pipeline_progress(
            f"step=layers index=0 status=skipped category={category} "
            f"target_race={target_label} reason=no-parquet elapsed_seconds=0.000"
        )
        return False
    current = base_dir
    chain = layer_chain_for(category)
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
            _log_pipeline_progress(
                f"step=layer index={index + 1}/{len(chain)} status=failed "
                f"category={category} script={script} target_race={target_label} "
                f"elapsed_seconds={perf_counter() - layer_start:.3f}"
            )
            raise
        _log_pipeline_progress(
            f"step=layer index={index + 1}/{len(chain)} status=done "
            f"category={category} script={script} target_race={target_label} "
            f"elapsed_seconds={perf_counter() - layer_start:.3f}"
        )
        current = nxt
    current.rename(final_dir)
    _log_pipeline_progress(
        f"done pipeline category={category} target_race={target_label} output={final_dir}"
    )
    return True
