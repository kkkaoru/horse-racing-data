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
     NAR / 111 Ban-ei) per FINISH_POSITION_MODEL_V7_LINEAGE.md sections 4 / 8 / 9
     and FINISH_POSITION_MODEL_V6_STACKED.md section 2. Each layer preserves
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
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

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


def run_with_stderr_capture(args: Sequence[str]) -> None:
    """Run a subprocess, surfacing the child's stderr tail on non-zero exit.

    ``subprocess.run(check=True)`` raises ``CalledProcessError`` but does NOT
    include the child's stderr in the message, so a silent ``exit 1`` from the
    feature pipeline turns into an opaque parent-side traceback. Capturing and
    re-raising with the tail makes those failures diagnosable. Any ``--pg-url``
    in argv is masked before logging so the Neon password never reaches logs.
    """
    result = subprocess.run(list(args), check=False, capture_output=True, text=True)
    if result.returncode == 0:
        return
    safe_args = [mask_pg_url(arg) for arg in args]
    stderr_text = result.stderr or ""
    stderr_tail = stderr_text[-STDERR_TAIL_BYTES:]
    message = (
        f"subprocess failed (exit {result.returncode}): {safe_args}\n"
        f"stderr (last {STDERR_TAIL_BYTES} bytes):\n{stderr_tail}"
    )
    raise RuntimeError(message)


def _final_parquet_dir(category: Category) -> Path:
    return WORK_DIR / f"feat-{category}-v7-final"


def build_upcoming_feature_rows(
    category: Category,
    target_date: str,
    days_ahead: int,
    database_url: str,
) -> Mapping[str, list[Mapping[str, object]]]:
    """Run the pipeline and return ``race_id`` -> ordered entry feature dicts.

    Returns an empty mapping when the base build emits zero target rows (e.g.
    JRA on a NAR-only weekday). In that case the per-category layer chain is
    skipped — there is nothing to score — and the caller continues with the
    next category without raising.
    """
    import pandas as pd

    final_dir = _final_parquet_dir(category)
    built = build_pipeline(category, target_date, days_ahead, database_url, final_dir)
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
) -> bool:
    """Run the DuckDB base build then each v7 layer into ``final_dir``.

    Returns ``True`` when a populated ``final_dir`` was produced, ``False`` when
    the base build emitted zero target rows (in which case the layer chain is
    skipped because layer scripts cannot read an empty parquet directory).
    """
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    base_dir = WORK_DIR / f"feat-{category}-base"
    run_with_stderr_capture(
        build_base_argv(
            DUCKDB_BUILDER,
            category,
            target_date,
            days_ahead,
            database_url,
            base_dir,
        )
    )
    if not has_parquet_output(base_dir):
        return False
    current = base_dir
    for index, script in enumerate(layer_chain_for(category)):
        nxt = WORK_DIR / f"feat-{category}-layer-{index}"
        run_with_stderr_capture(
            build_layer_argv(
                script,
                category,
                LAYER_DIR,
                current,
                nxt,
                database_url,
            )
        )
        current = nxt
    current.rename(final_dir)
    return True
