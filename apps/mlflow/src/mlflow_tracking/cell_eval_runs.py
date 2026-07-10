"""Per-(category, CELL, model_version) serve-accuracy evaluation, as
individual PERSISTENT MLflow runs so a cell's trend over time is visible and
drillable in the MLflow UI.

This module is the sibling of, and reuses the same upstream data path as,
`champion_cell_eval.py` (`serve_eval.fetch_fp_prediction_rows`/
`fetch_rs_prediction_rows` -> `filter_genuine_rows` -> `partition_by_banei`
-> per-date `fetch_race_results`/`fetch_race_metadata` -> `build_fp_race_eval_
rows`/`build_rs_horse_eval_rows`), but the two modules answer different
questions and log fundamentally different run shapes:

  * `champion_cell_eval.py` scores ONLY the current champion (widened to
    champion-OR-variant), logs ONE run per (category, task), and the
    per-cell breakdown lives as a table/artifact INSIDE that one run (see
    its own module docstring).
  * THIS module scores EVERY model_version that served enough volume in the
    window (champion, every cell-routed variant, and any other version --
    e.g. a stale/rolled-back challenger still serving stray traffic), and
    logs ONE PERSISTENT run PER (category, canonical cell, model_version)
    triple. Each such run accumulates one metric point per day this
    command runs (see `_append_cell_metric_point`), so a single cell's
    accuracy trend over time is a real MLflow line chart, drillable from
    the run list by any individual tag (venue, class_code, model_version,
    champion_relation, ...) -- the headline ask this module exists for.

★ Design choice: unlike `champion_cell_eval.py`, a window with literally NO
genuinely-served rows for a (category, task) produces ZERO runs here, not a
placeholder "no coverage" run. That module's placeholder-run design exists
because it always evaluates exactly one thing (the champion) and needs a
place to record "the champion had no coverage"; this module evaluates
however many (model_version, cell) combinations actually served, and when
that set is empty there is genuinely nothing to drill into -- an absent run
list is the correct signal here, not a missing artifact.

── Per-(model_version, cell) run creation vs. `--min-races` ────────────────

`_collect_all_prediction_rows` fetches every genuinely-served, Ban-ei-
partitioned prediction row for (category, task) over the trailing window in
ONE range query (mirroring `champion_cell_eval._collect_champion_prediction_
rows` exactly, minus the final champion-only filter that function applies --
this module deliberately omits it, since evaluating every served version is
the whole point). Rows are grouped by `model_version`
(`serve_eval.group_by_model_version`), and for EVERY distinct date any row
touches, `serve_eval.fetch_race_results`/`fetch_race_metadata` (RS only) is
queried ONCE and shared across every model_version's join -- so the local-
replica query cost scales with distinct DATES touched, not with the number
of model_versions or cells, even though the actual per-race/per-horse join
(`build_fp_race_eval_rows`/`build_rs_horse_eval_rows`) is invoked once per
model_version (each such row-builder call stamps its OWN `model_version`
argument onto every output row, per `champion_cell_eval.py`'s own docstring
on why a per-race/per-horse variant identity cannot be read back off a
built eval row directly).

Built eval rows are then grouped into one bucket per (model_version,
*FP_CELL_DIMENSIONS-or-RS_CELL_DIMENSIONS) combination -- a "group". A
group's size (`race_count` for FP, `horse_count` for RS) is exactly the unit
count a "-- has >= --min-races units in the window" requirement needs to
test PER SERVED VERSION per this module's spec: a model_version whose TOTAL
volume this window is below `--min-races` can, by construction, never have
any individual cell whose count reaches `--min-races` either (a cell's count
never exceeds its version's total), so there is no separate "does this
model_version qualify at all" pass distinct from the per-group threshold
test below -- they are mathematically the same test, applied at the
naturally-relevant (model_version, cell) granularity directly.

A group with `count >= min_races_used` (see the volume guard below) gets a
persistent run (created or reused) and a metric point appended for
`as_of`. A group with `count < min_races_used` is SKIPPED from run creation
entirely, but COUNTED in `CellEvalRunsSummary.cells_skipped_low_volume` --
never a silent cap. This is a DIFFERENT, decoupled threshold from the
per-run `low_n` TAG (see below): `low_n` reuses `champion_cell_eval.
MIN_CELL_COUNT` (this package's fixed "statistically thin" cutoff), which a
caller may set `--min-races` above OR below -- e.g. `--min-races 5` still
lets a marginal 6-race cell create a run, but that run is tagged
`low_n=true` for visibility, distinct from "not worth a run at all".

── Volume guard (never silently truncate the run list) ─────────────────────

If, at the user's requested `--min-races`, ONE (category, task) call would
create more than `MAX_RUNS_PER_CATEGORY_TASK` (1500) runs, the floor is
deterministically RAISED (see `_raise_min_races_floor_if_needed`'s own
docstring for the exact algorithm: sort the qualifying group sizes
descending and set the new floor to (the 1501st-largest) + 1, the smallest
integer floor guaranteed to leave at most 1500 groups qualifying) and the
raise is surfaced via `warnings.warn` plus
`CellEvalRunsSummary.volume_guard_triggered`/`projected_run_count_at_
requested_floor` -- never a silent truncation of which cells get a run.

── Idempotency: `cell_run_key` + step-dedup (mirrors timeline.py) ──────────

Each run is found-or-created by a `cell_run_key = "{category}:{cell_key}:
{model_version}:{window_days}"` tag (see `_encode_cell_key` for the
canonical `cell_key` string encoding of one cell's dimension tuple), the
same idiom `champion_cell_eval._find_existing_cell_eval_run`/`sync_
production._find_sync_run` already use for their own (different-grain) keys.
Once found-or-created, one metric point is appended per metric key, deduped
by STEP exactly like `timeline.upsert_timeline_point` already does (see
`_append_cell_metric_point`): `client.get_metric_history(run_id, key)` is
consulted first, and a point at an already-logged step is skipped entirely.
`step = timeline.step_for_date(as_of_str)` (the v2 days-since-2020-01-01
scheme -- see timeline.py's own docstring for why: MLflow 3.14's GenAI/Eval
chart-tile renderer spins forever on a raw YYYYMMDD-magnitude step) and the
matching timestamp mirrors `timeline._timestamp_ms_for_date`'s 12:00-JST
convention (re-implemented locally as `_timestamp_ms_for_date` below, since
the source function is module-private in timeline.py and this package's
convention is to duplicate small private helpers across sibling modules
rather than reach into another module's private members -- see this
module's own "Champion-relation classification" section below for another
example, and for the one deliberate EXCEPTION to that convention this
module makes). A same-day re-run is therefore a
cheap no-op per group: the run is found (not recreated), and every metric
key's point for today's step is already present, so nothing new is logged
-- exactly the "same day re-run must be a cheap dedup-by-step no-op"
requirement.

Tags are always RE-APPLIED on every call (whether the run was just created
or found), mirroring `timeline.upsert_timeline_point`'s own rationale: a
tag like `champion_relation` can legitimately change day to day (a variant
gets promoted to champion), so it must never be frozen at creation time.

── Champion-relation classification ────────────────────────────────────────

`_classify_champion_match` is a DELIBERATE LOCAL DUPLICATE of `champion_
cell_eval._classify_champion_match` (same "champion" / "variant" / "other"
semantics: exact match -> "champion"; `served.startswith(champion_label +
"-")` -> "variant"; anything else, INCLUDING a coincidental raw-string-
prefix match without the "-" separator, -> "other") -- per this package's
established convention of duplicating small classification/registry-lookup
helpers across sibling modules built independently (see champion_cell_
eval.py's own docstring). Unlike that module's version, this module's copy
accepts `champion_label: str | None` (not just `str`): champion_cell_eval.py
only ever calls its version once a non-None champion label has already been
confirmed (it short-circuits before that point otherwise), but THIS module
classifies EVERY served version regardless of whether a champion is
resolved at all, so "no champion resolved" (`champion_label is None`) must
also collapse to "other" -- the same convention `sync_production.
_classify_served_model_version` already uses for its own Optional-label
variant.

`champion_cell_eval.resolve_champion_model_version` itself (the registry
alias lookup, NOT the variant-matching logic) IS reused directly from
`champion_cell_eval.py` rather than duplicated -- it is a shared, single-
purpose registry-lookup concern (not the business logic the module-
independence convention is protecting), and re-deriving it here would risk
the two modules silently drifting on how "no resolvable champion" is
defined. This is the one deliberate exception to this module's own general
"duplicate rather than import a sibling module's helper" convention (see
above): `resolve_champion_model_version` was PROMOTED from private to
public in champion_cell_eval.py specifically so this import is a legitimate
public-API call, not a `reportPrivateUsage` violation of that module's
internals.

── Metric names ─────────────────────────────────────────────────────────────

Finish-position: `race_count` plus every OTHER key `serve_eval.aggregate_fp_
day_metrics` currently returns, PASSED THROUGH UNENUMERATED rather than
hardcoded (`top1_pct, place2_pct, place3_pct, top3_box_pct, fukusho_2p_pct`
as of this writing -- see `_fp_cell_metrics`, which reuses `aggregate_fp_
day_metrics` verbatim: that function is "day" vs. "cell" agnostic, it just
aggregates whatever `FpRaceEvalRow` sequence it is given). Because `_fp_
cell_metrics` iterates that function's own returned keys instead of naming
them one by one, a future rank-4/5/6 addition to `aggregate_fp_day_metrics`
(e.g. `place4_pct`/`place5_pct`/`place6_pct`) flows through here
automatically, with no matching edit required in this module.

Running-style: `horse_count, accuracy_pct, macro_f1_pct` plus per-class
`f1_{label}_pct` for every `serve_eval.RS_CLASS_LABELS` entry with a defined
(non-None) F1 this cell (see `_rs_cell_metrics`, reusing `serve_eval.
aggregate_rs_day_metrics` verbatim). `macro_f1_pct`/a given class's F1 are
OMITTED (never logged as a fabricated 0.0) when the source aggregation
returns None for them -- matching `timeline.rs_metrics_for_timeline`'s own
"only log a defined value" convention exactly.

── Run name / cell_key encoding ─────────────────────────────────────────────

`cell_key` (used inside `cell_run_key`, machine-oriented) is `"{dimension}=
{value}"` pairs joined by "|", in `FP_CELL_DIMENSIONS`/`RS_CELL_DIMENSIONS`
order, with a None dimension value rendered as the literal string "none"
(see `_encode_cell_key`).

The run NAME (human-oriented, e.g. "jra venue=05 class=000 dist=mile
season=summer surf=turf fs=medium v=jra-cb-v9-a1b2c3") labels every cell
dimension with its own short prefix (see `_FP_RUN_NAME_LABELS`/`_RS_RUN_
NAME_LABELS`) and reserves `v=` EXCLUSIVELY for a short/truncated
`model_version` identifier (see `_short_model_version_label`) -- venue gets
its own explicit `venue=` label instead of reusing "v=", specifically so a
run name never uses "v=" for two different meanings in the same string.
`_short_model_version_label` truncates a model_version longer than 24
characters to its first 17 characters (the most human-recognizable part --
category/architecture/training-window info always comes first in this
package's naming convention) plus a "-" separator and a 6-hex-character
sha256-digest suffix of the FULL untruncated string, so two long versions
sharing a truncated prefix (e.g. a champion and one of its own cell-routed
variants) still render as visibly distinct run names. Run-name collisions
are cosmetic only -- idempotency is keyed by `cell_run_key`, never the name.

Every DB-facing function here takes an INJECTED connection (or a zero-
argument factory defaulting to `db.connect_racing_neon`/
`db.connect_local_replica`), matching this package's established
hermetic-testing rationale (see db.py's/serve_eval.py's own module
docstrings).
"""

from __future__ import annotations

import hashlib
import logging
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Final, Literal, cast

import psycopg2
from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag
from mlflow.exceptions import MlflowException

from mlflow_tracking import champion_cell_eval, config, db, registry, serve_eval, timeline
from mlflow_tracking.logging_api import get_or_create_experiment, log_batch_chunked

logger: Final = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS: Final[int] = 90
DEFAULT_MIN_RACES: Final[int] = 20
# Volume-guard cap: the maximum number of runs ONE (category, task) call may
# create before `--min-races` is automatically raised (see this module's
# docstring and `_raise_min_races_floor_if_needed`). Applied per (category,
# task), not summed across both tasks for one category, since FP and RS use
# entirely different registered models and cell dimension sets -- an FP
# explosion in one category must never dictate that category's RS floor.
MAX_RUNS_PER_CATEGORY_TASK: Final[int] = 1500

CELL_RUN_KEY_TAG: Final[str] = "cell_run_key"

# Independently redefined (not imported) from champion_cell_eval.py /
# sync_production.py's own identical constants -- matching this package's
# established per-module-independence convention for small, low-drift-risk
# constants (see e.g. sync_production.py defining its own FP_CATEGORIES/
# RS_CATEGORIES rather than importing champion_cell_eval.py's).
FP_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar", "banei")
RS_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar")

_JST_NOON_AS_UTC_HOUR: Final[int] = 3  # 12:00 JST (UTC+9) == 03:00 UTC, same calendar date.

_VERSION_LABEL_MAX_LEN: Final[int] = 24
_VERSION_HASH_LEN: Final[int] = 6

_FP_RUN_NAME_LABELS: Final[dict[str, str]] = {
    "venue": "venue",
    "class_code": "class",
    "distance_band": "dist",
    "season_band": "season",
    "surface": "surf",
    "field_size_band": "fs",
}
_RS_RUN_NAME_LABELS: Final[dict[str, str]] = {
    "venue": "venue",
    "class_code": "class",
    "distance_band": "dist",
    "surface": "surf",
}

_ChampionMatchKind = Literal["champion", "variant", "other"]

_ISOLATED_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    ValueError,
    KeyError,
    TypeError,
    psycopg2.Error,
    MlflowException,
)

_CellGroupKey = tuple[str, tuple[object, ...]]  # (model_version, cell_values)


@dataclass(frozen=True)
class CellEvalRunsSummary:
    """Outcome counters for one `eval_cells_for_category` call.

    `min_races_used` equals `min_races_requested` unless the volume guard
    raised it (see `_raise_min_races_floor_if_needed`); `volume_guard_
    triggered`/`projected_run_count_at_requested_floor` make that raise
    visible rather than silent. `cells_skipped_low_volume` counts every
    distinct (model_version, cell) group seen this call whose unit count
    fell below `min_races_used` -- counted, never silently dropped.
    `runs_created`/`runs_reused` split the groups that DID clear the floor
    by whether their persistent run already existed from a previous call.
    """

    category: str
    task: str
    window_days: int
    min_races_requested: int
    min_races_used: int
    volume_guard_triggered: bool
    projected_run_count_at_requested_floor: int
    runs_created: int
    runs_reused: int
    cells_skipped_low_volume: int
    champion_model_version: str | None


def _classify_champion_match(
    served_model_version: str, champion_label: str | None
) -> _ChampionMatchKind:
    """Classify one served `model_version` against the resolved champion
    label for (category, task) -- see this module's docstring ("Champion-
    relation classification") for why this is a deliberate local duplicate
    of `champion_cell_eval._classify_champion_match`, extended to accept a
    possibly-absent `champion_label`.
    """
    if champion_label is None:
        return "other"
    if served_model_version == champion_label:
        return "champion"
    if served_model_version.startswith(f"{champion_label}-"):
        return "variant"
    return "other"


def _short_model_version_label(model_version: str) -> str:
    """Truncate a (potentially long) `model_version` label for use in a run
    name -- see this module's docstring ("Run name / cell_key encoding") for
    the full truncation/hash-suffix rationale."""
    if len(model_version) <= _VERSION_LABEL_MAX_LEN:
        return model_version
    digest = hashlib.sha256(model_version.encode("utf-8")).hexdigest()[:_VERSION_HASH_LEN]
    prefix_len = _VERSION_LABEL_MAX_LEN - _VERSION_HASH_LEN - 1
    return f"{model_version[:prefix_len]}-{digest}"


def _encode_cell_key(dimensions: tuple[str, ...], values: tuple[object, ...]) -> str:
    """Build the canonical `cell_key` string encoding of one cell's
    dimension tuple: `"{dimension}={value}"` pairs joined by "|", in
    `dimensions` order, with a None value rendered as the literal string
    "none" (matching `_cell_tags`'s identical convention for the same
    case, so the two never disagree about how a missing dimension is
    spelled)."""
    return "|".join(
        f"{dimension}={value if value is not None else 'none'}"
        for dimension, value in zip(dimensions, values, strict=True)
    )


def _cell_tags(dimensions: tuple[str, ...], values: tuple[object, ...]) -> dict[str, str]:
    """Render one cell's dimension tuple as `{dimension_name: value}` tags --
    each cell dimension gets its OWN tag key (not folded into one opaque
    string), so the MLflow UI's run-table filter/group-by controls work
    directly against e.g. `tags.venue` or `tags.class_code`."""
    return {
        dimension: (str(value) if value is not None else "none")
        for dimension, value in zip(dimensions, values, strict=True)
    }


def _build_run_name(
    category: str,
    dimensions: tuple[str, ...],
    values: tuple[object, ...],
    labels: Mapping[str, str],
    model_version: str,
) -> str:
    """Build a human-scannable run name -- see this module's docstring ("Run
    name / cell_key encoding") for an example and the full rationale behind
    reserving `v=` exclusively for the (truncated) model_version."""
    parts = [category]
    for dimension, value in zip(dimensions, values, strict=True):
        label = labels[dimension]
        rendered = value if value is not None else "none"
        parts.append(f"{label}={rendered}")
    parts.append(f"v={_short_model_version_label(model_version)}")
    return " ".join(parts)


def _timestamp_ms_for_date(date_yyyymmdd: str) -> int:
    """Return the epoch-ms timestamp for 12:00 JST on `date_yyyymmdd`.

    Deliberately mirrors (not imports) `timeline._timestamp_ms_for_date`:
    that function is module-private, and this package's convention is to
    duplicate small private helpers across sibling modules rather than
    reach into another module's private members (see this module's
    docstring)."""
    year, month, day = int(date_yyyymmdd[:4]), int(date_yyyymmdd[4:6]), int(date_yyyymmdd[6:8])
    moment = datetime(year, month, day, _JST_NOON_AS_UTC_HOUR, 0, 0, tzinfo=UTC)
    return int(moment.timestamp() * 1000)


def _tasks_for_category(category: str) -> tuple[str, ...]:
    """Local duplicate of `champion_cell_eval._tasks_for_category` (same
    tiny rule: finish-position always, running-style only for
    `RS_CATEGORIES`) -- not imported, matching this package's per-module-
    independence convention for small private helpers."""
    if category in RS_CATEGORIES:
        return ("finish-position", "running-style")
    return ("finish-position",)


def _collect_all_prediction_rows(
    neon_conn: db.ConnectionLike,
    category: str,
    task: str,
    date_from: str,
    date_to: str,
) -> list[dict[str, object]]:
    """Fetch, genuine-filter, and Ban-ei-partition ALL production-prediction
    rows for one (category, task) pair over [date_from, date_to] -- every
    served model_version, not just the champion's. Mirrors `champion_cell_
    eval._collect_champion_prediction_rows` exactly, with the final
    champion-only filter that function applies deliberately OMITTED (see
    this module's docstring for why: per-cell-per-served-version scope is
    this module's whole point).
    """
    source = serve_eval.resolve_source(category)
    raw_rows = (
        serve_eval.fetch_fp_prediction_rows(neon_conn, source, date_from, date_to)
        if task == "finish-position"
        else serve_eval.fetch_rs_prediction_rows(neon_conn, source, date_from, date_to)
    )
    genuine_rows = serve_eval.filter_genuine_rows(raw_rows)
    if category == "nar":
        genuine_rows, _ = serve_eval.partition_by_banei(genuine_rows)
    elif category == "banei":
        _, genuine_rows = serve_eval.partition_by_banei(genuine_rows)
    return genuine_rows


def _group_rows_by_race_date(
    rows: list[dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """Local duplicate of `champion_cell_eval._group_rows_by_race_date` --
    groups already-filtered prediction rows by race_date (kaisai_nen +
    kaisai_tsukihi) so callers issue exactly one `fetch_race_results`/
    `fetch_race_metadata` call per distinct date, shared across every
    model_version's own join (see this module's docstring)."""
    groups: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        race_date = str(row["kaisai_nen"]) + str(row["kaisai_tsukihi"])
        groups.setdefault(race_date, []).append(row)
    return groups


def _build_fp_eval_rows_by_version(
    category: str,
    rows_by_version: Mapping[str, list[dict[str, object]]],
    results_by_date: Mapping[str, dict[tuple[str, str, str], dict[str, object]]],
) -> dict[str, list[serve_eval.FpRaceEvalRow]]:
    """Join FP prediction rows against `results_by_date` PER model_version
    (each `build_fp_race_eval_rows` call stamps its own model_version onto
    every output row -- see this module's docstring), reusing the SAME
    already-fetched `results_by_date` dict for every version (no extra local
    -replica queries per version)."""
    out: dict[str, list[serve_eval.FpRaceEvalRow]] = {}
    for model_version, version_rows in rows_by_version.items():
        eval_rows: list[serve_eval.FpRaceEvalRow] = []
        for race_date, day_rows in _group_rows_by_race_date(version_rows).items():
            results = results_by_date.get(race_date, {})
            eval_rows.extend(
                serve_eval.build_fp_race_eval_rows(category, model_version, day_rows, results)
            )
        out[model_version] = eval_rows
    return out


def _build_rs_eval_rows_by_version(
    category: str,
    rows_by_version: Mapping[str, list[dict[str, object]]],
    results_by_date: Mapping[str, dict[tuple[str, str, str], dict[str, object]]],
    meta_by_date: Mapping[str, dict[tuple[str, str], dict[str, object]]],
) -> dict[str, list[serve_eval.RsHorseEvalRow]]:
    """Running-style counterpart to `_build_fp_eval_rows_by_version` -- also
    needs `meta_by_date` (race-level metadata, to normalize corner_1 into a
    running-style class AND to derive the RS cell dimensions themselves,
    since raw RS prediction rows carry no cell-dimension columns of their
    own, unlike FP's)."""
    out: dict[str, list[serve_eval.RsHorseEvalRow]] = {}
    for model_version, version_rows in rows_by_version.items():
        eval_rows: list[serve_eval.RsHorseEvalRow] = []
        for race_date, day_rows in _group_rows_by_race_date(version_rows).items():
            results = results_by_date.get(race_date, {})
            meta = meta_by_date.get(race_date, {})
            eval_rows.extend(
                serve_eval.build_rs_horse_eval_rows(
                    category, model_version, day_rows, results, meta
                )
            )
        out[model_version] = eval_rows
    return out


def _fp_cell_metrics(rows: Sequence[serve_eval.FpRaceEvalRow]) -> dict[str, float]:
    """Reduce one (model_version, cell) group's FP eval rows to this
    module's FP metric set -- reusing `serve_eval.aggregate_fp_day_metrics`
    verbatim (it is "day" vs. "cell" agnostic), renaming its `races` key to
    `race_count` and passing every OTHER key through UNCHANGED and
    UNENUMERATED (not a hardcoded key list): whatever `*_pct` keys that
    function currently returns (`top1_pct`, `place2_pct`, `place3_pct`,
    `top3_box_pct`, `fukusho_2p_pct` as of this writing) all flow straight
    through, so a future addition to that function's own return shape is
    picked up here automatically, with no matching edit required in this
    module.

    ★ 2026-07-10 rank-4/5/6 addendum -- the ONE deliberate, minimal
    exception to this module's "no logic edits" boundary: `place4_pct`/
    `place5_pct`/`place6_pct` (see `serve_eval.aggregate_fp_day_metrics`'s
    own docstring) are `float | None` -- None whenever every race in THIS
    (model_version, cell) group had a total starter count below that rank
    (a real, if uncommon, occurrence for a thin/niche group, e.g. a
    challenger model_version's only few served races all being small
    NAR/Ban-ei fields). The original "pass every OTHER key through
    unchanged" comment above did not anticipate a None VALUE reaching this
    point -- only a future ADDITIONAL key, always assumed to be a plain
    float like every key that existed when this module was written. Without
    filtering it out here, `_append_cell_metric_point`'s `float(value)`
    call raises `TypeError: float() argument must be ... not 'NoneType'`
    for that (model_version, cell) group's run -- empirically confirmed
    against this file's own test suite once
    `serve_eval.aggregate_fp_day_metrics` started returning
    `place4_pct`/`place5_pct`/`place6_pct`. `and value is not None` is the
    entire fix: it does not change WHICH (model_version, cell, metric)
    combinations get scored, nor how -- it only skips logging a metric this
    module's own metric-point machinery cannot represent as "not
    applicable" (there is no per-point NULL in an MLflow metric series, only
    absence), exactly the same "omit rather than log a fabricated value"
    discipline `timeline.rs_metrics_for_timeline`/`_rs_cell_metrics` already
    apply to `macro_f1_pct`/per-class F1.
    """
    day_metrics = serve_eval.aggregate_fp_day_metrics(rows)
    metrics = {
        key: value for key, value in day_metrics.items() if key != "races" and value is not None
    }
    metrics["race_count"] = cast(float, day_metrics["races"])
    return metrics


def _rs_cell_metrics(rows: Sequence[serve_eval.RsHorseEvalRow]) -> dict[str, float]:
    """Reduce one (model_version, cell) group's RS eval rows to this
    module's documented RS metric set -- reusing `serve_eval.aggregate_rs_
    day_metrics` verbatim. `macro_f1_pct`/a given class's `f1_{label}_pct`
    are omitted (not logged as a fabricated 0.0) when the source aggregation
    returns None for them, matching `timeline.rs_metrics_for_timeline`'s own
    convention."""
    day_metrics = serve_eval.aggregate_rs_day_metrics(rows)
    metrics: dict[str, float] = {
        "horse_count": float(cast(int, day_metrics["total_horses"])),
        "accuracy_pct": cast(float, day_metrics["overall_accuracy_pct"]),
    }
    macro_f1_pct = day_metrics["macro_f1_pct"]
    if isinstance(macro_f1_pct, int | float):
        metrics["macro_f1_pct"] = float(macro_f1_pct)
    per_class = cast("list[dict[str, object]]", day_metrics["per_class"])
    for entry in per_class:
        label = entry["label"]
        f1_pct = entry["f1_pct"]
        if isinstance(label, str) and isinstance(f1_pct, int | float):
            metrics[f"f1_{label}_pct"] = float(f1_pct)
    return metrics


def _group_fp_eval_rows_by_version_and_cell(
    eval_rows_by_version: Mapping[str, list[serve_eval.FpRaceEvalRow]],
) -> dict[_CellGroupKey, list[serve_eval.FpRaceEvalRow]]:
    """Group already-built, per-model_version FP eval rows into one bucket
    per (model_version, *champion_cell_eval.FP_CELL_DIMENSIONS) combination."""
    groups: dict[_CellGroupKey, list[serve_eval.FpRaceEvalRow]] = {}
    for model_version, rows in eval_rows_by_version.items():
        for row in rows:
            cell_values = tuple(
                getattr(row, dimension) for dimension in champion_cell_eval.FP_CELL_DIMENSIONS
            )
            groups.setdefault((model_version, cell_values), []).append(row)
    return groups


def _group_rs_eval_rows_by_version_and_cell(
    eval_rows_by_version: Mapping[str, list[serve_eval.RsHorseEvalRow]],
) -> dict[_CellGroupKey, list[serve_eval.RsHorseEvalRow]]:
    """RS counterpart of `_group_fp_eval_rows_by_version_and_cell`, grouping
    by (model_version, *champion_cell_eval.RS_CELL_DIMENSIONS)."""
    groups: dict[_CellGroupKey, list[serve_eval.RsHorseEvalRow]] = {}
    for model_version, rows in eval_rows_by_version.items():
        for row in rows:
            cell_values = tuple(
                getattr(row, dimension) for dimension in champion_cell_eval.RS_CELL_DIMENSIONS
            )
            groups.setdefault((model_version, cell_values), []).append(row)
    return groups


def _fp_groups_with_metrics(
    eval_rows_by_version: Mapping[str, list[serve_eval.FpRaceEvalRow]],
) -> dict[_CellGroupKey, tuple[int, dict[str, float]]]:
    """Reduce every FP (model_version, cell) group to `(unit_count,
    metrics)`, so the main per-category-task loop can treat FP and RS
    identically from this point on (both tasks' groups become the same
    homogeneous shape)."""
    grouped = _group_fp_eval_rows_by_version_and_cell(eval_rows_by_version)
    return {key: (len(rows), _fp_cell_metrics(rows)) for key, rows in grouped.items()}


def _rs_groups_with_metrics(
    eval_rows_by_version: Mapping[str, list[serve_eval.RsHorseEvalRow]],
) -> dict[_CellGroupKey, tuple[int, dict[str, float]]]:
    """RS counterpart of `_fp_groups_with_metrics`."""
    grouped = _group_rs_eval_rows_by_version_and_cell(eval_rows_by_version)
    return {key: (len(rows), _rs_cell_metrics(rows)) for key, rows in grouped.items()}


def _raise_min_races_floor_if_needed(
    group_counts: Sequence[int],
    requested_min_races: int,
    *,
    cap: int = MAX_RUNS_PER_CATEGORY_TASK,
) -> tuple[int, bool, int]:
    """Decide the min-races floor actually used for one (category, task)
    call, raising it above `requested_min_races` only when necessary to keep
    the number of runs this call would create at or under `cap`.

    `group_counts` is the list of per-(model_version, cell) unit counts
    (races for FP, horses for RS) seen this call, one entry per distinct
    group with at least one genuinely-served row in the window, regardless
    of how small -- the SAME per-group counts that later decide run creation
    (see `eval_cells_for_category`).

    Deterministic algorithm: if the number of groups already clearing
    `requested_min_races` is `<= cap`, the floor is unchanged (no raise),
    and that count is returned as `projected_run_count`. Otherwise, the
    qualifying counts are sorted descending and the floor is raised to
    `(the (cap + 1)-th largest qualifying count) + 1` -- the smallest
    integer floor guaranteed to leave AT MOST `cap` groups qualifying. Ties
    at the new boundary are broken toward EXCLUDING more groups (never
    toward silently exceeding `cap`): if several groups share the count
    value at index `cap`, all of them end up excluded by the "+1", which can
    leave strictly fewer than `cap` groups qualifying at the new floor --
    safe, and preferred over any scheme that could still exceed the cap on
    a tie.

    Returns `(min_races_used, guard_triggered, projected_run_count)`, where
    `projected_run_count` is always the count AT `requested_min_races` (i.e.
    what this call would have produced without the guard), so a caller can
    log "would have been N, floor raised instead" even when N is huge.
    """
    qualifying_at_requested = sorted(
        (count for count in group_counts if count >= requested_min_races), reverse=True
    )
    projected = len(qualifying_at_requested)
    if projected <= cap:
        return requested_min_races, False, projected
    new_floor = qualifying_at_requested[cap] + 1
    return new_floor, True, projected


def _find_existing_cell_run(
    client: MlflowClient, experiment_id: str, cell_run_key: str
) -> str | None:
    """Find an existing per-cell run for `cell_run_key` by tag search --
    same idiom as `champion_cell_eval._find_existing_cell_eval_run`/`sync_
    production._find_sync_run`/`timeline._find_timeline_run`."""
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{CELL_RUN_KEY_TAG} = '{cell_run_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def _append_cell_metric_point(
    client: MlflowClient,
    run_id: str,
    metrics: Mapping[str, float],
    *,
    step: int,
    timestamp_ms: int,
) -> None:
    """Append one point per metric key, deduped by STEP -- the exact idiom
    `timeline.upsert_timeline_point` already uses for its own metric-append
    half (see that function's docstring). Duplicated here (not called
    through timeline.py) because `upsert_timeline_point` is hardwired to its
    own (task, category) `timeline_key` run-finding scheme -- a different
    identity/grain than this module's (category, cell, model_version,
    window_days) `cell_run_key` -- so only the metric-append half of that
    function's behavior applies here, not the run-finding half.
    """
    metric_items: list[Metric] = []
    for key, value in metrics.items():
        already_logged_steps = {point.step for point in client.get_metric_history(run_id, key)}
        if step in already_logged_steps:
            continue
        metric_items.append(Metric(key, float(value), timestamp_ms, step))
    log_batch_chunked(client, run_id, metrics=metric_items)


def eval_cells_for_category(
    client: MlflowClient,
    category: str,
    task: str,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_races: int = DEFAULT_MIN_RACES,
    as_of: date | None = None,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
    local_connect: Callable[[], db.ConnectionLike] = db.connect_local_replica,
) -> CellEvalRunsSummary:
    """Score every model_version that served enough volume for (category,
    task) at CELL granularity over a trailing window, logging one
    persistent MLflow run per (category, cell, model_version) and returning
    a summary. See this module's docstring for the full design.

    Raises ValueError for an unsupported `task`, or for
    `task="running-style"` combined with a Ban-ei category (running-style
    has no Ban-ei support) -- both checks run before any DB or MLflow work,
    matching `champion_cell_eval.eval_champion_cells_for_category`'s own
    validation order.
    """
    if task not in ("finish-position", "running-style"):
        raise ValueError(f"task must be 'finish-position' or 'running-style', got: {task!r}")
    normalized_category = registry.normalize_category(category)
    if task == "running-style" and normalized_category == "banei":
        raise ValueError("running-style does not support category=banei (Ban-ei is out of scope)")

    resolved_as_of = as_of if as_of is not None else date.today()
    as_of_str = resolved_as_of.strftime("%Y%m%d")
    date_from = (resolved_as_of - timedelta(days=window_days)).strftime("%Y%m%d")
    date_to = as_of_str

    champion_ref = champion_cell_eval.resolve_champion_model_version(
        client, normalized_category, task
    )
    champion_label = champion_ref.model_version_label

    experiment_name = (
        config.EXPERIMENT_FP_CELL_EVAL
        if task == "finish-position"
        else config.EXPERIMENT_RS_CELL_EVAL
    )
    experiment_id = get_or_create_experiment(client, experiment_name)

    dimensions = (
        champion_cell_eval.FP_CELL_DIMENSIONS
        if task == "finish-position"
        else champion_cell_eval.RS_CELL_DIMENSIONS
    )
    run_name_labels = _FP_RUN_NAME_LABELS if task == "finish-position" else _RS_RUN_NAME_LABELS

    neon_conn = neon_connect()
    local_conn = local_connect()
    try:
        raw_rows = _collect_all_prediction_rows(
            neon_conn, normalized_category, task, date_from, date_to
        )
        if not raw_rows:
            logger.info(
                "eval-cells category=%s task=%s window_days=%d: no genuinely-served rows in "
                "window, 0 runs created",
                normalized_category,
                task,
                window_days,
            )
            return CellEvalRunsSummary(
                category=normalized_category,
                task=task,
                window_days=window_days,
                min_races_requested=min_races,
                min_races_used=min_races,
                volume_guard_triggered=False,
                projected_run_count_at_requested_floor=0,
                runs_created=0,
                runs_reused=0,
                cells_skipped_low_volume=0,
                champion_model_version=champion_label,
            )

        rows_by_version = serve_eval.group_by_model_version(raw_rows)
        distinct_dates = {str(row["kaisai_nen"]) + str(row["kaisai_tsukihi"]) for row in raw_rows}
        results_by_date = {
            race_date: serve_eval.fetch_race_results(local_conn, normalized_category, race_date)
            for race_date in distinct_dates
        }

        if task == "finish-position":
            fp_eval_rows_by_version = _build_fp_eval_rows_by_version(
                normalized_category, rows_by_version, results_by_date
            )
            groups_with_metrics = _fp_groups_with_metrics(fp_eval_rows_by_version)
        else:
            meta_by_date = {
                race_date: serve_eval.fetch_race_metadata(
                    local_conn, normalized_category, race_date
                )
                for race_date in distinct_dates
            }
            rs_eval_rows_by_version = _build_rs_eval_rows_by_version(
                normalized_category, rows_by_version, results_by_date, meta_by_date
            )
            groups_with_metrics = _rs_groups_with_metrics(rs_eval_rows_by_version)

        group_counts = [unit_count for unit_count, _metrics in groups_with_metrics.values()]
        # `cap` is passed EXPLICITLY (not left to `_raise_min_races_floor_if_
        # needed`'s own default parameter) so a test/operator override of the
        # module-level `MAX_RUNS_PER_CATEGORY_TASK` constant takes effect at
        # call time -- a default parameter value is bound once at function-
        # definition time, so relying on it here would silently ignore any
        # later reassignment of the module attribute.
        min_races_used, guard_triggered, projected = _raise_min_races_floor_if_needed(
            group_counts, min_races, cap=MAX_RUNS_PER_CATEGORY_TASK
        )
        if guard_triggered:
            warnings.warn(
                f"eval-cells volume guard triggered category={normalized_category!r} "
                f"task={task!r}: requested min_races={min_races} would have produced "
                f"{projected} runs (cap={MAX_RUNS_PER_CATEGORY_TASK}); floor raised to "
                f"{min_races_used}",
                stacklevel=2,
            )

        runs_created = 0
        runs_reused = 0
        cells_skipped_low_volume = 0
        step = timeline.step_for_date(as_of_str)
        timestamp_ms = _timestamp_ms_for_date(as_of_str)

        for (model_version, cell_values), (unit_count, metrics) in groups_with_metrics.items():
            if unit_count < min_races_used:
                cells_skipped_low_volume += 1
                continue

            relation = _classify_champion_match(model_version, champion_label)
            cell_key = _encode_cell_key(dimensions, cell_values)
            cell_run_key = f"{normalized_category}:{cell_key}:{model_version}:{window_days}"
            low_n = unit_count < champion_cell_eval.MIN_CELL_COUNT
            tags = {
                CELL_RUN_KEY_TAG: cell_run_key,
                "category": normalized_category,
                **_cell_tags(dimensions, cell_values),
                "model_version": model_version,
                "champion_relation": relation,
                "window_days": str(window_days),
                "low_n": "true" if low_n else "false",
            }
            run_name = _build_run_name(
                normalized_category, dimensions, cell_values, run_name_labels, model_version
            )

            existing_run_id = _find_existing_cell_run(client, experiment_id, cell_run_key)
            if existing_run_id is None:
                run = client.create_run(
                    experiment_id, run_name=run_name, tags={CELL_RUN_KEY_TAG: cell_run_key}
                )
                run_id = run.info.run_id
                runs_created += 1
            else:
                run_id = existing_run_id
                runs_reused += 1

            log_batch_chunked(client, run_id, tags=[RunTag(k, v) for k, v in tags.items()])
            _append_cell_metric_point(client, run_id, metrics, step=step, timestamp_ms=timestamp_ms)
            client.set_terminated(run_id, status="FINISHED")
    finally:
        neon_conn.close()
        local_conn.close()

    logger.info(
        "eval-cells category=%s task=%s window_days=%d min_races=%d(used=%d) runs_created=%d "
        "runs_reused=%d cells_skipped_low_volume=%d",
        normalized_category,
        task,
        window_days,
        min_races,
        min_races_used,
        runs_created,
        runs_reused,
        cells_skipped_low_volume,
    )
    return CellEvalRunsSummary(
        category=normalized_category,
        task=task,
        window_days=window_days,
        min_races_requested=min_races,
        min_races_used=min_races_used,
        volume_guard_triggered=guard_triggered,
        projected_run_count_at_requested_floor=projected,
        runs_created=runs_created,
        runs_reused=runs_reused,
        cells_skipped_low_volume=cells_skipped_low_volume,
        champion_model_version=champion_label,
    )


def eval_cells(
    client: MlflowClient,
    categories: Sequence[str] = FP_CATEGORIES,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_races: int = DEFAULT_MIN_RACES,
    as_of: date | None = None,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
    local_connect: Callable[[], db.ConnectionLike] = db.connect_local_replica,
) -> list[CellEvalRunsSummary]:
    """Run `eval_cells_for_category` for every (category, task) pair implied
    by `categories` -- finish-position for every category, plus running-
    style for every category also in `RS_CATEGORIES` -- isolating failures
    per pair exactly like `champion_cell_eval.eval_champion_cells` (see that
    function's own docstring for the full rationale): one pair failing never
    prevents the others from being evaluated, and a failed pair is simply
    omitted from the returned list (with a `warnings.warn`)."""
    results: list[CellEvalRunsSummary] = []
    for category in categories:
        for task in _tasks_for_category(category):
            try:
                results.append(
                    eval_cells_for_category(
                        client,
                        category,
                        task,
                        window_days=window_days,
                        min_races=min_races,
                        as_of=as_of,
                        neon_connect=neon_connect,
                        local_connect=local_connect,
                    )
                )
            except _ISOLATED_EXCEPTIONS as exc:
                warnings.warn(
                    f"eval-cells failed for category={category!r} task={task!r}: {exc}",
                    stacklevel=2,
                )
    return results
