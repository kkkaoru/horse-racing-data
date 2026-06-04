"""Iter 23 per-class ensemble optimization driver (Optuna TPE).

Loads per-class candidate predictions, normalizes within-race, then runs an
Optuna TPE search over a softmax-simplex weight vector with an enforced
``--min-baseline-weight`` on the iter 14 baseline anchor. Validation years
maximize top-1; holdout years drive the accept gate.

Outputs:
- always: ``--output-summary`` JSON with weights, metrics, decision, pairwise
  Spearman correlations between member predictions
- accepted-only: ``--output-manifest-dir/manifest.json`` per the iter 23 plan

Pure-function helpers live in ``per_class_ensemble_lib``; this module wires
them into argparse + Optuna + duckdb + manifest writing.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol, cast

import pandas as pd

import per_class_ensemble_lib as lib

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------

ENSEMBLE_TYPE: Final[str] = "rank_blend"
SEARCH_METHOD: Final[str] = "optuna_tpe"
DEFAULT_VALIDATION_YEARS: Final[str] = "2018,2019,2020,2021,2022"
DEFAULT_HOLDOUT_YEARS: Final[str] = "2023,2024,2025,2026"
DEFAULT_MIN_BASELINE_WEIGHT: Final[float] = 0.20
DEFAULT_N_TRIALS: Final[int] = 200
DEFAULT_SEED: Final[int] = 42
DEFAULT_MIN_HOLDOUT_RACES: Final[int] = 200
DEFAULT_DELTA_PP_FLOOR: Final[float] = 0.0
DEFAULT_LOGIT_RANGE: Final[tuple[float, float]] = (-3.0, 3.0)
BASELINE_VERSION: Final[str] = "iter14-jra-cb-pacestyle-course-v8"
DECISION_ACCEPT: Final[str] = "accept"
DECISION_REJECT: Final[str] = "reject"
REJECT_REASON_DELTA: Final[str] = "delta_pp<0"
REJECT_REASON_HOLDOUT: Final[str] = "insufficient_holdout_races"
PG_RACE_META_SQL: Final[str] = (
    "SELECT "
    "  'jra:' || kaisai_nen || ':' || kaisai_tsukihi || ':' || keibajo_code "
    "  || ':' || race_bango AS race_id, "
    "  NULLIF(TRIM(kyoso_joken_code), '') AS kyoso_joken_code "
    "FROM jvd_ra"
)
PP_SCALE: Final[float] = 100.0


# ---------------------------------------------------------------------------
# Dataclasses / Protocols
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OptimizeArgs:
    class_code: str
    baseline_parquet_dir: Path
    candidate_parquet_dirs: list[Path]
    validation_years: list[int]
    holdout_years: list[int]
    min_baseline_weight: float
    n_trials: int
    seed: int
    pg_url: str
    output_summary: Path
    output_manifest_dir: Path | None
    min_holdout_races: int
    delta_pp_floor: float


@dataclass(frozen=True)
class CandidateBundle:
    model_version: str
    parquet_dir: Path
    validation_df: pd.DataFrame  # normalized
    holdout_df: pd.DataFrame  # normalized


@dataclass(frozen=True)
class HoldoutMetrics:
    top1: float
    iter14_top1: float
    delta_pp: float
    n_races: int
    wilson_lower_delta: float


@dataclass(frozen=True)
class OptimizationResult:
    best_weights: list[float]
    validation_top1: float
    holdout: HoldoutMetrics


@dataclass(frozen=True)
class DecisionResult:
    decision: str
    rejected_reason: str | None


class TrialLike(Protocol):
    def suggest_float(
        self, name: str, low: float, high: float, *, log: bool = ...,
    ) -> float: ...


class StudyLike(Protocol):
    def optimize(
        self, objective: "Callable[[TrialLike], float]", *, n_trials: int,
    ) -> None: ...

    @property
    def best_params(self) -> dict[str, float]: ...

    @property
    def best_value(self) -> float: ...


if TYPE_CHECKING:
    CreateStudyFn = Callable[[int], StudyLike]
    LoadRaceMetaFn = Callable[[str], pd.DataFrame]


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="optimize-per-class-ensemble")
    parser.add_argument("--class-code", required=True)
    parser.add_argument("--baseline-parquet-dir", type=Path, required=True)
    parser.add_argument(
        "--candidate-parquet-dir", type=Path, action="append", default=[],
    )
    parser.add_argument(
        "--validation-years", default=DEFAULT_VALIDATION_YEARS,
    )
    parser.add_argument("--holdout-years", default=DEFAULT_HOLDOUT_YEARS)
    parser.add_argument(
        "--min-baseline-weight", type=float, default=DEFAULT_MIN_BASELINE_WEIGHT,
    )
    parser.add_argument("--n-trials", type=int, default=DEFAULT_N_TRIALS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--output-summary", type=Path, required=True)
    parser.add_argument(
        "--output-manifest-dir", type=Path, default=None,
    )
    parser.add_argument(
        "--min-holdout-races", type=int, default=DEFAULT_MIN_HOLDOUT_RACES,
    )
    parser.add_argument(
        "--delta-pp-floor", type=float, default=DEFAULT_DELTA_PP_FLOOR,
    )
    return parser


def _parse_year_list(spec: str) -> list[int]:
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    if not parts:
        raise ValueError(f"year list is empty: {spec!r}")
    return [int(p) for p in parts]


def normalize_args(args: argparse.Namespace) -> OptimizeArgs:
    return OptimizeArgs(
        class_code=str(cast(str, args.class_code)),
        baseline_parquet_dir=Path(cast(str, args.baseline_parquet_dir)),
        candidate_parquet_dirs=[
            Path(cast(str, p)) for p in cast(list[object], args.candidate_parquet_dir)
        ],
        validation_years=_parse_year_list(cast(str, args.validation_years)),
        holdout_years=_parse_year_list(cast(str, args.holdout_years)),
        min_baseline_weight=float(cast(float, args.min_baseline_weight)),
        n_trials=int(cast(int, args.n_trials)),
        seed=int(cast(int, args.seed)),
        pg_url=str(cast(str, args.pg_url)),
        output_summary=Path(cast(str, args.output_summary)),
        output_manifest_dir=(
            None if args.output_manifest_dir is None
            else Path(cast(str, args.output_manifest_dir))
        ),
        min_holdout_races=int(cast(int, args.min_holdout_races)),
        delta_pp_floor=float(cast(float, args.delta_pp_floor)),
    )


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------


def derive_model_version(parquet_dir: Path) -> str:
    """Infer the model_version label from the parquet directory name.

    The parquet path layout is ``<root>/<model_version>/predictions/...``, so
    we walk up until we find a directory that is the parent of ``predictions``.
    Defaults to the leaf directory name when no ``predictions`` ancestor is
    detected (e.g. tests using a synthetic flat layout).
    """
    for part in (parquet_dir, *parquet_dir.parents):
        if (part / "predictions").is_dir():
            return part.name
    return parquet_dir.name


def default_load_race_meta(pg_url: str) -> pd.DataFrame:
    """Load (race_id, kyoso_joken_code) via DuckDB postgres_scanner."""
    duckdb_module = importlib.import_module("duckdb")
    con = duckdb_module.connect(":memory:")
    try:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        con.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
        sys.stderr.write(
            f"[optimize-per-class-ensemble] loading race meta from {pg_url}\n",
        )
        df = con.execute(
            f"SELECT * FROM postgres_query('pg', $$ {PG_RACE_META_SQL} $$)",
        ).fetchdf()
    finally:
        con.close()
    return df


def _resolve_predictions_root(parquet_dir: Path) -> Path:
    """Walk down ``parquet_dir`` to the ``predictions/category=jra/`` directory.

    Supports two layouts:
    - ``<root>/predictions/category=jra/race_year=...``
    - ``<root>/race_year=...`` (flat — used by tests).
    """
    cat_dir = parquet_dir / "predictions" / "category=jra"
    if cat_dir.exists():
        return cat_dir
    return parquet_dir


def load_normalized_member(
    parquet_dir: Path,
    class_code: str,
    years: "Sequence[int]",
    pg_class_map_df: pd.DataFrame,
) -> pd.DataFrame:
    """Load member predictions for ``years``, class-filter, normalize within race."""
    root = _resolve_predictions_root(parquet_dir)
    raw = lib.load_class_predictions(root, class_code, years, pg_class_map_df)
    return lib.normalize_within_race(raw)


def _load_one_candidate(
    parquet_dir: Path,
    args: OptimizeArgs,
    pg_class_map_df: pd.DataFrame,
) -> CandidateBundle | None:
    model_version = derive_model_version(parquet_dir)
    val_df = load_normalized_member(
        parquet_dir, args.class_code, args.validation_years, pg_class_map_df,
    )
    if val_df.empty:
        sys.stderr.write(
            f"[optimize-per-class-ensemble] candidate {model_version!r} has no "
            "validation rows for class — dropped from search\n",
        )
        return None
    hold_df = load_normalized_member(
        parquet_dir, args.class_code, args.holdout_years, pg_class_map_df,
    )
    return CandidateBundle(
        model_version=model_version,
        parquet_dir=parquet_dir,
        validation_df=val_df,
        holdout_df=hold_df,
    )


def build_bundles(
    args: OptimizeArgs, pg_class_map_df: pd.DataFrame,
) -> list[CandidateBundle]:
    """Build the (baseline-first) ordered list of candidate bundles.

    The iter 14 baseline is always included as bundle index 0. Candidates with
    no validation rows are dropped (the orchestrator log explains why).
    """
    bundles: list[CandidateBundle] = []
    baseline = _load_one_candidate(args.baseline_parquet_dir, args, pg_class_map_df)
    if baseline is None:
        raise RuntimeError(
            f"baseline parquet {args.baseline_parquet_dir} produced no class rows",
        )
    # Force the baseline's model_version to the canonical anchor label so
    # downstream manifests / summary files do not depend on path naming.
    bundles.append(
        CandidateBundle(
            model_version=BASELINE_VERSION,
            parquet_dir=baseline.parquet_dir,
            validation_df=baseline.validation_df,
            holdout_df=baseline.holdout_df,
        ),
    )
    for cand_dir in args.candidate_parquet_dirs:
        bundle = _load_one_candidate(cand_dir, args, pg_class_map_df)
        if bundle is None:
            continue
        bundles.append(bundle)
    return bundles


# ---------------------------------------------------------------------------
# Optuna optimization
# ---------------------------------------------------------------------------


def default_create_study(seed: int) -> StudyLike:
    optuna_module = importlib.import_module("optuna")
    sampler = optuna_module.samplers.TPESampler(seed=seed)
    study: StudyLike = cast(
        StudyLike,
        optuna_module.create_study(direction="maximize", sampler=sampler),
    )
    return study


def _suggest_logits(trial: TrialLike, n_members: int) -> list[float]:
    lo, hi = DEFAULT_LOGIT_RANGE
    return [
        trial.suggest_float(f"z_{idx}", lo, hi)
        for idx in range(n_members)
    ]


def evaluate_weights(
    bundles: list[CandidateBundle], weights: list[float], use_validation: bool,
) -> tuple[float, int]:
    """Blend ``bundles`` with ``weights`` and return (top1, n_races).

    When any member frame is empty (i.e. has no rows in that class window) the
    inner join collapses to nothing — return (0.0, 0). When the inner join
    yields no surviving rows for the cross-product of races + horses, same.
    """
    members = [
        (b.validation_df if use_validation else b.holdout_df)
        for b in bundles
    ]
    if any(m.empty for m in members):
        return 0.0, 0
    blended = lib.blend_normalized(members, weights)
    if blended.empty:
        return 0.0, 0
    n_races = int(blended["race_id"].nunique())
    return lib.compute_top1(blended), n_races


def make_objective(
    bundles: list[CandidateBundle], min_baseline_weight: float,
) -> "Callable[[TrialLike], float]":
    def objective(trial: TrialLike) -> float:
        n_members = len(bundles)
        logits = _suggest_logits(trial, n_members)
        weights = lib.simplex_softmax(logits, 0, min_baseline_weight)
        score, _ = evaluate_weights(bundles, weights, use_validation=True)
        return score
    return objective


def _extract_best_logits(params: dict[str, float], n_members: int) -> list[float]:
    return [float(params[f"z_{idx}"]) for idx in range(n_members)]


def run_optuna_search(
    bundles: list[CandidateBundle],
    args: OptimizeArgs,
    create_study: CreateStudyFn = default_create_study,
) -> tuple[list[float], float]:
    """Drive Optuna TPE search; return (best_weights, validation_top1)."""
    study = create_study(args.seed)
    objective = make_objective(bundles, args.min_baseline_weight)
    study.optimize(objective, n_trials=args.n_trials)
    best_logits = _extract_best_logits(study.best_params, len(bundles))
    best_weights = lib.simplex_softmax(best_logits, 0, args.min_baseline_weight)
    return best_weights, float(study.best_value)


# ---------------------------------------------------------------------------
# Holdout metrics + decision
# ---------------------------------------------------------------------------


def compute_holdout_metrics(
    bundles: list[CandidateBundle], best_weights: list[float],
) -> HoldoutMetrics:
    top1, n_races = evaluate_weights(bundles, best_weights, use_validation=False)
    baseline_only_w = [1.0] + [0.0] * (len(bundles) - 1)
    iter14_top1, _ = evaluate_weights(bundles, baseline_only_w, use_validation=False)
    delta_pp = (top1 - iter14_top1) * PP_SCALE
    wilson_low = lib.wilson_lower_bound(top1, n_races) - lib.wilson_lower_bound(
        iter14_top1, n_races,
    )
    return HoldoutMetrics(
        top1=top1, iter14_top1=iter14_top1, delta_pp=delta_pp,
        n_races=n_races, wilson_lower_delta=wilson_low,
    )


def make_decision(
    metrics: HoldoutMetrics, min_holdout_races: int, delta_pp_floor: float,
) -> DecisionResult:
    if metrics.n_races < min_holdout_races:
        return DecisionResult(
            decision=DECISION_REJECT, rejected_reason=REJECT_REASON_HOLDOUT,
        )
    if metrics.delta_pp < delta_pp_floor:
        return DecisionResult(
            decision=DECISION_REJECT, rejected_reason=REJECT_REASON_DELTA,
        )
    return DecisionResult(decision=DECISION_ACCEPT, rejected_reason=None)


# ---------------------------------------------------------------------------
# Diagnostics: pairwise Spearman correlations
# ---------------------------------------------------------------------------


def _spearman_rho(left: pd.Series, right: pd.Series) -> float:
    joined = pd.concat({"a": left, "b": right}, axis=1).dropna()
    if joined.shape[0] < 2:
        return float("nan")
    return float(joined["a"].rank().corr(joined["b"].rank(), method="pearson"))


def compute_pairwise_correlations(
    bundles: list[CandidateBundle],
) -> dict[str, float]:
    """Pairwise Spearman ρ on the held-out (race_id, horse) joined normalized scores.

    Returns a dict keyed by ``"<model_a>__<model_b>"`` to support JSON serialization.
    """
    out: dict[str, float] = {}
    n = len(bundles)
    for i in range(n):
        for j in range(i + 1, n):
            left = bundles[i].holdout_df
            right = bundles[j].holdout_df
            merged = left.merge(
                right[["race_id", "ketto_toroku_bango", "normalized_score"]],
                on=["race_id", "ketto_toroku_bango"],
                how="inner",
                suffixes=("_left", "_right"),
            )
            if merged.empty:
                out[f"{bundles[i].model_version}__{bundles[j].model_version}"] = (
                    float("nan")
                )
                continue
            rho = _spearman_rho(
                merged["normalized_score_left"], merged["normalized_score_right"],
            )
            out[f"{bundles[i].model_version}__{bundles[j].model_version}"] = rho
    return out


# ---------------------------------------------------------------------------
# Manifest / summary writers
# ---------------------------------------------------------------------------


def build_manifest(
    args: OptimizeArgs,
    bundles: list[CandidateBundle],
    result: OptimizationResult,
) -> dict[str, object]:
    val_races = int(
        pd.concat([b.validation_df for b in bundles], ignore_index=True)[
            "race_id"
        ].nunique()
    )
    members_payload: list[dict[str, object]] = []
    for idx, bundle in enumerate(bundles):
        members_payload.append({
            "model_version": bundle.model_version,
            "weight": round(result.best_weights[idx], 6),
            "is_baseline": idx == 0,
        })
    val_years_sorted = sorted(args.validation_years)
    hold_years_sorted = sorted(args.holdout_years)
    return {
        "model_version": f"iter23-jra-cb-ensemble-{args.class_code}-v8",
        "category": "jra",
        "kyoso_joken_code": args.class_code,
        "ensemble_type": ENSEMBLE_TYPE,
        "members": members_payload,
        "validation_window": {
            "start_year": val_years_sorted[0],
            "end_year": val_years_sorted[-1],
            "race_count": val_races,
        },
        "holdout_window": {
            "start_year": hold_years_sorted[0],
            "end_year": hold_years_sorted[-1],
            "race_count": result.holdout.n_races,
        },
        "validation_top1": result.validation_top1,
        "holdout_top1": result.holdout.top1,
        "iter14_holdout_top1": result.holdout.iter14_top1,
        "delta_pp": result.holdout.delta_pp,
        "search_method": SEARCH_METHOD,
        "n_trials": args.n_trials,
        "seed": args.seed,
    }


def build_summary(
    args: OptimizeArgs,
    bundles: list[CandidateBundle],
    result: OptimizationResult,
    decision: DecisionResult,
    pairwise: dict[str, float],
) -> dict[str, object]:
    return {
        "class_code": args.class_code,
        "decision": decision.decision,
        "delta_pp": result.holdout.delta_pp,
        "available_members": [b.model_version for b in bundles],
        "best_weights": {
            bundle.model_version: round(result.best_weights[idx], 6)
            for idx, bundle in enumerate(bundles)
        },
        "validation_top1": result.validation_top1,
        "holdout_top1": result.holdout.top1,
        "iter14_holdout_top1": result.holdout.iter14_top1,
        "holdout_races": result.holdout.n_races,
        "wilson_lower_delta": result.holdout.wilson_lower_delta,
        "pairwise_correlations": pairwise,
        "rejected_reason": decision.rejected_reason,
        "min_baseline_weight": args.min_baseline_weight,
        "search_method": SEARCH_METHOD,
        "n_trials": args.n_trials,
        "seed": args.seed,
    }


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------


def run(
    args: OptimizeArgs,
    load_race_meta: LoadRaceMetaFn = default_load_race_meta,
    create_study: CreateStudyFn = default_create_study,
) -> dict[str, object]:
    pg_class_map_df = load_race_meta(args.pg_url)
    bundles = build_bundles(args, pg_class_map_df)
    best_weights, val_top1 = run_optuna_search(bundles, args, create_study)
    holdout = compute_holdout_metrics(bundles, best_weights)
    result = OptimizationResult(
        best_weights=best_weights,
        validation_top1=val_top1,
        holdout=holdout,
    )
    decision = make_decision(
        result.holdout, args.min_holdout_races, args.delta_pp_floor,
    )
    pairwise = compute_pairwise_correlations(bundles)
    summary = build_summary(args, bundles, result, decision, pairwise)
    write_json(args.output_summary, summary)
    if decision.decision == DECISION_ACCEPT and args.output_manifest_dir is not None:
        manifest = build_manifest(args, bundles, result)
        write_json(args.output_manifest_dir / "manifest.json", manifest)
    return summary


def main(argv: list[str] | None = None) -> int:
    args = normalize_args(build_arg_parser().parse_args(argv))
    summary = run(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
