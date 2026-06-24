#!/usr/bin/env python3
"""Iter 13 (L3): Optuna HPO for JRA CatBoost on iter 9 pacestyle features.

Bayesian search over 50 trials x 3-fold leave-one-year-out CV (2023/2024/2025).
Multi-objective Pareto over (global NDCG@3, worst-bucket NDCG@3) with picker
weight ``0.7 * global + 0.3 * worst_bucket``.

Search space (stability-10 floor enforced):

- ``depth in [4, 9]`` int (ceil clamped to 10)
- ``learning_rate in [0.04, 0.08]`` log (floor 0.04)
- ``l2_leaf_reg in [3, 10]`` log (floor 3)
- ``bagging_temperature in [0.0, 1.5]`` float
- ``random_strength in [0.5, 5.0]`` float
- ``subsample in [0.6, 1.0]`` float (applied when bagging > 0)
- ``iterations in [400, 1000]`` int

Output:

- ``<output_dir>/best-params.json``
- ``<output_dir>/pareto-front.json``
- ``<output_dir>/study-summary.json``
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol, cast

import numpy as np
import polars as pl

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence


CV_YEARS: Final[tuple[int, ...]] = (2023, 2024, 2025)
HPO_PICKER_GLOBAL_WEIGHT: Final[float] = 0.7
HPO_PICKER_WORST_WEIGHT: Final[float] = 0.3
LR_FLOOR: Final[float] = 0.04
L2_FLOOR: Final[float] = 3.0
DEPTH_CEIL: Final[int] = 10
NDCG_AT_K: Final[int] = 3
NDCG_LOG2_OFFSET: Final[float] = 2.0
RELEVANCE_RANK1: Final[int] = 3
RELEVANCE_RANK2: Final[int] = 2
RELEVANCE_RANK3: Final[int] = 1
EARLY_STOPPING_ROUNDS: Final[int] = 30
DEFAULT_N_TRIALS: Final[int] = 50
DEFAULT_TIMEOUT_SECONDS: Final[int] = 14400
DEFAULT_RANDOM_SEED: Final[int] = 42
WORST_BUCKET_MIN_SUPPORT: Final[int] = 50
META_COLUMNS: Final[tuple[str, ...]] = (
    "race_id", "race_date", "race_year", "source", "kaisai_nen", "kaisai_tsukihi",
    "race_bango", "ketto_toroku_bango", "bamei",
    "kishumei_ryakusho", "chokyoshimei_ryakusho", "category",
)
LABEL_COLUMNS: Final[tuple[str, ...]] = ("finish_position", "finish_norm")
EXCLUDED_COLS: Final[frozenset[str]] = frozenset(META_COLUMNS) | frozenset(LABEL_COLUMNS)
EXTRA_NON_FEATURE_COLS: Final[frozenset[str]] = frozenset({
    "target_race_id",
    "kyori_band",
    "season_band",
    "feature_schema_version",
    "futan_weight_class",
    "keibajo_code",
})


@dataclass(frozen=True)
class TuneArgs:
    features_parquet_root: Path
    bucket_membership_parquet_root: Path
    output_dir: Path
    n_trials: int
    timeout_seconds: int
    random_seed: int
    cv_years: tuple[int, ...]


class TrialLike(Protocol):
    def suggest_int(self, name: str, low: int, high: int) -> int: ...
    def suggest_float(
        self, name: str, low: float, high: float, *, log: bool = False,
    ) -> float: ...


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tune_finish_position_jra_cb")
    parser.add_argument("--features-parquet-root", type=Path, required=True)
    parser.add_argument("--bucket-membership-parquet", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--n-trials", type=int, default=DEFAULT_N_TRIALS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--random-seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument(
        "--cv-years", type=int, nargs="+", default=list(CV_YEARS),
    )
    return parser


def normalize_args(args: argparse.Namespace) -> TuneArgs:
    return TuneArgs(
        features_parquet_root=Path(cast(str, args.features_parquet_root)),
        bucket_membership_parquet_root=Path(cast(str, args.bucket_membership_parquet)),
        output_dir=Path(cast(str, args.output_dir)),
        n_trials=int(cast(int, args.n_trials)),
        timeout_seconds=int(cast(int, args.timeout)),
        random_seed=int(cast(int, args.random_seed)),
        cv_years=tuple(int(y) for y in cast("Sequence[int]", args.cv_years)),
    )


def to_relevance(value: object) -> int:
    if value is None:
        return 0
    fv = float(cast(float, value))
    if fv != fv:
        return 0
    iv = int(fv)
    if iv == 1:
        return RELEVANCE_RANK1
    if iv == 2:
        return RELEVANCE_RANK2
    if iv == 3:
        return RELEVANCE_RANK3
    return 0


def resolve_feature_columns(df: pl.DataFrame) -> list[str]:
    out: list[str] = []
    for c in df.columns:
        if c in EXCLUDED_COLS or c in EXTRA_NON_FEATURE_COLS:
            continue
        dtype = df.schema[c]
        if dtype == pl.Boolean:
            continue
        if not dtype.is_numeric():
            continue
        out.append(c)
    return out


def load_year_parquet(root: Path, year: int) -> pl.DataFrame | None:
    year_dir = root / f"race_year={year}"
    if not year_dir.exists():
        return None
    files = sorted(glob.glob(str(year_dir / "*.parquet")))
    if not files:
        return None
    parts = [pl.read_parquet(f) for f in files]
    df = pl.concat(parts) if len(parts) > 1 else parts[0]
    df = df.with_columns(pl.lit(year).alias("race_year"))
    if "race_id" in df.columns and "ketto_toroku_bango" in df.columns:
        df = df.unique(
            subset=["race_id", "ketto_toroku_bango"], maintain_order=True,
        )
    return df


def load_bucket_year(root: Path, year: int) -> pl.DataFrame | None:
    year_dir = root / "category=jra" / f"race_year={year}"
    if not year_dir.exists():
        return None
    files = sorted(glob.glob(str(year_dir / "*.parquet")))
    if not files:
        return None
    return pl.read_parquet(files[0])


def build_group_sizes(df: pl.DataFrame) -> list[int]:
    return df.group_by("race_id", maintain_order=True).len()["len"].to_list()


def dcg_at_k(relevances: list[int], k: int = NDCG_AT_K) -> float:
    if not relevances:
        return 0.0
    head = relevances[:k]
    total = 0.0
    for idx, rel in enumerate(head):
        denom = float(np.log2(float(idx) + NDCG_LOG2_OFFSET))
        total += float(rel) / denom
    return total


def ndcg_at_k_per_race(
    predictions: np.ndarray, labels: np.ndarray, k: int = NDCG_AT_K,
) -> float:
    if predictions.size == 0:
        return 0.0
    order = np.argsort(-predictions, kind="stable")
    ranked_labels = labels[order].tolist()
    ideal_labels = sorted(labels.tolist(), reverse=True)
    dcg = dcg_at_k(ranked_labels, k=k)
    idcg = dcg_at_k(ideal_labels, k=k)
    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def compute_global_ndcg(
    predictions: np.ndarray, labels: np.ndarray, group_sizes: list[int],
) -> float:
    total = 0.0
    count = 0
    cursor = 0
    for gsize in group_sizes:
        race_pred = predictions[cursor : cursor + gsize]
        race_label = labels[cursor : cursor + gsize]
        total += ndcg_at_k_per_race(race_pred, race_label, k=NDCG_AT_K)
        count += 1
        cursor += gsize
    return (total / count) if count > 0 else 0.0


def compute_worst_bucket_ndcg(
    predictions: np.ndarray,
    labels: np.ndarray,
    group_sizes: list[int],
    bucket_keys_per_race: list[str],
) -> float:
    if not bucket_keys_per_race:
        return 0.0
    bucket_scores: dict[str, list[float]] = {}
    cursor = 0
    for gsize, bucket_key in zip(group_sizes, bucket_keys_per_race, strict=False):
        race_pred = predictions[cursor : cursor + gsize]
        race_label = labels[cursor : cursor + gsize]
        ndcg = ndcg_at_k_per_race(race_pred, race_label, k=NDCG_AT_K)
        bucket_scores.setdefault(bucket_key, []).append(ndcg)
        cursor += gsize
    means: list[float] = []
    for scores in bucket_scores.values():
        if len(scores) >= WORST_BUCKET_MIN_SUPPORT:
            means.append(float(np.mean(scores)))
    if not means:
        return 0.0
    return float(min(means))


def assert_no_race_overlap(train_df: pl.DataFrame, valid_df: pl.DataFrame) -> None:
    train_races = set(train_df["race_id"].cast(pl.String).to_list())
    valid_races = set(valid_df["race_id"].cast(pl.String).to_list())
    overlap = train_races & valid_races
    if overlap:
        raise AssertionError(
            f"race_id overlap detected between train/valid: count={len(overlap)} "
            f"sample={sorted(overlap)[:5]}",
        )


@dataclass(frozen=True)
class FoldFrames:
    train_df: pl.DataFrame
    valid_df: pl.DataFrame
    bucket_df: pl.DataFrame | None
    feature_cols: list[str]


def build_fold_frames(
    features_root: Path,
    bucket_root: Path | None,
    cv_years: tuple[int, ...],
    held_out_year: int,
    feature_cols: list[str],
) -> FoldFrames:
    train_parts: list[pl.DataFrame] = []
    for y in cv_years:
        if y == held_out_year:
            continue
        df = load_year_parquet(features_root, y)
        if df is None:
            continue
        train_parts.append(df)
    if not train_parts:
        raise RuntimeError(
            f"No training years available among {cv_years} excluding {held_out_year}",
        )
    train_df = pl.concat(train_parts, how="diagonal_relaxed")
    valid_df = load_year_parquet(features_root, held_out_year)
    if valid_df is None:
        raise RuntimeError(f"No valid data for year {held_out_year}")
    train_df = train_df.filter(pl.col("finish_position").is_not_null())
    valid_df = valid_df.filter(pl.col("finish_position").is_not_null())
    train_df = train_df.sort(["race_id", "umaban"], maintain_order=True)
    valid_df = valid_df.sort(["race_id", "umaban"], maintain_order=True)
    assert_no_race_overlap(train_df, valid_df)
    available_cols = [c for c in feature_cols if c in valid_df.columns and c in train_df.columns]
    bucket_df = None
    if bucket_root is not None:
        bucket_df = load_bucket_year(bucket_root, held_out_year)
    return FoldFrames(
        train_df=train_df,
        valid_df=valid_df,
        bucket_df=bucket_df,
        feature_cols=available_cols,
    )


def attach_bucket_keys(
    valid_df: pl.DataFrame, bucket_df: pl.DataFrame | None,
) -> list[str]:
    if bucket_df is None:
        return ["__all__"] * int(valid_df["race_id"].n_unique())
    if "race_id" not in bucket_df.columns:
        return ["__all__"] * int(valid_df["race_id"].n_unique())
    key_col_candidates = (
        "bucket_grade_code", "grade_code", "kyoso_joken_code", "bucket_key",
    )
    key_col: str | None = None
    for c in key_col_candidates:
        if c in bucket_df.columns:
            key_col = c
            break
    if key_col is None:
        return ["__all__"] * int(valid_df["race_id"].n_unique())
    bucket_keyed = bucket_df.select(["race_id", key_col]).unique(
        subset=["race_id"], maintain_order=True,
    )
    race_order = valid_df["race_id"].unique(maintain_order=True).to_list()
    lookup = dict(
        zip(
            bucket_keyed["race_id"].cast(pl.String).to_list(),
            bucket_keyed[key_col].cast(pl.String).to_list(),
            strict=False,
        ),
    )
    return [lookup.get(str(rid), "__unknown__") for rid in race_order]


def suggest_params(trial: TrialLike) -> dict[str, object]:
    return {
        "depth": trial.suggest_int("depth", 4, 9),
        "learning_rate": trial.suggest_float(
            "learning_rate", LR_FLOOR, 0.08, log=True,
        ),
        "l2_leaf_reg": trial.suggest_float(
            "l2_leaf_reg", L2_FLOOR, 10.0, log=True,
        ),
        "bagging_temperature": trial.suggest_float(
            "bagging_temperature", 0.0, 1.5,
        ),
        "random_strength": trial.suggest_float(
            "random_strength", 0.5, 5.0,
        ),
        "iterations": trial.suggest_int("iterations", 400, 1000),
    }


def enforce_stability_floor(params: "Mapping[str, object]") -> dict[str, object]:
    merged: dict[str, object] = dict(params)
    lr_raw = cast(float, merged.get("learning_rate", LR_FLOOR))
    merged["learning_rate"] = max(float(lr_raw), LR_FLOOR)
    l2_raw = cast(float, merged.get("l2_leaf_reg", L2_FLOOR))
    merged["l2_leaf_reg"] = max(float(l2_raw), L2_FLOOR)
    depth_raw = cast(int, merged.get("depth", DEPTH_CEIL))
    merged["depth"] = min(int(depth_raw), DEPTH_CEIL)
    return merged


def train_cb_fold(
    train_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    feature_cols: list[str],
    params: "Mapping[str, object]",
    seed: int,
) -> np.ndarray:
    from catboost import CatBoost, Pool  # pyright: ignore[reportMissingTypeStubs]

    train_labels = (
        train_df["finish_position"].map_elements(to_relevance, return_dtype=pl.Int32).to_numpy()
    )
    valid_labels = (
        valid_df["finish_position"].map_elements(to_relevance, return_dtype=pl.Int32).to_numpy()
    )
    train_features = train_df.select(feature_cols).cast(pl.Float32).to_numpy()
    valid_features = valid_df.select(feature_cols).cast(pl.Float32).to_numpy()
    train_groups = train_df["race_id"].cast(pl.Categorical).to_physical().to_numpy()
    valid_groups = valid_df["race_id"].cast(pl.Categorical).to_physical().to_numpy()
    train_pool = Pool(
        data=train_features, label=train_labels, group_id=train_groups,
    )
    valid_pool = Pool(
        data=valid_features, label=valid_labels, group_id=valid_groups,
    )
    cb_params: dict[str, object] = {
        "loss_function": "YetiRank",
        "eval_metric": "NDCG:top=3",
        "iterations": int(cast(int, params["iterations"])),
        "learning_rate": float(cast(float, params["learning_rate"])),
        "depth": int(cast(int, params["depth"])),
        "l2_leaf_reg": float(cast(float, params["l2_leaf_reg"])),
        "bagging_temperature": float(cast(float, params["bagging_temperature"])),
        "random_strength": float(cast(float, params["random_strength"])),
        "bootstrap_type": "Bayesian",
        "od_type": "Iter",
        "od_wait": EARLY_STOPPING_ROUNDS,
        "random_seed": seed,
        "task_type": "CPU",
        "verbose": 0,
    }
    model = CatBoost(cb_params)
    model.fit(train_pool, eval_set=valid_pool, verbose=False)
    return np.asarray(model.predict(valid_pool), dtype=np.float64)


@dataclass(frozen=True)
class FoldDeps:
    fold_frames: "Callable[[int, list[str]], FoldFrames]"
    train_fn: "Callable[[pl.DataFrame, pl.DataFrame, list[str], Mapping[str, object], int], np.ndarray]"


def evaluate_params(
    params: "Mapping[str, object]",
    cv_years: tuple[int, ...],
    seed: int,
    deps: FoldDeps,
    feature_cols: list[str],
) -> tuple[float, float]:
    global_ndcgs: list[float] = []
    worst_ndcgs: list[float] = []
    for held_out in cv_years:
        frames = deps.fold_frames(held_out, feature_cols)
        if not frames.feature_cols:
            continue
        preds = deps.train_fn(
            frames.train_df, frames.valid_df, frames.feature_cols, params, seed,
        )
        labels = (
            frames.valid_df["finish_position"]
            .map_elements(to_relevance, return_dtype=pl.Int32)
            .to_numpy()
        )
        group_sizes = build_group_sizes(frames.valid_df)
        global_ndcgs.append(compute_global_ndcg(preds, labels, group_sizes))
        bucket_keys = attach_bucket_keys(frames.valid_df, frames.bucket_df)
        worst_ndcgs.append(
            compute_worst_bucket_ndcg(preds, labels, group_sizes, bucket_keys),
        )
    return (
        float(np.mean(global_ndcgs)) if global_ndcgs else 0.0,
        float(np.mean(worst_ndcgs)) if worst_ndcgs else 0.0,
    )


def picker_score(global_ndcg: float, worst_ndcg: float) -> float:
    return (
        HPO_PICKER_GLOBAL_WEIGHT * global_ndcg
        + HPO_PICKER_WORST_WEIGHT * worst_ndcg
    )


def pick_best_trial(
    trials: "Sequence[Mapping[str, object]]",
) -> dict[str, object]:
    if not trials:
        raise ValueError("No completed trials to pick from")
    scored = [
        (
            picker_score(
                float(cast(float, t["global_ndcg"])),
                float(cast(float, t["worst_ndcg"])),
            ),
            t,
        )
        for t in trials
    ]
    scored.sort(key=lambda x: -x[0])
    best_score, best_trial = scored[0]
    return {
        "trial_number": best_trial["trial_number"],
        "params": best_trial["params"],
        "global_ndcg": best_trial["global_ndcg"],
        "worst_ndcg": best_trial["worst_ndcg"],
        "picker_score": best_score,
    }


def write_outputs(
    output_dir: Path,
    best: "Mapping[str, object]",
    trials: "Sequence[Mapping[str, object]]",
    study_meta: "Mapping[str, object]",
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    best_params_path = output_dir / "best-params.json"
    best_params_path.write_text(
        json.dumps(best, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    pareto_path = output_dir / "pareto-front.json"
    pareto_path.write_text(
        json.dumps(trials, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    summary_path = output_dir / "study-summary.json"
    summary_path.write_text(
        json.dumps(study_meta, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def run_study(args: TuneArgs) -> dict[str, object]:
    import optuna

    sys.stderr.write("[hpo] loading shared CV feature columns ...\n")
    sample_parts = [load_year_parquet(args.features_parquet_root, y) for y in args.cv_years]
    sample_dfs = [df for df in sample_parts if df is not None]
    if not sample_dfs:
        raise RuntimeError(
            f"Cannot load any CV year {args.cv_years} for feature column resolution",
        )
    combined_sample = pl.concat(sample_dfs, how="diagonal_relaxed")
    feature_cols = resolve_feature_columns(combined_sample)
    sys.stderr.write(f"[hpo] feature_cols count={len(feature_cols)}\n")

    def fold_frames_loader(held_out: int, cols: list[str]) -> FoldFrames:
        return build_fold_frames(
            args.features_parquet_root,
            args.bucket_membership_parquet_root,
            args.cv_years,
            held_out,
            cols,
        )

    deps = FoldDeps(fold_frames=fold_frames_loader, train_fn=train_cb_fold)

    trial_records: list[dict[str, object]] = []

    def objective(trial: optuna.Trial) -> tuple[float, float]:
        raw_params = suggest_params(trial)
        params = enforce_stability_floor(raw_params)
        global_ndcg, worst_ndcg = evaluate_params(
            params, args.cv_years, args.random_seed, deps, feature_cols,
        )
        trial_records.append({
            "trial_number": trial.number,
            "params": params,
            "global_ndcg": global_ndcg,
            "worst_ndcg": worst_ndcg,
        })
        sys.stderr.write(
            f"[hpo] trial={trial.number} g={global_ndcg:.6f} w={worst_ndcg:.6f} "
            f"params={params}\n",
        )
        return global_ndcg, worst_ndcg

    sampler = optuna.samplers.NSGAIISampler(seed=args.random_seed)
    study = optuna.create_study(directions=["maximize", "maximize"], sampler=sampler)
    study.optimize(
        objective,
        n_trials=args.n_trials,
        timeout=args.timeout_seconds,
        show_progress_bar=False,
    )

    best = pick_best_trial(trial_records)
    study_meta = {
        "n_trials_completed": len(trial_records),
        "cv_years": list(args.cv_years),
        "random_seed": args.random_seed,
        "features_parquet_root": str(args.features_parquet_root),
        "bucket_membership_parquet_root": str(args.bucket_membership_parquet_root),
        "best_picker_score": best["picker_score"],
    }
    write_outputs(args.output_dir, best, trial_records, study_meta)
    return {"best": best, "n_trials": len(trial_records)}


def main(argv: list[str] | None = None) -> int:
    args = normalize_args(build_arg_parser().parse_args(argv))
    out = run_study(args)
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
