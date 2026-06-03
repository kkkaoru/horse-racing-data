#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Stage 3 of the finish-position v7-lineage walk-forward 21y plan.

Per-fold train -> predict -> output. For each validation year ``N`` in the
requested fold range we train on every race with ``kaisai_nen <= N-1`` (the full
available history, max range) and predict the races in year ``N`` (true
out-of-sample). Architecture is dispatched by ``--category``:

  * ``jra`` / ``banei`` -> CatBoost YetiRank (``--no-cat-features``)
  * ``nar``             -> XGBoost ``rank:pairwise``

Hyperparameters are inherited from ``docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md`` and
duplicated as defaults here so a future run reproduces the deployed lineage.

Two outputs are written per fold:

  * a 14-column Hive parquet (``category=<src>/race_year=<year>/``) whose schema
    matches ``load_bucket_predictions.py`` so Stage 4 bucket-eval can load it;
  * a 5-column JSONL matching ``import-finish-position-predictions.ts``
    ``PredictionRecord`` so Stage 6 deploy can stream it.

This script trains per fold; it does not wait for an external artifact. All
file / model I/O is injected so the unit tests can run fully mocked.

Run with: ``uv run python src/scripts/score_finish_position_walk_forward.py ...``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Protocol, TypedDict, cast

import pandas as pd

import finish_position_catboost as cb_walk
import finish_position_xgboost as xgb_walk

CATEGORY_JRA: str = "jra"
CATEGORY_NAR: str = "nar"
CATEGORY_BANEI: str = "banei"
SUPPORTED_CATEGORIES: tuple[str, str, str] = (CATEGORY_JRA, CATEGORY_NAR, CATEGORY_BANEI)

ARCHITECTURE_CATBOOST: str = "catboost"
ARCHITECTURE_XGBOOST: str = "xgboost"

CATEGORY_ARCHITECTURE: dict[str, str] = {
    CATEGORY_JRA: ARCHITECTURE_CATBOOST,
    CATEGORY_NAR: ARCHITECTURE_XGBOOST,
    CATEGORY_BANEI: ARCHITECTURE_CATBOOST,
}

# Category -> (source value used in race_id / parquet partition, no_cat_features).
CATEGORY_SOURCE: dict[str, str] = {
    CATEGORY_JRA: "jra",
    CATEGORY_NAR: "nar",
    CATEGORY_BANEI: "nar",
}

# Category -> partition / loader-facing category string. Stage 4
# (``evaluate-bucket-21y-v7lineage.ts``) and ``load_bucket_predictions.py`` both
# key the bucket-eval slice on the literal ``ban-ei`` (hyphenated), so the parquet
# ``category`` partition value must use that canonical name even though this
# script's own ``--category`` flag spells it ``banei`` (no hyphen).
CATEGORY_PARTITION: dict[str, str] = {
    CATEGORY_JRA: "jra",
    CATEGORY_NAR: "nar",
    CATEGORY_BANEI: "ban-ei",
}

CATEGORY_NO_CAT_FEATURES: dict[str, bool] = {
    CATEGORY_JRA: True,
    CATEGORY_NAR: False,
    CATEGORY_BANEI: True,
}

EXPECTED_FEATURE_COUNT: dict[str, int] = {
    CATEGORY_JRA: 226,
    CATEGORY_NAR: 175,
    CATEGORY_BANEI: 111,
}

CATEGORY_MODEL_VERSION: dict[str, str] = {
    CATEGORY_JRA: "jra-cb-v7-lineage-wf-21y",
    CATEGORY_NAR: "nar-xgb-v7-lineage-wf-21y",
    CATEGORY_BANEI: "banei-cb-v7-lineage-wf-21y",
}

# CatBoost (jra / banei) inherited defaults.
DEFAULT_CB_ITERATIONS_JRA: int = 500
DEFAULT_CB_ITERATIONS_BANEI: int = 300
DEFAULT_CB_DEPTH: int = 8
DEFAULT_CB_L2: float = 3.0
DEFAULT_CB_LEARNING_RATE: float = 0.05
DEFAULT_CB_RELEVANCE_RANK1: int = 3
DEFAULT_CB_RELEVANCE_RANK2: int = 2
DEFAULT_CB_RELEVANCE_RANK3: int = 1

# XGBoost (nar) inherited defaults.
DEFAULT_XGB_NUM_ROUNDS: int = 450
DEFAULT_XGB_MAX_DEPTH: int = 6
DEFAULT_XGB_RELEVANCE_RANK1: int = 3
DEFAULT_XGB_RELEVANCE_RANK2: int = 2
DEFAULT_XGB_RELEVANCE_RANK3: int = 2

DEFAULT_EARLY_STOPPING_ROUNDS: int = 30
DEFAULT_SEED: int = 20260519

RUNNING_STYLE_FEATURE_VERSION_DEFAULT: str = "v3"
FINISH_POSITION_VERSION_DEFAULT: str = "v1"

PARQUET_OUTPUT_COLUMNS: tuple[str, ...] = (
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "umaban",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
    "model_version",
    "running_style_feature_version",
    "finish_position_version",
    "category",
    "race_year",
)

RACE_ID_PART_COUNT: int = 5
RACE_ID_SEPARATOR: str = ":"


class WalkForwardArguments(TypedDict):
    features_parquet: Path
    category: str
    walk_forward_namespace: str
    year_from: int
    year_to: int
    train_start_date: str
    output_parquet_root: Path
    output_jsonl_dir: Path
    running_style_feature_version: str
    finish_position_version: str
    iterations: int
    depth: int
    l2_leaf_reg: float
    learning_rate: float
    num_rounds: int
    max_depth: int
    relevance_rank1: int
    relevance_rank2: int
    relevance_rank3: int
    early_stopping_rounds: int
    seed: int
    iteration_id: int
    calibration_path: Path | None


class ParquetReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class FeatureResolverLike(Protocol):
    def __call__(self, df: pd.DataFrame, *, use_cat_features: bool) -> list[str]: ...


class FoldTrainerLike(Protocol):
    def __call__(
        self,
        train_df: pd.DataFrame,
        valid_df: pd.DataFrame,
        feature_cols: list[str],
        args: argparse.Namespace,
    ) -> pd.DataFrame: ...


class ParquetWriterLike(Protocol):
    def __call__(self, frame: pd.DataFrame, output_dir: Path) -> None: ...


class JsonlWriterLike(Protocol):
    def __call__(self, frame: pd.DataFrame, output_path: Path) -> None: ...


def default_iterations_for_category(category: str) -> int:
    if category == CATEGORY_BANEI:
        return DEFAULT_CB_ITERATIONS_BANEI
    return DEFAULT_CB_ITERATIONS_JRA


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="score_finish_position_walk_forward")
    parser.add_argument("--features-parquet", type=Path, required=True)
    parser.add_argument("--category", type=str, choices=list(SUPPORTED_CATEGORIES), required=True)
    parser.add_argument("--walk-forward-namespace", type=str, required=True)
    parser.add_argument("--year-from", type=int, required=True)
    parser.add_argument("--year-to", type=int, required=True)
    parser.add_argument("--train-start-date", type=str, required=True)
    parser.add_argument("--output-parquet-root", type=Path, required=True)
    parser.add_argument("--output-jsonl-dir", type=Path, required=True)
    parser.add_argument(
        "--running-style-feature-version",
        type=str,
        default=RUNNING_STYLE_FEATURE_VERSION_DEFAULT,
    )
    parser.add_argument(
        "--finish-position-version", type=str, default=FINISH_POSITION_VERSION_DEFAULT,
    )
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--depth", type=int, default=DEFAULT_CB_DEPTH)
    parser.add_argument("--l2-leaf-reg", type=float, default=DEFAULT_CB_L2)
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_CB_LEARNING_RATE)
    parser.add_argument("--num-rounds", type=int, default=DEFAULT_XGB_NUM_ROUNDS)
    parser.add_argument("--max-depth", type=int, default=DEFAULT_XGB_MAX_DEPTH)
    parser.add_argument("--relevance-rank1", type=int, default=None)
    parser.add_argument("--relevance-rank2", type=int, default=None)
    parser.add_argument("--relevance-rank3", type=int, default=None)
    parser.add_argument("--early-stopping-rounds", type=int, default=DEFAULT_EARLY_STOPPING_ROUNDS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--iteration-id", type=int, default=0)
    parser.add_argument("--calibration-path", type=Path, default=None)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def resolve_relevance(category: str, raw: int | None, cb_default: int, xgb_default: int) -> int:
    if raw is not None:
        return raw
    if CATEGORY_ARCHITECTURE[category] == ARCHITECTURE_XGBOOST:
        return xgb_default
    return cb_default


def normalize_arguments(args: argparse.Namespace) -> WalkForwardArguments:
    category = cast(str, args.category)
    iterations = (
        int(args.iterations)
        if args.iterations is not None
        else default_iterations_for_category(category)
    )
    return {
        "features_parquet": Path(args.features_parquet),
        "category": category,
        "walk_forward_namespace": args.walk_forward_namespace,
        "year_from": int(args.year_from),
        "year_to": int(args.year_to),
        "train_start_date": args.train_start_date,
        "output_parquet_root": Path(args.output_parquet_root),
        "output_jsonl_dir": Path(args.output_jsonl_dir),
        "running_style_feature_version": args.running_style_feature_version,
        "finish_position_version": args.finish_position_version,
        "iterations": iterations,
        "depth": int(args.depth),
        "l2_leaf_reg": float(args.l2_leaf_reg),
        "learning_rate": float(args.learning_rate),
        "num_rounds": int(args.num_rounds),
        "max_depth": int(args.max_depth),
        "relevance_rank1": resolve_relevance(
            category, args.relevance_rank1, DEFAULT_CB_RELEVANCE_RANK1, DEFAULT_XGB_RELEVANCE_RANK1,
        ),
        "relevance_rank2": resolve_relevance(
            category, args.relevance_rank2, DEFAULT_CB_RELEVANCE_RANK2, DEFAULT_XGB_RELEVANCE_RANK2,
        ),
        "relevance_rank3": resolve_relevance(
            category, args.relevance_rank3, DEFAULT_CB_RELEVANCE_RANK3, DEFAULT_XGB_RELEVANCE_RANK3,
        ),
        "early_stopping_rounds": int(args.early_stopping_rounds),
        "seed": int(args.seed),
        "iteration_id": int(args.iteration_id),
        "calibration_path": (
            Path(args.calibration_path) if args.calibration_path is not None else None
        ),
    }


def resolve_fold_years(args: WalkForwardArguments) -> list[int]:
    """The fold range is inclusive on both ends; each year is one true-OOS fold."""
    if args["year_to"] < args["year_from"]:
        raise ValueError(
            f"--year-to ({args['year_to']}) must be >= --year-from ({args['year_from']}).",
        )
    return list(range(args["year_from"], args["year_to"] + 1))


def assert_feature_count(category: str, feature_cols: list[str]) -> None:
    expected = EXPECTED_FEATURE_COUNT[category]
    actual = len(feature_cols)
    if actual != expected:
        raise ValueError(
            f"Feature count parity guard failed for category={category}: "
            f"expected {expected} features but resolved {actual}.",
        )


def resolve_feature_columns_for_category(
    df: pd.DataFrame,
    category: str,
    *,
    catboost_resolver: FeatureResolverLike,
    xgboost_resolver: FeatureResolverLike,
) -> list[str]:
    if CATEGORY_ARCHITECTURE[category] == ARCHITECTURE_XGBOOST:
        return xgboost_resolver(df, use_cat_features=False)
    use_cat = not CATEGORY_NO_CAT_FEATURES[category]
    return catboost_resolver(df, use_cat_features=use_cat)


def build_fold_namespace_args(args: WalkForwardArguments, valid_year: int) -> argparse.Namespace:
    """Build the argparse.Namespace the reused catboost/xgboost trainers expect.

    Categorical handling: jra/banei pass ``no_cat_features=True`` so unseen levels
    (e.g. a new keibajo_code in 2026) never reach the model; nar's numeric-only
    feature resolver already drops the categorical columns, so unseen levels are
    likewise impossible to crash the booster.
    """
    train_end = f"{valid_year - 1}1231"
    return argparse.Namespace(
        train_start_date=args["train_start_date"],
        train_end_date=train_end,
        iterations=args["iterations"],
        depth=args["depth"],
        l2_leaf_reg=args["l2_leaf_reg"],
        learning_rate=args["learning_rate"],
        num_rounds=args["num_rounds"],
        max_depth=args["max_depth"],
        min_child_weight=xgb_walk.DEFAULT_MIN_CHILD_WEIGHT,
        reg_lambda=xgb_walk.DEFAULT_LAMBDA,
        relevance_rank1=args["relevance_rank1"],
        relevance_rank2=args["relevance_rank2"],
        relevance_rank3=args["relevance_rank3"],
        early_stopping_rounds=args["early_stopping_rounds"],
        seed=args["seed"],
        no_cat_features=CATEGORY_NO_CAT_FEATURES[args["category"]],
    )


def default_train_catboost_fold(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    args: argparse.Namespace,
) -> pd.DataFrame:
    result = cb_walk.train_catboost_ranker(train_df, valid_df, feature_cols, args)
    return cast(pd.DataFrame, result["valid_predictions"])


def default_train_xgboost_fold(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    args: argparse.Namespace,
) -> pd.DataFrame:
    _, result = xgb_walk.train_xgboost_ranker(train_df, valid_df, feature_cols, args)
    return cast(pd.DataFrame, result["valid_predictions"])


def resolve_fold_trainer(
    category: str,
    *,
    catboost_trainer: FoldTrainerLike,
    xgboost_trainer: FoldTrainerLike,
) -> FoldTrainerLike:
    if CATEGORY_ARCHITECTURE[category] == ARCHITECTURE_XGBOOST:
        return xgboost_trainer
    return catboost_trainer


def split_race_id_column(frame: pd.DataFrame) -> pd.DataFrame:
    parts = frame["race_id"].str.split(RACE_ID_SEPARATOR, expand=True)
    if parts.shape[1] != RACE_ID_PART_COUNT:
        raise ValueError(
            f"race_id must contain {RACE_ID_PART_COUNT} colon-separated parts; got {parts.shape[1]}.",
        )
    out = frame.copy()
    out["source"] = parts[0]
    out["kaisai_nen"] = parts[1]
    out["kaisai_tsukihi"] = parts[2]
    out["keibajo_code"] = parts[3]
    out["race_bango"] = parts[4]
    return out


def to_parquet_frame(
    predictions: pd.DataFrame,
    args: WalkForwardArguments,
    valid_year: int,
) -> pd.DataFrame:
    """Project the fold predictions onto the 14-column bucket-eval schema.

    The bucket-eval aggregate SQL only reads ``predicted_rank`` /
    ``predicted_score``, so the rank-only CatBoost / XGBoost rankers leave
    ``predicted_top1_prob`` / ``predicted_top3_prob`` null; the loader's temp
    table declares both columns as nullable ``numeric``.
    """
    split = split_race_id_column(predictions)
    frame = pd.DataFrame({
        "source": split["source"],
        "kaisai_nen": split["kaisai_nen"],
        "kaisai_tsukihi": split["kaisai_tsukihi"],
        "keibajo_code": split["keibajo_code"],
        "race_bango": split["race_bango"],
        "ketto_toroku_bango": split["ketto_toroku_bango"],
        "umaban": split["umaban"],
        "predicted_score": split["predicted_score"].astype(float),
        "predicted_rank": split["predicted_rank"].astype(int),
        "predicted_top1_prob": pd.Series([None] * len(split), dtype="object"),
        "predicted_top3_prob": pd.Series([None] * len(split), dtype="object"),
        "predicted_finish_position": split["predicted_rank"].astype(int),
        "model_version": args["walk_forward_namespace"],
        "running_style_feature_version": args["running_style_feature_version"],
        "finish_position_version": args["finish_position_version"],
        "category": CATEGORY_PARTITION[args["category"]],
        "race_year": valid_year,
    })
    return frame[list(PARQUET_OUTPUT_COLUMNS)]


def default_write_parquet(frame: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(
        output_dir.as_posix(),
        partition_cols=["category", "race_year"],
        index=False,
    )


def default_write_jsonl(frame: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = frame[["race_id", "ketto_toroku_bango", "umaban", "predicted_score", "predicted_rank"]]
    with output_path.open("w", encoding="utf-8") as fp:
        for row in rows.itertuples(index=False):
            umaban_value = cast(float, row.umaban)
            fp.write(
                json.dumps({
                    "race_id": row.race_id,
                    "ketto_toroku_bango": row.ketto_toroku_bango,
                    "umaban": int(umaban_value) if pd.notna(umaban_value) else None,
                    "predicted_score": float(cast(float, row.predicted_score)),
                    "predicted_rank": int(cast(float, row.predicted_rank)),
                })
                + "\n",
            )
    return None


class ScoreFoldDeps(TypedDict):
    parquet_reader: ParquetReaderLike
    catboost_resolver: FeatureResolverLike
    xgboost_resolver: FeatureResolverLike
    catboost_trainer: FoldTrainerLike
    xgboost_trainer: FoldTrainerLike
    write_parquet: ParquetWriterLike
    write_jsonl: JsonlWriterLike


def build_fold_train_valid(
    df: pd.DataFrame,
    args: WalkForwardArguments,
    valid_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_end = f"{valid_year - 1}1231"
    train_df = cb_walk.filter_range(df, args["train_start_date"], train_end)
    valid_df = cb_walk.filter_year(df, valid_year)
    return train_df, valid_df


def load_calibration_map(path: Path | None) -> dict[str, list[list[float]]]:
    """Read calibration JSON ``{bucket_key: [[score, calibrated], ...]}``.

    Empty / missing path returns an empty map so the fold uses raw scores.
    """
    if path is None:
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(
            f"calibration JSON must be a top-level object, got {type(parsed)!r}",
        )
    return cast(dict[str, list[list[float]]], parsed)


def interp_calibrated(score: float, pairs: list[list[float]]) -> float:
    if not pairs:
        return score
    xs = [float(p[0]) for p in pairs]
    ys = [float(p[1]) for p in pairs]
    if score <= xs[0]:
        return ys[0]
    if score >= xs[-1]:
        return ys[-1]
    for index in range(len(xs) - 1):
        if xs[index] <= score <= xs[index + 1]:
            span = xs[index + 1] - xs[index]
            if span == 0:
                return ys[index]
            ratio = (score - xs[index]) / span
            return ys[index] + ratio * (ys[index + 1] - ys[index])
    return score


def apply_calibration(
    predictions: pd.DataFrame,
    calibration_map: dict[str, list[list[float]]],
    bucket_key: str,
) -> pd.DataFrame:
    """Apply isotonic calibration if ``bucket_key`` is present in the map."""
    if not calibration_map:
        return predictions
    pairs = calibration_map.get(bucket_key)
    if not pairs:
        return predictions
    out = predictions.copy()
    out["predicted_score"] = out["predicted_score"].astype(float).map(
        lambda score: interp_calibrated(float(score), pairs),
    )
    out["predicted_rank"] = (
        out.groupby("race_id")["predicted_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    return out


def score_fold(
    df: pd.DataFrame,
    feature_cols: list[str],
    args: WalkForwardArguments,
    valid_year: int,
    deps: ScoreFoldDeps,
) -> dict[str, object]:
    train_df, valid_df = build_fold_train_valid(df, args, valid_year)
    if len(train_df) == 0 or len(valid_df) == 0:
        return {"fold_year": valid_year, "skipped": True, "rows": 0}
    fold_args = build_fold_namespace_args(args, valid_year)
    trainer = resolve_fold_trainer(
        args["category"],
        catboost_trainer=deps["catboost_trainer"],
        xgboost_trainer=deps["xgboost_trainer"],
    )
    predictions = trainer(train_df, valid_df, feature_cols, fold_args)
    calibration_map = load_calibration_map(args["calibration_path"])
    predictions = apply_calibration(predictions, calibration_map, args["category"])
    parquet_frame = to_parquet_frame(predictions, args, valid_year)
    deps["write_parquet"](parquet_frame, args["output_parquet_root"])
    jsonl_path = args["output_jsonl_dir"] / build_jsonl_filename(args, valid_year)
    deps["write_jsonl"](predictions, jsonl_path)
    return {"fold_year": valid_year, "skipped": False, "rows": int(len(predictions))}


def build_jsonl_filename(args: WalkForwardArguments, valid_year: int) -> str:
    return f"{args['category']}-v7-lineage-wf-21y-{valid_year}.jsonl"


def run(args: WalkForwardArguments, deps: ScoreFoldDeps) -> dict[str, object]:
    df = deps["parquet_reader"](args["features_parquet"])
    feature_cols = resolve_feature_columns_for_category(
        df,
        args["category"],
        catboost_resolver=deps["catboost_resolver"],
        xgboost_resolver=deps["xgboost_resolver"],
    )
    assert_feature_count(args["category"], feature_cols)
    fold_years = resolve_fold_years(args)
    folds = [score_fold(df, feature_cols, args, valid_year, deps) for valid_year in fold_years]
    return {
        "category": args["category"],
        "model_version": args["walk_forward_namespace"],
        "fold_count": len(folds),
        "folds": folds,
        "feature_count": len(feature_cols),
    }


def build_default_deps() -> ScoreFoldDeps:
    return {
        "parquet_reader": cb_walk.load_parquet_dir,
        "catboost_resolver": cb_walk.resolve_feature_columns,
        "xgboost_resolver": xgboost_numeric_resolver,
        "catboost_trainer": default_train_catboost_fold,
        "xgboost_trainer": default_train_xgboost_fold,
        "write_parquet": default_write_parquet,
        "write_jsonl": default_write_jsonl,
    }


def xgboost_numeric_resolver(df: pd.DataFrame, *, use_cat_features: bool) -> list[str]:
    """Adapter so the XGBoost numeric-only resolver matches FeatureResolverLike.

    XGBoost trains on numeric features only (categorical columns are excluded by
    ``finish_position_xgboost.META_COLUMNS``), so ``use_cat_features`` is ignored.
    """
    _ = use_cat_features
    return xgb_walk.resolve_feature_columns(df)


def main(argv: list[str] | None = None) -> None:
    args = normalize_arguments(parse_args(argv))
    result = run(args, build_default_deps())
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
