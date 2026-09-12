"""Train Worker-portable JRA cell models from a production target plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Final, Protocol, cast

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from catboost import CatBoostRanker, Pool

from predict_lib.jra_cell_scope import MINIMUM_DIVERSE_TRAINING_RACES
from predict_lib.rank_relevance import (
    DEFAULT_RELEVANCE_MODE,
    RELEVANCE_MODES,
    RelevanceMode,
    parse_relevance_mode,
    rank_gains,
)

IDENTITY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "race_id",
        "race_date",
        "race_year",
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "bamei",
        "kishumei_ryakusho",
        "chokyoshimei_ryakusho",
        "category",
        "track_code",
        "grade_code",
        "umaban",
    }
)
LABEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "finish_position",
        "finish_norm",
        "target_corner_1_norm",
        "target_corner_2_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
    }
)
REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "race_id",
    "race_date",
    "ketto_toroku_bango",
    "umaban",
    "finish_position",
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="train_jra_cell_models")
    parser.add_argument("--features-root", type=Path, required=True)
    parser.add_argument("--production-plan", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--condition-code", default=None)
    parser.add_argument("--iterations", type=int, default=250)
    parser.add_argument("--depth", type=int, default=8)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--thread-count", type=int, default=6)
    parser.add_argument("--relevance-mode", choices=RELEVANCE_MODES, default=DEFAULT_RELEVANCE_MODE)
    parser.add_argument("--fixed-iterations", action="store_true")
    return parser.parse_args(argv)


def numeric_feature_names(schema: pa.Schema) -> list[str]:
    excluded = IDENTITY_COLUMNS | LABEL_COLUMNS
    return [
        field.name
        for field in schema
        if field.name not in excluded
        and (
            pa.types.is_integer(field.type)
            or pa.types.is_floating(field.type)
            or pa.types.is_boolean(field.type)
            or pa.types.is_decimal(field.type)
        )
    ]


def relevance_labels(values: pd.Series) -> np.ndarray:
    numeric = cast(pd.Series, pd.to_numeric(values, errors="coerce"))
    ranks = numeric.fillna(0).astype(int)
    mapped = ranks.map({1: 3, 2: 2, 3: 1}).fillna(0)
    return mapped.to_numpy(dtype=np.int32)


def chronological_race_split(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    races = (
        frame.loc[:, ["race_id", "race_date"]]
        .drop_duplicates()
        .sort_values(["race_date", "race_id"])
    )
    if len(races) < 4:
        return frame.copy(), frame.iloc[0:0].copy()
    validation_count = max(2, math.ceil(len(races) * 0.1))
    validation_ids = cast(pd.Series, races.iloc[-validation_count:]["race_id"]).astype(str).tolist()
    race_id_series = cast(pd.Series, frame["race_id"]).astype(str)
    validation = cast(pd.DataFrame, frame[race_id_series.isin(validation_ids)]).copy()
    training = cast(pd.DataFrame, frame[~race_id_series.isin(validation_ids)]).copy()
    return training, validation


def rank_predictions(
    race_ids: Sequence[str], horse_ids: Sequence[str], scores: Sequence[float]
) -> list[int]:
    if len(race_ids) != len(horse_ids) or len(race_ids) != len(scores):
        raise ValueError("race_ids, horse_ids, and scores must have equal lengths")
    ranks = [0] * len(scores)
    grouped: dict[str, list[int]] = {}
    for index, race_id in enumerate(race_ids):
        grouped.setdefault(race_id, []).append(index)
    for indices in grouped.values():
        ordered = sorted(indices, key=lambda index: (-scores[index], horse_ids[index]))
        for rank, index in enumerate(ordered, start=1):
            ranks[index] = rank
    return ranks


def winner_topk_metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    races = 0
    hits = {depth: 0 for depth in range(1, 6)}
    for _, group in frame.groupby("race_id", sort=False):
        winner_rows = group[group["finish_position"] == 1]
        if len(winner_rows) != 1:
            continue
        races += 1
        winner_rank = int(winner_rows.iloc[0]["predicted_rank"])
        for depth in hits:
            hits[depth] += int(winner_rank <= depth)
    result: dict[str, float | int] = {"race_count": races}
    for depth, count in hits.items():
        result[f"top{depth}_hits"] = count
        result[f"top{depth}_accuracy"] = count / races if races else 0.0
    return result


def atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


class DatasetFieldLike(Protocol):
    def isin(self, values: list[str]) -> object: ...


class DatasetLike(Protocol):
    schema: pa.Schema

    def to_table(self, *, columns: list[str], filter: object) -> pa.Table: ...


def load_feature_rows(
    dataset: DatasetLike,
    race_ids: Sequence[str],
    feature_names: Sequence[str],
) -> pd.DataFrame:
    columns = list(dict.fromkeys((*REQUIRED_COLUMNS, *feature_names)))
    if not race_ids:
        return pd.DataFrame(columns=columns)
    field_factory = cast(Callable[[str], DatasetFieldLike], ds.__dict__["field"])
    table = dataset.to_table(
        columns=columns,
        filter=field_factory("race_id").isin(list(race_ids)),
    )
    return table.to_pandas()


def feature_matrix(frame: pd.DataFrame, feature_names: Sequence[str]) -> np.ndarray:
    values = frame.loc[:, list(feature_names)].apply(pd.to_numeric, errors="coerce")
    return values.fillna(0.0).to_numpy(dtype=np.float64)


def validate_target_feature_coverage(
    target: pd.DataFrame,
    target_ids: Sequence[str],
    expected_target_rows: int,
    model_version: str,
) -> None:
    observed_target_races = set(cast(pd.Series, target["race_id"]).astype(str))
    if len(target) != expected_target_rows or observed_target_races != set(target_ids):
        raise ValueError(f"target feature coverage mismatch: {model_version}")


def build_rank_pool(
    frame: pd.DataFrame,
    feature_names: Sequence[str],
    *,
    with_labels: bool,
    relevance_mode: RelevanceMode = DEFAULT_RELEVANCE_MODE,
) -> Pool:
    ordered = frame.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    group_ids = pd.factorize(ordered["race_id"].astype(str), sort=False)[0]
    finish_positions = cast(pd.Series, ordered["finish_position"])
    labels = rank_gains(finish_positions.tolist(), mode=relevance_mode) if with_labels else None
    return Pool(
        data=feature_matrix(ordered, feature_names),
        label=labels,
        group_id=group_ids,
        feature_names=list(feature_names),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_diverse_training_scope(cell: dict[str, object], model_version: str) -> None:
    if (
        not bool(cell.get("training_scope_is_diverse"))
        or int(cast(int, cell.get("training_cross_cell_race_count", 0))) <= 0
    ):
        raise ValueError(f"target-cell-only JRA training scope is forbidden: {model_version}")


def validate_complete_training_races(historical: pd.DataFrame, model_version: str) -> int:
    race_count = len(set(cast(pd.Series, historical["race_id"]).astype(str)))
    if race_count < MINIMUM_DIVERSE_TRAINING_RACES:
        raise ValueError(
            f"JRA model has fewer than {MINIMUM_DIVERSE_TRAINING_RACES} complete "
            f"training races: {model_version}"
        )
    return race_count


def resolve_training_parameters(
    cell: dict[str, object], args: argparse.Namespace, model_version: str
) -> tuple[int, int, float]:
    parameters = cast(dict[str, object], cell.get("training_parameters", {}))
    iterations = int(cast(int, parameters.get("iterations", args.iterations)))
    depth = int(cast(int, parameters.get("depth", args.depth)))
    learning_rate = float(cast(float, parameters.get("learning_rate", args.learning_rate)))
    if iterations <= 0 or depth <= 0 or not 0 < learning_rate <= 1:
        raise ValueError(f"invalid JRA training parameters: {model_version}")
    return iterations, depth, learning_rate


def resolve_final_iterations(
    requested: int, best_iteration: int | None, *, fixed_iterations: bool
) -> int:
    if fixed_iterations or best_iteration is None or best_iteration < 0:
        return requested
    return best_iteration + 1


def resolve_cell_relevance_mode(cell: dict[str, object], args: argparse.Namespace) -> RelevanceMode:
    parameters = cell.get("training_parameters", {})
    if not isinstance(parameters, dict):
        raise ValueError("JRA training parameters must be an object")
    return parse_relevance_mode(
        parameters.get("relevance_mode", getattr(args, "relevance_mode", DEFAULT_RELEVANCE_MODE))
    )


def train_cell(
    dataset: DatasetLike,
    cell: dict[str, object],
    feature_names: Sequence[str],
    output_root: Path,
    args: argparse.Namespace,
) -> dict[str, object]:
    model_version = cast(str, cell["model_version"])
    training_ids = cast(list[str], cell["training_race_ids"])
    target_ids = cast(list[str], cell["target_race_ids"])
    validate_diverse_training_scope(cell, model_version)
    iterations, depth, learning_rate = resolve_training_parameters(cell, args, model_version)
    relevance_mode = resolve_cell_relevance_mode(cell, args)
    historical = load_feature_rows(dataset, training_ids, feature_names)
    finish_positions = cast(
        pd.Series, pd.to_numeric(historical["finish_position"], errors="coerce")
    )
    historical = cast(pd.DataFrame, historical[finish_positions > 0]).copy()
    complete_training_race_count = validate_complete_training_races(historical, model_version)
    training, validation = chronological_race_split(historical)
    tuning_model = CatBoostRanker(
        loss_function="YetiRank",
        eval_metric="NDCG:top=3",
        iterations=iterations,
        learning_rate=learning_rate,
        depth=depth,
        l2_leaf_reg=3.0,
        random_seed=20260905,
        task_type="CPU",
        thread_count=args.thread_count,
        verbose=False,
    )
    training_pool = build_rank_pool(
        training, feature_names, with_labels=True, relevance_mode=relevance_mode
    )
    validation_pool = (
        build_rank_pool(validation, feature_names, with_labels=True, relevance_mode=relevance_mode)
        if not validation.empty
        else None
    )
    tuning_model.fit(training_pool, eval_set=validation_pool, early_stopping_rounds=30)
    validation_metrics: dict[str, float | int] | None = None
    final_iterations = iterations
    if validation_pool is not None:
        validation_scores = np.asarray(tuning_model.predict(validation_pool), dtype=np.float64)
        ordered_validation = validation.sort_values(["race_id", "umaban"]).reset_index(drop=True)
        ordered_validation["predicted_score"] = validation_scores
        ordered_validation["predicted_rank"] = rank_predictions(
            ordered_validation["race_id"].astype(str).tolist(),
            ordered_validation["ketto_toroku_bango"].astype(str).tolist(),
            validation_scores.tolist(),
        )
        validation_metrics = winner_topk_metrics(ordered_validation)
        best_iteration = tuning_model.get_best_iteration()
        final_iterations = resolve_final_iterations(
            iterations, best_iteration, fixed_iterations=getattr(args, "fixed_iterations", False)
        )
    model = CatBoostRanker(
        loss_function="YetiRank",
        iterations=final_iterations,
        learning_rate=learning_rate,
        depth=depth,
        l2_leaf_reg=3.0,
        random_seed=20260905,
        task_type="CPU",
        thread_count=args.thread_count,
        verbose=False,
    )
    model.fit(
        build_rank_pool(historical, feature_names, with_labels=True, relevance_mode=relevance_mode)
    )
    artifact_dir = output_root / "finish-position" / "jra" / model_version
    artifact_dir.mkdir(parents=True, exist_ok=True)
    model_path = artifact_dir / "model.json"
    model.save_model(model_path, format="json")
    target = load_feature_rows(dataset, target_ids, feature_names)
    expected_target_rows = int(cast(int, cell["target_runner_count"]))
    validate_target_feature_coverage(target, target_ids, expected_target_rows, model_version)
    target_pool = build_rank_pool(target, feature_names, with_labels=False)
    target_scores = np.asarray(model.predict(target_pool), dtype=np.float64)
    ordered_target = target.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    ordered_target["predicted_score"] = target_scores
    ordered_target["predicted_rank"] = rank_predictions(
        ordered_target["race_id"].astype(str).tolist(),
        ordered_target["ketto_toroku_bango"].astype(str).tolist(),
        target_scores.tolist(),
    )
    predictions = ordered_target.loc[
        :, ["race_id", "ketto_toroku_bango", "umaban", "predicted_score", "predicted_rank"]
    ].to_dict(orient="records")
    metadata = {
        "architecture": "catboost-yetirank",
        "model_version": model_version,
        "feature_count": len(feature_names),
        "feature_names": list(feature_names),
        "cell_id": cell["cell_id"],
        "cell_canonical": cell["canonical"],
        "condition_code": cell["condition_code"],
        "race_identity": cell["race_identity"],
        "training_scope": {
            "selector_version": cell.get(
                "scope_selector_version", "entrant-history-related-course-expansion-20y-v3"
            ),
            "mode": cell.get("training_scope_mode", "target-entrant-history"),
            "cutoff": cell["cutoff"],
            "history_start": cell["history_start"],
            "training_scope_race_count": cell["training_race_count"],
            "complete_training_race_count": complete_training_race_count,
            "training_runner_count": len(historical),
            "training_race_sha256": cell["training_race_sha256"],
            "related_seed_race_count": cell["related_seed_race_count"],
            "training_target_cell_race_count": cell["training_target_cell_race_count"],
            "training_cross_cell_race_count": cell["training_cross_cell_race_count"],
            "training_scope_is_diverse": cell["training_scope_is_diverse"],
            "target_cell_only_training_allowed": False,
            "target_day_outcomes_allowed": False,
        },
        "validation_metrics": validation_metrics,
        "requested_training_parameters": {
            "fixed_iterations": getattr(args, "fixed_iterations", False),
            "relevance_mode": relevance_mode,
            "iterations": iterations,
            "depth": depth,
            "learning_rate": learning_rate,
        },
        "final_iterations": final_iterations,
        "model_sha256": _sha256(model_path),
        "model_size": model_path.stat().st_size,
    }
    atomic_json(artifact_dir / "metadata.json", metadata)
    atomic_json(artifact_dir / "predictions.json", predictions)
    return {
        "model_version": model_version,
        "training_rows": len(historical),
        "target_rows": len(target),
        "validation_metrics": validation_metrics,
        "model_sha256": metadata["model_sha256"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    plan = json.loads(args.production_plan.read_text(encoding="utf-8"))
    cells = cast(list[dict[str, object]], plan["cells"])
    selected = [
        cell
        for cell in cells
        if args.condition_code is None or cell["condition_code"] == args.condition_code
    ]
    dataset_factory = cast(Callable[..., DatasetLike], ds.__dict__["dataset"])
    dataset = dataset_factory(args.features_root, format="parquet", partitioning="hive")
    feature_names = numeric_feature_names(dataset.schema)
    if not feature_names:
        raise ValueError("feature dataset has no numeric model features")
    results = [
        train_cell(dataset, cell, feature_names, args.output_root, args) for cell in selected
    ]
    report = {
        "version": plan["version"],
        "target_date": plan["target_date"],
        "feature_count": len(feature_names),
        "model_count": len(results),
        "models": results,
    }
    report_path = args.output_root / f"training-{str(plan['target_date']).replace('-', '')}.json"
    atomic_json(report_path, report)
    print(json.dumps({"report": str(report_path), **report}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
