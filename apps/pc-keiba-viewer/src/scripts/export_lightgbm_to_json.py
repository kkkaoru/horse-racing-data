#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Compact JSON exporter for LightGBM boosters used by the Worker tree evaluator.

Reads a LightGBM .txt model produced by `running_style_lightgbm.py train-production`
and emits a trimmed JSON keeping only what the Worker-side JS evaluator needs:
feature_names + tree structure (split_feature, threshold, decision_type,
default_left, left_child, right_child, leaf_value). Stats like gain/weight/count
are dropped to keep the file small (R2 download cost on Worker cold start).

Categorical (`decision_type == "=="`) split thresholds need a translation step:
whenever a categorical training column was a pandas "category" dtype (which
`running_style_lightgbm.py`'s `to_lgb_frame` always uses), `booster.dump_model()`
encodes the threshold as category-code INDICES into `dump_model()["pandas_categorical"]`,
not the raw feature values the Worker actually feeds through `Number(token)` at
serve time. Left untranslated, every categorical split silently compares the
served raw value against an unrelated index and routes almost every horse
through the wrong branch. `build_categorical_value_maps` / `translate_node_threshold`
resolve indices back to raw values before they are written out, so the compact
JSON (and the flatbin converted from it) already carries the values the Worker
expects.

Run with:
  cd apps/pc-keiba-viewer && .venv/bin/python src/scripts/export_lightgbm_to_json.py \\
    --model-dir tmp/models/nar-running-style-lgbm-prod-v1.5 \\
    --output tmp/models/nar-running-style-lgbm-prod-v1.5/model.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TypedDict, cast

import lightgbm as lgb

KEEP_NODE_KEYS: tuple[str, ...] = (
    "split_feature",
    "threshold",
    "decision_type",
    "default_left",
    "left_child",
    "right_child",
    "leaf_value",
    "leaf_index",
    "cat_boundary",
    "cat_threshold",
    "missing_type",
)

CATEGORICAL_DECISION_TYPE: str = "=="
CATEGORICAL_TOKEN_DELIMITER: str = "||"
# Emitted when every raw category behind a `==` split is non-numeric (so it can
# never be a served value at all - the Worker resolves it to a missing feature
# cell before any categorical comparison runs). `Number(UNREACHABLE_CATEGORICAL_TOKEN)`
# is NaN, and `NaN === value` is always false, so both the JS tree evaluator
# (`evaluateCategoricalSplit`) and the flatbin's `encodeCategoricalThreshold`
# (which filters non-finite tokens out entirely) treat this as "never matches" -
# matching the true semantics without the `Number("") === 0` footgun an empty
# threshold string would otherwise hit.
UNREACHABLE_CATEGORICAL_TOKEN: str = "__unreachable__"


class CompactModel(TypedDict):
    model_version: str
    objective: str
    num_class: int
    num_tree_per_iteration: int
    class_labels: list[str]
    feature_names: list[str]
    categorical_features: list[str]
    trees: list[dict[str, object]]


class CategoricalTranslationContext(TypedDict):
    feature_names: list[str]
    value_maps: dict[str, list[object]]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="export_lightgbm_to_json")
    parser.add_argument("--model-dir", type=Path, required=True, help="dir with model.txt + metadata.json")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def _js_number_or_none(value: object) -> float | None:
    """Mirror the Worker's `Number(token)` coercion of a raw category value.

    `pandas_categorical` entries keep whatever dtype the training column had
    before it was cast to "category" - int/float/bool for a native numeric
    column, str for anything routed through a string cast first. The Worker's
    `toNumberOrNull` calls JS `Number(...)` on whatever Postgres returns for
    that same column, so a raw category only ever reaches a live served
    comparison if this same coercion yields a finite number. A non-numeric
    raw value (e.g. a JRA grade_code letter) can never appear as a served
    number - the Worker resolves it to a missing feature cell instead - so
    this must return `None` rather than inventing a numeric stand-in.
    """
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return 0.0  # JS `Number("")` and `Number("   ")` both coerce to 0.
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _format_categorical_token(value: float) -> str:
    return str(int(value)) if value.is_integer() else repr(value)


def translate_categorical_threshold(raw_categories: list[object], threshold: str) -> str:
    """Convert a `==` node's code-index threshold (e.g. "3||4||5") into the
    raw category values it actually refers to. Indices whose raw category can
    never appear as a finite served number are dropped: the Worker already
    routes those horses through the missing-value branch before a categorical
    comparison ever happens, so keeping them here would be redundant rather
    than merely unreachable.
    """
    indices = [int(token) for token in threshold.split(CATEGORICAL_TOKEN_DELIMITER)]
    tokens = [
        _format_categorical_token(numeric)
        for index in indices
        if 0 <= index < len(raw_categories)
        for numeric in [_js_number_or_none(raw_categories[index])]
        if numeric is not None
    ]
    if not tokens:
        return UNREACHABLE_CATEGORICAL_TOKEN
    return CATEGORICAL_TOKEN_DELIMITER.join(tokens)


def build_categorical_value_maps(
    dump: dict[str, object], metadata: dict[str, object],
) -> dict[str, list[object]]:
    """Map each categorical feature name to the ordered raw category values
    LightGBM assigned during training (`booster.dump_model()["pandas_categorical"]`).

    `pandas_categorical` entries are ordered by the position of each
    categorical column within the full training feature order (`feature_columns`
    in metadata.json), not by the order columns were listed in `categorical_feature`.
    Returns an empty map (a no-op passthrough) when the booster never used a
    pandas "category" dtype at all, or metadata declares no categorical features.
    """
    pandas_categorical = dump.get("pandas_categorical")
    if not isinstance(pandas_categorical, list) or not pandas_categorical:
        return {}
    feature_columns = [str(v) for v in cast(list[object], metadata.get("feature_columns", []))]
    categorical_features = {
        str(v) for v in cast(list[object], metadata.get("categorical_features", []))
    }
    categorical_in_order = [name for name in feature_columns if name in categorical_features]
    return dict(zip(categorical_in_order, cast(list[list[object]], pandas_categorical)))


def translate_node_threshold(
    node: dict[str, object], context: CategoricalTranslationContext,
) -> object:
    threshold = node.get("threshold")
    split_feature = node.get("split_feature")
    if not isinstance(threshold, str) or not isinstance(split_feature, int):
        return threshold
    feature_names = context["feature_names"]
    if split_feature < 0 or split_feature >= len(feature_names):
        return threshold
    raw_categories = context["value_maps"].get(feature_names[split_feature])
    if raw_categories is None:
        return threshold
    return translate_categorical_threshold(raw_categories, threshold)


def trim_node(
    node: dict[str, object], context: CategoricalTranslationContext,
) -> dict[str, object]:
    trimmed: dict[str, object] = {}
    for key in KEEP_NODE_KEYS:
        if key not in node:
            continue
        value = node[key]
        if key in ("left_child", "right_child") and isinstance(value, dict):
            trimmed[key] = trim_node(cast(dict[str, object], value), context)
        elif key == "threshold" and node.get("decision_type") == CATEGORICAL_DECISION_TYPE:
            trimmed[key] = translate_node_threshold(node, context)
        else:
            trimmed[key] = value
    return trimmed


def trim_tree(
    tree: dict[str, object], context: CategoricalTranslationContext,
) -> dict[str, object]:
    structure = tree.get("tree_structure")
    if not isinstance(structure, dict):
        raise ValueError("tree_structure missing in tree dump")
    return {"tree_structure": trim_node(cast(dict[str, object], structure), context)}


def load_metadata(model_dir: Path) -> dict[str, object]:
    metadata_path = model_dir / "metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata.json missing in {model_dir}")
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def build_compact_model(booster: lgb.Booster, metadata: dict[str, object]) -> CompactModel:
    dump = booster.dump_model()
    trees_raw = dump.get("tree_info", [])
    if not isinstance(trees_raw, list):
        raise ValueError("tree_info missing in booster dump")
    feature_names = dump.get("feature_names")
    if not isinstance(feature_names, list):
        raise ValueError("feature_names missing in booster dump")
    context: CategoricalTranslationContext = {
        "feature_names": [str(name) for name in feature_names],
        "value_maps": build_categorical_value_maps(dump, metadata),
    }
    trees = [trim_tree(tree, context) for tree in trees_raw if isinstance(tree, dict)]
    return {
        "model_version": str(metadata.get("model_version", "unknown")),
        "objective": str(dump.get("objective", "multiclass")),
        "num_class": int(dump.get("num_class", 1)),
        "num_tree_per_iteration": int(dump.get("num_tree_per_iteration", 1)),
        "class_labels": [str(v) for v in cast(list[object], metadata.get("class_labels", []))],
        "feature_names": [str(name) for name in feature_names],
        "categorical_features": [
            str(v) for v in cast(list[object], metadata.get("categorical_features", []))
        ],
        "trees": trees,
    }


def write_compact_json(model: CompactModel, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(model, ensure_ascii=False, separators=(",", ":"))
    output_path.write_text(payload, encoding="utf-8")
    return len(payload.encode("utf-8"))


def run_export(model_dir: Path, output_path: Path) -> dict[str, object]:
    metadata = load_metadata(model_dir)
    booster = lgb.Booster(model_file=str(model_dir / "model.txt"))
    compact = build_compact_model(booster, metadata)
    size_bytes = write_compact_json(compact, output_path)
    return {
        "model_version": compact["model_version"],
        "num_trees": len(compact["trees"]),
        "num_features": len(compact["feature_names"]),
        "num_class": compact["num_class"],
        "output": str(output_path),
        "size_bytes": size_bytes,
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = run_export(args.model_dir, args.output)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
