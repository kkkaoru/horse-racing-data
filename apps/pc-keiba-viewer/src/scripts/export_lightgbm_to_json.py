#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Compact JSON exporter for LightGBM boosters used by the Worker tree evaluator.

Reads a LightGBM .txt model produced by `running_style_lightgbm.py train-production`
and emits a trimmed JSON keeping only what the Worker-side JS evaluator needs:
feature_names + tree structure (split_feature, threshold, decision_type,
default_left, left_child, right_child, leaf_value). Stats like gain/weight/count
are dropped to keep the file small (R2 download cost on Worker cold start).

Run with:
  cd apps/pc-keiba-viewer && .venv/bin/python src/scripts/export_lightgbm_to_json.py \\
    --model-dir tmp/models/nar-running-style-lgbm-prod-v1.5 \\
    --output tmp/models/nar-running-style-lgbm-prod-v1.5/model.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TypedDict

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


class CompactModel(TypedDict):
    model_version: str
    objective: str
    num_class: int
    num_tree_per_iteration: int
    class_labels: list[str]
    feature_names: list[str]
    categorical_features: list[str]
    trees: list[dict[str, object]]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="export_lightgbm_to_json")
    parser.add_argument("--model-dir", type=Path, required=True, help="dir with model.txt + metadata.json")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def trim_node(node: dict[str, object]) -> dict[str, object]:
    trimmed: dict[str, object] = {}
    for key in KEEP_NODE_KEYS:
        if key not in node:
            continue
        value = node[key]
        if key in ("left_child", "right_child") and isinstance(value, dict):
            trimmed[key] = trim_node(value)
        else:
            trimmed[key] = value
    return trimmed


def trim_tree(tree: dict[str, object]) -> dict[str, object]:
    structure = tree.get("tree_structure")
    if not isinstance(structure, dict):
        raise ValueError("tree_structure missing in tree dump")
    return {"tree_structure": trim_node(structure)}


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
    trees = [trim_tree(tree) for tree in trees_raw if isinstance(tree, dict)]
    feature_names = dump.get("feature_names")
    if not isinstance(feature_names, list):
        raise ValueError("feature_names missing in booster dump")
    return {
        "model_version": str(metadata.get("model_version", "unknown")),
        "objective": str(dump.get("objective", "multiclass")),
        "num_class": int(dump.get("num_class", 1)),
        "num_tree_per_iteration": int(dump.get("num_tree_per_iteration", 1)),
        "class_labels": list(metadata.get("class_labels", [])),
        "feature_names": [str(name) for name in feature_names],
        "categorical_features": list(metadata.get("categorical_features", [])),
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
