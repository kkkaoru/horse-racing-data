"""Optimize Prophet weights for every observed production served branch."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from predict_lib.cell_router import (
    all_conditions_match,
    card_max_race_bango_for_race_id,
    derive_card_max_race_bango_by_card,
    load_cell_router,
)
from predict_lib.prophet_adjustment import adjust_prediction_rows_with_prophet
from predict_lib.prophet_cell_policy import parse_prophet_cell_policy
from predict_upcoming import score_races

from timesfm_finish_position.policy_report import (
    CategoryReport,
    CellReport,
    EvaluationReport,
    MetricReport,
    require_category,
    require_float,
    require_int,
)
from timesfm_finish_position.prophet_policy_optimization import (
    LinearRaceScores,
    LinearRunnerScore,
    optimize_linear_race_weights,
)

DEFAULT_WEIGHT = 0.05
CATEGORIES = ("jra", "nar", "ban-ei")
YEARS = ("2024", "2025", "2026")
ROUTER = load_cell_router()
ALL_ON_POLICY = parse_prophet_cell_policy(
    {
        "version": "served-branch-evaluation-all-on",
        "default_enabled": True,
        "default_weight": DEFAULT_WEIGHT,
        "categories": {},
    }
)


@dataclass(slots=True)
class BranchEvidence:
    category: str
    cell: str
    branch: str
    races: list[LinearRaceScores] = field(default_factory=list)
    years: Counter[str] = field(default_factory=Counter)
    adjustment_fallbacks: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True, slots=True)
class EvaluationPaths:
    root: Path
    models: Path
    jra_frames: dict[str, Path]
    nar_frame: Path
    banei_frames: dict[str, Path]
    lookup: Path
    output: Path


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--models", type=Path, default=root / "apps/finish-position-predict-container/models"
    )
    parser.add_argument(
        "--jra-2024",
        type=Path,
        default=root / "apps/pc-keiba-viewer/tmp/candidate-jra-jockey-pedigree-cell/"
        "layer-parity-v2/race_year=2024/data_0.parquet",
    )
    parser.add_argument(
        "--jra-2025",
        type=Path,
        default=root / "apps/pc-keiba-viewer/tmp/candidate-jra-jockey-pedigree-cell/"
        "layer-parity-v2/race_year=2025/data_0.parquet",
    )
    parser.add_argument(
        "--jra-2026",
        type=Path,
        default=root / "apps/pc-keiba-viewer/tmp/candidate-jra-top1-querysoftmax-2026-08-25/"
        "rebuilt-2026/layers/17-add-jra-jockey-pedigree-cell-features/"
        "race_year=2026/data_0.parquet",
    )
    parser.add_argument(
        "--nar-frame",
        type=Path,
        default=root / "apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/"
        "nar_causal_weather_timesa_fixed_frame.parquet",
    )
    parser.add_argument(
        "--banei-2024",
        type=Path,
        default=root / "apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/"
        "banei-causal-weather-final/race_year=2024/data_0.parquet",
    )
    parser.add_argument(
        "--banei-2025",
        type=Path,
        default=root / "apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/"
        "banei-causal-weather-final/race_year=2025/data_0.parquet",
    )
    parser.add_argument(
        "--banei-2026",
        type=Path,
        default=root / "apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/"
        "banei-2026-final/race_year=2026/data_0.parquet",
    )
    parser.add_argument(
        "--lookup",
        type=Path,
        default=root / "apps/pc-keiba-viewer/finish-position/lookups/"
        "prophet-entity-trends-all-categories-2026.parquet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "apps/timesfm-finish-position/results/"
        "prophet-served-branch-weight-optimization-2024-2026.json",
    )
    return parser.parse_args()


def evaluation_paths(args: argparse.Namespace) -> EvaluationPaths:
    return EvaluationPaths(
        root=Path(__file__).resolve().parents[3],
        models=args.models,
        jra_frames={"2024": args.jra_2024, "2025": args.jra_2025, "2026": args.jra_2026},
        nar_frame=args.nar_frame,
        banei_frames={
            "2024": args.banei_2024,
            "2025": args.banei_2025,
            "2026": args.banei_2026,
        },
        lookup=args.lookup,
        output=args.output,
    )


def load_lab_trends(path: Path) -> dict[tuple[str, str], float]:
    table = pq.read_table(path, columns=["race_id", "horse_id", "ranking_score"])
    return {
        (str(race_id), str(horse_id)): float(score)
        for race_id, horse_id, score in zip(
            table.column("race_id").to_pylist(),
            table.column("horse_id").to_pylist(),
            table.column("ranking_score").to_pylist(),
            strict=True,
        )
    }


def load_trends(paths: EvaluationPaths) -> dict[tuple[str, str, str], float]:
    result: dict[tuple[str, str, str], float] = {}
    for category, folder in (
        ("jra", "prophet-jra-lab"),
        ("nar", "prophet-lab"),
        ("ban-ei", "prophet-lab"),
    ):
        for year in ("2024", "2025"):
            loaded = load_lab_trends(
                paths.root
                / "apps/timesfm-finish-position/tmp"
                / folder
                / f"prophet-entity-trends-{year}.parquet"
            )
            result.update(
                ((category, race_id, horse_id), value)
                for (race_id, horse_id), value in loaded.items()
            )
    return result


def load_lookup(path: Path) -> dict[tuple[str, str, str, str], float]:
    table = pq.read_table(path)
    return {
        (str(category), str(date), str(entity_type), str(code)): float(yhat)
        for category, date, entity_type, code, yhat in zip(
            table.column("category").to_pylist(),
            table.column("forecast_date").to_pylist(),
            table.column("entity_type").to_pylist(),
            table.column("entity_code").to_pylist(),
            table.column("yhat").to_pylist(),
            strict=True,
        )
    }


def compact_race_id(race_id: str) -> str:
    parts = race_id.split(":")
    if len(parts) != 5:
        raise ValueError(f"Invalid feature-frame race_id: {race_id}")
    return f"{parts[0]}:{parts[1]}{parts[2]}:{parts[3]}:{parts[4]}"


def _apply_historical_trends(
    category: str,
    year: str,
    entries: list[dict[str, object]],
    trends: dict[tuple[str, str, str], float],
) -> None:
    race_id = compact_race_id(str(entries[0]["race_id"]))
    for entry in entries:
        horse_id = str(entry["ketto_toroku_bango"])
        trend = trends.get((category, race_id, horse_id))
        entry["prophet_entity_performance_mean"] = trend
        entry["prophet_entity_coverage"] = 3 if trend is not None else 0


def _apply_2026_lookup(
    category: str,
    entries: list[dict[str, object]],
    lookup: dict[tuple[str, str, str, str], float],
) -> None:
    date = str(entries[0]["race_date"])
    for entry in entries:
        values: list[float] = []
        coverage = 0
        for entity_type, field_name in (
            ("venue", "keibajo_code"),
            ("jockey", "kishu_code"),
            ("trainer", "chokyoshi_code"),
        ):
            code = str(entry.get(field_name) or "").strip()
            exact = lookup.get((category, date, entity_type, code))
            fallback = lookup.get((category, date, entity_type, "__fallback__"))
            if exact is not None:
                coverage += 1
                values.append(exact)
            elif fallback is not None:
                values.append(fallback)
        entry["prophet_entity_performance_mean"] = (
            sum(values) / len(values) if len(values) == 3 else None
        )
        entry["prophet_entity_coverage"] = coverage


def enrich_entries(
    category: str,
    year: str,
    entries: list[dict[str, object]],
    trends: dict[tuple[str, str, str], float],
    lookup: dict[tuple[str, str, str, str], float],
) -> None:
    for entry in entries:
        entry["category"] = category
        entry["source"] = "jra" if category == "jra" else "nar"
        if category != "jra" and entry.get("track_code") is None:
            entry["track_code"] = "0"
    if year == "2026":
        _apply_2026_lookup(category, entries, lookup)
    else:
        _apply_historical_trends(category, year, entries, trends)


def has_finish_position(entry: dict[str, object]) -> bool:
    value = entry.get("finish_position")
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 1


def read_jra_races(path: Path) -> dict[str, list[dict[str, object]]]:
    groups: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for entry in pq.read_table(path).to_pylist():
        if has_finish_position(entry):
            groups[str(entry["race_id"])].append(entry)
    return dict(groups)


def read_nar_races(
    path: Path,
    year: str,
) -> dict[str, list[dict[str, object]]]:
    date_filter = (ds.field("race_date") >= f"{year}0101") & (
        ds.field("race_date") <= f"{year}1231"
    )
    table = ds.dataset(path, format="parquet").to_table(filter=date_filter)
    groups: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for entry in table.to_pylist():
        if has_finish_position(entry):
            groups[str(entry["race_id"])].append(entry)
    return dict(groups)


def policy_cell(
    category: str,
    entries: list[dict[str, object]],
    card_max_race_bango: int | None,
) -> str:
    routing = ROUTER.routing_for(category)
    normal = ROUTER.resolve_variant(category, entries, card_max_race_bango=card_max_race_bango)
    rule_variants = {rule.variant for rule in routing.rules}
    if normal != routing.default_variant and normal not in rule_variants:
        return normal
    first = entries[0]
    for rule in routing.rules:
        if all_conditions_match(
            first,
            rule.conditions,
            category,
            field_size=len(entries),
            card_max_race_bango=card_max_race_bango,
        ):
            return rule.variant
    return routing.default_variant


def resolved_cells(
    category: str,
    races: dict[str, list[dict[str, object]]],
) -> dict[str, str]:
    card_maxima = derive_card_max_race_bango_by_card(races)
    return {
        race_id: policy_cell(
            category,
            entries,
            card_max_race_bango_for_race_id(race_id, card_maxima),
        )
        for race_id, entries in races.items()
    }


def linear_race(
    rows: list[list[object]],
    entries: list[dict[str, object]],
    category: str,
    cell: str,
) -> tuple[LinearRaceScores, str]:
    by_horse = {str(entry["ketto_toroku_bango"]): entry for entry in entries}
    aligned_entries = [by_horse[str(row[6])] for row in rows]
    adjusted = adjust_prediction_rows_with_prophet(
        rows,
        aligned_entries,
        require_category(category),
        environment={"PROPHET_SCORE_ADJUSTMENT_WEIGHT": "1.0"},
        cell_variant=cell,
        policy=ALL_ON_POLICY,
    )
    adjusted_scores = {str(row[6]): require_float(row[8]) for row in adjusted.rows}
    runners = tuple(
        LinearRunnerScore(
            horse_id=str(row[6]),
            intercept=require_float(row[8]),
            adjustment=adjusted_scores[str(row[6])] - require_float(row[8]),
        )
        for row in rows
    )
    winners = tuple(
        runner
        for runner in runners
        if require_int(by_horse[runner.horse_id]["finish_position"]) == 1
    )
    winner_ids = {winner.horse_id for winner in winners}
    competitors = tuple(runner for runner in runners if runner.horse_id not in winner_ids)
    if not winners:
        raise ValueError("Scored race has no official winner")
    return LinearRaceScores(winners=winners, competitors=competitors), adjusted.reason


def collect_evidence(
    paths: EvaluationPaths,
) -> tuple[dict[tuple[str, str, str], BranchEvidence], Counter[str]]:
    trends = load_trends(paths)
    lookup = load_lookup(paths.lookup)
    evidence: dict[tuple[str, str, str], BranchEvidence] = {}
    skipped: Counter[str] = Counter()
    previous_enabled = os.environ.get("PROPHET_SCORE_ADJUSTMENT_ENABLED")
    os.environ["PROPHET_SCORE_ADJUSTMENT_ENABLED"] = "off"
    try:
        for category in CATEGORIES:
            for year in YEARS:
                if category == "jra":
                    races = read_jra_races(paths.jra_frames[year])
                elif category == "nar":
                    races = read_nar_races(paths.nar_frame, year)
                else:
                    races = read_jra_races(paths.banei_frames[year])
                for entries in races.values():
                    enrich_entries(category, year, entries, trends, lookup)
                cells = resolved_cells(category, races)
                scored = score_races(
                    races,
                    category,
                    paths.models,
                    evaluation_variants_by_race_id=cells,
                )
                for (race_id, entries), rows in zip(races.items(), scored, strict=True):
                    if not rows:
                        skipped[f"{category}-{year}-empty-score"] += 1
                        continue
                    cell = cells[race_id]
                    branch = str(rows[0][0])
                    race_evidence, reason = linear_race(rows, entries, category, cell)
                    key = (category, cell, branch)
                    state = evidence.setdefault(key, BranchEvidence(category, cell, branch))
                    state.races.append(race_evidence)
                    state.years[year] += 1
                    if reason != "applied":
                        state.adjustment_fallbacks[reason] += 1
    finally:
        if previous_enabled is None:
            del os.environ["PROPHET_SCORE_ADJUSTMENT_ENABLED"]
        else:
            os.environ["PROPHET_SCORE_ADJUSTMENT_ENABLED"] = previous_enabled
    return evidence, skipped


def branch_components(category: str, cell: str, branch: str) -> list[dict[str, str]]:
    variants = ROUTER.routing_for(category).variants
    if branch == "iter40-nar-settransformer-blend-v1":
        return [
            {
                "role": "served-composite",
                "model_version": "iter40-nar-settransformer-blend-v1",
            },
            {"role": "base-member", "model_version": "iter12-nar-xgb-hpo-v8-clean188"},
            {"role": "transformer-seed-member", "model_version": "iter40-seed-1"},
            {"role": "transformer-seed-member", "model_version": "iter40-seed-2"},
            {"role": "transformer-seed-member", "model_version": "iter40-seed-3"},
        ]
    if branch == "jra-dirt-small-005-hybrid-v1":
        return [
            {"role": "served-composite", "model_version": branch},
            {
                "role": "base-member",
                "model_version": "jra-cb-v10-prior-corner274-2013",
            },
            {"role": "companion-seed-member", "model_version": "jra-dirt-hybrid-seed-1"},
            {"role": "companion-seed-member", "model_version": "jra-dirt-hybrid-seed-2"},
            {"role": "companion-seed-member", "model_version": "jra-dirt-hybrid-seed-3"},
        ]
    root = variants.get(cell)
    if root is None or root.model_version != branch:
        return [{"role": "served-stage1-or-fallback", "model_version": branch}]
    components: list[dict[str, str]] = []
    visited: set[str] = set()

    def visit(variant: str, role: str) -> None:
        if variant in visited:
            return
        visited.add(variant)
        spec = variants.get(variant)
        if spec is None:
            return
        components.append({"role": role, "variant": variant, "model_version": spec.model_version})
        if spec.base_variant is not None:
            visit(spec.base_variant, "base-member")
        if spec.rerank_variant is not None:
            visit(spec.rerank_variant, "rerank-member")
        for member in spec.consensus_variants:
            visit(member, "consensus-member")

    visit(cell, "served-route")
    if root.routing_mode.startswith("nar_transformer_"):
        components.extend(
            [
                {
                    "role": "base-composite",
                    "model_version": "iter40-nar-settransformer-blend-v1",
                },
                {
                    "role": "base-xgboost-member",
                    "model_version": "iter12-nar-xgb-hpo-v8-clean188",
                },
                {"role": "transformer-seed-member", "model_version": "iter40-seed-1"},
                {"role": "transformer-seed-member", "model_version": "iter40-seed-2"},
                {"role": "transformer-seed-member", "model_version": "iter40-seed-3"},
            ]
        )
    return components


def metric_report(
    baseline_hits: tuple[int, ...],
    adjusted_hits: tuple[int, ...],
    races: int,
) -> dict[str, MetricReport]:
    return {
        f"top{top_k}": {
            "baseline_hits": baseline,
            "adjusted_hits": adjusted,
            "delta_hits": adjusted - baseline,
            "baseline_rate": baseline / races,
            "adjusted_rate": adjusted / races,
            "delta_pp": (adjusted - baseline) / races * 100.0,
        }
        for top_k, (baseline, adjusted) in enumerate(
            zip(baseline_hits, adjusted_hits, strict=True), start=1
        )
    }


def build_report(
    evidence: dict[tuple[str, str, str], BranchEvidence],
    skipped: Counter[str],
) -> EvaluationReport:
    categories: dict[str, CategoryReport] = {}
    for category in CATEGORIES:
        cells: dict[str, CellReport] = {}
        category_states = sorted(
            (state for state in evidence.values() if state.category == category),
            key=lambda state: (state.cell, state.branch),
        )
        for state in category_states:
            optimized = optimize_linear_race_weights(
                tuple(state.races), default_weight=DEFAULT_WEIGHT
            )
            cell = cells.setdefault(state.cell, {"branches": {}})
            branches = cell["branches"]
            branches[state.branch] = {
                "enabled": optimized.enabled,
                "weight": optimized.weight,
                "effective_weight": optimized.weight if optimized.enabled else 0.0,
                "races": len(state.races),
                "years": dict(state.years),
                "adjustment_fallbacks": dict(state.adjustment_fallbacks),
                "components": branch_components(category, state.cell, state.branch),
                "component_weight_contract": (
                    "members inherit the final composite effective weight; independent "
                    "member weights are not identifiable under post-composite adjustment"
                ),
                "metrics": metric_report(
                    optimized.baseline_hits, optimized.selected_hits, len(state.races)
                ),
            }
        branch_reports = [branch for cell in cells.values() for branch in cell["branches"].values()]
        race_count = sum(branch["races"] for branch in branch_reports)
        routing = ROUTER.routing_for(category)
        configured = set(routing.variants)
        observed = set(cells)
        named_cells = {id(cell): cell for cell in routing.named_race_index.values()}.values()
        top_level_routes = {
            routing.default_variant,
            *(rule.variant for rule in routing.rules),
            *(cell.variant for cell in named_cells),
            *(
                route.variant
                for cell in named_cells
                if cell.prerace_router is not None
                for route in cell.prerace_router.routes
                if route.variant is not None
            ),
        }
        folded_components = sorted(configured - top_level_routes)
        aggregate_metrics: dict[str, object] = {}
        for top_k in range(1, 6):
            metric_name = f"top{top_k}"
            baseline = sum(
                branch["metrics"][metric_name]["baseline_hits"] for branch in branch_reports
            )
            adjusted = sum(
                branch["metrics"][metric_name]["adjusted_hits"] for branch in branch_reports
            )
            aggregate_metrics[metric_name] = {
                "baseline_hits": baseline,
                "adjusted_hits": adjusted,
                "delta_hits": adjusted - baseline,
                "baseline_rate": baseline / race_count,
                "adjusted_rate": adjusted / race_count,
                "delta_pp": (adjusted - baseline) / race_count * 100.0,
            }
        categories[category] = {
            "cells": cells,
            "summary": {
                "configured_routes": len(configured),
                "observed_routes": len(observed),
                "served_branches": len(branch_reports),
                "races": race_count,
                "enabled_branches": sum(branch["enabled"] for branch in branch_reports),
                "disabled_branches": sum(not branch["enabled"] for branch in branch_reports),
                "component_entries": sum(len(branch["components"]) for branch in branch_reports),
                "metrics": aggregate_metrics,
            },
            "configured_routes_without_observed_top_level_branch": sorted(configured - observed),
            "folded_component_routes": folded_components,
            "routes_without_replay_support": sorted(top_level_routes - observed),
            "configured_route_inventory": {
                variant: {
                    "model_version": spec.model_version,
                    "routing_mode": spec.routing_mode,
                    "role": "top-level" if variant in top_level_routes else "internal-component",
                }
                for variant, spec in sorted(routing.variants.items())
            },
        }
    return {
        "contract": {
            "years": [2024, 2025, 2026],
            "unit": "resolved cell plus final served model/composite version",
            "search": "analytic winner-score crossing event sweep over stable intervals",
            "metric": "lexicographic aggregate winner-in-Top1 through winner-in-Top5 hits",
            "internal_member_contract": (
                "linear ensemble members are folded into the final served composite because "
                "their Prophet coefficients are not separately identifiable after blending"
            ),
        },
        "categories": categories,
        "skipped": dict(skipped),
    }


def main() -> None:
    paths = evaluation_paths(parse_args())
    evidence, skipped = collect_evidence(paths)
    report = build_report(evidence, skipped)
    paths.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    summary = {
        category: sum(
            len(cell["branches"]) for cell in report["categories"][category]["cells"].values()
        )
        for category in CATEGORIES
    }
    print(json.dumps({"output": str(paths.output), "served_branches": summary}, indent=2))


if __name__ == "__main__":
    main()
