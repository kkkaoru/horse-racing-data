"""Optimize production Prophet policy by analytic score-crossing sweeps."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from predict_lib.cell_router import all_conditions_match, load_cell_router
from predict_lib.prophet_adjustment import adjust_prediction_rows_with_prophet
from predict_lib.prophet_cell_policy import parse_prophet_cell_policy

from timesfm_finish_position.policy_report import (
    OptimizedCategoryReport,
    OptimizedCellReport,
    require_category,
    require_float,
    require_int,
)
from timesfm_finish_position.prophet_policy_optimization import (
    LinearRaceScores,
    LinearRunnerScore,
    OptimizedCellPolicy,
    optimize_linear_race_weights,
)

DEFAULT_LOCAL_URL = "postgresql://horse_racing:horse_racing@localhost:15432/horse_racing"
DEFAULT_WEIGHT = 0.05
YEARS = ("2024", "2025", "2026")
MODELS = (
    "iter14-jra-cb-pacestyle-course-v8",
    "iter12-nar-xgb-hpo-v8",
    "ban-ei-trans-lgbm-ensemble-v1.0",
    "banei-cb-v8-window2011-wf-15y",
    "banei-cb-v9-sim-2011",
)
MODEL_BY_CATEGORY_YEAR = {
    ("jra", "2024"): "iter14-jra-cb-pacestyle-course-v8",
    ("jra", "2025"): "iter14-jra-cb-pacestyle-course-v8",
    ("jra", "2026"): "iter14-jra-cb-pacestyle-course-v8",
    ("nar", "2024"): "iter12-nar-xgb-hpo-v8",
    ("nar", "2025"): "iter12-nar-xgb-hpo-v8",
    ("nar", "2026"): "iter12-nar-xgb-hpo-v8",
    ("ban-ei", "2024"): "ban-ei-trans-lgbm-ensemble-v1.0",
    ("ban-ei", "2025"): "ban-ei-trans-lgbm-ensemble-v1.0",
}
ROUTER = load_cell_router()
ALL_ON_POLICY = parse_prophet_cell_policy(
    {
        "version": "evaluation-all-on",
        "default_enabled": True,
        "default_weight": DEFAULT_WEIGHT,
        "categories": {},
    }
)


@dataclass(slots=True)
class CellEvidence:
    """Linear race evidence assigned to one production cell."""

    races: list[LinearRaceScores] = field(default_factory=list)
    years: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True, slots=True)
class OptimizationTask:
    category: str
    cell: str
    races: tuple[LinearRaceScores, ...]


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    category: str
    cell: str
    policy: OptimizedCellPolicy


def parse_args() -> argparse.Namespace:
    """Parse reproducible data-source, output, and parallelism controls."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--local-url", default=os.environ.get("LOCAL_DATABASE_URL", DEFAULT_LOCAL_URL)
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Independent cell worker processes; 1 is fastest on the measured dataset",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    return args


def optimize_task(task: OptimizationTask) -> OptimizationResult:
    """Optimize one independent cell in a worker process."""
    return OptimizationResult(
        category=task.category,
        cell=task.cell,
        policy=optimize_linear_race_weights(task.races, default_weight=DEFAULT_WEIGHT),
    )


def nar_subclass(value: object) -> str:
    text = str(value or "")
    for token, result in (
        ("\uff2f\uff30", "OP"),
        ("新馬", "NEW"),
        ("未勝利", "MUKATSU"),
        ("未出走", "MUKATSU"),
        ("２歳", "2YO"),
        ("2歳", "2YO"),
        ("３歳", "3YO"),
        ("3歳", "3YO"),
        ("\uff21", "A"),
        ("\uff22", "B"),
        ("\uff23", "C"),
    ):
        if token in text:
            return result
    return "OTHER"


def policy_cell(category: str, entries: list[dict[str, object]], card_max: int) -> str:
    routing = ROUTER.routing_for(category)
    normal = ROUTER.resolve_variant(category, entries, card_max_race_bango=card_max)
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
            card_max_race_bango=card_max,
        ):
            return rule.variant
    return routing.default_variant


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


def entry_query(table_prefix: str, source: str) -> str:
    models = ",".join(["%s"] * len(MODELS))
    banei_2026_fallback = (
        "or (se.kaisai_nen='2026' and se.keibajo_code='83')" if table_prefix == "nvd" else ""
    )
    return f"""
with se as (
  select distinct on (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    umaban, kishu_code, chokyoshi_code, ijo_kubun_code, kakutei_chakujun
  from {table_prefix}_se
  where kaisai_nen in ('2024','2025','2026')
  order by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, ctid desc
), ra as (
  select distinct on (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyori, track_code,
    grade_code, kyoso_joken_code, kyoso_joken_meisho, babajotai_code_dirt,
    kyosomei_hondai, kyosomei_fukudai, kyosomei_kakkonai
  from {table_prefix}_ra
  where kaisai_nen in ('2024','2025','2026')
  order by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ctid desc
)
select se.*, ra.kyori, ra.track_code, ra.grade_code, ra.kyoso_joken_code,
       ra.kyoso_joken_meisho, ra.babajotai_code_dirt,
       ra.kyosomei_hondai, ra.kyosomei_fukudai, ra.kyosomei_kakkonai
from se join ra using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
where coalesce(trim(se.ijo_kubun_code),'0') not in ('1','2')
  and nullif(trim(se.kakutei_chakujun),'') is not null
  and (
    exists (
      select 1 from race_finish_position_model_predictions p
      where p.source = '{source}' and p.kaisai_nen=se.kaisai_nen
        and p.kaisai_tsukihi=se.kaisai_tsukihi and p.keibajo_code=se.keibajo_code
        and p.race_bango=se.race_bango and p.model_version in ({models})
    )
    {banei_2026_fallback}
  )
order by se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango, se.umaban
"""


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[3]
    trends: dict[tuple[str, str, str], float] = {}
    for category, folder in (
        ("jra", "prophet-jra-lab"),
        ("nar", "prophet-lab"),
        ("ban-ei", "prophet-lab"),
    ):
        for year in ("2024", "2025"):
            loaded = load_lab_trends(
                root
                / "apps/timesfm-finish-position/tmp"
                / folder
                / f"prophet-entity-trends-{year}.parquet"
            )
            trends.update(
                ((category, race_id, horse_id), value)
                for (race_id, horse_id), value in loaded.items()
            )

    lookup_table = pq.read_table(
        root / "apps/pc-keiba-viewer/finish-position/lookups/"
        "prophet-entity-trends-all-categories-2026.parquet"
    )
    lookup = {
        (str(category), str(date), str(entity_type), str(code)): float(yhat)
        for category, date, entity_type, code, yhat in zip(
            lookup_table.column("category").to_pylist(),
            lookup_table.column("forecast_date").to_pylist(),
            lookup_table.column("entity_type").to_pylist(),
            lookup_table.column("entity_code").to_pylist(),
            lookup_table.column("yhat").to_pylist(),
            strict=True,
        )
    }

    entries_by_race: dict[tuple[str, str, str, str, str], list[dict[str, object]]] = defaultdict(
        list
    )
    predictions: dict[tuple[str, str, str, str, str, str, str], float] = {}
    with psycopg.connect(args.local_url) as connection:
        for table_prefix, source in (("jvd", "jra"), ("nvd", "nar")):
            with connection.cursor() as cursor:
                cursor.execute(entry_query(table_prefix, source).encode("utf-8"), MODELS)
                if cursor.description is None:
                    raise RuntimeError("Entry query returned no result columns")
                names = [column.name for column in cursor.description]
                for row in cursor:
                    raw = dict(zip(names, row, strict=True))
                    year = str(raw["kaisai_nen"])
                    month_day = str(raw["kaisai_tsukihi"])
                    venue = str(raw["keibajo_code"])
                    race = str(raw["race_bango"])
                    category = "jra" if source == "jra" else ("ban-ei" if venue == "83" else "nar")
                    finish_text = str(raw["kakutei_chakujun"]).strip()
                    if not finish_text.isdigit() or int(finish_text) < 1:
                        continue
                    raw.update(
                        {
                            "source": source,
                            "category": category,
                            "race_id": f"{source}:{year}{month_day}:{venue}:{race}",
                            "nar_subclass": nar_subclass(raw["kyoso_joken_meisho"])
                            if category == "nar"
                            else "",
                            "current_baba_condition": raw["babajotai_code_dirt"],
                            "finish_position": int(finish_text),
                        }
                    )
                    entries_by_race[(category, year, month_day, venue, race)].append(raw)
        with connection.cursor() as cursor:
            cursor.execute(
                """select source,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,
                          ketto_toroku_bango,model_version,predicted_score
                   from race_finish_position_model_predictions
                   where kaisai_nen in ('2024','2025','2026') and model_version = any(%s)""",
                (list(MODELS),),
            )
            for source, year, month_day, venue, race, horse, model, score in cursor:
                predictions[
                    (
                        str(source),
                        str(year),
                        str(month_day),
                        str(venue),
                        str(race),
                        str(horse),
                        str(model),
                    )
                ] = float(score)

    replica_env = root / "apps/local-postgresql/.env.replica"
    neon_url = next(
        line.split("=", 1)[1].strip().strip('"').strip("'")
        for line in replica_env.read_text(encoding="utf-8").splitlines()
        if line.startswith("NEON_DATABASE_URL=")
    )
    with psycopg.connect(neon_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """select source,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,
                      ketto_toroku_bango,model_version,predicted_score
               from race_finish_position_model_predictions
               where kaisai_nen='2026'
                 and model_version = any(%s)""",
            (["banei-cb-v8-window2011-wf-15y", "banei-cb-v9-sim-2011"],),
        )
        for source, year, month_day, venue, race, horse, model, score in cursor:
            predictions[
                (
                    str(source),
                    str(year),
                    str(month_day),
                    str(venue),
                    str(race),
                    str(horse),
                    str(model),
                )
            ] = float(score)

    card_max: dict[tuple[str, str, str, str], int] = {}
    for category, year, month_day, venue, race in entries_by_race:
        card = (category, year, month_day, venue)
        card_max[card] = max(card_max.get(card, 0), int(race))
    stats: defaultdict[tuple[str, str], CellEvidence] = defaultdict(CellEvidence)
    skipped: dict[str, int] = defaultdict(int)

    for (category, year, month_day, venue, race), entries in entries_by_race.items():
        cell = policy_cell(category, entries, card_max[(category, year, month_day, venue)])
        if category == "ban-ei" and year == "2026":
            model = "banei-cb-v9-sim-2011" if cell == "sim" else "banei-cb-v8-window2011-wf-15y"
        else:
            model = MODEL_BY_CATEGORY_YEAR.get((category, year))
        if model is None:
            skipped["missing-model-contract"] += 1
            continue
        source = "jra" if category == "jra" else "nar"
        scores: list[float] = []
        complete = True
        for entry in entries:
            key = (
                source,
                year,
                month_day,
                venue,
                race,
                str(entry["ketto_toroku_bango"]),
                model,
            )
            score = predictions.get(key)
            if score is None:
                complete = False
                break
            scores.append(score)
        if not complete or len(entries) < 2:
            skipped[f"{category}-{year}-incomplete-predictions"] += 1
            continue

        race_id = f"{source}:{year}{month_day}:{venue}:{race}"
        if year in ("2024", "2025"):
            for entry in entries:
                trend = trends.get((category, race_id, str(entry["ketto_toroku_bango"])))
                entry["prophet_entity_performance_mean"] = trend
                entry["prophet_entity_coverage"] = 3 if trend is not None else 0
        else:
            date = f"{year}{month_day}"
            for entry in entries:
                values: list[float] = []
                coverage = 0
                for entity_type, field in (
                    ("venue", "keibajo_code"),
                    ("jockey", "kishu_code"),
                    ("trainer", "chokyoshi_code"),
                ):
                    code = str(entry[field] or "").strip()
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

        rows = [
            [
                model,
                source,
                year,
                month_day,
                venue,
                race,
                str(entry["ketto_toroku_bango"]),
                str(entry["umaban"]),
                score,
                rank,
            ]
            for rank, (entry, score) in enumerate(
                sorted(
                    zip(entries, scores, strict=True),
                    key=lambda pair: (-pair[1], str(pair[0]["ketto_toroku_bango"])),
                ),
                start=1,
            )
        ]
        entry_by_horse = {str(entry["ketto_toroku_bango"]): entry for entry in entries}
        aligned_entries = [entry_by_horse[str(row[6])] for row in rows]
        adjusted = adjust_prediction_rows_with_prophet(
            rows,
            aligned_entries,
            require_category(category),
            environment={"PROPHET_SCORE_ADJUSTMENT_WEIGHT": "1.0"},
            cell_variant=cell,
            policy=ALL_ON_POLICY,
        )
        if not adjusted.applied:
            skipped[f"{category}-{year}-{adjusted.reason}"] += 1
        adjusted_scores = {str(row[6]): require_float(row[8]) for row in adjusted.rows}
        linear_runners = tuple(
            LinearRunnerScore(
                horse_id=str(row[6]),
                intercept=float(row[8]),
                adjustment=adjusted_scores[str(row[6])] - float(row[8]),
            )
            for row in rows
        )
        finish_by_horse = {
            str(entry["ketto_toroku_bango"]): require_int(entry["finish_position"])
            for entry in entries
        }
        winners = tuple(
            runner for runner in linear_runners if finish_by_horse[runner.horse_id] == 1
        )
        winner_ids = {winner.horse_id for winner in winners}
        competitors = tuple(
            runner for runner in linear_runners if runner.horse_id not in winner_ids
        )
        evidence = stats[(category, cell)]
        evidence.races.append(LinearRaceScores(winners=winners, competitors=competitors))
        evidence.years[year] += 1

    result: dict[str, object] = {
        "contract": {
            "years": [2024, 2025, 2026],
            "search": "analytic winner-score crossing event sweep over stable intervals",
            "complexity": (
                "O(E log E) per cell; no fixed grid, linear weight scan, "
                "binary search, or Cartesian cell search"
            ),
            "parallel_workers": args.workers,
            "metric": "lexicographic aggregate winner-in-Top1 through winner-in-Top5 hits",
            "tie_break": "earliest lower-weight stable interval representative",
            "decision": "ON when a positive stable interval improves over weight 0; otherwise OFF",
            "2026": "partial persisted prediction ledger",
        },
        "categories": {},
        "non_adjusted_or_unavailable_counts": dict(sorted(skipped.items())),
    }
    tasks = tuple(
        OptimizationTask(
            category=category,
            cell=cell,
            races=tuple(stats.get((category, cell), CellEvidence()).races),
        )
        for category in ("jra", "nar", "ban-ei")
        for cell in sorted(ROUTER.routing_for(category).variants)
    )
    if args.workers == 1:
        optimized_results = tuple(map(optimize_task, tasks))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            optimized_results = tuple(executor.map(optimize_task, tasks))
    optimized_by_cell = {
        (optimized.category, optimized.cell): optimized.policy for optimized in optimized_results
    }

    categories: dict[str, OptimizedCategoryReport] = {}
    for category in ("jra", "nar", "ban-ei"):
        cells: dict[str, OptimizedCellReport] = {}
        for cell in sorted(ROUTER.routing_for(category).variants):
            evidence = stats.get((category, cell), CellEvidence())
            optimized = optimized_by_cell[(category, cell)]
            races = len(evidence.races)
            metrics: dict[str, object] = {}
            for top_k, (baseline, adjusted_hits) in enumerate(
                zip(optimized.baseline_hits, optimized.selected_hits, strict=True), start=1
            ):
                metrics[f"top{top_k}"] = {
                    "baseline_hits": baseline,
                    "adjusted_hits": adjusted_hits,
                    "delta_hits": adjusted_hits - baseline,
                    "baseline_rate": baseline / races if races else None,
                    "adjusted_rate": adjusted_hits / races if races else None,
                    "delta_pp": (adjusted_hits - baseline) / races * 100 if races else None,
                }
            cells[cell] = {
                "enabled": optimized.enabled,
                "weight": optimized.weight,
                "effective_weight": optimized.weight if optimized.enabled else 0.0,
                "selected_weight": optimized.selected_weight,
                "races": races,
                "years": dict(evidence.years),
                "support": "observed" if races else "no_historical_support_default_on",
                "metrics": metrics,
            }
        categories[category] = {
            "default_enabled": True,
            "default_weight": DEFAULT_WEIGHT,
            "cells": cells,
        }
    result["categories"] = categories
    output = args.output or (
        root
        / "apps/timesfm-finish-position/results/prophet-cell-weight-optimization-2024-2026.json"
    )
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    summary = {
        category: {
            "on": sum(cell["enabled"] for cell in value["cells"].values()),
            "off": sum(not cell["enabled"] for cell in value["cells"].values()),
        }
        for category, value in categories.items()
    }
    print(json.dumps({"output": str(output), "summary": summary}, indent=2))


if __name__ == "__main__":
    main()
