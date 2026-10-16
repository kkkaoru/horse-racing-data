"""Auditable Top1 search with per-year Top2–5 preservation constraints.

Development selection uses pre-2023 races. The separately reported constrained
selection DOES use 2023–2025 outcomes: its score is tuned, not an untouched test.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path

import pandas as pd

from arc_finish_model import (
    DEFAULT_CONFIG,
    EVALUATION_YEARS,
    TrainingConfig,
    predict_card,
    train_before,
    unordered_topk,
)


@dataclass(frozen=True)
class FoldResult:
    year: int
    predicted: tuple[str, ...]
    actual: tuple[str, ...]
    hits: tuple[int, ...]


@dataclass(frozen=True)
class CandidateResult:
    config: TrainingConfig
    development_hits: tuple[int, ...]
    evaluation: tuple[FoldResult, ...]
    preserves_topk: bool


DEVELOPMENT_YEARS: tuple[int, ...] = (
    2011,
    2012,
    2013,
    2014,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
)


def candidate_configs() -> tuple[TrainingConfig, ...]:
    combinations = product((1, 2, 3), (1, 5), (1, 10), (80, 240))
    structural = tuple(combinations)
    rankers = tuple(
        TrainingConfig("rank:pairwise", trees, depth, child, penalty)
        for depth, child, penalty, trees in structural
    )
    ndcg = tuple(
        TrainingConfig("rank:ndcg", trees, depth, child, penalty)
        for depth, child, penalty, trees in structural
    )
    winners = tuple(
        TrainingConfig("reg:squarederror", trees, depth, child, penalty)
        for depth, child, penalty, trees in structural
    )
    return (DEFAULT_CONFIG, *rankers, *ndcg, *winners)


def preserves_other_topk(
    *, predicted: Sequence[str], baseline: Sequence[str], actual: Sequence[str]
) -> bool:
    unordered_topk(predicted, actual)
    unordered_topk(baseline, actual)
    # Preserve the actual horses already hit, not merely their aggregate count.
    return all(
        (set(baseline[:k]) & set(actual[:k])).issubset(predicted[:k])
        for k in range(2, 6)
    )


def evaluate_config(
    data: pd.DataFrame, *, config: TrainingConfig, year: int
) -> FoldResult:
    card = data.loc[data["race_date"].str.startswith(str(year))]
    if card.empty:
        raise ValueError(f"Missing evaluation field {year}")
    model = train_before(data, cutoff=str(card["race_date"].iloc[0]), config=config)
    predicted = tuple(str(value) for value in predict_card(model, card)["horse_id"])
    actual = tuple(
        str(value) for value in card.sort_values(["finish", "horse_id"])["horse_id"]
    )
    hits = tuple(len(set(predicted[:k]) & set(actual[:k])) for k in range(1, 6))
    return FoldResult(year, predicted, actual, hits)


def development_score(data: pd.DataFrame, config: TrainingConfig) -> tuple[int, ...]:
    folds = [
        evaluate_config(data, config=config, year=year) for year in DEVELOPMENT_YEARS
    ]
    return tuple(sum(fold.hits[k] for fold in folds) for k in range(5))


def constrained_key(candidate: CandidateResult) -> tuple[int, int, int, int]:
    return (
        sum(fold.hits[0] for fold in candidate.evaluation),
        candidate.development_hits[0],
        sum(sum(fold.hits[1:]) for fold in candidate.evaluation),
        -candidate.config.n_estimators * candidate.config.max_depth,
    )


def best_constrained(candidates: Sequence[CandidateResult]) -> CandidateResult | None:
    eligible = [candidate for candidate in candidates if candidate.preserves_topk]
    return max(eligible, key=constrained_key) if eligible else None


def search(
    data: pd.DataFrame,
    *,
    baseline: Mapping[int, Sequence[str]],
    configs: Sequence[TrainingConfig],
    output: Path,
) -> CandidateResult | None:
    if not configs:
        raise ValueError("At least one candidate is required")
    output.mkdir(parents=True, exist_ok=False)
    # Freeze the development recommendation before examining target-year outcomes.
    development = [development_score(data, config) for config in configs]
    dev_index = max(range(len(configs)), key=lambda index: development[index])
    (output / "development-selection.json").write_text(
        json.dumps(
            {
                "years": DEVELOPMENT_YEARS,
                "config": asdict(configs[dev_index]),
                "scores": development,
                "evaluationYearsUsedForSelection": False,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    candidates: list[CandidateResult] = []
    for index, config in enumerate(configs):
        folds = tuple(
            evaluate_config(data, config=config, year=year) for year in EVALUATION_YEARS
        )
        preserved = all(
            preserves_other_topk(
                predicted=fold.predicted,
                baseline=baseline[fold.year],
                actual=fold.actual,
            )
            for fold in folds
        )
        candidate = CandidateResult(config, development[index], folds, preserved)
        candidates.append(candidate)
        (output / f"candidate-{index:03d}.json").write_text(
            json.dumps(asdict(candidate), indent=2), encoding="utf-8"
        )
        print(
            json.dumps(
                {
                    "candidate": index,
                    "top1Hits": sum(fold.hits[0] for fold in folds),
                    "preservesTop2to5": preserved,
                }
            ),
            flush=True,
        )
    selected = best_constrained(candidates)
    (output / "selection.json").write_text(
        json.dumps(
            {
                "candidates": len(candidates),
                "selected": None if selected is None else asdict(selected),
                "developmentSelectedIndex": dev_index,
                "evaluationYearsUsedForConstrainedSelection": True,
                "warning": "Retrospectively tuned 2023–2025 results; not an unbiased future accuracy estimate. No guaranteed global optimum outside the tested search space.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return selected
