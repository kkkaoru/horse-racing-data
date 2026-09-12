"""Cell-local development-only Rustuna search; never a production approval."""

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date
from typing import Literal

import rustuna

from timesfm_finish_position.chronos_domains import TrainingDomain, parse_training_domain
from timesfm_finish_position.chronos_mlx_study import StudyConfig

HitDelta = tuple[int, int, int, int, int]


@dataclass(frozen=True)
class CellScope:
    cell_id: str
    data_sha256: str
    runner_identity_sha256: str
    training_end: date
    development_start: date
    development_end: date
    application_date: date
    year_race_counts: tuple[tuple[int, int], ...]
    training_start: date = date(2020, 1, 1)
    domain: TrainingDomain = "jra"

    def __post_init__(self) -> None:
        parse_training_domain(self.domain)
        if not self.cell_id or self.cell_id != self.cell_id.strip():
            raise ValueError("Explicit cell identity is required")
        if not all(
            re.fullmatch(r"[0-9a-f]{64}", value)
            for value in (self.data_sha256, self.runner_identity_sha256)
        ):
            raise ValueError("Immutable data and runner identity SHA-256 are required")
        if (
            not self.training_start
            <= self.training_end
            < self.development_start
            <= self.development_end
            < self.application_date
        ):
            raise ValueError("Training, development and application dates must be separated")
        years = [year for year, _ in self.year_race_counts]
        if (
            not years
            or len(set(years)) != len(years)
            or any(
                count < 1 or not self.development_start.year <= year <= self.development_end.year
                for year, count in self.year_race_counts
            )
        ):
            raise ValueError("Unique development years with positive race counts are required")


@dataclass(frozen=True)
class CellParameters:
    training: StudyConfig
    market_weight: float


@dataclass(frozen=True)
class AnnualDelta:
    year: int
    race_count: int
    market: HitDelta
    incumbent: HitDelta


@dataclass(frozen=True)
class CellEvaluation:
    scope: CellScope
    model_sha256: str
    annual: tuple[AnnualDelta, ...]
    parameters: CellParameters


@dataclass(frozen=True)
class CellTrialRecord:
    number: int
    parameters: CellParameters
    evaluation: CellEvaluation
    objective: float


@dataclass(frozen=True)
class CellTuningResult:
    scope: CellScope
    study_name: str
    trials: tuple[CellTrialRecord, ...]
    selected: CellTrialRecord | None
    production_eligible: Literal[False] = False


def sample_cell_parameters(
    trial: rustuna.Trial,
    *,
    scope: CellScope,
    seed: int,
    max_steps: int,
    history_bounds: tuple[int, int] = (2, 8),
) -> CellParameters:
    if not 1 <= history_bounds[0] <= history_bounds[1] <= 8:
        raise ValueError("History search bounds must lie within 1..8")
    mode: Literal["head", "lora"] = "head" if trial.suggest_int("adapter", 0, 1) == 0 else "lora"
    steps = trial.suggest_int("steps", 100, max_steps, step=100)
    rank = 2 ** trial.suggest_int("lora_rank_power", 2, 4) if mode == "lora" else 8
    training = StudyConfig(
        steps=steps,
        batch_size=2 ** trial.suggest_int("batch_power", 3, 5),
        context_length=2 ** trial.suggest_int("context_power", 5, 7),
        minimum_history=trial.suggest_int("minimum_history", *history_bounds),
        seed=seed,
        mode=mode,
        learning_rate=trial.suggest_float("learning_rate", 1e-6, 5e-5, log=True),
        weight_decay=trial.suggest_float("weight_decay", 0.0, 0.1),
        warmup_steps=round(steps * trial.suggest_float("warmup_fraction", 0.0, 0.2)),
        lora_rank=rank,
        lora_alpha=2.0 * rank,
        training_start=scope.training_start.strftime("%Y%m%d"),
        training_end=scope.training_end.strftime("%Y%m%d"),
        development_start=scope.development_start.strftime("%Y%m%d"),
        development_end=scope.development_end.strftime("%Y%m%d"),
        training_domain=scope.domain,
    )
    return CellParameters(training, trial.suggest_float("market_weight", 0.0, 1.0))


def _score(scope: CellScope, evaluation: CellEvaluation) -> float:
    if evaluation.scope != scope or not re.fullmatch(r"[0-9a-f]{64}", evaluation.model_sha256):
        raise ValueError("Evaluation must match the exact cell/cohort and identify its model")
    counts = tuple(sorted((row.year, row.race_count) for row in evaluation.annual))
    if counts != tuple(sorted(scope.year_race_counts)):
        raise ValueError("Evaluation must retain the complete development race cohort")
    if any(len(row.market) != 5 or len(row.incumbent) != 5 for row in evaluation.annual):
        raise ValueError("Both market and incumbent Top1-Top5 comparisons are required")
    if any(
        type(delta) is not int
        for row in evaluation.annual
        for delta in (*row.market, *row.incumbent)
    ):
        raise ValueError("Hit deltas must be integer counts")
    violation = sum(
        max(0, -delta)
        for row in evaluation.annual
        for delta in (*row.market[1:], *row.incumbent[1:])
    )
    if violation:
        return -1e9 - violation
    return float(
        min(
            sum(row.market[0] for row in evaluation.annual),
            sum(row.incumbent[0] for row in evaluation.annual),
        )
    )


def tune_cell(
    *,
    scope: CellScope,
    evaluate: Callable[[CellScope, CellParameters], CellEvaluation],
    n_trials: int,
    max_steps: int = 1000,
) -> CellTuningResult:
    """Evaluate serially. Callback may use only this scope's training/development data.

    Caller must execute the supplied training config and evaluate portable CPU
    forecasts on exact identities against both baselines. Holdout evaluation and
    production authorization are deliberately not accepted by this API.
    """
    if n_trials < 1 or max_steps < 100 or max_steps % 100:
        raise ValueError("Positive trials and a step budget divisible by 100 are required")
    identity = json.dumps(asdict(scope), sort_keys=True, default=str)
    seed = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:4], "big")
    name = f"chronos-cell:{scope.domain}:{scope.cell_id}:{seed:08x}"
    study = rustuna.create_study(
        study_name=name, direction="maximize", sampler=rustuna.samplers.TPESampler(seed=seed)
    )
    records: list[CellTrialRecord] = []

    def objective(trial: rustuna.Trial) -> float:
        parameters = sample_cell_parameters(trial, scope=scope, seed=seed, max_steps=max_steps)
        evaluation = evaluate(scope, parameters)
        if evaluation.parameters != parameters:
            raise ValueError("Evaluated model configuration must match the sampled cell parameters")
        value = _score(scope, evaluation)
        records.append(CellTrialRecord(len(records), parameters, evaluation, value))
        return value

    study.optimize(objective, n_trials=n_trials)
    best = max(records, key=lambda record: record.objective)
    return CellTuningResult(scope, name, tuple(records), best if best.objective > 0 else None)
