"""Cell searches are independent, development-bound and fail closed."""

from dataclasses import replace
from datetime import date

import pytest
import rustuna

from timesfm_finish_position.chronos_cell_tuning import (
    AnnualDelta,
    CellEvaluation,
    CellParameters,
    CellScope,
    sample_cell_parameters,
    tune_cell,
)


@pytest.fixture
def scope() -> CellScope:
    return CellScope(
        "cell-a",
        "a" * 64,
        "b" * 64,
        date(2022, 12, 31),
        date(2023, 1, 1),
        date(2023, 12, 31),
        date(2026, 9, 12),
        ((2023, 2),),
    )


def test_explicit_sparse_history_search_bounds(scope: CellScope) -> None:
    def objective(trial: rustuna.Trial) -> float:
        parameters = sample_cell_parameters(
            trial, scope=scope, seed=42, max_steps=100, history_bounds=(1, 1)
        )
        assert parameters.training.minimum_history == 1
        with pytest.raises(ValueError, match="History search bounds"):
            sample_cell_parameters(
                trial, scope=scope, seed=42, max_steps=100, history_bounds=(0, 4)
            )
        return parameters.market_weight

    study = rustuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=1)
    assert len(study.trials) == 1


def test_independent_cell_trials(scope: CellScope) -> None:
    def evaluate(current: CellScope, parameters: CellParameters) -> CellEvaluation:
        return CellEvaluation(
            current, "c" * 64, (AnnualDelta(2023, 2, (1, 0, 0, 0, 0), (1, 0, 0, 0, 0)),), parameters
        )

    first = tune_cell(scope=scope, evaluate=evaluate, n_trials=8, max_steps=100)
    second = tune_cell(
        scope=replace(scope, cell_id="cell-b"), evaluate=evaluate, n_trials=8, max_steps=100
    )
    assert first.study_name != second.study_name
    assert first.scope.cell_id == "cell-a"
    assert second.scope.cell_id == "cell-b"
    assert len(first.trials) == 8
    assert len({row.parameters.training.learning_rate for row in first.trials}) > 1
    assert first.selected is not None
    assert first.selected.objective == 1.0
    assert first.production_eligible is False
    assert first.trials[0].parameters.training.seed != second.trials[0].parameters.training.seed
    banei = tune_cell(
        scope=replace(scope, domain="banei"), evaluate=evaluate, n_trials=1, max_steps=100
    )
    assert banei.study_name != first.study_name
    assert banei.trials[0].parameters.training.training_domain == "banei"


@pytest.mark.parametrize("incumbent", [(0, 0, 0, 0, 0), (1, -1, 0, 0, 0)])
def test_no_incumbent_improvement_or_annual_regression_is_rejected(
    scope: CellScope, incumbent: tuple[int, int, int, int, int]
) -> None:
    def evaluate(current: CellScope, parameters: CellParameters) -> CellEvaluation:
        return CellEvaluation(
            current, "c" * 64, (AnnualDelta(2023, 2, (1, 0, 0, 0, 0), incumbent),), parameters
        )

    result = tune_cell(scope=scope, evaluate=evaluate, n_trials=1, max_steps=100)
    assert result.selected is None
    assert result.production_eligible is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("cell_id", ""),
        ("data_sha256", "bad"),
        ("development_start", date(2022, 1, 1)),
        ("year_race_counts", ()),
    ],
)
def test_scope_guards(scope: CellScope, field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(scope, **{field: value})


def test_cross_cell_evaluation_is_rejected(scope: CellScope) -> None:
    def evaluate(current: CellScope, parameters: CellParameters) -> CellEvaluation:
        return CellEvaluation(replace(current, cell_id="other-cell"), "c" * 64, (), parameters)

    with pytest.raises(ValueError, match="exact cell/cohort"):
        tune_cell(scope=scope, evaluate=evaluate, n_trials=1, max_steps=100)


def test_wrong_training_configuration_is_rejected(scope: CellScope) -> None:
    def evaluate(current: CellScope, parameters: CellParameters) -> CellEvaluation:
        wrong = replace(parameters, training=replace(parameters.training, learning_rate=1.0))
        return CellEvaluation(current, "c" * 64, (), wrong)

    with pytest.raises(ValueError, match="sampled cell parameters"):
        tune_cell(scope=scope, evaluate=evaluate, n_trials=1, max_steps=100)


def test_missing_race_cohort_is_rejected(scope: CellScope) -> None:
    def evaluate(current: CellScope, parameters: CellParameters) -> CellEvaluation:
        return CellEvaluation(current, "c" * 64, (), parameters)

    with pytest.raises(ValueError, match="complete development"):
        tune_cell(scope=scope, evaluate=evaluate, n_trials=1, max_steps=100)
    with pytest.raises(ValueError, match="Positive trials"):
        tune_cell(scope=scope, evaluate=evaluate, n_trials=0)
