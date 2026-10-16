"""Top1 selection must retain every previously correct Top2–5 horse."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import arc_model_search as search
from arc_finish_model import FEATURES, TrainingConfig


def test_grid_is_bounded_and_contains_original_configuration() -> None:
    configs = search.candidate_configs()
    assert len(configs) == 73
    assert configs[0] == TrainingConfig()
    assert configs[-1] == TrainingConfig("reg:squarederror", 240, 3, 5, 10)


def test_reordering_top_two_can_improve_top_one_without_other_changes() -> None:
    assert search.preserves_other_topk(
        predicted=["a", "b", "c", "d", "e"],
        baseline=["b", "a", "c", "d", "e"],
        actual=["a", "b", "c", "d", "e"],
    )


def test_equal_overlap_cannot_replace_an_already_correct_horse() -> None:
    assert not search.preserves_other_topk(
        predicted=["a", "x", "c", "d", "e", "b"],
        baseline=["b", "x", "c", "d", "e", "a"],
        actual=["a", "b", "c", "d", "e"],
    )


def test_best_candidate_prioritizes_top_one_and_rejects_regression() -> None:
    weak = search.CandidateResult(
        TrainingConfig(),
        (9, 9, 9, 9, 9),
        (search.FoldResult(2023, (), (), (0, 2, 3, 4, 5)),),
        True,
    )
    strong = search.CandidateResult(
        TrainingConfig(max_depth=1),
        (0, 0, 0, 0, 0),
        (search.FoldResult(2023, (), (), (1, 2, 3, 4, 5)),),
        True,
    )
    invalid = search.CandidateResult(
        TrainingConfig(max_depth=3),
        (9, 9, 9, 9, 9),
        (search.FoldResult(2023, (), (), (1, 2, 3, 4, 5)),),
        False,
    )
    selected = search.best_constrained([weak, invalid, strong])
    assert selected is not None
    assert selected.config.max_depth == 1
    assert search.best_constrained([invalid]) is None


def test_evaluate_config_is_chronological() -> None:
    data = pd.DataFrame(np.zeros((15, len(FEATURES))), columns=FEATURES)
    data["horse_id"] = np.tile(["a", "b", "c", "d", "e"], 3)
    data["race_date"] = np.repeat(["2021-10-01", "2022-10-01", "2023-10-01"], 5)
    data["finish"] = np.tile([1, 2, 3, 4, 5], 3)
    result = search.evaluate_config(data, config=TrainingConfig(), year=2023)
    assert result.hits == (1, 2, 3, 4, 5)
    with pytest.raises(ValueError, match="Missing evaluation field"):
        search.evaluate_config(data, config=TrainingConfig(), year=2020)


def test_search_records_development_and_tuned_selection_separately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        search,
        "evaluate_config",
        lambda data, *, config, year: search.FoldResult(
            year, ("a", "b", "c", "d", "e"), ("a", "b", "c", "d", "e"), (1, 2, 3, 4, 5)
        ),
    )
    assert search.development_score(pd.DataFrame(), TrainingConfig()) == (
        12,
        24,
        36,
        48,
        60,
    )
    baseline = {
        2023: ["a", "b", "c", "d", "e"],
        2024: ["a", "b", "c", "d", "e"],
        2025: ["a", "b", "c", "d", "e"],
    }
    result = search.search(
        pd.DataFrame(),
        baseline=baseline,
        configs=[TrainingConfig()],
        output=tmp_path / "run",
    )
    assert result is not None
    assert result.preserves_topk is True
    report = json.loads(
        (tmp_path / "run" / "selection.json").read_text(encoding="utf-8")
    )
    assert report["evaluationYearsUsedForConstrainedSelection"] is True
    development = json.loads(
        (tmp_path / "run" / "development-selection.json").read_text(encoding="utf-8")
    )
    assert development["evaluationYearsUsedForSelection"] is False
    with pytest.raises(ValueError, match="one candidate"):
        search.search(
            pd.DataFrame(), baseline=baseline, configs=[], output=tmp_path / "empty"
        )
