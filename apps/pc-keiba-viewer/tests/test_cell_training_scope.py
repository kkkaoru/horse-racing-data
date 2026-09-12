from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from learning.cell_training_scope import CellScopeConfig, build_cell_training_scope


@pytest.fixture
def config() -> CellScopeConfig:
    return CellScopeConfig(
        cell_id="nar-55-C",
        category="nar",
        dimensions=(("venue", "55"),),
        training_cutoff=date(2025, 1, 1),
        evaluation_end=date(2026, 9, 11),
    )


def test_scope_expands_older_other_venue_races_and_full_competitors(
    config: CellScopeConfig,
) -> None:
    history = pl.DataFrame(
        {
            "race_id": ["old", "old", "seed", "seed", "unrelated", "eval", "future"],
            "horse_id": ["h1", "competitor", "h1", "h2", "h3", "h4", "h1"],
            "race_date": [
                "20000101",
                "20000101",
                "20240101",
                "20240101",
                "20240102",
                "20250101",
                "20260912",
            ],
            "category": ["nar", "nar", "nar", "nar", "nar", "nar", "nar"],
            "venue": ["54", "54", "55", "55", "54", "55", "55"],
        }
    )
    result = build_cell_training_scope(history, config=config)
    assert result.seed_race_ids.to_dict(as_series=False) == {"race_id": ["seed"]}
    assert result.training_rows.select("race_id", "horse_id").rows() == [
        ("old", "competitor"),
        ("old", "h1"),
        ("seed", "h1"),
        ("seed", "h2"),
    ]
    assert result.evaluation_rows.select("race_id").rows() == [("eval",)]
    assert result.race_universe.rows() == [("eval",), ("old",), ("seed",)]


def test_no_historical_seeds_does_not_use_future_entrants(
    config: CellScopeConfig,
) -> None:
    history = pl.DataFrame(
        {
            "race_id": ["old", "eval"],
            "horse_id": ["h1", "h1"],
            "race_date": ["20240101", "20250101"],
            "category": ["nar", "nar"],
            "venue": ["54", "55"],
        }
    )
    result = build_cell_training_scope(history, config=config)
    assert result.training_rows.height == 0
    assert result.race_universe.rows() == [("eval",)]


def test_scope_rejects_missing_columns(config: CellScopeConfig) -> None:
    with pytest.raises(ValueError, match="Missing scope columns"):
        build_cell_training_scope(pl.DataFrame(), config=config)


def test_scope_rejects_null_horse_identity(config: CellScopeConfig) -> None:
    history = pl.DataFrame(
        {
            "race_id": ["a"],
            "horse_id": [None],
            "race_date": ["20240101"],
            "category": ["nar"],
            "venue": ["55"],
        }
    )
    with pytest.raises(ValueError, match="must not be null"):
        build_cell_training_scope(history, config=config)


@pytest.mark.parametrize(
    ("category", "dimensions", "seed_years", "end", "start", "message"),
    [
        ("jra", (), 20, date(2026, 1, 1), 2020, "Only nar"),
        (
            "nar",
            (("venue", "55"), ("venue", "54")),
            20,
            date(2026, 1, 1),
            2020,
            "unique",
        ),
        ("nar", (), 20, date(2026, 1, 1), 2020, "explicit venue"),
        ("ban-ei", (), 0, date(2026, 1, 1), 2020, "valid historical"),
        ("ban-ei", (), 2025, date(2026, 1, 1), 2020, "valid historical"),
        ("ban-ei", (), 20, date(2024, 1, 1), 2020, "must not precede"),
        ("ban-ei", (), 20, date(2027, 1, 1), 2020, "Evaluation years"),
        ("ban-ei", (), 20, date(2026, 1, 1), 2019, "Evaluation years"),
    ],
)
def test_invalid_scope_configuration(
    category: str,
    dimensions: tuple[tuple[str, str], ...],
    seed_years: int,
    end: date,
    start: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        CellScopeConfig(
            cell_id="cell",
            category=category,
            dimensions=dimensions,
            training_cutoff=date(2025, 1, 1),
            evaluation_end=end,
            seed_years=seed_years,
            evaluation_start_year=start,
        )


def test_banei_scope_allows_pooled_category_without_venue() -> None:
    config = CellScopeConfig(
        cell_id="banei",
        category="ban-ei",
        dimensions=(),
        training_cutoff=date(2025, 1, 1),
        evaluation_end=date(2026, 1, 1),
    )
    history = pl.DataFrame(
        {
            "race_id": ["seed", "eval"],
            "horse_id": ["h1", "h1"],
            "race_date": ["20240101", "20250101"],
            "category": ["ban-ei", "ban-ei"],
        }
    )
    result = build_cell_training_scope(history, config=config)
    assert result.training_rows.select("race_id").rows() == [("seed",)]
    assert result.evaluation_rows.select("race_id").rows() == [("eval",)]
