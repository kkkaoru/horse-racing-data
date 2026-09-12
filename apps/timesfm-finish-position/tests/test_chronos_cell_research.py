"""Research driver executes sampled training and CPU scoring, never incumbent fiction."""

import hashlib
import json
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import rustuna

from timesfm_finish_position.chronos_cell_research import (
    ForecastRows,
    ResearchCell,
    cell_windows,
    cpu_points,
    evaluate_points,
    file_sha256,
    forecast_races,
    load_cell_data,
    load_research_cells,
    load_validation_data,
    main,
    run_cell_research,
)
from timesfm_finish_position.chronos_cell_tuning import (
    CellParameters,
    CellScope,
)
from timesfm_finish_position.chronos_forecasting import Chronos2Forecaster
from timesfm_finish_position.chronos_mlx_study import StudyConfig
from timesfm_finish_position.domain import FloatArray


@pytest.fixture
def cell(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ResearchCell:
    years = np.repeat(np.array(["2018", "2019", "2020", "2021", "2022", "2023"]), 40)
    groups = np.tile(np.repeat(np.arange(4), 10), 6).astype(str)
    positions = np.tile(np.arange(1, 11), 24)
    path = tmp_path / "history.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": np.char.add(np.char.add("jra:", years), np.char.add(":", groups)),
                "race_date": np.char.add(years, "0101"),
                "horse_id": np.tile(np.char.add("h", np.arange(40).astype(str)), 6),
                "performance_rating": (10 - positions) / 9,
                "finish_position": positions,
                "decimal_odds": (12 - positions).astype(float),
                "field_size": np.full(240, 10),
                "venue_code": ["05"] * 240,
            }
        ).sort_by(
            [("race_date", "ascending"), ("race_id", "ascending"), ("horse_id", "ascending")]
        ),
        path,
    )
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    return ResearchCell(
        "cell-a",
        path,
        file_sha256(path),
        ("jra:2023:0", "jra:2023:1", "jra:2023:2", "jra:2023:3"),
        "jra",
    )


def test_cached_rows_with_odds_prior_keep_market_baseline(cell: ResearchCell) -> None:
    data = load_cell_data(cell)
    windows = cell_windows(
        data, StudyConfig(context_length=32, minimum_history=1), development=True
    )
    result = evaluate_points(
        data=data,
        windows=ForecastRows(windows.source_indices),
        points=np.zeros(len(windows.source_indices), dtype=np.float64),
        relative=False,
        market_weight=1.0,
        market_prior="pl_expected_performance",
    )
    assert result["market_hits"] == [0, 0, 0, 0, 0]
    assert result["model_hits"] == [0, 0, 0, 0, 0]
    assert result["known_runners"] == 40
    assert result["market_prior"] == "pl_expected_performance"
    races = forecast_races(
        data,
        windows=ForecastRows(windows.source_indices),
        points=np.zeros(40, dtype=np.float64),
        relative=False,
        market_prior="pl_expected_performance",
    )
    assert len(races) == 4
    assert races[0].horse_ids.tolist() == [
        "h0",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "h7",
        "h8",
        "h9",
    ]


def test_real_trial_orchestration(
    cell: ResearchCell, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def sample(
        trial: rustuna.Trial,
        *,
        scope: CellScope,
        seed: int,
        max_steps: int,
        history_bounds: tuple[int, int],
    ) -> CellParameters:
        assert scope.cell_id == "cell-a"
        assert history_bounds == (1, 4)
        return CellParameters(
            StudyConfig(
                steps=max_steps, batch_size=8, context_length=32, minimum_history=1, seed=seed
            ),
            0.0,
        )

    def train(
        *,
        history: Path,
        source_config: Path,
        output: Path,
        config: StudyConfig,
        checkpoint_every: int,
    ) -> dict[str, object]:
        assert config.target_race_prefix == "jra:"
        assert config.training_domain == "jra"
        assert config.steps == 100
        assert checkpoint_every == 100
        portable = output / "portable"
        portable.mkdir(parents=True)
        (portable / "model.safetensors").write_bytes(b"weights")
        return {"data_sha256": file_sha256(history)}

    def predict(portable: Path, contexts: np.ndarray) -> np.ndarray:
        assert portable.name == "portable"
        assert contexts.shape == (40, 32)
        return np.tile(np.linspace(1, 0, 10), 4)

    monkeypatch.setattr(
        "timesfm_finish_position.chronos_cell_research.sample_cell_parameters", sample
    )
    monkeypatch.setattr("timesfm_finish_position.chronos_cell_research.run_study", train)
    monkeypatch.setattr("timesfm_finish_position.chronos_cell_research.cpu_points", predict)
    report = run_cell_research(
        cell=cell,
        source_config=tmp_path / "config.json",
        output=tmp_path / "run",
        n_trials=2,
        max_steps=100,
    )
    assert report["production_eligible"] is False
    selected = report["selected"]
    assert isinstance(selected, dict)
    assert selected["delta"] == [4, 4, 4, 4, 4]
    assert selected["status"] == "trained_and_cpu_evaluated"
    assert (tmp_path / "run/trial-000.json").is_file()
    assert (tmp_path / "run/trial-001/cell-cpu-points.npy").is_file()


def test_infeasible_trials_are_audited(
    cell: ResearchCell, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def sample(
        trial: rustuna.Trial,
        *,
        scope: CellScope,
        seed: int,
        max_steps: int,
        history_bounds: tuple[int, int],
    ) -> CellParameters:
        return CellParameters(StudyConfig(batch_size=1000), 0.5)

    monkeypatch.setattr(
        "timesfm_finish_position.chronos_cell_research.sample_cell_parameters", sample
    )
    report = run_cell_research(
        cell=cell, source_config=tmp_path, output=tmp_path / "run", n_trials=1, max_steps=100
    )
    assert report["selected"] is None
    best = report["best_observed"]
    assert isinstance(best, dict)
    assert best["status"] == "infeasible_static_batch"


def test_input_guards(cell: ResearchCell, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="declared training domain"):
        load_cell_data(replace(cell, domain="nar"))
    with pytest.raises(ValueError, match="checksum"):
        load_cell_data(replace(cell, history_sha256="bad"))
    with pytest.raises(ValueError, match="Explicit targets"):
        load_cell_data(replace(cell, target_races=()))
    with pytest.raises(ValueError, match="Incomplete"):
        load_cell_data(replace(cell, target_races=("missing",)))
    with pytest.raises(ValueError, match="Targets must"):
        load_cell_data(replace(cell, target_races=("jra:2022:0",)))
    with pytest.raises(ValueError, match="budget"):
        run_cell_research(cell=cell, source_config=tmp_path, output=tmp_path, n_trials=0)
    monkeypatch.delenv("HF_HUB_OFFLINE")
    with pytest.raises(ValueError, match="HF_HUB_OFFLINE"):
        run_cell_research(cell=cell, source_config=tmp_path, output=tmp_path)


def test_decimal_odds_one_is_valid_market(cell: ResearchCell) -> None:
    table = pq.read_table(cell.history)
    table = table.set_column(
        table.schema.get_field_index("decimal_odds"), "decimal_odds", pa.array([1.0] * len(table))
    )
    pq.write_table(table, cell.history)
    data = load_cell_data(replace(cell, history_sha256=file_sha256(cell.history)))
    assert len(data.target_indices) == 4
    assert data.excluded_races == ()


def test_no_holdout_and_invalid_odds(cell: ResearchCell) -> None:
    table = pq.read_table(cell.history)
    future = table.set_column(1, "race_date", pa.array(["20240101"] * len(table)))
    pq.write_table(future, cell.history)
    with pytest.raises(ValueError, match="2024"):
        load_cell_data(replace(cell, history_sha256=file_sha256(cell.history)))
    invalid = table.set_column(
        table.schema.get_field_index("decimal_odds"),
        "decimal_odds",
        pa.array([None] * len(table), type=pa.float64()),
    )
    pq.write_table(invalid, cell.history)
    with pytest.raises(ValueError, match="No complete valid"):
        load_cell_data(replace(cell, history_sha256=file_sha256(cell.history)))


def test_development_predicts_without_current_target_value(cell: ResearchCell) -> None:
    data = load_cell_data(cell)
    values = data.values.copy()
    values[-1] = np.nan
    windows = cell_windows(
        replace(data, values=values), StudyConfig(minimum_history=1), development=True
    )
    assert len(windows.source_indices) == 40
    assert windows.source_indices[-1] == 239


def test_later_readout_has_separate_date_boundary(cell: ResearchCell) -> None:
    table = pq.read_table(cell.history)
    dates = np.asarray(table["race_date"].to_pylist(), dtype=np.str_)
    races = np.asarray(table["race_id"].to_pylist(), dtype=np.str_)
    dates = np.char.replace(dates, "2023", "2024")
    races = np.char.replace(races, "2023", "2024")
    table = table.set_column(0, "race_id", pa.array(races)).set_column(
        1, "race_date", pa.array(dates)
    )
    pq.write_table(table, cell.history)
    later = replace(
        cell,
        history_sha256=file_sha256(cell.history),
        target_races=("jra:2024:0", "jra:2024:1", "jra:2024:2", "jra:2024:3"),
    )
    assert len(load_validation_data(later).target_indices) == 4
    with pytest.raises(ValueError, match="2024"):
        load_cell_data(later)
    pq.write_table(
        table.set_column(1, "race_date", pa.array(["20260907"] * len(table))), cell.history
    )
    with pytest.raises(ValueError, match="cutoff"):
        load_validation_data(replace(later, history_sha256=file_sha256(cell.history)))


def test_digest_verified_cpu_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "model.safetensors").write_bytes(b"weights")
    (tmp_path / "config.json").write_text('{"chronos_config":{}}', encoding="utf-8")
    (tmp_path / "export_metadata.json").write_text(
        json.dumps({"model_sha256": hashlib.sha256(b"weights").hexdigest()}), encoding="utf-8"
    )

    def predict(
        self: Chronos2Forecaster, contexts: Sequence[FloatArray], *, horizon: int
    ) -> tuple[FloatArray, ...]:
        assert self.device == "cpu"
        assert horizon == 1
        return (np.array([[0.8]]),)

    monkeypatch.setattr(Chronos2Forecaster, "predict", predict)
    np.testing.assert_allclose(cpu_points(tmp_path, np.ones((1, 32), dtype=np.float32)), [0.8])
    with pytest.raises(ValueError, match="complete and finite"):
        cpu_points(tmp_path, np.ones((2, 32), dtype=np.float32))
    (tmp_path / "export_metadata.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="identify model"):
        cpu_points(tmp_path, np.ones((1, 32), dtype=np.float32))


def test_manifest_and_cli(
    cell: ResearchCell, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, cohorts = tmp_path / "manifest.json", tmp_path / "cohorts.json"
    manifest.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_id": "cell-a",
                        "domain": "jra",
                        "history_path": str(cell.history),
                        "history_sha256": cell.history_sha256,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    cohorts.write_text(
        json.dumps({"cells": [{"cell_id": "cell-a", "development_race_ids": ["jra:2023:0"]}]}),
        encoding="utf-8",
    )
    assert load_research_cells(manifest, cohorts)[0].target_races == ("jra:2023:0",)

    def run(
        *, cell: ResearchCell, source_config: Path, output: Path, n_trials: int, max_steps: int
    ) -> dict[str, object]:
        assert cell.cell_id == "cell-a"
        assert n_trials == 16
        assert max_steps == 400
        assert output.parent.name == "jra"
        return {}

    monkeypatch.setattr("timesfm_finish_position.chronos_cell_research.run_cell_research", run)

    def configure_threads(count: int) -> None:
        assert count == 4

    monkeypatch.setattr("torch.set_num_threads", configure_threads)
    monkeypatch.setattr(
        "sys.argv",
        [
            "research",
            "--manifest",
            str(manifest),
            "--cohorts",
            str(cohorts),
            "--source-config",
            "config",
            "--output",
            str(tmp_path),
        ],
    )
    main()
    manifest.write_text('{"cells":[{"cell_id":null}]}', encoding="utf-8")
    with pytest.raises(ValueError, match="manifest string"):
        load_research_cells(manifest, cohorts)
