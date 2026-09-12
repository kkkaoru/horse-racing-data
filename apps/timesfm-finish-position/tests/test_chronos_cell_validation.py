import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timesfm_finish_position.chronos_cell_validation as module
from timesfm_finish_position.chronos_cell_research import ResearchCell, file_sha256
from timesfm_finish_position.chronos_cell_validation import Selection, run_validation, validate_cell


@pytest.fixture
def cell(tmp_path: Path) -> ResearchCell:
    history = tmp_path / "history.parquet"
    years = np.repeat(np.array(["2020", "2024", "2025", "2026"]), 4)
    pq.write_table(
        pa.table(
            {
                "race_id": np.char.add("jra:", years),
                "race_date": np.char.add(years, "0101"),
                "horse_id": np.tile(["h1", "h2", "h3", "h4"], 4),
                "venue_code": ["05"] * 16,
                "finish_position": np.tile([1, 2, 3, 4], 4),
                "field_size": [4] * 16,
                "decimal_odds": np.tile([4.0, 3.0, 2.0, 1.5], 4),
                "performance_rating": np.tile([1.0, 0.66, 0.33, 0.0], 4),
            }
        ),
        history,
    )
    return ResearchCell(
        "cell", history, file_sha256(history), ("jra:2024", "jra:2025", "jra:2026"), "jra"
    )


@pytest.fixture
def selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Selection:
    portable = tmp_path / "trained" / "portable"
    portable.mkdir(parents=True)
    (portable / "model.safetensors").write_bytes(b"frozen")
    (portable / "config.json").write_text("{}", encoding="utf-8")

    def predict(portable: Path, contexts: npt.NDArray[np.float32]) -> npt.NDArray[np.float64]:
        return contexts[:, -1].astype(np.float64)

    monkeypatch.setattr(module, "cpu_points", predict)
    return Selection(portable, file_sha256(portable / "model.safetensors"), 32, 1, 0.0, False)


def test_fixed_model_years(cell: ResearchCell, selection: Selection, tmp_path: Path) -> None:
    report = validate_cell(cell=cell, selection=selection, output=tmp_path / "result")
    annual = report["annual"]
    assert isinstance(annual, dict)
    assert annual["2024"]["delta"] == [1, 1, 1, 1, 0]
    assert annual["2025"]["known_runners"] == 4
    assert annual["2026"]["exact_rank_support"] == [1, 1, 1, 1, 0]
    assert report["production_eligible"] is False
    assert np.load(tmp_path / "result" / "cpu-points.npy").shape == (12,)


def test_no_selection_and_sparse(cell: ResearchCell, selection: Selection, tmp_path: Path) -> None:
    report = validate_cell(cell=cell, selection=None, output=tmp_path / "baseline")
    assert report["status"] == "no_development_selection"
    sparse = validate_cell(
        cell=cell, selection=replace(selection, minimum_history=100), output=tmp_path / "sparse"
    )
    annual = sparse["annual"]
    assert isinstance(annual, dict)
    assert annual["2024"]["known_runners"] == 0
    assert annual["2024"]["delta"] == [0, 0, 0, 0, 0]


def test_validation_guards(cell: ResearchCell, selection: Selection, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="model checksum"):
        validate_cell(
            cell=cell, selection=replace(selection, model_sha256="bad"), output=tmp_path / "bad"
        )
    table = pq.read_table(cell.history)
    odds = np.asarray(table["decimal_odds"].to_pylist())
    odds[4] = 0.0
    pq.write_table(
        table.set_column(
            table.schema.get_field_index("decimal_odds"), "decimal_odds", pa.array(odds)
        ),
        cell.history,
    )
    with pytest.raises(ValueError, match="Unscorable"):
        validate_cell(
            cell=replace(cell, history_sha256=file_sha256(cell.history)),
            selection=None,
            output=tmp_path / "invalid",
        )


def test_manifest_cli(cell: ResearchCell, selection: Selection, tmp_path: Path) -> None:
    freeze = tmp_path / "freeze.json"
    frozen = {
        "domain": "jra",
        "cells": [
            {
                "cell_id": "cell",
                "scope": {"domain": "jra"},
                "selected": {
                    "config": {
                        "training_domain": "jra",
                        "context_length": 32,
                        "minimum_history": 1,
                    },
                    "output": str(selection.portable.parent),
                    "model_sha256": selection.model_sha256,
                    "market_weight": 0.0,
                    "relative": False,
                },
            }
        ],
    }
    freeze.write_text(json.dumps(frozen), encoding="utf-8")
    digest = file_sha256(freeze)
    cohorts = {
        "freeze_sha256": digest,
        "cells": [{"cell_id": "cell", "race_ids": list(cell.target_races)}],
    }
    (tmp_path / "jra-target-cohorts.json").write_text(json.dumps(cohorts), encoding="utf-8")
    (tmp_path / "history-manifest.json").write_text(
        json.dumps({"history": str(cell.history), "sha256": cell.history_sha256}), encoding="utf-8"
    )
    module.main(
        [
            "--freeze",
            str(freeze),
            "--freeze-sha256",
            digest,
            "--inputs",
            str(tmp_path),
            "--output",
            str(tmp_path / "cli"),
        ]
    )
    assert (tmp_path / "cli/jra/cell/report.json").is_file()
    with pytest.raises(ValueError, match="freeze checksum"):
        run_validation(freeze=freeze, freeze_sha256="bad", inputs=tmp_path, output=tmp_path)
    cohorts["freeze_sha256"] = "bad"
    (tmp_path / "jra-target-cohorts.json").write_text(json.dumps(cohorts), encoding="utf-8")
    with pytest.raises(ValueError, match="different development"):
        run_validation(freeze=freeze, freeze_sha256=digest, inputs=tmp_path, output=tmp_path)


@pytest.mark.parametrize("domain", ["banei", "unknown"])
def test_domain_mismatch_rejected_before_input_loading(tmp_path: Path, domain: str) -> None:
    freeze = tmp_path / "freeze.json"
    freeze.write_text(
        json.dumps({"domain": domain, "cells": [{"scope": {"domain": "jra"}}]}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="domain"):
        run_validation(
            freeze=freeze,
            freeze_sha256=file_sha256(freeze),
            inputs=tmp_path,
            output=tmp_path / "out",
        )


def test_banei_uses_its_own_cohort_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    freeze = tmp_path / "freeze.json"
    freeze.write_text(
        json.dumps(
            {
                "domain": "banei",
                "cells": [{"cell_id": "ban-cell", "scope": {"domain": "banei"}, "selected": None}],
            }
        ),
        encoding="utf-8",
    )
    digest = file_sha256(freeze)
    (tmp_path / "banei-target-cohorts.json").write_text(
        json.dumps(
            {"freeze_sha256": digest, "cells": [{"cell_id": "ban-cell", "race_ids": ["nar:2024"]}]}
        ),
        encoding="utf-8",
    )
    (tmp_path / "history-manifest.json").write_text(
        json.dumps({"history": "history.parquet", "sha256": "digest"}), encoding="utf-8"
    )

    def evaluate(
        *, cell: ResearchCell, selection: Selection | None, output: Path
    ) -> dict[str, object]:
        assert cell.domain == "banei"
        assert selection is None
        assert output.parent.name == "banei"
        return {"production_eligible": False}

    monkeypatch.setattr(module, "validate_cell", evaluate)
    run_validation(freeze=freeze, freeze_sha256=digest, inputs=tmp_path, output=tmp_path / "out")


@pytest.mark.parametrize("value", [None, "", 1])
def test_text_guard(value: object) -> None:
    with pytest.raises(ValueError, match="string"):
        module.parse_text(value)


@pytest.mark.parametrize("value", [None, 0, True])
def test_integer_guard(value: object) -> None:
    with pytest.raises(ValueError, match="integer"):
        module.parse_integer(value)


@pytest.mark.parametrize("value", [None, -1.0, float("nan"), True])
def test_weight_guard(value: object) -> None:
    with pytest.raises(ValueError, match="weight"):
        module.parse_weight(value)


def test_representation_guard() -> None:
    with pytest.raises(ValueError, match="representation"):
        module.parse_relative("true")
