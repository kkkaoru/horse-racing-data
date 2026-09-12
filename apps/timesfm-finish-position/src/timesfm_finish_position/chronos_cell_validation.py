"""Fixed-selection later-year readout, never a hyperparameter search."""

import argparse
import json
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
import numpy.typing as npt
import torch

from timesfm_finish_position.chronos_cell_research import (
    ResearchCell,
    cell_windows,
    cpu_points,
    evaluate_points,
    file_sha256,
    load_validation_data,
)
from timesfm_finish_position.chronos_domains import parse_training_domain
from timesfm_finish_position.chronos_mlx_data import HorseWindows
from timesfm_finish_position.chronos_mlx_study import StudyConfig

YEARS: tuple[int, ...] = (2024, 2025, 2026)


@dataclass(frozen=True)
class Selection:
    portable: Path
    model_sha256: str
    context_length: int
    minimum_history: int
    market_weight: float
    relative: bool


def parse_text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Expected nonempty string")
    return value


def parse_integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("Expected positive integer")
    return value


def parse_weight(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not 0 <= value <= 1:
        raise ValueError("Invalid frozen market weight")
    return float(value)


def parse_relative(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError("Expected frozen representation flag")
    return value


def _subset(windows: HorseWindows, mask: npt.NDArray[np.bool_]) -> HorseWindows:
    return HorseWindows(
        windows.contexts[mask],
        windows.targets[mask],
        windows.source_indices[mask],
        windows.history_counts[mask],
    )


def validate_cell(
    *, cell: ResearchCell, selection: Selection | None, output: Path
) -> dict[str, object]:
    data = load_validation_data(cell)
    if data.excluded_races:
        raise ValueError(
            f"Unscorable target races must be resolved explicitly: {data.excluded_races}"
        )
    output.mkdir(parents=True, exist_ok=False)
    config = StudyConfig(
        context_length=selection.context_length if selection else 32,
        minimum_history=selection.minimum_history if selection else 1,
        training_domain=cell.domain,
        development_start="20240101",
        development_end="20260906",
    )
    windows = cell_windows(data, config, development=True)
    receipt: dict[str, object] = {
        "cell_id": cell.cell_id,
        "domain": cell.domain,
        "history_sha256": cell.history_sha256,
        "selection": None,
    }
    if selection is not None:
        if file_sha256(selection.portable / "model.safetensors") != selection.model_sha256:
            raise ValueError("Frozen selected model checksum mismatch")
        receipt["selection"] = {
            "model_sha256": selection.model_sha256,
            "config_sha256": file_sha256(selection.portable / "config.json"),
            "market_weight": selection.market_weight,
            "relative": selection.relative,
            "context_length": selection.context_length,
            "minimum_history": selection.minimum_history,
        }
    else:
        windows = _subset(windows, np.zeros(len(windows.source_indices), dtype=np.bool_))
    (output / "input-receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    points = (
        cpu_points(selection.portable, windows.contexts)
        if selection is not None and len(windows.source_indices)
        else np.empty(0, dtype=np.float64)
    )
    np.save(output / "cpu-points.npy", points)
    np.save(output / "source-indices.npy", windows.source_indices)
    annual: dict[str, object] = {}
    for year in YEARS:
        indices = tuple(
            index
            for index in data.target_indices
            if str(data.dates[index[0]]).startswith(str(year))
        )
        mask = np.char.startswith(data.dates[windows.source_indices], str(year))
        annual[str(year)] = evaluate_points(
            replace(data, target_indices=indices),
            windows=_subset(windows, mask),
            points=points[mask],
            relative=selection.relative if selection else False,
            market_weight=selection.market_weight if selection else 1.0,
        )
    report: dict[str, object] = {
        **receipt,
        "annual": annual,
        "status": "fixed_selected_model" if selection else "no_development_selection",
        "production_eligible": False,
        "evaluation_status": "previously observed years; no retuning",
        "incumbent_comparison": "unavailable",
    }
    (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def run_validation(*, freeze: Path, freeze_sha256: str, inputs: Path, output: Path) -> None:
    if file_sha256(freeze) != freeze_sha256:
        raise ValueError("Development freeze checksum mismatch")
    frozen = json.loads(freeze.read_text(encoding="utf-8"))
    domain = parse_training_domain(frozen.get("domain"))
    if any(parse_training_domain(item["scope"]["domain"]) != domain for item in frozen["cells"]):
        raise ValueError("Mixed-domain validation freeze")
    cohorts = json.loads((inputs / f"{domain}-target-cohorts.json").read_text(encoding="utf-8"))
    if cohorts["freeze_sha256"] != freeze_sha256:
        raise ValueError("Cohorts belong to a different development freeze")
    history = json.loads((inputs / "history-manifest.json").read_text(encoding="utf-8"))
    targets = {
        parse_text(item["cell_id"]): tuple(parse_text(race) for race in item["race_ids"])
        for item in cohorts["cells"]
    }
    if len(targets) != len(cohorts["cells"]) or set(targets) != {
        item["cell_id"] for item in frozen["cells"]
    }:
        raise ValueError("Frozen cell cohort mismatch")
    for item in frozen["cells"]:
        selected = item["selected"]
        selection = None
        if selected is not None:
            config = selected["config"]
            if config["training_domain"] != domain:
                raise ValueError("Selected training domain mismatch")
            selection = Selection(
                Path(parse_text(selected["output"])) / "portable",
                parse_text(selected["model_sha256"]),
                parse_integer(config["context_length"]),
                parse_integer(config["minimum_history"]),
                parse_weight(selected["market_weight"]),
                parse_relative(selected["relative"]),
            )
        cell = ResearchCell(
            parse_text(item["cell_id"]),
            Path(parse_text(history["history"])),
            parse_text(history["sha256"]),
            targets[item["cell_id"]],
            domain,
        )
        report = validate_cell(
            cell=cell, selection=selection, output=output / domain / cell.cell_id
        )
        print(json.dumps(report), flush=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, required=True)
    parser.add_argument("--freeze-sha256", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    torch.set_num_threads(4)
    run_validation(
        freeze=args.freeze, freeze_sha256=args.freeze_sha256, inputs=args.inputs, output=args.output
    )


if __name__ == "__main__":
    main()
