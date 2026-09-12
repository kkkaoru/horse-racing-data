"""Actual cell-local MLX training with portable CPU development objectives.

Market-only research is deliberately separate from the strict incumbent-aware
application tuner. These results cannot authorize production application.
"""

import argparse
import gc
import hashlib
import json
import math
import os
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path

import mlx.core as mx
import numpy as np
import numpy.typing as npt
import pyarrow.parquet as pq
import rustuna
import torch

from timesfm_finish_position.chronos_cell_diagnostics import (
    exact_hits,
    exact_support,
    race_hits,
    relative_scores,
)
from timesfm_finish_position.chronos_cell_tuning import CellScope, sample_cell_parameters
from timesfm_finish_position.chronos_domains import (
    TrainingDomain,
    domain_contexts,
    domain_labels,
    parse_training_domain,
)
from timesfm_finish_position.chronos_forecasting import Chronos2Forecaster
from timesfm_finish_position.chronos_market_prior import MarketPrior, market_scores
from timesfm_finish_position.chronos_mlx_data import (
    HorseWindows,
    WindowConfig,
    build_windows,
    filter_window_targets,
)
from timesfm_finish_position.chronos_mlx_study import StudyConfig, run_study
from timesfm_finish_position.chronos_portable import PortableArtifact
from timesfm_finish_position.chronos_readout import ReadoutRace


@dataclass(frozen=True)
class ResearchCell:
    cell_id: str
    history: Path
    history_sha256: str
    target_races: tuple[str, ...]
    domain: TrainingDomain


@dataclass(frozen=True)
class CellData:
    race_ids: npt.NDArray[np.str_]
    horse_ids: npt.NDArray[np.str_]
    dates: npt.NDArray[np.str_]
    values: npt.NDArray[np.float32]
    finish: npt.NDArray[np.int64]
    odds: npt.NDArray[np.float64]
    field_sizes: npt.NDArray[np.int64]
    label_rows: npt.NDArray[np.bool_]
    target_indices: tuple[npt.NDArray[np.int64], ...]
    excluded_races: tuple[str, ...]


@dataclass(frozen=True)
class ForecastRows:
    """Source identity for cached forecasts, without fabricated training windows."""

    source_indices: npt.NDArray[np.int64]


def file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load_cell_data(cell: ResearchCell) -> CellData:
    return _load_dated_data(cell, start="20230101", end="20231231")


def load_validation_data(cell: ResearchCell) -> CellData:
    """Separate fixed later-year readout; never used by the research objective."""
    return _load_dated_data(cell, start="20240101", end="20260906")


def _load_dated_data(cell: ResearchCell, *, start: str, end: str) -> CellData:
    if file_sha256(cell.history) != cell.history_sha256:
        raise ValueError("Frozen cell history checksum mismatch")
    table = pq.read_table(cell.history)
    context_keep = domain_contexts(
        np.asarray(table["venue_code"].to_pylist(), dtype=np.str_), cell.domain
    )
    table = table.take(np.flatnonzero(context_keep))
    races = np.asarray(table["race_id"].to_pylist(), dtype=np.str_)
    horses = np.asarray(table["horse_id"].to_pylist(), dtype=np.str_)
    dates = np.asarray(table["race_date"].to_pylist(), dtype=np.str_)
    if not cell.target_races or len(set(zip(races, horses, strict=True))) != len(races):
        raise ValueError("Explicit targets and unique runner identities are required")
    if np.any(dates > end):
        raise ValueError(f"Input exceeds cutoff {end}; 2024+ is forbidden in development")
    finish = np.asarray(table["finish_position"].to_pylist(), dtype=np.int64)
    odds = np.asarray(table["decimal_odds"].to_pylist(), dtype=np.float64)
    sizes = np.asarray(table["field_size"].to_pylist(), dtype=np.int64)
    labels = domain_labels(
        races, np.asarray(table["venue_code"].to_pylist(), dtype=np.str_), cell.domain
    )
    indices: list[npt.NDArray[np.int64]] = []
    excluded: list[str] = []
    for race in cell.target_races:
        selected = np.flatnonzero(races == race)
        if len(selected) == 0 or not np.all(sizes[selected] == len(selected)):
            raise ValueError("Incomplete frozen target race")
        if not np.all(labels[selected]):
            raise ValueError("Target races do not match the declared training domain")
        if not np.all((dates[selected] >= start) & (dates[selected] <= end)):
            raise ValueError(f"Targets must be between {start} and {end}")
        if not np.all(np.isfinite(odds[selected]) & (odds[selected] >= 1)) or not np.any(
            finish[selected] == 1
        ):
            excluded.append(race)
        else:
            indices.append(selected)
    if not indices:
        raise ValueError("No complete valid-market target races")
    return CellData(
        races,
        horses,
        dates,
        np.asarray(table["performance_rating"].to_pylist(), dtype=np.float32),
        finish,
        odds,
        sizes,
        labels,
        tuple(indices),
        tuple(excluded),
    )


def cell_windows(data: CellData, config: StudyConfig, *, development: bool) -> HorseWindows:
    windows = build_windows(
        horse_ids=data.horse_ids,
        dates=data.dates,
        values=data.values,
        config=WindowConfig(
            config.development_start if development else config.training_start,
            config.development_end if development else config.training_end,
            config.context_length,
            config.minimum_history,
            require_finite_targets=not development,
        ),
    )
    keep = data.label_rows.copy()
    if development:
        keep = np.zeros(len(data.race_ids), dtype=np.bool_)
        keep[np.concatenate(data.target_indices)] = True
    return filter_window_targets(windows, keep)


def cpu_points(portable: Path, contexts: npt.NDArray[np.float32]) -> npt.NDArray[np.float64]:
    metadata = json.loads((portable / "export_metadata.json").read_text(encoding="utf-8"))
    digest = metadata.get("model_sha256")
    if not isinstance(digest, str):
        raise ValueError("Export must identify model weights")
    PortableArtifact(portable, digest, file_sha256(portable / "config.json")).verify()
    forecaster = Chronos2Forecaster(checkpoint=str(portable), device="cpu", batch_size=128)
    forecasts = forecaster.predict(
        [np.asarray(row[None], dtype=np.float64) for row in contexts], horizon=1
    )
    points = np.asarray([float(value[0, 0]) for value in forecasts], dtype=np.float64)
    if len(points) != len(contexts) or not np.isfinite(points).all():
        raise ValueError("CPU forecasts must be complete and finite")
    return points


def forecast_races(
    data: CellData,
    *,
    windows: HorseWindows | ForecastRows,
    points: npt.NDArray[np.float64],
    relative: bool,
    market_prior: MarketPrior = "rank_percentile",
) -> list[ReadoutRace]:
    """Expose the identical race inputs for paired, race-level effect diagnostics."""
    scores = np.full(len(data.race_ids), np.nan, dtype=np.float64)
    scores[windows.source_indices] = points
    races: list[ReadoutRace] = []
    for indices in data.target_indices:
        ids = data.horse_ids[indices]
        market = market_scores(odds=data.odds[indices], horse_ids=ids, prior=market_prior)
        temporal = relative_scores(scores[indices]) if relative else scores[indices]
        races.append(ReadoutRace(ids, market, data.finish[indices], temporal))
    return races


def evaluate_points(
    data: CellData,
    *,
    windows: HorseWindows | ForecastRows,
    points: npt.NDArray[np.float64],
    relative: bool,
    market_weight: float,
    market_prior: MarketPrior = "rank_percentile",
) -> dict[str, object]:
    races = forecast_races(
        data, windows=windows, points=points, relative=relative, market_prior=market_prior
    )
    baseline, hits = exact_hits(races, 1.0), exact_hits(races, market_weight)
    delta = [value - base for value, base in zip(hits, baseline, strict=True)]
    violation = sum(max(0, -value) for value in delta[1:])
    objective = (
        -1e9 - 1000 * violation + delta[0] if violation else 1000 * delta[0] + sum(delta[1:])
    )
    return {
        "races": len(races),
        "primary_metric": "exact finishing-position matches, ranks 1-5",
        "market_prior": market_prior,
        "exact_rank_support": exact_support(races),
        "market_winner_topk": race_hits(races, 1.0),
        "model_winner_topk": race_hits(races, market_weight),
        "market_hits": baseline,
        "model_hits": hits,
        "delta": delta,
        "objective": float(objective),
        "market_guard": delta[0] > 0 and violation == 0,
        "known_runners": len(points),
        "production_eligible": False,
    }


def _objective_value(record: dict[str, object]) -> float:
    value = record["objective"]
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("Trial objective must be finite")
    return float(value)


def run_cell_research(
    *,
    cell: ResearchCell,
    source_config: Path,
    output: Path,
    n_trials: int = 16,
    max_steps: int = 400,
) -> dict[str, object]:
    if n_trials < 1 or max_steps < 100 or max_steps % 100:
        raise ValueError("Invalid research trial budget")
    if os.environ.get("HF_HUB_OFFLINE") != "1":
        raise ValueError("Research CPU evaluation requires HF_HUB_OFFLINE=1")
    data = load_cell_data(cell)
    output.mkdir(parents=True, exist_ok=False)
    keys = sorted(
        f"{data.race_ids[index]}:{data.horse_ids[index]}"
        for index in np.concatenate(data.target_indices)
    )
    identity = hashlib.sha256("\n".join(keys).encode()).hexdigest()
    scope = CellScope(
        cell.cell_id,
        cell.history_sha256,
        identity,
        date(2022, 12, 31),
        date(2023, 1, 1),
        date(2023, 12, 31),
        date(2026, 9, 13),
        ((2023, len(data.target_indices)),),
        domain=cell.domain,
    )
    seed = int.from_bytes(
        hashlib.sha256(f"{cell.domain}:{cell.cell_id}".encode()).digest()[:4], "big"
    )
    study = rustuna.create_study(
        study_name=f"chronos-research:{cell.domain}:{cell.cell_id}",
        direction="maximize",
        sampler=rustuna.samplers.TPESampler(seed=seed, n_startup_trials=4),
        storage=rustuna.storages.SQLite3Storage(str(output / "study.sqlite"), create_database=True),
    )
    records: list[dict[str, object]] = []

    def objective(trial: rustuna.Trial) -> float:
        parameters = sample_cell_parameters(
            trial, scope=scope, seed=seed, max_steps=max_steps, history_bounds=(1, 4)
        )
        config = replace(
            parameters.training,
            training_domain=cell.domain,
            target_race_prefix="jra:" if cell.domain == "jra" else "nar:",
        )
        relative = trial.suggest_int("relative_representation", 0, 1) == 1
        train = cell_windows(data, config, development=False)
        directory = output / f"trial-{trial.number:03d}"
        record: dict[str, object] = {
            "trial": trial.number,
            "config": asdict(config),
            "market_weight": parameters.market_weight,
            "relative": relative,
            "output": str(directory),
            "production_eligible": False,
        }
        if len(train.targets) < config.batch_size:
            record.update(
                {"status": "infeasible_static_batch", "objective": -1e12, "market_guard": False}
            )
        else:
            training = run_study(
                history=cell.history,
                source_config=source_config,
                output=directory,
                config=config,
                checkpoint_every=max_steps,
            )
            if training["data_sha256"] != cell.history_sha256:
                raise ValueError("Training source changed during study")
            windows = cell_windows(data, config, development=True)
            points = cpu_points(directory / "portable", windows.contexts)
            record.update(
                evaluate_points(
                    data,
                    windows=windows,
                    points=points,
                    relative=relative,
                    market_weight=parameters.market_weight,
                )
            )
            record.update(
                {
                    "status": "trained_and_cpu_evaluated",
                    "model_sha256": file_sha256(directory / "portable/model.safetensors"),
                }
            )
            np.save(directory / "cell-cpu-points.npy", points)
            np.save(directory / "cell-source-indices.npy", windows.source_indices)
        records.append(record)
        (output / f"trial-{trial.number:03d}.json").write_text(
            json.dumps(record, indent=2), encoding="utf-8"
        )
        print(
            json.dumps(
                {
                    "cell": cell.cell_id,
                    "trial": trial.number,
                    "status": record["status"],
                    "delta": record.get("delta"),
                }
            ),
            flush=True,
        )
        gc.collect()
        mx.clear_cache()
        return _objective_value(record)

    study.optimize(objective, n_trials=n_trials)
    passing = [record for record in records if record.get("market_guard") is True]
    report: dict[str, object] = {
        "scope": asdict(scope),
        "trials": records,
        "selected": max(passing, key=_objective_value) if passing else None,
        "best_observed": max(records, key=_objective_value),
        "excluded_invalid_market_races": data.excluded_races,
        "comparison": "market-only development research; incumbent unavailable",
        "production_eligible": False,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report


def _string(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Expected nonempty manifest string")
    return value


def load_research_cells(manifest: Path, cohorts: Path) -> list[ResearchCell]:
    declarations = json.loads(cohorts.read_text(encoding="utf-8"))["cells"]
    targets = {
        _string(item["cell_id"]): tuple(_string(value) for value in item["development_race_ids"])
        for item in declarations
    }
    entries = json.loads(manifest.read_text(encoding="utf-8"))["cells"]
    return [
        ResearchCell(
            _string(item["cell_id"]),
            Path(_string(item["history_path"])),
            _string(item["history_sha256"]),
            targets[_string(item["cell_id"])],
            parse_training_domain(item.get("domain")),
        )
        for item in entries
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cohorts", type=Path, required=True)
    parser.add_argument("--source-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=16)
    parser.add_argument("--max-steps", type=int, default=400)
    args = parser.parse_args()
    torch.set_num_threads(4)
    for cell in load_research_cells(args.manifest, args.cohorts):
        run_cell_research(
            cell=cell,
            source_config=args.source_config,
            output=args.output / cell.domain / cell.cell_id,
            n_trials=args.trials,
            max_steps=args.max_steps,
        )


if __name__ == "__main__":
    main()
