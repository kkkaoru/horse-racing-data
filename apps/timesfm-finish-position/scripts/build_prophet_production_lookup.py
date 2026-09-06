#!/usr/bin/env python3
"""Build the frozen, Prophet-free production lookup parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prophet import Prophet

from timesfm_finish_position.prophet_lookup import ProphetLookupRows, build_prophet_lookup_rows

SOURCE_COLUMNS = (
    "race_date",
    "venue_code",
    "jockey_code",
    "trainer_code",
    "performance_rating",
)
ENTITY_TYPES = ("venue", "jockey", "trainer")


@dataclass(frozen=True)
class LookupBuildOptions:
    """Shared causal fitting options for one annual artifact."""

    year: int
    max_entities: int
    minimum_history_rows: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", "--nar-input", dest="nar_input", type=Path, required=True)
    parser.add_argument("--jra-input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--max-entities", type=int, default=32)
    parser.add_argument("--minimum-history-rows", type=int, default=100)
    return parser.parse_args()


def prophet_forecast(dates: np.ndarray, values: np.ndarray, target_dates: np.ndarray) -> np.ndarray:
    model = Prophet(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        n_changepoints=5,
        changepoint_prior_scale=0.05,
        uncertainty_samples=0,
    )
    model.fit(pd.DataFrame({"ds": pd.to_datetime(dates), "y": values}), algorithm="LBFGS")
    return np.asarray(
        model.predict(pd.DataFrame({"ds": pd.to_datetime(target_dates)}))["yhat"].to_numpy(),
        dtype=np.float64,
    )


def _build_source(
    source_path: Path,
    categories: tuple[str, ...],
    options: LookupBuildOptions,
) -> ProphetLookupRows:
    source = pq.read_table(source_path, columns=list(SOURCE_COLUMNS))
    columns = {name: np.asarray(source.column(name).to_pylist()) for name in source.column_names}
    return build_prophet_lookup_rows(
        race_dates=columns["race_date"].astype(np.str_),
        entity_columns=(
            columns["venue_code"].astype(np.str_),
            columns["jockey_code"].astype(np.str_),
            columns["trainer_code"].astype(np.str_),
        ),
        entity_types=ENTITY_TYPES,
        performance=columns["performance_rating"].astype(np.float64),
        year=options.year,
        forecaster=prophet_forecast,
        categories=categories,
        max_entities=options.max_entities,
        minimum_history_rows=options.minimum_history_rows,
    )


def _arrow_columns(result: ProphetLookupRows) -> dict[str, np.ndarray]:
    return {
        "category": result.category,
        "forecast_date": result.forecast_date,
        "entity_type": result.entity_type,
        "entity_code": result.entity_code,
        "yhat": result.yhat,
    }


def main() -> None:
    args = parse_args()
    logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
    options = LookupBuildOptions(args.year, args.max_entities, args.minimum_history_rows)
    jra = _build_source(args.jra_input, ("jra",), options)
    nar = _build_source(args.nar_input, ("nar", "ban-ei"), options)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    table = pa.concat_tables([pa.table(_arrow_columns(jra)), pa.table(_arrow_columns(nar))])
    pq.write_table(table, args.output, compression="zstd")
    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
    metadata = {
        "artifact": args.output.name,
        "sha256": digest,
        "source_sha256": {
            "jra": hashlib.sha256(args.jra_input.read_bytes()).hexdigest(),
            "nar": hashlib.sha256(args.nar_input.read_bytes()).hexdigest(),
        },
        "year": args.year,
        "rows": len(table),
        "selected_entities": {
            "jra": jra.selected_entities,
            "nar_and_ban_ei": nar.selected_entities,
        },
        "history_contract": f"race_date < {args.year}0101",
        "categories": ["jra", "nar", "ban-ei"],
        "runtime_prophet_dependency": False,
    }
    args.output.with_suffix(".json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(metadata, sort_keys=True))


if __name__ == "__main__":
    main()
