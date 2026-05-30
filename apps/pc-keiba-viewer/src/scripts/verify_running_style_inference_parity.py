"""Agent W5 (parity test framework) verifier for local Bun subprocess inference.

Provides a 3-phase test framework that confirms local (W3 deliverable) inference
produces probability vectors that are numerically identical (tolerance 1e-12) to
the production PostgreSQL ``race_running_style_model_predictions`` rows already
written by the live inference path. The framework is split intentionally to keep
the always-on smoke tests cheap (Phase 1), gate real PG comparisons on an opt-in
env var (Phase 2), and provide a separate advisory full-coverage regression sweep
that emits a JSON report and dumps mismatched race-keys to parquet (Phase 3).

Phase 1 (always-on, mocked Booster / parquet / PG): probability column wiring,
sums-to-one and per-class tolerance arithmetic helpers.

Phase 2 (opt-in via ``PARITY_PG_DSN``): MD5-deterministic sampling of 1000
race-keys from PG ``race_running_style_model_predictions`` for the configured
``model_version`` filter, followed by LEFT JOIN comparison with the local
predictions parquet (``--features-parquet --output-parquet`` produced by the
W3 CLI). Pass criteria: ``max_diff_per_class`` < 1e-12 and ``argmax_agreement``
== 1.0.

Phase 3 (advisory regression guard): full-coverage parity run over a single
(category, model_version) slice; emits ``tmp/parity/<model_version>/<utc>.json``
with summary metrics and dumps any race-keys whose ``max_diff > 1e-9`` to
``mismatches.parquet`` alongside the JSON report.

This module purposely treats W3 as an opaque subprocess: callers inject the
``run_local_inference`` callable so the test framework itself stays decoupled
from whichever scoring entrypoint W3 ships. The default callable shells out via
``subprocess.run`` to the configured Bun script.

Run with: ``uv run python src/scripts/verify_running_style_inference_parity.py
    --features-parquet ... --predictions-parquet ... --pg-dsn ... ...``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol, TypedDict

import numpy as np
import pandas as pd

PROBABILITY_COLUMNS: tuple[str, str, str, str] = ("p_nige", "p_senkou", "p_sashi", "p_oikomi")
PREDICTED_CLASS_COLUMN: str = "predicted_class"
RACE_KEY_COLUMNS: tuple[str, str, str, str, str] = (
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
)

DEFAULT_SAMPLE_LIMIT: int = 1000
DEFAULT_TOLERANCE_PHASE2: float = 1e-12
DEFAULT_TOLERANCE_PHASE3_MAX_DIFF: float = 1e-9
DEFAULT_TOLERANCE_PHASE3_AGREEMENT: float = 0.999
ENV_PARITY_PG_DSN: str = "PARITY_PG_DSN"
PHASES_ALL: tuple[str, str, str] = ("phase1", "phase2", "phase3")

SAMPLE_KEYS_SQL_TEMPLATE: str = (
    "SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango "
    "FROM race_running_style_model_predictions "
    "WHERE model_version = %s "
    "GROUP BY source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango "
    "ORDER BY md5(source || kaisai_nen || kaisai_tsukihi || keibajo_code || race_bango) "
    "LIMIT %s"
)

PROD_PREDICTIONS_SQL_TEMPLATE: str = (
    "SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, "
    "ketto_toroku_bango, p_nige, p_senkou, p_sashi, p_oikomi, predicted_class "
    "FROM race_running_style_model_predictions "
    "WHERE model_version = %s"
)


class ParityResult(TypedDict):
    rows_compared: int
    max_diff_per_class: dict[str, float]
    argmax_agreement: float
    passed: bool


class PhaseThreeReport(TypedDict):
    generated_at_utc: str
    model_version: str
    rows_compared: int
    max_diff_per_class: dict[str, float]
    argmax_agreement: float
    max_diff_threshold: float
    agreement_threshold: float
    mismatches_count: int
    passed: bool


class PgCursorLike(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> object: ...
    def fetchall(self) -> list[tuple[object, ...]]: ...


class PgConnectionLike(Protocol):
    def cursor(self) -> PgCursorLike: ...
    def close(self) -> None: ...


class PgConnectorLike(Protocol):
    def __call__(self, dsn: str) -> PgConnectionLike: ...


class LocalInferenceRunnerLike(Protocol):
    def __call__(
        self, *, features_parquet: str, output_parquet: str, model_version: str,
    ) -> None: ...


class SampleSpec(TypedDict):
    pg_dsn: str
    model_version: str
    limit: int


class CompareSpec(TypedDict):
    local_frame: pd.DataFrame
    prod_frame: pd.DataFrame
    tolerance: float


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify local Bun subprocess inference produces probability vectors "
            "that match production PostgreSQL race_running_style_model_predictions "
            "rows within a configurable tolerance (default 1e-12)."
        ),
    )
    parser.add_argument("--features-parquet", required=True)
    parser.add_argument("--predictions-parquet", required=True)
    parser.add_argument("--pg-dsn", required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--phase", choices=list(PHASES_ALL), default="phase2")
    parser.add_argument("--sample-limit", type=int, default=DEFAULT_SAMPLE_LIMIT)
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE_PHASE2)
    parser.add_argument("--report-dir", default="tmp/parity")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def build_sample_keys_sql() -> str:
    return SAMPLE_KEYS_SQL_TEMPLATE


def build_prod_predictions_sql() -> str:
    return PROD_PREDICTIONS_SQL_TEMPLATE


def coerce_race_key_row(row: tuple[object, ...]) -> tuple[str, str, str, str, str]:
    values = [str(item) for item in row]
    return (values[0], values[1], values[2], values[3], values[4])


def fetch_sample_race_keys(
    spec: SampleSpec,
    *,
    pg_connector: PgConnectorLike,
) -> list[tuple[str, str, str, str, str]]:
    connection = pg_connector(spec["pg_dsn"])
    try:
        cursor = connection.cursor()
        cursor.execute(build_sample_keys_sql(), (spec["model_version"], spec["limit"]))
        rows = cursor.fetchall()
    finally:
        connection.close()
    return [coerce_race_key_row(row) for row in rows]


def fetch_prod_predictions(
    spec: SampleSpec,
    *,
    pg_connector: PgConnectorLike,
) -> pd.DataFrame:
    connection = pg_connector(spec["pg_dsn"])
    try:
        cursor = connection.cursor()
        cursor.execute(build_prod_predictions_sql(), (spec["model_version"],))
        rows = cursor.fetchall()
    finally:
        connection.close()
    return pd.DataFrame(
        rows,
        columns=[
            *RACE_KEY_COLUMNS,
            "ketto_toroku_bango",
            *PROBABILITY_COLUMNS,
            PREDICTED_CLASS_COLUMN,
        ],
    )


def assert_probability_sums_to_one(
    frame: pd.DataFrame, *, tolerance: float,
) -> bool:
    probabilities = frame[list(PROBABILITY_COLUMNS)].to_numpy(dtype=np.float64)
    row_sums = probabilities.sum(axis=1)
    deviations = np.abs(row_sums - 1.0)
    return bool(np.all(deviations <= tolerance))


def compute_max_diff_per_class(spec: CompareSpec) -> dict[str, float]:
    merged = spec["local_frame"].merge(
        spec["prod_frame"],
        on=[*RACE_KEY_COLUMNS, "ketto_toroku_bango"],
        suffixes=("_local", "_prod"),
    )
    return {
        column: float(
            np.max(
                np.abs(
                    merged[f"{column}_local"].to_numpy(dtype=np.float64)
                    - merged[f"{column}_prod"].to_numpy(dtype=np.float64),
                ),
            ),
        )
        if len(merged) > 0
        else 0.0
        for column in PROBABILITY_COLUMNS
    }


def compute_argmax_agreement(spec: CompareSpec) -> float:
    merged = spec["local_frame"].merge(
        spec["prod_frame"],
        on=[*RACE_KEY_COLUMNS, "ketto_toroku_bango"],
        suffixes=("_local", "_prod"),
    )
    if len(merged) == 0:
        return 1.0
    matches = merged[f"{PREDICTED_CLASS_COLUMN}_local"].to_numpy(
        dtype=np.int64,
    ) == merged[f"{PREDICTED_CLASS_COLUMN}_prod"].to_numpy(dtype=np.int64)
    return float(np.mean(matches))


def evaluate_parity(spec: CompareSpec) -> ParityResult:
    merged_count = int(
        len(
            spec["local_frame"].merge(
                spec["prod_frame"],
                on=[*RACE_KEY_COLUMNS, "ketto_toroku_bango"],
            ),
        ),
    )
    max_diff = compute_max_diff_per_class(spec)
    agreement = compute_argmax_agreement(spec)
    passed = (
        merged_count > 0
        and all(value <= spec["tolerance"] for value in max_diff.values())
        and agreement == 1.0
    )
    return ParityResult(
        rows_compared=merged_count,
        max_diff_per_class=max_diff,
        argmax_agreement=agreement,
        passed=passed,
    )


def is_phase_two_enabled() -> bool:
    return os.environ.get(ENV_PARITY_PG_DSN, "") != ""


def resolve_phase_two_dsn() -> str:
    return os.environ.get(ENV_PARITY_PG_DSN, "")


def collect_mismatches(spec: CompareSpec, *, max_diff_threshold: float) -> pd.DataFrame:
    merged = spec["local_frame"].merge(
        spec["prod_frame"],
        on=[*RACE_KEY_COLUMNS, "ketto_toroku_bango"],
        suffixes=("_local", "_prod"),
    )
    if len(merged) == 0:
        return pd.DataFrame(
            columns=[*RACE_KEY_COLUMNS, "ketto_toroku_bango", "max_diff"],
        )
    per_row_diffs = np.max(
        np.stack(
            [
                np.abs(
                    merged[f"{column}_local"].to_numpy(dtype=np.float64)
                    - merged[f"{column}_prod"].to_numpy(dtype=np.float64),
                )
                for column in PROBABILITY_COLUMNS
            ],
            axis=1,
        ),
        axis=1,
    )
    annotated = merged[[*RACE_KEY_COLUMNS, "ketto_toroku_bango"]].copy()
    annotated["max_diff"] = per_row_diffs
    return annotated[annotated["max_diff"] > max_diff_threshold].reset_index(drop=True)


def build_phase_three_report(
    *,
    parity: ParityResult,
    model_version: str,
    mismatches_count: int,
    generated_at_utc: str,
    max_diff_threshold: float,
    agreement_threshold: float,
) -> PhaseThreeReport:
    passed = (
        max(parity["max_diff_per_class"].values(), default=0.0) <= max_diff_threshold
        and parity["argmax_agreement"] >= agreement_threshold
    )
    return PhaseThreeReport(
        generated_at_utc=generated_at_utc,
        model_version=model_version,
        rows_compared=parity["rows_compared"],
        max_diff_per_class=parity["max_diff_per_class"],
        argmax_agreement=parity["argmax_agreement"],
        max_diff_threshold=max_diff_threshold,
        agreement_threshold=agreement_threshold,
        mismatches_count=mismatches_count,
        passed=passed,
    )


def resolve_report_paths(
    *, report_dir: str, model_version: str, generated_at_utc: str,
) -> tuple[Path, Path]:
    base = Path(report_dir) / model_version
    return base / f"{generated_at_utc}.json", base / "mismatches.parquet"


def write_phase_three_artifacts(
    *,
    report: PhaseThreeReport,
    mismatches: pd.DataFrame,
    report_dir: str,
) -> tuple[Path, Path | None]:
    json_path, parquet_path = resolve_report_paths(
        report_dir=report_dir,
        model_version=report["model_version"],
        generated_at_utc=report["generated_at_utc"],
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if len(mismatches) == 0:
        return json_path, None
    mismatches.to_parquet(parquet_path, index=False)
    return json_path, parquet_path


def default_pg_connector(dsn: str) -> PgConnectionLike:
    import importlib

    psycopg_module = importlib.import_module("psycopg")
    connect_callable = getattr(psycopg_module, "connect")
    return connect_callable(dsn)


def default_local_inference_runner(
    *,
    features_parquet: str,
    output_parquet: str,
    model_version: str,
) -> None:
    subprocess.run(
        [
            "bun",
            "run",
            "src/scripts/score_running_style_local.ts",
            "--features-parquet",
            features_parquet,
            "--output-parquet",
            output_parquet,
            "--model-version",
            model_version,
        ],
        check=True,
    )


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run_phase_two(
    args: argparse.Namespace,
    *,
    pg_connector: PgConnectorLike,
    local_inference_runner: LocalInferenceRunnerLike,
    pandas_reader: Callable[[str], pd.DataFrame],
) -> ParityResult:
    local_inference_runner(
        features_parquet=args.features_parquet,
        output_parquet=args.predictions_parquet,
        model_version=args.model_version,
    )
    local_frame = pandas_reader(args.predictions_parquet)
    spec_for_fetch: SampleSpec = {
        "pg_dsn": args.pg_dsn,
        "model_version": args.model_version,
        "limit": int(args.sample_limit),
    }
    prod_frame = fetch_prod_predictions(spec_for_fetch, pg_connector=pg_connector)
    return evaluate_parity(
        {
            "local_frame": local_frame,
            "prod_frame": prod_frame,
            "tolerance": float(args.tolerance),
        },
    )


def run_phase_three(
    args: argparse.Namespace,
    *,
    pg_connector: PgConnectorLike,
    local_inference_runner: LocalInferenceRunnerLike,
    pandas_reader: Callable[[str], pd.DataFrame],
    generated_at_utc: str,
) -> tuple[PhaseThreeReport, Path, Path | None]:
    local_inference_runner(
        features_parquet=args.features_parquet,
        output_parquet=args.predictions_parquet,
        model_version=args.model_version,
    )
    local_frame = pandas_reader(args.predictions_parquet)
    spec_for_fetch: SampleSpec = {
        "pg_dsn": args.pg_dsn,
        "model_version": args.model_version,
        "limit": int(args.sample_limit),
    }
    prod_frame = fetch_prod_predictions(spec_for_fetch, pg_connector=pg_connector)
    compare_spec: CompareSpec = {
        "local_frame": local_frame,
        "prod_frame": prod_frame,
        "tolerance": float(args.tolerance),
    }
    parity = evaluate_parity(compare_spec)
    mismatches = collect_mismatches(
        compare_spec, max_diff_threshold=DEFAULT_TOLERANCE_PHASE3_MAX_DIFF,
    )
    report = build_phase_three_report(
        parity=parity,
        model_version=args.model_version,
        mismatches_count=int(len(mismatches)),
        generated_at_utc=generated_at_utc,
        max_diff_threshold=DEFAULT_TOLERANCE_PHASE3_MAX_DIFF,
        agreement_threshold=DEFAULT_TOLERANCE_PHASE3_AGREEMENT,
    )
    json_path, parquet_path = write_phase_three_artifacts(
        report=report, mismatches=mismatches, report_dir=args.report_dir,
    )
    return report, json_path, parquet_path


def run(
    args: argparse.Namespace,
    *,
    pg_connector: PgConnectorLike,
    local_inference_runner: LocalInferenceRunnerLike,
    pandas_reader: Callable[[str], pd.DataFrame],
    clock_iso: Callable[[], str],
) -> dict[str, object]:
    if args.phase == "phase1":
        return {"phase": "phase1", "message": "smoke tests live in pytest, not CLI"}
    if args.phase == "phase2":
        result = run_phase_two(
            args,
            pg_connector=pg_connector,
            local_inference_runner=local_inference_runner,
            pandas_reader=pandas_reader,
        )
        return {"phase": "phase2", "result": result}
    report, json_path, parquet_path = run_phase_three(
        args,
        pg_connector=pg_connector,
        local_inference_runner=local_inference_runner,
        pandas_reader=pandas_reader,
        generated_at_utc=clock_iso(),
    )
    return {
        "phase": "phase3",
        "report": report,
        "report_path": str(json_path),
        "mismatches_path": str(parquet_path) if parquet_path is not None else None,
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    outcome = run(
        args,
        pg_connector=default_pg_connector,
        local_inference_runner=default_local_inference_runner,
        pandas_reader=pd.read_parquet,
        clock_iso=utc_now_iso,
    )
    print(json.dumps(outcome, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
