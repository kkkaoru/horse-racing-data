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

Phase 2 (opt-in via ``PARITY_PG_DSN`` env var or explicit ``--pg-dsn``):
MD5-deterministic sampling of 1000 race-keys from PG
``race_running_style_model_predictions`` for the configured ``model_version`` +
``year`` filter, filtering the W3 features parquet to those sampled race-keys,
spawning the W3 CLI (`run-running-style-inference-local.ts`) as a subprocess, and
LEFT JOIN comparison between the W3 output parquet and the production
predictions. Pass criteria: ``max_diff_per_class`` < 1e-12 and ``argmax_agreement``
== 1.0.

Phase 3 (advisory regression guard): full-coverage parity run over a single
(category, model_version) slice; emits ``tmp/parity/<model_version>/<utc>.json``
with summary metrics and dumps any race-keys whose ``max_diff > 1e-9`` to
``mismatches.parquet`` alongside the JSON report.

This module purposely treats W3 as an opaque subprocess: callers inject the
``local_inference_runner`` callable so the test framework itself stays decoupled
from whichever scoring entrypoint W3 ships. The default callable shells out via
``subprocess.run`` to the configured Bun script.

Run with: ``uv run python src/scripts/verify_running_style_inference_parity.py
    --features-parquet ... --output-parquet ... --model-flatbin ... ...``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from subprocess import CompletedProcess
from typing import Callable, Protocol, TypedDict

import numpy as np
import polars as pl

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
SUPPORTED_CATEGORIES: tuple[str, str] = ("jra", "nar")
W3_INFERENCE_SCRIPT_PATH: str = (
    "apps/sync-realtime-data/src/scripts/run-running-style-inference-local.ts"
)

SAMPLE_KEYS_SQL_TEMPLATE: str = (
    "SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango "
    "FROM race_running_style_model_predictions "
    "WHERE model_version = %s AND kaisai_nen = %s "
    "GROUP BY source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango "
    "ORDER BY md5(source || kaisai_nen || kaisai_tsukihi || keibajo_code || race_bango) "
    "LIMIT %s"
)

PROD_PREDICTIONS_SQL_TEMPLATE: str = (
    "SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, "
    "ketto_toroku_bango, p_nige, p_senkou, p_sashi, p_oikomi, predicted_class "
    "FROM race_running_style_model_predictions "
    "WHERE model_version = %s AND kaisai_nen = %s"
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


SubprocessRunner = Callable[..., CompletedProcess[str]]


class LocalInferenceRunnerLike(Protocol):
    def __call__(
        self,
        *,
        features_parquet: str,
        output_parquet: str,
        model_flatbin: str,
        category: str,
        model_version: str,
        feature_version: str,
        predicted_at: str,
        rs_p_from_flatbin: str | None,
        runner: SubprocessRunner,
    ) -> None: ...


class ParquetWriter(Protocol):
    def __call__(self, frame: pl.DataFrame, path: str) -> None: ...


class SampleSpec(TypedDict):
    pg_dsn: str
    model_version: str
    year: str
    limit: int


class CompareSpec(TypedDict):
    local_frame: pl.DataFrame
    prod_frame: pl.DataFrame
    tolerance: float


class PhaseTwoDeps(TypedDict):
    pg_connector: PgConnectorLike
    local_inference_runner: LocalInferenceRunnerLike
    parquet_reader: Callable[[str], pl.DataFrame]
    parquet_writer: ParquetWriter
    subprocess_runner: SubprocessRunner


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify local Bun subprocess inference produces probability vectors "
            "that match production PostgreSQL race_running_style_model_predictions "
            "rows within a configurable tolerance (default 1e-12)."
        ),
    )
    parser.add_argument("--features-parquet", required=True)
    parser.add_argument("--output-parquet", required=True)
    parser.add_argument("--model-flatbin", required=True)
    parser.add_argument("--rs-p-from-flatbin", default=None)
    parser.add_argument("--predicted-at", required=True)
    parser.add_argument("--pg-dsn", default=None)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--feature-version", required=True)
    parser.add_argument("--category", required=True, choices=list(SUPPORTED_CATEGORIES))
    parser.add_argument("--year", required=True)
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
        cursor.execute(
            build_sample_keys_sql(),
            (spec["model_version"], spec["year"], spec["limit"]),
        )
        rows = cursor.fetchall()
    finally:
        connection.close()
    return [coerce_race_key_row(row) for row in rows]


def fetch_prod_predictions(
    spec: SampleSpec,
    *,
    pg_connector: PgConnectorLike,
) -> pl.DataFrame:
    connection = pg_connector(spec["pg_dsn"])
    try:
        cursor = connection.cursor()
        cursor.execute(
            build_prod_predictions_sql(),
            (spec["model_version"], spec["year"]),
        )
        rows = cursor.fetchall()
    finally:
        connection.close()
    return pl.DataFrame(
        rows,
        schema=[
            *RACE_KEY_COLUMNS,
            "ketto_toroku_bango",
            *PROBABILITY_COLUMNS,
            PREDICTED_CLASS_COLUMN,
        ],
        orient="row",
    )


def assert_probability_sums_to_one(
    frame: pl.DataFrame, *, tolerance: float,
) -> bool:
    probabilities = frame.select(list(PROBABILITY_COLUMNS)).to_numpy().astype(np.float64)
    row_sums = probabilities.sum(axis=1)
    deviations = np.abs(row_sums - 1.0)
    return bool(np.all(deviations <= tolerance))


def _merge_local_prod(spec: CompareSpec, *, with_suffixes: bool) -> pl.DataFrame:
    on = [*RACE_KEY_COLUMNS, "ketto_toroku_bango"]
    if not with_suffixes:
        return spec["local_frame"].join(spec["prod_frame"], on=on, how="inner")
    prod = spec["prod_frame"].rename(
        {
            column: f"{column}_prod"
            for column in spec["prod_frame"].columns
            if column not in on
        }
    )
    local = spec["local_frame"].rename(
        {
            column: f"{column}_local"
            for column in spec["local_frame"].columns
            if column not in on
        }
    )
    return local.join(prod, on=on, how="inner")


def compute_max_diff_per_class(spec: CompareSpec) -> dict[str, float]:
    merged = _merge_local_prod(spec, with_suffixes=True)
    return {
        column: float(
            np.max(
                np.abs(
                    merged[f"{column}_local"].to_numpy().astype(np.float64)
                    - merged[f"{column}_prod"].to_numpy().astype(np.float64),
                ),
            ),
        )
        if len(merged) > 0
        else 0.0
        for column in PROBABILITY_COLUMNS
    }


def compute_argmax_agreement(spec: CompareSpec) -> float:
    merged = _merge_local_prod(spec, with_suffixes=True)
    if len(merged) == 0:
        return 1.0
    matches = merged[f"{PREDICTED_CLASS_COLUMN}_local"].to_numpy().astype(
        np.int64,
    ) == merged[f"{PREDICTED_CLASS_COLUMN}_prod"].to_numpy().astype(np.int64)
    return float(np.mean(matches))


def evaluate_parity(spec: CompareSpec) -> ParityResult:
    merged_count = int(len(_merge_local_prod(spec, with_suffixes=False)))
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


def resolve_pg_dsn(explicit_dsn: str | None) -> str:
    if explicit_dsn is not None and explicit_dsn != "":
        return explicit_dsn
    env_dsn = resolve_phase_two_dsn()
    if env_dsn != "":
        return env_dsn
    raise ValueError(
        "Postgres DSN is required; pass --pg-dsn or set PARITY_PG_DSN environment variable.",
    )


def collect_mismatches(spec: CompareSpec, *, max_diff_threshold: float) -> pl.DataFrame:
    merged = _merge_local_prod(spec, with_suffixes=True)
    if len(merged) == 0:
        return pl.DataFrame(
            schema=[*RACE_KEY_COLUMNS, "ketto_toroku_bango", "max_diff"],
        )
    per_row_diffs = np.max(
        np.stack(
            [
                np.abs(
                    merged[f"{column}_local"].to_numpy().astype(np.float64)
                    - merged[f"{column}_prod"].to_numpy().astype(np.float64),
                )
                for column in PROBABILITY_COLUMNS
            ],
            axis=1,
        ),
        axis=1,
    )
    annotated = merged.select([*RACE_KEY_COLUMNS, "ketto_toroku_bango"]).with_columns(
        pl.Series("max_diff", per_row_diffs)
    )
    return annotated.filter(pl.col("max_diff") > max_diff_threshold)


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
    mismatches: pl.DataFrame,
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
    mismatches.write_parquet(parquet_path)
    return json_path, parquet_path


def default_pg_connector(dsn: str) -> PgConnectionLike:
    import importlib

    psycopg_module = importlib.import_module("psycopg")
    connect_callable = getattr(psycopg_module, "connect")
    return connect_callable(dsn)


def build_w3_command(
    *,
    features_parquet: str,
    output_parquet: str,
    model_flatbin: str,
    category: str,
    model_version: str,
    feature_version: str,
    predicted_at: str,
    rs_p_from_flatbin: str | None,
) -> list[str]:
    base = [
        "bun",
        "run",
        W3_INFERENCE_SCRIPT_PATH,
        "--model-flatbin",
        model_flatbin,
        "--features-parquet",
        features_parquet,
        "--output-parquet",
        output_parquet,
        "--category",
        category,
        "--model-version",
        model_version,
        "--feature-version",
        feature_version,
        "--predicted-at",
        predicted_at,
    ]
    if rs_p_from_flatbin is None:
        return base
    return [*base, "--rs-p-from-flatbin", rs_p_from_flatbin]


def default_local_inference_runner(
    *,
    features_parquet: str,
    output_parquet: str,
    model_flatbin: str,
    category: str,
    model_version: str,
    feature_version: str,
    predicted_at: str,
    rs_p_from_flatbin: str | None,
    runner: SubprocessRunner,
) -> None:
    cmd = build_w3_command(
        features_parquet=features_parquet,
        output_parquet=output_parquet,
        model_flatbin=model_flatbin,
        category=category,
        model_version=model_version,
        feature_version=feature_version,
        predicted_at=predicted_at,
        rs_p_from_flatbin=rs_p_from_flatbin,
    )
    runner(cmd, check=True, capture_output=True, text=True)


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def filter_features_by_race_keys(
    features: pl.DataFrame, race_keys: list[tuple[str, str, str, str, str]],
) -> pl.DataFrame:
    if len(race_keys) == 0:
        return features.clear()
    keys_frame = pl.DataFrame(race_keys, schema=list(RACE_KEY_COLUMNS), orient="row")
    return features.join(keys_frame, on=list(RACE_KEY_COLUMNS), how="inner")


def default_parquet_writer(frame: pl.DataFrame, path: str) -> None:
    frame.write_parquet(path)


class PhaseTwoArgs(TypedDict):
    features_parquet: str
    output_parquet: str
    model_flatbin: str
    rs_p_from_flatbin: str | None
    predicted_at: str
    pg_dsn: str
    model_version: str
    feature_version: str
    category: str
    year: str
    sample_limit: int
    tolerance: float


def coerce_phase_two_args(args: argparse.Namespace) -> PhaseTwoArgs:
    return PhaseTwoArgs(
        features_parquet=str(args.features_parquet),
        output_parquet=str(args.output_parquet),
        model_flatbin=str(args.model_flatbin),
        rs_p_from_flatbin=(
            None if args.rs_p_from_flatbin is None else str(args.rs_p_from_flatbin)
        ),
        predicted_at=str(args.predicted_at),
        pg_dsn=resolve_pg_dsn(args.pg_dsn),
        model_version=str(args.model_version),
        feature_version=str(args.feature_version),
        category=str(args.category),
        year=str(args.year),
        sample_limit=int(args.sample_limit),
        tolerance=float(args.tolerance),
    )


def spawn_w3_for_filtered_features(
    *,
    args: PhaseTwoArgs,
    filtered_features_path: str,
    output_parquet: str,
    local_inference_runner: LocalInferenceRunnerLike,
    subprocess_runner: SubprocessRunner,
) -> None:
    local_inference_runner(
        features_parquet=filtered_features_path,
        output_parquet=output_parquet,
        model_flatbin=args["model_flatbin"],
        category=args["category"],
        model_version=args["model_version"],
        feature_version=args["feature_version"],
        predicted_at=args["predicted_at"],
        rs_p_from_flatbin=args["rs_p_from_flatbin"],
        runner=subprocess_runner,
    )


def derive_predicted_class(local_frame: pl.DataFrame) -> pl.DataFrame:
    if PREDICTED_CLASS_COLUMN in local_frame.columns:
        return local_frame
    probabilities = local_frame.select(list(PROBABILITY_COLUMNS)).to_numpy().astype(np.float64)
    return local_frame.with_columns(
        pl.Series(PREDICTED_CLASS_COLUMN, probabilities.argmax(axis=1).astype(np.int64))
    )


def run_phase_two(
    args: argparse.Namespace,
    *,
    deps: PhaseTwoDeps,
) -> ParityResult:
    coerced = coerce_phase_two_args(args)
    sample_spec: SampleSpec = {
        "pg_dsn": coerced["pg_dsn"],
        "model_version": coerced["model_version"],
        "year": coerced["year"],
        "limit": coerced["sample_limit"],
    }
    race_keys = fetch_sample_race_keys(sample_spec, pg_connector=deps["pg_connector"])
    features = deps["parquet_reader"](coerced["features_parquet"])
    filtered = filter_features_by_race_keys(features, race_keys)
    with tempfile.TemporaryDirectory() as tmpdir:
        filtered_path = str(Path(tmpdir) / "filtered_features.parquet")
        deps["parquet_writer"](filtered, filtered_path)
        spawn_w3_for_filtered_features(
            args=coerced,
            filtered_features_path=filtered_path,
            output_parquet=coerced["output_parquet"],
            local_inference_runner=deps["local_inference_runner"],
            subprocess_runner=deps["subprocess_runner"],
        )
        local_frame = derive_predicted_class(deps["parquet_reader"](coerced["output_parquet"]))
    prod_frame = fetch_prod_predictions(sample_spec, pg_connector=deps["pg_connector"])
    return evaluate_parity(
        {
            "local_frame": local_frame,
            "prod_frame": prod_frame,
            "tolerance": coerced["tolerance"],
        },
    )


class PhaseThreeDeps(TypedDict):
    pg_connector: PgConnectorLike
    local_inference_runner: LocalInferenceRunnerLike
    parquet_reader: Callable[[str], pl.DataFrame]
    parquet_writer: ParquetWriter
    subprocess_runner: SubprocessRunner
    generated_at_utc: str


def run_phase_three(
    args: argparse.Namespace,
    *,
    deps: PhaseThreeDeps,
) -> tuple[PhaseThreeReport, Path, Path | None]:
    parity = run_phase_two(
        args,
        deps={
            "pg_connector": deps["pg_connector"],
            "local_inference_runner": deps["local_inference_runner"],
            "parquet_reader": deps["parquet_reader"],
            "parquet_writer": deps["parquet_writer"],
            "subprocess_runner": deps["subprocess_runner"],
        },
    )
    coerced = coerce_phase_two_args(args)
    prod_frame = fetch_prod_predictions(
        {
            "pg_dsn": coerced["pg_dsn"],
            "model_version": coerced["model_version"],
            "year": coerced["year"],
            "limit": coerced["sample_limit"],
        },
        pg_connector=deps["pg_connector"],
    )
    local_frame = derive_predicted_class(deps["parquet_reader"](coerced["output_parquet"]))
    compare_spec: CompareSpec = {
        "local_frame": local_frame,
        "prod_frame": prod_frame,
        "tolerance": coerced["tolerance"],
    }
    mismatches = collect_mismatches(
        compare_spec, max_diff_threshold=DEFAULT_TOLERANCE_PHASE3_MAX_DIFF,
    )
    report = build_phase_three_report(
        parity=parity,
        model_version=coerced["model_version"],
        mismatches_count=int(len(mismatches)),
        generated_at_utc=deps["generated_at_utc"],
        max_diff_threshold=DEFAULT_TOLERANCE_PHASE3_MAX_DIFF,
        agreement_threshold=DEFAULT_TOLERANCE_PHASE3_AGREEMENT,
    )
    json_path, parquet_path = write_phase_three_artifacts(
        report=report, mismatches=mismatches, report_dir=str(args.report_dir),
    )
    return report, json_path, parquet_path


class RunDeps(TypedDict):
    pg_connector: PgConnectorLike
    local_inference_runner: LocalInferenceRunnerLike
    parquet_reader: Callable[[str], pl.DataFrame]
    parquet_writer: ParquetWriter
    subprocess_runner: SubprocessRunner
    clock_iso: Callable[[], str]


def run_phase_one_outcome() -> dict[str, object]:
    return {"phase": "phase1", "message": "smoke tests live in pytest, not CLI"}


def run_phase_two_outcome(
    args: argparse.Namespace, *, deps: RunDeps,
) -> dict[str, object]:
    result = run_phase_two(
        args,
        deps={
            "pg_connector": deps["pg_connector"],
            "local_inference_runner": deps["local_inference_runner"],
            "parquet_reader": deps["parquet_reader"],
            "parquet_writer": deps["parquet_writer"],
            "subprocess_runner": deps["subprocess_runner"],
        },
    )
    return {"phase": "phase2", "result": result}


def run_phase_three_outcome(
    args: argparse.Namespace, *, deps: RunDeps,
) -> dict[str, object]:
    report, json_path, parquet_path = run_phase_three(
        args,
        deps={
            "pg_connector": deps["pg_connector"],
            "local_inference_runner": deps["local_inference_runner"],
            "parquet_reader": deps["parquet_reader"],
            "parquet_writer": deps["parquet_writer"],
            "subprocess_runner": deps["subprocess_runner"],
            "generated_at_utc": deps["clock_iso"](),
        },
    )
    return {
        "phase": "phase3",
        "report": report,
        "report_path": str(json_path),
        "mismatches_path": str(parquet_path) if parquet_path is not None else None,
    }


def run(args: argparse.Namespace, *, deps: RunDeps) -> dict[str, object]:
    if args.phase == "phase1":
        return run_phase_one_outcome()
    if args.phase == "phase2":
        return run_phase_two_outcome(args, deps=deps)
    return run_phase_three_outcome(args, deps=deps)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    outcome = run(
        args,
        deps={
            "pg_connector": default_pg_connector,
            "local_inference_runner": default_local_inference_runner,
            "parquet_reader": pl.read_parquet,
            "parquet_writer": default_parquet_writer,
            "subprocess_runner": subprocess.run,
            "clock_iso": utc_now_iso,
        },
    )
    print(json.dumps(outcome, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
