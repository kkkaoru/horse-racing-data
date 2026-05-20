#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Per-horse feature batch builder for v1.5 entry-time Worker inference.

Reads the partitioned parquet built by `finish_position_features_duckdb.py`,
extracts rows in a target race-date range (e.g. today + tomorrow for NAR),
and writes one JSONL line per (race, horse) shaped for the Worker's
`RaceHorseFeatureRow` consumer in apps/sync-realtime-data.

The Worker can compute race-internal peer features from peerInputs, but
model features such as field_strength_avg_speed are already stable
per-race context and must be shipped with the per-horse feature map.

Run with:
  cd apps/pc-keiba-viewer && .venv/bin/python src/scripts/build_running_style_feature_batch.py \\
    --parquet ../../tmp/feat-v15/nar \\
    --from-date 20260518 --to-date 20260519 \\
    --source nar \\
    --output ../../tmp/running-style-features-nar-20260518.jsonl
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

PEER_INPUT_COLUMNS: dict[str, str] = {
    "past_nige_rate_self": "pastNigeRate",
    "past_senkou_rate_self": "pastSenkouRate",
    "past_sashi_rate_self": "pastSashiRate",
    "past_oikomi_rate_self": "pastOikomiRate",
    "past_corner_1_norm_avg_5": "pastCorner1NormAvg5",
    "speed_index_avg_5": "speedIndexAvg5",
    "speed_index_best_5": "speedIndexBest5",
    "past_first_3f_avg_5": "pastFirst3fAvg5",
    "kohan_3f_avg_5": "kohan3fAvg5",
    "career_win_rate": "careerWinRate",
}
EXCLUDED_FROM_PER_HORSE: tuple[str, ...] = (
    "source", "race_date", "kaisai_nen", "kaisai_tsukihi", "keibajo_code",
    "race_bango", "ketto_toroku_bango", "umaban", "category", "race_id",
    "race_year", "feature_schema_version", "finish_position", "finish_norm",
    "target_corner_1_norm", "target_corner_3_norm", "target_corner_4_norm",
    "target_running_style_class", "bamei",
)
SELF_VS_FIELD_PREFIX = "self_"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="build_running_style_feature_batch")
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--from-date", type=str, required=True)
    parser.add_argument("--to-date", type=str, required=True)
    parser.add_argument("--source", type=str, required=True, choices=("jra", "nar"))
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def load_parquet_rows(parquet_dir: Path) -> pd.DataFrame:
    return pd.read_parquet(parquet_dir)


def filter_target_window(df: pd.DataFrame, source: str, from_date: str, to_date: str) -> pd.DataFrame:
    matches_source = df["source"] == source
    matches_date = (df["race_date"] >= from_date) & (df["race_date"] <= to_date)
    return df[matches_source & matches_date].copy()


def build_race_key(row: pd.Series) -> str:
    return f"{row['source']}:{row['race_date']}:{row['keibajo_code']}:{row['race_bango']}"


def sanitize_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return value
    if isinstance(value, (int, bool)):
        return float(value)
    return None


def extract_peer_inputs(row: pd.Series) -> dict[str, float | None]:
    return {target_key: sanitize_float(row.get(source_key)) for source_key, target_key in PEER_INPUT_COLUMNS.items()}


def is_per_horse_feature_column(column: str) -> bool:
    if column in EXCLUDED_FROM_PER_HORSE:
        return False
    if column.startswith(SELF_VS_FIELD_PREFIX):
        return False
    return True


def select_per_horse_columns(columns: list[str]) -> list[str]:
    return [col for col in columns if is_per_horse_feature_column(col)]


def extract_per_horse_features(row: pd.Series, per_horse_columns: list[str]) -> dict[str, float | None]:
    return {column: sanitize_float(row.get(column)) for column in per_horse_columns}


def derive_category(row: pd.Series) -> str:
    return str(row.get("category", row["source"]))


def build_feature_row(row: pd.Series, per_horse_columns: list[str]) -> dict[str, object]:
    return {
        "bamei": (None if pd.isna(row.get("bamei")) else str(row.get("bamei", "")).strip() or None),
        "category": derive_category(row),
        "kaisaiNen": str(row["kaisai_nen"]),
        "kaisaiTsukihi": str(row["kaisai_tsukihi"]),
        "keibajoCode": str(row["keibajo_code"]),
        "kettoTorokuBango": str(row["ketto_toroku_bango"]),
        "perHorseFeatures": extract_per_horse_features(row, per_horse_columns),
        "peerInputs": extract_peer_inputs(row),
        "raceBango": str(row["race_bango"]),
        "raceKey": build_race_key(row),
        "source": str(row["source"]),
        "umaban": int(row["umaban"]) if not pd.isna(row.get("umaban")) else 0,
    }


def write_jsonl(rows: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def run_build(args: argparse.Namespace) -> dict[str, object]:
    df = load_parquet_rows(args.parquet)
    filtered = filter_target_window(df, args.source, args.from_date, args.to_date)
    per_horse_columns = select_per_horse_columns(list(filtered.columns))
    rows = [build_feature_row(row, per_horse_columns) for _, row in filtered.iterrows()]
    write_jsonl(rows, args.output)
    return {
        "source": args.source,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "row_count": len(rows),
        "per_horse_feature_count": len(per_horse_columns),
        "output": str(args.output),
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = run_build(args)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
