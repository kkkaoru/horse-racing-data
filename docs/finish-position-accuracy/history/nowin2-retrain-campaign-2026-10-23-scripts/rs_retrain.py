"""Retrain the production running-style LightGBM (prod-v3 feature set) without win odds.

Usage (viewer env):
  uv run --project ../../apps/pc-keiba-viewer python rs_retrain.py --category jra --first-year 2013

Steps (per category):
  1. Phase-A base store from the campaign feature build (``features/<cat>-<y>/base``)
     plus ``field_nige_pressure_rank`` (serving SQL: rank desc nulls last).
  2. ``running_style_lightgbm train-production`` restricted to the prod-v3 header
     features minus ``popularity_score`` / ``odds_score``; fit through 2024,
     early stopping on 2025.
  3. Compact JSON export -> flat binary (the Worker format).
  4. Raw probabilities through the serving flatbin evaluator (bun) for 2025
     (calibration fit) and 2026 (blind comparison with prod-v3), with the
     serve-null columns nulled as they are at genuine serve time.
  5. OvR isotonic calibrators fit on 2025; accuracy report for 2026.
"""
from __future__ import annotations

import argparse
import glob
import json
import struct
import subprocess
import sys
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.isotonic import IsotonicRegression

REPO = Path(__file__).resolve().parents[4]
CAMPAIGN = REPO / ".cache/tansho-ban-retrain"  # run from the git-ignored campaign folder
VIEWER = REPO / "apps/pc-keiba-viewer"
SYNC = REPO / "apps/sync-realtime-data"
PROD_DIR = Path(__import__("os").environ.get("RS_PROD_DIR", str(Path(__file__).resolve().parent / "rs-prod-nowin1")))
sys.path.insert(0, str(VIEWER / "src/scripts"))
import export_lightgbm_to_json as exporter
import running_style_lightgbm as rs
from learning.feature_selection_policy import is_banned_market_feature

RACE_KEYS = ["source", "kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango"]
SERVE_NULL = ("popularity_score", "odds_score", "track_condition_normalized")
CLASSES = ("nige", "senkou", "sashi", "oikomi")


def prod_header(category: str) -> dict:
    raw = (PROD_DIR / f"{category}-latest.flatbin").read_bytes()
    length = struct.unpack("<I", raw[8:12])[0]
    return json.loads(raw[12 : 12 + length])


def load_base(category: str, years: list[int], keep: list[str]) -> pl.DataFrame:
    frames = []
    for year in years:
        files = sorted(glob.glob(str(CAMPAIGN / f"features/{category}-{year}/base/**/*.parquet"), recursive=True))
        if not files:
            raise SystemExit(f"base store missing for {category}-{year}")
        frame = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
        frames.append(frame.select([c for c in keep if c in frame.columns]))
    return pl.concat(frames, how="diagonal_relaxed")


def add_nige_pressure_rank(df: pl.DataFrame) -> pl.DataFrame:
    """Serving SQL: rank() over race order by past_nige_rate_self_recent_5 desc nulls last."""
    value = pl.col("past_nige_rate_self_recent_5")
    ranked = value.rank(method="min", descending=True).over(RACE_KEYS)
    non_null = value.count().over(RACE_KEYS)
    return df.with_columns(
        pl.when(value.is_null()).then(non_null + 1).otherwise(ranked).cast(pl.Float64).alias("field_nige_pressure_rank")
    )


def flatbin_probs(flatbin: Path, frame: pl.DataFrame, work: Path, tag: str, version: str, category: str) -> pl.DataFrame:
    features = work / f"{tag}-features.parquet"
    output = work / f"{tag}-out.parquet"
    frame.with_columns([pl.lit(None, dtype=pl.Float64).alias(c) for c in SERVE_NULL if c in frame.columns]).write_parquet(features)
    subprocess.run(
        ["bun", "run", "src/scripts/run-running-style-inference-local.ts", "--model-flatbin", str(flatbin),
         "--features-parquet", str(features), "--output-parquet", str(output), "--category", category,
         "--predicted-at", "2026-10-16T00:00:00Z", "--model-version", version, "--feature-version", "v1"],
        cwd=SYNC, check=True,
    )
    return pl.read_parquet(output)


def probs_matrix(out: pl.DataFrame) -> np.ndarray:
    return out.select([f"p_{c}" for c in CLASSES]).to_numpy().astype(np.float64)


def fit_calibrators(probs: np.ndarray, labels: np.ndarray, category: str, version: str) -> dict:
    tables = {}
    for idx, name in enumerate(CLASSES):
        iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(probs[:, idx], (labels == idx).astype(float))
        tables[name] = {"x": [float(v) for v in iso.X_thresholds_], "y": [float(v) for v in iso.y_thresholds_]}
    return {"category": category, "fit_year": 2025, "classes": list(CLASSES), "model_target": version,
            "fit_note": "win-odds-ban retrain 2026-10-16: OvR isotonic on 2025 (held out of the fit, early-stopping set) "
                        "raw flatbin probabilities with the serve-null columns nulled.",
            "calibrators": tables}


def calibrated_accuracy(probs: np.ndarray, labels: np.ndarray, calibrators: dict) -> float:
    from running_style_calibration import apply_calibration
    return float((apply_calibration(probs, calibrators).argmax(axis=1) == labels).mean())


def labelled(out: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    mask = out["target_running_style_class"].is_not_null().to_numpy()
    return probs_matrix(out)[mask], out["target_running_style_class"].to_numpy()[mask].astype(int)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", required=True, choices=("jra", "nar"))
    ap.add_argument("--first-year", type=int, required=True)
    a = ap.parse_args()
    header = prod_header(a.category)
    old_version = header["model_version"]
    version = old_version.removesuffix("-nowin1") + __import__("os").environ.get("RS_SUFFIX", "-nowin1")
    work = CAMPAIGN / "rs-work" / a.category
    out_dir = CAMPAIGN / "rs-models" / version
    work.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    wanted = [f for f in header["feature_names"] if not is_banned_market_feature(f)]
    field = set(rs.extend_feature_columns([], True))
    keep = list(dict.fromkeys(RACE_KEYS + ["race_date", "race_id", "umaban", "ketto_toroku_bango", "target_running_style_class",
                                           "past_nige_rate_self_recent_5", "finish_position", *rs.LABEL_COLUMNS,
                                           *header["feature_names"]]))
    df = add_nige_pressure_rank(load_base(a.category, list(range(a.first_year, 2027)), list(dict.fromkeys(keep + ["kyori"]))))
    # Same label as finish_position_features_duckdb.build_target_table: first corner run.
    first = pl.coalesce([f"target_corner_{i}_norm" for i in (1, 2, 3, 4)])
    df = df.with_columns(
        pl.when(first.is_null()).then(None).when(first == 0).then(0).when(first <= 0.30).then(1)
        .when(first <= 0.70).then(2).otherwise(3).cast(pl.Int64).alias("target_running_style_class")
    )
    base_feats = [f for f in wanted if f not in field]
    rs.resolve_feature_columns = lambda cols: [f for f in base_feats if f in cols]
    train_pq = work / "train.parquet"
    df.filter(pl.col("race_date").cast(pl.Utf8) <= "20251231").write_parquet(train_pq)
    rs.main(["train-production", "--csv", str(train_pq), "--train-start-date", f"{a.first_year}0101",
             "--train-end-date", "20251231", "--valid-start-date", "20250101",
             "--model-version", version, "--output-model-dir", str(out_dir)])
    meta = json.loads((out_dir / "metadata.json").read_text())
    trained = meta.get("feature_columns") or meta.get("feature_names")
    if sorted(trained) != sorted(wanted):
        raise SystemExit(f"feature mismatch: missing={sorted(set(wanted) - set(trained))} extra={sorted(set(trained) - set(wanted))}")
    exporter.run_export(out_dir, out_dir / "model.compact.json")
    subprocess.run(["bun", "run", "scripts/convert-running-style-model-to-binary.ts", "--input", str(out_dir / "model.compact.json"),
                    "--output", str(out_dir / "model.flatbin")], cwd=SYNC, check=True)
    year = pl.col("race_date").cast(pl.Utf8).str.slice(0, 4)
    calib_out = flatbin_probs(out_dir / "model.flatbin", df.filter(year == "2025"), work, "new-2025", version, a.category)
    probs, labels = labelled(calib_out)
    calibrators = fit_calibrators(probs, labels, a.category, version)
    (out_dir / "calibrators.json").write_text(json.dumps(calibrators, indent=2))
    report = {"category": a.category, "new_version": version, "old_version": old_version}
    test = df.filter(year == "2026")
    for tag, flatbin, cal_path in (("old", PROD_DIR / f"{a.category}-latest.flatbin", PROD_DIR / f"{a.category}-calibrators.json"),
                                   ("new", out_dir / "model.flatbin", out_dir / "calibrators.json")):
        out = flatbin_probs(flatbin, test, work, f"{tag}-2026", version, a.category)
        keys = ["kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango"]
        out = out.join(test.select(keys + ["kyori"]).unique(keys).with_columns(pl.col(k).cast(pl.Utf8) for k in keys),
                       on=keys, how="left")
        cal = json.loads(cal_path.read_text())
        report[tag] = {}
        for band, frame in (("all", out), ("sprint", out.filter(pl.col("kyori").cast(pl.Int64) <= 1200)),
                            ("non_sprint", out.filter(pl.col("kyori").cast(pl.Int64) > 1200))):
            p, y = labelled(frame)
            report[tag][band] = {"rows": int(len(y)), "raw_acc": float((p.argmax(axis=1) == y).mean()),
                                 "calibrated_acc": calibrated_accuracy(p, y, cal)}
    (out_dir / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
