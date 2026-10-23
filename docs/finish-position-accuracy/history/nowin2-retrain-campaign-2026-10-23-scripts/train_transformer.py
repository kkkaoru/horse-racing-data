"""Retrain the NAR Set Transformer companion under the win-odds ban.

Trains with the repo's MLX trainer (``finish_position_transformer`` train CLI) on
the served feature_order minus banned market columns plus the 馬連/3連複
deviation columns, three seeds, then exports the production numpy contract
(``weights_s{1,2,3}.npz`` + ``norm.json``) read by ``transformer_scorer``.

The served scorer has no race-level categorical embeddings (``race_meta: false``),
so training drops ``keibajo_code`` / ``kaisai_tsukihi``: the trainer's race-meta
embeddings then always look up the padding row, a constant vector that is folded
exactly into ``numeric_projection.bias`` before export.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[4]
CAMPAIGN = REPO / ".cache/tansho-ban-retrain"  # run from the git-ignored campaign folder
VIEWER = REPO / "apps/pc-keiba-viewer"
PROD = REPO / "apps/finish-position-predict-container/models/finish-position/nar/iter40-nar-settransformer-blend-v1"
OLD_VERSION = "iter40-nar-settransformer-blend-v1"
NEW_VERSION = OLD_VERSION + __import__("os").environ.get("TRANSFORMER_SUFFIX", "-nowin1")
OUT = CAMPAIGN / "models/finish-position/nar" / NEW_VERSION
WORK = CAMPAIGN / ("transformer-work" + __import__("os").environ.get("TRANSFORMER_SUFFIX", ""))
SEEDS = (20260517, 20260518, 20260519)
sys.path.insert(0, str(VIEWER / "src/scripts"))
from learning.feature_selection_policy import MARKET_DEVIATION_FEATURES, is_banned_market_feature

KEEP = ("race_id", "umaban", "finish_position", "ketto_toroku_bango")


def store(years: list[int], features: list[str]) -> pd.DataFrame:
    frames = []
    for y in years:
        f = pd.read_parquet((CAMPAIGN / f"features/nar-{y}/FINAL").read_text().strip())
        f = f[f["finish_position"].notna() & (pd.to_numeric(f["finish_position"], errors="coerce") > 0)]
        # Ban-ei venues 81/82/84 (until 2006) leaked into the NAR store with hex body weights.
        f = f[~f["race_id"].astype(str).str.split(":").str[3].isin(["81", "82", "83", "84"])]
        for c in features:
            if c not in f.columns:
                f[c] = np.nan
        frames.append(f[list(KEEP) + features])
    df = pd.concat(frames, ignore_index=True)
    df[features] = df[features].apply(pd.to_numeric, errors="coerce").astype("float64")
    return df


def export(model_dir: Path, seed_index: int) -> tuple[list[str], list[float], list[float], dict]:
    import mlx.core as mx

    stats = json.loads((model_dir / "stats.json").read_text())
    config = json.loads((model_dir / "config.json").read_text())
    weights = {k: np.asarray(v, dtype=np.float32) for k, v in mx.load(str(model_dir / "model.safetensors")).items()}
    bias_shift = np.zeros_like(weights["numeric_projection.bias"])
    for key in [k for k in weights if k.startswith("race_categorical_embeddings.")]:
        if key.endswith(".weight"):
            bias_shift += weights[key][0]  # padding/unknown row: the only row ever looked up
        del weights[key]
    if any(k.startswith("categorical_embeddings.") for k in weights):
        raise SystemExit("per-horse categorical embeddings are not part of the served contract")
    weights["numeric_projection.bias"] = weights["numeric_projection.bias"] + bias_shift
    np.savez(OUT / f"weights_s{seed_index}.npz", **weights)
    return stats["numeric_columns"], stats["numeric_mean"], stats["numeric_std"], config


def main() -> None:
    norm_old = json.loads((PROD / "norm.json").read_text())
    features = [c for c in norm_old["feature_order"] if not is_banned_market_feature(c)]
    features += [c for c in sorted(MARKET_DEVIATION_FEATURES) if c not in features]
    arch = norm_old["arch"]
    WORK.mkdir(parents=True, exist_ok=True)
    train = WORK / "train.parquet"
    valid = WORK / "valid.parquet"
    store(list(range(2006, 2025)), features).to_parquet(train, index=False)
    store([2025], features).to_parquet(valid, index=False)
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    exported = None
    for i, seed in enumerate(SEEDS, start=1):
        model_dir = WORK / f"seed{i}"
        subprocess.run(
            ["uv", "run", "python", "-m", "finish_position_transformer", "train",
             "--train-parquet", str(train), "--valid-parquet", str(valid), "--output-model-dir", str(model_dir),
             "--output-metadata", str(WORK / f"seed{i}-result.json"),
             "--embedding-dim", str(arch["dim"]), "--num-layers", str(arch["num_layers"]),
             "--num-heads", str(arch["num_heads"]), "--seed", str(seed)],
            cwd=VIEWER, env={**__import__("os").environ, "PYTHONPATH": str(VIEWER / "src/scripts"), "MLFLOW_DISABLED": "1"}, check=True,
        )
        cols, mean, std, config = export(model_dir, i)
        if exported is not None and exported[0] != cols:
            raise SystemExit("seed feature order differs")
        exported = (cols, mean, std, config)
    cols, mean, std, config = exported
    if cols != features:
        raise SystemExit("trainer resolved a different feature order than requested")
    norm = {
        "feature_order": cols, "mean": mean, "std": std,
        "arch": {**arch, "dim": config["embedding_dim"], "num_layers": config["num_layers"], "num_heads": config["num_heads"]},
        "seed_files": [f"weights_s{i}.npz" for i in range(1, len(SEEDS) + 1)],
        "model_version": NEW_VERSION, "based_on": OLD_VERSION, "win_odds_banned": True,
        "removed_market_features": [c for c in norm_old["feature_order"] if is_banned_market_feature(c)],
    }
    (OUT / "norm.json").write_text(json.dumps(norm, ensure_ascii=False, indent=1))
    print("exported", OUT)


if __name__ == "__main__":
    main()
