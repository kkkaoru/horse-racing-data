from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

try:
    import mlx.core as mx
except (ImportError, OSError):
    pytest.skip("MLX requires Apple Silicon/macOS", allow_module_level=True)

import numpy as np
import polars as pl

from finish_position_transformer import cli as cli_module
from finish_position_transformer.dataset import build_race_batches
from finish_position_transformer.serve_export import serving_weights, write_serving_artifact
from finish_position_transformer.training import predict_rank_scores

PREDICT_CONTAINER_SRC = Path(__file__).resolve().parents[2] / "finish-position-predict-container" / "src"


def _serve_frame() -> pl.DataFrame:
    """Numeric-only frame without race-meta columns: the shape the served NAR model trains on."""
    rng = np.random.default_rng(11)
    rows: list[dict[str, object]] = []
    for race_index in range(4):
        runner_count = 5 + race_index
        finishes = rng.permutation(np.arange(1, runner_count + 1)).tolist()
        for slot in range(runner_count):
            rows.append(
                {
                    "race_id": f"nar:2025:0101:30:{race_index + 1:02d}",
                    "ketto_toroku_bango": f"h{race_index}{slot:02d}",
                    "umaban": slot + 1,
                    "finish_position": float(finishes[slot]),
                    "speed_index_avg_5": float(rng.normal()),
                    "jockey_recent_win_rate": float(rng.random()),
                    "days_since_last": float(rng.integers(7, 90)),
                }
            )
    return pl.DataFrame(rows)


def _categorical_frame() -> pl.DataFrame:
    return _serve_frame().with_columns(pl.lit("A").alias("grade_code"))


def test_serving_weights_folds_race_meta_padding_rows_into_projection_bias():
    df = _serve_frame().with_columns(pl.lit("30").alias("keibajo_code"))
    model, _config, _stats = cli_module._build_model_for_dataframe(df, seed=3)
    expected_bias = (
        np.array(model.numeric_projection.bias)
        + np.array(model.race_categorical_embeddings[0].weight)[0]
        + np.array(model.race_categorical_embeddings[1].weight)[0]
    )
    weights = serving_weights(model)
    assert np.allclose(weights["numeric_projection.bias"], expected_bias, rtol=0, atol=1e-6) is True
    assert sorted(name for name in weights if name.startswith("race_categorical_embeddings.")) == []


def test_serving_weights_rejects_per_horse_categorical_embeddings():
    model, _config, _stats = cli_module._build_model_for_dataframe(_categorical_frame(), seed=3)
    with pytest.raises(ValueError, match="per-horse categorical embeddings"):
        _ = serving_weights(model)


def test_write_serving_artifact_writes_seed_files_and_norm_manifest(tmp_path: Path):
    df = _serve_frame()
    model_a, config, stats = cli_module._build_model_for_dataframe(df, seed=1)
    model_b, _config_b, _stats_b = cli_module._build_model_for_dataframe(df, seed=2)
    norm = write_serving_artifact(tmp_path / "artifact", [model_a, model_b], stats, config)
    written = json.loads((tmp_path / "artifact" / "norm.json").read_text(encoding="utf-8"))
    assert written == norm
    assert written["feature_order"] == ["speed_index_avg_5", "jockey_recent_win_rate", "days_since_last"]
    assert written["seed_files"] == ["weights_s1.npz", "weights_s2.npz"]
    assert written["arch"] == {
        "dim": 64,
        "num_layers": 2,
        "num_heads": 4,
        "eps": 1e-5,
        "umaban_vocab_size": 19,
        "max_runners": 18,
        "activation": "relu",
        "race_meta": False,
    }
    assert sorted(path.name for path in (tmp_path / "artifact").iterdir()) == [
        "norm.json",
        "weights_s1.npz",
        "weights_s2.npz",
    ]


def test_exported_artifact_scores_match_mlx_forward_through_the_deployed_numpy_scorer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Train/serve parity: the numpy serving forward must reproduce the MLX trainer's rank_score.

    Measured max diff is ~1.6e-7 with MLX_ENABLE_TF32=0 vs ~1.0e-3 with TF32 on, so the
    1e-5 bound also guards the package's TF32-off default.
    """
    monkeypatch.syspath_prepend(str(PREDICT_CONTAINER_SRC))
    scorer_module = importlib.import_module("predict_lib.transformer_scorer")
    df = _serve_frame()
    model, config, stats = cli_module._build_model_for_dataframe(df, seed=5)
    mx.eval(model.parameters())
    _ = write_serving_artifact(tmp_path / "artifact", [model], stats, config)
    arrays = build_race_batches(df, stats)
    mlx_scores = predict_rank_scores(model, arrays, batch_size=2)
    scorer = scorer_module.load_transformer(tmp_path / "artifact")
    race_entries = df.sort(["race_id", "umaban"]).partition_by("race_id", maintain_order=True)
    served = [np.asarray(scorer.seed_scores(race.to_dicts())[0]) for race in race_entries]
    trained = [mlx_scores[index, : len(race_scores)] for index, race_scores in enumerate(served)]
    max_abs_diff = max(float(np.max(np.abs(a - b))) for a, b in zip(served, trained, strict=True))
    assert arrays["race_ids"] == [str(race["race_id"][0]) for race in race_entries]
    assert max_abs_diff < 1e-5
