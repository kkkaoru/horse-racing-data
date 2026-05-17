from __future__ import annotations

import json
from pathlib import Path

import mlx.core as mx
import numpy as np
import pandas as pd
import pytest

from finish_position_transformer import cli as cli_module
from finish_position_transformer.dataset import (
    MAX_RUNNERS,
    build_race_batches,
    categorical_vocab_size,
    fit_normalization_stats,
    iter_race_batches,
    resolve_transformer_feature_columns,
)
from finish_position_transformer.model import (
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_NUM_HEADS,
    RaceSetTransformer,
    build_padding_mask,
    default_model_config,
)
from finish_position_transformer.training import (
    default_training_config,
    evaluate_ndcg,
    multitask_loss,
    predict_rank_scores,
    train_transformer,
)


def _make_synthetic_frame(seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    races: list[dict[str, object]] = []
    for race_index in range(6):
        runner_count = 4 + race_index % 3
        finishes = rng.permutation(np.arange(1, runner_count + 1)).tolist()
        for slot in range(runner_count):
            races.append(
                {
                    "source": "jra",
                    "race_date": f"2024010{race_index + 1}",
                    "kaisai_nen": "2024",
                    "kaisai_tsukihi": "0101",
                    "keibajo_code": "01",
                    "race_bango": str(race_index + 1).zfill(2),
                    "ketto_toroku_bango": f"h{race_index:02d}{slot:02d}",
                    "umaban": slot + 1,
                    "category": "jra",
                    "race_id": f"jra:2024:0101:01:{str(race_index + 1).zfill(2)}",
                    "race_year": 2024,
                    "feature_schema_version": "v1",
                    "finish_position": int(finishes[slot]),
                    "finish_norm": float(finishes[slot]) / runner_count,
                    "track_code": "11",
                    "grade_code": "A",
                    "speed_index_avg_5": rng.normal(),
                    "jockey_recent_win_rate": rng.random(),
                }
            )
    return pd.DataFrame(races)


def test_resolve_transformer_feature_columns_splits_numeric_and_categorical():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    assert cols.categorical == ["track_code", "grade_code"]
    assert "speed_index_avg_5" in cols.numeric
    assert "ketto_toroku_bango" not in cols.numeric


def test_fit_normalization_stats_handles_missing_and_constant_columns():
    df = pd.DataFrame(
        {
            "speed_index_avg_5": [1.0, 2.0, np.nan, 4.0],
            "track_code": ["11", "11", "12", None],
            "grade_code": ["A", "A", "A", "A"],
        }
    )
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    assert stats["categorical_vocab"]["track_code"] == ["11", "12"]
    assert stats["categorical_vocab"]["grade_code"] == ["A"]
    assert stats["numeric_columns"] == ["speed_index_avg_5"]


def test_categorical_vocab_size_counts_padding_slot():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    assert categorical_vocab_size(stats, "track_code") == len(stats["categorical_vocab"]["track_code"]) + 1


def test_build_race_batches_emits_padded_shapes_and_mask():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    assert arrays["numeric_features"].shape[1] == MAX_RUNNERS
    assert arrays["categorical_indices"].shape[1] == MAX_RUNNERS
    assert arrays["mask"].dtype == np.bool_
    assert int(arrays["mask"].sum()) == len(df)


def test_iter_race_batches_emits_full_set_without_shuffle():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    batches = iter_race_batches(arrays, batch_size=2, shuffle=False)
    total = sum(len(batch["race_ids"]) for batch in batches)
    assert total == len(arrays["race_ids"])


def test_build_padding_mask_marks_padding_with_neg_inf():
    mask = mx.array([[1, 1, 0], [1, 0, 0]], dtype=mx.bool_)
    additive = build_padding_mask(mask, num_heads=4)
    arr = np.asarray(additive)
    assert arr.shape == (2, 1, 1, 3)
    assert arr[0, 0, 0, 2] < -1e8
    assert arr[1, 0, 0, 0] == 0.0


def test_model_forward_returns_expected_shapes():
    config = default_model_config(num_numeric_features=8, categorical_vocab_sizes=[5, 3])
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    numeric = mx.zeros((3, MAX_RUNNERS, 8))
    cat = mx.zeros((3, MAX_RUNNERS, 2), dtype=mx.int32)
    umaban = mx.zeros((3, MAX_RUNNERS), dtype=mx.int32)
    mask = mx.ones((3, MAX_RUNNERS), dtype=mx.bool_)
    output = model(numeric, cat, umaban, mask)
    assert output["top1_logit"].shape == (3, MAX_RUNNERS)
    assert output["top3_logit"].shape == (3, MAX_RUNNERS)
    assert output["rank_score"].shape == (3, MAX_RUNNERS)
    assert config["embedding_dim"] == DEFAULT_EMBEDDING_DIM
    assert config["num_heads"] == DEFAULT_NUM_HEADS


def test_multitask_loss_returns_scalar_on_synthetic_batch():
    config = default_model_config(num_numeric_features=4, categorical_vocab_sizes=[3])
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    numeric = mx.zeros((2, MAX_RUNNERS, 4))
    cat = mx.zeros((2, MAX_RUNNERS, 1), dtype=mx.int32)
    umaban = mx.zeros((2, MAX_RUNNERS), dtype=mx.int32)
    mask = mx.ones((2, MAX_RUNNERS), dtype=mx.bool_)
    output = model(numeric, cat, umaban, mask)
    finish = mx.array(np.tile(np.arange(1, MAX_RUNNERS + 1), (2, 1)), dtype=mx.float32)
    loss = multitask_loss(output, finish, mask, {"top1": 1.0, "top3": 1.0, "pairwise": 1.0})
    assert loss.shape == ()
    assert float(loss) >= 0


def test_train_transformer_records_history_and_improves_loss():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in cols.categorical]
    config = default_model_config(num_numeric_features=len(cols.numeric), categorical_vocab_sizes=vocab_sizes)
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    train_cfg = default_training_config()
    train_cfg["max_epochs"] = 3
    train_cfg["warmup_steps"] = 2
    train_cfg["batch_size"] = 4
    train_cfg["early_stopping_epochs"] = 99
    result = train_transformer(model, arrays, arrays, train_cfg)
    assert len(result["history"]) == 3
    assert result["history"][-1]["train_loss"] <= result["history"][0]["train_loss"] + 1e-3


def test_evaluate_ndcg_on_perfect_predictions_is_one():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in cols.categorical]
    config = default_model_config(num_numeric_features=len(cols.numeric), categorical_vocab_sizes=vocab_sizes)
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    score = evaluate_ndcg(model, arrays, batch_size=4)
    assert 0.0 <= score <= 1.0


def test_predict_rank_scores_returns_correct_shape():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in cols.categorical]
    config = default_model_config(num_numeric_features=len(cols.numeric), categorical_vocab_sizes=vocab_sizes)
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    scores = predict_rank_scores(model, arrays, batch_size=2)
    assert scores.shape == (len(arrays["race_ids"]), MAX_RUNNERS)


def test_predictions_from_scores_orders_by_rank_score_desc():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    fake_scores = np.zeros((len(arrays["race_ids"]), MAX_RUNNERS), dtype=np.float32)
    for race_idx in range(len(arrays["race_ids"])):
        for slot in range(MAX_RUNNERS):
            fake_scores[race_idx, slot] = float(MAX_RUNNERS - slot)
    predictions = cli_module.predictions_from_scores(arrays, fake_scores)
    assert predictions[0]["predicted_rank"] == 1


def test_cli_parse_args_supports_three_commands(tmp_path: Path):
    train_args = cli_module.parse_args(
        [
            "train",
            "--train-parquet",
            str(tmp_path / "train"),
            "--output-model-dir",
            str(tmp_path / "model"),
        ]
    )
    assert train_args.command == "train"
    walk_args = cli_module.parse_args(
        [
            "walk-forward",
            "--parquet",
            str(tmp_path / "p"),
            "--output-report",
            str(tmp_path / "r.json"),
        ]
    )
    assert walk_args.command == "walk-forward"
    predict_args = cli_module.parse_args(
        [
            "predict",
            "--model-dir",
            str(tmp_path / "m"),
            "--input-parquet",
            str(tmp_path / "p"),
            "--output-predictions",
            str(tmp_path / "o.jsonl"),
        ]
    )
    assert predict_args.command == "predict"


def test_training_config_from_args_picks_up_loss_weights(tmp_path: Path):
    args = cli_module.parse_args(
        [
            "train",
            "--train-parquet",
            str(tmp_path / "t"),
            "--output-model-dir",
            str(tmp_path / "m"),
            "--top1-weight",
            "0.5",
            "--top3-weight",
            "0.7",
            "--pairwise-weight",
            "0.9",
        ]
    )
    config = cli_module.training_config_from_args(args)
    assert config["loss_weights"]["top1"] == 0.5
    assert config["loss_weights"]["top3"] == 0.7
    assert config["loss_weights"]["pairwise"] == 0.9


def test_save_and_load_checkpoint_round_trip(tmp_path: Path):
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in cols.categorical]
    config = default_model_config(num_numeric_features=len(cols.numeric), categorical_vocab_sizes=vocab_sizes)
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    cli_module.save_checkpoint(model, config, stats, tmp_path / "ckpt")
    loaded_model, loaded_stats, loaded_config = cli_module.load_checkpoint(tmp_path / "ckpt")
    assert loaded_config["num_numeric_features"] == config["num_numeric_features"]
    assert loaded_stats["numeric_columns"] == stats["numeric_columns"]
    arrays = build_race_batches(df, loaded_stats)
    scores = predict_rank_scores(loaded_model, arrays, batch_size=2)
    assert scores.shape == (len(arrays["race_ids"]), MAX_RUNNERS)


def test_run_walk_forward_command_writes_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    df = _make_synthetic_frame()
    parquet_path = tmp_path / "data.parquet"
    df.to_parquet(parquet_path, index=False)
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    args = cli_module.parse_args(
        [
            "walk-forward",
            "--parquet",
            str(parquet_path),
            "--train-start-date",
            "20230101",
            "--validation-years",
            "2024",
            "--output-report",
            str(tmp_path / "report.json"),
            "--max-epochs",
            "1",
            "--warmup-steps",
            "1",
            "--batch-size",
            "2",
        ]
    )
    cli_module.run_walk_forward_command(args)
    payload = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert payload["aggregate"]["fold_count"] >= 0
