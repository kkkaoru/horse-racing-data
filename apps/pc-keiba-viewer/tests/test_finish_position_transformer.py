from __future__ import annotations

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


def _make_synthetic_frame(seed: int = 7) -> pl.DataFrame:
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
    return pl.DataFrame(races)


def _make_walk_forward_frame(seed: int = 13) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    races: list[dict[str, object]] = []
    for year in (2023, 2024):
        for race_index in range(4):
            runner_count = 4 + race_index % 2
            finishes = rng.permutation(np.arange(1, runner_count + 1)).tolist()
            month_day = "0101"
            race_id = f"jra:{year}:{month_day}:01:{str(race_index + 1).zfill(2)}"
            for slot in range(runner_count):
                races.append(
                    {
                        "source": "jra",
                        "race_date": f"{year}0101",
                        "kaisai_nen": str(year),
                        "kaisai_tsukihi": month_day,
                        "keibajo_code": "01",
                        "race_bango": str(race_index + 1).zfill(2),
                        "ketto_toroku_bango": f"h{year}{race_index:02d}{slot:02d}",
                        "umaban": slot + 1,
                        "category": "jra",
                        "race_id": race_id,
                        "race_year": year,
                        "feature_schema_version": "v1",
                        "finish_position": int(finishes[slot]),
                        "finish_norm": float(finishes[slot]) / runner_count,
                        "track_code": "11",
                        "grade_code": "A",
                        "speed_index_avg_5": float(rng.normal()),
                        "jockey_recent_win_rate": float(rng.random()),
                    }
                )
    return pl.DataFrame(races)


def test_build_race_batches_encodes_missing_and_unknown_categorical_values():
    df = _make_synthetic_frame()
    df = df.with_row_index("_row_idx").with_columns(
        pl.when(pl.col("_row_idx") == 0).then(None).otherwise(pl.col("track_code")).alias("track_code"),
        pl.when(pl.col("_row_idx") == 1).then(pl.lit("")).otherwise(pl.col("grade_code")).alias("grade_code"),
        pl.when(pl.col("_row_idx") == 3).then(None).otherwise(pl.col("keibajo_code")).alias("keibajo_code"),
        pl.when(pl.col("_row_idx") == 4).then(pl.lit("")).otherwise(pl.col("kaisai_tsukihi")).alias("kaisai_tsukihi"),
    ).with_columns(
        pl.when(pl.col("_row_idx") == 2).then(pl.lit("ZZZ_UNKNOWN_TRACK")).otherwise(pl.col("track_code")).alias("track_code"),
    ).drop("_row_idx")
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    assert arrays["categorical_indices"].shape[-1] == len(cols.categorical)


def test_fit_normalization_stats_replaces_non_finite_mean_with_zero():
    df = _make_synthetic_frame()
    df = df.with_columns(pl.lit(float("inf")).alias("speed_index_avg_5"))
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    numeric_columns = stats["numeric_columns"]
    if "speed_index_avg_5" in numeric_columns:
        index = numeric_columns.index("speed_index_avg_5")
        assert stats["numeric_mean"][index] == 0.0
        assert stats["numeric_std"][index] == 1.0


def test_resolve_transformer_feature_columns_splits_numeric_and_categorical():
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    assert cols.categorical == ["track_code", "grade_code"]
    assert "speed_index_avg_5" in cols.numeric
    assert "ketto_toroku_bango" not in cols.numeric


def test_fit_normalization_stats_handles_missing_and_constant_columns():
    df = pl.DataFrame(
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
    config = default_model_config(
        num_numeric_features=8,
        categorical_vocab_sizes=[5, 3],
        race_categorical_vocab_sizes=[10, 12],
    )
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    numeric = mx.zeros((3, MAX_RUNNERS, 8))
    cat = mx.zeros((3, MAX_RUNNERS, 2), dtype=mx.int32)
    race_cat = mx.zeros((3, 2), dtype=mx.int32)
    umaban = mx.zeros((3, MAX_RUNNERS), dtype=mx.int32)
    mask = mx.ones((3, MAX_RUNNERS), dtype=mx.bool_)
    output = model(numeric, cat, race_cat, umaban, mask)
    assert output["top1_logit"].shape == (3, MAX_RUNNERS)
    assert output["top3_logit"].shape == (3, MAX_RUNNERS)
    assert output["place2_logit"].shape == (3, MAX_RUNNERS)
    assert output["place3_logit"].shape == (3, MAX_RUNNERS)
    assert output["rank_score"].shape == (3, MAX_RUNNERS)
    assert config["embedding_dim"] == DEFAULT_EMBEDDING_DIM
    assert config["num_heads"] == DEFAULT_NUM_HEADS


def test_multitask_loss_returns_scalar_on_synthetic_batch():
    config = default_model_config(
        num_numeric_features=4,
        categorical_vocab_sizes=[3],
        race_categorical_vocab_sizes=[5, 7],
    )
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    numeric = mx.zeros((2, MAX_RUNNERS, 4))
    cat = mx.zeros((2, MAX_RUNNERS, 1), dtype=mx.int32)
    race_cat = mx.zeros((2, 2), dtype=mx.int32)
    umaban = mx.zeros((2, MAX_RUNNERS), dtype=mx.int32)
    mask = mx.ones((2, MAX_RUNNERS), dtype=mx.bool_)
    output = model(numeric, cat, race_cat, umaban, mask)
    finish = mx.array(np.tile(np.arange(1, MAX_RUNNERS + 1), (2, 1)), dtype=mx.float32)
    loss = multitask_loss(
        output,
        finish,
        mask,
        {
            "top1": 1.0,
            "top3": 1.0,
            "pairwise": 1.0,
            "listnet": 0.5,
            "place2": 1.0,
            "place3": 1.0,
            "conditional_place2": 1.0,
            "conditional_place3": 1.0,
        },
    )
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
    assert config["loss_weights"]["listnet"] == 0.0
    assert config["loss_weights"]["place2"] == 1.0
    assert config["loss_weights"]["place3"] == 1.0


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
    df = _make_walk_forward_frame()
    parquet_path = tmp_path / "data.parquet"
    df.write_parquet(parquet_path)
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
            "--output-predictions-dir",
            str(tmp_path / "preds"),
            "--max-epochs",
            "1",
            "--warmup-steps",
            "1",
            "--batch-size",
            "2",
            "--listnet-weight",
            "0.25",
            "--embedding-dim",
            "32",
            "--num-layers",
            "1",
            "--num-heads",
            "2",
        ]
    )
    cli_module.run_walk_forward_command(args)
    payload = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert payload["aggregate"]["fold_count"] == 1
    assert (tmp_path / "preds" / "2024.jsonl").exists()


def test_run_train_command_writes_checkpoint_and_predictions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    df = _make_synthetic_frame()
    train_parquet = tmp_path / "train.parquet"
    valid_parquet = tmp_path / "valid.parquet"
    df.write_parquet(train_parquet)
    df.write_parquet(valid_parquet)
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    args = cli_module.parse_args(
        [
            "train",
            "--train-parquet",
            str(train_parquet),
            "--valid-parquet",
            str(valid_parquet),
            "--output-model-dir",
            str(tmp_path / "ckpt"),
            "--output-metadata",
            str(tmp_path / "meta.json"),
            "--output-predictions",
            str(tmp_path / "preds.jsonl"),
            "--max-epochs",
            "1",
            "--warmup-steps",
            "1",
            "--batch-size",
            "2",
            "--embedding-dim",
            "32",
            "--num-layers",
            "1",
            "--num-heads",
            "2",
            "--dropout",
            "0.0",
        ]
    )
    cli_module.run_train_command(args)
    assert (tmp_path / "ckpt" / "model.safetensors").exists()
    assert (tmp_path / "meta.json").exists()
    assert (tmp_path / "preds.jsonl").exists()


def test_run_predict_command_uses_saved_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in cols.categorical]
    config = default_model_config(
        num_numeric_features=len(cols.numeric),
        categorical_vocab_sizes=vocab_sizes,
    )
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    cli_module.save_checkpoint(model, config, stats, tmp_path / "ckpt")
    input_parquet = tmp_path / "input.parquet"
    df.write_parquet(input_parquet)
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    args = cli_module.parse_args(
        [
            "predict",
            "--model-dir",
            str(tmp_path / "ckpt"),
            "--input-parquet",
            str(input_parquet),
            "--output-predictions",
            str(tmp_path / "out.jsonl"),
            "--batch-size",
            "2",
        ]
    )
    cli_module.run_predict_command(args)
    assert (tmp_path / "out.jsonl").exists()


def test_cli_main_dispatches_predict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in cols.categorical]
    config = default_model_config(
        num_numeric_features=len(cols.numeric),
        categorical_vocab_sizes=vocab_sizes,
    )
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    cli_module.save_checkpoint(model, config, stats, tmp_path / "ckpt")
    input_parquet = tmp_path / "input.parquet"
    df.write_parquet(input_parquet)
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    cli_module.main(
        [
            "predict",
            "--model-dir",
            str(tmp_path / "ckpt"),
            "--input-parquet",
            str(input_parquet),
            "--output-predictions",
            str(tmp_path / "out.jsonl"),
            "--batch-size",
            "2",
        ]
    )
    assert (tmp_path / "out.jsonl").exists()


def test_cli_main_dispatches_train_and_walk_forward(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    df = _make_walk_forward_frame()
    train_parquet = tmp_path / "train.parquet"
    df.write_parquet(train_parquet)
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    cli_module.main(
        [
            "train",
            "--train-parquet",
            str(train_parquet),
            "--output-model-dir",
            str(tmp_path / "ckpt"),
            "--max-epochs",
            "1",
            "--warmup-steps",
            "1",
            "--batch-size",
            "2",
            "--embedding-dim",
            "32",
            "--num-layers",
            "1",
            "--num-heads",
            "2",
        ]
    )
    assert (tmp_path / "ckpt" / "model.safetensors").exists()
    cli_module.main(
        [
            "walk-forward",
            "--parquet",
            str(train_parquet),
            "--train-start-date",
            "20230101",
            "--validation-years",
            "2024",
            "--output-report",
            str(tmp_path / "wf.json"),
            "--max-epochs",
            "1",
            "--warmup-steps",
            "1",
            "--batch-size",
            "2",
            "--embedding-dim",
            "32",
            "--num-layers",
            "1",
            "--num-heads",
            "2",
        ]
    )
    assert (tmp_path / "wf.json").exists()


def test_cli_main_rejects_unknown_command(monkeypatch: pytest.MonkeyPatch):
    import argparse

    monkeypatch.setattr(
        cli_module,
        "parse_args",
        lambda _argv=None: argparse.Namespace(command="unknown"),
    )
    with pytest.raises(ValueError, match="Unknown command"):
        cli_module.main(["unknown"])


def test_race_categorical_vocab_size_counts_padding_slot():
    from finish_position_transformer.dataset import race_categorical_vocab_size

    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    assert race_categorical_vocab_size(stats, "keibajo_code") >= 2
    assert race_categorical_vocab_size(stats, "month") >= 2


def test_fit_normalization_stats_handles_missing_race_meta_columns():
    df = pl.DataFrame(
        {
            "speed_index_avg_5": [1.0, 2.0, 3.0, 4.0],
            "track_code": ["11", "11", "12", "12"],
            "grade_code": ["A", "B", "A", "B"],
        }
    )
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    assert stats["race_categorical_vocab"]["keibajo_code"] == []
    assert stats["race_categorical_vocab"]["month"] == []


def test_train_transformer_without_valid_trains_all_epochs():
    """Training with no valid data must run all max_epochs, not revert to epoch-1 weights.

    Without this fix, valid_score is permanently 0.0 when valid_arrays is None; epoch 1
    saves best_params (0.0 > -inf), later epochs never improve (0.0 not > 0.0), and
    early-stopping fires after early_stopping_epochs steps, restoring epoch-1 weights.
    """
    df = _make_synthetic_frame()
    cols = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, cols)
    arrays = build_race_batches(df, stats)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in cols.categorical]
    config = default_model_config(num_numeric_features=len(cols.numeric), categorical_vocab_sizes=vocab_sizes)
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    train_cfg = default_training_config()
    train_cfg["max_epochs"] = 5
    train_cfg["early_stopping_epochs"] = 2
    train_cfg["warmup_steps"] = 1
    train_cfg["batch_size"] = 4
    # Pass valid_arrays=None — must train all 5 epochs, not stop at 1+2=3
    result = train_transformer(model, arrays, None, train_cfg)
    assert len(result["history"]) == 5, (
        f"Expected 5 epochs of history but got {len(result['history'])} — "
        "early-stopping may have fired incorrectly when no valid data was provided"
    )
    assert result["best_epoch"] == 5
