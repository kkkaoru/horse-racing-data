from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

try:
    import mlx.core as mx
except (ImportError, OSError):
    pytest.skip("MLX requires Apple Silicon/macOS", allow_module_level=True)

import numpy as np
import polars as pl
from mlx.utils import tree_flatten, tree_map

import running_style_transformer as rst
from finish_position_transformer.dataset import MAX_RUNNERS, RaceBatchArrays
from finish_position_transformer.training import TrainingDivergedError
from running_style_transformer import FoldMetrics, TrainingParams


def _make_frame(years: tuple[int, ...] = (2024,), races_per_year: int = 3, seed: int = 5) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    rows = [
        {
            "source": "jra",
            "race_date": f"{year}0105",
            "kaisai_nen": str(year),
            "kaisai_tsukihi": "0105",
            "keibajo_code": "01",
            "race_bango": str(race_index + 1).zfill(2),
            "ketto_toroku_bango": f"h{year}{race_index:02d}{slot:02d}",
            "umaban": slot + 1,
            "category": "jra",
            "race_id": f"jra:{year}:0105:01:{str(race_index + 1).zfill(2)}",
            "race_year": year,
            "feature_schema_version": "v1",
            "finish_position": slot + 1,
            "finish_norm": float(slot + 1) / 4.0,
            "target_running_style_class": slot % 4,
            "track_code": "11",
            "grade_code": "A",
            "speed_index_avg_5": float(rng.normal()),
            "jockey_recent_win_rate": float(rng.random()),
        }
        for year in years
        for race_index in range(races_per_year)
        for slot in range(4)
    ]
    return pl.DataFrame(rows)


def _small_params(seed: int = 1, max_epochs: int = 1, early_stopping_epochs: int = 1) -> TrainingParams:
    return {
        "embedding_dim": 8,
        "num_layers": 1,
        "num_heads": 2,
        "dropout": 0.0,
        "batch_size": 2,
        "learning_rate": 1e-3,
        "weight_decay": 0.0,
        "max_epochs": max_epochs,
        "early_stopping_epochs": early_stopping_epochs,
        "seed": seed,
    }


def _empty_arrays(race_ids: list[str], ketto: list[list[str]], mask: np.ndarray) -> RaceBatchArrays:
    num_races = len(race_ids)
    return {
        "numeric_features": np.zeros((num_races, MAX_RUNNERS, 1), dtype=np.float32),
        "categorical_indices": np.zeros((num_races, MAX_RUNNERS, 1), dtype=np.int32),
        "race_categorical_indices": np.zeros((num_races, 2), dtype=np.int32),
        "umaban": np.zeros((num_races, MAX_RUNNERS), dtype=np.int32),
        "finish_position": np.zeros((num_races, MAX_RUNNERS), dtype=np.float32),
        "mask": mask,
        "race_ids": race_ids,
        "ketto_toroku_bango": ketto,
    }


def _flat_weights(model: rst.RunningStyleTransformer) -> np.ndarray:
    return np.concatenate([np.array(value).ravel() for _, value in tree_flatten(model.parameters())])


def test_resolve_running_style_feature_columns_drops_target_columns():
    columns = rst.resolve_running_style_feature_columns(
        ["race_id", "speed_index_avg_5", "target_running_style_class", "track_code", "finish_norm"]
    )
    assert columns.numeric == ["speed_index_avg_5"]
    assert columns.categorical == ["track_code"]


def test_default_training_params_returns_defaults():
    assert rst.default_training_params() == {
        "embedding_dim": 64,
        "num_layers": 2,
        "num_heads": 4,
        "dropout": 0.1,
        "batch_size": 64,
        "learning_rate": 1e-3,
        "weight_decay": 1e-4,
        "max_epochs": 20,
        "early_stopping_epochs": 4,
        "seed": 20260518,
    }


def test_read_partitioned_parquet_keeps_existing_race_year(tmp_path: Path):
    child = tmp_path / "race_year=2024" / "part.parquet"
    child.parent.mkdir()
    pl.DataFrame({"race_id": ["a"], "race_year": [1999]}).write_parquet(child)
    frame = rst._read_partitioned_parquet(child)
    assert frame["race_year"].to_list() == [1999]


def test_read_partitioned_parquet_derives_race_year_from_directory(tmp_path: Path):
    child = tmp_path / "race_year=2024" / "part.parquet"
    child.parent.mkdir()
    pl.DataFrame({"race_id": ["a"]}).write_parquet(child)
    frame = rst._read_partitioned_parquet(child)
    assert frame["race_year"].to_list() == [2024]


def test_read_partitioned_parquet_leaves_frame_when_directory_is_not_partition(tmp_path: Path):
    child = tmp_path / "plain" / "part.parquet"
    child.parent.mkdir()
    pl.DataFrame({"race_id": ["a"]}).write_parquet(child)
    frame = rst._read_partitioned_parquet(child)
    assert frame.columns == ["race_id"]


def test_load_dataset_parquet_reads_partitioned_directory(tmp_path: Path):
    (tmp_path / "race_year=2023").mkdir()
    (tmp_path / "race_year=2024").mkdir()
    pl.DataFrame({"race_id": ["a"]}).write_parquet(tmp_path / "race_year=2023" / "p.parquet")
    pl.DataFrame({"race_id": ["b"]}).write_parquet(tmp_path / "race_year=2024" / "p.parquet")
    frame = rst.load_dataset_parquet(tmp_path)
    assert frame.to_dicts() == [{"race_id": "a", "race_year": 2023}, {"race_id": "b", "race_year": 2024}]


def test_load_dataset_parquet_reads_flat_directory(tmp_path: Path):
    pl.DataFrame({"race_id": ["a"]}).write_parquet(tmp_path / "a.parquet")
    pl.DataFrame({"race_id": ["b"], "extra": [1]}).write_parquet(tmp_path / "b.parquet")
    frame = rst.load_dataset_parquet(tmp_path)
    assert frame.to_dicts() == [{"race_id": "a", "extra": None}, {"race_id": "b", "extra": 1}]


def test_load_dataset_parquet_raises_for_empty_directory(tmp_path: Path):
    with pytest.raises(ValueError, match="No parquet files found under"):
        rst.load_dataset_parquet(tmp_path)


def test_load_dataset_parquet_reads_single_file(tmp_path: Path):
    path = tmp_path / "single.parquet"
    pl.DataFrame({"race_id": ["a"]}).write_parquet(path)
    frame = rst.load_dataset_parquet(path)
    assert frame.to_dicts() == [{"race_id": "a"}]


def test_split_by_year_separates_train_and_validation():
    df = pl.DataFrame(
        {
            "race_date": ["20150101", "20160101", "20230601", "20240101"],
            "race_year": [2015, 2016, 2023, 2024],
        }
    )
    train_df, valid_df = rst.split_by_year(df, "20160101", 2024)
    assert train_df["race_date"].to_list() == ["20160101", "20230601"]
    assert valid_df["race_date"].to_list() == ["20240101"]


def test_extract_running_style_targets_fills_slots_and_skips_missing():
    df = pl.DataFrame(
        {
            "race_id": ["r1", "r1", "r1"],
            "umaban": [2, 1, 3],
            "target_running_style_class": [3, 0, None],
        }
    )
    targets = rst.extract_running_style_targets(df, ["r1", "absent"])
    assert targets.shape == (2, 18)
    assert targets[0, :4].tolist() == [0, 3, -1, -1]
    assert targets[1].tolist() == [-1] * 18


def test_extract_running_style_targets_truncates_races_over_max_runners():
    df = pl.DataFrame(
        {
            "race_id": ["r1"] * 19,
            "umaban": list(range(1, 20)),
            "target_running_style_class": [1] * 19,
        }
    )
    targets = rst.extract_running_style_targets(df, ["r1"])
    assert targets.tolist() == [[1] * 18]


def test_build_padding_mask_returns_additive_mask():
    mask = mx.array([[True, False]])
    additive = rst.build_padding_mask(mask, 4)
    assert additive.shape == (1, 1, 1, 2)
    assert np.array(additive).tolist() == [[[[0.0, -1e9]]]]


def test_running_style_transformer_forward_emits_four_class_logits():
    model = rst.RunningStyleTransformer(
        num_numeric_features=3,
        categorical_vocab_sizes=[4, 5],
        race_categorical_vocab_sizes=[3],
        embedding_dim=8,
        num_layers=1,
        num_heads=2,
        dropout=0.0,
    )
    logits = model(
        mx.zeros((2, MAX_RUNNERS, 3)),
        mx.ones((2, MAX_RUNNERS, 2), dtype=mx.int32),
        mx.ones((2, 1), dtype=mx.int32),
        mx.ones((2, MAX_RUNNERS), dtype=mx.int32),
        mx.ones((2, MAX_RUNNERS), dtype=mx.bool_),
    )
    assert logits.shape == (2, 18, 4)


def test_to_mx_batch_converts_every_array():
    batch = rst.to_mx_batch(
        {
            "numeric_features": np.zeros((1, 2, 3), dtype=np.float32),
            "categorical_indices": np.zeros((1, 2, 1), dtype=np.int32),
            "race_categorical_indices": np.zeros((1, 2), dtype=np.int32),
            "umaban": np.array([[1, 2]], dtype=np.int32),
            "mask": np.array([[True, False]]),
        },
        np.array([[0, -1]], dtype=np.int32),
    )
    assert sorted(batch) == [
        "categorical_indices",
        "mask",
        "numeric_features",
        "race_categorical_indices",
        "targets",
        "umaban",
    ]
    assert np.array(batch["umaban"]).tolist() == [[1, 2]]
    assert np.array(batch["targets"]).tolist() == [[0, -1]]


def test_cross_entropy_with_mask_ignores_padding_and_pad_targets():
    logits = mx.array([[[0.0, 0.0, 0.0, 0.0], [5.0, 0.0, 0.0, 0.0], [9.0, 0.0, 0.0, 0.0]]])
    targets = mx.array([[1, -1, 2]])
    mask = mx.array([[True, True, False]])
    loss = rst.cross_entropy_with_mask(logits, targets, mask)
    assert float(loss.item()) == pytest.approx(float(np.log(4.0)), rel=1e-5)


def test_cross_entropy_with_mask_returns_zero_when_nothing_valid():
    logits = mx.zeros((1, 2, 4))
    targets = mx.array([[-1, -1]])
    mask = mx.array([[True, False]])
    loss = rst.cross_entropy_with_mask(logits, targets, mask)
    assert float(loss.item()) == 0.0


def test_build_batched_inputs_selects_model_inputs_and_targets():
    arrays = _empty_arrays(["r1"], [[""] * MAX_RUNNERS], np.zeros((1, MAX_RUNNERS), dtype=np.bool_))
    targets = np.full((1, MAX_RUNNERS), -1, dtype=np.int32)
    batched = rst._build_batched_inputs(arrays, targets)
    assert sorted(batched) == [
        "categorical_indices",
        "mask",
        "numeric_features",
        "race_categorical_indices",
        "targets",
        "umaban",
    ]
    assert batched["targets"] is targets


def test_iterate_batches_without_shuffle_keeps_order():
    batched = {
        "numeric_features": np.zeros((3, 2, 1), dtype=np.float32),
        "categorical_indices": np.zeros((3, 2, 1), dtype=np.int32),
        "race_categorical_indices": np.zeros((3, 1), dtype=np.int32),
        "umaban": np.array([[1, 0], [2, 0], [3, 0]], dtype=np.int32),
        "mask": np.ones((3, 2), dtype=np.bool_),
        "targets": np.zeros((3, 2), dtype=np.int32),
    }
    batches = rst.iterate_batches(batched, 2, shuffle=False, rng=np.random.default_rng(0))
    assert len(batches) == 2
    assert np.array(batches[0]["umaban"]).tolist() == [[1, 0], [2, 0]]
    assert np.array(batches[1]["umaban"]).tolist() == [[3, 0]]


def test_iterate_batches_with_shuffle_uses_rng_permutation():
    batched = {
        "numeric_features": np.zeros((4, 1, 1), dtype=np.float32),
        "categorical_indices": np.zeros((4, 1, 1), dtype=np.int32),
        "race_categorical_indices": np.zeros((4, 1), dtype=np.int32),
        "umaban": np.array([[1], [2], [3], [4]], dtype=np.int32),
        "mask": np.ones((4, 1), dtype=np.bool_),
        "targets": np.zeros((4, 1), dtype=np.int32),
    }
    batches = rst.iterate_batches(batched, 4, shuffle=True, rng=np.random.default_rng(3))
    assert np.array(batches[0]["umaban"]).ravel().tolist() == [4, 3, 2, 1]


def test_compute_softmax_predictions_returns_normalized_probabilities():
    model = rst.RunningStyleTransformer(
        num_numeric_features=1,
        categorical_vocab_sizes=[],
        race_categorical_vocab_sizes=[],
        embedding_dim=8,
        num_layers=1,
        num_heads=2,
        dropout=0.0,
    )
    model.eval()
    batch = {
        "numeric_features": mx.zeros((1, MAX_RUNNERS, 1)),
        "categorical_indices": mx.zeros((1, MAX_RUNNERS, 0), dtype=mx.int32),
        "race_categorical_indices": mx.zeros((1, 0), dtype=mx.int32),
        "umaban": mx.ones((1, MAX_RUNNERS), dtype=mx.int32),
        "mask": mx.ones((1, MAX_RUNNERS), dtype=mx.bool_),
    }
    probabilities = rst.compute_softmax_predictions(model, [batch, batch])
    assert probabilities.shape == (2, 18, 4)
    assert np.allclose(probabilities.sum(axis=-1), 1.0, atol=1e-5)


def test_evaluate_validation_loss_averages_batches():
    model = rst.RunningStyleTransformer(
        num_numeric_features=1,
        categorical_vocab_sizes=[],
        race_categorical_vocab_sizes=[],
        embedding_dim=8,
        num_layers=1,
        num_heads=2,
        dropout=0.0,
    )
    model.eval()
    batch = {
        "numeric_features": mx.zeros((1, MAX_RUNNERS, 1)),
        "categorical_indices": mx.zeros((1, MAX_RUNNERS, 0), dtype=mx.int32),
        "race_categorical_indices": mx.zeros((1, 0), dtype=mx.int32),
        "umaban": mx.ones((1, MAX_RUNNERS), dtype=mx.int32),
        "mask": mx.ones((1, MAX_RUNNERS), dtype=mx.bool_),
        "targets": mx.zeros((1, MAX_RUNNERS), dtype=mx.int32),
    }
    single = rst.evaluate_validation_loss(model, [batch])
    double = rst.evaluate_validation_loss(model, [batch, batch])
    assert single > 0.0
    assert double == pytest.approx(single, rel=1e-6)


def test_evaluate_validation_loss_returns_zero_for_no_batches():
    model = rst.RunningStyleTransformer(
        num_numeric_features=1,
        categorical_vocab_sizes=[],
        race_categorical_vocab_sizes=[],
        embedding_dim=8,
        num_layers=1,
        num_heads=2,
        dropout=0.0,
    )
    assert rst.evaluate_validation_loss(model, []) == 0.0


def test_cross_entropy_with_mask_for_model_matches_direct_loss():
    model = rst.RunningStyleTransformer(
        num_numeric_features=1,
        categorical_vocab_sizes=[],
        race_categorical_vocab_sizes=[],
        embedding_dim=8,
        num_layers=1,
        num_heads=2,
        dropout=0.0,
    )
    model.eval()
    numeric = mx.zeros((1, MAX_RUNNERS, 1))
    categorical = mx.zeros((1, MAX_RUNNERS, 0), dtype=mx.int32)
    race_categorical = mx.zeros((1, 0), dtype=mx.int32)
    umaban = mx.ones((1, MAX_RUNNERS), dtype=mx.int32)
    mask = mx.ones((1, MAX_RUNNERS), dtype=mx.bool_)
    targets = mx.zeros((1, MAX_RUNNERS), dtype=mx.int32)
    via_model = rst.cross_entropy_with_mask_for_model(
        model, numeric, categorical, race_categorical, umaban, mask, targets
    )
    direct = rst.cross_entropy_with_mask(model(numeric, categorical, race_categorical, umaban, mask), targets, mask)
    assert float(via_model.item()) == pytest.approx(float(direct.item()), rel=1e-6)


def test_train_one_fold_seeds_mlx_rng_for_weight_init(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("builtins.print", lambda *_args: None)
    df = _make_frame()
    model_a = rst.train_one_fold(df, df, _small_params(seed=11, max_epochs=0))[0]
    mx.random.seed(999)
    model_b = rst.train_one_fold(df, df, _small_params(seed=11, max_epochs=0))[0]
    model_c = rst.train_one_fold(df, df, _small_params(seed=12, max_epochs=0))[0]
    assert np.array_equal(_flat_weights(model_a), _flat_weights(model_b))
    assert not np.array_equal(_flat_weights(model_a), _flat_weights(model_c))


def test_train_one_fold_without_epochs_returns_untrained_predictions(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("builtins.print", lambda *_args: None)
    df = _make_frame()
    _model, stats, probabilities, valid_targets, valid_arrays, best_epoch = rst.train_one_fold(
        df, df, _small_params(max_epochs=0)
    )
    assert best_epoch == 0
    assert stats["numeric_columns"] == ["speed_index_avg_5", "jockey_recent_win_rate"]
    assert probabilities.shape == (3, 18, 4)
    assert valid_targets[0, :5].tolist() == [0, 1, 2, 3, -1]
    assert valid_arrays["race_ids"] == ["jra:2024:0105:01:01", "jra:2024:0105:01:02", "jra:2024:0105:01:03"]


def test_train_one_fold_tracks_best_epoch_and_stops_early(monkeypatch: pytest.MonkeyPatch):
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    losses: Iterator[float] = iter([1.0, 2.0, 0.5, 3.0, 3.0, 0.1])
    monkeypatch.setattr(rst, "evaluate_validation_loss", lambda _model, _batches: next(losses))
    df = _make_frame()
    result = rst.train_one_fold(df, df, _small_params(max_epochs=10, early_stopping_epochs=2))
    assert result[5] == 3
    assert captured == [
        '{"epoch": 1, "early_stop_loss": 1.0}',
        '{"epoch": 2, "early_stop_loss": 2.0}',
        '{"epoch": 3, "early_stop_loss": 0.5}',
        '{"epoch": 4, "early_stop_loss": 3.0}',
        '{"epoch": 5, "early_stop_loss": 3.0}',
    ]


def test_train_one_fold_runs_real_training_epoch(monkeypatch: pytest.MonkeyPatch):
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    df = _make_frame()
    _model, _stats, probabilities, _targets, _arrays, best_epoch = rst.train_one_fold(
        df, df, _small_params(max_epochs=1)
    )
    assert best_epoch == 1
    assert len(captured) == 1
    assert np.isfinite(probabilities).all()


def test_train_one_fold_skips_steps_with_non_finite_gradients(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("builtins.print", lambda *_args: None)

    def nan_value_and_grad(model: rst.RunningStyleTransformer, _loss_fn: object):
        def loss_and_grad(*_args: object):
            return mx.array(0.5), tree_map(lambda value: value * mx.array(float("nan")), model.trainable_parameters())

        return loss_and_grad

    monkeypatch.setattr("mlx.nn.value_and_grad", nan_value_and_grad)
    df = _make_frame()
    untrained = rst.train_one_fold(df, df, _small_params(seed=11, max_epochs=0))[0]
    skipped = rst.train_one_fold(df, df, _small_params(seed=11, max_epochs=1))[0]
    assert np.array_equal(_flat_weights(untrained), _flat_weights(skipped))


class _NanOptimizer:
    state: dict[str, object] = {}

    def __init__(self, **_kwargs: object) -> None:
        pass

    def update(self, model: rst.RunningStyleTransformer, _grads: object) -> None:
        model.update(tree_map(lambda value: value * mx.array(float("nan")), model.parameters()))


def test_train_one_fold_raises_when_parameters_go_non_finite(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("builtins.print", lambda *_args: None)
    monkeypatch.setattr("mlx.optimizers.AdamW", _NanOptimizer)
    df = _make_frame()
    with pytest.raises(TrainingDivergedError, match="non-finite parameters after epoch 1"):
        _ = rst.train_one_fold(df, df, _small_params(max_epochs=2))


def test_sanitize_float_handles_none_non_finite_and_numbers():
    assert rst._sanitize_float(None) is None
    assert rst._sanitize_float(float("nan")) is None
    assert rst._sanitize_float(0.25) == 0.25
    assert rst._sanitize_float(2) == 2.0


def test_build_valid_lookup_keeps_first_duplicate():
    df = pl.DataFrame(
        {
            "race_id": ["r1", "r1", "r1"],
            "ketto_toroku_bango": ["h1", "h1", "h2"],
            "target_running_style_class": [0, 3, 1],
            "umaban": [1, 9, 2],
            "race_year": [2024, 2024, 2024],
        }
    )
    lookup = rst._build_valid_lookup(df)
    assert lookup == {("r1", "h1"): (0, 1, 2024), ("r1", "h2"): (1, 2, 2024)}


def test_optional_int_handles_none_non_finite_and_numbers():
    assert rst._optional_int(None) is None
    assert rst._optional_int(float("inf")) is None
    assert rst._optional_int(3.0) == 3
    assert rst._optional_int(7) == 7


def test_build_predictions_df_skips_padding_blank_and_unknown_horses():
    mask = np.zeros((1, MAX_RUNNERS), dtype=np.bool_)
    mask[0, :4] = True
    ketto = [["h1", "", "ghost", "h2"] + [""] * (MAX_RUNNERS - 4)]
    probabilities = np.zeros((1, MAX_RUNNERS, 4), dtype=np.float32)
    probabilities[0, 0] = [0.1, 0.6, 0.2, 0.1]
    probabilities[0, 3] = [0.7, 0.1, 0.1, 0.1]
    valid_df = pl.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["h1", "h2"],
            "target_running_style_class": [1, None],
            "umaban": [1, 4],
            "race_year": [2024, 2024],
        }
    )
    predictions = rst.build_predictions_df(valid_df, _empty_arrays(["r1"], ketto, mask), probabilities)
    assert predictions.select(
        ["ketto_toroku_bango", "umaban", "race_year", "target_running_style_class", "predicted_label", "predicted_class"]
    ).to_dicts() == [
        {
            "ketto_toroku_bango": "h1",
            "umaban": 1,
            "race_year": 2024,
            "target_running_style_class": 1,
            "predicted_label": "senkou",
            "predicted_class": 1,
        },
        {
            "ketto_toroku_bango": "h2",
            "umaban": 4,
            "race_year": 2024,
            "target_running_style_class": None,
            "predicted_label": "nige",
            "predicted_class": 0,
        },
    ]
    assert predictions["p_senkou"].to_list() == pytest.approx([0.6, 0.1])


def test_compute_accuracy_counts_labeled_rows():
    predictions = pl.DataFrame(
        {"target_running_style_class": [0, 1, 2, None], "predicted_class": [0, 1, 3, 0]}
    )
    assert rst.compute_accuracy(predictions) == pytest.approx(2.0 / 3.0)


def test_compute_accuracy_returns_nan_without_labels():
    predictions = pl.DataFrame(
        {"target_running_style_class": [None], "predicted_class": [0]},
        schema={"target_running_style_class": pl.Int64, "predicted_class": pl.Int64},
    )
    assert np.isnan(rst.compute_accuracy(predictions))


def test_compute_macro_f1_averages_classes_with_true_positives():
    predictions = pl.DataFrame(
        {"target_running_style_class": [0, 0, 1, 2, None], "predicted_class": [0, 1, 1, 3, 2]}
    )
    assert rst.compute_macro_f1(predictions) == pytest.approx((2.0 / 3.0 + 2.0 / 3.0) / 2.0)


def test_compute_macro_f1_returns_nan_without_true_positives():
    predictions = pl.DataFrame({"target_running_style_class": [0, 1], "predicted_class": [1, 0]})
    assert np.isnan(rst.compute_macro_f1(predictions))


def test_compute_macro_f1_returns_nan_without_labels():
    predictions = pl.DataFrame(
        {"target_running_style_class": [None], "predicted_class": [0]},
        schema={"target_running_style_class": pl.Int64, "predicted_class": pl.Int64},
    )
    assert np.isnan(rst.compute_macro_f1(predictions))


def test_write_predictions_jsonl_replaces_non_finite_floats(tmp_path: Path):
    output = tmp_path / "nested" / "out.jsonl"
    predictions = pl.DataFrame({"race_id": ["r1", "r2"], "p_nige": [float("nan"), 0.5]})
    rst.write_predictions_jsonl(predictions, output)
    assert output.read_text(encoding="utf-8") == (
        '{"race_id": "r1", "p_nige": null}\n{"race_id": "r2", "p_nige": 0.5}\n'
    )


def test_write_walk_forward_report_aggregates_folds(tmp_path: Path):
    output = tmp_path / "nested" / "report.json"
    folds: list[FoldMetrics] = [
        {"validation_year": 2024, "train_rows": 10, "valid_rows": 4, "accuracy": 0.5, "macro_f1": 0.25, "best_epoch": 1},
        {"validation_year": 2025, "train_rows": 14, "valid_rows": 4, "accuracy": 0.7, "macro_f1": float("nan"), "best_epoch": 2},
    ]
    rst.write_walk_forward_report(folds, output)
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["aggregate"] == {"accuracy_mean": pytest.approx(0.6), "macro_f1_mean": 0.25}
    assert payload["folds"][0] == {
        "validation_year": 2024,
        "train_rows": 10,
        "valid_rows": 4,
        "accuracy": 0.5,
        "macro_f1": 0.25,
        "best_epoch": 1,
    }


def test_parse_args_applies_defaults(tmp_path: Path):
    args = rst.parse_args(["walk-forward", "--csv", str(tmp_path / "in"), "--output-predictions-dir", str(tmp_path / "out")])
    assert args.command == "walk-forward"
    assert args.train_start_date == "20160101"
    assert args.validation_years == "2024,2025"
    assert args.output_report is None
    assert args.max_epochs == 20
    assert args.early_stopping_epochs == 4
    assert args.seed == 20260518


def test_parse_validation_years_skips_blank_tokens():
    assert rst.parse_validation_years(" 2024, ,2025,") == [2024, 2025]


def test_main_runs_walk_forward_with_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    data_path = tmp_path / "features.parquet"
    _make_frame(years=(2023, 2024, 2025), races_per_year=2).write_parquet(data_path)
    report = tmp_path / "report.json"
    rst.main(
        [
            "walk-forward",
            "--csv",
            str(data_path),
            "--train-start-date",
            "20230101",
            "--validation-years",
            "2024,2025",
            "--output-predictions-dir",
            str(tmp_path / "preds"),
            "--output-report",
            str(report),
            "--max-epochs",
            "1",
            "--seed",
            "3",
        ]
    )
    lines = (tmp_path / "preds" / "2024-2025.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 16
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert [fold["validation_year"] for fold in payload["folds"]] == [2024, 2025]
    assert [fold["train_rows"] for fold in payload["folds"]] == [8, 16]
    assert json.loads(captured[-1])["rows"] == 16


def test_run_walk_forward_skips_report_when_not_requested(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("builtins.print", lambda *_args: None)
    data_path = tmp_path / "features.parquet"
    _make_frame(years=(2023, 2024), races_per_year=2).write_parquet(data_path)
    args = rst.parse_args(
        [
            "walk-forward",
            "--csv",
            str(data_path),
            "--train-start-date",
            "20230101",
            "--validation-years",
            "2024",
            "--output-predictions-dir",
            str(tmp_path / "preds"),
            "--max-epochs",
            "0",
        ]
    )
    rst.run_walk_forward(args)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["features.parquet", "preds"]
    assert len((tmp_path / "preds" / "2024-2024.jsonl").read_text(encoding="utf-8").splitlines()) == 8


def test_train_one_fold_early_stops_on_train_tail_and_scores_valid_year(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("builtins.print", lambda *_args: None)
    early_stop_race_counts: list[int] = []

    def counting_loss(_model: rst.RunningStyleTransformer, batches: list[dict[str, mx.array]]) -> float:
        early_stop_race_counts.append(sum(int(batch["mask"].shape[0]) for batch in batches))
        return 1.0

    monkeypatch.setattr(rst, "evaluate_validation_loss", counting_loss)
    _model, _stats, probabilities, _targets, valid_arrays, _best_epoch = rst.train_one_fold(
        _make_frame(years=(2024,), races_per_year=10), _make_frame(years=(2025,), races_per_year=3), _small_params()
    )
    assert early_stop_race_counts == [1]
    assert probabilities.shape == (3, 18, 4)
    assert valid_arrays["race_ids"] == ["jra:2025:0105:01:01", "jra:2025:0105:01:02", "jra:2025:0105:01:03"]
