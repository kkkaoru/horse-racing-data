"""Exercise the study end-to-end with tiny local data and real MLX updates."""

import copy
from pathlib import Path

import mlx.core as mx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel
from chronos2_mlx.pipeline import Chronos2MLXPipeline

from timesfm_finish_position.chronos_mlx_data import HorseWindows
from timesfm_finish_position.chronos_mlx_study import (
    StudyConfig,
    configure_adaptation,
    main,
    predict_windows,
    run_study,
)
from timesfm_finish_position.chronos_mlx_training import ChronosTrainStep, StepConfig


@pytest.fixture
def tiny_pipeline(monkeypatch: pytest.MonkeyPatch) -> Chronos2MLXPipeline:
    config = Chronos2MLXConfig(
        d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2
    )
    pipeline = Chronos2MLXPipeline(Chronos2MLXModel(config), config)

    def load(source_config: Path, *, dtype: str) -> Chronos2MLXPipeline:
        assert source_config.name == "config.json"
        assert dtype == "bfloat16"
        loaded = copy.deepcopy(pipeline)
        loaded.model.set_dtype(mx.bfloat16)
        return loaded

    monkeypatch.setattr("timesfm_finish_position.chronos_mlx_study.load_local_pipeline", load)
    monkeypatch.setenv("MLX_ENABLE_TF32", "0")
    return pipeline


@pytest.mark.usefixtures("tiny_pipeline")
def test_real_study_artifacts_and_sparse_retention(tmp_path: Path) -> None:
    history = tmp_path / "history.parquet"
    pq.write_table(
        pa.table(
            {
                "horse_id": ["a", "a", "a", "a", "a", "a", "b"],
                "race_id": ["nar:1", "jra:2", "nar:3", "jra:4", "jra:5", "jra:6", "jra:7"],
                "venue_code": ["50", "05", "50", "05", "05", "05", "05"],
                "race_date": [
                    "20190101",
                    "20190201",
                    "20200101",
                    "20210101",
                    "20220101",
                    "20230101",
                    "20230101",
                ],
                "performance_rating": [0.0, 1.0, 0.5, 0.2, 0.8, 0.4, 1.0],
            }
        ),
        history,
    )
    config = tmp_path / "config.json"
    config.write_text('{"chronos_config": {}}', encoding="utf-8")
    output = tmp_path / "study"
    report = run_study(
        history=history,
        source_config=config,
        output=output,
        config=StudyConfig(
            steps=2,
            batch_size=2,
            context_length=32,
            target_race_prefix="jra:",
            training_domain="jra",
        ),
    )
    assert report["train_windows"] == 2
    assert report["development_windows"] == 1
    assert report["development_all_rows"] == 2
    assert report["production_eligible"] is False
    predictions = pq.read_table(output / "development-predictions.parquet")
    assert predictions["chronos_head"].null_count == 1
    assert (output / "portable/model.safetensors").is_file()
    assert (output / "checkpoint/checkpoint.json").is_file()


@pytest.mark.usefixtures("tiny_pipeline")
def test_resume_matches_uninterrupted_updates(tmp_path: Path) -> None:
    history = tmp_path / "history.parquet"
    pq.write_table(
        pa.table(
            {
                "horse_id": ["a"] * 4,
                "race_date": ["20180101", "20190101", "20200101", "20230101"],
                "performance_rating": [0.1, 0.2, 0.4, 0.3],
            }
        ),
        history,
    )
    source = tmp_path / "config.json"
    source.write_text('{"chronos_config": {}}', encoding="utf-8")
    config = StudyConfig(steps=4, batch_size=1, context_length=32)
    complete = tmp_path / "complete"
    run_study(
        history=history, source_config=source, output=complete, config=config, checkpoint_every=2
    )
    resumed = tmp_path / "resumed"
    report = run_study(
        history=history,
        source_config=source,
        output=resumed,
        config=config,
        resume=complete / "checkpoints/step-000002",
    )
    assert report["resumed_step"] == 2
    np.testing.assert_array_equal(
        np.load(complete / "final-quantiles.npy"), np.load(resumed / "final-quantiles.npy")
    )


def test_lora_freezes_base_and_updates_adapter(tiny_pipeline: Chronos2MLXPipeline) -> None:
    model = tiny_pipeline.model
    configure_adaptation(model, "lora", lora_rank=4, lora_alpha=8.0)
    step = ChronosTrainStep(model, StepConfig(warmup_steps=0, compile_mode="full"))
    base = np.array(model.shared.weight)
    module = dict(model.named_modules())["encoder.block.0.layer.0.self_attention.q"]
    before = np.array(module.lora_b)
    assert before.shape == (32, 4)
    step(mx.ones((1, 2, 32)), mx.ones((1, 2, 1)))
    np.testing.assert_array_equal(np.array(model.shared.weight), base)
    assert not np.array_equal(np.array(module.lora_b), before)


def test_study_env_guard(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("MLX_ENABLE_TF32", raising=False)
    with pytest.raises(ValueError, match="TF32 disabled"):
        run_study(history=tmp_path, source_config=tmp_path, output=tmp_path, config=StudyConfig())
    monkeypatch.setenv("MLX_ENABLE_TF32", "0")
    with pytest.raises(ValueError, match="YYYYMMDD"):
        StudyConfig(training_start="2020-01-01")
    with pytest.raises(ValueError, match="separated"):
        StudyConfig(training_end="20230101")
    with pytest.raises(ValueError, match="positive"):
        run_study(
            history=tmp_path, source_config=tmp_path, output=tmp_path, config=StudyConfig(steps=0)
        )


def test_no_development(tiny_pipeline: Chronos2MLXPipeline) -> None:
    windows = HorseWindows(
        np.empty((0, 32), dtype=np.float32),
        np.empty((0, 1), dtype=np.float32),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.int64),
    )
    with pytest.raises(ValueError, match="No development"):
        predict_windows(tiny_pipeline, windows)


def test_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    def study(
        *,
        history: Path,
        source_config: Path,
        output: Path,
        config: StudyConfig,
        resume: Path | None,
        checkpoint_every: int,
    ) -> dict[str, object]:
        assert str(history) == "history"
        assert str(source_config) == "config"
        assert str(output) == "output"
        assert config.steps == 1000
        assert config.batch_size == 32
        assert config.context_length == 128
        assert config.minimum_history == 2
        assert config.seed == 42
        assert config.learning_rate == 1e-5
        assert config.weight_decay == 0.01
        assert config.warmup_steps == 100
        assert config.lora_rank == 8
        assert config.lora_alpha == 16.0
        assert resume is None
        assert checkpoint_every == 250
        return {}

    monkeypatch.setattr("timesfm_finish_position.chronos_mlx_study.run_study", study)
    monkeypatch.setattr(
        "sys.argv",
        ["study", "--history", "history", "--source-config", "config", "--output", "output"],
    )
    main()
