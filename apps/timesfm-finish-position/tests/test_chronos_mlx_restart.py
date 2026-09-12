"""Restart the public study CLI in a fresh process using a tiny local artifact."""

import json
import os
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import mlx.core as mx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel

from timesfm_finish_position.chronos_mlx_export import portable_weights


def test_fresh_process_resume_is_exact(tmp_path: Path) -> None:
    model_config = Chronos2MLXConfig(
        d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2
    )
    mx.random.seed(123)
    model = Chronos2MLXModel(model_config)
    source = tmp_path / "config.json"
    source.write_text(json.dumps({"chronos_config": asdict(model_config)}), encoding="utf-8")
    mx.save_safetensors(str(tmp_path / "model.safetensors"), portable_weights(model))
    history = tmp_path / "history.parquet"
    pq.write_table(
        pa.table(
            {
                "horse_id": ["a", "a", "a", "a"],
                "race_date": ["20180101", "20190101", "20200101", "20230101"],
                "performance_rating": [0.1, 0.2, 0.4, 0.3],
            }
        ),
        history,
    )
    environment = {**os.environ, "MLX_ENABLE_TF32": "0", "HF_HUB_OFFLINE": "1"}
    package = Path(__file__).resolve().parents[1]
    complete, resumed = tmp_path / "complete", tmp_path / "resumed"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "timesfm_finish_position.chronos_mlx_study",
            "--history",
            str(history),
            "--source-config",
            str(source),
            "--output",
            str(complete),
            "--steps",
            "4",
            "--batch-size",
            "1",
            "--checkpoint-every",
            "2",
        ],
        cwd=package,
        env=environment,
        check=True,
        timeout=60,
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "timesfm_finish_position.chronos_mlx_study",
            "--history",
            str(history),
            "--source-config",
            str(source),
            "--output",
            str(resumed),
            "--steps",
            "4",
            "--batch-size",
            "1",
            "--resume",
            str(complete / "checkpoints/step-000002"),
        ],
        cwd=package,
        env=environment,
        check=True,
        timeout=60,
    )
    report = json.loads((resumed / "report.json").read_text(encoding="utf-8"))
    assert report["resumed_step"] == 2
    assert report["production_eligible"] is False
    np.testing.assert_array_equal(
        np.load(complete / "final-quantiles.npy"), np.load(resumed / "final-quantiles.npy")
    )
