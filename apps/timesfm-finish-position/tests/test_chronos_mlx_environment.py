"""The launcher must set precision before importing MLX and keep artifacts local."""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run_chronos_mlx.sh"


def test_launcher_requires_command() -> None:
    result = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, check=False)
    assert result.returncode == 2
    assert "Usage:" in result.stderr


def test_launcher_overrides_external_cache_and_tf32() -> None:
    environment = dict(os.environ, HF_HOME="/not-used", MLX_ENABLE_TF32="1")
    result = subprocess.run(
        [
            "bash",
            str(SCRIPT),
            "bash",
            "-c",
            'test "$HF_HOME" = "$CHRONOS_CAMPAIGN_ROOT/huggingface" && '
            'test "$TMPDIR" = "$CHRONOS_CAMPAIGN_ROOT/tmp" && '
            'test "$PWD/../../.cache/uv" -ef "$UV_CACHE_DIR" && '
            'printf "%s" "$MLX_ENABLE_TF32"',
        ],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stdout == "0"
