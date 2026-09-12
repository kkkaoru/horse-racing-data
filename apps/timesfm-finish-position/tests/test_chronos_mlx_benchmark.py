"""Benchmark contract checks using a tiny real MLX model without downloads."""

from pathlib import Path

import pytest
from chronos2_mlx.config import Chronos2MLXConfig
from chronos2_mlx.model import Chronos2MLXModel
from chronos2_mlx.pipeline import Chronos2MLXPipeline

from timesfm_finish_position.chronos_mlx_benchmark import BenchmarkConfig, benchmark, main


@pytest.fixture
def local_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    def load(model_id: str, dtype: str) -> Chronos2MLXPipeline:
        assert model_id == "amazon/chronos-2"
        assert dtype == "float32"
        config = Chronos2MLXConfig(
            d_model=32, d_kv=16, d_ff=64, num_layers=1, num_heads=2, vocab_size=2
        )
        return Chronos2MLXPipeline(Chronos2MLXModel(config), config)

    monkeypatch.setenv("MLX_ENABLE_TF32", "0")
    monkeypatch.setattr(Chronos2MLXPipeline, "from_pretrained", load)


@pytest.mark.usefixtures("local_pipeline")
@pytest.mark.parametrize("fused", [False, True])
def test_benchmark_contract(fused: bool) -> None:
    result = benchmark(BenchmarkConfig(steps=4, context_length=32, batch_size=2, fused=fused))
    assert result["synthetic"] is True
    assert result["production_eligible"] is False
    assert result["tf32"] is False
    assert result["trainable_dtype"] == "float32"
    assert result["gpu_utilization"] is None


def test_invalid_shape() -> None:
    with pytest.raises(ValueError, match="Require steps"):
        BenchmarkConfig(steps=1)


def test_requires_precision_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MLX_ENABLE_TF32", raising=False)
    with pytest.raises(ValueError, match="MLX_ENABLE_TF32"):
        benchmark(BenchmarkConfig())


@pytest.mark.usefixtures("local_pipeline")
def test_cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    monkeypatch.setattr("sys.argv", ["benchmark", "--steps", "4", "--output", str(output)])
    main()
    assert output.is_file()
    with pytest.raises(FileExistsError):
        main()
