"""Prevent experimental weights, GPU dependencies or training code entering CPU image."""

from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]


def test_cpu_dependencies_and_build_contract() -> None:
    lock = (PACKAGE / "requirements-chronos-cpu.txt").read_text(encoding="utf-8")
    dockerfile = (PACKAGE / "Dockerfile.chronos").read_text(encoding="utf-8")
    assert "torch-2.13.0%2Bcpu-cp312-cp312-manylinux_2_28_x86_64.whl" in lock
    assert "--hash=sha256:" in lock
    assert "nvidia-" not in lock
    assert "mlx==" not in lock
    assert "--require-hashes" in dockerfile
    assert "HF_HUB_OFFLINE=1" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "COPY models" not in dockerfile


def test_context_allowlist() -> None:
    patterns = (PACKAGE / "Dockerfile.chronos.dockerignore").read_text(encoding="utf-8")
    assert patterns.splitlines()[1:] == [
        "**",
        "!requirements-chronos-cpu.txt",
        "!src/",
        "src/**",
        "!src/timesfm_finish_position/",
        "src/timesfm_finish_position/**",
        "!src/timesfm_finish_position/__init__.py",
        "!src/timesfm_finish_position/domain.py",
        "!src/timesfm_finish_position/chronos_forecasting.py",
        "!src/timesfm_finish_position/chronos_portable.py",
        "!src/timesfm_finish_position/chronos_service.py",
    ]
