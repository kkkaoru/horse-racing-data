"""Resource diagnostics never change workload results or leak credentials."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from predict_lib import resource_usage
from predict_lib.system_memory_sampler import SystemMemorySnapshot


@pytest.fixture(autouse=True)
def sampler_mock(monkeypatch: pytest.MonkeyPatch) -> Mock:
    sampler = Mock(spec=resource_usage.SystemMemorySampler)
    sampler.finish.return_value = SystemMemorySnapshot(None, None, None, 0, False)
    monkeypatch.setattr(resource_usage, "SystemMemorySampler", Mock(return_value=sampler))
    return sampler


def test_resource_report(monkeypatch: pytest.MonkeyPatch, sampler_mock: Mock) -> None:
    output = Mock()
    monkeypatch.setattr("builtins.print", output)
    monkeypatch.setattr(resource_usage.time, "monotonic", Mock(side_effect=[1.0, 2.25]))
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=["100", "200", "300"]))
    with resource_usage.observe_resources(
        resource_usage.ResourceWorkload("jra", "20260913", "rescore", "01")
    ):
        pass
    assert json.loads(output.call_args.args[0]) == {
        "category": "jra",
        "container_lifetime_memory_peak_bytes": 200,
        "elapsed_seconds": 1.25,
        "event": "container-resource-usage",
        "memory_measurement_source": "cgroup-v2",
        "memory_current_bytes": 100,
        "memory_limit_bytes": 300,
        "mode": "rescore",
        "race": "01",
        "run_date": "20260913",
        "succeeded": True,
        "system_memory_total_bytes": None,
        "system_memory_used_bytes": None,
        "sampled_system_memory_peak_bytes": None,
        "system_memory_sample_count": 0,
        "system_memory_sampler_start_failed": False,
        "system_memory_sample_interval_seconds": 0.25,
    }
    sampler_mock.start.assert_called_once_with()
    sampler_mock.finish.assert_called_once_with()
    assert output.call_args.kwargs == {"flush": True}


def test_missing_cgroup_preserves_error(monkeypatch: pytest.MonkeyPatch) -> None:
    output = Mock()
    monkeypatch.setattr("builtins.print", output)
    monkeypatch.setattr(resource_usage.time, "monotonic", Mock(side_effect=[1.0, 1.5]))
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=OSError("missing")))
    with (
        pytest.raises(ValueError, match="prediction failed"),
        resource_usage.observe_resources(
            resource_usage.ResourceWorkload("nar", "20260913", "day-base", None)
        ),
    ):
        raise ValueError("prediction failed")
    assert json.loads(output.call_args.args[0]) == {
        "category": "nar",
        "container_lifetime_memory_peak_bytes": None,
        "elapsed_seconds": 0.5,
        "event": "container-resource-usage",
        "memory_measurement_source": "unavailable",
        "memory_current_bytes": None,
        "memory_limit_bytes": None,
        "mode": "day-base",
        "race": None,
        "run_date": "20260913",
        "succeeded": False,
        "system_memory_total_bytes": None,
        "system_memory_used_bytes": None,
        "sampled_system_memory_peak_bytes": None,
        "system_memory_sample_count": 0,
        "system_memory_sampler_start_failed": False,
        "system_memory_sample_interval_seconds": 0.25,
    }


def test_nested_v1_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    read = Mock(side_effect=[OSError(), OSError(), OSError(), "100", "200", "300"])
    monkeypatch.setattr(Path, "read_text", read)
    assert resource_usage.read_cgroup_memory() == resource_usage.CgroupMemory(
        "cgroup-v1", 100, 200, 300
    )


def test_root_v1_unlimited_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    read = Mock(
        side_effect=[
            OSError(),
            OSError(),
            OSError(),
            OSError(),
            OSError(),
            OSError(),
            "0",
            "200",
            "9223372036854771712",
        ]
    )
    monkeypatch.setattr(Path, "read_text", read)
    assert resource_usage.read_cgroup_memory() == resource_usage.CgroupMemory(
        "cgroup-v1", 0, 200, None
    )


def test_partial_controller_does_not_mix_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    read = Mock(side_effect=["-1", "200", "max"])
    monkeypatch.setattr(Path, "read_text", read)
    assert resource_usage.read_cgroup_memory() == resource_usage.CgroupMemory(
        "cgroup-v2", None, 200, None
    )
    assert read.call_count == 3


def test_limit_only_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    read = Mock(side_effect=["max", "invalid", "300"])
    monkeypatch.setattr(Path, "read_text", read)
    assert resource_usage.read_cgroup_memory() == resource_usage.CgroupMemory(
        "cgroup-v2", None, None, 300
    )
    assert read.call_count == 3


def test_logging_failure_is_harmless(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("builtins.print", Mock(side_effect=BrokenPipeError()))
    monkeypatch.setattr(Path, "read_text", Mock(return_value="max"))
    with resource_usage.observe_resources(
        resource_usage.ResourceWorkload("nar", "20260913", "full", None)
    ):
        pass
