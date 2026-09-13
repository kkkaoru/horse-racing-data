"""Deterministic sampler tests: no live threads, clocks, or host memory reads."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from predict_lib import system_memory_sampler as subject


def test_read_system_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        Path, "read_text", Mock(return_value="MemTotal: 4096 kB\nMemAvailable: 1024 kB\n")
    )
    assert subject.read_system_memory() == (4194304, 3145728)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "MemTotal: 1 kB\n",
        "MemAvailable: 1 kB\n",
        "MemTotal: 0 kB\nMemAvailable: 0 kB\n",
        "MemTotal: 1 kB\nMemAvailable: 2 kB\n",
        "MemTotal: -1 kB\nMemAvailable: 0 kB\n",
        "MemTotal: 1 MB\nMemAvailable: 0 kB\n",
    ],
)
def test_unusable_system_memory(monkeypatch: pytest.MonkeyPatch, text: str) -> None:
    monkeypatch.setattr(Path, "read_text", Mock(return_value=text))
    assert subject.read_system_memory() is None


def test_unreadable_system_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=OSError("unavailable")))
    assert subject.read_system_memory() is None


def test_sampler_retains_peak(monkeypatch: pytest.MonkeyPatch) -> None:
    thread = Mock()
    factory = Mock(return_value=thread)
    event = Mock()
    event.wait.side_effect = [False, True]
    monkeypatch.setattr(subject.threading, "Event", Mock(return_value=event))
    monkeypatch.setattr(subject.threading, "Thread", factory)
    monkeypatch.setattr(
        subject, "read_system_memory", Mock(side_effect=[(1000, 200), (1000, 700), (1000, 300)])
    )
    sampler = subject.SystemMemorySampler()
    sampler.start()
    target = factory.call_args.kwargs["target"]
    assert callable(target)
    target()
    assert sampler.finish() == subject.SystemMemorySnapshot(1000, 300, 700, 3, False)
    thread.start.assert_called_once_with()
    thread.join.assert_called_once_with(timeout=1.0)


def test_sampler_wait_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    event = Mock()
    event.wait.side_effect = [False, True]
    monkeypatch.setattr(subject.threading, "Event", Mock(return_value=event))
    factory = Mock()
    monkeypatch.setattr(subject.threading, "Thread", factory)
    monkeypatch.setattr(subject, "read_system_memory", Mock(side_effect=[None, (1000, 100), None]))
    sampler = subject.SystemMemorySampler()
    sampler.start()
    target = factory.call_args.kwargs["target"]
    assert callable(target)
    target()
    assert sampler.finish() == subject.SystemMemorySnapshot(1000, 100, 100, 1, False)
    assert event.wait.call_count == 2
    event.set.assert_called_once_with()


def test_missing_samples_and_thread_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    thread = Mock()
    thread.start.side_effect = RuntimeError("cannot start thread")
    monkeypatch.setattr(subject.threading, "Thread", Mock(return_value=thread))
    monkeypatch.setattr(subject, "read_system_memory", Mock(return_value=None))
    sampler = subject.SystemMemorySampler()
    sampler.start()
    assert sampler.finish() == subject.SystemMemorySnapshot(None, None, None, 0, True)
    thread.join.assert_not_called()
