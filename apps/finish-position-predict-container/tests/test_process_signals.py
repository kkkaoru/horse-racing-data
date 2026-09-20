"""Signal-disposition tests without changing the test process's real handlers."""

from collections.abc import Callable
from signal import SIG_DFL, SIG_IGN, SIGTERM
from types import FrameType

import pytest

from predict_lib import process_signals

SignalHandler = Callable[[int, FrameType | None], None] | int | None


def test_restores_default_disposition_after_normal_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    handlers: list[SignalHandler] = []
    signals: list[int] = []

    def register(signum: int, handler: SignalHandler) -> int:
        signals.append(signum)
        handlers.append(handler)
        return SIG_DFL

    monkeypatch.setattr(process_signals, "signal", register)
    with process_signals.exit_on_sigterm():
        assert len(handlers) == 1
        assert callable(handlers[0])
    assert signals == [15, 15]
    assert handlers[1] == 0


def test_sigterm_exits_zero_and_restores_previous_disposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handlers: list[SignalHandler] = []

    def register(_signum: int, handler: SignalHandler) -> int:
        handlers.append(handler)
        return SIG_IGN

    monkeypatch.setattr(process_signals, "signal", register)
    with pytest.raises(SystemExit) as stopped, process_signals.exit_on_sigterm():
        handler = handlers[0]
        assert callable(handler)
        handler(SIGTERM, None)
    assert stopped.value.code == 0
    assert len(handlers) == 2
    assert handlers[1] == 1


def test_restores_handler_when_server_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    handlers: list[SignalHandler] = []

    def register(_signum: int, handler: SignalHandler) -> int:
        handlers.append(handler)
        return SIG_DFL

    monkeypatch.setattr(process_signals, "signal", register)
    with pytest.raises(RuntimeError, match="server failed"), process_signals.exit_on_sigterm():
        raise RuntimeError("server failed")
    assert len(handlers) == 2
    assert handlers[1] == 0
