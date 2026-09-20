"""Honor the Container runtime's SIGTERM even when the HTTP server is PID 1."""

from collections.abc import Generator
from contextlib import contextmanager
from signal import SIGTERM, signal
from types import FrameType
from typing import NoReturn


def _exit_on_sigterm(_signum: int, _frame: FrameType | None) -> NoReturn:
    # Linux PID 1 ignores default-disposition SIGTERM. An explicit handler lets
    # the existing HTTP-server context close its socket and exit normally.
    raise SystemExit(0)


@contextmanager
def exit_on_sigterm() -> Generator[None, None, None]:
    """Install only in the main HTTP-server thread, restoring prior disposition.

    Do not call HTTPServer.shutdown() from its own serve_forever() thread: that
    waits for the same thread and deadlocks. Unwinding the server context instead
    closes the listener; its daemon request threads do not hold process exit.
    """
    previous = signal(SIGTERM, _exit_on_sigterm)
    try:
        yield
    finally:
        signal(SIGTERM, previous)
