"""Validate requests and execute the native JV-Link COM client through Wine."""

from __future__ import annotations

import re
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypedDict

DATA_SPEC_PATTERN: re.Pattern[str] = re.compile(r"^(?:[A-Z0-9]{4})+$")
FROM_TIME_PATTERN: re.Pattern[str] = re.compile(r"^\d{14}(?:-\d{14})?$")
MAX_LIMIT: int = 100
MAX_TIMEOUT_SECONDS: int = 900
WINE_OUTPUT_PREFIX: str = "Z:"
CLIENT_PATH: str = r"C:\JVClient\jvlink-demo.exe"
CHECKPOINT_COMMAND: tuple[str, ...] = ("/opt/jvlink/state.sh", "checkpoint")
ACQUISITION_LOCK: threading.Lock = threading.Lock()


class RequestPayload(TypedDict):
    """JSON shape accepted by the records endpoint."""

    dataSpec: str
    fromTime: str
    limit: int
    timeoutSeconds: int


@dataclass(frozen=True)
class AcquisitionRequest:
    """Validated parameters for one JVOpen call."""

    data_spec: str
    from_time: str
    limit: int
    timeout_seconds: int


class ProcessRunner(Protocol):
    """Callable subprocess boundary used by production and unit tests."""

    def __call__(
        self,
        command: list[str] | tuple[str, ...],
        *,
        check: bool,
        capture_output: bool = False,
        text: bool = False,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]: ...


def parse_request(payload: object) -> AcquisitionRequest:
    """Validate an exact JSON request without coercing external values."""
    if not isinstance(payload, dict):
        raise ValueError("Request body must be a JSON object")
    expected_keys = {"dataSpec", "fromTime", "limit", "timeoutSeconds"}
    if set(payload) != expected_keys:
        raise ValueError("Request body must contain dataSpec, fromTime, limit, and timeoutSeconds")
    data_spec = payload["dataSpec"]
    from_time = payload["fromTime"]
    limit = payload["limit"]
    timeout_seconds = payload["timeoutSeconds"]
    if not isinstance(data_spec, str) or DATA_SPEC_PATTERN.fullmatch(data_spec) is None:
        raise ValueError("dataSpec must contain one or more four-character uppercase IDs")
    if not isinstance(from_time, str) or FROM_TIME_PATTERN.fullmatch(from_time) is None:
        raise ValueError("fromTime must be YYYYMMDDhhmmss or start-end")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    if (
        not isinstance(timeout_seconds, int)
        or isinstance(timeout_seconds, bool)
        or not 1 <= timeout_seconds <= MAX_TIMEOUT_SECONDS
    ):
        raise ValueError(f"timeoutSeconds must be between 1 and {MAX_TIMEOUT_SECONDS}")
    return AcquisitionRequest(data_spec, from_time, limit, timeout_seconds)


def windows_path(path: Path) -> str:
    """Translate an absolute Linux path through Wine's Z: drive."""
    return f"{WINE_OUTPUT_PREFIX}{path}".replace("/", "\\")


def build_command(request: AcquisitionRequest, output: Path) -> list[str]:
    """Build the fixed executable invocation; the service key stays in the environment."""
    return [
        "wine",
        CLIENT_PATH,
        "--data-spec",
        request.data_spec,
        "--from-time",
        request.from_time,
        "--output",
        windows_path(output),
        "--save-path",
        r"C:\JVData",
        "--limit",
        str(request.limit),
        "--timeout",
        str(request.timeout_seconds),
    ]


def acquire_records(request: AcquisitionRequest, runner: ProcessRunner = subprocess.run) -> bytes:
    """Serialize JV-Link access, checkpoint terminal identity, and return UTF-8 records."""
    with ACQUISITION_LOCK, tempfile.TemporaryDirectory(prefix="jvlink-") as directory:
        output = Path(directory) / "records.txt"
        try:
            runner(
                build_command(request, output),
                check=True,
                capture_output=True,
                text=True,
                timeout=request.timeout_seconds + 30,
            )
            return output.read_bytes()
        finally:
            runner(CHECKPOINT_COMMAND, check=True)
