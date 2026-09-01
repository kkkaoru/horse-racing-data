"""Run a small JV-Link acquisition through Windows Python hosted by Wine."""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

BUFFER_SIZE = 110_000
DEFAULT_DATA_SPEC = "RACE"
DEFAULT_LIMIT = 20
DEFAULT_TIMEOUT_SECONDS = 300
JST = timezone(timedelta(hours=9), name="JST")
SERVICE_KEY_PATTERN = re.compile(r"^[A-Za-z0-9]{17}$")
JV_ERROR_MESSAGES = {
    -1: "No matching data",
    -100: "The parameter is invalid or could not be saved to the registry",
    -111: "Invalid data specification",
    -114: "Invalid from-time",
    -201: "JVInit has not completed",
    -211: "JV-Link registry settings are invalid",
    -301: "Authentication failed",
    -305: "The current terms of service have not been accepted",
}


@dataclass(frozen=True)
class DemoConfig:
    """Validated inputs for one acquisition."""

    service_key: str
    data_spec: str
    from_time: str
    output_path: Path
    save_path: Path
    limit: int
    timeout_seconds: int


class JvLink(Protocol):
    """Subset of the dynamic JV-Link COM interface used by the demo."""

    def JVInit(self, software_id: str) -> int: ...

    def JVSetServiceKey(self, service_key: str) -> int: ...

    def JVSetSaveFlag(self, save_flag: int) -> int: ...

    def JVSetSavePath(self, save_path: str) -> int: ...

    def JVOpen(
        self,
        data_spec: str,
        from_time: str,
        option: int,
        read_count: int,
        download_count: int,
        last_file_timestamp: str,
    ) -> object: ...

    def JVStatus(self) -> int: ...

    def JVGets(self, buffer: bytearray, buffer_size: int, filename: bytearray) -> object: ...

    def JVCancel(self) -> int: ...

    def JVClose(self) -> int: ...


def default_from_time(now: datetime | None = None) -> str:
    """Return midnight yesterday in the format required by JVOpen."""
    current = now if now is not None else datetime.now(JST)
    return (current - timedelta(days=1)).strftime("%Y%m%d000000")


def parse_args(argv: list[str], environ: dict[str, str]) -> DemoConfig:
    """Parse and validate command-line and environment inputs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-spec", default=DEFAULT_DATA_SPEC)
    parser.add_argument("--from-time", default=default_from_time())
    parser.add_argument("--output", type=Path, default=Path("Z:/data/records.txt"))
    parser.add_argument("--save-path", type=Path, default=Path("C:/JVData"))
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args(argv)

    raw_service_key = environ.get("JRA_VAN_DATALAB_KEY", "")
    service_key = raw_service_key.replace("-", "")
    if (
        re.fullmatch(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*", raw_service_key) is None
        or SERVICE_KEY_PATTERN.fullmatch(service_key) is None
    ):
        parser.error(
            "JRA_VAN_DATALAB_KEY must contain 17 ASCII letters or digits, "
            "optionally separated by hyphens"
        )
    if re.fullmatch(r"(?:[A-Z0-9]{4})+", args.data_spec) is None:
        parser.error("--data-spec must be one or more four-character uppercase IDs")
    if re.fullmatch(r"\d{14}(?:-\d{14})?", args.from_time) is None:
        parser.error("--from-time must be YYYYMMDDhhmmss or start-end")
    if args.limit < 1:
        parser.error("--limit must be positive")
    if args.timeout < 1:
        parser.error("--timeout must be positive")

    return DemoConfig(
        service_key=service_key,
        data_spec=args.data_spec,
        from_time=args.from_time,
        output_path=args.output,
        save_path=args.save_path,
        limit=args.limit,
        timeout_seconds=args.timeout,
    )


def unpack_open_result(result: object) -> tuple[int, int, int, str]:
    """Normalize pywin32's JVOpen result tuple."""
    if not isinstance(result, (tuple, list)) or len(result) != 4:
        raise RuntimeError(f"Unexpected JVOpen result: {result!r}")
    code, read_count, download_count, last_timestamp = result
    return int(code), int(read_count or 0), int(download_count or 0), str(last_timestamp or "")


def unpack_read_result(result: object) -> tuple[int, bytes, str]:
    """Normalize pywin32's JVGets result tuple."""
    if not isinstance(result, (tuple, list)) or len(result) != 3:
        raise RuntimeError(f"Unexpected JVGets result: {result!r}")
    code, contents, filename = result
    if isinstance(contents, memoryview):
        raw = contents.tobytes()
    elif isinstance(contents, bytes):
        raw = contents
    else:
        raise RuntimeError(f"Unexpected JVGets buffer: {type(contents).__name__}")
    return int(code), raw, str(filename or "")


def require_success(operation: str, code: int) -> None:
    """Raise a descriptive error for a negative JV-Link return code."""
    if code >= 0:
        return
    detail = JV_ERROR_MESSAGES.get(code, "See the JV-Link interface specification")
    raise RuntimeError(f"{operation} failed with {code}: {detail}")


def set_service_key(jvlink: JvLink, service_key: str) -> None:
    """Register a key, accepting JV-Link's already-registered result."""
    code = int(jvlink.JVSetServiceKey(service_key))
    if code != -101:
        require_success("JVSetServiceKey", code)


def wait_for_download(jvlink: JvLink, count: int, timeout_seconds: int) -> None:
    """Wait until JV-Link finishes its asynchronous downloads."""
    deadline = time.monotonic() + timeout_seconds
    while count > 0:
        status = int(jvlink.JVStatus())
        require_success("JVStatus", status)
        if status >= count:
            return
        if time.monotonic() >= deadline:
            jvlink.JVCancel()
            raise TimeoutError(f"JV-Link download did not finish within {timeout_seconds} seconds")
        time.sleep(0.25)


def read_records(jvlink: JvLink, output_path: Path, limit: int) -> int:
    """Write up to ``limit`` CP932 JV-Data records to a UTF-8 text file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_path.open("w", encoding="utf-8", newline="") as output:
        while written < limit:
            result = jvlink.JVGets(bytearray(BUFFER_SIZE), BUFFER_SIZE, bytearray())
            code, raw, _filename = unpack_read_result(result)
            if code == 0:
                break
            if code == -1:
                continue
            require_success("JVGets", code)
            output.write(raw[:code].decode("cp932", errors="replace"))
            written += 1
    return written


def run_demo(config: DemoConfig, jvlink: JvLink) -> int:
    """Configure JV-Link, download matching data, and write a small record sample."""
    require_success("JVInit", int(jvlink.JVInit("UNKNOWN")))
    set_service_key(jvlink, config.service_key)
    config.save_path.mkdir(parents=True, exist_ok=True)
    require_success("JVSetSavePath", int(jvlink.JVSetSavePath(str(config.save_path))))
    require_success("JVSetSaveFlag", int(jvlink.JVSetSaveFlag(1)))

    try:
        result = jvlink.JVOpen(config.data_spec, config.from_time, 1, 0, 0, "")
        code, read_count, download_count, last_timestamp = unpack_open_result(result)
        require_success("JVOpen", code)
        wait_for_download(jvlink, download_count, config.timeout_seconds)
        written = read_records(jvlink, config.output_path, config.limit)
        print(
            f"JV-Link OK: files={read_count}, downloads={download_count}, "
            f"records={written}, last={last_timestamp or '-'}"
        )
        print(f"Output: {config.output_path}")
        return written
    finally:
        jvlink.JVClose()


def create_jvlink() -> JvLink:
    """Create the registered JV-Link COM automation object."""
    import win32com.client

    return win32com.client.Dispatch("JVDTLab.JVLink")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    config = parse_args(sys.argv[1:] if argv is None else argv, dict(os.environ))
    run_demo(config, create_jvlink())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
