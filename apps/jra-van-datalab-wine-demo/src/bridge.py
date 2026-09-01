"""Token-protected host bridge for multi-architecture container clients."""

from __future__ import annotations

import argparse
import hmac
import os
import re
import subprocess
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

DATA_SPEC_PATTERN = re.compile(r"^[A-Z0-9]{1,16}$")
FROM_TIME_PATTERN = re.compile(r"^\d{14}-\d{14}$")
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 56532
MAX_LIMIT = 10_000
MAX_TIMEOUT_SECONDS = 3_600


@dataclass(frozen=True)
class AcquireRequest:
    """Validated acquisition parameters received from a container."""

    data_spec: str
    from_time: str
    limit: int
    timeout_seconds: int


Acquire = Callable[[AcquireRequest], bytes]
Runner = Callable[..., subprocess.CompletedProcess[str]]


def parse_positive(query: Mapping[str, list[str]], name: str, maximum: int) -> int:
    """Parse one bounded positive integer query parameter."""
    values = query.get(name)
    if values is None or len(values) != 1:
        raise ValueError(f"Exactly one {name} value is required")
    try:
        value = int(values[0])
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def parse_request(raw_query: str) -> AcquireRequest:
    """Validate a URL query without accepting unknown or duplicate values."""
    query = parse_qs(raw_query, keep_blank_values=True)
    unknown = set(query) - {"data_spec", "from_time", "limit", "timeout"}
    if unknown:
        raise ValueError(f"Unknown query parameter: {min(unknown)}")
    data_spec = query.get("data_spec")
    from_time = query.get("from_time")
    if data_spec is None or len(data_spec) != 1 or not DATA_SPEC_PATTERN.fullmatch(data_spec[0]):
        raise ValueError("data_spec must contain 1-16 uppercase letters or digits")
    if from_time is None or len(from_time) != 1 or not FROM_TIME_PATTERN.fullmatch(from_time[0]):
        raise ValueError("from_time must be two 14-digit timestamps separated by a hyphen")
    return AcquireRequest(
        data_spec=data_spec[0],
        from_time=from_time[0],
        limit=parse_positive(query, "limit", MAX_LIMIT),
        timeout_seconds=parse_positive(query, "timeout", MAX_TIMEOUT_SECONDS),
    )


def acquire_from_native(
    request: AcquireRequest,
    app_dir: Path,
    environment: Mapping[str, str],
    runner: Runner = subprocess.run,
) -> bytes:
    """Run the authenticated host runtime and return its UTF-8 record file."""
    if not environment.get("JRA_VAN_DATALAB_KEY"):
        raise RuntimeError("JRA_VAN_DATALAB_KEY is required by the host bridge")
    output = app_dir / "data" / "native-records.txt"
    command = [
        str(app_dir / "scripts" / "run-native-macos.sh"),
        "--data-spec",
        request.data_spec,
        "--from-time",
        request.from_time,
        "--limit",
        str(request.limit),
        "--timeout",
        str(request.timeout_seconds),
    ]
    runner(
        command,
        cwd=app_dir,
        env=dict(environment),
        check=True,
        capture_output=True,
        text=True,
        timeout=request.timeout_seconds + 30,
    )
    return output.read_bytes()


class BridgeServer(ThreadingHTTPServer):
    """HTTP server carrying its token and acquisition callback."""

    def __init__(
        self,
        address: tuple[str, int],
        token: str,
        acquire: Acquire,
    ) -> None:
        self.token = token
        self.acquire = acquire
        self.acquire_lock = threading.Lock()
        super().__init__(address, BridgeHandler)


class BridgeHandler(BaseHTTPRequestHandler):
    """Serve one authenticated records endpoint."""

    server: BridgeServer

    def do_GET(self) -> None:
        """Validate the request, run JV-Link, and return UTF-8 records."""
        authorization = self.headers.get("Authorization", "")
        if not hmac.compare_digest(authorization, f"Bearer {self.server.token}"):
            self.send_error(HTTPStatus.UNAUTHORIZED)
            return
        parsed_url = urlsplit(self.path)
        if parsed_url.path != "/records":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            request = parse_request(parsed_url.query)
            with self.server.acquire_lock:
                records = self.server.acquire(request)
        except ValueError as error:
            self.send_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            self.send_error(HTTPStatus.BAD_GATEWAY, str(error))
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(records)))
        self.end_headers()
        self.wfile.write(records)

    def log_message(self, format: str, *args: object) -> None:
        """Keep credentials and query strings out of default access logs."""


def create_server(host: str, port: int, token: str, acquire: Acquire) -> BridgeServer:
    """Create a configured bridge server after validating its secret."""
    if len(token) < 32:
        raise ValueError("JRA_VAN_BRIDGE_TOKEN must contain at least 32 characters")
    return BridgeServer((host, port), token, acquire)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """Parse bridge listener options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the bridge until interrupted."""
    arguments = parse_args([] if argv is None else argv)
    token = os.environ.get("JRA_VAN_BRIDGE_TOKEN", "")
    app_dir = Path(__file__).resolve().parents[1]
    server = create_server(
        arguments.host,
        arguments.port,
        token,
        lambda request: acquire_from_native(request, app_dir, os.environ),
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))
