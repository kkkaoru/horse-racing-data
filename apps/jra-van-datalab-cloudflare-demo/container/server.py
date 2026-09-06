"""Private HTTP server behind the authenticated Cloudflare Worker."""

from __future__ import annotations

import json
import subprocess
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from acquisition import acquire_records, parse_request

HOST: str = "0.0.0.0"
PORT: int = 8080
READY_PATH: Path = Path("/tmp/jvlink-ready")
MAX_BODY_BYTES: int = 4096
JSON_CONTENT_TYPE: str = "application/json; charset=utf-8"
TEXT_CONTENT_TYPE: str = "text/plain; charset=utf-8"
GUI_COMMAND: tuple[str, ...] = ("wine", r"C:\Program Files\JRA-VAN\Data Lab\JV-Link.exe")
CHECKPOINT_COMMAND: tuple[str, ...] = ("/opt/jvlink/state.sh", "checkpoint")


class Handler(BaseHTTPRequestHandler):
    """Handle health, records, UI launch, and explicit state checkpoints."""

    def send_body(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        """Write one bounded response."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json_error(self, status: HTTPStatus, message: str) -> None:
        """Return a stable JSON error without exposing process environment values."""
        self.send_body(status, json.dumps({"error": message}).encode(), JSON_CONTENT_TYPE)

    def do_GET(self) -> None:
        """Serve container health; nginx owns bootstrap assets."""
        if self.path != "/health/container":
            self.send_json_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        if not READY_PATH.is_file():
            self.send_body(HTTPStatus.SERVICE_UNAVAILABLE, b'{"ok":false}', JSON_CONTENT_TYPE)
            return
        self.send_body(HTTPStatus.OK, b'{"ok":true}', JSON_CONTENT_TYPE)

    def do_POST(self) -> None:
        """Dispatch the three mutation endpoints after JV-Link is ready."""
        if not READY_PATH.is_file():
            self.send_json_error(HTTPStatus.SERVICE_UNAVAILABLE, "JV-Link is starting")
            return
        actions = {
            "/v1/records": self.acquire,
            "/v1/checkpoint": self.checkpoint,
            "/bootstrap/start": self.start_gui,
        }
        action = actions.get(self.path)
        if action is None:
            self.send_json_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        action()

    def acquire(self) -> None:
        """Read and validate a small JSON body, then run JV-Link."""
        raw_length = self.headers.get("Content-Length")
        if raw_length is None or not raw_length.isdigit():
            self.send_json_error(HTTPStatus.LENGTH_REQUIRED, "Content-Length is required")
            return
        length = int(raw_length)
        if length < 1 or length > MAX_BODY_BYTES:
            self.send_json_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request body is too large")
            return
        try:
            payload = json.loads(self.rfile.read(length))
            records = acquire_records(parse_request(payload))
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
            self.send_json_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        except (OSError, subprocess.SubprocessError) as error:
            self.send_json_error(HTTPStatus.BAD_GATEWAY, str(error))
            return
        self.send_body(HTTPStatus.OK, records, TEXT_CONTENT_TYPE)

    def checkpoint(self) -> None:
        """Persist the Wine terminal after the operator closes the setup UI."""
        try:
            subprocess.run(CHECKPOINT_COMMAND, check=True)
        except subprocess.SubprocessError as error:
            self.send_json_error(HTTPStatus.BAD_GATEWAY, str(error))
            return
        self.send_body(HTTPStatus.OK, b'{"ok":true}', JSON_CONTENT_TYPE)

    def start_gui(self) -> None:
        """Launch the official configuration UI on the private X display."""
        try:
            subprocess.Popen(GUI_COMMAND, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError as error:
            self.send_json_error(HTTPStatus.BAD_GATEWAY, str(error))
            return
        self.send_body(HTTPStatus.ACCEPTED, b'{"ok":true}', JSON_CONTENT_TYPE)

    def log_message(self, format: str, *args: object) -> None:
        """Avoid logging paths or request bodies that identify acquisition queries."""


def main() -> None:
    """Run until the container lifecycle sends a termination signal."""
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
