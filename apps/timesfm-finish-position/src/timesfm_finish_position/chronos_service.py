"""Authenticated, serial CPU feature service for an isolated research container."""

import hmac
import json
import os
from dataclasses import asdict
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import TypedDict

from timesfm_finish_position.chronos_portable import (
    HistoryPoint,
    PortableArtifact,
    PortableChronosRuntime,
    RunnerHistory,
)

MAX_REQUEST_BYTES = 2_000_000
MAX_RUNNERS = 32
MAX_HISTORY_POINTS = 4096


class ServiceReply(TypedDict):
    production_eligible: bool
    features: list[dict[str, object]]


def parse_runner(value: object) -> RunnerHistory:
    if not isinstance(value, dict):
        raise ValueError("Runner must be an object")
    race_id, horse_id, as_of = (
        value.get("race_id"),
        value.get("horse_id"),
        value.get("evaluation_date"),
    )
    history = value.get("history")
    if not isinstance(race_id, str) or not isinstance(horse_id, str) or not isinstance(as_of, str):
        raise ValueError("Runner identity and evaluation_date strings are required")
    if not isinstance(history, list) or len(history) > MAX_HISTORY_POINTS:
        raise ValueError("Invalid history list")
    points: list[HistoryPoint] = []
    for item in history:
        if not isinstance(item, dict):
            raise ValueError("History point must be an object")
        day, scalar = item.get("race_date"), item.get("value")
        if (
            not isinstance(day, str)
            or isinstance(scalar, bool)
            or not isinstance(scalar, (int, float))
        ):
            raise ValueError("History point requires race_date and numeric value")
        points.append(HistoryPoint(date.fromisoformat(day), float(scalar)))
    return RunnerHistory(race_id, horse_id, date.fromisoformat(as_of), tuple(points))


def predict_request(payload: object, runtime: PortableChronosRuntime) -> ServiceReply:
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("Expected request schema_version 1")
    runners = payload.get("runners")
    if not isinstance(runners, list) or not 1 <= len(runners) <= MAX_RUNNERS:
        raise ValueError("Expected between 1 and 32 runners")
    features = runtime.predict([parse_runner(value) for value in runners])
    return {"production_eligible": False, "features": [asdict(feature) for feature in features]}


def make_handler(runtime: PortableChronosRuntime, token: str) -> type[BaseHTTPRequestHandler]:
    if len(token) < 32:
        raise ValueError("Service token must contain at least 32 characters")

    class Handler(BaseHTTPRequestHandler):
        timeout = 10.0

        def _reply(self, status: int, payload: object) -> None:
            body = json.dumps(payload, allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            if self.path == "/health":
                self._reply(200, {"status": "artifact-verified", "production_eligible": False})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:
            if not hmac.compare_digest(
                self.headers.get("Authorization", "").encode("utf-8"),
                f"Bearer {token}".encode(),
            ):
                self._reply(401, {"error": "unauthorized"})
                return
            if self.path != "/forecast":
                self._reply(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= MAX_REQUEST_BYTES:
                    self._reply(413, {"error": "invalid body length"})
                    return
                payload = json.loads(self.rfile.read(length))
                response = predict_request(payload, runtime)
            except (ValueError, UnicodeDecodeError):
                self._reply(400, {"error": "invalid forecast request"})
                return
            self._reply(200, response)

    return Handler


def main() -> None:
    artifact = PortableArtifact(
        Path(os.environ["CHRONOS_ARTIFACT_DIR"]),
        os.environ["CHRONOS_MODEL_SHA256"],
        os.environ["CHRONOS_CONFIG_SHA256"],
    )
    handler = make_handler(PortableChronosRuntime(artifact), os.environ["CHRONOS_SERVICE_TOKEN"])
    with HTTPServer(("0.0.0.0", 8080), handler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
