"""Authenticated HTTP boundary tests; sparse requests never load model weights."""

import hashlib
import http.client
import json
import threading
from collections.abc import Iterator
from http.server import HTTPServer
from pathlib import Path

import pytest

from timesfm_finish_position.chronos_portable import PortableArtifact, PortableChronosRuntime
from timesfm_finish_position.chronos_service import make_handler, parse_runner, predict_request

TOKEN = "test-token-not-secret-01234567890123456789"


@pytest.fixture
def runtime(tmp_path: Path) -> PortableChronosRuntime:
    weights = b"test"
    config = b'{"chronos_config": {}}'
    (tmp_path / "model.safetensors").write_bytes(weights)
    (tmp_path / "config.json").write_bytes(config)
    return PortableChronosRuntime(
        PortableArtifact(
            tmp_path, hashlib.sha256(weights).hexdigest(), hashlib.sha256(config).hexdigest()
        )
    )


@pytest.fixture
def server(runtime: PortableChronosRuntime) -> Iterator[HTTPServer]:
    with HTTPServer(("127.0.0.1", 0), make_handler(runtime, TOKEN)) as instance:
        thread = threading.Thread(target=instance.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            yield instance
        finally:
            instance.shutdown()
            thread.join(timeout=2)


@pytest.mark.parametrize(
    "method,path,authorization,body,status",
    [
        ("GET", "/health", "", "", 200),
        ("GET", "/missing", "", "", 404),
        ("POST", "/forecast", "", "{}", 401),
        ("POST", "/missing", TOKEN, "{}", 404),
        ("POST", "/forecast", TOKEN, "", 413),
        ("POST", "/forecast", TOKEN, "bad-json", 400),
        ("POST", "/forecast", TOKEN, "{}", 400),
        (
            "POST",
            "/forecast",
            TOKEN,
            '{"schema_version":1,"runners":[{"race_id":"r","horse_id":"h","evaluation_date":"2023-01-01","history":[]}]}',
            200,
        ),
    ],
)
def test_http(
    server: HTTPServer, method: str, path: str, authorization: str, body: str, status: int
) -> None:
    connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
    try:
        connection.request(
            method, path, body=body, headers={"Authorization": f"Bearer {authorization}"}
        )
        response = connection.getresponse()
        assert response.status == status
        payload = json.loads(response.read())
        assert isinstance(payload, dict)
        assert response.getheader("Cache-Control") == "no-store"
    finally:
        connection.close()


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"race_id": "r", "horse_id": "h", "evaluation_date": "bad", "history": []},
        {"race_id": "r", "horse_id": "h", "evaluation_date": "2023-01-01", "history": [1]},
        {
            "race_id": "r",
            "horse_id": "h",
            "evaluation_date": "2023-01-01",
            "history": [{"race_date": "2022-01-01", "value": True}],
        },
        {"race_id": "r", "horse_id": "h", "evaluation_date": "2023-01-01", "history": None},
    ],
)
def test_invalid_runner(payload: object) -> None:
    with pytest.raises(ValueError):
        parse_runner(payload)


def test_valid_point_and_bounds(runtime: PortableChronosRuntime) -> None:
    runner = parse_runner(
        {
            "race_id": "r",
            "horse_id": "h",
            "evaluation_date": "2023-01-01",
            "history": [{"race_date": "2022-01-01", "value": 0.5}],
        }
    )
    assert runner.history[0].value == 0.5
    with pytest.raises(ValueError, match="between 1 and 32"):
        predict_request({"schema_version": 1, "runners": []}, runtime)
    with pytest.raises(ValueError, match="at least 32"):
        make_handler(runtime, "short")
