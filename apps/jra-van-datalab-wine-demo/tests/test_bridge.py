from __future__ import annotations

import http.client
import subprocess
import threading
from pathlib import Path

import pytest

from src import bridge


def perform_request(
    server: bridge.BridgeServer,
    path: str,
    token: str | None,
) -> tuple[int, bytes]:
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    connection = http.client.HTTPConnection(*server.server_address, timeout=2)
    headers = {} if token is None else {"Authorization": f"Bearer {token}"}
    connection.request("GET", path, headers=headers)
    response = connection.getresponse()
    status = response.status
    body = response.read()
    connection.close()
    thread.join(timeout=2)
    server.server_close()
    return status, body


def test_parse_request_accepts_valid_values() -> None:
    request = bridge.parse_request(
        "data_spec=RACE&from_time=20260829000000-20260830235959&limit=10&timeout=90"
    )

    assert request == bridge.AcquireRequest(
        data_spec="RACE",
        from_time="20260829000000-20260830235959",
        limit=10,
        timeout_seconds=90,
    )


def test_parse_request_rejects_unknown_parameter() -> None:
    with pytest.raises(ValueError, match="Unknown query parameter: extra"):
        bridge.parse_request(
            "data_spec=RACE&from_time=20260829000000-20260830235959&limit=10&timeout=90&extra=1"
        )


def test_parse_request_rejects_invalid_data_spec() -> None:
    with pytest.raises(ValueError, match="data_spec must contain"):
        bridge.parse_request(
            "data_spec=race&from_time=20260829000000-20260830235959&limit=10&timeout=90"
        )


def test_parse_request_rejects_invalid_from_time() -> None:
    with pytest.raises(ValueError, match="from_time must be"):
        bridge.parse_request("data_spec=RACE&from_time=bad&limit=10&timeout=90")


def test_parse_request_requires_one_limit() -> None:
    with pytest.raises(ValueError, match="Exactly one limit"):
        bridge.parse_request(
            "data_spec=RACE&from_time=20260829000000-20260830235959&limit=1&limit=2&timeout=90"
        )


def test_parse_request_rejects_non_integer_timeout() -> None:
    with pytest.raises(ValueError, match="timeout must be an integer"):
        bridge.parse_request(
            "data_spec=RACE&from_time=20260829000000-20260830235959&limit=10&timeout=slow"
        )


def test_parse_request_rejects_out_of_range_limit() -> None:
    with pytest.raises(ValueError, match="limit must be between 1 and 10000"):
        bridge.parse_request(
            "data_spec=RACE&from_time=20260829000000-20260830235959&limit=0&timeout=90"
        )


def test_acquire_from_native_requires_host_key(tmp_path: Path) -> None:
    request = bridge.AcquireRequest("RACE", "20260829000000-20260830235959", 10, 90)

    with pytest.raises(RuntimeError, match="JRA_VAN_DATALAB_KEY is required"):
        bridge.acquire_from_native(request, tmp_path, {})


def test_acquire_from_native_runs_wrapper_and_reads_records(tmp_path: Path) -> None:
    request = bridge.AcquireRequest("RACE", "20260829000000-20260830235959", 10, 90)
    output = tmp_path / "data" / "native-records.txt"
    output.parent.mkdir()
    output.write_bytes(b"JG-record\n")
    calls: list[tuple[list[str], dict[str, object]]] = []

    def runner(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, "", "")

    records = bridge.acquire_from_native(
        request,
        tmp_path,
        {"JRA_VAN_DATALAB_KEY": "present"},
        runner,
    )

    assert records == b"JG-record\n"
    assert calls[0][0] == [
        str(tmp_path / "scripts" / "run-native-macos.sh"),
        "--data-spec",
        "RACE",
        "--from-time",
        "20260829000000-20260830235959",
        "--limit",
        "10",
        "--timeout",
        "90",
    ]
    assert calls[0][1] == {
        "cwd": tmp_path,
        "env": {"JRA_VAN_DATALAB_KEY": "present"},
        "check": True,
        "capture_output": True,
        "text": True,
        "timeout": 120,
    }


def test_create_server_rejects_short_token() -> None:
    with pytest.raises(ValueError, match="at least 32 characters"):
        bridge.create_server("127.0.0.1", 0, "short", lambda request: b"")


def test_bridge_server_returns_records() -> None:
    token = "a" * 32
    server = bridge.create_server("127.0.0.1", 0, token, lambda request: b"JG-record\n")

    status, body = perform_request(
        server,
        "/records?data_spec=RACE&from_time=20260829000000-20260830235959&limit=10&timeout=90",
        token,
    )

    assert status == 200
    assert body == b"JG-record\n"


def test_bridge_server_rejects_missing_token() -> None:
    server = bridge.create_server("127.0.0.1", 0, "a" * 32, lambda request: b"")

    status, _body = perform_request(server, "/records", None)

    assert status == 401


def test_bridge_server_rejects_unknown_path() -> None:
    token = "a" * 32
    server = bridge.create_server("127.0.0.1", 0, token, lambda request: b"")

    status, _body = perform_request(server, "/unknown", token)

    assert status == 404


def test_bridge_server_rejects_bad_query() -> None:
    token = "a" * 32
    server = bridge.create_server("127.0.0.1", 0, token, lambda request: b"")

    status, body = perform_request(server, "/records?data_spec=bad", token)

    assert status == 400
    assert b"data_spec must contain" in body


def test_bridge_server_reports_acquisition_failure() -> None:
    token = "a" * 32

    def fail(request: bridge.AcquireRequest) -> bytes:
        raise RuntimeError("native failure")

    server = bridge.create_server("127.0.0.1", 0, token, fail)

    status, body = perform_request(
        server,
        "/records?data_spec=RACE&from_time=20260829000000-20260830235959&limit=10&timeout=90",
        token,
    )

    assert status == 502
    assert b"native failure" in body


def test_parse_args_uses_explicit_listener() -> None:
    arguments = bridge.parse_args(["--host", "127.0.0.1", "--port", "12345"])

    assert arguments.host == "127.0.0.1"
    assert arguments.port == 12345


def test_main_serves_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class FakeServer:
        def serve_forever(self) -> None:
            events.append("serve")
            raise KeyboardInterrupt

        def server_close(self) -> None:
            events.append("close")

    monkeypatch.setenv("JRA_VAN_BRIDGE_TOKEN", "a" * 32)
    monkeypatch.setattr(bridge, "create_server", lambda host, port, token, acquire: FakeServer())

    result = bridge.main(["--host", "127.0.0.1", "--port", "12345"])

    assert result == 0
    assert events == ["serve", "close"]
