"""Tests for Container readiness responses before Wine initialization completes."""

from __future__ import annotations

import importlib
import sys
from http import HTTPStatus
from pathlib import Path
from unittest.mock import Mock

from pytest import MonkeyPatch

from container import acquisition

sys.modules.setdefault("acquisition", acquisition)
server = importlib.import_module("container.server")


def handler_for(path: str) -> server.Handler:
    """Create a handler without opening a socket."""
    handler = object.__new__(server.Handler)
    handler.path = path
    handler.send_body = Mock()
    handler.send_json_error = Mock()
    return handler


def test_health_reports_starting_then_ready(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    ready_path = tmp_path / "ready"
    monkeypatch.setattr(server, "READY_PATH", ready_path)
    handler = handler_for("/health/container")

    handler.do_GET()
    handler.send_body.assert_called_once_with(
        HTTPStatus.SERVICE_UNAVAILABLE,
        b'{"ok":false}',
        server.JSON_CONTENT_TYPE,
    )

    ready_path.touch()
    handler.send_body.reset_mock()
    handler.do_GET()
    handler.send_body.assert_called_once_with(
        HTTPStatus.OK,
        b'{"ok":true}',
        server.JSON_CONTENT_TYPE,
    )


def test_post_rejects_requests_while_starting(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(server, "READY_PATH", tmp_path / "ready")
    handler = handler_for("/v1/records")

    handler.do_POST()

    handler.send_json_error.assert_called_once_with(
        HTTPStatus.SERVICE_UNAVAILABLE,
        "JV-Link is starting",
    )
