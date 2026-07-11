"""Tests for the shared R2 SigV4 GET helper (``predict_lib.r2_client``)."""

from __future__ import annotations

import email.message
import hashlib
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import final

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.r2_client import r2_get_parquet
from predict_lib.serve import R2Config

_R2 = R2Config(
    account_id="acct123",
    access_key_id="AKIAEXAMPLE",
    secret_access_key="supersecret",
    bucket="finish-position-cache",
)


@final
class _FakeResponse:
    """Minimal context-manager stand-in for ``http.client.HTTPResponse``."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_r2_get_parquet_success_writes_bytes_and_returns_true(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: list[urllib.request.Request] = []

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        captured.append(req)
        assert timeout == 30.0
        return _FakeResponse(b"PARQUET-BYTES")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    dest = tmp_path / "nested" / "features.parquet"
    result = r2_get_parquet(_R2, "feat-daybase/jra/20260712/features.parquet", dest)

    assert result is True
    assert dest.read_bytes() == b"PARQUET-BYTES"
    assert len(captured) == 1


def test_r2_get_parquet_success_creates_parent_dirs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        return _FakeResponse(b"data")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    dest = tmp_path / "a" / "b" / "c" / "features.parquet"
    assert not dest.parent.exists()
    r2_get_parquet(_R2, "some/key", dest)
    assert dest.parent.exists()


def test_r2_get_parquet_authorization_header_is_sigv4_shaped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: list[urllib.request.Request] = []

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        captured.append(req)
        return _FakeResponse(b"x")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    r2_get_parquet(_R2, "feat-daybase/nar/20260712/features.parquet", tmp_path / "out.parquet")

    req = captured[0]
    auth = req.get_header("Authorization")
    assert auth is not None
    assert auth.startswith("AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/")
    assert "/auto/s3/aws4_request," in auth
    assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date" in auth
    assert "Signature=" in auth
    # x-amz-date and x-amz-content-sha256 headers are also present (case-folded
    # by urllib.request.Request to Title-Case internally).
    assert req.get_header("X-amz-date") is not None
    assert req.get_header("X-amz-content-sha256") == hashlib.sha256(b"").hexdigest()


def test_r2_get_parquet_url_and_host_construction(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: list[urllib.request.Request] = []

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        captured.append(req)
        return _FakeResponse(b"x")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    r2_get_parquet(_R2, "feat-daybase/ban-ei/20260712/features.parquet", tmp_path / "out.parquet")

    req = captured[0]
    assert req.full_url == (
        "https://acct123.r2.cloudflarestorage.com/"
        "finish-position-cache/feat-daybase/ban-ei/20260712/features.parquet"
    )
    assert req.get_method() == "GET"


def test_r2_get_parquet_404_returns_false(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        raise urllib.error.HTTPError(req.full_url, 404, "Not Found", email.message.Message(), None)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    dest = tmp_path / "features.parquet"
    result = r2_get_parquet(_R2, "missing/key", dest)

    assert result is False
    assert not dest.exists()


def test_r2_get_parquet_non_404_http_error_propagates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        raise urllib.error.HTTPError(
            req.full_url, 500, "Internal Server Error", email.message.Message(), None
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        r2_get_parquet(_R2, "some/key", tmp_path / "features.parquet")
    assert exc_info.value.code == 500


def test_r2_get_parquet_other_exception_propagates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        raise TimeoutError("connect timed out")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(TimeoutError):
        r2_get_parquet(_R2, "some/key", tmp_path / "features.parquet")


def test_r2_get_parquet_logs_success_to_stderr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        return _FakeResponse(b"hello")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    r2_get_parquet(_R2, "feat-daybase/jra/20260712/features.parquet", tmp_path / "out.parquet")

    captured = capsys.readouterr()
    assert "r2-client" in captured.err
    assert "feat-daybase/jra/20260712/features.parquet" in captured.err
    assert "bytes=5" in captured.err
