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

from predict_lib.r2_client import (
    R2ObjectIdentity,
    r2_get_bytes,
    r2_get_parquet,
    r2_head_identity,
    r2_head_watermark,
)
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

    def read(self, size: int = -1) -> bytes:
        return self._body if size < 0 else self._body[:size]


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


def test_r2_get_parquet_404_returns_false(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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


def test_r2_get_parquet_silent_without_debug(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("PREDICT_DEBUG_LOGS", raising=False)

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        return _FakeResponse(b"hello")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    r2_get_parquet(_R2, "feat-daybase/jra/20260712/features.parquet", tmp_path / "out.parquet")

    captured = capsys.readouterr()
    assert captured.err == ""


def test_r2_get_bytes_is_bounded_and_handles_404(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _FakeResponse(b"12345"),
    )
    assert r2_get_bytes(_R2, "small", 5) == b"12345"
    assert r2_get_bytes(_R2, "oversized", 4) is None
    assert r2_get_bytes(_R2, "invalid-limit", 0) is None

    def missing(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        raise urllib.error.HTTPError(req.full_url, 404, "Not Found", email.message.Message(), None)

    monkeypatch.setattr(urllib.request, "urlopen", missing)
    assert r2_get_bytes(_R2, "missing", 4) is None


def test_r2_get_bytes_propagates_non_404(monkeypatch: pytest.MonkeyPatch) -> None:
    def failed(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        raise urllib.error.HTTPError(req.full_url, 500, "failed", email.message.Message(), None)

    monkeypatch.setattr(urllib.request, "urlopen", failed)
    with pytest.raises(urllib.error.HTTPError):
        r2_get_bytes(_R2, "failed", 4)


def test_r2_get_parquet_logs_success_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeResponse:
        return _FakeResponse(b"hello")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    r2_get_parquet(_R2, "feat-daybase/jra/20260712/features.parquet", tmp_path / "out.parquet")

    captured = capsys.readouterr()
    assert captured.err == (
        "[r2-client] get ok key=feat-daybase/jra/20260712/features.parquet bytes=5\n"
    )


# ---------------------------------------------------------------------------
# r2_head_watermark (task #32 -- R2 day-base watermark sidecar)
# ---------------------------------------------------------------------------


@final
class _FakeHeadResponse:
    """Minimal context-manager stand-in for a HEAD ``http.client.HTTPResponse``
    -- only ``.headers`` (case-insensitive ``.get``) is ever read, no body."""

    def __init__(self, headers: dict[str, str]) -> None:
        message = email.message.Message()
        for key, value in headers.items():
            message.add_header(key, value)
        self.headers = message

    def __enter__(self) -> _FakeHeadResponse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


_FULL_WATERMARK_HEADERS = {
    "x-amz-meta-max-data-sakusei-nengappi": "20260712",
    "x-amz-meta-row-count": "946",
    "x-amz-meta-rs-predicted-at-max": "2026-07-18T09:00:00",
    "x-amz-meta-rs-row-count": "12",
}


def test_r2_head_identity_normalizes_etag_and_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _FakeHeadResponse(
            {"etag": 'W/"abc123"', "x-amz-version-id": "version-1"}
        ),
    )
    assert r2_head_identity(_R2, "source") == R2ObjectIdentity("abc123", "version-1")


@pytest.mark.parametrize("headers", [{}, {"etag": '""'}])
def test_r2_head_identity_rejects_missing_etag(
    monkeypatch: pytest.MonkeyPatch, headers: dict[str, str]
) -> None:
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda *_args, **_kwargs: _FakeHeadResponse(headers)
    )
    assert r2_head_identity(_R2, "source") is None


def test_r2_head_identity_returns_none_on_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("timeout")),
    )
    assert r2_head_identity(_R2, "source") is None


def test_r2_head_watermark_success_returns_four_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        assert timeout == 30.0
        return _FakeHeadResponse(_FULL_WATERMARK_HEADERS)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "feat-daybase/catalog-v1/jra/20260712/features.parquet")

    assert result == ("20260712", 946, "2026-07-18T09:00:00", 12)


def test_r2_head_watermark_request_method_is_head_no_body_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[urllib.request.Request] = []

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        captured.append(req)
        return _FakeHeadResponse(_FULL_WATERMARK_HEADERS)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    r2_head_watermark(_R2, "some/key")

    assert len(captured) == 1
    assert captured[0].get_method() == "HEAD"


def test_r2_head_watermark_authorization_header_is_sigv4_shaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[urllib.request.Request] = []

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        captured.append(req)
        return _FakeHeadResponse(_FULL_WATERMARK_HEADERS)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    r2_head_watermark(_R2, "feat-daybase/catalog-v1/nar/20260712/features.parquet")

    req = captured[0]
    auth = req.get_header("Authorization")
    assert auth is not None
    assert auth.startswith("AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/")
    assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date" in auth


def test_r2_head_watermark_missing_object_returns_none(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        raise urllib.error.HTTPError(req.full_url, 404, "Not Found", email.message.Message(), None)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "missing/key")

    assert result is None
    assert capsys.readouterr().err == ""


def test_r2_head_watermark_logs_failure_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("PREDICT_DEBUG_LOGS", "1")

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        raise TimeoutError("connect timed out")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "some/key")

    assert result is None
    assert (
        "[r2-client] head failed key=some/key error=TimeoutError('connect timed out')"
        in capsys.readouterr().err
    )


def test_r2_head_watermark_other_exception_returns_none(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        raise TimeoutError("connect timed out")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "some/key")

    assert result is None
    assert capsys.readouterr().err == ""


def test_r2_head_watermark_missing_metadata_header_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incomplete = dict(_FULL_WATERMARK_HEADERS)
    del incomplete["x-amz-meta-rs-row-count"]

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        return _FakeHeadResponse(incomplete)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "some/key")

    assert result is None


def test_r2_head_watermark_no_metadata_at_all_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        return _FakeHeadResponse({})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "some/key")

    assert result is None


def test_r2_head_watermark_malformed_row_count_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed = dict(_FULL_WATERMARK_HEADERS)
    malformed["x-amz-meta-row-count"] = "not-a-number"

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        return _FakeHeadResponse(malformed)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "some/key")

    assert result is None


def test_r2_head_watermark_malformed_rs_row_count_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed = dict(_FULL_WATERMARK_HEADERS)
    malformed["x-amz-meta-rs-row-count"] = "not-a-number"

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        return _FakeHeadResponse(malformed)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "some/key")

    assert result is None


def test_r2_head_watermark_banei_none_rs_token_returns_four_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production Ban-ei sidecar: concat date + count + absent-RS token."""

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _FakeHeadResponse:
        return _FakeHeadResponse(
            {
                "x-amz-meta-max-data-sakusei-nengappi": "20260814",
                "x-amz-meta-row-count": "117",
                "x-amz-meta-rs-predicted-at-max": "none",
                "x-amz-meta-rs-row-count": "0",
            }
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = r2_head_watermark(_R2, "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet")

    assert result == ("20260814", 117, "none", 0)
