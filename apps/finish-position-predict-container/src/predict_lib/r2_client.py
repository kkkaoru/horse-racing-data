"""Shared R2 SigV4 GET/HEAD helper.

Extracted from ``predict_upcoming.py``'s original ``_r2_get_parquet`` so the
same signed-GET logic can be reused by a second call site — the day-base
freshness resolver (``pipeline_runner.ensure_day_base``) needs to fetch a
category+day day-base parquet from R2 exactly the same way the rescore path
fetches a whole-day feature-parquet cache object. Living under ``predict_lib``
(rather than the coverage-exempt ``predict_upcoming.py``) means this HTTP
signing logic is held to the same ``--cov=predict_lib --cov-fail-under=95``
gate as every other pure/mockable helper in this package — real coverage via
mocking ``urllib.request.urlopen``, not exclusion.

The SigV4 signing here is deliberately minimal (payload_hash of the empty
body, no query-string signing) because it only ever issues unsigned-query GET
or HEAD requests for a known object key. Container R2 tokens are read-only;
object writes go through the Worker FEATURES_CACHE binding, not SigV4 PUT.
"""

from __future__ import annotations

import hashlib
import hmac
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .debug_log import debug_log
from .serve import R2Config

_R2_HOST_SUFFIX: str = "r2.cloudflarestorage.com"
_SIGNING_SERVICE: str = "s3"
_SIGNING_REGION: str = "auto"
_REQUEST_TIMEOUT_SECONDS: float = 30.0
_GET_METHOD: str = "GET"
_HEAD_METHOD: str = "HEAD"


@dataclass(frozen=True)
class R2ObjectIdentity:
    """Stable identity fields exposed by R2's S3-compatible HEAD response."""

    etag: str
    version: str


R2ObjectWatermark = tuple[str, int, str, int]


@dataclass(frozen=True)
class R2ObjectHead:
    """Identity and freshness metadata observed by one atomic HEAD request."""

    identity: R2ObjectIdentity | None
    watermark: R2ObjectWatermark | None


class _HeaderLookup(Protocol):
    def get(self, name: str, default: str | None = None) -> str | None: ...


# Metadata key names as PUT by the Worker (container-ndjson-proxy.ts) --
# surfaced back on GET/HEAD as ``x-amz-meta-{key}`` response headers per the
# S3-compatible API R2 implements. Must stay in sync with that file's own
# ``customMetadata`` object keys.
_WATERMARK_META_MAX_UPDATED: str = "x-amz-meta-max-data-sakusei-nengappi"
_WATERMARK_META_ROW_COUNT: str = "x-amz-meta-row-count"
_WATERMARK_META_RS_PREDICTED_AT_MAX: str = "x-amz-meta-rs-predicted-at-max"
_WATERMARK_META_RS_ROW_COUNT: str = "x-amz-meta-rs-row-count"


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _signing_key(secret_access_key: str, datestamp: str) -> bytes:
    return _sign(
        _sign(
            _sign(
                _sign(f"AWS4{secret_access_key}".encode(), datestamp),
                _SIGNING_REGION,
            ),
            _SIGNING_SERVICE,
        ),
        "aws4_request",
    )


def _build_signed_request(*, r2: R2Config, object_key: str, method: str) -> urllib.request.Request:
    """Build a SigV4-signed unsigned-payload request for one R2 object.

    Only :data:`_GET_METHOD` and :data:`_HEAD_METHOD` are used. The empty-body
    payload hash matches the original GET helper; writes are not signed here.
    """
    payload_hash = hashlib.sha256(b"").hexdigest()
    now = datetime.now(UTC)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    host = f"{r2.account_id}.{_R2_HOST_SUFFIX}"
    url = f"https://{host}/{r2.bucket}/{object_key}"
    header_map: dict[str, str] = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amzdate,
    }
    signed_header_names = sorted(header_map)
    canonical_headers = "".join(f"{name}:{header_map[name]}\n" for name in signed_header_names)
    signed_headers = ";".join(signed_header_names)
    canonical_request = (
        f"{method}\n/{r2.bucket}/{object_key}\n\n{canonical_headers}\n"
        f"{signed_headers}\n{payload_hash}"
    )

    credential_scope = f"{datestamp}/{_SIGNING_REGION}/{_SIGNING_SERVICE}/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{amzdate}\n{credential_scope}\n"
        + hashlib.sha256(canonical_request.encode()).hexdigest()
    )

    signing_key = _signing_key(r2.secret_access_key, datestamp)
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={r2.access_key_id}/{credential_scope},"
        f" SignedHeaders={signed_headers}, Signature={signature}"
    )
    request_headers: dict[str, str] = {
        "Authorization": auth_header,
        "x-amz-date": amzdate,
        "x-amz-content-sha256": payload_hash,
    }
    return urllib.request.Request(
        url,
        data=None,
        method=method,
        headers=request_headers,
    )


def _build_signed_get_request(r2: R2Config, object_key: str) -> urllib.request.Request:
    """Build a SigV4-signed unsigned-payload GET request for one R2 object."""
    return _build_signed_request(r2=r2, object_key=object_key, method=_GET_METHOD)


def r2_get_parquet(r2: R2Config, object_key: str, dest_path: Path) -> bool:
    """Download an R2 object to ``dest_path``.

    Returns ``True`` on success, ``False`` when the object does not exist (HTTP
    404).  Any other error (network, auth) propagates to the caller.

    Args:
        r2:          R2 credentials and bucket name.
        object_key:  R2 object key to download.
        dest_path:   Local destination path (will be created / overwritten).
    """
    req = _build_signed_get_request(r2, object_key)
    try:
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_SECONDS) as resp:
            data = resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(data)
    debug_log(f"[r2-client] get ok key={object_key} bytes={len(data)}")
    return True


def r2_get_bytes(r2: R2Config, object_key: str, max_bytes: int) -> bytes | None:
    """Return a bounded R2 body, or ``None`` when missing/oversized."""
    if max_bytes <= 0:
        return None
    req = _build_signed_get_request(r2, object_key)
    try:
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_SECONDS) as resp:
            data = resp.read(max_bytes + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    return data if len(data) <= max_bytes else None


def _identity_from_headers(headers: _HeaderLookup) -> R2ObjectIdentity | None:
    """Parse an R2 identity from an email/http header mapping."""
    etag_raw = headers.get("etag")
    version = headers.get("x-amz-version-id") or ""
    if etag_raw is None:
        return None
    etag = etag_raw.strip().removeprefix("W/").strip('"')
    if not etag:
        return None
    return R2ObjectIdentity(etag=etag, version=version)


def _watermark_from_headers(headers: _HeaderLookup) -> R2ObjectWatermark | None:
    """Parse custom day-base freshness metadata, failing closed."""
    max_updated = headers.get(_WATERMARK_META_MAX_UPDATED)
    row_count_raw = headers.get(_WATERMARK_META_ROW_COUNT)
    rs_predicted_at_max = headers.get(_WATERMARK_META_RS_PREDICTED_AT_MAX)
    rs_row_count_raw = headers.get(_WATERMARK_META_RS_ROW_COUNT)
    if (
        max_updated is None
        or row_count_raw is None
        or rs_predicted_at_max is None
        or rs_row_count_raw is None
    ):
        return None
    try:
        row_count = int(row_count_raw)
        rs_row_count = int(rs_row_count_raw)
    except ValueError:
        return None
    return (max_updated, row_count, rs_predicted_at_max, rs_row_count)


def r2_head_object(r2: R2Config, object_key: str) -> R2ObjectHead | None:
    """Read identity and freshness metadata with one signed HEAD request."""
    req = _build_signed_request(r2=r2, object_key=object_key, method=_HEAD_METHOD)
    try:
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_SECONDS) as resp:
            headers = resp.headers
    except (TimeoutError, OSError, urllib.error.URLError) as exc:
        debug_log(f"[r2-client] head failed key={object_key} error={exc!r}")
        return None
    return R2ObjectHead(
        identity=_identity_from_headers(headers),
        watermark=_watermark_from_headers(headers),
    )


def r2_head_identity(r2: R2Config, object_key: str) -> R2ObjectIdentity | None:
    """Return the source object's ETag/version, failing closed on ambiguity."""
    result = r2_head_object(r2, object_key)
    return None if result is None else result.identity


def r2_head_watermark(r2: R2Config, object_key: str) -> R2ObjectWatermark | None:
    """Return the day-base watermark from an R2 object's custom metadata via
    a signed HEAD request (no body download), or ``None`` on ANY ambiguity --
    missing object, network/auth failure, or a present object whose custom
    metadata is missing/malformed (e.g. written by a container image from
    before this metadata existed). Fail-closed, matching every other
    watermark helper in this codebase
    (``pipeline_runner._compute_source_watermark`` /
    ``_compute_rs_watermark``) -- the caller must never trust an R2 day-base
    it cannot positively verify as fresh.

    Args:
        r2:         R2 credentials and bucket name.
        object_key: R2 object key (the day-base parquet's own key, per
                    ``pipeline_runner.build_r2_day_base_key``).
    """
    result = r2_head_object(r2, object_key)
    return None if result is None else result.watermark
