import json
from collections.abc import Mapping
from dataclasses import dataclass
from http.client import HTTPResponse
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

HTTPS_SCHEME: str = "https"
USER_AGENT: str = "pc-keiba-viewer-plugin/1.0"
CONTENT_LENGTH_HEADER: str = "Content-Length:"
HEADER_BODY_SEPARATOR: str = "\r\n\r\n"
HEADER_BODY_SEPARATOR_LENGTH: int = 4
ACCEPTED_STATUS: int = 202
JSON_RPC_HTTP_ERROR_CODE: int = -32000
HTTP_TIMEOUT_SECONDS: int = 60
UTF8: str = "utf-8"


@dataclass(frozen=True)
class McpProxyConfig:
    access_client_id: str | None
    access_client_secret: str | None
    mcp_auth_token: str
    mcp_url: str


@dataclass(frozen=True)
class ExtractedStdio:
    messages: tuple[str, ...]
    rest: str


@dataclass(frozen=True)
class McpHttpResponse:
    status: int
    text: str


class McpHttpPoster(Protocol):
    def __call__(self, *, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse: ...


def load_mcp_proxy_config(env: Mapping[str, str]) -> McpProxyConfig | str:
    mcp_url = _trimmed_env(env, "PC_KEIBA_VIEWER_MCP_URL")
    mcp_auth_token = _trimmed_env(env, "MCP_AUTH_TOKEN")
    access_client_id = _trimmed_env(env, "PC_KEIBA_ACCESS_CLIENT_ID")
    access_client_secret = _trimmed_env(env, "PC_KEIBA_ACCESS_CLIENT_SECRET")
    if mcp_url is None:
        return "PC_KEIBA_VIEWER_MCP_URL is required (absolute https URL ending with /mcp)"
    if mcp_auth_token is None:
        return "MCP_AUTH_TOKEN is required"
    url_error = _validate_mcp_url(mcp_url)
    if url_error is not None:
        return url_error
    if access_client_id is None and access_client_secret is None:
        return McpProxyConfig(
            access_client_id=None,
            access_client_secret=None,
            mcp_auth_token=mcp_auth_token,
            mcp_url=mcp_url,
        )
    if access_client_id is None or access_client_secret is None:
        return "PC_KEIBA_ACCESS_CLIENT_ID and PC_KEIBA_ACCESS_CLIENT_SECRET must both be set or both omitted"
    return McpProxyConfig(
        access_client_id=access_client_id,
        access_client_secret=access_client_secret,
        mcp_auth_token=mcp_auth_token,
        mcp_url=mcp_url,
    )


def extract_stdio_messages(buffer: str) -> ExtractedStdio:
    stripped = buffer.lstrip()
    if stripped != buffer:
        return extract_stdio_messages(stripped)
    if buffer == "":
        return ExtractedStdio(messages=(), rest="")
    if buffer.startswith("{"):
        return _extract_ndjson_message(buffer)
    return _extract_content_length_message(buffer)


def encode_stdio_message(body: str) -> str:
    size = len(body.encode(UTF8))
    return f"{CONTENT_LENGTH_HEADER} {size}{HEADER_BODY_SEPARATOR}{body}"


def build_forward_headers(config: McpProxyConfig) -> dict[str, str]:
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.mcp_auth_token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    if config.access_client_id is None or config.access_client_secret is None:
        return headers
    headers["CF-Access-Client-Id"] = config.access_client_id
    headers["CF-Access-Client-Secret"] = config.access_client_secret
    return headers


def forward_mcp_jsonrpc(
    *,
    config: McpProxyConfig,
    body: str,
    poster: McpHttpPoster,
) -> str | None:
    try:
        response = poster(
            url=config.mcp_url,
            body=body,
            headers=build_forward_headers(config),
        )
    except URLError as error:
        return _json_rpc_http_error(f"Remote MCP transport error: {error.reason}")
    if response.status == ACCEPTED_STATUS:
        return None
    if response.status < 200 or response.status >= 300:
        return _json_rpc_http_error(f"Remote MCP HTTP {response.status}")
    if response.text.strip() == "":
        return None
    return response.text


def post_mcp_http(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
    request = Request(
        url,
        data=body.encode(UTF8),
        headers=dict(headers),
        method="POST",
    )
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            return _read_http_response(response)
    except HTTPError as error:
        return McpHttpResponse(status=error.code, text=error.read().decode(UTF8))


def _trimmed_env(env: Mapping[str, str], key: str) -> str | None:
    if key not in env:
        return None
    trimmed = env[key].strip()
    if trimmed == "":
        return None
    return trimmed


def _validate_mcp_url(mcp_url: str) -> str | None:
    parsed = urlparse(mcp_url)
    if parsed.scheme == "" or parsed.netloc == "":
        return "PC_KEIBA_VIEWER_MCP_URL is not a valid URL"
    if parsed.scheme != HTTPS_SCHEME:
        return "PC_KEIBA_VIEWER_MCP_URL must be https"
    if parsed.username is not None or parsed.password is not None:
        return "PC_KEIBA_VIEWER_MCP_URL must not include userinfo"
    if parsed.fragment != "":
        return "PC_KEIBA_VIEWER_MCP_URL must not include a fragment"
    if not parsed.path.endswith("/mcp"):
        return "PC_KEIBA_VIEWER_MCP_URL path must end with /mcp"
    return None


def _extract_ndjson_message(buffer: str) -> ExtractedStdio:
    newline_index = buffer.find("\n")
    if newline_index < 0:
        return ExtractedStdio(messages=(), rest=buffer)
    line = buffer[:newline_index].strip()
    following = extract_stdio_messages(buffer[newline_index + 1 :])
    return ExtractedStdio(messages=(line, *following.messages), rest=following.rest)


def _extract_content_length_message(buffer: str) -> ExtractedStdio:
    header_index = buffer.find(CONTENT_LENGTH_HEADER)
    if header_index < 0:
        return ExtractedStdio(messages=(), rest=buffer)
    after_header = buffer[header_index + len(CONTENT_LENGTH_HEADER) :]
    header_end = after_header.find(HEADER_BODY_SEPARATOR)
    if header_end < 0:
        return ExtractedStdio(messages=(), rest=buffer)
    line_end = after_header.find("\r\n")
    length_text = after_header[:line_end].strip()
    if not length_text.isdigit():
        return extract_stdio_messages(after_header[header_end + HEADER_BODY_SEPARATOR_LENGTH :])
    body_length = int(length_text)
    body_start = (
        header_index + len(CONTENT_LENGTH_HEADER) + header_end + HEADER_BODY_SEPARATOR_LENGTH
    )
    if len(buffer) < body_start + body_length:
        return ExtractedStdio(messages=(), rest=buffer)
    message = buffer[body_start : body_start + body_length]
    following = extract_stdio_messages(buffer[body_start + body_length :])
    return ExtractedStdio(messages=(message, *following.messages), rest=following.rest)


def _json_rpc_http_error(message: str) -> str:
    return json.dumps(
        {
            "error": {"code": JSON_RPC_HTTP_ERROR_CODE, "message": message},
            "id": None,
            "jsonrpc": "2.0",
        },
        separators=(",", ":"),
    )


def _read_http_response(response: HTTPResponse) -> McpHttpResponse:
    return McpHttpResponse(status=response.status, text=response.read().decode(UTF8))
