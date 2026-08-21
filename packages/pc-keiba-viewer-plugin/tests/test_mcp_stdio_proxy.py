import json
from collections.abc import Mapping
from email.message import Message
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request

from pytest import MonkeyPatch

from mcp_stdio_proxy import (
    McpHttpResponse,
    McpProxyConfig,
    build_forward_headers,
    encode_stdio_message,
    extract_stdio_messages,
    forward_mcp_jsonrpc,
    load_mcp_proxy_config,
    post_mcp_http,
)

PLUGIN_ROOT = Path(__file__).resolve().parents[1]


def test_load_mcp_proxy_config_requires_url() -> None:
    assert load_mcp_proxy_config({}) == (
        "PC_KEIBA_VIEWER_MCP_URL is required (absolute https URL ending with /mcp)"
    )


def test_load_mcp_proxy_config_requires_token() -> None:
    assert load_mcp_proxy_config(
        {"PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/mcp"}
    ) == ("MCP_AUTH_TOKEN is required")


def test_load_mcp_proxy_config_rejects_blank_url() -> None:
    assert load_mcp_proxy_config({"PC_KEIBA_VIEWER_MCP_URL": "   "}) == (
        "PC_KEIBA_VIEWER_MCP_URL is required (absolute https URL ending with /mcp)"
    )


def test_load_mcp_proxy_config_rejects_invalid_url() -> None:
    assert (
        load_mcp_proxy_config(
            {
                "MCP_AUTH_TOKEN": "mcp-token",
                "PC_KEIBA_VIEWER_MCP_URL": "not a url",
            }
        )
        == "PC_KEIBA_VIEWER_MCP_URL is not a valid URL"
    )


def test_load_mcp_proxy_config_rejects_http() -> None:
    assert (
        load_mcp_proxy_config(
            {
                "MCP_AUTH_TOKEN": "mcp-token",
                "PC_KEIBA_VIEWER_MCP_URL": "http://viewer.example.test/mcp",
            }
        )
        == "PC_KEIBA_VIEWER_MCP_URL must be https"
    )


def test_load_mcp_proxy_config_rejects_userinfo() -> None:
    assert (
        load_mcp_proxy_config(
            {
                "MCP_AUTH_TOKEN": "mcp-token",
                "PC_KEIBA_VIEWER_MCP_URL": "https://user:pass@viewer.example.test/mcp",
            }
        )
        == "PC_KEIBA_VIEWER_MCP_URL must not include userinfo"
    )


def test_load_mcp_proxy_config_rejects_fragment() -> None:
    assert (
        load_mcp_proxy_config(
            {
                "MCP_AUTH_TOKEN": "mcp-token",
                "PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/mcp#x",
            }
        )
        == "PC_KEIBA_VIEWER_MCP_URL must not include a fragment"
    )


def test_load_mcp_proxy_config_rejects_non_mcp_path() -> None:
    assert (
        load_mcp_proxy_config(
            {
                "MCP_AUTH_TOKEN": "mcp-token",
                "PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/api",
            }
        )
        == "PC_KEIBA_VIEWER_MCP_URL path must end with /mcp"
    )


def test_load_mcp_proxy_config_rejects_secret_without_id() -> None:
    assert (
        load_mcp_proxy_config(
            {
                "MCP_AUTH_TOKEN": "mcp-token",
                "PC_KEIBA_ACCESS_CLIENT_SECRET": "access-secret",
                "PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/mcp",
            }
        )
        == "PC_KEIBA_ACCESS_CLIENT_ID and PC_KEIBA_ACCESS_CLIENT_SECRET must both be set or both omitted"
    )


def test_load_mcp_proxy_config_rejects_partial_access() -> None:
    assert (
        load_mcp_proxy_config(
            {
                "MCP_AUTH_TOKEN": "mcp-token",
                "PC_KEIBA_ACCESS_CLIENT_ID": "access-id",
                "PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/mcp",
            }
        )
        == "PC_KEIBA_ACCESS_CLIENT_ID and PC_KEIBA_ACCESS_CLIENT_SECRET must both be set or both omitted"
    )


def test_load_mcp_proxy_config_accepts_token_only() -> None:
    assert load_mcp_proxy_config(
        {
            "MCP_AUTH_TOKEN": "mcp-token",
            "PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/mcp",
        }
    ) == McpProxyConfig(
        access_client_id=None,
        access_client_secret=None,
        mcp_auth_token="mcp-token",
        mcp_url="https://viewer.example.test/mcp",
    )


def test_load_mcp_proxy_config_accepts_access_pair() -> None:
    assert load_mcp_proxy_config(
        {
            "MCP_AUTH_TOKEN": "mcp-token",
            "PC_KEIBA_ACCESS_CLIENT_ID": "access-id",
            "PC_KEIBA_ACCESS_CLIENT_SECRET": "access-secret",
            "PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/mcp",
        }
    ) == McpProxyConfig(
        access_client_id="access-id",
        access_client_secret="access-secret",
        mcp_auth_token="mcp-token",
        mcp_url="https://viewer.example.test/mcp",
    )


def test_extract_stdio_messages_reads_newline_delimited_json() -> None:
    extracted = extract_stdio_messages('\n{"id":1}\n\n{"id":2}\n')
    assert extracted.messages == ('{"id":1}', '{"id":2}')
    assert extracted.rest == ""


def test_extract_stdio_messages_keeps_incomplete_ndjson() -> None:
    extracted = extract_stdio_messages('{"id":1}\n{"id":')
    assert extracted.messages == ('{"id":1}',)
    assert extracted.rest == '{"id":'


def test_extract_stdio_messages_reads_content_length_frame() -> None:
    body = '{"id":1}'
    framed = encode_stdio_message(body)
    extracted = extract_stdio_messages(framed)
    assert extracted.messages == ('{"id":1}',)
    assert extracted.rest == ""


def test_extract_stdio_messages_waits_for_content_length_header() -> None:
    extracted = extract_stdio_messages("Content-Length: 4")
    assert extracted.messages == ()
    assert extracted.rest == "Content-Length: 4"


def test_extract_stdio_messages_waits_for_content_length_body() -> None:
    extracted = extract_stdio_messages('Content-Length: 50\r\n\r\n{"id":1}')
    assert extracted.messages == ()
    assert extracted.rest == 'Content-Length: 50\r\n\r\n{"id":1}'


def test_extract_stdio_messages_skips_invalid_content_length() -> None:
    extracted = extract_stdio_messages("Content-Length: no\r\n\r\n")
    assert extracted.messages == ()
    assert extracted.rest == ""


def test_extract_stdio_messages_returns_unframed_rest() -> None:
    extracted = extract_stdio_messages("hello")
    assert extracted.messages == ()
    assert extracted.rest == "hello"


def test_build_forward_headers_omits_access_when_unset() -> None:
    headers = build_forward_headers(
        McpProxyConfig(
            access_client_id=None,
            access_client_secret=None,
            mcp_auth_token="mcp-token",
            mcp_url="https://viewer.example.test/mcp",
        )
    )
    assert headers == {
        "Accept": "application/json",
        "Authorization": "Bearer mcp-token",
        "Content-Type": "application/json",
        "User-Agent": "pc-keiba-viewer-plugin/1.0",
    }


def test_forward_mcp_jsonrpc_posts_access_and_bearer_headers() -> None:
    captured: dict[str, object] = {}

    def poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
        captured["url"] = url
        captured["body"] = body
        captured["headers"] = dict(headers)
        return McpHttpResponse(status=200, text='{"id":1,"jsonrpc":"2.0","result":{}}')

    reply = forward_mcp_jsonrpc(
        config=McpProxyConfig(
            access_client_id="access-id",
            access_client_secret="access-secret",
            mcp_auth_token="mcp-token",
            mcp_url="https://viewer.example.test/mcp",
        ),
        body='{"id":1,"jsonrpc":"2.0","method":"ping"}',
        poster=poster,
    )
    assert captured["url"] == "https://viewer.example.test/mcp"
    assert captured["body"] == '{"id":1,"jsonrpc":"2.0","method":"ping"}'
    assert captured["headers"] == {
        "Accept": "application/json",
        "Authorization": "Bearer mcp-token",
        "CF-Access-Client-Id": "access-id",
        "CF-Access-Client-Secret": "access-secret",
        "Content-Type": "application/json",
        "User-Agent": "pc-keiba-viewer-plugin/1.0",
    }
    assert reply == '{"id":1,"jsonrpc":"2.0","result":{}}'


def test_forward_mcp_jsonrpc_returns_none_for_accepted() -> None:
    def poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
        return McpHttpResponse(status=202, text="")

    reply = forward_mcp_jsonrpc(
        config=McpProxyConfig(
            access_client_id=None,
            access_client_secret=None,
            mcp_auth_token="mcp-token",
            mcp_url="https://viewer.example.test/mcp",
        ),
        body="{}",
        poster=poster,
    )
    assert reply is None


def test_forward_mcp_jsonrpc_returns_none_for_empty_body() -> None:
    def poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
        return McpHttpResponse(status=200, text="  ")

    reply = forward_mcp_jsonrpc(
        config=McpProxyConfig(
            access_client_id=None,
            access_client_secret=None,
            mcp_auth_token="mcp-token",
            mcp_url="https://viewer.example.test/mcp",
        ),
        body="{}",
        poster=poster,
    )
    assert reply is None


def test_forward_mcp_jsonrpc_maps_http_errors() -> None:
    def poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
        return McpHttpResponse(status=401, text="nope")

    reply = forward_mcp_jsonrpc(
        config=McpProxyConfig(
            access_client_id=None,
            access_client_secret=None,
            mcp_auth_token="mcp-token",
            mcp_url="https://viewer.example.test/mcp",
        ),
        body="{}",
        poster=poster,
    )
    assert json.loads(reply if reply is not None else "") == {
        "error": {"code": -32000, "message": "Remote MCP HTTP 401"},
        "id": None,
        "jsonrpc": "2.0",
    }


def test_forward_mcp_jsonrpc_maps_transport_errors() -> None:
    def poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
        raise URLError("offline")

    reply = forward_mcp_jsonrpc(
        config=McpProxyConfig(
            access_client_id=None,
            access_client_secret=None,
            mcp_auth_token="mcp-token",
            mcp_url="https://viewer.example.test/mcp",
        ),
        body="{}",
        poster=poster,
    )
    assert json.loads(reply if reply is not None else "") == {
        "error": {"code": -32000, "message": "Remote MCP transport error: offline"},
        "id": None,
        "jsonrpc": "2.0",
    }


def test_post_mcp_http_reads_success(monkeypatch: MonkeyPatch) -> None:
    class SuccessResponse:
        status: int = 200

        def read(self) -> bytes:
            return b'{"ok":true}'

        def __enter__(self) -> "SuccessResponse":
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    def fake_urlopen(request: Request, timeout: int) -> SuccessResponse:
        assert request.full_url == "https://viewer.example.test/mcp"
        assert timeout == 60
        return SuccessResponse()

    monkeypatch.setattr("mcp_stdio_proxy.urlopen", fake_urlopen)
    response = post_mcp_http(
        url="https://viewer.example.test/mcp",
        body="{}",
        headers={"Authorization": "Bearer mcp-token"},
    )
    assert response == McpHttpResponse(status=200, text='{"ok":true}')


def test_post_mcp_http_reads_http_error(monkeypatch: MonkeyPatch) -> None:
    def fake_urlopen(request: Request, timeout: int) -> object:
        raise HTTPError(
            url="https://viewer.example.test/mcp",
            code=401,
            msg="Unauthorized",
            hdrs=Message(),
            fp=BytesIO(b"nope"),
        )

    monkeypatch.setattr("mcp_stdio_proxy.urlopen", fake_urlopen)
    response = post_mcp_http(
        url="https://viewer.example.test/mcp",
        body="{}",
        headers={"Authorization": "Bearer mcp-token"},
    )
    assert response == McpHttpResponse(status=401, text="nope")


def test_plugin_json_matches_agent_plugins() -> None:
    manifest = json.loads((PLUGIN_ROOT / "plugin.json").read_text(encoding="utf-8"))
    assert manifest == {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "description": (
            "Authenticated MCP access to PC-KEIBA viewer race data, plus the skill that tells agents how to use it. Human Cloudflare Access stays in place; MCP adds a Bearer token. Hostnames and credentials are supplied by the client environment, not this package."
        ),
        "keywords": ["agent-plugins", "mcp", "horse-racing", "pc-keiba", "keiba"],
        "license": "Apache-2.0",
        "name": "pc-keiba-viewer",
        "repository": "https://github.com/kkkaoru/horse-racing-data",
        "version": "1.0.0",
    }


def test_mcp_json_uses_python3_stdio() -> None:
    mcp = json.loads((PLUGIN_ROOT / "mcp.json").read_text(encoding="utf-8"))
    assert mcp == {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json",
        "mcpServers": {
            "pc-keiba-viewer": {
                "args": ["${PLUGIN_ROOT}/src/run_mcp_stdio_proxy.py"],
                "command": "python3",
                "type": "stdio",
            }
        },
    }


def test_prompt_md_tells_agents_to_fetch_and_install() -> None:
    text = (PLUGIN_ROOT / "prompt.md").read_text(encoding="utf-8")
    assert text.find("grok plugin marketplace add kkkaoru/horse-racing-data") != -1
    assert text.find("claude plugin marketplace add kkkaoru/horse-racing-data") != -1
    assert text.find("copilot plugin install pc-keiba-viewer@horse-racing-data") != -1
    assert text.find("codex mcp add pc-keiba-viewer --url MCP_URL") != -1
    assert text.find("PC_KEIBA_VIEWER_MCP_URL") != -1
    assert text.find("Never guess a production hostname") != -1
    assert (
        text.find(
            "raw.githubusercontent.com/kkkaoru/horse-racing-data/main/packages/pc-keiba-viewer-plugin/prompt.md"
        )
        != -1
    )
    assert text.find("python3") != -1
