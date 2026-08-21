from collections.abc import Mapping
from io import StringIO

from pytest import MonkeyPatch

from mcp_stdio_proxy import McpHttpResponse, McpProxyConfig
from run_mcp_stdio_proxy import consume_stdio, main, run_proxy


def unused_ok_poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
    return McpHttpResponse(status=200, text="{}")


def test_run_proxy_writes_config_error() -> None:
    stderr_file = StringIO()
    status = run_proxy(
        env={},
        stdin_chunks=[],
        stdout_file=StringIO(),
        stderr_file=stderr_file,
        poster=unused_ok_poster,
    )
    assert status == 1
    assert stderr_file.getvalue() == (
        "PC_KEIBA_VIEWER_MCP_URL is required (absolute https URL ending with /mcp)\n"
    )


def test_run_proxy_forwards_ndjson() -> None:
    stdout_file = StringIO()

    def poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
        return McpHttpResponse(status=200, text='{"id":1,"jsonrpc":"2.0","result":{}}')

    status = run_proxy(
        env={
            "MCP_AUTH_TOKEN": "mcp-token",
            "PC_KEIBA_VIEWER_MCP_URL": "https://viewer.example.test/mcp",
        },
        stdin_chunks=['{"id":1,"jsonrpc":"2.0","method":"ping"}\n'],
        stdout_file=stdout_file,
        stderr_file=StringIO(),
        poster=poster,
    )
    assert status == 0
    assert stdout_file.getvalue() == (
        'Content-Length: 36\r\n\r\n{"id":1,"jsonrpc":"2.0","result":{}}'
    )


def test_consume_stdio_skips_accepted_replies() -> None:
    stdout_file = StringIO()

    def poster(*, url: str, body: str, headers: Mapping[str, str]) -> McpHttpResponse:
        return McpHttpResponse(status=202, text="")

    consume_stdio(
        config=McpProxyConfig(
            access_client_id=None,
            access_client_secret=None,
            mcp_auth_token="mcp-token",
            mcp_url="https://viewer.example.test/mcp",
        ),
        stdin_chunks=["{}"],
        stdout_file=stdout_file,
        poster=poster,
    )
    assert stdout_file.getvalue() == ""


def test_main_returns_config_error(monkeypatch: MonkeyPatch) -> None:
    stderr_file = StringIO()
    monkeypatch.setattr("run_mcp_stdio_proxy.environ", {})
    monkeypatch.setattr("run_mcp_stdio_proxy.stdin", [])
    monkeypatch.setattr("run_mcp_stdio_proxy.stdout", StringIO())
    monkeypatch.setattr("run_mcp_stdio_proxy.stderr", stderr_file)
    assert main() == 1
    assert stderr_file.getvalue() == (
        "PC_KEIBA_VIEWER_MCP_URL is required (absolute https URL ending with /mcp)\n"
    )
