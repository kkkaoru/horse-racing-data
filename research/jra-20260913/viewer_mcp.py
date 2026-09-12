"""Read authenticated Cloudflare viewer evidence without modifying production."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from urllib.request import Request, urlopen

ORIGIN: str = "https://pc-keiba-viewer.kkk4oru.com/mcp"
TIMEOUT_SECONDS: int = 15
ALLOWED_READ_TOOLS: frozenset[str] = frozenset(
    {
        "get_daily_finish_predictions",
        "get_race_section",
        "get_finish_prediction_summary",
        "get_win_rate_heatmap_display",
        "get_json",
    }
)
ROOT: Path = Path(__file__).resolve().parents[2]
OUTPUT_ROOT: Path = ROOT / "research/jra-20260913"


def read_tool(*, name: str, arguments: Mapping[str, object], token: str) -> bytes:
    """Return the exact MCP response; callers must inspect pagination and tool errors."""
    if name not in ALLOWED_READ_TOOLS:
        raise ValueError("Only explicitly allowlisted read tools are permitted")
    if not token.strip():
        raise ValueError("An existing viewer credential is required")
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": dict(arguments)},
        }
    ).encode("utf-8")
    request = Request(
        ORIGIN,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "horse-racing-data-research/1.0",
        },
        method="POST",
    )
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return response.read()


def decode_page(response: bytes, *, cursor: int) -> tuple[str, int | None, int]:
    """Validate MCP pagination using UTF-16 offsets used by the Worker."""
    envelope = json.loads(response)
    if not isinstance(envelope, dict):
        raise ValueError("MCP response must be an object")
    result = envelope.get("result")
    if not isinstance(result, dict) or result.get("isError") is True:
        raise ValueError("MCP tool returned an error or no result")
    content = result.get("content")
    if not isinstance(content, list) or len(content) != 1:
        raise ValueError("Expected one MCP content item")
    item = content[0]
    if not isinstance(item, dict) or not isinstance(item.get("text"), str):
        raise ValueError("Expected MCP text content")
    text = item["text"]
    payload = json.loads(text)
    if not isinstance(payload, dict) or "dataChunk" not in payload:
        if cursor != 0:
            raise ValueError("Pagination unexpectedly ended without a chunk")
        return text, None, len(text.encode("utf-16-le", errors="surrogatepass")) // 2
    chunk = payload.get("dataChunk")
    total = payload.get("totalCharacters")
    next_cursor = payload.get("nextResponseCursor")
    if not isinstance(chunk, str) or type(total) is not int or total < 0:
        raise ValueError("Invalid chunk or total size")
    end = cursor + len(chunk.encode("utf-16-le", errors="surrogatepass")) // 2
    if payload.get("responseCursor") != cursor or end > total:
        raise ValueError("Pagination offsets do not match the retained response")
    if payload.get("complete") is True:
        if end != total or next_cursor is not None:
            raise ValueError("Incomplete terminal chunk")
    elif type(next_cursor) is not int or next_cursor != end or next_cursor <= cursor:
        raise ValueError("Nonprogressing pagination cursor")
    return chunk, next_cursor, total


def read_complete(*, name: str, arguments: Mapping[str, object], token: str, pages: Path) -> bytes:
    """Retain every page and reject changed sizes rather than accepting partial JSON."""
    pages.mkdir(parents=True, exist_ok=False)
    cursor = 0
    expected_total: int | None = None
    chunks: list[str] = []
    while True:
        params = dict(arguments)
        params["responseCursor"] = cursor
        response = read_tool(name=name, arguments=params, token=token)
        with (pages / f"page-{len(chunks) + 1:03d}.json").open("xb") as stream:
            stream.write(response)
        chunk, next_cursor, total = decode_page(response, cursor=cursor)
        if expected_total is not None and total != expected_total:
            raise ValueError("Cloudflare payload changed while paging")
        expected_total = total
        chunks.append(chunk)
        if next_cursor is None:
            break
        cursor = next_cursor
    text = "".join(chunks).encode("utf-16-le", errors="surrogatepass").decode("utf-16-le")
    json.loads(text)
    return text.encode("utf-8")


def validate_output(path: Path) -> Path:
    """Reject external locations and accidental overwrites before requesting data."""
    resolved = path.resolve()
    if not resolved.is_relative_to(OUTPUT_ROOT.resolve()):
        raise ValueError("Evidence must stay in the campaign directory")
    if resolved.exists():
        raise FileExistsError(resolved)
    return resolved


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tool", choices=sorted(ALLOWED_READ_TOOLS), required=True)
    parser.add_argument("--arguments", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--complete", action="store_true")
    args = parser.parse_args(argv)
    output = validate_output(args.output)
    arguments: object = json.loads(args.arguments)
    if not isinstance(arguments, dict) or not all(isinstance(key, str) for key in arguments):
        raise ValueError("Tool arguments must be a JSON object")
    token = os.environ.get("MCP_AUTH_TOKEN")
    if not isinstance(token, str):
        raise ValueError("The existing MCP_AUTH_TOKEN is unavailable")
    response = (
        read_complete(
            name=args.tool,
            arguments=arguments,
            token=token,
            pages=output.with_suffix(".pages"),
        )
        if args.complete
        else read_tool(name=args.tool, arguments=arguments, token=token)
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(response)
    print(f"Saved Cloudflare response: {output.relative_to(ROOT)} ({len(response)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
