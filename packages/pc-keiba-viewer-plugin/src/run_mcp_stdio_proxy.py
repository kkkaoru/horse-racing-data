from collections.abc import Iterable, Mapping
from os import environ
from sys import stderr, stdin, stdout
from typing import TextIO

from mcp_stdio_proxy import (
    McpHttpPoster,
    McpProxyConfig,
    encode_stdio_message,
    extract_stdio_messages,
    forward_mcp_jsonrpc,
    load_mcp_proxy_config,
    post_mcp_http,
)


def consume_stdio(
    *,
    config: McpProxyConfig,
    stdin_chunks: Iterable[str],
    stdout_file: TextIO,
    poster: McpHttpPoster,
) -> None:
    rest = ""
    for chunk in stdin_chunks:
        extracted = extract_stdio_messages(f"{rest}{chunk}")
        rest = extracted.rest
        _write_replies(
            config=config,
            messages=extracted.messages,
            stdout_file=stdout_file,
            poster=poster,
        )


def run_proxy(
    *,
    env: Mapping[str, str],
    stdin_chunks: Iterable[str],
    stdout_file: TextIO,
    stderr_file: TextIO,
    poster: McpHttpPoster,
) -> int:
    loaded = load_mcp_proxy_config(env)
    if isinstance(loaded, str):
        stderr_file.write(f"{loaded}\n")
        return 1
    consume_stdio(
        config=loaded,
        stdin_chunks=stdin_chunks,
        stdout_file=stdout_file,
        poster=poster,
    )
    return 0


def main() -> int:
    return run_proxy(
        env=environ,
        stdin_chunks=stdin,
        stdout_file=stdout,
        stderr_file=stderr,
        poster=post_mcp_http,
    )


def _write_replies(
    *,
    config: McpProxyConfig,
    messages: tuple[str, ...],
    stdout_file: TextIO,
    poster: McpHttpPoster,
) -> None:
    replies = tuple(
        forward_mcp_jsonrpc(config=config, body=message, poster=poster) for message in messages
    )
    encoded = tuple(encode_stdio_message(reply) for reply in replies if reply is not None)
    stdout_file.write("".join(encoded))
    stdout_file.flush()


if __name__ == "__main__":
    raise SystemExit(main())
