"""Pagination integrity tests for the Cloudflare evidence collector."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import viewer_mcp


def test_nonpaginated_response() -> None:
    response = b'{"result":{"content":[{"text":"{\\"x\\":1}"}]}}'
    assert viewer_mcp.decode_page(response, cursor=0) == ('{"x":1}', None, 7)


@pytest.mark.parametrize(
    "response",
    [
        b"[]",
        b"{}",
        b'{"result":{"isError":true}}',
        b'{"result":{"content":[]}}',
        b'{"result":{"content":[{}]}}',
        b'{"result":{"content":[{"text":3}]}}',
        b'{"result":{"content":[{"text":"{\\"dataChunk\\":null}"}]}}',
    ],
)
def test_malformed_envelopes_fail(response: bytes) -> None:
    with pytest.raises(ValueError):
        viewer_mcp.decode_page(response, cursor=0)


def test_nonpaginated_continuation_fails() -> None:
    with pytest.raises(ValueError, match="unexpectedly ended"):
        viewer_mcp.decode_page(b'{"result":{"content":[{"text":"{}"}]}}', cursor=1)


@pytest.mark.parametrize(
    "payload",
    [
        {"dataChunk": "{", "totalCharacters": 2, "responseCursor": 1},
        {"dataChunk": "{", "totalCharacters": 0, "responseCursor": 0},
        {
            "dataChunk": "{",
            "totalCharacters": 2,
            "responseCursor": 0,
            "complete": True,
            "nextResponseCursor": None,
        },
        {
            "dataChunk": "{",
            "totalCharacters": 1,
            "responseCursor": 0,
            "complete": True,
            "nextResponseCursor": 1,
        },
        {
            "dataChunk": "{",
            "totalCharacters": 2,
            "responseCursor": 0,
            "complete": False,
            "nextResponseCursor": 0,
        },
        {
            "dataChunk": "{",
            "totalCharacters": 2,
            "responseCursor": 0,
            "complete": False,
            "nextResponseCursor": 2,
        },
        {
            "dataChunk": "{",
            "totalCharacters": 2,
            "responseCursor": 0,
            "complete": False,
            "nextResponseCursor": None,
        },
        {"dataChunk": "{", "totalCharacters": True, "responseCursor": 0},
        {"dataChunk": "{", "totalCharacters": -1, "responseCursor": 0},
    ],
)
def test_invalid_offsets_fail(payload: dict[str, object]) -> None:
    response = json.dumps({"result": {"content": [{"text": json.dumps(payload)}]}}).encode()
    with pytest.raises(ValueError):
        viewer_mcp.decode_page(response, cursor=0)


def test_complete_pages_preserve_raw_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = json.dumps(
        {
            "result": {
                "content": [
                    {
                        "text": json.dumps(
                            {
                                "dataChunk": "{",
                                "totalCharacters": 2,
                                "responseCursor": 0,
                                "complete": False,
                                "nextResponseCursor": 1,
                            }
                        )
                    }
                ]
            }
        }
    ).encode()
    last = json.dumps(
        {
            "result": {
                "content": [
                    {
                        "text": json.dumps(
                            {
                                "dataChunk": "}",
                                "totalCharacters": 2,
                                "responseCursor": 1,
                                "complete": True,
                                "nextResponseCursor": None,
                            }
                        )
                    }
                ]
            }
        }
    ).encode()
    request = Mock(side_effect=[first, last])
    monkeypatch.setattr(viewer_mcp, "read_tool", request)
    assert (
        viewer_mcp.read_complete(
            name="get_json", arguments={}, token="example", pages=tmp_path / "pages"
        )
        == b"{}"
    )
    assert request.call_args.kwargs["arguments"] == {"responseCursor": 1}
    assert (tmp_path / "pages/page-001.json").exists() is True
    assert (tmp_path / "pages/page-002.json").exists() is True


def test_changed_size_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first = json.dumps(
        {
            "result": {
                "content": [
                    {
                        "text": json.dumps(
                            {
                                "dataChunk": "{",
                                "totalCharacters": 2,
                                "responseCursor": 0,
                                "complete": False,
                                "nextResponseCursor": 1,
                            }
                        )
                    }
                ]
            }
        }
    ).encode()
    last = json.dumps(
        {
            "result": {
                "content": [
                    {
                        "text": json.dumps(
                            {
                                "dataChunk": "}",
                                "totalCharacters": 3,
                                "responseCursor": 1,
                                "complete": False,
                                "nextResponseCursor": 2,
                            }
                        )
                    }
                ]
            }
        }
    ).encode()
    monkeypatch.setattr(viewer_mcp, "read_tool", Mock(side_effect=[first, last]))
    with pytest.raises(ValueError, match="changed while paging"):
        viewer_mcp.read_complete(
            name="get_json", arguments={}, token="example", pages=tmp_path / "pages"
        )


def test_utf16_character_count() -> None:
    payload = {
        "dataChunk": '"🐎"',
        "totalCharacters": 4,
        "responseCursor": 0,
        "complete": True,
        "nextResponseCursor": None,
    }
    response = json.dumps({"result": {"content": [{"text": json.dumps(payload)}]}}).encode()
    assert viewer_mcp.decode_page(response, cursor=0) == ('"🐎"', None, 4)


def test_main_complete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(viewer_mcp, "ROOT", tmp_path)
    monkeypatch.setattr(viewer_mcp, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setenv("MCP_AUTH_TOKEN", "example")
    monkeypatch.setattr(viewer_mcp, "read_complete", Mock(return_value=b"{}"))
    assert (
        viewer_mcp.main(
            [
                "--tool",
                "get_json",
                "--arguments",
                "{}",
                "--output",
                str(tmp_path / "out.json"),
                "--complete",
            ]
        )
        == 0
    )
    assert (tmp_path / "out.json").read_bytes() == b"{}"
