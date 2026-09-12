"""Test read-only request construction and durable evidence boundaries."""

from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pytest
import viewer_mcp


def test_read_tool_preserves_response(monkeypatch: pytest.MonkeyPatch) -> None:
    request = Mock(return_value=BytesIO(b'{"result": {}}'))
    monkeypatch.setattr(viewer_mcp, "urlopen", request)
    assert (
        viewer_mcp.read_tool(
            name="get_daily_finish_predictions", arguments={"day": "12"}, token="example"
        )
        == b'{"result": {}}'
    )
    assert request.call_args.kwargs == {"timeout": 15}
    assert request.call_args.args[0].full_url == "https://pc-keiba-viewer.kkk4oru.com/mcp"
    assert request.call_args.args[0].get_method() == "POST"
    assert request.call_args.args[0].get_header("Authorization") == "Bearer example"
    assert request.call_args.args[0].get_header("User-agent") == "horse-racing-data-research/1.0"


def test_write_tool_is_rejected() -> None:
    with pytest.raises(ValueError, match="read tools"):
        viewer_mcp.read_tool(name="update_paddock_state", arguments={}, token="example")


def test_blank_credential_is_rejected() -> None:
    with pytest.raises(ValueError, match="credential"):
        viewer_mcp.read_tool(name="get_race_section", arguments={}, token=" ")


def test_external_output_is_rejected() -> None:
    with pytest.raises(ValueError, match="campaign directory"):
        viewer_mcp.validate_output(Path("outside-campaign.json"))


def test_existing_output_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(viewer_mcp, "OUTPUT_ROOT", tmp_path)
    path = tmp_path / "existing.json"
    path.write_text("retained", encoding="utf-8")
    with pytest.raises(FileExistsError):
        viewer_mcp.validate_output(path)
    assert path.read_text(encoding="utf-8") == "retained"


def test_main_saves_without_exposing_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(viewer_mcp, "ROOT", tmp_path)
    monkeypatch.setattr(viewer_mcp, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setenv("MCP_AUTH_TOKEN", "private")
    monkeypatch.setattr(viewer_mcp, "read_tool", Mock(return_value=b'{"result":{}}'))
    assert (
        viewer_mcp.main(
            ["--tool", "get_json", "--arguments", "{}", "--output", str(tmp_path / "saved.json")]
        )
        == 0
    )
    assert (tmp_path / "saved.json").read_bytes() == b'{"result":{}}'
    assert "private" not in capsys.readouterr().out


def test_non_object_arguments_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(viewer_mcp, "OUTPUT_ROOT", tmp_path)
    with pytest.raises(ValueError, match="JSON object"):
        viewer_mcp.main(
            ["--tool", "get_json", "--arguments", "[]", "--output", str(tmp_path / "bad.json")]
        )


def test_missing_credential_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(viewer_mcp, "OUTPUT_ROOT", tmp_path)
    monkeypatch.delenv("MCP_AUTH_TOKEN", raising=False)
    with pytest.raises(ValueError, match="unavailable"):
        viewer_mcp.main(
            ["--tool", "get_json", "--arguments", "{}", "--output", str(tmp_path / "bad.json")]
        )
