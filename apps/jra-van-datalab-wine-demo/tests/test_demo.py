"""Tests for the Wine-hosted JV-Link demo orchestration."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import demo


class FakeJvLink:
    """Small deterministic COM test double."""

    def __init__(self) -> None:
        self.statuses = [1]
        self.read_results: list[object] = [
            (4, memoryview("RAあ".encode("cp932")), "race.jvd"),
            (-1, b"", "race.jvd"),
            (0, b"", ""),
        ]
        self.cancelled = False
        self.closed = False

    def JVInit(self, software_id: str) -> int:
        return 0

    def JVSetServiceKey(self, service_key: str) -> int:
        return 0

    def JVSetSaveFlag(self, save_flag: int) -> int:
        return 0

    def JVSetSavePath(self, save_path: str) -> int:
        return 0

    def JVOpen(
        self,
        data_spec: str,
        from_time: str,
        option: int,
        read_count: int,
        download_count: int,
        last_file_timestamp: str,
    ) -> object:
        return (0, 2, 1, "20260713093000")

    def JVStatus(self) -> int:
        return self.statuses.pop(0)

    def JVGets(self, buffer: bytearray, buffer_size: int, filename: bytearray) -> object:
        return self.read_results.pop(0)

    def JVCancel(self) -> int:
        self.cancelled = True
        return 0

    def JVClose(self) -> int:
        self.closed = True
        return 0


def test_default_from_time_uses_previous_day() -> None:
    now = datetime(2026, 7, 14, 12, 30)

    assert demo.default_from_time(now) == "20260713000000"


def test_parse_args_builds_config(tmp_path: Path) -> None:
    config = demo.parse_args(
        [
            "--data-spec",
            "RACEDIFF",
            "--from-time",
            "20260713000000-20260713235959",
            "--output",
            str(tmp_path / "records.txt"),
            "--save-path",
            str(tmp_path / "cache"),
            "--limit",
            "3",
            "--timeout",
            "9",
        ],
        {"JRA_VAN_DATALAB_KEY": "ABC12345678901234"},
    )

    assert config == demo.DemoConfig(
        service_key="ABC12345678901234",
        data_spec="RACEDIFF",
        from_time="20260713000000-20260713235959",
        output_path=tmp_path / "records.txt",
        save_path=tmp_path / "cache",
        limit=3,
        timeout_seconds=9,
    )


def test_parse_args_normalizes_hyphenated_key() -> None:
    config = demo.parse_args([], {"JRA_VAN_DATALAB_KEY": "ABC1-2345-6789-0123-4"})

    assert config.service_key == "ABC12345678901234"


def test_parse_args_rejects_invalid_key() -> None:
    with pytest.raises(SystemExit):
        demo.parse_args([], {"JRA_VAN_DATALAB_KEY": "short"})


def test_parse_args_rejects_invalid_data_spec() -> None:
    with pytest.raises(SystemExit):
        demo.parse_args(["--data-spec", "race"], {"JRA_VAN_DATALAB_KEY": "ABC12345678901234"})


def test_parse_args_rejects_invalid_time() -> None:
    with pytest.raises(SystemExit):
        demo.parse_args(["--from-time", "yesterday"], {"JRA_VAN_DATALAB_KEY": "ABC12345678901234"})


def test_parse_args_rejects_nonpositive_limit() -> None:
    with pytest.raises(SystemExit):
        demo.parse_args(["--limit", "0"], {"JRA_VAN_DATALAB_KEY": "ABC12345678901234"})


def test_parse_args_rejects_nonpositive_timeout() -> None:
    with pytest.raises(SystemExit):
        demo.parse_args(["--timeout", "0"], {"JRA_VAN_DATALAB_KEY": "ABC12345678901234"})


def test_unpack_open_result_normalizes_values() -> None:
    assert demo.unpack_open_result([0, None, 2, None]) == (0, 0, 2, "")


def test_unpack_open_result_rejects_unexpected_value() -> None:
    with pytest.raises(RuntimeError, match="Unexpected JVOpen result"):
        demo.unpack_open_result(0)


def test_unpack_read_result_accepts_bytes() -> None:
    assert demo.unpack_read_result((2, b"RA", None)) == (2, b"RA", "")


def test_unpack_read_result_rejects_shape() -> None:
    with pytest.raises(RuntimeError, match="Unexpected JVGets result"):
        demo.unpack_read_result((0, b""))


def test_unpack_read_result_rejects_buffer_type() -> None:
    with pytest.raises(RuntimeError, match="Unexpected JVGets buffer: str"):
        demo.unpack_read_result((1, "x", "file"))


def test_require_success_describes_known_and_unknown_errors() -> None:
    demo.require_success("JVInit", 0)
    with pytest.raises(RuntimeError, match="Authentication failed"):
        demo.require_success("JVOpen", -301)
    with pytest.raises(RuntimeError, match="parameter is invalid"):
        demo.require_success("JVSetServiceKey", -100)
    with pytest.raises(RuntimeError, match="See the JV-Link interface specification"):
        demo.require_success("JVOpen", -999)


def test_set_service_key_accepts_existing_and_rejects_other_errors() -> None:
    jvlink = FakeJvLink()
    jvlink.JVSetServiceKey = lambda _key: -101
    demo.set_service_key(jvlink, "ABC12345678901234")

    jvlink.JVSetServiceKey = lambda _key: -100
    with pytest.raises(RuntimeError, match="parameter is invalid"):
        demo.set_service_key(jvlink, "ABC12345678901234")


def test_wait_for_download_returns_after_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    jvlink = FakeJvLink()
    jvlink.statuses = [0, 1]
    monkeypatch.setattr(demo.time, "sleep", lambda _seconds: None)

    demo.wait_for_download(jvlink, 1, 5)


def test_wait_for_download_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    jvlink = FakeJvLink()
    jvlink.statuses = [0]
    moments = iter([1.0, 2.0])
    monkeypatch.setattr(demo.time, "monotonic", lambda: next(moments))

    with pytest.raises(TimeoutError, match="within 0 seconds"):
        demo.wait_for_download(jvlink, 1, 0)
    assert jvlink.cancelled is True


def test_read_records_writes_utf8_and_honors_limit(tmp_path: Path) -> None:
    jvlink = FakeJvLink()
    jvlink.read_results = [(4, memoryview("RAあ".encode("cp932")), "race.jvd")]
    output = tmp_path / "nested" / "records.txt"

    assert demo.read_records(jvlink, output, 1) == 1
    assert output.read_text(encoding="utf-8") == "RAあ"


def test_read_records_skips_file_boundary_and_stops_at_eof(tmp_path: Path) -> None:
    jvlink = FakeJvLink()
    output = tmp_path / "records.txt"

    assert demo.read_records(jvlink, output, 10) == 1
    assert output.read_text(encoding="utf-8") == "RAあ"


def test_run_demo_downloads_and_closes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    jvlink = FakeJvLink()
    config = demo.DemoConfig(
        service_key="ABC12345678901234",
        data_spec="RACE",
        from_time="20260713000000",
        output_path=tmp_path / "records.txt",
        save_path=tmp_path / "cache",
        limit=2,
        timeout_seconds=5,
    )

    assert demo.run_demo(config, jvlink) == 1
    assert jvlink.closed is True
    assert (
        "JV-Link OK: files=2, downloads=1, records=1, last=20260713093000"
        in capsys.readouterr().out
    )


def test_run_demo_closes_when_open_fails(tmp_path: Path) -> None:
    jvlink = FakeJvLink()
    jvlink.JVOpen = lambda *_args: (-301, 0, 0, "")
    config = demo.DemoConfig(
        service_key="ABC12345678901234",
        data_spec="RACE",
        from_time="20260713000000",
        output_path=tmp_path / "records.txt",
        save_path=tmp_path / "cache",
        limit=1,
        timeout_seconds=1,
    )

    with pytest.raises(RuntimeError, match="Authentication failed"):
        demo.run_demo(config, jvlink)
    assert jvlink.closed is True


def test_create_jvlink_dispatches_registered_com_object(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = FakeJvLink()
    client = SimpleNamespace(Dispatch=lambda name: expected)
    monkeypatch.setitem(sys.modules, "win32com", SimpleNamespace(client=client))
    monkeypatch.setitem(sys.modules, "win32com.client", client)

    assert demo.create_jvlink() is expected


def test_main_runs_with_dependencies_replaced(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    key = "ABC12345678901234"
    monkeypatch.setenv("JRA_VAN_DATALAB_KEY", key)
    jvlink = FakeJvLink()
    monkeypatch.setattr(demo, "create_jvlink", lambda: jvlink)

    assert (
        demo.main(
            [
                "--output",
                str(tmp_path / "records.txt"),
                "--save-path",
                str(tmp_path / "cache"),
            ]
        )
        == 0
    )
