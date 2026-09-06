"""Unit tests for the container's validated JV-Link process boundary."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from container.acquisition import (
    AcquisitionRequest,
    acquire_records,
    build_command,
    parse_request,
    windows_path,
)


def test_parse_request_accepts_a_valid_range() -> None:
    result = parse_request(
        {
            "dataSpec": "RACE",
            "fromTime": "20260829000000-20260830235959",
            "limit": 10,
            "timeoutSeconds": 90,
        }
    )

    assert result == AcquisitionRequest(
        data_spec="RACE",
        from_time="20260829000000-20260830235959",
        limit=10,
        timeout_seconds=90,
    )


def test_parse_request_rejects_a_non_object() -> None:
    with pytest.raises(ValueError, match="Request body must be a JSON object"):
        parse_request([])


def test_parse_request_rejects_missing_or_unknown_keys() -> None:
    with pytest.raises(ValueError, match="Request body must contain"):
        parse_request(
            {
                "dataSpec": "RACE",
                "fromTime": "20260829000000",
                "limit": 10,
                "unexpected": 90,
            }
        )


def test_parse_request_rejects_invalid_data_spec() -> None:
    with pytest.raises(ValueError, match="dataSpec must contain"):
        parse_request(
            {
                "dataSpec": "race",
                "fromTime": "20260829000000",
                "limit": 10,
                "timeoutSeconds": 90,
            }
        )


def test_parse_request_rejects_non_string_data_spec() -> None:
    with pytest.raises(ValueError, match="dataSpec must contain"):
        parse_request(
            {
                "dataSpec": 1234,
                "fromTime": "20260829000000",
                "limit": 10,
                "timeoutSeconds": 90,
            }
        )


def test_parse_request_rejects_invalid_from_time() -> None:
    with pytest.raises(ValueError, match="fromTime must be"):
        parse_request(
            {
                "dataSpec": "RACE",
                "fromTime": "2026-08-29",
                "limit": 10,
                "timeoutSeconds": 90,
            }
        )


def test_parse_request_rejects_non_string_from_time() -> None:
    with pytest.raises(ValueError, match="fromTime must be"):
        parse_request(
            {
                "dataSpec": "RACE",
                "fromTime": 20260829000000,
                "limit": 10,
                "timeoutSeconds": 90,
            }
        )


def test_parse_request_rejects_out_of_range_limit() -> None:
    with pytest.raises(ValueError, match="limit must be between 1 and 100"):
        parse_request(
            {
                "dataSpec": "RACE",
                "fromTime": "20260829000000",
                "limit": 101,
                "timeoutSeconds": 90,
            }
        )


def test_parse_request_rejects_boolean_limit() -> None:
    with pytest.raises(ValueError, match="limit must be between 1 and 100"):
        parse_request(
            {
                "dataSpec": "RACE",
                "fromTime": "20260829000000",
                "limit": True,
                "timeoutSeconds": 90,
            }
        )


def test_parse_request_rejects_out_of_range_timeout() -> None:
    with pytest.raises(ValueError, match="timeoutSeconds must be between 1 and 900"):
        parse_request(
            {
                "dataSpec": "RACE",
                "fromTime": "20260829000000",
                "limit": 10,
                "timeoutSeconds": 0,
            }
        )


def test_parse_request_rejects_boolean_timeout() -> None:
    with pytest.raises(ValueError, match="timeoutSeconds must be between 1 and 900"):
        parse_request(
            {
                "dataSpec": "RACE",
                "fromTime": "20260829000000",
                "limit": 10,
                "timeoutSeconds": False,
            }
        )


def test_windows_path_maps_an_absolute_linux_path() -> None:
    assert windows_path(Path("/tmp/result.txt")) == r"Z:\tmp\result.txt"


def test_build_command_keeps_credentials_out_of_arguments() -> None:
    result = build_command(
        AcquisitionRequest("RACE", "20260829000000", 10, 90), Path("/tmp/result.txt")
    )

    assert result == [
        "wine",
        r"C:\JVClient\jvlink-demo.exe",
        "--data-spec",
        "RACE",
        "--from-time",
        "20260829000000",
        "--output",
        r"Z:\tmp\result.txt",
        "--save-path",
        r"C:\JVData",
        "--limit",
        "10",
        "--timeout",
        "90",
    ]


def test_acquire_records_returns_output_and_checkpoints() -> None:
    calls: list[list[str] | tuple[str, ...]] = []

    def runner(
        command: list[str] | tuple[str, ...],
        *,
        check: bool,
        capture_output: bool = False,
        text: bool = False,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] == "wine":
            output = Path(command[7][2:].replace("\\", "/"))
            output.write_bytes(b"JG-record\n")
        return subprocess.CompletedProcess(command, 0, "", "")

    result = acquire_records(AcquisitionRequest("RACE", "20260829000000", 10, 90), runner)

    assert result == b"JG-record\n"
    assert calls[1] == ("/opt/jvlink/state.sh", "checkpoint")


def test_acquire_records_checkpoints_after_client_failure() -> None:
    calls: list[list[str] | tuple[str, ...]] = []

    def runner(
        command: list[str] | tuple[str, ...],
        *,
        check: bool,
        capture_output: bool = False,
        text: bool = False,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] == "wine":
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0, "", "")

    with pytest.raises(subprocess.CalledProcessError):
        acquire_records(AcquisitionRequest("RACE", "20260829000000", 10, 90), runner)

    assert calls[1] == ("/opt/jvlink/state.sh", "checkpoint")
