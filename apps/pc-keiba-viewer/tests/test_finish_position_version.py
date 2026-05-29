from __future__ import annotations

import pytest

import finish_position_version as subject


def test_finish_position_version_is_v1_literal():
    assert subject.FINISH_POSITION_VERSION == "v1"


def test_finish_position_version_description_is_non_empty():
    assert isinstance(subject.FINISH_POSITION_VERSION_DESCRIPTION, str)
    assert len(subject.FINISH_POSITION_VERSION_DESCRIPTION) > 0


def test_ssot_path_points_at_finish_position_version_json():
    assert subject.SSOT_PATH.name == "finish-position-version.json"


def test_parse_finish_position_version_payload_with_valid_object():
    parsed = subject.parse_finish_position_version_payload(
        '{"version":"v9","description":"hello"}'
    )
    assert parsed == {"version": "v9", "description": "hello"}


def test_parse_finish_position_version_payload_rejects_array_root():
    with pytest.raises(ValueError) as info:
        subject.parse_finish_position_version_payload("[]")
    assert str(info.value) == "finish-position-version.json must be a JSON object"


def test_parse_finish_position_version_payload_rejects_null_root():
    with pytest.raises(ValueError) as info:
        subject.parse_finish_position_version_payload("null")
    assert str(info.value) == "finish-position-version.json must be a JSON object"


def test_parse_finish_position_version_payload_rejects_missing_version():
    with pytest.raises(ValueError) as info:
        subject.parse_finish_position_version_payload('{"description":"x"}')
    assert str(info.value) == "finish-position-version.json is missing string field 'version'"


def test_parse_finish_position_version_payload_rejects_empty_version():
    with pytest.raises(ValueError) as info:
        subject.parse_finish_position_version_payload('{"version":"","description":"x"}')
    assert str(info.value) == "finish-position-version.json is missing string field 'version'"


def test_parse_finish_position_version_payload_rejects_missing_description():
    with pytest.raises(ValueError) as info:
        subject.parse_finish_position_version_payload('{"version":"v1"}')
    assert (
        str(info.value) == "finish-position-version.json is missing string field 'description'"
    )


def test_read_finish_position_version_file_reads_real_ssot_path():
    parsed = subject.read_finish_position_version_file(subject.SSOT_PATH)
    assert parsed["version"] == "v1"
