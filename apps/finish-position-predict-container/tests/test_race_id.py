"""Tests for race_id parse / format."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.race_id import RaceIdParts, format_race_id, parse_race_id


def test_parse_race_id_jra() -> None:
    parts = parse_race_id("jra:2024:0101:45:08")
    assert parts == RaceIdParts("jra", "2024", "0101", "45", "08")


def test_parse_race_id_nar() -> None:
    parts = parse_race_id("nar:2026:0523:54:11")
    assert parts.source == "nar"
    assert parts.race_bango == "11"


def test_parse_race_id_too_few_parts() -> None:
    with pytest.raises(ValueError, match="must have 5 parts"):
        parse_race_id("jra:2024:0101:45")


def test_parse_race_id_too_many_parts() -> None:
    with pytest.raises(ValueError, match="must have 5 parts"):
        parse_race_id("jra:2024:0101:45:08:99")


def test_parse_race_id_empty_part() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        parse_race_id("jra:2024::45:08")


def test_format_race_id_round_trip() -> None:
    parts = RaceIdParts("ban-ei", "2026", "0601", "83", "07")
    assert format_race_id(parts) == "ban-ei:2026:0601:83:07"
