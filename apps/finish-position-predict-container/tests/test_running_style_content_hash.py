"""Parity and fail-closed tests for running-style content fingerprints."""

from __future__ import annotations

import math
from typing import cast

import pytest

from predict_lib.running_style_content_hash import (
    RunningStyleContentRow,
    compute_running_style_content_hash,
)


def test_hash_matches_worker_golden_and_ignores_row_order() -> None:
    first = (
        "nar",
        "20260910",
        "50",
        "11",
        1,
        "HORSE-1",
        0.1,
        0.2,
        0.3,
        0.4,
        0,
    )
    second = (
        "nar",
        "20260910",
        "50",
        "11",
        2,
        "HORSE-2",
        0.1,
        0.2,
        0.3,
        0.4,
        1,
    )

    expected = "e7e716489c0065429f998fabd7ac83d9ce443d8f2763521fc0d6d034b2c34df5"
    assert compute_running_style_content_hash([second, first]) == expected
    assert compute_running_style_content_hash([first, second]) == expected


@pytest.mark.parametrize(
    ("index", "value", "message"),
    [
        (0, "other", "invalid running-style source"),
        (1, "bad", "invalid running-style date"),
        (2, "0", "invalid running-style identity"),
        (3, "0", "invalid running-style identity"),
        (4, 0, "invalid running-style identity"),
        (5, " ", "invalid running-style identity"),
        (6, math.nan, "invalid running-style probability"),
        (10, 4, "invalid running-style class"),
    ],
)
def test_rejects_invalid_feature_content(index: int, value: object, message: str) -> None:
    row: list[object] = [
        "nar",
        "20260910",
        "50",
        "11",
        1,
        "HORSE-1",
        0.1,
        0.2,
        0.3,
        0.4,
        0,
    ]
    row[index] = value

    with pytest.raises(ValueError, match=message):
        compute_running_style_content_hash([cast(RunningStyleContentRow, tuple(row))])
