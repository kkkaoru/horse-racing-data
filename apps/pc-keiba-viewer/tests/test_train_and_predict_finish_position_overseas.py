from __future__ import annotations

from types import TracebackType
from typing import final

import pandas as pd
import pytest

import train_and_predict_finish_position_overseas as subject


@final
class _RecordingCursor:
    def __init__(self, executed: list[tuple[str, dict[str, str]]]) -> None:
        self._executed: list[tuple[str, dict[str, str]]] = executed
        self.description: None = None

    def __enter__(self) -> _RecordingCursor:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        return None

    def execute(self, sql: str, params: dict[str, str]) -> None:
        self._executed.append((sql, params))

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


@final
class _RecordingConnection:
    def __init__(self, executed: list[tuple[str, dict[str, str]]]) -> None:
        self._executed: list[tuple[str, dict[str, str]]] = executed

    def __enter__(self) -> _RecordingConnection:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        return None

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self._executed)


def test_step1_extract_training_data_excludes_the_card_day_and_later(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr("psycopg.connect", lambda _url: _RecordingConnection(executed))
    monkeypatch.setattr(subject, "decode_jv_tansho_odds", lambda frame: frame)
    monkeypatch.setattr(subject, "log", lambda _message: None)
    frame = subject.step1_extract_training_data("postgresql://example", "20260816")
    assert frame.empty is True
    assert executed[0][1] == {"cutoff_date": "20260816"}
    assert "s.kaisai_nen || s.kaisai_tsukihi < %(cutoff_date)s" in executed[0][0]


def test_step2_engineer_features_computes_field_relative_features_per_race(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "log", lambda _message: None)
    runner = {
        "kaisai_nen": "2026", "keibajo_code": "A8", "age_raw": "05", "sex": "1",
        "kyori": "1600", "track_code": "10", "grade": "A", "field_size_raw": "2",
        "finish_position": "1",
    }
    df = pd.DataFrame(
        [
            {**runner, "kaisai_tsukihi": "0801", "race_bango": "01", "umaban": 1, "weight_raw": "500"},
            {**runner, "kaisai_tsukihi": "0801", "race_bango": "01", "umaban": 2, "weight_raw": "520"},
            {**runner, "kaisai_tsukihi": "0802", "race_bango": "01", "umaban": 1, "weight_raw": "600"},
            {**runner, "kaisai_tsukihi": "0802", "race_bango": "01", "umaban": 2, "weight_raw": "640"},
        ]
    )
    features, _target, _race_ids, groups = subject.step2_engineer_features(df)
    assert groups == [2, 2]
    assert features["weight_vs_field"].tolist() == [-1.0, 1.0, -2.0, 2.0]
