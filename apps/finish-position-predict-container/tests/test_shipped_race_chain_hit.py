"""Local emulation of the shipped day-base HIT -> RACE_CHAIN -> one-race UPSERT path.

No production Worker, R2, Neon, or wrangler. Catalog watermark, Ban-ei RS
``none`` token, and Neon writes are all in-process fakes. The production Ban-ei
20260816 sidecar is ``20260814 / 117 / none / 0`` -- those literals are what
these tests lock.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import cast, final

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeline_runner
import predict_upcoming
from predict_lib.model_meta import Category

_DAY_BASE_DIR_ATTR = "_day_base_dir"
_WRITE_WATERMARK_ATTR = "_write_watermark"
_COMBINE_WATERMARKS_ATTR = "_combine_watermarks"
_day_base_dir = cast(
    Callable[[str, str], Path],
    getattr(pipeline_runner, _DAY_BASE_DIR_ATTR),
)
_write_watermark = cast(
    Callable[[Path, tuple[str, int, str, int]], None],
    getattr(pipeline_runner, _WRITE_WATERMARK_ATTR),
)
_combine_watermarks = cast(
    Callable[
        [tuple[str, int] | None, tuple[str, int] | None],
        tuple[str, int, str, int] | None,
    ],
    getattr(pipeline_runner, _COMBINE_WATERMARKS_ATTR),
)


@final
class _RecordingCursor:
    last_sql: str
    last_params: object
    upserts: list[tuple[str, list[object]]]

    def __init__(self, upserts: list[tuple[str, list[object]]]) -> None:
        self.last_sql = ""
        self.last_params = None
        self.upserts = upserts

    def execute(self, query: str, params: object = None) -> object:
        self.last_sql = query
        self.last_params = params
        if "insert into race_finish_position_model_predictions" in query:
            bound = list(params) if isinstance(params, list) else []
            self.upserts.append((query, bound))
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return []

    def fetchone(self) -> tuple[object, ...] | None:
        if self.last_sql == "SHOW transaction_read_only":
            return ("off",)
        return None


@final
class _RecordingConnection:
    committed: int
    closed: bool
    upserts: list[tuple[str, list[object]]]
    _cursor: _RecordingCursor

    def __init__(self) -> None:
        self.committed = 0
        self.closed = False
        self.upserts = []
        self._cursor = _RecordingCursor(self.upserts)

    def cursor(self) -> _RecordingCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


def test_combine_watermarks_banei_none_rs_is_valid_four_tuple() -> None:
    """Production Ban-ei RS token ``none`` plus source max/count is a 4-tuple."""
    assert _combine_watermarks(("20260814", 117), ("none", 0)) == (
        "20260814",
        117,
        "none",
        0,
    )


def test_combine_watermarks_returns_none_when_source_missing() -> None:
    assert _combine_watermarks(None, ("none", 0)) is None


def test_combine_watermarks_returns_none_when_rs_missing() -> None:
    assert _combine_watermarks(("20260814", 117), None) is None


def test_ensure_day_base_banei_watermark_match_hits_twice_without_rebuild(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Catalog sidecar match: first and second same-day calls HIT, no rebuild."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    hive_dir = final_dir / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"BANEI-DAY-BASE")
    _write_watermark(day_dir, ("20260814", 117, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260814", 117)],
    )

    def refuse_rebuild(*_args: object, **_kwargs: object) -> Path:
        raise AssertionError("build_day_base must not run on a watermark HIT")

    monkeypatch.setattr(pipeline_runner, "build_day_base", refuse_rebuild)
    r2_calls: list[str] = []
    monkeypatch.setattr(
        pipeline_runner,
        "r2_get_parquet",
        lambda *_args, **_kwargs: r2_calls.append("get") or True,
    )
    monkeypatch.setattr(
        pipeline_runner,
        "r2_head_watermark",
        lambda *_args, **_kwargs: r2_calls.append("head") or ("20260814", 117, "none", 0),
    )

    first = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)
    second = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert first == final_dir
    assert second == final_dir
    assert r2_calls == []


def test_ensure_day_base_banei_missing_watermark_does_not_hit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    final_dir = _day_base_dir("ban-ei", "20260816") / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"NO-SIDECAR")
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260814", 117)],
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result is None


def test_ensure_day_base_banei_watermark_mismatch_does_not_hit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"STALE-BANEI")
    _write_watermark(day_dir, ("20260814", 117, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260815", 118)],
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result is None


def test_ensure_day_base_banei_zero_count_does_not_hit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    final_dir = day_dir / "final"
    final_dir.mkdir(parents=True)
    (final_dir / "features.parquet").write_bytes(b"ZERO-COUNT")
    _write_watermark(day_dir, ("20260814", 117, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [(None, 0)],
    )

    result = pipeline_runner.ensure_day_base("ban-ei", "20260816", 0, "r2-catalog://pc-keiba", None)

    assert result is None


def test_split_path_first_and_second_banei_predict_are_race_chain_hits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Day-base already present: both per-race predicts run RACE_CHAIN only."""
    work_dir = tmp_path / "work"
    monkeypatch.setattr(pipeline_runner, "WORK_DIR", work_dir)
    day_dir = _day_base_dir("ban-ei", "20260816")
    day_base_final = day_dir / "final"
    hive_dir = day_base_final / "race_year=2026"
    hive_dir.mkdir(parents=True)
    (hive_dir / "features.parquet").write_bytes(b"BANEI-DAY-BASE")
    _write_watermark(day_dir, ("20260814", 117, "none", 0))
    monkeypatch.setattr(
        pipeline_runner,
        "_query_source_rows",
        lambda *_args, **_kwargs: [("20260814", 117)],
    )
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", lambda *_a, **_k: True)

    def refuse_rebuild(*_args: object, **_kwargs: object) -> Path:
        raise AssertionError("build_day_base must not run when the day-base watermark HITs")

    monkeypatch.setattr(pipeline_runner, "build_day_base", refuse_rebuild)

    race_chain_calls: list[tuple[Path, str]] = []

    def fake_race_chain(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        day_base_dir_arg: Path,
        final_dir: Path,
        target_race: str,
        realtime_odds_path: Path | None = None,
        venue_weather_dir: Path | None = None,
    ) -> bool:
        race_chain_calls.append((day_base_dir_arg, target_race))
        final_dir.mkdir(parents=True, exist_ok=True)
        if target_race == "83:01":
            pd.DataFrame(
                {
                    "race_id": ["ban-ei:2026:0816:83:01", "ban-ei:2026:0816:83:02"],
                    "umaban": [1, 2],
                }
            ).to_parquet(final_dir / "data.parquet")
            return True
        pd.DataFrame(
            {
                "race_id": ["ban-ei:2026:0816:83:02"],
                "umaban": [3],
            }
        ).to_parquet(final_dir / "data.parquet")
        return True

    monkeypatch.setattr(pipeline_runner, "build_pipeline_from_day_base", fake_race_chain)

    first = pipeline_runner.build_upcoming_feature_rows_split(
        "ban-ei", "20260816", 0, "r2-catalog://pc-keiba", "83:01"
    )
    second = pipeline_runner.build_upcoming_feature_rows_split(
        "ban-ei", "20260816", 0, "r2-catalog://pc-keiba", "83:02"
    )

    assert first == {
        "ban-ei:2026:0816:83:01": [
            {"race_id": "ban-ei:2026:0816:83:01", "umaban": 1},
        ]
    }
    assert second == {
        "ban-ei:2026:0816:83:02": [
            {"race_id": "ban-ei:2026:0816:83:02", "umaban": 3},
        ]
    }
    assert race_chain_calls == [
        (day_base_final, "83:01"),
        (day_base_final, "83:02"),
    ]


def test_predict_category_upserts_only_the_scoped_banei_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scoped Ban-ei predict must UPSERT only 83:01 even if the builder leaked 83:02."""
    conn = _RecordingConnection()

    def fake_build(
        category: Category,
        window: predict_upcoming.PredictWindow,
        target_race: str | None = None,
        r2_config: object = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        return {
            "ban-ei:2026:0816:83:01": [{"umaban": 1, "ketto_toroku_bango": "2010101010"}],
            "ban-ei:2026:0816:83:02": [{"umaban": 2, "ketto_toroku_bango": "2010101011"}],
        }

    def fake_score(
        races: Mapping[str, Sequence[Mapping[str, object]]],
        category: Category,
        models_dir: Path,
        feature_names: Sequence[str],
        card_max_race_bango: int | None = None,
    ) -> list[list[list[object]]]:
        if "ban-ei:2026:0816:83:02" in races:
            return [
                [
                    [
                        "banei-cb-v9-sim-2011",
                        "ban-ei",
                        "2026",
                        "0816",
                        "83",
                        "01",
                        "2010101010",
                        1,
                        0.5,
                        1,
                        0.4,
                        0.7,
                        1,
                        0.1,
                        3.2,
                        56.0,
                        -1.0,
                        "short",
                        "small",
                        "summer",
                        "ban",
                        "dirt",
                    ]
                ],
                [
                    [
                        "banei-cb-v9-sim-2011",
                        "ban-ei",
                        "2026",
                        "0816",
                        "83",
                        "02",
                        "2010101011",
                        2,
                        0.4,
                        2,
                        0.3,
                        0.6,
                        2,
                        0.2,
                        5.0,
                        56.0,
                        0.0,
                        "short",
                        "small",
                        "summer",
                        "ban",
                        "dirt",
                    ]
                ],
            ]
        if "ban-ei:2026:0816:83:01" in races:
            return [
                [
                    [
                        "banei-cb-v9-sim-2011",
                        "ban-ei",
                        "2026",
                        "0816",
                        "83",
                        "01",
                        "2010101010",
                        1,
                        0.5,
                        1,
                        0.4,
                        0.7,
                        1,
                        0.1,
                        3.2,
                        56.0,
                        -1.0,
                        "short",
                        "small",
                        "summer",
                        "ban",
                        "dirt",
                    ]
                ]
            ]
        return []

    monkeypatch.setattr(predict_upcoming, "_build_feature_rows", fake_build)
    monkeypatch.setattr(predict_upcoming, "_load_model_metadata", lambda *_a, **_k: ["feat"])
    monkeypatch.setattr(predict_upcoming, "score_races", fake_score)
    monkeypatch.setattr(predict_upcoming, "_connect", lambda *_a, **_k: conn)

    written = predict_upcoming.predict_category(
        "postgresql://mock/db",
        "ban-ei",
        Path("/models"),
        predict_upcoming.PredictWindow(
            target_date="20260816",
            days_ahead=0,
            database_url="postgresql://mock/db",
        ),
        target_race="83:01",
    )

    assert written == 1
    assert len(conn.upserts) == 1
    assert conn.upserts[0][1] == [
        "banei-cb-v9-sim-2011",
        "ban-ei",
        "2026",
        "0816",
        "83",
        "01",
        "2010101010",
        1,
        0.5,
        1,
        0.4,
        0.7,
        1,
        0.1,
        3.2,
        56.0,
        -1.0,
        "short",
        "small",
        "summer",
        "ban",
        "dirt",
    ]
    assert "insert into race_finish_position_model_predictions" in conn.upserts[0][0]
    assert conn.closed is True
