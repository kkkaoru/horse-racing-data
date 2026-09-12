"""Readouts preserve missing-history runners and reject identity inconsistencies."""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.chronos_readout import ReadoutRace, evaluate_readout, main, winner_hits


def test_missing_history_keeps_market_score() -> None:
    race = ReadoutRace(
        np.array(["a", "b"]), np.array([1.0, 0.0]), np.array([1, 2]), np.array([np.nan, 0.5])
    )
    assert winner_hits([race], 0.0) == [1, 1, 1, 1, 1]
    assert winner_hits([race, race], 0.0) == [2, 2, 2, 2, 2]
    with pytest.raises(ValueError, match="Market weight"):
        winner_hits([race], 2.0)


def test_complete_source_readout_and_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "predictions.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": ["r1", "r1", "r2"],
                "horse_id": ["a", "b", "c"],
                "finish_position": [2, 1, 1],
                "decimal_odds": [2.0, 3.0, None],
                "field_size": [2, 2, 1],
                "candidate": [0.2, 0.8, 1.0],
            }
        ),
        source,
    )
    report = evaluate_readout(source, forecast_column="candidate")
    assert report["races"] == 1
    assert report["market_hits"] == [0, 1, 1, 1, 1]
    assert report["excluded_invalid_market_races"] == ["r2"]
    assert report["production_eligible"] is False
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "readout",
            "--source",
            str(source),
            "--forecast-column",
            "candidate",
            "--output",
            str(output),
        ],
    )
    main()
    assert output.is_file()


def test_invalid_identity(tmp_path: Path) -> None:
    source = tmp_path / "bad.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": ["r"],
                "horse_id": ["a"],
                "finish_position": [1],
                "decimal_odds": [2.0],
                "field_size": [2],
                "candidate": [0.5],
            }
        ),
        source,
    )
    with pytest.raises(ValueError, match="incomplete source race"):
        evaluate_readout(source, forecast_column="candidate")


def test_no_market_races(tmp_path: Path) -> None:
    source = tmp_path / "no-odds.parquet"
    pq.write_table(
        pa.table(
            {
                "race_id": ["r"],
                "horse_id": ["a"],
                "finish_position": [1],
                "decimal_odds": [None],
                "field_size": [1],
                "candidate": [0.5],
            }
        ),
        source,
    )
    with pytest.raises(ValueError, match="No races"):
        evaluate_readout(source, forecast_column="candidate")
