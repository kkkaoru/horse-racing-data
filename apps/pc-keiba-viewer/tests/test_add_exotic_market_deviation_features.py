"""Unit tests for add_exotic_market_deviation_features.py (no network / PG I/O)."""

from __future__ import annotations

import json
import math
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import add_exotic_market_deviation_features as subject


def _baseline() -> subject.Baseline:
    return subject.Baseline(
        umaren={"3:1.0": 0.4, "3:2.0": 0.0, "3:3.0": -0.6},
        sanrenpuku={"3:1.0": 0.1, "3:2.0": 0.0, "3:3.0": -0.1},
    )


def test_parse_args_defaults(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--pg-url",
            "pg",
            "--category",
            "jra",
        ]
    )
    assert (args.category, args.from_date, args.baseline) == (
        "jra",
        "20100101",
        subject.DEFAULT_BASELINE_PATH,
    )


def test_default_baseline_lookup_has_every_category() -> None:
    payload = json.loads(subject.DEFAULT_BASELINE_PATH.read_text(encoding="utf-8"))
    assert sorted(payload["categories"]) == ["ban-ei", "jra", "nar"]


def test_load_baseline_reads_category_pools(tmp_path: Path) -> None:
    path = tmp_path / "b.json"
    path.write_text(
        json.dumps(
            {"categories": {"jra": {"umaren": {"3:1.0": 0.4}, "sanrenpuku": {"3:1.0": 0.1}}}}
        ),
        encoding="utf-8",
    )
    assert subject.load_baseline(path, "jra") == subject.Baseline(
        umaren={"3:1.0": 0.4}, sanrenpuku={"3:1.0": 0.1}
    )


def test_decode_umaren_normalizes_inverse_odds() -> None:
    packed = "0102000020001" + "0103000040002" + "0203000040003"
    shares = subject.decode_umaren(packed)
    assert {h: round(v, 6) for h, v in shares.items()} == {1: 0.375, 2: 0.375, 3: 0.25}


def test_decode_umaren_skips_zero_odds_bad_digits_and_zero_horse() -> None:
    packed = "0102000000001" + "01xx000020001" + "0001000020001" + "0203000020001"
    assert subject.decode_umaren(packed) == {2: 0.5, 3: 0.5}


def test_decode_umaren_empty_string_returns_empty() -> None:
    assert subject.decode_umaren("") == {}


def test_decode_sanrenpuku_uses_six_digit_odds() -> None:
    packed = "010203000030001" + "010204000060002"
    shares = subject.decode_sanrenpuku(packed)
    assert {h: round(v, 6) for h, v in shares.items()} == {
        1: 0.333333,
        2: 0.333333,
        3: 0.222222,
        4: 0.111111,
    }


def test_market_ranks_average_ties() -> None:
    assert subject.market_ranks({1: 0.25, 2: 0.5, 3: 0.25}) == {1: 2.5, 2: 1.0, 3: 2.5}


def test_baseline_key_rounds_to_half() -> None:
    assert subject.baseline_key(12, 2.5) == "12:2.5"


def test_rank_residuals_subtract_expectation_and_skip_missing_cells() -> None:
    residuals = subject.rank_residuals({1: 0.5, 2: 0.3, 3: 0.2}, {"3:1.0": 0.4, "3:2.0": 0.0})
    assert {h: round(v, 6) for h, v in residuals.items()} == {1: 0.005465, 2: -0.105361}


def test_compute_race_features_uses_common_horses() -> None:
    odds = subject.RaceOdds(
        umaren={1: 0.5, 2: 0.3, 3: 0.2, 9: 0.1}, sanrenpuku={1: 0.4, 2: 0.35, 3: 0.25}
    )
    features = subject.compute_race_features(odds, _baseline())
    assert sorted(features) == [1, 2, 3]
    assert round(features[1]["exotic_umaren_p2_rank_residual"], 6) == 0.005465
    assert round(features[3]["exotic_sanrenpuku_p3_rank_residual"], 6) == -0.187682


def test_compute_race_features_omits_cells_missing_from_baseline() -> None:
    odds = subject.RaceOdds(umaren={1: 0.5, 2: 0.3, 3: 0.2}, sanrenpuku={1: 0.4, 2: 0.35, 3: 0.25})
    baseline = subject.Baseline(umaren={}, sanrenpuku={"3:1.0": 0.0})
    features = subject.compute_race_features(odds, baseline)
    assert features[2] == {}
    assert list(features[1]) == ["exotic_sanrenpuku_p3_rank_residual"]


def test_compute_race_features_needs_three_common_horses() -> None:
    odds = subject.RaceOdds(umaren={1: 0.5, 2: 0.5}, sanrenpuku={1: 0.3, 2: 0.3, 3: 0.4})
    assert subject.compute_race_features(odds, _baseline()) == {}


def test_odds_tables_per_category() -> None:
    assert subject._odds_tables("jra") == ("pg.jvd_o2", "pg.jvd_o5", "true")
    assert subject._odds_tables("nar") == ("pg.nvd_o2", "pg.nvd_o5", "t.keibajo_code <> '83'")
    assert subject._odds_tables("ban-ei") == ("pg.nvd_o2", "pg.nvd_o5", "t.keibajo_code = '83'")


def test_load_warehouse_odds_returns_empty_without_races() -> None:
    assert subject.load_warehouse_odds(duckdb.connect(), "jra", []) == {}


def test_build_feature_rows_fills_none_for_unpriced_entries() -> None:
    rows = subject.build_feature_rows([("2026", "1018", "05", "11", 1)], {}, _baseline())
    assert rows == [("2026", "1018", "05", "11", 1, None, None)]


def _stub_catalog(con: duckdb.DuckDBPyConnection, url: str) -> None:
    assert url == "unused"
    con.execute("attach ':memory:' as pg")
    con.execute(
        "create table pg.jvd_o2 (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,"
        " race_bango varchar, data_kubun varchar, odds_umaren varchar)"
    )
    con.execute(
        "create table pg.jvd_o5 (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,"
        " race_bango varchar, data_kubun varchar, odds_sanrenpuku varchar)"
    )
    con.execute(
        "insert into pg.jvd_o2 values ('2025', '0601', '05', '01', '5',"
        " '0102000020001010300004000202030000400003')"
    )
    con.execute("insert into pg.jvd_o5 values ('2025', '0601', '05', '01', '5', '010203000030001')")


def test_main_appends_residual_columns_without_race_year_in_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_dir = tmp_path / "in" / "race_year=2025"
    input_dir.mkdir(parents=True)
    table = pa.table(
        {
            "kaisai_nen": ["2025", "2025", "2025", "2025"],
            "kaisai_tsukihi": ["0601", "0601", "0601", "0602"],
            "keibajo_code": ["05", "05", "05", "05"],
            "race_bango": ["01", "01", "01", "02"],
            "umaban": [1, 2, 3, 1],
        }
    )
    pq.write_table(table, input_dir / "data_0.parquet")
    baseline = tmp_path / "b.json"
    baseline.write_text(
        json.dumps(
            {
                "categories": {
                    "jra": {
                        "umaren": {"3:1.5": 0.1, "3:3.0": -0.3},
                        "sanrenpuku": {"3:2.0": 0.0},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(subject, "attach_source_catalog", _stub_catalog)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    subject.main(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(output_dir),
            "--pg-url",
            "unused",
            "--category",
            "jra",
            "--baseline",
            str(baseline),
        ]
    )
    result = (
        pq.ParquetFile(output_dir / "race_year=2025" / "data_0.parquet")
        .read()
        .sort_by([("kaisai_tsukihi", "ascending"), ("umaban", "ascending")])
    )
    umaren = result.column("exotic_umaren_p2_rank_residual").to_pylist()
    sanrenpuku = result.column("exotic_sanrenpuku_p3_rank_residual").to_pylist()
    assert "race_year" not in result.column_names
    assert [None if v is None else round(v, 6) for v in umaren] == [
        0.017783,
        0.017783,
        0.012318,
        None,
    ]
    assert sanrenpuku == [0.0, 0.0, 0.0, None]
    assert math.isclose(umaren[0], math.log(0.375 * 3) - 0.1)
