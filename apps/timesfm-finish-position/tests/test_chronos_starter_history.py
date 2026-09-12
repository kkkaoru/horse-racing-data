from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.chronos_starter_history import export_starter_history


@pytest.fixture
def source(tmp_path: Path) -> tuple[Path, Path]:
    runners, races = tmp_path / "se.parquet", tmp_path / "ra.parquet"
    pq.write_table(
        pa.table(
            {
                "kaisai_nen": ["2023"] * 4,
                "kaisai_tsukihi": ["0101"] * 4,
                "keibajo_code": ["83"] * 4,
                "race_bango": ["01"] * 4,
                "ketto_toroku_bango": ["h1", "h2", "h3", "h4"],
                "umaban": ["01", "02", "03", "04"],
                "ijo_kubun_code": ["0", "4", "5", "3"],
                "kakutei_chakujun": ["1", "0", "0", "0"],
                "tansho_odds": ["20", "30", "50", "100"],
            }
        ),
        runners,
    )
    pq.write_table(
        pa.table(
            {
                "kaisai_nen": ["2023"],
                "kaisai_tsukihi": ["0101"],
                "keibajo_code": ["83"],
                "race_bango": ["01"],
                "data_kubun": ["7"],
                "shusso_tosu": ["3"],
            }
        ),
        races,
    )
    return runners, races


def test_complete_starters_keep_dnf_and_dq(source: tuple[Path, Path], tmp_path: Path) -> None:
    runners, races = source
    output = tmp_path / "result.parquet"
    assert export_starter_history(runners=runners, races=races, output=output) == {
        "starters": 3,
        "races": 1,
        "unclassified_starters": 2,
        "missing_historical_labels": 0,
    }
    table = pq.read_table(output)
    assert table["horse_id"].to_pylist() == ["h1", "h2", "h3"]
    assert table["performance_rating"].to_pylist() == [1.0, None, None]
    assert table["field_size"].to_pylist() == [3, 3, 3]
    with pytest.raises(FileExistsError):
        export_starter_history(runners=runners, races=races, output=output)


@pytest.mark.parametrize(
    ("column", "values", "message"),
    [
        ("ijo_kubun_code", ["8", "4", "5", "3"], "Unknown abnormality"),
        ("ketto_toroku_bango", ["0000000000", "h2", "h3", "h4"], "Invalid starter"),
        ("kakutei_chakujun", ["4", "0", "0", "0"], "Invalid starter"),
        ("ketto_toroku_bango", ["h1", "h1", "h3", "h4"], "Ambiguous"),
        ("kakutei_chakujun", ["0", "0", "0", "0"], "Missing classified"),
    ],
)
def test_runner_guards(
    source: tuple[Path, Path], tmp_path: Path, column: str, values: list[str], message: str
) -> None:
    runners, races = source
    table = pq.read_table(runners)
    pq.write_table(
        table.set_column(table.schema.get_field_index(column), column, pa.array(values)), runners
    )
    with pytest.raises(ValueError, match=message):
        export_starter_history(runners=runners, races=races, output=tmp_path / "result.parquet")


def test_duplicate_sources(source: tuple[Path, Path], tmp_path: Path) -> None:
    runners, races = source
    table = pq.read_table(runners)
    pq.write_table(pa.concat_tables([table, table]), runners)
    with pytest.raises(ValueError, match="Duplicate"):
        export_starter_history(runners=runners, races=races, output=tmp_path / "result.parquet")


def test_missing_metadata(source: tuple[Path, Path], tmp_path: Path) -> None:
    runners, races = source
    pq.write_table(pq.read_table(races).slice(0, 0), races)
    with pytest.raises(ValueError, match="Missing official"):
        export_starter_history(runners=runners, races=races, output=tmp_path / "result.parquet")


def test_starter_count_mismatch(source: tuple[Path, Path], tmp_path: Path) -> None:
    runners, races = source
    table = pq.read_table(races)
    pq.write_table(
        table.set_column(
            table.schema.get_field_index("shusso_tosu"), "shusso_tosu", pa.array(["4"])
        ),
        races,
    )
    with pytest.raises(ValueError, match="Official starter count"):
        export_starter_history(runners=runners, races=races, output=tmp_path / "result.parquet")


def test_undefined_old_context_is_preserved(source: tuple[Path, Path], tmp_path: Path) -> None:
    runners, races = source
    table = pq.read_table(runners).set_column(0, "kaisai_nen", pa.array(["2019"] * 4))
    table = table.set_column(
        table.schema.get_field_index("ijo_kubun_code"),
        "ijo_kubun_code",
        pa.array(["0", "0", "5", "3"]),
    )
    pq.write_table(table, runners)
    pq.write_table(pq.read_table(races).set_column(0, "kaisai_nen", pa.array(["2019"])), races)
    report = export_starter_history(
        runners=runners, races=races, output=tmp_path / "result.parquet"
    )
    assert report["missing_historical_labels"] == 1
