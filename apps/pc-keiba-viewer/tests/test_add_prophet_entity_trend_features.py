from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-prophet-entity-trend-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "add_prophet_entity_trend_features", MODULE_PATH
)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_prophet_entity_trend_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_carries_day_category_and_lookup(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--pg-url",
            "duckdb:///tmp/source.duckdb",
            "--category",
            "nar",
            "--target-date",
            "20260903",
            "--prophet-lookup",
            str(tmp_path / "lookup.parquet"),
        ]
    )
    assert args.category == "nar"
    assert args.target_date == "20260903"
    assert args.prophet_lookup == tmp_path / "lookup.parquet"


def test_target_entity_sql_has_fail_closed_category_filters() -> None:
    jra_sql = subject.target_entities_sql("jra", "20260903")
    nar_sql = subject.target_entities_sql("nar", "20260903")
    banei_sql = subject.target_entities_sql("ban-ei", "20260903")
    assert "from pg.jvd_se se" in jra_sql
    assert "'jra' as source" in jra_sql
    assert "se.keibajo_code <> '83'" in nar_sql
    assert "se.keibajo_code = '83'" in banei_sql
    assert "not in ('1', '2')" in banei_sql
    with pytest.raises(ValueError, match="target_date must use YYYYMMDD"):
        subject.target_entities_sql("nar", "2026-09-03")


def test_run_joins_frozen_forecasts_and_preserves_unknown_entities(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.duckdb"
    source = duckdb.connect(str(source_path))
    source.execute(
        "create table nvd_se (kaisai_nen varchar, kaisai_tsukihi varchar, "
        "keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar, "
        "kishu_code varchar, chokyoshi_code varchar, ijo_kubun_code varchar)"
    )
    source.execute(
        "insert into nvd_se values "
        "('2026','0903','30','01','h1','j1','t1','0'),"
        "('2026','0903','30','01','h1','j1','t1','0'),"
        "('2026','0903','30','01','h2','j2','t2','0'),"
        "('2026','0903','30','01','scratched','j1','t1','1')"
    )
    source.close()

    input_dir = tmp_path / "input" / "race_year=2026"
    input_dir.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "source": ["nar", "nar"],
                "race_date": ["20260903", "20260903"],
                "kaisai_nen": ["2026", "2026"],
                "kaisai_tsukihi": ["0903", "0903"],
                "keibajo_code": ["30", "30"],
                "race_bango": ["01", "01"],
                "ketto_toroku_bango": ["h1", "h2"],
                "race_year": [2026, 2026],
            }
        ),
        input_dir / "data.parquet",
    )
    lookup_path = tmp_path / "lookup.parquet"
    pq.write_table(
        pa.table(
            {
                "category": ["nar"] * 6,
                "forecast_date": ["20260903"] * 6,
                "entity_type": [
                    "venue",
                    "jockey",
                    "trainer",
                    "venue",
                    "jockey",
                    "trainer",
                ],
                "entity_code": [
                    "30",
                    "j1",
                    "t1",
                    "__fallback__",
                    "__fallback__",
                    "__fallback__",
                ],
                "yhat": [0.6, 0.9, 0.3, 0.6, 0.6, 0.6],
            }
        ),
        lookup_path,
    )
    output_dir = tmp_path / "output"
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "input"),
            "--output-dir",
            str(output_dir),
            "--pg-url",
            f"duckdb://{source_path}",
            "--category",
            "nar",
            "--target-date",
            "20260903",
            "--prophet-lookup",
            str(lookup_path),
        ]
    )
    subject.run(args)
    result = (
        duckdb.connect(":memory:")
        .execute(
            "select ketto_toroku_bango, prophet_venue_performance_yhat, "
            "prophet_jockey_performance_yhat, prophet_trainer_performance_yhat, "
            "prophet_entity_coverage, prophet_entity_performance_mean "
            f"from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet') "
            "order by ketto_toroku_bango"
        )
        .fetchall()
    )
    assert result == [
        ("h1", 0.6, 0.9, 0.3, 3, 0.6),
        ("h2", 0.6, 0.6, 0.6, 1, 0.6),
    ]


def test_baked_lookup_matches_tracked_metadata_and_contract() -> None:
    lookup_path = (
        REPO_ROOT
        / "finish-position"
        / "lookups"
        / "prophet-entity-trends-all-categories-2026.parquet"
    )
    metadata = json.loads(lookup_path.with_suffix(".json").read_text())
    assert hashlib.sha256(lookup_path.read_bytes()).hexdigest() == metadata["sha256"]
    connection = duckdb.connect(":memory:")
    totals = connection.execute(
        "select count(*), count(distinct (category, forecast_date, entity_type, entity_code)), "
        "min(forecast_date), max(forecast_date), count(*) filter (where not isfinite(yhat)) "
        "from read_parquet(?)",
        [str(lookup_path)],
    ).fetchone()
    connection.close()
    assert totals == (89060, 89060, "20260101", "20261231", 0)
    assert metadata["categories"] == ["jra", "nar", "ban-ei"]
    assert metadata["history_contract"] == "race_date < 20260101"
    assert metadata["runtime_prophet_dependency"] is False


def test_stage_lookup_rejects_missing_and_duplicate_artifacts(tmp_path: Path) -> None:
    connection = duckdb.connect(":memory:")
    with pytest.raises(FileNotFoundError, match="Prophet lookup not found"):
        subject.stage_prophet_lookup(
            connection, tmp_path / "missing.parquet", "nar", "20260903"
        )
    duplicate_path = tmp_path / "duplicate.parquet"
    pq.write_table(
        pa.table(
            {
                "category": ["nar", "nar"],
                "forecast_date": ["20260903", "20260903"],
                "entity_type": ["venue", "venue"],
                "entity_code": ["30", "30"],
                "yhat": [0.5, 0.6],
            }
        ),
        duplicate_path,
    )
    with pytest.raises(ValueError, match="duplicate entity forecasts"):
        subject.stage_prophet_lookup(connection, duplicate_path, "nar", "20260903")
    connection.close()
