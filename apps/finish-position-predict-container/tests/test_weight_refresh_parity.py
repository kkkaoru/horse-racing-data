"""Cross-runtime weight-feature parity against the actual full-build SQL source."""

from __future__ import annotations

import ast
import json
import math
import re
import subprocess
from pathlib import Path

import duckdb
import pytest

from predict_lib.late_binding import (
    OddsSnapshot,
    WeightSnapshot,
    apply_late_binding_to_entry,
    recompute_market_signal_features,
)
from predict_lib.model_meta import Category, architecture_for
from predict_lib.rescore import RaceFreshSnapshot, apply_fresh_snapshots
from predict_lib.scorer import build_feature_matrix

REPO_ROOT = Path(__file__).resolve().parents[3]
BUILDER_PATH = REPO_ROOT / "apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py"
WORKER_PATH = REPO_ROOT / "apps/finish-position-cron/src/scoring/feature-cache.ts"
CASES: tuple[tuple[float | None, float | None, float | None], ...] = (
    (488.0, 480.0, 4.0),
    (None, 480.0, 4.0),
    (488.0, 480.0, 0.0),
    (472.0, 480.0, 0.0),
    (480.0, 480.0, 4.0),
    (482.0, 480.0, None),
    (488.0, None, 4.0),
)


def _builder_expression() -> str:
    source = BUILDER_PATH.read_text(encoding="utf-8")
    constants = {
        target.id: ast.literal_eval(node.value)
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id.startswith("WEIGHT_ZSCORE_")
    }
    expression = re.search(r"^\s*(.+) as weight_zscore,\s*$", source, re.MULTILINE)
    if expression is None:
        raise ValueError("Canonical builder weight_zscore expression not found")
    return expression.group(1).format_map(constants)


def _full_build_values() -> list[object]:
    with duckdb.connect() as connection:
        connection.execute(
            "create table wa(current_bataiju_kept double, weight_avg_5 double, "
            "weight_volatility_5 double)"
        )
        connection.executemany("insert into wa values (?, ?, ?)", CASES)
        return [
            row[0]
            for row in connection.execute(f"select {_builder_expression()} from wa").fetchall()
        ]


def _python_rescore_values() -> list[object]:
    return [
        apply_late_binding_to_entry(
            {"weight_avg_5": average, "weight_volatility_5": volatility, "weight_zscore": 99.0},
            OddsSnapshot(None, None),
            WeightSnapshot(current),
            "jra",
        )["weight_zscore"]
        for current, average, volatility in CASES
    ]


def _worker_rescore_values() -> object:
    source = (
        f"import {{ refreshLateBindingColumns }} from {json.dumps(str(WORKER_PATH))};"
        "const cases=await Bun.stdin.json();"
        "console.log(JSON.stringify(cases.map(([current,average,volatility])=>"
        "refreshLateBindingColumns({category:'jra',currentBataiju:current,runnerCount:16,"
        "tanshoOdds:null,tanshoNinkijun:null,row:{weight_avg_5:average,"
        "weight_volatility_5:volatility,weight_zscore:99}}).weight_zscore)));"
    )
    result = subprocess.run(
        ["bun", "--eval", source],
        input=json.dumps(CASES),
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout)


def _near_miss_sql_values() -> list[tuple[object, ...]]:
    path = (
        REPO_ROOT
        / "apps/pc-keiba-viewer/src/scripts/finish-position-features/add-near-miss-features.py"
    )
    source = path.read_text(encoding="utf-8")
    favorites = re.search(r"(fav_ranked as \(.*?),\n    joined as \(", source, re.DOTALL)
    ratio = re.search(
        r"^\s*(b\.tansho_ninkijun[^\n]+)\n\s*as horse_popularity_vs_field", source, re.MULTILINE
    )
    if favorites is None or ratio is None:
        raise ValueError("Canonical near-miss market SQL not found")
    with duckdb.connect() as connection:
        return connection.execute(
            "with base as (select 'jra' as source, 2026 as kaisai_nen, '1004' as kaisai_tsukihi, "
            "'05' as keibajo_code, '01' as race_bango, * from "
            "(values (1,8.0,2,3),(2,4.0,1,3),(3,2.0,3,3)) "
            "t(umaban,tansho_odds,tansho_ninkijun,shusso_tosu)), "
            f"{favorites.group(1)} select f.field_dominant_favorite_indicator, {ratio.group(1)} "
            "from base b cross join race_favorite_dominance f order by b.umaban"
        ).fetchall()


def _python_near_miss_values() -> list[tuple[object, object]]:
    rows: list[dict[str, object]] = [
        {"tansho_odds_raw": 8.0, "tansho_ninkijun_raw": 2, "shusso_tosu": None, "shusso_tosu_1": 3},
        {"tansho_odds_raw": 4.0, "tansho_ninkijun_raw": 1, "shusso_tosu": None, "shusso_tosu_1": 3},
        {"tansho_odds_raw": 2.0, "tansho_ninkijun_raw": 3, "shusso_tosu": None, "shusso_tosu_1": 3},
    ]
    return [
        (row["field_dominant_favorite_indicator"], row["horse_popularity_vs_field"])
        for row in recompute_market_signal_features(rows)
    ]


def test_near_miss_full_sql_and_rescore_match_popularity_order_and_trained_null_alias() -> None:
    assert _near_miss_sql_values() == [(0.5, 2 / 3), (0.5, 1 / 3), (0.5, 1.0)]
    assert _python_near_miss_values() == [(0.5, 2 / 3), (0.5, 1 / 3), (0.5, 1.0)]


def _valid_weight_fixture(venue: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    with duckdb.connect() as connection:
        connection.execute(
            "create table wa(umaban integer, current_bataiju_kept double, "
            "weight_avg_5 double, weight_volatility_5 double)"
        )
        connection.execute("insert into wa values (1,488,480,4),(2,500,NULL,4),(3,483,480,0)")
        full = (
            connection.execute(
                "select *, current_bataiju_kept-weight_avg_5 as weight_diff_from_avg, "
                f"{_builder_expression()} as weight_zscore from wa order by umaban"
            )
            .fetchdf()
            .to_dict(orient="records")
        )
        cached = (
            connection.execute(
                "select umaban, weight_avg_5, weight_volatility_5, "
                "cast(NULL as double) as weight_diff_from_avg, -5.0::double as weight_zscore "
                "from wa order by umaban"
            )
            .fetchdf()
            .to_dict(orient="records")
        )
    return [{**row, "keibajo_code": venue, "race_bango": "01"} for row in cached], list(full)


@pytest.mark.parametrize(("category", "venue"), [("jra", "05"), ("nar", "35"), ("ban-ei", "83")])
def test_valid_postweight_model_inputs_match_full_sql_with_missing_history(
    category: Category,
    venue: str,
) -> None:
    cached, full = _valid_weight_fixture(venue)
    source = "jra" if category == "jra" else "nar"
    race_id = f"{source}:2026:1004:{venue}:01"
    refreshed = apply_fresh_snapshots(
        {race_id: cached},
        {(venue, "01"): RaceFreshSnapshot({}, {1: 488.0, 2: 500.0, 3: 483.0})},
        category,
    )[race_id]
    columns = ("weight_diff_from_avg", "weight_zscore")
    expected_matrix = build_feature_matrix(full, columns, architecture_for(category))
    actual_matrix = build_feature_matrix(refreshed, columns, architecture_for(category))
    assert expected_matrix[0] == [8.0, 2.0]
    assert actual_matrix[0] == [8.0, 2.0]
    assert math.isnan(expected_matrix[1][0])
    assert math.isnan(actual_matrix[1][0])
    assert expected_matrix[1][1] == -5.0
    assert actual_matrix[1][1] == -5.0
    assert expected_matrix[2] == [3.0, 3.0]
    assert actual_matrix[2] == [3.0, 3.0]


def test_full_builder_python_and_worker_refresh_match_frozen_weight_inputs() -> None:
    assert _full_build_values() == [2.0, -5.0, 5.0, -5.0, 0.0, 2.0, -5.0]
    assert _python_rescore_values() == [2.0, -5.0, 5.0, -5.0, 0.0, 2.0, -5.0]
    assert _worker_rescore_values() == [2.0, -5.0, 5.0, -5.0, 0.0, 2.0, -5.0]
