from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "src" / "scripts" / "finish-position-features" / "compare-corner-position-predictions.py"

_spec = importlib.util.spec_from_file_location("compare_corner_position_predictions", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["compare_corner_position_predictions"] = subject
_spec.loader.exec_module(subject)


def test_corner_heads_cover_three_corners():
    assert subject.CORNER_HEADS == ("corner_1", "corner_3", "corner_4")


def test_top_k_agreement_is_three():
    assert subject.TOP_K_AGREEMENT == 3


def test_mae_for_head_excludes_null_pairs():
    records = [
        {"target_corner_1_norm": 0.10, "corner_1_pred": 0.15},
        {"target_corner_1_norm": None, "corner_1_pred": 0.30},
        {"target_corner_1_norm": 0.40, "corner_1_pred": None},
        {"target_corner_1_norm": 0.50, "corner_1_pred": 0.55},
    ]
    mae = subject.mae_for_head(records, "corner_1")
    # only first and last rows contribute: (0.05 + 0.05) / 2 = 0.05
    assert abs(mae - 0.05) < 1e-9


def test_mae_for_head_returns_none_when_no_valid_pairs():
    records = [{"target_corner_1_norm": None, "corner_1_pred": None}]
    assert subject.mae_for_head(records, "corner_1") is None


def test_mean_mae_averages_non_null_heads():
    per_head = {"corner_1": 0.10, "corner_3": 0.20, "corner_4": None}
    assert subject.mean_mae(per_head) == pytest.approx(0.15)


def test_mean_mae_returns_none_when_all_null():
    assert subject.mean_mae({"corner_1": None, "corner_3": None, "corner_4": None}) is None


def test_race_count_counts_distinct_race_ids():
    records = [
        {"race_id": "jra:2025:0101:05:01"},
        {"race_id": "jra:2025:0101:05:01"},
        {"race_id": "jra:2025:0101:05:02"},
    ]
    assert subject.race_count(records) == 2


def test_corner_1_top3_agreement_perfect_when_ranks_match():
    records = [
        {"race_id": "r1", "target_corner_1_norm": 0.1, "corner_1_pred": 0.1},
        {"race_id": "r1", "target_corner_1_norm": 0.3, "corner_1_pred": 0.3},
        {"race_id": "r1", "target_corner_1_norm": 0.5, "corner_1_pred": 0.5},
        {"race_id": "r1", "target_corner_1_norm": 0.9, "corner_1_pred": 0.9},
    ]
    assert subject.corner_1_top3_agreement(records) == 1.0


def test_corner_1_top3_agreement_returns_none_for_small_field():
    records = [
        {"race_id": "r1", "target_corner_1_norm": 0.1, "corner_1_pred": 0.1},
        {"race_id": "r1", "target_corner_1_norm": 0.3, "corner_1_pred": 0.3},
    ]
    assert subject.corner_1_top3_agreement(records) is None


def test_build_evaluation_aggregates_metrics(tmp_path: Path):
    records = [
        {
            "race_id": "r1",
            "target_corner_1_norm": 0.1, "corner_1_pred": 0.12,
            "target_corner_3_norm": 0.2, "corner_3_pred": 0.18,
            "target_corner_4_norm": 0.3, "corner_4_pred": 0.32,
        },
        {
            "race_id": "r1",
            "target_corner_1_norm": 0.4, "corner_1_pred": 0.5,
            "target_corner_3_norm": 0.5, "corner_3_pred": 0.55,
            "target_corner_4_norm": 0.6, "corner_4_pred": 0.58,
        },
        {
            "race_id": "r1",
            "target_corner_1_norm": 0.8, "corner_1_pred": 0.7,
            "target_corner_3_norm": 0.9, "corner_3_pred": 0.85,
            "target_corner_4_norm": 0.95, "corner_4_pred": 0.92,
        },
    ]
    payload = subject.build_evaluation(records, "v1", "jra", "20240101", "20251231")
    assert payload["model_version"] == "v1"
    assert payload["category"] == "jra"
    assert payload["race_count"] == 1
    assert payload["prediction_count"] == 3
    assert payload["corner_1_mae"] is not None
    assert payload["corner_3_mae"] is not None
    assert payload["corner_4_mae"] is not None
    assert payload["mean_mae"] is not None


def test_build_upsert_sql_contains_required_columns():
    payload: dict[str, object] = {
        "model_version": "v1",
        "category": "jra",
        "window_from": "20240101",
        "window_to": "20251231",
        "race_count": 100,
        "prediction_count": 1500,
        "corner_1_mae": 0.15,
        "corner_3_mae": 0.10,
        "corner_4_mae": 0.12,
        "mean_mae": 0.123,
        "corner_1_top3_agreement": 0.35,
    }
    sql = subject.build_upsert_sql(payload)
    assert "insert into corner_position_model_evaluations" in sql
    assert "on conflict (model_version, category, evaluation_window_from, evaluation_window_to)" in sql
    assert "corner_1_mae = excluded.corner_1_mae" in sql
    assert "mean_mae = excluded.mean_mae" in sql
    assert "corner_1_top3_agreement = excluded.corner_1_top3_agreement" in sql


def test_build_upsert_sql_emits_null_for_missing_metrics():
    payload: dict[str, object] = {
        "model_version": "v1",
        "category": "jra",
        "window_from": "20240101",
        "window_to": "20251231",
        "race_count": 0,
        "prediction_count": 0,
        "corner_1_mae": None,
        "corner_3_mae": None,
        "corner_4_mae": None,
        "mean_mae": None,
        "corner_1_top3_agreement": None,
    }
    sql = subject.build_upsert_sql(payload)
    assert "NULL" in sql


def test_load_jsonl_skips_blank_lines(tmp_path: Path):
    path = tmp_path / "predictions.jsonl"
    path.write_text(
        json.dumps({"race_id": "r1"}) + "\n\n" + json.dumps({"race_id": "r2"}) + "\n",
        encoding="utf-8",
    )
    records = subject.load_jsonl(path)
    assert records == [{"race_id": "r1"}, {"race_id": "r2"}]


def test_parse_args_requires_jsonl_and_model_version():
    args = subject.parse_args(
        [
            "--jsonl",
            "tmp/x.jsonl",
            "--model-version",
            "v1",
            "--window-from",
            "20240101",
            "--window-to",
            "20251231",
        ]
    )
    assert args.jsonl == Path("tmp/x.jsonl")
    assert args.model_version == "v1"
    assert args.category == "jra"
