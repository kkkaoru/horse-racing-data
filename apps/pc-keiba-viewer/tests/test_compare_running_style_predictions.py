from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "src" / "scripts" / "finish-position-features" / "compare-running-style-predictions.py"

_spec = importlib.util.spec_from_file_location("compare_running_style_predictions", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["compare_running_style_predictions"] = subject
_spec.loader.exec_module(subject)


def test_class_names_listed_in_running_style_order():
    assert subject.CLASS_NAMES == ("nige", "senkou", "sashi", "oikomi")


def test_num_classes_is_four():
    assert subject.NUM_CLASSES == 4


def test_filter_labeled_records_drops_null_targets_and_preds():
    records = [
        {"target_running_style_class": 0, "predicted_class": 1},
        {"target_running_style_class": None, "predicted_class": 1},
        {"target_running_style_class": 2, "predicted_class": None},
        {"target_running_style_class": 3, "predicted_class": 3},
    ]
    labeled = subject.filter_labeled_records(records)
    assert len(labeled) == 2


def test_compute_accuracy_perfect_predictions():
    records = [
        {"target_running_style_class": 0, "predicted_class": 0},
        {"target_running_style_class": 1, "predicted_class": 1},
        {"target_running_style_class": 2, "predicted_class": 2},
    ]
    assert subject.compute_accuracy(records) == 1.0


def test_compute_accuracy_returns_none_for_empty():
    assert subject.compute_accuracy([]) is None


def test_compute_accuracy_partial_match():
    records = [
        {"target_running_style_class": 0, "predicted_class": 0},
        {"target_running_style_class": 1, "predicted_class": 2},
        {"target_running_style_class": 2, "predicted_class": 2},
    ]
    assert abs(subject.compute_accuracy(records) - (2 / 3)) < 1e-9


def test_compute_per_class_metrics_returns_per_class_precision_and_recall():
    records = [
        {"target_running_style_class": 0, "predicted_class": 0},
        {"target_running_style_class": 0, "predicted_class": 1},
        {"target_running_style_class": 1, "predicted_class": 1},
        {"target_running_style_class": 2, "predicted_class": 1},
        {"target_running_style_class": 3, "predicted_class": 3},
    ]
    precision, recall, support = subject.compute_per_class_metrics(records)
    # nige: predicted 1 (correct 1), actual 2 → precision=1/1=1.0, recall=1/2=0.5
    assert precision["nige"] == 1.0
    assert recall["nige"] == 0.5
    # senkou: predicted 3 (correct 1), actual 1 → precision=1/3, recall=1/1
    assert abs(precision["senkou"] - (1 / 3)) < 1e-9
    assert recall["senkou"] == 1.0
    # sashi: predicted 0, actual 1 → precision=None, recall=0
    assert precision["sashi"] is None
    assert recall["sashi"] == 0.0
    # oikomi: predicted 1 (correct 1), actual 1 → 1.0/1.0
    assert precision["oikomi"] == 1.0
    assert recall["oikomi"] == 1.0
    # support
    assert support["nige"] == 2
    assert support["sashi"] == 1


def test_compute_macro_f1_averages_per_class_f1():
    precision = {"nige": 1.0, "senkou": 0.5, "sashi": 1.0, "oikomi": 1.0}
    recall = {"nige": 1.0, "senkou": 1.0, "sashi": 0.5, "oikomi": 1.0}
    macro_f1 = subject.compute_macro_f1(precision, recall)
    # f1(nige)=1.0, f1(senkou)=2/3, f1(sashi)=2/3, f1(oikomi)=1.0 -> mean ≈ 0.833
    expected = (1.0 + (2.0 * 0.5 * 1.0 / 1.5) + (2.0 * 1.0 * 0.5 / 1.5) + 1.0) / 4
    assert abs(macro_f1 - expected) < 1e-9


def test_compute_macro_f1_skips_undefined_classes():
    precision = {"nige": None, "senkou": 1.0, "sashi": 1.0, "oikomi": 1.0}
    recall = {"nige": None, "senkou": 1.0, "sashi": 1.0, "oikomi": 1.0}
    macro_f1 = subject.compute_macro_f1(precision, recall)
    assert macro_f1 == 1.0


def test_compute_macro_f1_returns_none_when_no_classes_defined():
    precision = {name: None for name in subject.CLASS_NAMES}
    recall = {name: None for name in subject.CLASS_NAMES}
    assert subject.compute_macro_f1(precision, recall) is None


def test_race_count_counts_distinct_race_ids():
    records = [
        {"race_id": "r1"},
        {"race_id": "r1"},
        {"race_id": "r2"},
        {"foo": "bar"},
    ]
    assert subject.race_count(records) == 2


def test_build_evaluation_returns_full_payload():
    records = [
        {
            "race_id": "r1",
            "target_running_style_class": 0,
            "predicted_class": 0,
        },
        {
            "race_id": "r1",
            "target_running_style_class": 1,
            "predicted_class": 1,
        },
    ]
    payload = subject.build_evaluation(records, "v1", "jra", "20240101", "20251231")
    assert payload["model_version"] == "v1"
    assert payload["category"] == "jra"
    assert payload["race_count"] == 1
    assert payload["prediction_count"] == 2
    assert payload["accuracy"] == 1.0


def test_build_upsert_sql_contains_required_columns():
    payload: subject.RunningStyleEvaluation = {
        "model_version": "v1",
        "category": "jra",
        "window_from": "20240101",
        "window_to": "20251231",
        "race_count": 100,
        "prediction_count": 1500,
        "accuracy": 0.55,
        "macro_f1": 0.40,
        "precision_per_class": {"nige": 0.5, "senkou": 0.5, "sashi": 0.5, "oikomi": 0.5},
        "recall_per_class": {"nige": 0.5, "senkou": 0.5, "sashi": 0.5, "oikomi": 0.5},
        "support_per_class": {"nige": 5, "senkou": 10, "sashi": 20, "oikomi": 15},
        "kyakushitsuhantei_agreement": None,
    }
    sql = subject.build_upsert_sql(payload)
    assert "insert into running_style_model_evaluations" in sql
    assert "accuracy = excluded.accuracy" in sql
    assert "macro_f1 = excluded.macro_f1" in sql
    assert "precision_nige = excluded.precision_nige" in sql
    assert "support_oikomi = excluded.support_oikomi" in sql
    assert "kyakushitsuhantei_agreement = excluded.kyakushitsuhantei_agreement" in sql


def test_load_jsonl_skips_blank_lines(tmp_path: Path):
    path = tmp_path / "predictions.jsonl"
    path.write_text(
        json.dumps({"race_id": "r1", "predicted_class": 0}) + "\n\n"
        + json.dumps({"race_id": "r2", "predicted_class": 1}) + "\n",
        encoding="utf-8",
    )
    records = subject.load_jsonl(path)
    assert len(records) == 2


def test_parse_args_requires_required_flags():
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
    assert args.category == "jra"
