"""Tests for export_lightgbm_to_json, especially the categorical split
threshold translation (code-index -> raw value) that fixes the JRA
running-style serve-path defect."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast
from unittest.mock import patch

import lightgbm as lgb
import pytest

import export_lightgbm_to_json as subject


class FakeBooster:
    """Duck-typed stand-in for lgb.Booster - only `.dump_model()` is used by
    the module under test, so a real trained booster is unnecessary."""

    def __init__(self, dump: dict[str, object]) -> None:
        self._dump: dict[str, object] = dump

    def dump_model(self) -> dict[str, object]:
        return self._dump


def test_parse_args_reads_model_dir_and_output() -> None:
    args = subject.parse_args(["--model-dir", "tmp/models/x", "--output", "tmp/models/x/model.json"])
    assert args.model_dir == Path("tmp/models/x")
    assert args.output == Path("tmp/models/x/model.json")


def test_js_number_or_none_true_boolean_is_one() -> None:
    assert subject._js_number_or_none(True) == 1.0


def test_js_number_or_none_false_boolean_is_zero() -> None:
    assert subject._js_number_or_none(False) == 0.0


def test_js_number_or_none_int_passthrough() -> None:
    assert subject._js_number_or_none(11) == 11.0


def test_js_number_or_none_float_passthrough() -> None:
    assert subject._js_number_or_none(17.0) == 17.0


def test_js_number_or_none_empty_string_is_zero() -> None:
    assert subject._js_number_or_none("") == 0.0


def test_js_number_or_none_whitespace_only_string_is_zero() -> None:
    assert subject._js_number_or_none("   ") == 0.0


def test_js_number_or_none_numeric_string_parses() -> None:
    assert subject._js_number_or_none("11") == 11.0


def test_js_number_or_none_numeric_string_with_surrounding_whitespace_parses() -> None:
    assert subject._js_number_or_none(" 11 ") == 11.0


def test_js_number_or_none_zero_padded_numeric_string_parses() -> None:
    assert subject._js_number_or_none("005") == 5.0


def test_js_number_or_none_non_numeric_string_returns_none() -> None:
    assert subject._js_number_or_none("A") is None


def test_js_number_or_none_unsupported_type_returns_none() -> None:
    assert subject._js_number_or_none(None) is None


def test_format_categorical_token_whole_number_drops_decimal() -> None:
    assert subject._format_categorical_token(5.0) == "5"


def test_format_categorical_token_fractional_keeps_decimal() -> None:
    assert subject._format_categorical_token(5.5) == "5.5"


def test_translate_categorical_threshold_maps_indices_to_raw_values() -> None:
    raw_categories: list[object] = ["11", "17", "18", "20"]
    result = subject.translate_categorical_threshold(raw_categories, "0||1||2")
    assert result == "11||17||18"


def test_translate_categorical_threshold_single_index() -> None:
    raw_categories: list[object] = ["11", "17", "18"]
    result = subject.translate_categorical_threshold(raw_categories, "1")
    assert result == "17"


def test_translate_categorical_threshold_drops_out_of_range_index() -> None:
    raw_categories: list[object] = ["11", "17"]
    result = subject.translate_categorical_threshold(raw_categories, "0||99")
    assert result == "11"


def test_translate_categorical_threshold_drops_negative_index() -> None:
    raw_categories: list[object] = ["11", "17"]
    result = subject.translate_categorical_threshold(raw_categories, "-1||0")
    assert result == "11"


def test_translate_categorical_threshold_all_non_numeric_is_unreachable() -> None:
    raw_categories: list[object] = [" ", "A", "B"]
    result = subject.translate_categorical_threshold(raw_categories, "1||2")
    assert result == subject.UNREACHABLE_CATEGORICAL_TOKEN


def test_translate_categorical_threshold_drops_only_non_numeric_members() -> None:
    raw_categories: list[object] = [" ", "A", "11"]
    result = subject.translate_categorical_threshold(raw_categories, "0||1||2")
    assert result == "0||11"


def test_translate_categorical_threshold_integer_categories() -> None:
    raw_categories: list[object] = [0, 1, 2, 3]
    result = subject.translate_categorical_threshold(raw_categories, "1||3")
    assert result == "1||3"


def test_build_categorical_value_maps_orders_by_feature_columns() -> None:
    dump: dict[str, object] = {"pandas_categorical": [["11", "17", "18"], [" ", "A", "B"]]}
    metadata: dict[str, object] = {
        "feature_columns": ["kyori", "track_code", "grade_code"],
        "categorical_features": ["track_code", "grade_code"],
    }
    result = subject.build_categorical_value_maps(dump, metadata)
    assert result == {"track_code": ["11", "17", "18"], "grade_code": [" ", "A", "B"]}


def test_build_categorical_value_maps_missing_pandas_categorical_key_is_noop() -> None:
    dump: dict[str, object] = {}
    metadata: dict[str, object] = {
        "feature_columns": ["track_code"],
        "categorical_features": ["track_code"],
    }
    assert subject.build_categorical_value_maps(dump, metadata) == {}


def test_build_categorical_value_maps_empty_pandas_categorical_list_is_noop() -> None:
    dump: dict[str, object] = {"pandas_categorical": []}
    metadata: dict[str, object] = {
        "feature_columns": ["track_code"],
        "categorical_features": ["track_code"],
    }
    assert subject.build_categorical_value_maps(dump, metadata) == {}


def test_build_categorical_value_maps_no_categorical_features_is_noop() -> None:
    dump: dict[str, object] = {"pandas_categorical": [["11", "17"]]}
    metadata: dict[str, object] = {"feature_columns": ["track_code"], "categorical_features": []}
    assert subject.build_categorical_value_maps(dump, metadata) == {}


def test_translate_node_threshold_leaves_non_string_threshold_untouched() -> None:
    node: dict[str, object] = {"threshold": 0.5, "split_feature": 0, "decision_type": "<="}
    context: subject.CategoricalTranslationContext = {
        "feature_names": ["track_code"],
        "value_maps": {"track_code": ["11", "17"]},
    }
    assert subject.translate_node_threshold(node, context) == 0.5


def test_translate_node_threshold_leaves_non_int_split_feature_untouched() -> None:
    node: dict[str, object] = {"threshold": "0||1", "decision_type": "=="}
    context: subject.CategoricalTranslationContext = {
        "feature_names": ["track_code"],
        "value_maps": {"track_code": ["11", "17"]},
    }
    assert subject.translate_node_threshold(node, context) == "0||1"


def test_translate_node_threshold_leaves_negative_split_feature_untouched() -> None:
    node: dict[str, object] = {"threshold": "0||1", "split_feature": -1, "decision_type": "=="}
    context: subject.CategoricalTranslationContext = {
        "feature_names": ["track_code"],
        "value_maps": {"track_code": ["11", "17"]},
    }
    assert subject.translate_node_threshold(node, context) == "0||1"


def test_translate_node_threshold_leaves_out_of_range_split_feature_untouched() -> None:
    node: dict[str, object] = {"threshold": "0||1", "split_feature": 5, "decision_type": "=="}
    context: subject.CategoricalTranslationContext = {
        "feature_names": ["track_code"],
        "value_maps": {"track_code": ["11", "17"]},
    }
    assert subject.translate_node_threshold(node, context) == "0||1"


def test_translate_node_threshold_leaves_unmapped_feature_untouched() -> None:
    node: dict[str, object] = {"threshold": "0||1", "split_feature": 0, "decision_type": "=="}
    context: subject.CategoricalTranslationContext = {
        "feature_names": ["kyori"],
        "value_maps": {},
    }
    assert subject.translate_node_threshold(node, context) == "0||1"


def test_translate_node_threshold_translates_matched_feature() -> None:
    node: dict[str, object] = {"threshold": "0||1", "split_feature": 0, "decision_type": "=="}
    context: subject.CategoricalTranslationContext = {
        "feature_names": ["track_code"],
        "value_maps": {"track_code": ["11", "17"]},
    }
    assert subject.translate_node_threshold(node, context) == "11||17"


def _empty_context() -> subject.CategoricalTranslationContext:
    return {"feature_names": [], "value_maps": {}}


def test_trim_node_keeps_only_leaf_value_for_leaf() -> None:
    node: dict[str, object] = {"leaf_value": 0.25, "leaf_index": 3, "internal_count": 999}
    trimmed = subject.trim_node(node, _empty_context())
    assert trimmed == {"leaf_value": 0.25, "leaf_index": 3}


def test_trim_node_keeps_numeric_threshold_untranslated() -> None:
    node: dict[str, object] = {
        "split_feature": 0,
        "threshold": 0.5,
        "decision_type": "<=",
        "default_left": True,
        "left_child": {"leaf_value": 0.1},
        "right_child": {"leaf_value": 0.2},
    }
    trimmed = subject.trim_node(node, _empty_context())
    assert trimmed["threshold"] == 0.5
    assert trimmed["left_child"] == {"leaf_value": 0.1}
    assert trimmed["right_child"] == {"leaf_value": 0.2}


def test_trim_node_translates_categorical_threshold_recursively() -> None:
    node: dict[str, object] = {
        "split_feature": 0,
        "threshold": "0||1",
        "decision_type": "==",
        "default_left": False,
        "left_child": {
            "split_feature": 0,
            "threshold": "0",
            "decision_type": "==",
            "default_left": False,
            "left_child": {"leaf_value": 1.0},
            "right_child": {"leaf_value": 2.0},
        },
        "right_child": {"leaf_value": 3.0},
    }
    context: subject.CategoricalTranslationContext = {
        "feature_names": ["track_code"],
        "value_maps": {"track_code": ["11", "17"]},
    }
    trimmed = subject.trim_node(node, context)
    left_child = cast(dict[str, object], trimmed["left_child"])
    assert trimmed["threshold"] == "11||17"
    assert left_child["threshold"] == "11"


def test_trim_tree_wraps_trimmed_structure() -> None:
    tree: dict[str, object] = {"tree_structure": {"leaf_value": 0.5}}
    assert subject.trim_tree(tree, _empty_context()) == {"tree_structure": {"leaf_value": 0.5}}


def test_trim_tree_raises_when_tree_structure_missing() -> None:
    with pytest.raises(ValueError, match="tree_structure missing"):
        subject.trim_tree({}, _empty_context())


def test_load_metadata_reads_existing_file(tmp_path: Path) -> None:
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "metadata.json").write_text(json.dumps({"model_version": "v3"}), encoding="utf-8")
    assert subject.load_metadata(model_dir) == {"model_version": "v3"}


def test_load_metadata_raises_when_file_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="metadata.json missing"):
        subject.load_metadata(tmp_path / "missing-model")


def _sample_dump() -> dict[str, object]:
    return {
        "objective": "multiclass",
        "num_class": 4,
        "num_tree_per_iteration": 4,
        "feature_names": ["track_code", "kyori"],
        "pandas_categorical": [["11", "17", "18"]],
        "tree_info": [
            {
                "tree_structure": {
                    "split_feature": 0,
                    "threshold": "0||1",
                    "decision_type": "==",
                    "default_left": False,
                    "left_child": {"leaf_value": 0.1},
                    "right_child": {"leaf_value": 0.2},
                },
            },
        ],
    }


def _sample_metadata() -> dict[str, object]:
    return {
        "model_version": "jra-running-style-lgbm-prod-v3",
        "class_labels": ["nige", "senkou", "sashi", "oikomi"],
        "feature_columns": ["track_code", "kyori"],
        "categorical_features": ["track_code"],
    }


def test_build_compact_model_translates_categorical_thresholds() -> None:
    booster = cast(lgb.Booster, FakeBooster(_sample_dump()))
    compact = subject.build_compact_model(booster, _sample_metadata())
    tree_structure = cast(dict[str, object], compact["trees"][0]["tree_structure"])
    assert compact["model_version"] == "jra-running-style-lgbm-prod-v3"
    assert compact["feature_names"] == ["track_code", "kyori"]
    assert compact["categorical_features"] == ["track_code"]
    assert tree_structure["threshold"] == "11||17"


def test_build_compact_model_defaults_missing_metadata_lists() -> None:
    dump: dict[str, object] = {
        "objective": "multiclass",
        "num_class": 4,
        "num_tree_per_iteration": 4,
        "feature_names": ["kyori"],
        "tree_info": [{"tree_structure": {"leaf_value": 0.1}}],
    }
    booster = cast(lgb.Booster, FakeBooster(dump))
    compact = subject.build_compact_model(booster, {"model_version": "v"})
    assert compact["class_labels"] == []
    assert compact["categorical_features"] == []


def test_build_compact_model_raises_when_tree_info_not_a_list() -> None:
    dump: dict[str, object] = {"feature_names": ["kyori"], "tree_info": "not-a-list"}
    booster = cast(lgb.Booster, FakeBooster(dump))
    with pytest.raises(ValueError, match="tree_info missing"):
        subject.build_compact_model(booster, {})


def test_build_compact_model_raises_when_feature_names_not_a_list() -> None:
    dump: dict[str, object] = {"tree_info": [], "feature_names": "not-a-list"}
    booster = cast(lgb.Booster, FakeBooster(dump))
    with pytest.raises(ValueError, match="feature_names missing"):
        subject.build_compact_model(booster, {})


def test_write_compact_json_creates_parent_dir_and_returns_size(tmp_path: Path) -> None:
    output_path = tmp_path / "nested" / "model.json"
    compact: subject.CompactModel = {
        "model_version": "v3",
        "objective": "multiclass",
        "num_class": 4,
        "num_tree_per_iteration": 4,
        "class_labels": ["nige", "senkou", "sashi", "oikomi"],
        "feature_names": ["track_code"],
        "categorical_features": ["track_code"],
        "trees": [],
    }
    size_bytes = subject.write_compact_json(compact, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["model_version"] == "v3"
    assert size_bytes == len(output_path.read_bytes())


def test_run_export_writes_translated_model_and_returns_summary(tmp_path: Path) -> None:
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "metadata.json").write_text(json.dumps(_sample_metadata()), encoding="utf-8")
    output_path = model_dir / "model.json"
    with patch.object(lgb, "Booster", return_value=FakeBooster(_sample_dump())) as fake_ctor:
        summary = subject.run_export(model_dir, output_path)
    fake_ctor.assert_called_once_with(model_file=str(model_dir / "model.txt"))
    assert summary["model_version"] == "jra-running-style-lgbm-prod-v3"
    assert summary["num_trees"] == 1
    assert summary["num_features"] == 2
    assert summary["num_class"] == 4
    assert summary["output"] == str(output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["trees"][0]["tree_structure"]["threshold"] == "11||17"


def test_main_prints_run_export_summary_as_json(capsys: pytest.CaptureFixture[str]) -> None:
    fake_summary: dict[str, object] = {"model_version": "v3", "num_trees": 1}
    with patch.object(subject, "run_export", return_value=fake_summary) as fake_run_export:
        subject.main(["--model-dir", "tmp/models/x", "--output", "tmp/models/x/model.json"])
    fake_run_export.assert_called_once_with(Path("tmp/models/x"), Path("tmp/models/x/model.json"))
    printed = json.loads(capsys.readouterr().out)
    assert printed == fake_summary
