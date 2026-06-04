"""Tests for the container-baked model metadata mapping (v7-lineage; v8 boosters
staged under models/ but not yet wired into the runtime feature pipeline)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.model_meta import (
    architecture_for,
    build_r2_object_key,
    feature_count_for,
    is_category,
    model_version_for,
    resolve_category,
)


def test_model_version_jra() -> None:
    assert model_version_for("jra") == "jra-cb-v7-lineage-wf-21y"


def test_model_version_nar() -> None:
    assert model_version_for("nar") == "nar-xgb-v7-lineage-wf-21y"


def test_model_version_banei() -> None:
    assert model_version_for("ban-ei") == "banei-cb-v7-lineage-wf-21y"


def test_architecture_jra_catboost() -> None:
    assert architecture_for("jra") == "catboost"


def test_architecture_nar_xgboost() -> None:
    assert architecture_for("nar") == "xgboost"


def test_architecture_banei_catboost() -> None:
    assert architecture_for("ban-ei") == "catboost"


def test_feature_count_jra() -> None:
    assert feature_count_for("jra") == 226


def test_feature_count_nar() -> None:
    assert feature_count_for("nar") == 175


def test_feature_count_banei() -> None:
    assert feature_count_for("ban-ei") == 111


def test_build_r2_object_key_model() -> None:
    assert build_r2_object_key("jra", "model.json") == (
        "finish-position/jra/jra-cb-v7-lineage-wf-21y/model.json"
    )


def test_build_r2_object_key_metadata() -> None:
    assert build_r2_object_key("ban-ei", "metadata.json") == (
        "finish-position/ban-ei/banei-cb-v7-lineage-wf-21y/metadata.json"
    )


def test_is_category_true() -> None:
    assert is_category("nar") is True


def test_is_category_false() -> None:
    assert is_category("keiba") is False


def test_resolve_category_ok() -> None:
    assert resolve_category("jra") == "jra"


def test_resolve_category_banei() -> None:
    assert resolve_category("ban-ei") == "ban-ei"


def test_resolve_category_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unsupported category"):
        resolve_category("nra")
