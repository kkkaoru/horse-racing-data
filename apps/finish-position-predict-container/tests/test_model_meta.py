"""Tests for the container-baked model metadata mapping (v8 production deploy:
JRA=iter20-jra-cb-2013-v8, NAR=iter12-nar-xgb-hpo-v8, Ban-ei
unchanged from v7-lineage)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.model_meta import (
    LGB_MODEL_FILE_NAME,
    MODEL_FILE_NAME,
    architecture_for,
    build_r2_object_key,
    feature_count_for,
    is_category,
    is_lightgbm_model_version,
    member_model_file_name,
    model_version_for,
    resolve_category,
)

NAR_LGB_RESIDUAL_C: str = "iter36-nar-lgb-lambdarank-residual-C-v8"
NAR_CB_RESIDUAL_C: str = "iter30-nar-cb-residual-C-v8"


def test_model_version_jra() -> None:
    assert model_version_for("jra") == "iter20-jra-cb-2013-v8"


def test_model_version_nar() -> None:
    assert model_version_for("nar") == "iter12-nar-xgb-hpo-v8"


def test_model_version_banei() -> None:
    assert model_version_for("ban-ei") == "banei-cb-v7-lineage-wf-21y"


def test_architecture_jra_catboost() -> None:
    assert architecture_for("jra") == "catboost"


def test_architecture_nar_xgboost() -> None:
    assert architecture_for("nar") == "xgboost"


def test_architecture_banei_catboost() -> None:
    assert architecture_for("ban-ei") == "catboost"


def test_feature_count_jra() -> None:
    assert feature_count_for("jra") == 244


def test_feature_count_nar() -> None:
    assert feature_count_for("nar") == 192


def test_feature_count_banei() -> None:
    assert feature_count_for("ban-ei") == 111


def test_build_r2_object_key_model() -> None:
    assert build_r2_object_key("jra", "model.json") == (
        "finish-position/jra/iter20-jra-cb-2013-v8/model.json"
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


# --- iter 36: LightGBM member model-version + artifact-file resolution -----


def test_is_lightgbm_model_version_true_for_lgb_token() -> None:
    # The iter 36 NAR class-C member carries both the ``-lgb-`` arch token and
    # the ``-lambdarank-`` objective token; either marks it as LightGBM.
    assert is_lightgbm_model_version(NAR_LGB_RESIDUAL_C) is True


def test_is_lightgbm_model_version_true_for_lambdarank_token_only() -> None:
    # A member named only with the ``-lambdarank-`` objective token (no
    # ``-lgb-``) still resolves to LightGBM — both tokens are checked.
    assert is_lightgbm_model_version("iter36-nar-lambdarank-residual-C-v8") is True


def test_is_lightgbm_model_version_false_for_catboost_member() -> None:
    # The iter 30 CatBoost residual carries ``-cb-`` and neither LightGBM token.
    assert is_lightgbm_model_version(NAR_CB_RESIDUAL_C) is False


def test_is_lightgbm_model_version_false_for_xgboost_baseline() -> None:
    assert is_lightgbm_model_version("iter12-nar-xgb-hpo-v8") is False


def test_member_model_file_name_lightgbm_is_model_txt() -> None:
    # LightGBM members serialise to the native text dump ``model.txt``.
    assert member_model_file_name(NAR_LGB_RESIDUAL_C) == LGB_MODEL_FILE_NAME
    assert member_model_file_name(NAR_LGB_RESIDUAL_C) == "model.txt"


def test_member_model_file_name_catboost_is_model_json() -> None:
    # CatBoost / XGBoost members serialise to the JSON dump ``model.json``.
    assert member_model_file_name(NAR_CB_RESIDUAL_C) == MODEL_FILE_NAME
    assert member_model_file_name(NAR_CB_RESIDUAL_C) == "model.json"


def test_member_model_file_name_xgboost_is_model_json() -> None:
    assert member_model_file_name("iter12-nar-xgb-hpo-v8") == "model.json"
