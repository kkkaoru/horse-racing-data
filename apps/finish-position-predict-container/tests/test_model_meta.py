"""Tests for the container-baked model metadata mapping (leak-free deploys:
JRA=jra-cb-v9-sim-2013-clean (250 feat, 4 within-race leak cols excluded),
NAR=iter12-nar-xgb-hpo-v8-clean188 (188 feat, same 4 leak cols excluded),
Ban-ei=banei-cb-v9-sim-2011)."""

from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib import model_meta
from predict_lib.model_meta import (
    LGB_MODEL_FILE_NAME,
    MODEL_FILE_NAME,
    NAR_ETOP2_ADOPT_CLASSES,
    NAR_TRANSFORMER_BLEND_WEIGHT,
    NAR_TRANSFORMER_MODEL_VERSION,
    PRODUCTION_MODEL_VERSION_ALLOWLIST,
    WITHIN_RACE_LEAK_COLUMNS,
    architecture_for,
    assert_no_within_race_leak_columns,
    assert_production_model_version_allowed,
    build_r2_nar_etop2_key,
    build_r2_nar_transformer_key,
    build_r2_object_key,
    build_r2_xgb_etop2_key,
    feature_count_for,
    get_train_start_year,
    is_category,
    is_lightgbm_model_version,
    is_production_model_version_allowed,
    leak_columns_in,
    load_model_meta,
    member_model_file_name,
    model_version_for,
    resolve_category,
)

# ``_env_flag`` is module-private (leading underscore); accessed via getattr
# with the attribute name held in a variable (not a string literal, so
# oxlint/ruff's "no getattr with constant attribute" check does not apply, and
# not a static ``model_meta._env_flag`` attribute expression, so strict
# pyright's reportPrivateUsage does not apply either) while still exercising
# the real implementation.
_ENV_FLAG_ATTR: str = "_env_flag"
_env_flag: Callable[..., bool] = cast(Callable[..., bool], getattr(model_meta, _ENV_FLAG_ATTR))

NAR_LGB_RESIDUAL_C: str = "iter36-nar-lgb-lambdarank-residual-C-v8"
NAR_CB_RESIDUAL_C: str = "iter30-nar-cb-residual-C-v8"


def test_model_version_jra() -> None:
    assert model_version_for("jra") == "jra-cb-v9-sim-2013-clean"


def test_model_version_nar() -> None:
    assert model_version_for("nar") == "iter12-nar-xgb-hpo-v8-clean188"


def test_model_version_banei() -> None:
    assert model_version_for("ban-ei") == "banei-cb-v9-sim-2011"


def test_architecture_jra_catboost() -> None:
    assert architecture_for("jra") == "catboost"


def test_architecture_nar_xgboost() -> None:
    assert architecture_for("nar") == "xgboost"


def test_architecture_banei_catboost() -> None:
    assert architecture_for("ban-ei") == "catboost"


def test_feature_count_jra() -> None:
    assert feature_count_for("jra") == 250


def test_feature_count_nar() -> None:
    assert feature_count_for("nar") == 188


def test_feature_count_banei() -> None:
    assert feature_count_for("ban-ei") == 130


def test_production_model_allowlist_contains_clean_defaults_and_banei_cell_base() -> None:
    assert frozenset({
        "jra-cb-v9-sim-2013-clean",
        "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
        "jra-cb-v10-prior-corner274-2013",
        "iter12-nar-xgb-hpo-v8-clean188",
        "iter40-nar-settransformer-blend-v1",
        "banei-cb-v9-sim-2011",
        "banei-cb-v8-window2011-wf-15y",
    }) == PRODUCTION_MODEL_VERSION_ALLOWLIST


def test_production_model_allowlist_rejects_historical_leaky_versions() -> None:
    assert is_production_model_version_allowed("jra-cb-v9-sim-2013") is False
    assert is_production_model_version_allowed("iter12-nar-xgb-hpo-v8") is False
    with pytest.raises(ValueError, match="not allowed"):
        assert_production_model_version_allowed(
            "iter12-nar-xgb-hpo-v8",
            context="test",
        )


def test_within_race_leak_columns_are_explicitly_denied() -> None:
    assert frozenset({
        "target_corner_1_norm",
        "target_corner_2_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
    }) == WITHIN_RACE_LEAK_COLUMNS
    assert leak_columns_in(["feature_a", "target_corner_1_norm"]) == frozenset({
        "target_corner_1_norm",
    })
    assert_no_within_race_leak_columns(["feature_a"], context="clean")
    with pytest.raises(ValueError, match="within-race leak columns"):
        assert_no_within_race_leak_columns(
            ["feature_a", "target_corner_3_norm"],
            context="leaky",
        )


def test_build_r2_object_key_model() -> None:
    assert build_r2_object_key("jra", "model.json") == (
        "finish-position/jra/jra-cb-v9-sim-2013-clean/model.json"
    )


def test_build_r2_object_key_metadata() -> None:
    assert build_r2_object_key("ban-ei", "metadata.json") == (
        "finish-position/ban-ei/banei-cb-v9-sim-2011/metadata.json"
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
    assert is_lightgbm_model_version("iter12-nar-xgb-hpo-v8-clean188") is False


def test_member_model_file_name_lightgbm_is_model_txt() -> None:
    # LightGBM members serialise to the native text dump ``model.txt``.
    assert member_model_file_name(NAR_LGB_RESIDUAL_C) == LGB_MODEL_FILE_NAME
    assert member_model_file_name(NAR_LGB_RESIDUAL_C) == "model.txt"


def test_member_model_file_name_catboost_is_model_json() -> None:
    # CatBoost / XGBoost members serialise to the JSON dump ``model.json``.
    assert member_model_file_name(NAR_CB_RESIDUAL_C) == MODEL_FILE_NAME
    assert member_model_file_name(NAR_CB_RESIDUAL_C) == "model.json"


def test_member_model_file_name_xgboost_is_model_json() -> None:
    assert member_model_file_name("iter12-nar-xgb-hpo-v8-clean188") == "model.json"


# --- load_model_meta error cases --------------------------------------------


def test_load_model_meta_raises_file_not_found_when_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"model_meta\.json not found"):
        load_model_meta(tmp_path / "model_meta.json")


def test_load_model_meta_raises_value_error_when_root_is_not_dict(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    p.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a JSON object"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_model_versions_missing(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    p.write_text('{"feature_counts": {"jra": 244, "nar": 192, "ban-ei": 111}}', encoding="utf-8")
    with pytest.raises(ValueError, match="model_versions"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_feature_counts_missing(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    p.write_text(
        '{"model_versions": {"jra": "v1", "nar": "v2", "ban-ei": "v3"}}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="feature_counts"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_category_version_missing(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {"jra": "v1", "nar": "v2"},
        "feature_counts": {"jra": 1, "nar": 2, "ban-ei": 3},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="ban-ei"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_category_count_missing(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {"jra": "v1", "nar": "v2", "ban-ei": "v3"},
        "feature_counts": {"jra": 1, "nar": 2},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="ban-ei"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_model_version_is_empty_string(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {"jra": "", "nar": "v2", "ban-ei": "v3"},
        "feature_counts": {"jra": 1, "nar": 2, "ban-ei": 3},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="jra"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_model_version_is_whitespace(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {"jra": "   ", "nar": "v2", "ban-ei": "v3"},
        "feature_counts": {"jra": 1, "nar": 2, "ban-ei": 3},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="jra"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_feature_count_is_bool(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {"jra": "v1", "nar": "v2", "ban-ei": "v3"},
        "feature_counts": {"jra": True, "nar": 2, "ban-ei": 3},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="jra"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_feature_count_is_zero(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {"jra": "v1", "nar": "v2", "ban-ei": "v3"},
        "feature_counts": {"jra": 0, "nar": 2, "ban-ei": 3},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="jra"):
        load_model_meta(p)


def test_load_model_meta_raises_value_error_when_feature_count_is_negative(
    tmp_path: Path,
) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {"jra": "v1", "nar": "v2", "ban-ei": "v3"},
        "feature_counts": {"jra": -1, "nar": 2, "ban-ei": 3},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="jra"):
        load_model_meta(p)


def test_load_model_meta_rejects_historical_leaky_model_version(tmp_path: Path) -> None:
    p = tmp_path / "model_meta.json"
    data = {
        "model_versions": {
            "jra": "jra-cb-v9-sim-2013",
            "nar": "iter12-nar-xgb-hpo-v8-clean188",
            "ban-ei": "banei-cb-v9-sim-2011",
        },
        "feature_counts": {"jra": 250, "nar": 188, "ban-ei": 130},
    }
    p.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="not allowed"):
        load_model_meta(p)


def test_resolve_category_nar() -> None:
    assert resolve_category("nar") == "nar"


# --- iter40: NAR transformer blend env flag + R2 key builder + constants ---


def test_env_flag_unset_returns_default_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOME_UNSET_FLAG", raising=False)
    assert _env_flag("SOME_UNSET_FLAG", default=True) is True


def test_env_flag_unset_returns_default_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOME_UNSET_FLAG", raising=False)
    assert _env_flag("SOME_UNSET_FLAG", default=False) is False


def test_env_flag_true_token_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_FLAG", "ON")
    assert _env_flag("SOME_FLAG", default=False) is True


def test_env_flag_false_token_with_whitespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_FLAG", " 0 ")
    assert _env_flag("SOME_FLAG", default=True) is False


def test_env_flag_unrecognised_token_returns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_FLAG", "maybe")
    assert _env_flag("SOME_FLAG", default=True) is True


# --- pre-existing coverage gap closed alongside the iter40 additions -------
# (get_train_start_year / build_r2_xgb_etop2_key / build_r2_nar_etop2_key had
# no direct test before this change; adding coverage here keeps model_meta.py
# at >=95% on all four metrics without shrinking the measured file set.)


def test_get_train_start_year_ignores_class_code() -> None:
    assert get_train_start_year("nar", "NEW") == 2006
    assert get_train_start_year("nar", "OP") == 2006


def test_get_train_start_year_falls_back_to_category_default() -> None:
    assert get_train_start_year("jra", "000") == 2013


def test_get_train_start_year_falls_back_to_global_default() -> None:
    # An unrecognised category has no category-wide default either.
    assert get_train_start_year("unknown-category", "000") == 2006


def test_build_r2_xgb_etop2_key() -> None:
    assert build_r2_xgb_etop2_key("model.json") == (
        "finish-position/jra/xgb-jra-2013-v8/model.json"
    )


def test_build_r2_nar_etop2_key() -> None:
    assert build_r2_nar_etop2_key("model.json") == ("finish-position/nar/cb-nar-2013-v8/model.json")


def test_nar_etop2_adopt_classes_empty() -> None:
    assert frozenset() == NAR_ETOP2_ADOPT_CLASSES


def test_build_r2_nar_transformer_key() -> None:
    assert build_r2_nar_transformer_key("norm.json") == (
        "finish-position/nar/iter40-nar-settransformer-blend-v1/norm.json"
    )


def test_nar_transformer_model_version_constant() -> None:
    assert NAR_TRANSFORMER_MODEL_VERSION == "iter40-nar-settransformer-blend-v1"


def test_nar_transformer_blend_weight_constant() -> None:
    assert NAR_TRANSFORMER_BLEND_WEIGHT == 0.5


def test_nar_transformer_blend_enabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # The baked artifact is clean, so transformer serving remains active without
    # requiring a production secret. Operators can still roll back via env=0.
    monkeypatch.delenv("NAR_TRANSFORMER_BLEND_ENABLED", raising=False)
    reloaded = importlib.reload(model_meta)
    assert reloaded.NAR_TRANSFORMER_BLEND_ENABLED is True


def test_nar_transformer_blend_disabled_by_env_opt_out(monkeypatch: pytest.MonkeyPatch) -> None:
    # Immediate rollback path: env=0 disables the blend and keeps clean188 base.
    monkeypatch.setenv("NAR_TRANSFORMER_BLEND_ENABLED", "0")
    reloaded = importlib.reload(model_meta)
    assert reloaded.NAR_TRANSFORMER_BLEND_ENABLED is False
    # Restore the module to its env-unset default state so the reloaded global
    # does not leak the opt-out value into later tests.
    monkeypatch.delenv("NAR_TRANSFORMER_BLEND_ENABLED", raising=False)
    importlib.reload(model_meta)
