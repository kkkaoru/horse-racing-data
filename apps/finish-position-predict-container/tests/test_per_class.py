"""Tests for the per-class JRA model routing helpers.

The helpers are pure functions backed by ``PER_CLASS_MODEL_VERSIONS`` /
``PER_CLASS_ENABLED_CATEGORIES`` module-level dicts. Tests that need a non-empty
registry use ``pytest.MonkeyPatch.setattr`` to temporarily mutate the module
dict — the production registry is empty as of v8 iter 20 (no per-class winner
has beaten iter 14 globally on its own subset).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib import per_class
from predict_lib.per_class import (
    PER_CLASS_ENABLED_CATEGORIES,
    PER_CLASS_MODEL_VERSIONS,
    is_per_class_enabled_for,
    per_class_codes_for,
    resolve_per_class_model_version,
)

JRA_FALLBACK_MODEL_VERSION: str = "iter14-jra-cb-pacestyle-course-v8"
NAR_FALLBACK_MODEL_VERSION: str = "iter12-nar-xgb-hpo-v8"
BANEI_FALLBACK_MODEL_VERSION: str = "banei-cb-v7-lineage-wf-21y"
JRA_CLASS_005_MODEL_VERSION: str = "iter21-jra-cb-class005-v8"
JRA_CLASS_010_MODEL_VERSION: str = "iter21-jra-cb-class010-v8"


def test_per_class_model_versions_is_empty_in_production() -> None:
    assert PER_CLASS_MODEL_VERSIONS == {}


def test_per_class_enabled_categories_contains_jra_only() -> None:
    assert frozenset({"jra"}) == PER_CLASS_ENABLED_CATEGORIES


def test_is_per_class_enabled_for_jra_returns_true() -> None:
    assert is_per_class_enabled_for("jra") is True


def test_is_per_class_enabled_for_nar_returns_false() -> None:
    assert is_per_class_enabled_for("nar") is False


def test_is_per_class_enabled_for_banei_returns_false() -> None:
    assert is_per_class_enabled_for("ban-ei") is False


def test_resolve_falls_back_when_category_not_enabled() -> None:
    assert resolve_per_class_model_version("nar", "005") == NAR_FALLBACK_MODEL_VERSION


def test_resolve_falls_back_when_category_not_enabled_banei() -> None:
    assert resolve_per_class_model_version("ban-ei", "703") == BANEI_FALLBACK_MODEL_VERSION


def test_resolve_falls_back_when_kyoso_joken_code_is_none() -> None:
    assert resolve_per_class_model_version("jra", None) == JRA_FALLBACK_MODEL_VERSION


def test_resolve_falls_back_when_no_registered_model_for_code() -> None:
    assert resolve_per_class_model_version("jra", "005") == JRA_FALLBACK_MODEL_VERSION


def test_resolve_falls_back_when_no_registered_model_for_other_code() -> None:
    assert resolve_per_class_model_version("jra", "999") == JRA_FALLBACK_MODEL_VERSION


def test_resolve_returns_registered_model_when_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("jra", "005"): JRA_CLASS_005_MODEL_VERSION},
    )
    assert per_class.resolve_per_class_model_version("jra", "005") == JRA_CLASS_005_MODEL_VERSION


def test_resolve_returns_fallback_for_unregistered_code_when_registry_has_other(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("jra", "005"): JRA_CLASS_005_MODEL_VERSION},
    )
    assert per_class.resolve_per_class_model_version("jra", "010") == JRA_FALLBACK_MODEL_VERSION


def test_resolve_ignores_registry_when_category_not_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("nar", "005"): "iter21-nar-xgb-class005-v8"},
    )
    assert per_class.resolve_per_class_model_version("nar", "005") == NAR_FALLBACK_MODEL_VERSION


def test_per_class_codes_for_jra_returns_empty_when_no_registrations() -> None:
    assert per_class_codes_for("jra") == ()


def test_per_class_codes_for_nar_returns_empty_when_disabled() -> None:
    assert per_class_codes_for("nar") == ()


def test_per_class_codes_for_banei_returns_empty_when_disabled() -> None:
    assert per_class_codes_for("ban-ei") == ()


def test_per_class_codes_for_jra_returns_codes_sorted_when_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {
            ("jra", "010"): JRA_CLASS_010_MODEL_VERSION,
            ("jra", "005"): JRA_CLASS_005_MODEL_VERSION,
        },
    )
    assert per_class.per_class_codes_for("jra") == ("005", "010")


def test_per_class_codes_for_jra_filters_other_category_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {
            ("jra", "005"): JRA_CLASS_005_MODEL_VERSION,
            ("nar", "005"): "iter21-nar-xgb-class005-v8",
        },
    )
    assert per_class.per_class_codes_for("jra") == ("005",)


def test_per_class_codes_for_disabled_category_ignores_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("nar", "005"): "iter21-nar-xgb-class005-v8"},
    )
    assert per_class.per_class_codes_for("nar") == ()
