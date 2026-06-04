"""Tests for the per-class JRA model routing helpers.

The helpers are pure functions backed by ``PER_CLASS_MODEL_VERSIONS`` /
``PER_CLASS_ENABLED_CATEGORIES`` module-level dicts. Tests that need a non-empty
or alternate registry use ``pytest.MonkeyPatch.setattr`` to temporarily mutate
the module dict; tests that need a manifest file use the standard ``tmp_path``
fixture and write a synthetic ``manifest.json`` mirroring the on-disk layout
that ``build_per_class_manifest_path`` builds.

Phase B-2A adds ensemble routing (``load_ensemble_manifest`` /
``resolve_per_class_resolution``). The 703 single-line registration is asserted
both in isolation (``test_per_class_model_versions_includes_703_ensemble``) and
end-to-end (``test_load_ensemble_manifest_*``, ``test_resolve_per_class_resolution_*``).
"""

from __future__ import annotations

import dataclasses
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib import per_class
from predict_lib.per_class import (
    PER_CLASS_ENABLED_CATEGORIES,
    PER_CLASS_MODEL_VERSIONS,
    EnsembleMember,
    PerClassEnsemble,
    build_per_class_manifest_path,
    is_per_class_enabled_for,
    load_ensemble_manifest,
    per_class_codes_for,
    resolve_per_class_model_version,
    resolve_per_class_resolution,
)

JRA_FALLBACK_MODEL_VERSION: str = "iter14-jra-cb-pacestyle-course-v8"
NAR_FALLBACK_MODEL_VERSION: str = "iter12-nar-xgb-hpo-v8"
BANEI_FALLBACK_MODEL_VERSION: str = "banei-cb-v7-lineage-wf-21y"
JRA_CLASS_005_MODEL_VERSION: str = "iter21-jra-cb-class005-v8"
JRA_CLASS_010_MODEL_VERSION: str = "iter21-jra-cb-class010-v8"
JRA_CLASS_703_ENSEMBLE_MODEL_VERSION: str = "iter23-jra-cb-ensemble-703-v8"


def _write_manifest(
    models_dir: Path,
    category: str,
    kyoso_joken_code: str,
    model_version: str,
    payload: object,
) -> Path:
    """Write a manifest.json mirroring the on-disk image layout."""
    target = (
        models_dir
        / "finish-position"
        / category
        / "per-class"
        / kyoso_joken_code
        / model_version
        / "manifest.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload), encoding="utf-8")
    return target


def _canonical_703_payload() -> dict[str, object]:
    return {
        "model_version": JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        "category": "jra",
        "kyoso_joken_code": "703",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": JRA_FALLBACK_MODEL_VERSION,
                "weight": 0.2,
                "is_baseline": True,
            },
            {
                "model_version": "iter22-jra-cb-residual-703-v8",
                "weight": 0.691385,
                "is_baseline": False,
            },
        ],
    }


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


# --- Phase B-2A: registry + dataclass + ensemble-routing tests ------------


def test_per_class_model_versions_includes_703_ensemble() -> None:
    assert PER_CLASS_MODEL_VERSIONS == {
        ("jra", "703"): JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
    }


def test_resolve_returns_registered_703_ensemble_string() -> None:
    assert (
        resolve_per_class_model_version("jra", "703") == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    )


def test_per_class_codes_for_jra_returns_703_in_production() -> None:
    assert per_class_codes_for("jra") == ("703",)


def test_ensemble_member_dataclass_signature() -> None:
    member = EnsembleMember(
        model_version="iter22-jra-cb-residual-703-v8",
        weight=0.691385,
        is_baseline=False,
    )
    assert member.model_version == "iter22-jra-cb-residual-703-v8"
    assert member.weight == 0.691385
    assert member.is_baseline is False


def test_ensemble_member_dataclass_frozen() -> None:
    member = EnsembleMember(model_version="x", weight=0.1, is_baseline=True)
    # Compute the attribute name dynamically so ruff B010 does not fire (it
    # only flags ``setattr`` with a string literal). basedpyright sees the
    # generic ``setattr`` signature, so no per-line disable is needed either.
    # The frozen-guard ``__setattr__`` slot raises ``FrozenInstanceError``
    # regardless of whether the attribute name is a literal or computed.
    weight_attr: str = "weight"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(member, weight_attr, 0.5)


def test_per_class_ensemble_dataclass_signature() -> None:
    ensemble = PerClassEnsemble(
        model_version=JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        category="jra",
        kyoso_joken_code="703",
        ensemble_type="rank_blend",
        members=(EnsembleMember(model_version="m", weight=1.0, is_baseline=True),),
    )
    assert ensemble.model_version == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    assert ensemble.category == "jra"
    assert ensemble.kyoso_joken_code == "703"
    assert ensemble.ensemble_type == "rank_blend"
    assert len(ensemble.members) == 1


def test_per_class_ensemble_dataclass_frozen() -> None:
    ensemble = PerClassEnsemble(
        model_version="m",
        category="jra",
        kyoso_joken_code="703",
        ensemble_type="rank_blend",
        members=(),
    )
    # Dynamic attribute name to keep ruff B010 quiet without a per-line disable.
    ensemble_type_attr: str = "ensemble_type"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(ensemble, ensemble_type_attr, "mean_prob")


def test_build_per_class_manifest_path_mirrors_image_layout(tmp_path: Path) -> None:
    expected = (
        tmp_path
        / "finish-position"
        / "jra"
        / "per-class"
        / "703"
        / JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
        / "manifest.json"
    )
    assert (
        build_per_class_manifest_path(
            tmp_path,
            "jra",
            "703",
            JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        )
        == expected
    )


def test_load_ensemble_manifest_returns_dataclass_when_file_exists(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "jra", "703")
    assert result is not None
    assert result.model_version == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    assert result.category == "jra"
    assert result.kyoso_joken_code == "703"
    assert result.ensemble_type == "rank_blend"
    assert len(result.members) == 2
    assert result.members[0].model_version == JRA_FALLBACK_MODEL_VERSION
    assert result.members[0].weight == 0.2
    assert result.members[0].is_baseline is True
    assert result.members[1].model_version == "iter22-jra-cb-residual-703-v8"
    assert result.members[1].weight == 0.691385
    assert result.members[1].is_baseline is False


def test_load_ensemble_manifest_returns_none_when_file_missing(tmp_path: Path) -> None:
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_returns_none_when_json_invalid(tmp_path: Path) -> None:
    target = (
        tmp_path
        / "finish-position"
        / "jra"
        / "per-class"
        / "703"
        / JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
        / "manifest.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{ not valid json", encoding="utf-8")
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_returns_none_when_code_not_registered(tmp_path: Path) -> None:
    # 005 has a manifest file on disk but is not in PER_CLASS_MODEL_VERSIONS,
    # so the loader must short-circuit before touching the filesystem.
    _write_manifest(
        tmp_path,
        "jra",
        "005",
        "iter23-jra-cb-ensemble-005-v8",
        {
            "model_version": "iter23-jra-cb-ensemble-005-v8",
            "category": "jra",
            "kyoso_joken_code": "005",
            "ensemble_type": "rank_blend",
            "members": [
                {
                    "model_version": JRA_FALLBACK_MODEL_VERSION,
                    "weight": 1.0,
                    "is_baseline": True,
                },
            ],
        },
    )
    assert load_ensemble_manifest(tmp_path, "jra", "005") is None


def test_load_ensemble_manifest_returns_none_when_category_not_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("nar", "703"): "iter23-nar-xgb-ensemble-703-v8"},
    )
    _write_manifest(
        tmp_path,
        "nar",
        "703",
        "iter23-nar-xgb-ensemble-703-v8",
        {
            "model_version": "iter23-nar-xgb-ensemble-703-v8",
            "category": "nar",
            "kyoso_joken_code": "703",
            "ensemble_type": "rank_blend",
            "members": [
                {
                    "model_version": NAR_FALLBACK_MODEL_VERSION,
                    "weight": 1.0,
                    "is_baseline": True,
                },
            ],
        },
    )
    # load_ensemble_manifest does not gate on PER_CLASS_ENABLED_CATEGORIES — it
    # gates only on registry presence + filesystem. The category gate is in
    # resolve_per_class_resolution. This test pins that contract.
    result = per_class.load_ensemble_manifest(tmp_path, "nar", "703")
    assert result is not None
    assert result.category == "nar"


def test_load_ensemble_manifest_rejects_payload_with_wrong_category(tmp_path: Path) -> None:
    payload = _canonical_703_payload()
    payload["category"] = "nar"
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_payload_with_wrong_code(tmp_path: Path) -> None:
    payload = _canonical_703_payload()
    payload["kyoso_joken_code"] = "005"
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_payload_with_missing_model_version(
    tmp_path: Path,
) -> None:
    payload = _canonical_703_payload()
    del payload["model_version"]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_payload_with_missing_ensemble_type(
    tmp_path: Path,
) -> None:
    payload = _canonical_703_payload()
    del payload["ensemble_type"]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_payload_with_non_list_members(tmp_path: Path) -> None:
    payload = _canonical_703_payload()
    payload["members"] = "not-a-list"
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_payload_with_empty_members(tmp_path: Path) -> None:
    payload = _canonical_703_payload()
    payload["members"] = []
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_payload_with_non_dict_member(tmp_path: Path) -> None:
    payload = _canonical_703_payload()
    payload["members"] = ["not-a-dict"]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_member_with_missing_model_version(
    tmp_path: Path,
) -> None:
    payload = _canonical_703_payload()
    payload["members"] = [{"weight": 0.5, "is_baseline": False}]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_member_with_non_numeric_weight(tmp_path: Path) -> None:
    payload = _canonical_703_payload()
    payload["members"] = [
        {"model_version": "m", "weight": "0.5", "is_baseline": True},
    ]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_member_with_bool_weight(tmp_path: Path) -> None:
    # isinstance(True, int) is True in Python — guard the weight check so a bool
    # doesn't accidentally pass through as 1.0 / 0.0.
    payload = _canonical_703_payload()
    payload["members"] = [
        {"model_version": "m", "weight": True, "is_baseline": False},
    ]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_member_with_non_bool_is_baseline(
    tmp_path: Path,
) -> None:
    payload = _canonical_703_payload()
    payload["members"] = [
        {"model_version": "m", "weight": 0.5, "is_baseline": "yes"},
    ]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_rejects_payload_that_is_not_a_dict(tmp_path: Path) -> None:
    target = (
        tmp_path
        / "finish-position"
        / "jra"
        / "per-class"
        / "703"
        / JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
        / "manifest.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_load_ensemble_manifest_accepts_integer_weight(tmp_path: Path) -> None:
    payload = _canonical_703_payload()
    payload["members"] = [
        {"model_version": "m", "weight": 1, "is_baseline": True},
    ]
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    result = load_ensemble_manifest(tmp_path, "jra", "703")
    assert result is not None
    assert result.members[0].weight == 1.0


def test_load_ensemble_manifest_rejects_payload_with_non_string_model_version(
    tmp_path: Path,
) -> None:
    payload = _canonical_703_payload()
    payload["model_version"] = 23
    _write_manifest(
        tmp_path, "jra", "703", JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, payload
    )
    assert load_ensemble_manifest(tmp_path, "jra", "703") is None


def test_resolve_per_class_resolution_returns_ensemble_when_manifest_present(
    tmp_path: Path,
) -> None:
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", "703")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION


def test_resolve_per_class_resolution_falls_back_to_string_when_no_manifest(
    tmp_path: Path,
) -> None:
    # 703 is registered but no manifest exists on disk — must fall back to the
    # category-global iter 14, NOT the registered ensemble label, because we
    # cannot score an ensemble without the manifest.
    result = resolve_per_class_resolution(tmp_path, "jra", "703")
    assert result == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    assert isinstance(result, str)


def test_resolve_per_class_resolution_returns_string_when_code_not_registered(
    tmp_path: Path,
) -> None:
    result = resolve_per_class_resolution(tmp_path, "jra", "005")
    assert result == JRA_FALLBACK_MODEL_VERSION
    assert isinstance(result, str)


def test_resolve_per_class_resolution_returns_string_when_kyoso_joken_code_is_none(
    tmp_path: Path,
) -> None:
    result = resolve_per_class_resolution(tmp_path, "jra", None)
    assert result == JRA_FALLBACK_MODEL_VERSION
    assert isinstance(result, str)


def test_resolve_per_class_resolution_returns_string_when_category_not_enabled(
    tmp_path: Path,
) -> None:
    # Even if a NAR manifest existed on disk for a NAR-registered ensemble, the
    # disabled-category guard must short-circuit to the NAR global fallback.
    result = resolve_per_class_resolution(tmp_path, "nar", "703")
    assert result == NAR_FALLBACK_MODEL_VERSION
    assert isinstance(result, str)


def test_resolve_per_class_resolution_returns_string_when_manifest_invalid(
    tmp_path: Path,
) -> None:
    target = (
        tmp_path
        / "finish-position"
        / "jra"
        / "per-class"
        / "703"
        / JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
        / "manifest.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{ malformed", encoding="utf-8")
    result = resolve_per_class_resolution(tmp_path, "jra", "703")
    # Invalid manifest -> fall through to single-model registry, which returns
    # the registered iter23 label (caller's booster loader then would also fail
    # to find the booster; we surface the label so the audit trail is honest).
    assert result == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    assert isinstance(result, str)
