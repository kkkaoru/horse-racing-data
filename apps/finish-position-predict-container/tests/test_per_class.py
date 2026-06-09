"""Tests for the per-class JRA model routing helpers.

The helpers are pure functions backed by ``PER_CLASS_MODEL_VERSIONS`` /
``PER_CLASS_ENABLED_CATEGORIES`` module-level dicts. Tests that need a non-empty
or alternate registry use ``pytest.MonkeyPatch.setattr`` to temporarily mutate
the module dict; tests that need a manifest file use the standard ``tmp_path``
fixture and write a synthetic ``manifest.json`` mirroring the on-disk layout
that ``build_per_class_manifest_path`` builds.

Phase B-2A adds ensemble routing (``load_ensemble_manifest`` /
``resolve_per_class_resolution``). The 005 + 010 + 016 + 703 + other
registrations are asserted both in isolation
(``test_per_class_model_versions_includes_production_ensembles``) and end-to-end
(``test_load_ensemble_manifest_*``, ``test_resolve_per_class_resolution_*``).
010 was activated on 2026-06-05 (iter 25 v2 ensemble, +0.632pp top1 — the
largest per-class win in the v8 loop); 005 / 016 / 703 were re-activated the
same day with iter 26 v4 ensemble (relationship features added to the member
pool): 005 flipped from iter 25 v2 (+0.095pp) to iter 26 v4 (+0.572pp), 016
was newly activated at +0.138pp (was iter 14 fallback because v2 measured
-0.550 REJECT), 703 flipped from iter 23 (+0.142pp) to iter 26 v4 (+0.189pp).
The ``other`` catch-all (NOT IN ``{005, 010, 016, 701, 703}`` or NULL) was
activated on the iter 25 v2 ensemble (+0.094pp top1) and is routed via
``normalize_class_code`` which collapses unregistered real codes onto the
``"other"`` virtual bucket.
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
    NAMED_PER_CLASS_CODES,
    NAMED_PER_CLASS_CODES_BY_CATEGORY,
    OTHER_CLASS_CODE,
    PER_CLASS_ENABLED_CATEGORIES,
    PER_CLASS_MODEL_VERSIONS,
    EnsembleMember,
    PerClassEnsemble,
    build_per_class_manifest_path,
    is_per_class_enabled_for,
    load_ensemble_manifest,
    normalize_class_code,
    per_class_codes_for,
    resolve_per_class_model_version,
    resolve_per_class_resolution,
)

JRA_FALLBACK_MODEL_VERSION: str = "iter14-jra-cb-pacestyle-course-v8"
NAR_FALLBACK_MODEL_VERSION: str = "iter12-nar-xgb-hpo-v8"
BANEI_FALLBACK_MODEL_VERSION: str = "banei-cb-v7-lineage-wf-21y"
JRA_CLASS_005_MODEL_VERSION: str = "iter21-jra-cb-class005-v8"
JRA_CLASS_010_MODEL_VERSION: str = "iter21-jra-cb-class010-v8"
JRA_CLASS_010_ENSEMBLE_MODEL_VERSION: str = "iter25-jra-cb-ensemble-010-v8"
JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION: str = "iter25-jra-cb-ensemble-other-v8"
JRA_CLASS_005_ENSEMBLE_MODEL_VERSION: str = "iter26-jra-cb-ensemble-005-v8"
JRA_CLASS_016_ENSEMBLE_MODEL_VERSION: str = "iter26-jra-cb-ensemble-016-v8"
JRA_CLASS_703_ENSEMBLE_MODEL_VERSION: str = "iter26-jra-cb-ensemble-703-v8"
NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION: str = "iter30-nar-cb-ensemble-NEW-v8"
NAR_CLASS_MUKATSU_ENSEMBLE_MODEL_VERSION: str = "iter30-nar-cb-ensemble-MUKATSU-v8"
# iter 36 (2026-06-10) flipped NAR class C to a LightGBM-LambdaRank-member
# ensemble; the residual member below is the lgb model, not the iter 30 CatBoost.
NAR_CLASS_C_ENSEMBLE_MODEL_VERSION: str = "iter36-nar-lgb-ensemble-C-v8"
NAR_CLASS_A_ENSEMBLE_MODEL_VERSION: str = "iter30-nar-cb-ensemble-A-v8"
NAR_CLASS_OP_ENSEMBLE_MODEL_VERSION: str = "iter30-nar-cb-ensemble-OP-v8"
NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION: str = "iter30-nar-cb-ensemble-other-v8"
NAR_RESIDUAL_NEW: str = "iter30-nar-cb-residual-NEW-v8"
NAR_RESIDUAL_MUKATSU: str = "iter30-nar-cb-residual-MUKATSU-v8"
NAR_RESIDUAL_C: str = "iter36-nar-lgb-lambdarank-residual-C-v8"
NAR_RESIDUAL_A: str = "iter30-nar-cb-residual-A-v8"
NAR_RESIDUAL_OP: str = "iter30-nar-cb-residual-OP-v8"
NAR_RESIDUAL_OTHER: str = "iter30-nar-cb-residual-other-v8"


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


def _canonical_010_payload() -> dict[str, object]:
    # Mirrors the production iter 25 v2 ensemble manifest activated 2026-06-05
    # (+0.632pp top1 vs iter 14 baseline on 1583-race holdout).
    return {
        "model_version": JRA_CLASS_010_ENSEMBLE_MODEL_VERSION,
        "category": "jra",
        "kyoso_joken_code": "010",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": JRA_FALLBACK_MODEL_VERSION,
                "weight": 0.2,
                "is_baseline": True,
            },
            {
                "model_version": "iter20-jra-cb-perclass-010-v8",
                "weight": 0.070897,
                "is_baseline": False,
            },
            {
                "model_version": "iter21-jra-cb-chain-010-v8",
                "weight": 0.063353,
                "is_baseline": False,
            },
            {
                "model_version": "iter22-jra-cb-residual-010-v8",
                "weight": 0.006776,
                "is_baseline": False,
            },
            {
                "model_version": "iter25-jra-cb-low-cap-010-v8",
                "weight": 0.658974,
                "is_baseline": False,
            },
        ],
    }


def _canonical_other_payload() -> dict[str, object]:
    # Mirrors the production iter 25 v2 ensemble manifest activated 2026-06-05
    # (+0.094pp top1 vs iter 14 baseline on 1064-race holdout). iter14 carries
    # the dominant 0.637897 weight, with iter25 low-cap second at 0.195595
    # — the exact production weights.
    return {
        "model_version": JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        "category": "jra",
        "kyoso_joken_code": "other",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": JRA_FALLBACK_MODEL_VERSION,
                "weight": 0.637897,
                "is_baseline": True,
            },
            {
                "model_version": "iter20-jra-cb-perclass-other-v8",
                "weight": 0.088837,
                "is_baseline": False,
            },
            {
                "model_version": "iter21-jra-cb-chain-other-v8",
                "weight": 0.070443,
                "is_baseline": False,
            },
            {
                "model_version": "iter22-jra-cb-residual-other-v8",
                "weight": 0.007228,
                "is_baseline": False,
            },
            {
                "model_version": "iter25-jra-cb-low-cap-other-v8",
                "weight": 0.195595,
                "is_baseline": False,
            },
        ],
    }


def _canonical_005_payload() -> dict[str, object]:
    # Mirrors the production iter 26 v4 ensemble manifest activated 2026-06-05
    # (+0.572pp top1 vs iter 14 baseline on 3147-race holdout — the largest
    # 005 gain in the v8 loop, flipping the class from iter 25 v2 +0.095pp).
    # iter 26 relationship booster dominates the blend at weight 0.505597 with
    # iter 22 residual second at 0.179245 and iter 14 baseline carried at 0.2.
    return {
        "model_version": JRA_CLASS_005_ENSEMBLE_MODEL_VERSION,
        "category": "jra",
        "kyoso_joken_code": "005",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": JRA_FALLBACK_MODEL_VERSION,
                "weight": 0.2,
                "is_baseline": True,
            },
            {
                "model_version": "iter20-jra-cb-perclass-005-v8",
                "weight": 0.054137,
                "is_baseline": False,
            },
            {
                "model_version": "iter20-jra-cb-perclass-005-hpo-v8",
                "weight": 0.004774,
                "is_baseline": False,
            },
            {
                "model_version": "iter21-jra-cb-chain-005-v8",
                "weight": 0.024707,
                "is_baseline": False,
            },
            {
                "model_version": "iter22-jra-cb-residual-005-v8",
                "weight": 0.179245,
                "is_baseline": False,
            },
            {
                "model_version": "iter25-jra-cb-low-cap-005-v8",
                "weight": 0.03154,
                "is_baseline": False,
            },
            {
                "model_version": "iter26-jra-cb-relationships-005-v8",
                "weight": 0.505597,
                "is_baseline": False,
            },
        ],
    }


def _canonical_016_payload() -> dict[str, object]:
    # Mirrors the production iter 26 v4 ensemble manifest activated 2026-06-05
    # (+0.138pp top1 vs iter 14 baseline on 727-race holdout). NEW activation:
    # 016 was iter 14 fallback before (iter 25 v2 measured -0.550 REJECT).
    # iter 25 low-cap booster dominates the blend at weight 0.544315 with
    # iter 26 relationships second at 0.110428 and iter 14 baseline at 0.2.
    return {
        "model_version": JRA_CLASS_016_ENSEMBLE_MODEL_VERSION,
        "category": "jra",
        "kyoso_joken_code": "016",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": JRA_FALLBACK_MODEL_VERSION,
                "weight": 0.2,
                "is_baseline": True,
            },
            {
                "model_version": "iter20-jra-cb-perclass-016-v8",
                "weight": 0.007368,
                "is_baseline": False,
            },
            {
                "model_version": "iter21-jra-cb-chain-016-v8",
                "weight": 0.080411,
                "is_baseline": False,
            },
            {
                "model_version": "iter22-jra-cb-residual-016-v8",
                "weight": 0.057479,
                "is_baseline": False,
            },
            {
                "model_version": "iter25-jra-cb-low-cap-016-v8",
                "weight": 0.544315,
                "is_baseline": False,
            },
            {
                "model_version": "iter26-jra-cb-relationships-016-v8",
                "weight": 0.110428,
                "is_baseline": False,
            },
        ],
    }


def test_per_class_enabled_categories_contains_jra_and_nar() -> None:
    # Phase F (2026-06-05) adds NAR alongside JRA. Ban-ei stays disabled — no
    # per-class plan yet.
    assert frozenset({"jra", "nar"}) == PER_CLASS_ENABLED_CATEGORIES


def test_is_per_class_enabled_for_jra_returns_true() -> None:
    assert is_per_class_enabled_for("jra") is True


def test_is_per_class_enabled_for_nar_returns_true() -> None:
    # Phase F flipped this from False to True. NAR now routes off
    # ``nar_subclass`` to the iter 30 ensembles.
    assert is_per_class_enabled_for("nar") is True


def test_is_per_class_enabled_for_banei_returns_false() -> None:
    assert is_per_class_enabled_for("ban-ei") is False


def test_resolve_falls_back_when_category_not_enabled_banei() -> None:
    assert resolve_per_class_model_version("ban-ei", "703") == BANEI_FALLBACK_MODEL_VERSION


def test_resolve_returns_other_ensemble_when_kyoso_joken_code_is_none() -> None:
    # NULL ``kyoso_joken_code`` collapses to the ``"other"`` bucket via
    # ``normalize_class_code``; the registered ``other`` ensemble label is
    # returned. Pre-2026-06-05 this branch returned the iter14 fallback — the
    # ``other`` activation flipped this contract for the entire NULL-class
    # population in production.
    assert (
        resolve_per_class_model_version("jra", None)
        == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_falls_back_when_no_registered_model_for_named_code() -> None:
    # 701 is one of the named class codes (``NAMED_PER_CLASS_CODES``) that
    # stays unregistered; the normaliser keeps it as ``"701"`` (NOT mapped to
    # ``"other"``) so the registry lookup misses and we fall back to iter14.
    assert resolve_per_class_model_version("jra", "701") == JRA_FALLBACK_MODEL_VERSION


def test_resolve_returns_other_ensemble_for_unknown_kyoso_code() -> None:
    # ``"999"`` is not a named code, so the normaliser collapses it to the
    # ``"other"`` bucket and the registered ``other`` ensemble label is
    # returned. The same path covers ``"000"`` and any other unregistered
    # numeric code from PG.
    assert (
        resolve_per_class_model_version("jra", "999")
        == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    )


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
    # Ban-ei stays disabled even when an entry is hand-injected — the
    # ``PER_CLASS_ENABLED_CATEGORIES`` allowlist is the single switch and a
    # rogue registry row cannot route a Ban-ei race to a per-class booster.
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("ban-ei", "999"): "banei-cb-class999-v8"},
    )
    assert (
        per_class.resolve_per_class_model_version("ban-ei", "999")
        == BANEI_FALLBACK_MODEL_VERSION
    )


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
    # Ban-ei is the only disabled category in Phase F; even if a registry row
    # were injected, the gate must short-circuit to an empty tuple.
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("ban-ei", "999"): "banei-cb-class999-v8"},
    )
    assert per_class.per_class_codes_for("ban-ei") == ()


# --- Phase B-2A: registry + dataclass + ensemble-routing tests ------------


def test_per_class_model_versions_includes_production_ensembles() -> None:
    assert PER_CLASS_MODEL_VERSIONS == {
        ("jra", "005"): JRA_CLASS_005_ENSEMBLE_MODEL_VERSION,
        ("jra", "010"): JRA_CLASS_010_ENSEMBLE_MODEL_VERSION,
        ("jra", "016"): JRA_CLASS_016_ENSEMBLE_MODEL_VERSION,
        ("jra", "703"): JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        ("jra", "other"): JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        ("nar", "NEW"): NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        ("nar", "MUKATSU"): NAR_CLASS_MUKATSU_ENSEMBLE_MODEL_VERSION,
        ("nar", "C"): NAR_CLASS_C_ENSEMBLE_MODEL_VERSION,
        ("nar", "A"): NAR_CLASS_A_ENSEMBLE_MODEL_VERSION,
        ("nar", "OP"): NAR_CLASS_OP_ENSEMBLE_MODEL_VERSION,
        ("nar", "other"): NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
    }


def test_resolve_returns_registered_703_ensemble_string() -> None:
    # 703 was flipped to iter 26 v4 on 2026-06-05 (+0.189pp top1, +0.047pp
    # delta over the iter 23 ensemble).
    assert (
        resolve_per_class_model_version("jra", "703") == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_010_ensemble_string() -> None:
    # 010 was activated 2026-06-05 (iter 25 v2 ensemble, +0.632pp top1). iter 26
    # v4 was only +0.190pp on 010 so v2 stays.
    assert (
        resolve_per_class_model_version("jra", "010") == JRA_CLASS_010_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_005_ensemble_string() -> None:
    # 005 was flipped to iter 26 v4 on 2026-06-05 (+0.572pp top1, +0.477pp
    # delta over the iter 25 v2 ensemble).
    assert (
        resolve_per_class_model_version("jra", "005") == JRA_CLASS_005_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_016_ensemble_string() -> None:
    # 016 was newly activated on 2026-06-05 with iter 26 v4 (+0.138pp top1).
    # Before iter 26 it fell back to iter 14 because iter 25 v2 measured
    # -0.550 REJECT on 016.
    assert (
        resolve_per_class_model_version("jra", "016") == JRA_CLASS_016_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_other_ensemble_string() -> None:
    # ``other`` was activated 2026-06-05 (iter 25 v2 ensemble, +0.094pp top1)
    # — the literal ``"other"`` argument is the post-normalisation registry
    # key, which the resolver returns as-is for downstream symmetry.
    assert (
        resolve_per_class_model_version("jra", "other")
        == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    )


def test_per_class_codes_for_jra_returns_sorted_production_codes() -> None:
    # per_class_codes_for returns the sorted union of registered JRA codes.
    # 005, 010, 016, 703 and the virtual ``other`` bucket are the five
    # production ensembles as of 2026-06-05 (iter 26 v4 added 005 / 016 / 703
    # to the iter 25 baseline; 010 / other kept their iter 25 v2 ensembles).
    # Alphabetical sort puts ``other`` last.
    assert per_class_codes_for("jra") == ("005", "010", "016", "703", "other")


def test_per_class_codes_for_nar_returns_sorted_production_codes() -> None:
    # Phase F production NAR codes: NEW / MUKATSU / C / A / OP / other.
    # ``B`` is NOT registered (stays on iter 12 fallback) so it does not
    # appear here even though it lives in ``NAMED_PER_CLASS_CODES_BY_CATEGORY
    # ['nar']``. ASCII sort: 'A' (65) < 'C' < 'M' < 'N' < 'O' (79) < 'o'
    # (111) — capital letters precede lowercase, so ``other`` lands last.
    assert per_class_codes_for("nar") == (
        "A",
        "C",
        "MUKATSU",
        "NEW",
        "OP",
        "other",
    )


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
    # 701 has a manifest file on disk but is not in PER_CLASS_MODEL_VERSIONS,
    # so the loader must short-circuit before touching the filesystem.
    _write_manifest(
        tmp_path,
        "jra",
        "701",
        "iter23-jra-cb-ensemble-701-v8",
        {
            "model_version": "iter23-jra-cb-ensemble-701-v8",
            "category": "jra",
            "kyoso_joken_code": "701",
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
    assert load_ensemble_manifest(tmp_path, "jra", "701") is None


def test_load_ensemble_manifest_returns_none_when_category_not_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Use Ban-ei so the test stays meaningful in Phase F (NAR became enabled).
    # load_ensemble_manifest does NOT gate on PER_CLASS_ENABLED_CATEGORIES —
    # it gates only on registry presence + filesystem. The category gate is
    # in resolve_per_class_resolution. This test pins that contract: a
    # hand-injected disabled-category registry row still loads its manifest.
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {("ban-ei", "703"): "banei-cb-ensemble-703-v7-lineage"},
    )
    _write_manifest(
        tmp_path,
        "ban-ei",
        "703",
        "banei-cb-ensemble-703-v7-lineage",
        {
            "model_version": "banei-cb-ensemble-703-v7-lineage",
            "category": "ban-ei",
            "kyoso_joken_code": "703",
            "ensemble_type": "rank_blend",
            "members": [
                {
                    "model_version": BANEI_FALLBACK_MODEL_VERSION,
                    "weight": 1.0,
                    "is_baseline": True,
                },
            ],
        },
    )
    result = per_class.load_ensemble_manifest(tmp_path, "ban-ei", "703")
    assert result is not None
    assert result.category == "ban-ei"


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


def test_resolve_per_class_resolution_returns_string_when_named_code_not_registered(
    tmp_path: Path,
) -> None:
    # 701 is a named class code in ``NAMED_PER_CLASS_CODES`` but stays
    # unregistered in ``PER_CLASS_MODEL_VERSIONS``. The normaliser keeps it as
    # ``"701"`` so the registry lookup misses and we fall through to the JRA
    # category-global iter14 fallback.
    result = resolve_per_class_resolution(tmp_path, "jra", "701")
    assert result == JRA_FALLBACK_MODEL_VERSION
    assert isinstance(result, str)


def test_resolve_per_class_resolution_returns_other_label_when_kyoso_joken_code_is_none(
    tmp_path: Path,
) -> None:
    # NULL ``kyoso_joken_code`` normalises to ``"other"``. With no manifest on
    # disk in ``tmp_path``, ``load_ensemble_manifest`` returns ``None`` and we
    # fall through to ``resolve_per_class_model_version`` which returns the
    # registered ``other`` ensemble label string.
    result = resolve_per_class_resolution(tmp_path, "jra", None)
    assert result == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    assert isinstance(result, str)


def test_resolve_per_class_resolution_returns_string_when_category_not_enabled(
    tmp_path: Path,
) -> None:
    # Ban-ei stays disabled in Phase F — the guard must short-circuit to the
    # Ban-ei global fallback even when a code is provided.
    result = resolve_per_class_resolution(tmp_path, "ban-ei", "703")
    assert result == BANEI_FALLBACK_MODEL_VERSION
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


# --- Phase B-2 / iter 25: 010 ensemble registration tests ---------------


def test_load_ensemble_manifest_returns_dataclass_for_010(tmp_path: Path) -> None:
    # End-to-end: write the production iter 25 v2 manifest, ensure the loader
    # parses all five members and propagates manifest-level fields.
    _write_manifest(
        tmp_path,
        "jra",
        "010",
        JRA_CLASS_010_ENSEMBLE_MODEL_VERSION,
        _canonical_010_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "jra", "010")
    assert result is not None
    assert result.model_version == JRA_CLASS_010_ENSEMBLE_MODEL_VERSION
    assert result.category == "jra"
    assert result.kyoso_joken_code == "010"
    assert result.ensemble_type == "rank_blend"
    assert len(result.members) == 5
    assert result.members[0].model_version == JRA_FALLBACK_MODEL_VERSION
    assert result.members[0].is_baseline is True
    # iter 25 low-cap booster dominates the blend with weight 0.658974; the
    # exact value is part of the production ensemble contract.
    assert result.members[4].model_version == "iter25-jra-cb-low-cap-010-v8"
    assert result.members[4].weight == 0.658974
    assert result.members[4].is_baseline is False


def test_resolve_per_class_resolution_returns_010_ensemble_when_manifest_present(
    tmp_path: Path,
) -> None:
    _write_manifest(
        tmp_path,
        "jra",
        "010",
        JRA_CLASS_010_ENSEMBLE_MODEL_VERSION,
        _canonical_010_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", "010")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == JRA_CLASS_010_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "010"


# --- Phase B-2 / iter 25: 005 ensemble registration tests ---------------


def test_load_ensemble_manifest_returns_dataclass_for_005(tmp_path: Path) -> None:
    # End-to-end: write the production iter 26 v4 manifest, ensure the loader
    # parses all seven members and propagates manifest-level fields. iter 26
    # relationships dominates the blend at weight 0.505597, iter 22 residual
    # second at 0.179245 with iter 14 baseline carried at 0.2 — the exact
    # production weights.
    _write_manifest(
        tmp_path,
        "jra",
        "005",
        JRA_CLASS_005_ENSEMBLE_MODEL_VERSION,
        _canonical_005_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "jra", "005")
    assert result is not None
    assert result.model_version == JRA_CLASS_005_ENSEMBLE_MODEL_VERSION
    assert result.category == "jra"
    assert result.kyoso_joken_code == "005"
    assert result.ensemble_type == "rank_blend"
    assert len(result.members) == 7
    assert result.members[0].model_version == JRA_FALLBACK_MODEL_VERSION
    assert result.members[0].weight == 0.2
    assert result.members[0].is_baseline is True
    # iter 26 relationships booster sits at index 6 with weight 0.505597; the
    # exact value is part of the production ensemble contract.
    assert result.members[6].model_version == "iter26-jra-cb-relationships-005-v8"
    assert result.members[6].weight == 0.505597
    assert result.members[6].is_baseline is False


def test_resolve_per_class_resolution_returns_005_ensemble_when_manifest_present(
    tmp_path: Path,
) -> None:
    _write_manifest(
        tmp_path,
        "jra",
        "005",
        JRA_CLASS_005_ENSEMBLE_MODEL_VERSION,
        _canonical_005_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", "005")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == JRA_CLASS_005_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "005"


# --- Phase B-2 / iter 26 v4: 016 ensemble registration tests ------------


def test_load_ensemble_manifest_returns_dataclass_for_016(tmp_path: Path) -> None:
    # End-to-end: write the production iter 26 v4 manifest, ensure the loader
    # parses all six members and propagates manifest-level fields. iter 25
    # low-cap booster dominates the blend at weight 0.544315, with iter 26
    # relationships second at 0.110428 — the exact production weights from
    # the 2026-06-05 NEW 016 activation.
    _write_manifest(
        tmp_path,
        "jra",
        "016",
        JRA_CLASS_016_ENSEMBLE_MODEL_VERSION,
        _canonical_016_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "jra", "016")
    assert result is not None
    assert result.model_version == JRA_CLASS_016_ENSEMBLE_MODEL_VERSION
    assert result.category == "jra"
    assert result.kyoso_joken_code == "016"
    assert result.ensemble_type == "rank_blend"
    assert len(result.members) == 6
    assert result.members[0].model_version == JRA_FALLBACK_MODEL_VERSION
    assert result.members[0].weight == 0.2
    assert result.members[0].is_baseline is True
    # iter 25 low-cap booster sits at index 4 with weight 0.544315; the exact
    # value is part of the production ensemble contract.
    assert result.members[4].model_version == "iter25-jra-cb-low-cap-016-v8"
    assert result.members[4].weight == 0.544315
    assert result.members[4].is_baseline is False
    # iter 26 relationships booster sits at index 5 with weight 0.110428.
    assert result.members[5].model_version == "iter26-jra-cb-relationships-016-v8"
    assert result.members[5].weight == 0.110428
    assert result.members[5].is_baseline is False


def test_resolve_per_class_resolution_returns_016_ensemble_when_manifest_present(
    tmp_path: Path,
) -> None:
    _write_manifest(
        tmp_path,
        "jra",
        "016",
        JRA_CLASS_016_ENSEMBLE_MODEL_VERSION,
        _canonical_016_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", "016")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == JRA_CLASS_016_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "016"


# --- Phase B-2 / iter 25: ``other`` catch-all ensemble registration tests -


def test_named_per_class_codes_match_offline_class_filter_mask() -> None:
    # Pin the named-code allowlist to the five offline class boundaries
    # (``per_class_ensemble_lib.class_filter_mask``). Adding a new named code
    # must be a deliberate registry change — both ``NAMED_PER_CLASS_CODES``
    # and ``PER_CLASS_MODEL_VERSIONS`` would need to move together.
    assert frozenset({"005", "010", "016", "701", "703"}) == NAMED_PER_CLASS_CODES


def test_other_class_code_constant_value() -> None:
    # The virtual ``"other"`` constant is the post-normalisation registry key
    # for the catch-all bucket. Pinning the literal value keeps the offline
    # ``compute_iter20`` "other" partition aligned with the inference registry.
    assert OTHER_CLASS_CODE == "other"


def testnormalize_class_code_returns_other_when_none_jra() -> None:
    # NULL ``kyoso_joken_code`` (PG column was NULL or feature build emitted
    # ``None``) collapses to the ``"other"`` bucket regardless of category.
    assert normalize_class_code("jra", None) == "other"


def testnormalize_class_code_returns_other_when_none_nar() -> None:
    # NULL ``nar_subclass`` (e.g. a JRA / Ban-ei row passed through with NULL
    # in the column) also collapses — but only NAR's named-code set is
    # consulted before the fallback.
    assert normalize_class_code("nar", None) == "other"


def testnormalize_class_code_returns_other_when_none_banei() -> None:
    # Ban-ei has no named codes; the all-other branch maps everything,
    # including ``None``, to ``"other"``.
    assert normalize_class_code("ban-ei", None) == "other"


def testnormalize_class_code_returns_other_when_unknown_jra() -> None:
    # Real unregistered JRA codes (e.g. ``"999"``, ``"000"``) collapse to the
    # ``"other"`` bucket — same routing as NULL.
    assert normalize_class_code("jra", "999") == "other"
    assert normalize_class_code("jra", "000") == "other"


def testnormalize_class_code_returns_other_when_unknown_nar() -> None:
    # An unregistered NAR sub-class string (lowercase, mixed-case, partial
    # match etc.) collapses to ``"other"`` — only the six named NAR codes
    # pass through verbatim.
    assert normalize_class_code("nar", "x") == "other"
    assert normalize_class_code("nar", "new") == "other"  # case-sensitive


def testnormalize_class_code_returns_named_code_when_known_jra() -> None:
    # All five named JRA codes pass through verbatim — they have their own
    # class boundary in offline training so the registry routes them by
    # literal code.
    assert normalize_class_code("jra", "005") == "005"
    assert normalize_class_code("jra", "010") == "010"
    assert normalize_class_code("jra", "016") == "016"
    assert normalize_class_code("jra", "701") == "701"
    assert normalize_class_code("jra", "703") == "703"


def testnormalize_class_code_returns_named_code_when_known_nar() -> None:
    # All six named NAR sub-classes pass through verbatim. ``B`` is included
    # in the named set even though it does NOT have a registered ensemble —
    # the named-code allowlist is the offline class boundary, the registry
    # is the inference-time mapping; they are independent.
    assert normalize_class_code("nar", "NEW") == "NEW"
    assert normalize_class_code("nar", "MUKATSU") == "MUKATSU"
    assert normalize_class_code("nar", "C") == "C"
    assert normalize_class_code("nar", "B") == "B"
    assert normalize_class_code("nar", "A") == "A"
    assert normalize_class_code("nar", "OP") == "OP"


def testnormalize_class_code_jra_code_collapses_under_nar_category() -> None:
    # A JRA code passed under the NAR category is NOT a named NAR code, so
    # the normaliser collapses it to ``"other"``. Categorical isolation is
    # mandatory because ``PER_CLASS_MODEL_VERSIONS`` keys on
    # ``(category, code)`` — leaking would route NAR races to JRA models.
    assert normalize_class_code("nar", "703") == "other"


def testnormalize_class_code_passes_other_string_through() -> None:
    # The literal ``"other"`` string is its own fixed point — it is the
    # registry key so callers (and tests) can also pass it directly.
    assert normalize_class_code("jra", "other") == "other"
    assert normalize_class_code("nar", "other") == "other"
    assert normalize_class_code("ban-ei", "other") == "other"


def test_load_ensemble_manifest_returns_dataclass_for_other(tmp_path: Path) -> None:
    # End-to-end: write the production iter 25 v2 manifest under the literal
    # ``"other"`` path component, ensure the loader parses all five members
    # and propagates manifest-level fields. iter14 carries the dominant
    # 0.637897 weight, with iter25 low-cap second at 0.195595 — the exact
    # production weights.
    _write_manifest(
        tmp_path,
        "jra",
        "other",
        JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_other_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "jra", "other")
    assert result is not None
    assert result.model_version == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    assert result.category == "jra"
    assert result.kyoso_joken_code == "other"
    assert result.ensemble_type == "rank_blend"
    assert len(result.members) == 5
    assert result.members[0].model_version == JRA_FALLBACK_MODEL_VERSION
    assert result.members[0].weight == 0.637897
    assert result.members[0].is_baseline is True
    # iter 25 low-cap booster sits at index 4 with weight 0.195595; the exact
    # value is part of the production ensemble contract.
    assert result.members[4].model_version == "iter25-jra-cb-low-cap-other-v8"
    assert result.members[4].weight == 0.195595
    assert result.members[4].is_baseline is False


def test_resolve_per_class_resolution_returns_other_ensemble_for_unknown_kyoso_code(
    tmp_path: Path,
) -> None:
    # ``"999"`` is not a named code — the normaliser collapses it to ``"other"``
    # and ``load_ensemble_manifest`` reads the canonical ``other`` manifest.
    _write_manifest(
        tmp_path,
        "jra",
        "other",
        JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_other_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", "999")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "other"


def test_resolve_per_class_resolution_returns_other_ensemble_when_kyoso_code_is_none(
    tmp_path: Path,
) -> None:
    # NULL ``kyoso_joken_code`` routes through the same normalised ``"other"``
    # bucket as unknown codes — the ensemble is returned when the manifest is
    # on disk.
    _write_manifest(
        tmp_path,
        "jra",
        "other",
        JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_other_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", None)
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "other"


def test_resolve_per_class_resolution_returns_other_ensemble_when_passed_literal_other(
    tmp_path: Path,
) -> None:
    # Callers may pass the literal ``"other"`` directly (e.g. the
    # ``init_member_pool`` walker which iterates the registry). The resolver
    # treats it identically to the post-normalisation path.
    _write_manifest(
        tmp_path,
        "jra",
        "other",
        JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_other_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", "other")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "other"


def test_resolve_per_class_resolution_unregistered_named_code_skips_other_manifest(
    tmp_path: Path,
) -> None:
    # Even with the ``other`` manifest on disk, an unregistered named code
    # (016 / 701) must NOT route to ``other`` — the normaliser keeps named
    # codes verbatim. The resolver falls through to ``resolve_per_class_
    # model_version`` which misses the registry and returns iter14.
    _write_manifest(
        tmp_path,
        "jra",
        "other",
        JRA_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_other_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "jra", "701")
    assert result == JRA_FALLBACK_MODEL_VERSION
    assert isinstance(result, str)


# --- Phase F: NAR named-code routing tests ------------------------------


def test_named_per_class_codes_by_category_pins_nar_offline_subclass_set() -> None:
    # NAR named-code allowlist mirrors the offline ``nar_subclass`` regex
    # partition: ``OP / NEW / MUKATSU / A / B / C``. Adding a new sub-class
    # must move both this constant and the regex in
    # ``finish_position_features_duckdb.nar_subclass_case_sql``.
    assert NAMED_PER_CLASS_CODES_BY_CATEGORY["nar"] == frozenset(
        {"NEW", "MUKATSU", "C", "B", "A", "OP"}
    )


def test_named_per_class_codes_by_category_pins_jra_offline_class_set() -> None:
    # JRA named-code allowlist is unchanged from Phase B (pin so Phase F
    # cannot regress it).
    assert NAMED_PER_CLASS_CODES_BY_CATEGORY["jra"] == frozenset(
        {"005", "010", "016", "701", "703"}
    )


def test_named_per_class_codes_by_category_banei_is_empty() -> None:
    # Ban-ei never registers a per-class — keep the allowlist empty.
    assert NAMED_PER_CLASS_CODES_BY_CATEGORY["ban-ei"] == frozenset()


def test_named_per_class_codes_backward_compat_alias_matches_jra() -> None:
    # The legacy module-level ``NAMED_PER_CLASS_CODES`` constant (Phase B
    # callers) is preserved as an alias for the JRA entry. Tests / callers
    # that imported it before Phase F keep working unchanged.
    assert NAMED_PER_CLASS_CODES_BY_CATEGORY["jra"] == NAMED_PER_CLASS_CODES


def test_resolve_returns_registered_nar_new_ensemble_string() -> None:
    # NAR NEW was activated 2026-06-05 with the iter 30 ensemble.
    assert (
        resolve_per_class_model_version("nar", "NEW")
        == NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_nar_mukatsu_ensemble_string() -> None:
    assert (
        resolve_per_class_model_version("nar", "MUKATSU")
        == NAR_CLASS_MUKATSU_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_nar_c_ensemble_string() -> None:
    assert (
        resolve_per_class_model_version("nar", "C") == NAR_CLASS_C_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_nar_a_ensemble_string() -> None:
    assert (
        resolve_per_class_model_version("nar", "A") == NAR_CLASS_A_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_nar_op_ensemble_string() -> None:
    assert (
        resolve_per_class_model_version("nar", "OP")
        == NAR_CLASS_OP_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_returns_registered_nar_other_ensemble_string() -> None:
    # The literal ``"other"`` argument is the post-normalisation key.
    assert (
        resolve_per_class_model_version("nar", "other")
        == NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_falls_back_for_nar_b_class_unregistered() -> None:
    # ``B`` is a named NAR code (in ``NAMED_PER_CLASS_CODES_BY_CATEGORY``) but
    # is NOT in ``PER_CLASS_MODEL_VERSIONS`` — the normaliser keeps it as
    # ``"B"`` so the registry lookup misses and the resolver returns the NAR
    # category-global iter 12 fallback. Mirrors the JRA 701 fallback case.
    assert resolve_per_class_model_version("nar", "B") == NAR_FALLBACK_MODEL_VERSION


def test_resolve_routes_unknown_nar_code_to_other_ensemble() -> None:
    # Unknown sub-class strings collapse to ``"other"`` before the lookup —
    # same routing as NULL.
    assert (
        resolve_per_class_model_version("nar", "X")
        == NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    )


def test_resolve_routes_nar_null_subclass_to_other_ensemble() -> None:
    # NULL ``nar_subclass`` (NAR row with no meisho match → ``other``) hits
    # the registered ``other`` ensemble.
    assert (
        resolve_per_class_model_version("nar", None)
        == NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    )


# --- Phase F: NAR ensemble manifest load tests --------------------------


def _canonical_nar_new_payload() -> dict[str, object]:
    return {
        "model_version": NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        "category": "nar",
        "kyoso_joken_code": "NEW",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": NAR_FALLBACK_MODEL_VERSION,
                "weight": 0.689977,
                "is_baseline": True,
            },
            {
                "model_version": NAR_RESIDUAL_NEW,
                "weight": 0.310023,
                "is_baseline": False,
            },
        ],
    }


def _canonical_nar_mukatsu_payload() -> dict[str, object]:
    return {
        "model_version": NAR_CLASS_MUKATSU_ENSEMBLE_MODEL_VERSION,
        "category": "nar",
        "kyoso_joken_code": "MUKATSU",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": NAR_FALLBACK_MODEL_VERSION,
                "weight": 0.5,
                "is_baseline": True,
            },
            {
                "model_version": NAR_RESIDUAL_MUKATSU,
                "weight": 0.5,
                "is_baseline": False,
            },
        ],
    }


def _canonical_nar_c_payload() -> dict[str, object]:
    return {
        "model_version": NAR_CLASS_C_ENSEMBLE_MODEL_VERSION,
        "category": "nar",
        "kyoso_joken_code": "C",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": NAR_FALLBACK_MODEL_VERSION,
                "weight": 0.6,
                "is_baseline": True,
            },
            {
                "model_version": NAR_RESIDUAL_C,
                "weight": 0.4,
                "is_baseline": False,
            },
        ],
    }


def _canonical_nar_a_payload() -> dict[str, object]:
    return {
        "model_version": NAR_CLASS_A_ENSEMBLE_MODEL_VERSION,
        "category": "nar",
        "kyoso_joken_code": "A",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": NAR_FALLBACK_MODEL_VERSION,
                "weight": 0.55,
                "is_baseline": True,
            },
            {
                "model_version": NAR_RESIDUAL_A,
                "weight": 0.45,
                "is_baseline": False,
            },
        ],
    }


def _canonical_nar_op_payload() -> dict[str, object]:
    return {
        "model_version": NAR_CLASS_OP_ENSEMBLE_MODEL_VERSION,
        "category": "nar",
        "kyoso_joken_code": "OP",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": NAR_FALLBACK_MODEL_VERSION,
                "weight": 0.65,
                "is_baseline": True,
            },
            {
                "model_version": NAR_RESIDUAL_OP,
                "weight": 0.35,
                "is_baseline": False,
            },
        ],
    }


def _canonical_nar_other_payload() -> dict[str, object]:
    return {
        "model_version": NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        "category": "nar",
        "kyoso_joken_code": "other",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": NAR_FALLBACK_MODEL_VERSION,
                "weight": 0.45,
                "is_baseline": True,
            },
            {
                "model_version": NAR_RESIDUAL_OTHER,
                "weight": 0.55,
                "is_baseline": False,
            },
        ],
    }


def test_load_ensemble_manifest_returns_dataclass_for_nar_new(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        "nar",
        "NEW",
        NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_new_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "nar", "NEW")
    assert result is not None
    assert result.model_version == NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION
    assert result.category == "nar"
    assert result.kyoso_joken_code == "NEW"
    assert result.ensemble_type == "rank_blend"
    assert len(result.members) == 2
    assert result.members[0].model_version == NAR_FALLBACK_MODEL_VERSION
    assert result.members[0].is_baseline is True
    assert result.members[1].model_version == NAR_RESIDUAL_NEW
    assert result.members[1].is_baseline is False


def test_load_ensemble_manifest_returns_dataclass_for_nar_mukatsu(
    tmp_path: Path,
) -> None:
    _write_manifest(
        tmp_path,
        "nar",
        "MUKATSU",
        NAR_CLASS_MUKATSU_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_mukatsu_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "nar", "MUKATSU")
    assert result is not None
    assert result.kyoso_joken_code == "MUKATSU"
    assert len(result.members) == 2


def test_load_ensemble_manifest_returns_dataclass_for_nar_c(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        "nar",
        "C",
        NAR_CLASS_C_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_c_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "nar", "C")
    assert result is not None
    assert result.kyoso_joken_code == "C"
    assert result.members[1].model_version == NAR_RESIDUAL_C


def test_load_ensemble_manifest_returns_dataclass_for_nar_a(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        "nar",
        "A",
        NAR_CLASS_A_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_a_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "nar", "A")
    assert result is not None
    assert result.kyoso_joken_code == "A"
    assert result.members[1].model_version == NAR_RESIDUAL_A


def test_load_ensemble_manifest_returns_dataclass_for_nar_op(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        "nar",
        "OP",
        NAR_CLASS_OP_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_op_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "nar", "OP")
    assert result is not None
    assert result.kyoso_joken_code == "OP"
    assert result.members[1].model_version == NAR_RESIDUAL_OP


def test_load_ensemble_manifest_returns_dataclass_for_nar_other(
    tmp_path: Path,
) -> None:
    _write_manifest(
        tmp_path,
        "nar",
        "other",
        NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_other_payload(),
    )
    result = load_ensemble_manifest(tmp_path, "nar", "other")
    assert result is not None
    assert result.kyoso_joken_code == "other"
    assert result.members[1].model_version == NAR_RESIDUAL_OTHER


def test_load_ensemble_manifest_returns_none_for_nar_b_no_registry(
    tmp_path: Path,
) -> None:
    # ``B`` is a named NAR code but has no registry entry — the loader must
    # short-circuit on the registry miss before touching the filesystem.
    _write_manifest(
        tmp_path,
        "nar",
        "B",
        "iter30-nar-cb-ensemble-B-v8",
        {
            "model_version": "iter30-nar-cb-ensemble-B-v8",
            "category": "nar",
            "kyoso_joken_code": "B",
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
    assert load_ensemble_manifest(tmp_path, "nar", "B") is None


# --- Phase F: NAR resolve_per_class_resolution tests --------------------


def test_resolve_per_class_resolution_returns_nar_new_ensemble_when_manifest_present(
    tmp_path: Path,
) -> None:
    _write_manifest(
        tmp_path,
        "nar",
        "NEW",
        NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_new_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "nar", "NEW")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "NEW"


def test_resolve_per_class_resolution_returns_nar_other_ensemble_for_unknown(
    tmp_path: Path,
) -> None:
    # Unknown NAR sub-class strings collapse to ``"other"`` and route to the
    # registered NAR other ensemble.
    _write_manifest(
        tmp_path,
        "nar",
        "other",
        NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_other_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "nar", "Z")
    assert isinstance(result, PerClassEnsemble)
    assert result.model_version == NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION
    assert result.kyoso_joken_code == "other"


def test_resolve_per_class_resolution_nar_b_falls_back_to_iter12(
    tmp_path: Path,
) -> None:
    # ``B`` is named but unregistered — even with ``other`` manifest on disk
    # the normaliser keeps it as ``"B"`` so the registry miss falls through to
    # the NAR iter 12 fallback.
    _write_manifest(
        tmp_path,
        "nar",
        "other",
        NAR_CLASS_OTHER_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_other_payload(),
    )
    result = resolve_per_class_resolution(tmp_path, "nar", "B")
    assert result == NAR_FALLBACK_MODEL_VERSION
    assert isinstance(result, str)


def test_resolve_per_class_resolution_banei_stays_on_fallback(
    tmp_path: Path,
) -> None:
    # Ban-ei is still disabled in Phase F — never returns an ensemble.
    result = resolve_per_class_resolution(tmp_path, "ban-ei", "703")
    assert result == BANEI_FALLBACK_MODEL_VERSION
    assert isinstance(result, str)
