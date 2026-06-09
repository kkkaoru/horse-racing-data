"""Tests for per-race ensemble routing (Phase B-2E + Phase F).

Covers ``init_member_pool`` (startup walk of the per-class registry +
manifest loader) and ``score_race_with_resolution`` (per-race scoring path
that picks single-model vs ensemble vs fallback). All native CatBoost /
XGBoost dependencies are stubbed via ``BoosterLike`` doubles so the tests
stay I/O-free outside of ``tmp_path`` manifest writes.

Wave-2 production-safety contract: any failure inside the ensemble path
(missing member, scoring exception, shape mismatch, blend rejection) must
fall through to the category-global booster with the global ``model_version``
label so the daily prediction job never crashes on a corrupt per-class
artefact.

Phase F (2026-06-05) adds NAR per-class routing on top: six NAR sub-classes
(NEW / MUKATSU / C / A / OP / other) ship with iter 30 ensembles that blend
the iter 12 XGBoost baseline with iter 30 CatBoost residual members. The
pool is now architecture-aware so a single pool can serve mixed-arch NAR
ensembles without dropping accuracy through a wrong-dtype matrix.
"""

from __future__ import annotations

import dataclasses
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib import per_class
from predict_lib.booster_pool import BoosterPool, PoolBooster
from predict_lib.ensemble_routing import (
    SCORE_FEATURE_BY_CATEGORY,
    EnsembleRouteOutcome,
    MatrixCacheKey,
    augment_entries_with_score_col,
    catboost_model_feature_names,
    column_gap,
    drop_order_mismatched_members,
    find_baseline_member,
    init_member_pool,
    member_feature_names_for_record,
    member_feature_order_matches,
    score_member,
    score_race_with_resolution,
)
from predict_lib.model_meta import Architecture
from predict_lib.per_class import EnsembleMember, PerClassEnsemble
from predict_lib.scorer import BoosterLike

JRA_FALLBACK_MODEL_VERSION: str = "iter14-jra-cb-pacestyle-course-v8"
# Mirrors the registry entry in ``predict_lib.per_class.PER_CLASS_MODEL_VERSIONS``
# — 703 was flipped from iter 23 to iter 26 v4 on 2026-06-05 (+0.189pp top1).
JRA_CLASS_703_ENSEMBLE_MODEL_VERSION: str = "iter26-jra-cb-ensemble-703-v8"
ITER22_RESIDUAL_703: str = "iter22-jra-cb-residual-703-v8"
NAR_FALLBACK_MODEL_VERSION: str = "iter12-nar-xgb-hpo-v8"
NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION: str = "iter30-nar-cb-ensemble-NEW-v8"
NAR_RESIDUAL_NEW: str = "iter30-nar-cb-residual-NEW-v8"
# iter 36 NAR class-C: ensemble label + the LightGBM LambdaRank residual member.
NAR_CLASS_C_ENSEMBLE_MODEL_VERSION: str = "iter36-nar-lgb-ensemble-C-v8"
NAR_LGB_RESIDUAL_C: str = "iter36-nar-lgb-lambdarank-residual-C-v8"


# ---------------------------------------------------------------------------
# Doubles


class _StubBooster:
    """``BoosterLike`` stub returning a deterministic per-row score.

    Each booster is initialised with an ``offset`` so two members produce
    different score vectors over the same matrix, letting the test assert that
    the blend actually mixes both members and is not silently picking one.
    """

    _offset: float

    def __init__(self, offset: float) -> None:
        self._offset = offset

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        return [self._offset + float(index) for index, _ in enumerate(matrix)]


class _RaisingBooster:
    """Booster whose ``predict`` always raises, for failure-path testing."""

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        message = f"intentional failure on matrix size {len(matrix)}"
        raise RuntimeError(message)


class _WrongLengthBooster:
    """Booster that returns a vector of the wrong length, to exercise the shape
    guard in ``_score_ensemble``."""

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        return [0.5]


class _NamedBooster:
    """CatBoost-flavoured stub exposing ``feature_names_`` like the native model.

    The native CatBoost JSON booster populates ``feature_names_`` with the exact
    positional float-feature order. The order-mismatch guard compares it against
    the metadata-derived order, so this stub lets tests pin both the matching
    (kept) and disagreeing (dropped) postures."""

    feature_names_: list[str]
    _offset: float

    def __init__(self, feature_names: Sequence[str], offset: float = 0.0) -> None:
        self.feature_names_ = list(feature_names)
        self._offset = offset

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        return [self._offset + float(index) for index, _ in enumerate(matrix)]


class _FixedScoreBooster:
    """Booster returning a fixed, caller-supplied raw score per row.

    The baseline's RAW scores are injected verbatim (no normalisation) into the
    augmented entries, so the two-pass injection test uses this to control the
    exact float values that must reappear in the residual member's matrix."""

    _raw_scores: list[float]

    def __init__(self, raw_scores: Sequence[float]) -> None:
        self._raw_scores = list(raw_scores)

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        return [self._raw_scores[index] for index, _ in enumerate(matrix)]


class _MatrixRecordingBooster:
    """Booster that records every matrix it was scored against.

    Lets the two-pass injection test assert the residual member's matrix carries
    the baseline's raw scores in the injected score-column position, and that the
    matrix cache builds a separate matrix per distinct ``(arch, feature order)``
    key. ``predict`` returns ascending per-row scores so the blend still
    produces a length-aligned vector."""

    seen_matrices: list[list[list[float]]]

    def __init__(self) -> None:
        self.seen_matrices = []

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        self.seen_matrices.append([[float(cell) for cell in row] for row in matrix])
        return [float(index) for index, _ in enumerate(matrix)]


def _cb_record(booster: BoosterLike) -> PoolBooster:
    """Wrap a CatBoost-flavoured stub in a ``PoolBooster`` record."""
    return PoolBooster(booster=booster, architecture="catboost")


def _xgb_record(booster: BoosterLike) -> PoolBooster:
    """Wrap an XGBoost-flavoured stub in a ``PoolBooster`` record."""
    return PoolBooster(booster=booster, architecture="xgboost")


def _lgb_record(booster: BoosterLike) -> PoolBooster:
    """Wrap a LightGBM-flavoured stub in a ``PoolBooster`` record (iter 36 NAR
    class-C LambdaRank residual member)."""
    return PoolBooster(booster=booster, architecture="lightgbm")


def _cb_record_with_names(
    booster: BoosterLike,
    feature_names: Sequence[str],
) -> PoolBooster:
    """Wrap a CatBoost stub carrying its OWN ordered ``feature_names`` so the
    scorer projects entries onto that member-specific order."""
    return PoolBooster(
        booster=booster, architecture="catboost", feature_names=tuple(feature_names)
    )


def _write_manifest(
    models_dir: Path,
    category: str,
    kyoso_joken_code: str,
    model_version: str,
    payload: object,
) -> Path:
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


def _write_member_model_json(
    models_dir: Path,
    category: str,
    kyoso_joken_code: str,
    model_version: str,
) -> Path:
    """Write the per-class member's ``model.json`` mirror so
    ``discover_member_models`` finds it."""
    target = (
        models_dir
        / "finish-position"
        / category
        / "per-class"
        / kyoso_joken_code
        / model_version
        / "model.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{}", encoding="utf-8")
    return target


def _write_member_model_txt(
    models_dir: Path,
    category: str,
    kyoso_joken_code: str,
    model_version: str,
) -> Path:
    """Write a LightGBM per-class member's ``model.txt`` mirror so
    ``discover_member_models`` finds the native-text artifact (lgb members
    serialise to ``model.txt`` rather than the CatBoost / XGBoost
    ``model.json``)."""
    target = (
        models_dir
        / "finish-position"
        / category
        / "per-class"
        / kyoso_joken_code
        / model_version
        / "model.txt"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("tree\n", encoding="utf-8")
    return target


def _write_baseline_model_json(
    models_dir: Path,
    category: str,
    model_version: str,
) -> Path:
    """Write the category-global baseline ``model.json`` at the canonical
    single-model layout so ``discover_baseline_member_model`` finds it."""
    target = models_dir / "finish-position" / category / model_version / "model.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{}", encoding="utf-8")
    return target


def _write_member_metadata_json(
    models_dir: Path,
    category: str,
    kyoso_joken_code: str,
    model_version: str,
    payload: object,
) -> Path:
    """Write a per-class member's sibling ``metadata.json`` carrying
    ``feature_names``.

    ``init_member_pool`` now reads this file next to every member ``model.json``
    so the scorer can project entries onto the member's OWN column order; a
    member without it is skipped (non-baseline) or raises (baseline). The
    payload is written verbatim so tests can pass a malformed shape to exercise
    the corrupt-metadata posture."""
    target = (
        models_dir
        / "finish-position"
        / category
        / "per-class"
        / kyoso_joken_code
        / model_version
        / "metadata.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload), encoding="utf-8")
    return target


def _write_baseline_metadata_json(
    models_dir: Path,
    category: str,
    model_version: str,
    payload: object,
) -> Path:
    """Write the category-global baseline's sibling ``metadata.json`` at the
    canonical single-model layout so ``_resolve_member_feature_names`` finds it
    next to the baseline ``model.json``."""
    target = models_dir / "finish-position" / category / model_version / "metadata.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload), encoding="utf-8")
    return target


def _feature_names_payload(feature_names: Sequence[str]) -> dict[str, object]:
    """Build the metadata.json body ``init_member_pool`` reads (``feature_names``
    list only — the scorer ignores the rest of the metadata)."""
    return {"feature_names": list(feature_names)}


def _canonical_703_payload() -> dict[str, object]:
    return {
        "model_version": JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        "category": "jra",
        "kyoso_joken_code": "703",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": JRA_FALLBACK_MODEL_VERSION,
                "weight": 0.3,
                "is_baseline": True,
            },
            {
                "model_version": ITER22_RESIDUAL_703,
                "weight": 0.7,
                "is_baseline": False,
            },
        ],
    }


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


def _canonical_nar_c_payload() -> dict[str, object]:
    """iter 36 NAR class-C manifest: XGBoost baseline + LightGBM LambdaRank
    residual member."""
    return {
        "model_version": NAR_CLASS_C_ENSEMBLE_MODEL_VERSION,
        "category": "nar",
        "kyoso_joken_code": "C",
        "ensemble_type": "rank_blend",
        "members": [
            {
                "model_version": NAR_FALLBACK_MODEL_VERSION,
                "weight": 0.55,
                "is_baseline": True,
            },
            {
                "model_version": NAR_LGB_RESIDUAL_C,
                "weight": 0.45,
                "is_baseline": False,
            },
        ],
    }


def _nar_c_ensemble() -> PerClassEnsemble:
    return PerClassEnsemble(
        model_version=NAR_CLASS_C_ENSEMBLE_MODEL_VERSION,
        category="nar",
        kyoso_joken_code="C",
        ensemble_type="rank_blend",
        members=(
            EnsembleMember(
                model_version=NAR_FALLBACK_MODEL_VERSION,
                weight=0.55,
                is_baseline=True,
            ),
            EnsembleMember(
                model_version=NAR_LGB_RESIDUAL_C,
                weight=0.45,
                is_baseline=False,
            ),
        ),
    )


def _two_member_ensemble() -> PerClassEnsemble:
    return PerClassEnsemble(
        model_version=JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        category="jra",
        kyoso_joken_code="703",
        ensemble_type="rank_blend",
        members=(
            EnsembleMember(
                model_version=JRA_FALLBACK_MODEL_VERSION, weight=0.3, is_baseline=True
            ),
            EnsembleMember(
                model_version=ITER22_RESIDUAL_703, weight=0.7, is_baseline=False
            ),
        ),
    )


def _nar_new_ensemble() -> PerClassEnsemble:
    return PerClassEnsemble(
        model_version=NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        category="nar",
        kyoso_joken_code="NEW",
        ensemble_type="rank_blend",
        members=(
            EnsembleMember(
                model_version=NAR_FALLBACK_MODEL_VERSION,
                weight=0.689977,
                is_baseline=True,
            ),
            EnsembleMember(
                model_version=NAR_RESIDUAL_NEW,
                weight=0.310023,
                is_baseline=False,
            ),
        ),
    )


def _three_horse_entries() -> list[dict[str, object]]:
    return [
        {"ketto_toroku_bango": "9001", "umaban": 1, "feature_a": 0.1, "feature_b": 0.4},
        {"ketto_toroku_bango": "9002", "umaban": 2, "feature_a": 0.2, "feature_b": 0.5},
        {"ketto_toroku_bango": "9003", "umaban": 3, "feature_a": 0.3, "feature_b": 0.6},
    ]


FEATURE_NAMES: list[str] = ["feature_a", "feature_b"]
# Per-member ordered feature list written into each member's metadata.json
# sidecar. The baseline trains on the plain features; the residual member adds
# the injected ``iter14_score`` (JRA) column the two-pass scorer supplies. The
# metadata order must agree with the member booster's ``feature_names_`` or the
# order-mismatch guard drops it.
BASELINE_METADATA_FEATURE_NAMES: list[str] = ["feature_a", "feature_b"]
RESIDUAL_METADATA_FEATURE_NAMES: list[str] = ["feature_a", "feature_b", "iter14_score"]
NAR_BASELINE_METADATA_FEATURE_NAMES: list[str] = ["feature_a", "feature_b"]
NAR_RESIDUAL_METADATA_FEATURE_NAMES: list[str] = [
    "feature_a",
    "feature_b",
    "iter12_score",
]
# The iter 36 LightGBM LambdaRank residual is trained with the injected NAR
# baseline score (``iter12_score``) just like the iter 30 CatBoost residual.
NAR_LGB_RESIDUAL_METADATA_FEATURE_NAMES: list[str] = [
    "feature_a",
    "feature_b",
    "iter12_score",
]
JRA_SCORE_COL: str = "iter14_score"


# ---------------------------------------------------------------------------
# Adapter stubs


class _FakeAdapter(ModuleType):
    """Typed stand-in for ``catboost_adapter`` / ``xgboost_adapter`` /
    ``lightgbm_adapter`` modules.

    All three attributes are declared so basedpyright stays quiet on the
    assignment; only one is actually set per test. The class attribute typing
    matches the real loaders' signatures.
    """

    load_catboost_booster: object
    load_xgboost_booster: object
    load_lightgbm_booster: object


def _install_fake_catboost_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_load: object,
) -> None:
    """Inject a stub ``catboost_adapter`` module on ``sys.modules``."""
    fake_module = _FakeAdapter("catboost_adapter")
    fake_module.load_catboost_booster = fake_load
    monkeypatch.setitem(sys.modules, "catboost_adapter", fake_module)


def _install_fake_xgboost_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_load: object,
) -> None:
    """Inject a stub ``xgboost_adapter`` module on ``sys.modules``."""
    fake_module = _FakeAdapter("xgboost_adapter")
    fake_module.load_xgboost_booster = fake_load
    monkeypatch.setitem(sys.modules, "xgboost_adapter", fake_module)


def _install_fake_lightgbm_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_load: object,
) -> None:
    """Inject a stub ``lightgbm_adapter`` module on ``sys.modules`` so the iter 36
    NAR class-C LightGBM member loads through the test double."""
    fake_module = _FakeAdapter("lightgbm_adapter")
    fake_module.load_lightgbm_booster = fake_load
    monkeypatch.setitem(sys.modules, "lightgbm_adapter", fake_module)


# ---------------------------------------------------------------------------
# init_member_pool — JRA path


def test_init_member_pool_loads_registered_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: registry entry + manifest + on-disk members -> populated pool."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    _write_baseline_model_json(tmp_path, "jra", JRA_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "jra",
        JRA_FALLBACK_MODEL_VERSION,
        _feature_names_payload(BASELINE_METADATA_FEATURE_NAMES),
    )
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)
    _write_member_metadata_json(
        tmp_path,
        "jra",
        "703",
        ITER22_RESIDUAL_703,
        _feature_names_payload(RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0 if "iter14" in model_path else 0.5)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    pool = init_member_pool(tmp_path, "jra")

    assert pool.has(JRA_FALLBACK_MODEL_VERSION) is True
    assert pool.has(ITER22_RESIDUAL_703) is True
    # Sorted: 'iter14-' (...) before 'iter22-' (...).
    assert pool.model_versions() == (JRA_FALLBACK_MODEL_VERSION, ITER22_RESIDUAL_703)
    # Both JRA members are CatBoost.
    baseline_record = pool.get_record(JRA_FALLBACK_MODEL_VERSION)
    residual_record = pool.get_record(ITER22_RESIDUAL_703)
    assert baseline_record is not None
    assert residual_record is not None
    assert baseline_record.architecture == "catboost"
    assert residual_record.architecture == "catboost"
    # Each member's metadata-derived feature order is stored on its record.
    assert baseline_record.feature_names == tuple(BASELINE_METADATA_FEATURE_NAMES)
    assert residual_record.feature_names == tuple(RESIDUAL_METADATA_FEATURE_NAMES)


def test_init_member_pool_empty_when_no_registry_entry(
    tmp_path: Path,
) -> None:
    """Categories with no registered ensembles (Ban-ei) -> empty pool. Phase F
    flipped NAR to enabled, so it now has registered ensembles too — tested
    separately below."""
    pool_banei = init_member_pool(tmp_path, "ban-ei")

    assert pool_banei.model_versions() == ()


def test_init_member_pool_skips_when_manifest_missing(tmp_path: Path) -> None:
    """Registry entry but no manifest on disk -> empty pool (no booster ask)."""
    pool = init_member_pool(tmp_path, "jra")

    assert pool.model_versions() == ()


def test_init_member_pool_filters_out_missing_member_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest lists two members but only the per-class residual is on disk
    (baseline absent at the category root) -> pool has only the residual."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    # Only the per-class residual is on disk.
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)
    _write_member_metadata_json(
        tmp_path,
        "jra",
        "703",
        ITER22_RESIDUAL_703,
        _feature_names_payload(RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.5)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    pool = init_member_pool(tmp_path, "jra")

    assert pool.has(ITER22_RESIDUAL_703) is True
    assert pool.has(JRA_FALLBACK_MODEL_VERSION) is False


def test_init_member_pool_skips_other_categories_registry_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Iterating the registry, entries for other categories are skipped — the
    pool requested for ``category`` only loads its own members."""
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {
            ("jra", "703"): JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
            ("nar", "NEW"): NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        },
    )
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    _write_baseline_model_json(tmp_path, "jra", JRA_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "jra",
        JRA_FALLBACK_MODEL_VERSION,
        _feature_names_payload(BASELINE_METADATA_FEATURE_NAMES),
    )
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)
    _write_member_metadata_json(
        tmp_path,
        "jra",
        "703",
        ITER22_RESIDUAL_703,
        _feature_names_payload(RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    pool = init_member_pool(tmp_path, "jra")

    # NAR's registered entry never produced a manifest load and never inflated
    # the JRA pool.
    assert pool.model_versions() == (JRA_FALLBACK_MODEL_VERSION, ITER22_RESIDUAL_703)


# ---------------------------------------------------------------------------
# init_member_pool — NAR path (Phase F)


def test_init_member_pool_loads_nar_mixed_arch_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NAR per-class members blend an XGBoost baseline (iter 12) with a
    CatBoost residual (iter 30). The pool must record the right arch per
    member so the scorer routes each to the matching feature-matrix dtype.
    Confirms the architecture-aware walker bound up in
    :func:`predict_lib.ensemble_routing.init_member_pool`."""
    _write_manifest(
        tmp_path,
        "nar",
        "NEW",
        NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_new_payload(),
    )
    _write_baseline_model_json(tmp_path, "nar", NAR_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "nar",
        NAR_FALLBACK_MODEL_VERSION,
        _feature_names_payload(NAR_BASELINE_METADATA_FEATURE_NAMES),
    )
    _write_member_model_json(tmp_path, "nar", "NEW", NAR_RESIDUAL_NEW)
    _write_member_metadata_json(
        tmp_path,
        "nar",
        "NEW",
        NAR_RESIDUAL_NEW,
        _feature_names_payload(NAR_RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_catboost(model_path: str) -> BoosterLike:
        return _StubBooster(0.5)

    def fake_xgboost(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    _install_fake_catboost_adapter(monkeypatch, fake_catboost)
    _install_fake_xgboost_adapter(monkeypatch, fake_xgboost)

    pool = init_member_pool(tmp_path, "nar")

    baseline_record = pool.get_record(NAR_FALLBACK_MODEL_VERSION)
    residual_record = pool.get_record(NAR_RESIDUAL_NEW)
    assert baseline_record is not None
    assert residual_record is not None
    # The model_version naming convention pins the arch.
    assert baseline_record.architecture == "xgboost"
    assert residual_record.architecture == "catboost"
    # Each member's metadata-derived feature order is stored on its record.
    assert baseline_record.feature_names == tuple(NAR_BASELINE_METADATA_FEATURE_NAMES)
    assert residual_record.feature_names == tuple(NAR_RESIDUAL_METADATA_FEATURE_NAMES)


def test_init_member_pool_nar_loads_baseline_only_once_when_perclass_dup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the NAR baseline is ALSO present under per-class/<code>/<baseline_mv>/
    (e.g. an offline ensemble drop that placed the baseline in both layouts),
    the per-class copy wins and the category-root copy is skipped — the pool
    only records the baseline once."""
    _write_manifest(
        tmp_path,
        "nar",
        "NEW",
        NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_new_payload(),
    )
    # Both layouts hold the baseline (model.json + metadata.json sidecar each).
    _write_baseline_model_json(tmp_path, "nar", NAR_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "nar",
        NAR_FALLBACK_MODEL_VERSION,
        _feature_names_payload(NAR_BASELINE_METADATA_FEATURE_NAMES),
    )
    _write_member_model_json(tmp_path, "nar", "NEW", NAR_FALLBACK_MODEL_VERSION)
    _write_member_metadata_json(
        tmp_path,
        "nar",
        "NEW",
        NAR_FALLBACK_MODEL_VERSION,
        _feature_names_payload(NAR_BASELINE_METADATA_FEATURE_NAMES),
    )
    _write_member_model_json(tmp_path, "nar", "NEW", NAR_RESIDUAL_NEW)
    _write_member_metadata_json(
        tmp_path,
        "nar",
        "NEW",
        NAR_RESIDUAL_NEW,
        _feature_names_payload(NAR_RESIDUAL_METADATA_FEATURE_NAMES),
    )

    captured_paths: list[str] = []

    def fake_catboost(model_path: str) -> BoosterLike:
        captured_paths.append(model_path)
        return _StubBooster(0.5)

    def fake_xgboost(model_path: str) -> BoosterLike:
        captured_paths.append(model_path)
        return _StubBooster(0.0)

    _install_fake_catboost_adapter(monkeypatch, fake_catboost)
    _install_fake_xgboost_adapter(monkeypatch, fake_xgboost)

    pool = init_member_pool(tmp_path, "nar")

    assert pool.has(NAR_FALLBACK_MODEL_VERSION) is True
    assert pool.has(NAR_RESIDUAL_NEW) is True
    # The baseline path was loaded exactly once — per-class copy.
    baseline_loads = [p for p in captured_paths if NAR_FALLBACK_MODEL_VERSION in p]
    assert len(baseline_loads) == 1
    assert "per-class" in baseline_loads[0]


def test_init_member_pool_nar_skips_baseline_when_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The NAR baseline lives at the category root (NOT under per-class/) —
    when that file is missing it must NOT show up in the pool, and the per-
    class residual still loads. Mirrors the JRA missing-member test for the
    Phase F baseline path."""
    _write_manifest(
        tmp_path,
        "nar",
        "NEW",
        NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_new_payload(),
    )
    _write_member_model_json(tmp_path, "nar", "NEW", NAR_RESIDUAL_NEW)
    _write_member_metadata_json(
        tmp_path,
        "nar",
        "NEW",
        NAR_RESIDUAL_NEW,
        _feature_names_payload(NAR_RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_catboost(model_path: str) -> BoosterLike:
        return _StubBooster(0.5)

    _install_fake_catboost_adapter(monkeypatch, fake_catboost)

    pool = init_member_pool(tmp_path, "nar")

    assert pool.has(NAR_RESIDUAL_NEW) is True
    assert pool.has(NAR_FALLBACK_MODEL_VERSION) is False


def test_init_member_pool_loads_nar_lightgbm_member(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """iter 36 NAR class-C blends an XGBoost baseline (iter 12) with a LightGBM
    LambdaRank residual (iter 36). The walker must discover the residual's
    ``model.txt`` (NOT ``model.json``), load it through the LightGBM adapter,
    and record the ``lightgbm`` arch so the scorer routes it to the float64
    matrix path. End-to-end pin of the iter 36 container half."""
    _write_manifest(
        tmp_path,
        "nar",
        "C",
        NAR_CLASS_C_ENSEMBLE_MODEL_VERSION,
        _canonical_nar_c_payload(),
    )
    _write_baseline_model_json(tmp_path, "nar", NAR_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "nar",
        NAR_FALLBACK_MODEL_VERSION,
        _feature_names_payload(NAR_BASELINE_METADATA_FEATURE_NAMES),
    )
    # The lgb residual serialises to model.txt (NOT model.json).
    _write_member_model_txt(tmp_path, "nar", "C", NAR_LGB_RESIDUAL_C)
    _write_member_metadata_json(
        tmp_path,
        "nar",
        "C",
        NAR_LGB_RESIDUAL_C,
        _feature_names_payload(NAR_LGB_RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_xgboost(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    def fake_lightgbm(model_path: str) -> BoosterLike:
        return _StubBooster(0.5)

    _install_fake_xgboost_adapter(monkeypatch, fake_xgboost)
    _install_fake_lightgbm_adapter(monkeypatch, fake_lightgbm)

    pool = init_member_pool(tmp_path, "nar")

    baseline_record = pool.get_record(NAR_FALLBACK_MODEL_VERSION)
    lgb_record = pool.get_record(NAR_LGB_RESIDUAL_C)
    assert baseline_record is not None
    assert lgb_record is not None
    assert baseline_record.architecture == "xgboost"
    assert lgb_record.architecture == "lightgbm"
    # The lgb member's metadata-derived feature order is stored on its record.
    assert lgb_record.feature_names == tuple(NAR_LGB_RESIDUAL_METADATA_FEATURE_NAMES)


# ---------------------------------------------------------------------------
# EnsembleRouteOutcome dataclass


def test_ensemble_route_outcome_is_frozen() -> None:
    outcome = EnsembleRouteOutcome(scores=[0.1, 0.9], model_version="x", fallback_reason=None)
    attr_name: str = "model_version"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(outcome, attr_name, "y")


# ---------------------------------------------------------------------------
# score_race_with_resolution — single-model path


def test_score_race_with_resolution_single_model_path_uses_resolution_string() -> None:
    """When resolution is a string the single-model path runs and the string is
    written through as the prediction's model_version verbatim."""
    fallback = _StubBooster(0.4)
    pool = BoosterPool(boosters={})
    entries = _three_horse_entries()

    outcome = score_race_with_resolution(
        resolution=JRA_FALLBACK_MODEL_VERSION,
        race_id="jra:2026:0605:05:08",
        entries=entries,
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason is None
    # Stub emits offset + index for each row.
    assert outcome.scores == [0.4, 1.4, 2.4]


def test_score_race_with_resolution_single_model_emits_custom_string() -> None:
    """Even when a registered per-class single-model label like
    ``iter26-jra-cb-ensemble-703-v8`` is passed as the resolution string (no
    manifest on disk), the outcome carries that label through unchanged."""
    fallback = _StubBooster(0.0)
    pool = BoosterPool(boosters={})

    outcome = score_race_with_resolution(
        resolution=JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    assert outcome.fallback_reason is None


# ---------------------------------------------------------------------------
# score_race_with_resolution — ensemble happy path


def test_score_race_with_resolution_ensemble_happy_path() -> None:
    """Both members in pool, predict succeeds, blend produces an aligned vector:
    outcome carries the ensemble label and no fallback reason."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0)),
            ITER22_RESIDUAL_703: _cb_record(_StubBooster(1.0)),
        }
    )
    fallback = _StubBooster(99.0)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    assert outcome.fallback_reason is None
    # Both members produce ascending offsets per row, so the within-race
    # normalisation is the same shape for each: top horse -> 1.0, bottom -> 0.0.
    # The weighted blend over identical normalised vectors equals that vector.
    assert outcome.scores == [0.0, 0.5, 1.0]


def test_score_race_with_resolution_nar_mixed_arch_ensemble_happy_path() -> None:
    """A NAR ensemble blends an XGBoost baseline + CatBoost residual. The
    scorer builds a separate feature matrix per arch and the blend produces
    a length-aligned vector. Pins the Phase F mixed-arch contract."""
    ensemble = _nar_new_ensemble()
    pool = BoosterPool(
        boosters={
            NAR_FALLBACK_MODEL_VERSION: _xgb_record(_StubBooster(0.0)),
            NAR_RESIDUAL_NEW: _cb_record(_StubBooster(1.0)),
        }
    )
    fallback = _StubBooster(99.0)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="nar:2026:0605:30:11",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="xgboost",  # NAR category-global
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=NAR_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == NAR_CLASS_NEW_ENSEMBLE_MODEL_VERSION
    assert outcome.fallback_reason is None
    # Within-race rank-normalisation collapses to the same shape per member, so
    # the weighted blend of identical normalised vectors is that vector.
    assert outcome.scores == [0.0, 0.5, 1.0]


def test_score_race_with_resolution_nar_lightgbm_ensemble_happy_path() -> None:
    """iter 36 NAR class-C blends an XGBoost baseline + LightGBM LambdaRank
    residual. The scorer builds a float32 matrix for the XGBoost baseline and a
    float64 matrix for the LightGBM member, scores both, and blends them to a
    length-aligned vector under the iter 36 ensemble label. Pins the iter 36
    mixed-arch contract end-to-end through the public scoring entry point."""
    ensemble = _nar_c_ensemble()
    pool = BoosterPool(
        boosters={
            NAR_FALLBACK_MODEL_VERSION: _xgb_record(_StubBooster(0.0)),
            NAR_LGB_RESIDUAL_C: _lgb_record(_StubBooster(1.0)),
        }
    )
    fallback = _StubBooster(99.0)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="nar:2026:0610:30:11",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="xgboost",  # NAR category-global
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=NAR_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == NAR_CLASS_C_ENSEMBLE_MODEL_VERSION
    assert outcome.fallback_reason is None
    assert outcome.scores == [0.0, 0.5, 1.0]


def test_score_race_with_resolution_falls_back_when_lightgbm_member_raises() -> None:
    """If the iter 36 LightGBM residual's ``predict`` raises at runtime, the
    whole-ensemble fallback posture still holds: the race scores with the
    category-global single-model booster under the NAR fallback label and a
    ``score-error:RuntimeError`` reason. Confirms adding the lgb arm did NOT
    break the existing fallback-to-iter12 safety net."""
    ensemble = _nar_c_ensemble()
    pool = BoosterPool(
        boosters={
            NAR_FALLBACK_MODEL_VERSION: _xgb_record(_StubBooster(0.0)),
            NAR_LGB_RESIDUAL_C: _lgb_record(_RaisingBooster()),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="nar:2026:0610:30:11",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="xgboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=NAR_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == NAR_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == "score-error:RuntimeError"
    assert outcome.scores == [0.4, 1.4, 2.4]


# ---------------------------------------------------------------------------
# score_race_with_resolution — failure fallback paths


def test_score_race_with_resolution_falls_back_when_member_missing() -> None:
    """One ensemble member missing from the pool -> single-model fallback path
    with the global model_version label and ``member-missing:<mv>`` reason."""
    ensemble = _two_member_ensemble()
    # Only the iter14 member is in the pool; iter22 is missing.
    pool = BoosterPool(
        boosters={JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0))}
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == f"member-missing:{ITER22_RESIDUAL_703}"
    assert outcome.scores == [0.4, 1.4, 2.4]


def test_score_race_with_resolution_falls_back_when_member_predict_raises() -> None:
    """One member's ``predict`` raises -> single-model fallback with
    ``score-error:RuntimeError`` reason."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0)),
            ITER22_RESIDUAL_703: _cb_record(_RaisingBooster()),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == "score-error:RuntimeError"
    assert outcome.scores == [0.4, 1.4, 2.4]


def test_score_race_with_resolution_falls_back_when_blend_shape_mismatch() -> None:
    """A member returns the wrong vector length -> the inner ensemble scorer
    rejects via length mismatch -> single-model fallback with
    ``score-error:ValueError`` reason."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0)),
            ITER22_RESIDUAL_703: _cb_record(_WrongLengthBooster()),
        }
    )
    fallback = _StubBooster(0.7)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    # blend_normalized -> array length mismatch -> ValueError; the wrapper
    # surfaces the class name only so logs stay grep-friendly.
    assert outcome.fallback_reason == "score-error:ValueError"
    assert outcome.scores == [0.7, 1.7, 2.7]


def test_score_race_with_resolution_falls_back_when_outer_shape_mismatch() -> None:
    """The non-baseline member returns a wrong-but-uniform length so the inner
    blend's ``normalize_within_race`` rejects via length mismatch (the outer
    ``len(blended) != len(entries)`` guard is exercised in the monkeypatched
    branch below).

    The baseline (is_baseline=True) is scored FIRST and MUST emit a correctly-
    lengthed vector so the two-pass ``_augment_entries`` can inject its raw
    scores per entry; the residual then emits the single-row vector that trips
    the inner scorer. Mirrors the original intent: the inner scorer ValueError
    surfaces first and the race falls back to the single-model booster."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            # Baseline returns a length-3 vector (correct) so the augment pass
            # can index per entry; the residual returns length-1 to mismatch.
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0)),
            ITER22_RESIDUAL_703: _cb_record(_WrongLengthBooster()),
        }
    )
    fallback = _StubBooster(0.9)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    # The residual emits a single-row vector. ``normalize_within_race`` then
    # mismatches race_id (len 3) vs scores (len 1) -> ValueError, so the inner
    # scorer error surfaces first.
    assert outcome.fallback_reason == "score-error:ValueError"
    assert outcome.scores == [0.9, 1.9, 2.9]


def test_score_race_with_resolution_outer_shape_guard_triggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force the outer ``len(blended) != len(entries)`` guard by monkey-patching
    the blend function to return a vector with the wrong length. Confirms the
    fallback path surfaces ``score-error:shape(<actual>!=<expected>)``."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0)),
            ITER22_RESIDUAL_703: _cb_record(_StubBooster(1.0)),
        }
    )
    fallback = _StubBooster(0.7)

    import numpy as np

    import predict_lib.ensemble_routing as routing_module

    def stub_score_with_ensemble(
        member_scores: object,
        weights: object,
        race_id: object,
        tiebreak: object,
    ) -> object:
        # Return a 2-element vector even though entries has 3.
        return np.array([0.1, 0.2], dtype=np.float64)

    monkeypatch.setattr(
        routing_module, "score_with_ensemble", stub_score_with_ensemble
    )

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == "score-error:shape(2!=3)"
    assert outcome.scores == [0.7, 1.7, 2.7]


# ---------------------------------------------------------------------------
# Tiebreak / race_id helpers (covered via the public scoring path)


def test_score_race_with_resolution_handles_entries_with_missing_ketto() -> None:
    """Entry missing ``ketto_toroku_bango`` -> tiebreak coerces to '' and the
    blend still produces a length-aligned vector (no TypeError)."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0)),
            ITER22_RESIDUAL_703: _cb_record(_StubBooster(0.0)),
        }
    )
    fallback = _StubBooster(0.0)
    entries: list[dict[str, object]] = [
        # No ``ketto_toroku_bango`` key — relies on the dict.get default arm.
        {"umaban": 1, "feature_a": 0.1, "feature_b": 0.2},
        {"ketto_toroku_bango": "9002", "umaban": 2, "feature_a": 0.2, "feature_b": 0.3},
    ]

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=entries,
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    assert outcome.fallback_reason is None
    assert len(outcome.scores) == 2


def test_score_race_with_resolution_uses_xgboost_path() -> None:
    """Architecture-aware feature-matrix construction (float32 cast) is exercised
    by passing ``xgboost`` — the booster receives a float32-quantised matrix
    rather than the float64 path used by CatBoost."""
    fallback = _StubBooster(0.2)
    pool = BoosterPool(boosters={})

    outcome = score_race_with_resolution(
        resolution=NAR_FALLBACK_MODEL_VERSION,
        race_id="nar:2026:0605:30:11",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="xgboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=NAR_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == NAR_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason is None
    assert outcome.scores == [0.2, 1.2, 2.2]


# ---------------------------------------------------------------------------
# Architecture dispatch helpers


def test_resolve_member_architecture_returns_xgboost_for_xgb_token() -> None:
    """The ``_resolve_member_architecture`` dispatcher picks XGBoost for any
    model_version containing the ``-xgb-`` token (the NAR baseline)."""
    from predict_lib.ensemble_routing import resolve_member_architecture

    assert resolve_member_architecture(NAR_FALLBACK_MODEL_VERSION, "nar") == "xgboost"


def test_resolve_member_architecture_returns_catboost_for_cb_token() -> None:
    """Member model_versions containing ``-cb-`` are CatBoost regardless of
    category — covers both JRA per-class members and NAR iter 30 residuals."""
    from predict_lib.ensemble_routing import resolve_member_architecture

    assert (
        resolve_member_architecture(JRA_CLASS_703_ENSEMBLE_MODEL_VERSION, "jra")
        == "catboost"
    )
    assert resolve_member_architecture(NAR_RESIDUAL_NEW, "nar") == "catboost"


def test_resolve_member_architecture_returns_lightgbm_for_lgb_token() -> None:
    """The iter 36 NAR class-C residual carries the ``-lgb-`` (and
    ``-lambdarank-``) token, so the dispatcher resolves it to LightGBM. The
    LightGBM check runs FIRST so the member never falls through to the
    ``-cb-`` / ``-xgb-`` arms."""
    from predict_lib.ensemble_routing import resolve_member_architecture

    assert resolve_member_architecture(NAR_LGB_RESIDUAL_C, "nar") == "lightgbm"


def test_resolve_member_architecture_returns_lightgbm_for_lambdarank_token() -> None:
    """A member named only with the ``-lambdarank-`` objective token (no
    ``-lgb-`` arch token) still resolves to LightGBM."""
    from predict_lib.ensemble_routing import resolve_member_architecture

    assert (
        resolve_member_architecture("iter36-nar-lambdarank-residual-C-v8", "nar")
        == "lightgbm"
    )


def test_resolve_member_architecture_falls_back_to_category_default() -> None:
    """An unrecognised model_version (no ``-xgb-`` or ``-cb-`` token) defers to
    the category default. Mirrors the legacy banei-cb-v7-lineage-wf-21y name
    which only carries the ``-cb-`` substring in production — the fallback
    branch here exists for forward-compat with future naming schemes."""
    from predict_lib.ensemble_routing import resolve_member_architecture

    # A made-up bareword without any token -> defer to JRA default (catboost).
    result_jra: Architecture = resolve_member_architecture("unknown-mv", "jra")
    assert result_jra == "catboost"
    # NAR default is xgboost.
    result_nar: Architecture = resolve_member_architecture("unknown-mv", "nar")
    assert result_nar == "xgboost"


# ---------------------------------------------------------------------------
# member_feature_order_matches


def test_member_feature_order_matches_empty_model_names_is_a_match() -> None:
    """An EMPTY ``model_feature_names`` (XGBoost / unpopulated) is a no-op
    match — the order assertion cannot run, so it returns True."""
    assert member_feature_order_matches((), RESIDUAL_METADATA_FEATURE_NAMES) is True


def test_member_feature_order_matches_exact_order_is_a_match() -> None:
    """Identical positional order returns True."""
    assert (
        member_feature_order_matches(
            RESIDUAL_METADATA_FEATURE_NAMES, RESIDUAL_METADATA_FEATURE_NAMES
        )
        is True
    )


def test_member_feature_order_matches_permuted_order_is_a_mismatch() -> None:
    """A permuted order (same set, different positions) returns False — the
    matrix the scorer would build is permuted relative to the booster."""
    permuted = ["feature_b", "feature_a", "iter14_score"]
    assert member_feature_order_matches(RESIDUAL_METADATA_FEATURE_NAMES, permuted) is False


# ---------------------------------------------------------------------------
# catboost_model_feature_names


def test_catboost_model_feature_names_non_catboost_returns_empty() -> None:
    """An XGBoost record is never order-checked — returns the empty tuple."""
    record = _xgb_record(_NamedBooster(["feature_a", "feature_b"]))
    assert catboost_model_feature_names(record) == ()


def test_catboost_model_feature_names_attr_absent_returns_empty() -> None:
    """A CatBoost record whose booster lacks ``feature_names_`` returns empty —
    the ``getattr(..., None)`` default is not a list/tuple."""
    record = _cb_record(_StubBooster(0.0))
    assert catboost_model_feature_names(record) == ()


def test_catboost_model_feature_names_attr_not_sequence_returns_empty() -> None:
    """A ``feature_names_`` that is not a list/tuple (e.g. a string) returns
    empty rather than mis-comparing character-by-character."""

    class _StrNamesBooster:
        feature_names_: str = "feature_a,feature_b"

        def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
            return [0.0 for _ in matrix]

    record = _cb_record(_StrNamesBooster())
    assert catboost_model_feature_names(record) == ()


def test_catboost_model_feature_names_non_string_item_returns_empty() -> None:
    """A ``feature_names_`` list with a non-string item returns empty — the
    positional comparison needs every name to be a string."""

    class _MixedNamesBooster:
        feature_names_: list[object]

        def __init__(self) -> None:
            # Per-instance assignment (not a class default) so ruff RUF012 does
            # not flag a shared-mutable-default — every test gets a fresh list.
            self.feature_names_ = ["feature_a", 7]

        def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
            return [0.0 for _ in matrix]

    record = _cb_record(_MixedNamesBooster())
    assert catboost_model_feature_names(record) == ()


def test_catboost_model_feature_names_valid_list_returns_tuple() -> None:
    """A populated, all-string ``feature_names_`` is returned as a tuple."""
    record = _cb_record(_NamedBooster(RESIDUAL_METADATA_FEATURE_NAMES))
    assert catboost_model_feature_names(record) == tuple(RESIDUAL_METADATA_FEATURE_NAMES)


# ---------------------------------------------------------------------------
# drop_order_mismatched_members


def test_drop_order_mismatched_members_keeps_matching_catboost() -> None:
    """A CatBoost member whose native ``feature_names_`` matches its metadata
    order is kept."""
    record = _cb_record_with_names(
        _NamedBooster(RESIDUAL_METADATA_FEATURE_NAMES), RESIDUAL_METADATA_FEATURE_NAMES
    )
    pool = BoosterPool(boosters={ITER22_RESIDUAL_703: record})
    kept = drop_order_mismatched_members(pool)
    assert kept.has(ITER22_RESIDUAL_703) is True


def test_drop_order_mismatched_members_drops_permuted_catboost(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A CatBoost member whose booster order disagrees with metadata is dropped
    and a ``member-order-mismatch:<mv>`` line is logged to stderr."""
    permuted_booster = _NamedBooster(["feature_b", "feature_a", "iter14_score"])
    record = _cb_record_with_names(permuted_booster, RESIDUAL_METADATA_FEATURE_NAMES)
    pool = BoosterPool(boosters={ITER22_RESIDUAL_703: record})

    kept = drop_order_mismatched_members(pool)

    assert kept.has(ITER22_RESIDUAL_703) is False
    captured = capsys.readouterr()
    assert f"member-order-mismatch:{ITER22_RESIDUAL_703}" in captured.err


def test_drop_order_mismatched_members_always_keeps_xgboost() -> None:
    """An XGBoost member (empty ``feature_names_`` after the loader clears it)
    is never order-checked, so it always passes even with a metadata order set."""
    record = PoolBooster(
        booster=_StubBooster(0.0),
        architecture="xgboost",
        feature_names=tuple(NAR_BASELINE_METADATA_FEATURE_NAMES),
    )
    pool = BoosterPool(boosters={NAR_FALLBACK_MODEL_VERSION: record})
    kept = drop_order_mismatched_members(pool)
    assert kept.has(NAR_FALLBACK_MODEL_VERSION) is True


# ---------------------------------------------------------------------------
# init_member_pool — metadata failure postures


def test_init_member_pool_skips_member_when_metadata_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A non-baseline member whose sibling metadata.json is ABSENT is skipped
    (logged ``member-metadata-missing:<mv>``); the baseline still loads so the
    ensemble can fall back to it."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    _write_baseline_model_json(tmp_path, "jra", JRA_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "jra",
        JRA_FALLBACK_MODEL_VERSION,
        _feature_names_payload(BASELINE_METADATA_FEATURE_NAMES),
    )
    # Residual model.json on disk but NO metadata.json sidecar.
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    pool = init_member_pool(tmp_path, "jra")

    assert pool.has(JRA_FALLBACK_MODEL_VERSION) is True
    assert pool.has(ITER22_RESIDUAL_703) is False
    captured = capsys.readouterr()
    assert f"member-metadata-missing:{ITER22_RESIDUAL_703}" in captured.err


def test_init_member_pool_skips_member_when_metadata_corrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A non-baseline member whose metadata.json is malformed (missing
    ``feature_names`` key) is skipped + logged, exercising the ValueError arm of
    the non-baseline failure posture."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    _write_baseline_model_json(tmp_path, "jra", JRA_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "jra",
        JRA_FALLBACK_MODEL_VERSION,
        _feature_names_payload(BASELINE_METADATA_FEATURE_NAMES),
    )
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)
    # Malformed sidecar: no ``feature_names`` key.
    _write_member_metadata_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703, {"x": 1})

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    pool = init_member_pool(tmp_path, "jra")

    assert pool.has(ITER22_RESIDUAL_703) is False
    captured = capsys.readouterr()
    assert f"member-metadata-missing:{ITER22_RESIDUAL_703}" in captured.err


def test_init_member_pool_raises_when_baseline_metadata_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The category-global baseline's metadata.json is the fallback safety net —
    a missing baseline sidecar re-raises ``FileNotFoundError`` rather than
    silently degrading. The residual has a valid sidecar so the failure is
    isolated to the baseline path."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    # Baseline model.json present but NO metadata.json sidecar.
    _write_baseline_model_json(tmp_path, "jra", JRA_FALLBACK_MODEL_VERSION)
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)
    _write_member_metadata_json(
        tmp_path,
        "jra",
        "703",
        ITER22_RESIDUAL_703,
        _feature_names_payload(RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    with pytest.raises(FileNotFoundError, match="member metadata missing"):
        init_member_pool(tmp_path, "jra")


def test_init_member_pool_reraises_when_perclass_baseline_metadata_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the baseline model_version is discovered under the PER-CLASS layout
    (it is a manifest member) but its sibling metadata.json is absent, the
    per-class resolution arm re-raises rather than skipping — the baseline is the
    fallback safety net so a broken sidecar fails LOUD on either layout.

    The residual carries a valid sidecar, isolating the failure to the per-class
    baseline copy."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    # Baseline placed under per-class with model.json but NO metadata.json.
    _write_member_model_json(tmp_path, "jra", "703", JRA_FALLBACK_MODEL_VERSION)
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)
    _write_member_metadata_json(
        tmp_path,
        "jra",
        "703",
        ITER22_RESIDUAL_703,
        _feature_names_payload(RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    with pytest.raises(FileNotFoundError, match="member metadata missing"):
        init_member_pool(tmp_path, "jra")


def test_init_member_pool_drops_order_mismatched_member(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end: a CatBoost member whose loaded booster ``feature_names_``
    disagrees with its metadata order is dropped by the post-load order assertion
    (logged ``member-order-mismatch:<mv>``). The baseline (matching order) is
    kept."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    _write_baseline_model_json(tmp_path, "jra", JRA_FALLBACK_MODEL_VERSION)
    _write_baseline_metadata_json(
        tmp_path,
        "jra",
        JRA_FALLBACK_MODEL_VERSION,
        _feature_names_payload(BASELINE_METADATA_FEATURE_NAMES),
    )
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)
    _write_member_metadata_json(
        tmp_path,
        "jra",
        "703",
        ITER22_RESIDUAL_703,
        _feature_names_payload(RESIDUAL_METADATA_FEATURE_NAMES),
    )

    def fake_load(model_path: str) -> BoosterLike:
        # The baseline booster reports the matching order; the residual booster
        # reports a PERMUTED order so the post-load assertion drops it.
        if "iter14" in model_path:
            return _NamedBooster(BASELINE_METADATA_FEATURE_NAMES)
        return _NamedBooster(["feature_b", "feature_a", "iter14_score"])

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    pool = init_member_pool(tmp_path, "jra")

    assert pool.has(JRA_FALLBACK_MODEL_VERSION) is True
    assert pool.has(ITER22_RESIDUAL_703) is False
    captured = capsys.readouterr()
    assert f"member-order-mismatch:{ITER22_RESIDUAL_703}" in captured.err


# ---------------------------------------------------------------------------
# member_feature_names_for_record


def test_member_feature_names_uses_record_order_when_present() -> None:
    """A record carrying its OWN ``feature_names`` uses that order, ignoring the
    caller-supplied global list."""
    record = _cb_record_with_names(_StubBooster(0.0), RESIDUAL_METADATA_FEATURE_NAMES)
    result = member_feature_names_for_record(record, FEATURE_NAMES)
    assert result == tuple(RESIDUAL_METADATA_FEATURE_NAMES)


def test_member_feature_names_falls_back_to_global_when_empty() -> None:
    """A record with an EMPTY ``feature_names`` (legacy / single-model fallback)
    uses the caller-supplied global list."""
    record = _cb_record(_StubBooster(0.0))
    result = member_feature_names_for_record(record, FEATURE_NAMES)
    assert result == tuple(FEATURE_NAMES)


# ---------------------------------------------------------------------------
# column_gap


def test_column_gap_zero_when_all_features_present() -> None:
    """All required member features are entry keys -> gap 0."""
    gap = column_gap(["feature_a", "feature_b"], frozenset({"feature_a", "feature_b"}), None)
    assert gap == 0


def test_column_gap_counts_missing_features() -> None:
    """Member features absent from the entry keys are counted."""
    gap = column_gap(
        ["feature_a", "feature_b", "feature_c"], frozenset({"feature_a"}), None
    )
    assert gap == 2


def test_column_gap_excludes_injected_score_col() -> None:
    """The injected ``score_col`` is supplied by the two-pass injection, so it is
    excluded from the gap — a member requiring only the score column beyond the
    entry keys must NOT gap."""
    gap = column_gap(
        ["feature_a", "feature_b", "iter14_score"],
        frozenset({"feature_a", "feature_b"}),
        "iter14_score",
    )
    assert gap == 0


def test_column_gap_score_col_none_keeps_score_required() -> None:
    """When ``score_col`` is None nothing is discarded — a missing ``iter14_score``
    counts toward the gap (the score_col-is-None branch)."""
    gap = column_gap(
        ["feature_a", "iter14_score"], frozenset({"feature_a"}), None
    )
    assert gap == 1


# ---------------------------------------------------------------------------
# find_baseline_member


def test_find_baseline_member_returns_flagged_member() -> None:
    """The member with ``is_baseline=True`` is returned."""
    ensemble = _two_member_ensemble()
    baseline = find_baseline_member(ensemble)
    assert baseline is not None
    assert baseline.model_version == JRA_FALLBACK_MODEL_VERSION
    assert baseline.is_baseline is True


def test_find_baseline_member_returns_none_when_no_baseline() -> None:
    """A manifest with no ``is_baseline`` member returns None — the loop walks
    every member and falls through."""
    ensemble = PerClassEnsemble(
        model_version=JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        category="jra",
        kyoso_joken_code="703",
        ensemble_type="rank_blend",
        members=(
            EnsembleMember(
                model_version=JRA_FALLBACK_MODEL_VERSION, weight=0.3, is_baseline=False
            ),
            EnsembleMember(
                model_version=ITER22_RESIDUAL_703, weight=0.7, is_baseline=False
            ),
        ),
    )
    assert find_baseline_member(ensemble) is None


# ---------------------------------------------------------------------------
# augment_entries_with_score_col


def test_augment_entries_passes_through_when_score_col_none() -> None:
    """A category with no synthetic-score feature (``score_col`` None) passes the
    entries through unchanged."""
    import numpy as np

    entries = _three_horse_entries()
    result = augment_entries_with_score_col(entries, None, np.array([1.0, 2.0, 3.0]))
    assert result == entries


def test_augment_entries_injects_raw_scores_verbatim() -> None:
    """The baseline's RAW scores are injected verbatim under ``score_col`` (no
    normalisation) — one float per entry, original keys preserved."""
    import numpy as np

    entries = _three_horse_entries()
    raw = np.array([12.5, -3.25, 0.0], dtype=np.float64)
    result = augment_entries_with_score_col(entries, JRA_SCORE_COL, raw)

    assert [entry[JRA_SCORE_COL] for entry in result] == [12.5, -3.25, 0.0]
    # Original keys survive the merge.
    assert result[0]["feature_a"] == 0.1
    assert result[1]["ketto_toroku_bango"] == "9002"


# ---------------------------------------------------------------------------
# SCORE_FEATURE_BY_CATEGORY


def test_score_feature_by_category_maps_jra_and_nar() -> None:
    """The synthetic-score feature is ``iter14_score`` for JRA and
    ``iter12_score`` for NAR; Ban-ei has no entry (no synthetic score)."""
    assert SCORE_FEATURE_BY_CATEGORY["jra"] == "iter14_score"
    assert SCORE_FEATURE_BY_CATEGORY["nar"] == "iter12_score"
    assert SCORE_FEATURE_BY_CATEGORY.get("ban-ei") is None


# ---------------------------------------------------------------------------
# score_member — column-gap guard


def test_score_member_returns_column_gap_when_feature_absent() -> None:
    """A NON-BASELINE member whose metadata lists a feature ABSENT from the
    entry keys returns ``(None, member-column-gap:<mv>:<n>)`` so the ensemble
    falls back rather than silently 0-filling."""
    member = EnsembleMember(
        model_version=ITER22_RESIDUAL_703, weight=0.7, is_baseline=False
    )
    record = _cb_record_with_names(
        _StubBooster(0.0), ["feature_a", "feature_b", "missing_feature"]
    )
    pool = BoosterPool(boosters={ITER22_RESIDUAL_703: record})
    matrix_by_key: dict[MatrixCacheKey, Sequence[Sequence[float]]] = {}

    scores, reason = score_member(
        member, pool, matrix_by_key, _three_horse_entries(), FEATURE_NAMES, JRA_SCORE_COL
    )

    assert scores is None
    assert reason == f"member-column-gap:{ITER22_RESIDUAL_703}:1"


def test_score_member_baseline_skips_column_gap_guard() -> None:
    """The BASELINE member is exempt from the column-gap guard because the NAR
    baseline metadata carries a legacy duplicate-suffix column
    (``shusso_tosu`` at index 2 + ``shusso_tosu_1`` at 146) that the parquet
    only emits once — the legacy ``build_feature_matrix`` 0-fill preserves the
    pre-WIP baseline behaviour for the safety-net booster."""
    member = EnsembleMember(
        model_version=JRA_FALLBACK_MODEL_VERSION, weight=0.3, is_baseline=True
    )
    record = _cb_record_with_names(
        _StubBooster(0.0), ["feature_a", "feature_b", "missing_feature"]
    )
    pool = BoosterPool(boosters={JRA_FALLBACK_MODEL_VERSION: record})
    matrix_by_key: dict[MatrixCacheKey, Sequence[Sequence[float]]] = {}

    scores, reason = score_member(
        member, pool, matrix_by_key, _three_horse_entries(), FEATURE_NAMES, JRA_SCORE_COL
    )

    # No gap reason — baseline is silently 0-filled by build_feature_matrix.
    assert reason is None
    assert scores is not None


def test_score_member_does_not_gap_on_injected_score_col() -> None:
    """A member whose only beyond-entry feature is the injected ``score_col`` does
    NOT gap on pass 2 (the score_col is supplied by the augment pass)."""
    member = EnsembleMember(
        model_version=ITER22_RESIDUAL_703, weight=0.7, is_baseline=False
    )
    record = _cb_record_with_names(
        _StubBooster(0.0), ["feature_a", "feature_b", JRA_SCORE_COL]
    )
    pool = BoosterPool(boosters={ITER22_RESIDUAL_703: record})
    matrix_by_key: dict[MatrixCacheKey, Sequence[Sequence[float]]] = {}

    scores, reason = score_member(
        member, pool, matrix_by_key, _three_horse_entries(), FEATURE_NAMES, JRA_SCORE_COL
    )

    assert reason is None
    assert scores is not None


def test_score_member_empty_entries_uses_empty_key_set() -> None:
    """An empty entries sequence yields an empty key set; a member with a
    declared feature then gaps (the ``entries[0]`` guard is skipped)."""
    member = EnsembleMember(
        model_version=ITER22_RESIDUAL_703, weight=0.7, is_baseline=False
    )
    record = _cb_record_with_names(_StubBooster(0.0), ["feature_a"])
    pool = BoosterPool(boosters={ITER22_RESIDUAL_703: record})
    matrix_by_key: dict[MatrixCacheKey, Sequence[Sequence[float]]] = {}

    scores, reason = score_member(member, pool, matrix_by_key, [], FEATURE_NAMES, None)

    assert scores is None
    assert reason == f"member-column-gap:{ITER22_RESIDUAL_703}:1"


# ---------------------------------------------------------------------------
# score_race_with_resolution — column-gap + no-baseline fallback postures


def test_score_race_falls_back_when_membercolumn_gap(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An ensemble whose non-baseline member declares a feature absent from the
    entries falls back to the single-model booster with a
    ``member-column-gap:<mv>:<n>`` reason (the whole-ensemble fallback posture)."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record_with_names(
                _StubBooster(0.0), BASELINE_METADATA_FEATURE_NAMES
            ),
            ITER22_RESIDUAL_703: _cb_record_with_names(
                _StubBooster(1.0), ["feature_a", "feature_b", "relationship_missing"]
            ),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == f"member-column-gap:{ITER22_RESIDUAL_703}:1"
    assert outcome.scores == [0.4, 1.4, 2.4]


def test_score_race_falls_back_when_no_baseline_in_manifest() -> None:
    """A manifest with NO ``is_baseline`` member trips the new
    ``score-error:no-baseline`` guard (the baseline is scored first to inject the
    synthetic score), so the race falls back to the single-model booster."""
    ensemble = PerClassEnsemble(
        model_version=JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        category="jra",
        kyoso_joken_code="703",
        ensemble_type="rank_blend",
        members=(
            EnsembleMember(
                model_version=JRA_FALLBACK_MODEL_VERSION, weight=0.3, is_baseline=False
            ),
            EnsembleMember(
                model_version=ITER22_RESIDUAL_703, weight=0.7, is_baseline=False
            ),
        ),
    )
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_StubBooster(0.0)),
            ITER22_RESIDUAL_703: _cb_record(_StubBooster(1.0)),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == "score-error:no-baseline"
    assert outcome.scores == [0.4, 1.4, 2.4]


def test_score_race_falls_back_when_baseline_member_missing_from_pool() -> None:
    """The baseline is scored FIRST; if it is absent from the pool the
    ``member-missing:<mv>`` reason surfaces before any non-baseline member is
    touched, and the race falls back to the single-model booster."""
    ensemble = _two_member_ensemble()
    # Only the residual is in the pool; the baseline is missing.
    pool = BoosterPool(boosters={ITER22_RESIDUAL_703: _cb_record(_StubBooster(1.0))})
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == f"member-missing:{JRA_FALLBACK_MODEL_VERSION}"
    assert outcome.scores == [0.4, 1.4, 2.4]


def test_score_race_falls_back_when_baseline_predict_raises() -> None:
    """The baseline (scored first) raising surfaces ``score-error:RuntimeError``
    before pass 2 runs — the race falls back to the single-model booster."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_RaisingBooster()),
            ITER22_RESIDUAL_703: _cb_record(_StubBooster(1.0)),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.model_version == JRA_FALLBACK_MODEL_VERSION
    assert outcome.fallback_reason == "score-error:RuntimeError"
    assert outcome.scores == [0.4, 1.4, 2.4]


# ---------------------------------------------------------------------------
# Two-pass synthetic-score injection (happy path)


def test_score_ensemble_two_pass_injects_baseline_raw_scores() -> None:
    """The two-pass scorer scores the baseline on the PLAIN entries, then injects
    its RAW scores verbatim into the residual member's matrix under the JRA
    ``iter14_score`` column.

    The recording boosters capture every matrix they score. We assert:

    * the baseline matrix has the baseline's feature width (2 cols, no score col);
    * the residual matrix carries the baseline's RAW scores (verbatim floats) in
      the injected ``iter14_score`` position (last column of its 3-col order)."""
    baseline_raw = [7.0, 11.0, 13.0]
    baseline_booster = _FixedScoreBooster(baseline_raw)
    residual_booster = _MatrixRecordingBooster()
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record_with_names(
                baseline_booster, BASELINE_METADATA_FEATURE_NAMES
            ),
            ITER22_RESIDUAL_703: _cb_record_with_names(
                residual_booster, RESIDUAL_METADATA_FEATURE_NAMES
            ),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    # Happy path: the ensemble label is emitted, no fallback.
    assert outcome.fallback_reason is None
    assert outcome.model_version == JRA_CLASS_703_ENSEMBLE_MODEL_VERSION
    # The residual booster recorded exactly one matrix (3 rows x 3 cols).
    assert len(residual_booster.seen_matrices) == 1
    residual_matrix = residual_booster.seen_matrices[0]
    assert len(residual_matrix) == 3
    # ``iter14_score`` is the 3rd (index 2) feature in the residual's order, and
    # carries the baseline's RAW scores verbatim (no normalisation).
    injected_col = [row[2] for row in residual_matrix]
    assert injected_col == baseline_raw
    # The first two columns mirror the plain entry features.
    assert [row[0] for row in residual_matrix] == [0.1, 0.2, 0.3]
    assert [row[1] for row in residual_matrix] == [0.4, 0.5, 0.6]


def test_score_ensemble_baseline_scored_on_plain_entries() -> None:
    """Pass 1 scores the baseline on the PLAIN (un-augmented) entries — its
    matrix has the baseline's 2-col width with NO injected score column."""
    baseline_booster = _MatrixRecordingBooster()
    residual_booster = _StubBooster(1.0)
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record_with_names(
                baseline_booster, BASELINE_METADATA_FEATURE_NAMES
            ),
            ITER22_RESIDUAL_703: _cb_record_with_names(
                residual_booster, RESIDUAL_METADATA_FEATURE_NAMES
            ),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.fallback_reason is None
    baseline_matrix = baseline_booster.seen_matrices[0]
    # Baseline order is 2 columns — no ``iter14_score`` injected on pass 1.
    assert [len(row) for row in baseline_matrix] == [2, 2, 2]
    assert [row[0] for row in baseline_matrix] == [0.1, 0.2, 0.3]


# ---------------------------------------------------------------------------
# Matrix cache keying (architecture + feature order)


def test_matrix_cache_distinct_for_different_feature_orders() -> None:
    """Two members with the SAME arch but DIFFERENT feature orders each get their
    own correctly-shaped matrix (the core wrong-width fix). The baseline (2 cols)
    and residual (3 cols) record different matrix widths."""
    baseline_booster = _MatrixRecordingBooster()
    residual_booster = _MatrixRecordingBooster()
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record_with_names(
                baseline_booster, BASELINE_METADATA_FEATURE_NAMES
            ),
            ITER22_RESIDUAL_703: _cb_record_with_names(
                residual_booster, RESIDUAL_METADATA_FEATURE_NAMES
            ),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.fallback_reason is None
    # Different widths confirm separate matrices were built per member order.
    baseline_width = len(baseline_booster.seen_matrices[0][0])
    residual_width = len(residual_booster.seen_matrices[0][0])
    assert baseline_width == 2
    assert residual_width == 3


def test_matrix_cache_shared_for_same_arch_and_feature_order() -> None:
    """Two non-baseline members with the SAME arch AND the SAME feature order
    share one built matrix — the second member's matrix is the cached object, so
    both record the identical matrix contents from the single build."""
    member_a = _MatrixRecordingBooster()
    member_b = _MatrixRecordingBooster()
    shared_names = RESIDUAL_METADATA_FEATURE_NAMES
    second_residual_mv = "iter23-jra-cb-residual-703-v8"
    ensemble = PerClassEnsemble(
        model_version=JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        category="jra",
        kyoso_joken_code="703",
        ensemble_type="rank_blend",
        members=(
            EnsembleMember(
                model_version=JRA_FALLBACK_MODEL_VERSION, weight=0.2, is_baseline=True
            ),
            EnsembleMember(
                model_version=ITER22_RESIDUAL_703, weight=0.4, is_baseline=False
            ),
            EnsembleMember(
                model_version=second_residual_mv, weight=0.4, is_baseline=False
            ),
        ),
    )
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record_with_names(
                _FixedScoreBooster([3.0, 5.0, 9.0]), BASELINE_METADATA_FEATURE_NAMES
            ),
            ITER22_RESIDUAL_703: _cb_record_with_names(member_a, shared_names),
            second_residual_mv: _cb_record_with_names(member_b, shared_names),
        }
    )
    fallback = _StubBooster(0.4)

    outcome = score_race_with_resolution(
        resolution=ensemble,
        race_id="jra:2026:0605:05:08",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="catboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version=JRA_FALLBACK_MODEL_VERSION,
    )

    assert outcome.fallback_reason is None
    # Both residuals scored the SAME cached matrix (same arch + same order), so
    # the recorded contents are identical, including the injected score column.
    assert member_a.seen_matrices[0] == member_b.seen_matrices[0]
    assert [row[2] for row in member_a.seen_matrices[0]] == [3.0, 5.0, 9.0]
