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
    EnsembleRouteOutcome,
    init_member_pool,
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


def _cb_record(booster: BoosterLike) -> PoolBooster:
    """Wrap a CatBoost-flavoured stub in a ``PoolBooster`` record."""
    return PoolBooster(booster=booster, architecture="catboost")


def _xgb_record(booster: BoosterLike) -> PoolBooster:
    """Wrap an XGBoost-flavoured stub in a ``PoolBooster`` record."""
    return PoolBooster(booster=booster, architecture="xgboost")


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


# ---------------------------------------------------------------------------
# Adapter stubs


class _FakeAdapter(ModuleType):
    """Typed stand-in for ``catboost_adapter`` / ``xgboost_adapter`` modules.

    Both attributes are declared so basedpyright stays quiet on the assignment;
    only one is actually set per test. The class attribute typing matches the
    real loaders' signatures.
    """

    load_catboost_booster: object
    load_xgboost_booster: object


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
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)

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
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)

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
    _write_member_model_json(tmp_path, "nar", "NEW", NAR_RESIDUAL_NEW)

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
    # Both layouts hold the baseline.
    _write_baseline_model_json(tmp_path, "nar", NAR_FALLBACK_MODEL_VERSION)
    _write_member_model_json(tmp_path, "nar", "NEW", NAR_FALLBACK_MODEL_VERSION)
    _write_member_model_json(tmp_path, "nar", "NEW", NAR_RESIDUAL_NEW)

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

    def fake_catboost(model_path: str) -> BoosterLike:
        return _StubBooster(0.5)

    _install_fake_catboost_adapter(monkeypatch, fake_catboost)

    pool = init_member_pool(tmp_path, "nar")

    assert pool.has(NAR_RESIDUAL_NEW) is True
    assert pool.has(NAR_FALLBACK_MODEL_VERSION) is False


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
    """All members return the same wrong-but-uniform length so the inner blend
    succeeds, but the outer ``len(blended) != len(entries)`` guard fires."""
    ensemble = _two_member_ensemble()
    pool = BoosterPool(
        boosters={
            JRA_FALLBACK_MODEL_VERSION: _cb_record(_WrongLengthBooster()),
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
    # Both members emit a single-row vector. ``normalize_within_race`` would
    # then mismatch race_id (len 3) vs scores (len 1) -> ValueError. So the
    # inner scorer error surfaces first; the outer shape guard is exercised in
    # the no-mismatch-inside branch below.
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
